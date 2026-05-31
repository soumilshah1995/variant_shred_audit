#!/usr/bin/env python3
"""
Compare Iceberg variant write performance: shredding on vs off.

Creates two Iceberg v3 tables (shred on / off), writes GitHub archive JSON as VARIANT,
and prints epoch timing stats.

Requires: PySpark 4.x, pyarrow (optional), github_archive.json.gz

Run:
  python3 github_variant_shred_benchmark.py /path/to/github_archive.json.gz
  ICEBERG_WAREHOUSE=file:///tmp/wh python3 github_variant_shred_benchmark.py data.json.gz
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
import time
from pathlib import Path

DEFAULT_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
DEFAULT_CATALOG = "dev"
DEFAULT_RUNS = 10


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Benchmark Iceberg variant shredding write performance")
    p.add_argument("json_file", nargs="?", help="Path to github_archive.json.gz (or JSON lines)")
    p.add_argument("--warehouse", default=os.environ.get("ICEBERG_WAREHOUSE"), help="Warehouse URI")
    p.add_argument("--catalog", default=os.environ.get("ICEBERG_CATALOG", DEFAULT_CATALOG))
    p.add_argument("--packages", default=os.environ.get("ICEBERG_PACKAGES", DEFAULT_PACKAGE))
    p.add_argument("--runs", type=int, default=DEFAULT_RUNS, help="Number of write epochs")
    p.add_argument("--buffer-size", type=int, default=10000, help="variant-inference-buffer-size")
    return p.parse_args()


def build_spark(catalog: str, warehouse: str, packages: str):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    from pyspark.sql import SparkSession

    wh = Path(warehouse.replace("file://", "")).expanduser().resolve()
    wh.mkdir(parents=True, exist_ok=True)
    warehouse_uri = wh.as_uri()

    return (
        SparkSession.builder.appName("IcebergVariantShredBenchmark")
        .config("spark.jars.packages", packages)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse_uri)
        .config("spark.sql.defaultCatalog", catalog)
        .getOrCreate()
    ), warehouse_uri


def prepare_tables(spark, catalog: str, buffer_size: int) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.demo")
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.demo.github_no_shredding")
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.demo.github_with_shredding")

    spark.sql(
        f"""
        CREATE TABLE {catalog}.demo.github_no_shredding (
            id BIGINT,
            v VARIANT
        )
        USING iceberg
        TBLPROPERTIES (
            'format-version' = '3',
            'write.parquet.shred-variants' = 'false'
        )
        """
    )

    spark.sql(
        f"""
        CREATE TABLE {catalog}.demo.github_with_shredding (
            id BIGINT,
            v VARIANT
        )
        USING iceberg
        TBLPROPERTIES (
            'format-version' = '3',
            'write.parquet.shred-variants' = 'true',
            'write.parquet.variant-inference-buffer-size' = '{buffer_size}'
        )
        """
    )


def load_variant_df(spark, json_path: Path):
    from pyspark.sql.functions import col, monotonically_increasing_id, parse_json, struct, to_json

    raw = spark.read.json(str(json_path))
    return raw.select(
        monotonically_increasing_id().cast("bigint").alias("id"),
        parse_json(to_json(struct(*[col(c) for c in raw.columns]))).alias("v"),
    )


def run_epoch(spark, df, table: str, job_group: str) -> float:
    spark.sparkContext.setJobGroup(job_group, job_group)
    start = time.time()
    df.writeTo(table).append()
    spark.sparkContext.setJobGroup("", "")
    return time.time() - start


def print_stats(label: str, times: list[float]) -> None:
    print(f"\n{label} ({len(times)} epochs):")
    for i, t in enumerate(times, 1):
        print(f"  Epoch {i:2d}: {t:.2f}s")
    if not times:
        return
    print(f"\n  Average:  {statistics.mean(times):.2f}s")
    print(f"  Median:   {statistics.median(times):.2f}s")
    if len(times) > 1:
        print(f"  Std Dev:  {statistics.stdev(times):.2f}s")
    print(f"  Min:      {min(times):.2f}s")
    print(f"  Max:      {max(times):.2f}s")


def main() -> None:
    args = parse_args()
    if not args.json_file:
        sys.exit("Pass path to github_archive.json.gz (or set as first argument)")
    if not args.warehouse:
        args.warehouse = str(Path.cwd() / "warehouse")

    json_path = Path(args.json_file).expanduser().resolve()
    if not json_path.exists():
        sys.exit(f"File not found: {json_path}")

    print(f"Iceberg package: {args.packages}")
    print(f"JSON input:      {json_path}")
    print(f"Warehouse:       {args.warehouse}")

    spark, warehouse_uri = build_spark(args.catalog, args.warehouse, args.packages)
    print(f"Spark version:   {spark.version}")
    print(f"Warehouse URI:   {warehouse_uri}")

    prepare_tables(spark, args.catalog, args.buffer_size)
    print("Tables created.")

    df_variant = load_variant_df(spark, json_path)
    df_variant.printSchema()

    row_count = df_variant.count()
    print(f"\nTesting {row_count:,} rows × {args.runs} epochs\n")

    no_shred_times: list[float] = []
    with_shred_times: list[float] = []

    for run in range(1, args.runs + 1):
        print(f"=== EPOCH {run}/{args.runs} ===")

        t = run_epoch(
            spark,
            df_variant,
            f"{args.catalog}.demo.github_no_shredding",
            f"epoch_{run}_no_shred",
        )
        no_shred_times.append(t)
        print(f"  NO SHREDDING:   {t:.2f}s")

        t = run_epoch(
            spark,
            df_variant,
            f"{args.catalog}.demo.github_with_shredding",
            f"epoch_{run}_with_shred",
        )
        with_shred_times.append(t)
        print(f"  WITH SHREDDING: {t:.2f}s\n")

    avg_no = statistics.mean(no_shred_times)
    avg_yes = statistics.mean(with_shred_times)
    pct = (avg_yes - avg_no) / avg_no * 100 if avg_no else 0

    print("=" * 80)
    print("WRITE PERFORMANCE SUMMARY")
    print("=" * 80)
    print_stats("NO SHREDDING", no_shred_times)
    print_stats("WITH SHREDDING", with_shred_times)

    print(f"\n{'=' * 80}")
    print("COMPARISON")
    if avg_no:
        print(f"  Average slowdown: {pct:+.1f}% ({avg_yes / avg_no:.2f}x)")
    print(f"  Rows per write:     {row_count:,}")
    if avg_no:
        print(f"  Throughput no shred:   {row_count / avg_no:,.0f} rows/s")
    if avg_yes:
        print(f"  Throughput with shred: {row_count / avg_yes:,.0f} rows/s")
    print("=" * 80)
    print()
    print("Next: audit shredding coverage on the with-shredding table:")
    print(f"  python3 variant_shred_audit.py \\")
    print(f"    --parquet-dir {warehouse_uri.replace('file://', '')}/demo/github_with_shredding/data \\")
    print(f"    --variant-col v")

    spark.stop()


if __name__ == "__main__":
    main()
