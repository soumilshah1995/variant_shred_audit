#!/usr/bin/env python3
"""
Variant Shred Coverage Report — audit Iceberg variant shredding from Parquet metadata.

Modes:
  1) Parquet dir (no Spark):  --parquet-dir /path/to/table/data
  2) Iceberg table + Spark:   --table catalog.ns.table --warehouse file:///wh
  3) spark-submit (recommended for --table): pass Spark/Iceberg conf via spark-submit,
     then add --use-existing-spark

Sections A/B/C use Parquet metadata + column statistics (NOT row sampling).
Section B2 (--scan-rows) is optional and sampled.

Requires: pyarrow (always). PySpark only for --table or --scan-rows with Spark.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
DEFAULT_CATALOG_CLASS = "org.apache.iceberg.spark.SparkCatalog"
ENV_WAREHOUSE = "ICEBERG_WAREHOUSE"
ENV_CATALOG = "ICEBERG_CATALOG"
ENV_PACKAGES = "ICEBERG_PACKAGES"


@dataclass
class ValueColStats:
    path: str
    null_rows: int = 0
    non_null_rows: int = 0

    @property
    def total(self) -> int:
        return self.null_rows + self.non_null_rows

    @property
    def full_pct(self) -> float:
        return 100.0 * self.null_rows / self.total if self.total else 0.0

    @property
    def partial_pct(self) -> float:
        return 100.0 * self.non_null_rows / self.total if self.total else 0.0


@dataclass
class FileAudit:
    path: str
    record_count: int
    parquet_columns: int
    shredded_paths: set[str] = field(default_factory=set)
    value_stats: dict[str, ValueColStats] = field(default_factory=dict)

    @property
    def root_value(self) -> ValueColStats | None:
        for p, s in self.value_stats.items():
            parts = p.split(".")
            if parts[-1] == "value" and parts.count("typed_value") == 0:
                return s
        return None

    @property
    def mode(self) -> str:
        root = self.root_value
        if root is None:
            return "NO VARIANT"
        if root.partial_pct == 0:
            return "FULL (root)"
        if root.full_pct == 0:
            return "UNSHREDDED"
        return "MIXED (root)"


@dataclass
class PathQuality:
    """Per JSON path quality per Parquet VariantShredding spec."""

    path: str
    spec_status: str  # FULL, PARTIAL, NOT_SHREDDED
    file_coverage_pct: float
    row_full_pct: float
    row_partial_pct: float
    query_benefit: str  # HIGH, MEDIUM, LOW, NONE


@dataclass
class TableAudit:
    label: str
    location: str
    files: list[FileAudit] = field(default_factory=list)
    total_files_available: int = 0

    @property
    def total_rows(self) -> int:
        return sum(f.record_count for f in self.files)

    @property
    def all_shredded_paths(self) -> set[str]:
        out: set[str] = set()
        for f in self.files:
            out.update(f.shredded_paths)
        return out

    @property
    def root_full_rows(self) -> int:
        return sum((f.root_value.null_rows if f.root_value else 0) for f in self.files)

    @property
    def root_partial_rows(self) -> int:
        return sum((f.root_value.non_null_rows if f.root_value else 0) for f in self.files)


@dataclass
class SparkConfig:
    catalog: str
    warehouse: str | None
    packages: str | None
    catalog_type: str
    catalog_class: str
    extensions: str
    use_existing: bool
    extra_confs: dict[str, str] = field(default_factory=dict)
    stop_session: bool = True


def parse_spark_conf(values: list[str] | None) -> dict[str, str]:
    confs: dict[str, str] = {}
    for item in values or []:
        if "=" not in item:
            raise SystemExit(f"Invalid --spark-conf (expected key=value): {item}")
        key, val = item.split("=", 1)
        confs[key.strip()] = val.strip()
    return confs


def build_spark_config(args: argparse.Namespace) -> SparkConfig:
    catalog = args.catalog or os.environ.get(ENV_CATALOG, "dev")
    warehouse = args.warehouse or os.environ.get(ENV_WAREHOUSE)
    packages = args.packages or os.environ.get(ENV_PACKAGES)
    return SparkConfig(
        catalog=catalog,
        warehouse=warehouse,
        packages=packages,
        catalog_type=args.catalog_type,
        catalog_class=args.catalog_class,
        extensions=args.extensions,
        use_existing=args.use_existing_spark,
        extra_confs=parse_spark_conf(args.spark_conf),
        stop_session=not args.use_existing_spark,
    )


def get_spark(config: SparkConfig):
    from pyspark.sql import SparkSession

    if config.use_existing:
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.getOrCreate()
        return spark

    if not config.warehouse:
        raise SystemExit(
            f"--warehouse or ${ENV_WAREHOUSE} required when not using --use-existing-spark"
        )

    builder = SparkSession.builder.appName("VariantShredCoverage")
    if config.packages:
        builder = builder.config("spark.jars.packages", config.packages)
    builder = (
        builder.config("spark.sql.extensions", config.extensions)
        .config(f"spark.sql.catalog.{config.catalog}", config.catalog_class)
        .config(f"spark.sql.catalog.{config.catalog}.type", config.catalog_type)
        .config(f"spark.sql.catalog.{config.catalog}.warehouse", config.warehouse)
        .config("spark.sql.defaultCatalog", config.catalog)
    )
    for key, val in config.extra_confs.items():
        builder = builder.config(key, val)
    return builder.getOrCreate()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Variant Shred Coverage Report — audit Iceberg variant shredding",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Parquet files only (no Spark):
  python3 variant_shred_audit.py --parquet-dir /path/to/table/data --variant-col v

  # spark-submit (configure Spark/Iceberg externally):
  spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \\
    --conf spark.sql.catalog.dev=org.apache.iceberg.spark.SparkCatalog \\
    --conf spark.sql.catalog.dev.type=hadoop \\
    --conf spark.sql.catalog.dev.warehouse=file:///path/to/warehouse \\
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \\
    variant_shred_audit.py --use-existing-spark --table dev.demo.my_table --variant-col v
        """,
    )
    target = p.add_argument_group("target (pick one)")
    target.add_argument("--no-shred-dir", help="Parquet data dir for table WITHOUT shredding")
    target.add_argument("--with-shred-dir", help="Parquet data dir for table WITH shredding")
    target.add_argument("--parquet-dir", help="Single table Parquet data directory")
    target.add_argument("--table", help="Iceberg table e.g. catalog.db.table")

    p.add_argument("--variant-col", default="v", help="Variant column name (default: v)")
    p.add_argument(
        "--max-files",
        type=int,
        default=0,
        help="Max Parquet data files to inspect; 0 = all files (default: all)",
    )
    p.add_argument(
        "--scan-rows",
        type=int,
        default=0,
        help="Sample N rows for B2 unshredded path frequency (optional)",
    )
    p.add_argument("--json-file", help="JSON/JSON.GZ source for B2 (with --scan-rows, no Spark)")

    spark_grp = p.add_argument_group("Spark / Iceberg (for --table mode)")
    spark_grp.add_argument(
        "--use-existing-spark",
        action="store_true",
        help="Use Spark session from spark-submit (do not create/configure Spark)",
    )
    spark_grp.add_argument(
        "--warehouse",
        default=None,
        help=f"Iceberg warehouse URI (or env {ENV_WAREHOUSE})",
    )
    spark_grp.add_argument(
        "--catalog",
        default=None,
        help=f"Spark catalog name (default: dev, or env {ENV_CATALOG})",
    )
    spark_grp.add_argument(
        "--catalog-type",
        default="hadoop",
        help="Iceberg catalog type (default: hadoop)",
    )
    spark_grp.add_argument(
        "--catalog-class",
        default=DEFAULT_CATALOG_CLASS,
        help="Spark catalog implementation class",
    )
    spark_grp.add_argument(
        "--extensions",
        default=DEFAULT_ICEBERG_EXTENSIONS,
        help="spark.sql.extensions value for Iceberg",
    )
    spark_grp.add_argument(
        "--packages",
        default=None,
        help=f"Maven packages for Iceberg runtime (or env {ENV_PACKAGES})",
    )
    spark_grp.add_argument(
        "--spark-conf",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Extra Spark config (repeatable). Ignored with --use-existing-spark",
    )
    return p.parse_args()


def import_pyarrow():
    try:
        import pyarrow.parquet as pq

        return pq
    except ImportError as err:
        raise SystemExit("pyarrow required: pip install pyarrow") from err


def logical_path_from_parquet_path(parts: list[str], variant_col: str) -> str | None:
    """Map v.typed_value.actor.typed_value.city.typed_value → actor.city"""
    if not parts or parts[0] != variant_col:
        return None
    if "typed_value" not in parts:
        return None

    idx = 1
    if idx >= len(parts) or parts[idx] != "typed_value":
        return None
    idx += 1

    out: list[str] = []
    while idx < len(parts):
        name = parts[idx]
        if name == "typed_value":
            idx += 1
            continue
        if name == "value":
            break
        if name in ("list", "element"):
            if out:
                out[-1] = out[-1] + "[]"
            else:
                out.append("[]")
            idx += 1
            continue
        out.append(name)
        idx += 1
        if idx < len(parts) and parts[idx] == "typed_value":
            idx += 1

    return ".".join(out) if out else None


def parquet_column_paths(pq, file_path: str) -> list[str]:
    pf = pq.ParquetFile(file_path)
    return [pf.metadata.schema.column(i).path for i in range(pf.metadata.num_columns)]


def collect_shredded_paths(column_paths: list[str], variant_col: str) -> set[str]:
    paths: set[str] = set()
    for path in column_paths:
        if not path.startswith(variant_col + "."):
            continue
        parts = path.split(".")
        if parts[-1] != "typed_value":
            continue
        lp = logical_path_from_parquet_path(parts, variant_col)
        if lp:
            paths.add(lp)
    return paths


def collect_value_stats(pq, file_path: str, variant_col: str) -> dict[str, ValueColStats]:
    pf = pq.ParquetFile(file_path)
    meta = pf.metadata
    stats: dict[str, ValueColStats] = {}

    value_paths = [
        p
        for p in parquet_column_paths(pq, file_path)
        if p.startswith(variant_col + ".") and p.endswith(".value") and not p.endswith(".metadata")
    ]

    for value_path in value_paths:
        stats[value_path] = ValueColStats(path=value_path)

    for rg_idx in range(meta.num_row_groups):
        rg = meta.row_group(rg_idx)
        rows = rg.num_rows
        for col_idx in range(rg.num_columns):
            col = rg.column(col_idx)
            path = col.path_in_schema
            if path not in stats:
                continue
            st = col.statistics
            if st is None or not st.has_null_count:
                stats[path].non_null_rows += rows
            else:
                stats[path].null_rows += st.null_count
                stats[path].non_null_rows += rows - st.null_count

    # unshredded files: v.value always has binary, stats may be missing
    root = f"{variant_col}.value"
    if root in stats and stats[root].total == 0:
        stats[root].non_null_rows = meta.num_rows

    return stats


def audit_file(pq, file_path: str, variant_col: str, record_count: int | None) -> FileAudit:
    column_paths = parquet_column_paths(pq, file_path)
    shredded = collect_shredded_paths(column_paths, variant_col)
    value_stats = collect_value_stats(pq, file_path, variant_col)

    pf = pq.ParquetFile(file_path)
    total = record_count or pf.metadata.num_rows

    return FileAudit(
        path=file_path,
        record_count=total,
        parquet_columns=len(column_paths),
        shredded_paths=shredded,
        value_stats=value_stats,
    )


def discover_parquet_files(parquet_dir: str, max_files: int) -> tuple[list[tuple[str, int | None]], int]:
    root = Path(parquet_dir)
    if not root.exists():
        raise SystemExit(f"Directory not found: {root}")
    all_files = sorted(p for p in root.rglob("*.parquet") if not p.name.startswith("."))
    if max_files <= 0:
        selected = all_files
    else:
        selected = all_files[:max_files]
    return [(str(f), None) for f in selected], len(all_files)


def discover_via_spark(
    table: str, spark_config: SparkConfig, max_files: int
) -> tuple[list[tuple[str, int | None]], int]:
    spark = get_spark(spark_config)
    spark.sparkContext.setLogLevel("ERROR")

    limit_sql = "" if max_files <= 0 else f" LIMIT {max_files}"
    rows = spark.sql(
        f"SELECT file_path, record_count FROM {table}.files ORDER BY file_path{limit_sql}"
    ).collect()
    total_files = spark.sql(f"SELECT count(*) AS c FROM {table}.files").collect()[0].c
    if spark_config.stop_session:
        spark.stop()
    return [(r.file_path, int(r.record_count)) for r in rows], int(total_files)


def audit_table(pq, label: str, location: str, variant_col: str, max_files: int) -> TableAudit:
    file_list, total_available = discover_parquet_files(location, max_files)
    files = [audit_file(pq, path, variant_col, rc) for path, rc in file_list]
    return TableAudit(
        label=label,
        location=location,
        files=files,
        total_files_available=total_available,
    )


def short_name(path: str, width: int = 44) -> str:
    name = Path(path).name
    if len(name) > width:
        return "..." + name[-(width - 3) :]
    return name


def normalize_json_path(path: str) -> str:
    """Normalize [] to [][] for comparison with shredded path names."""
    return re.sub(r"\[\]", "[][]", path)


def flatten_json_paths(obj, prefix: str = "") -> set[str]:
    paths: set[str] = set()
    if isinstance(obj, dict):
        for key, val in obj.items():
            p = f"{prefix}.{key}" if prefix else key
            paths.add(normalize_json_path(p))
            paths.update(flatten_json_paths(val, p))
    elif isinstance(obj, list):
        p = prefix + "[]" if prefix else "[]"
        for item in obj[:5]:
            paths.update(flatten_json_paths(item, p))
    return paths


def scan_json_file(json_path: str, max_rows: int) -> tuple[Counter[str], int]:
    path_counts: Counter[str] = Counter()
    root = Path(json_path)
    opener = gzip.open if root.suffix == ".gz" or str(root).endswith(".json.gz") else open
    mode = "rt" if opener is gzip.open else "r"
    rows = 0
    with opener(root, mode, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            for p in flatten_json_paths(row):
                path_counts[p] += 1
            rows += 1
            if rows >= max_rows:
                break
    return path_counts, rows


def scan_table_paths(
    table: str, variant_col: str, spark_config: SparkConfig, max_rows: int
) -> tuple[Counter[str], int]:
    spark = get_spark(spark_config)
    spark.sparkContext.setLogLevel("ERROR")

    rows = spark.sql(
        f"SELECT to_json({variant_col}) AS j FROM {table} LIMIT {max_rows}"
    ).collect()
    if spark_config.stop_session:
        spark.stop()

    counts: Counter[str] = Counter()
    for row in rows:
        for p in flatten_json_paths(json.loads(row.j)):
            counts[p] += 1
    return counts, len(rows)


def path_is_covered_by_shredded(json_path: str, shredded: set[str]) -> bool:
    if json_path in shredded:
        return True
    for s in shredded:
        if json_path.startswith(s + ".") or json_path.startswith(s + "[]"):
            return True
        if s.startswith(json_path + ".") or s.startswith(json_path + "[]"):
            return True
    return False


def field_value_stats(table: TableAudit, variant_col: str, logical_path: str) -> ValueColStats:
    agg = ValueColStats(path=logical_path)
    for f in table.files:
        for parquet_path, st in f.value_stats.items():
            if ".list." in parquet_path or not parquet_path.endswith(".value"):
                continue
            lp = logical_path_from_parquet_path(parquet_path.split("."), variant_col)
            if lp == logical_path:
                agg.null_rows += st.null_rows
                agg.non_null_rows += st.non_null_rows
    return agg


def classify_query_benefit(
    spec_status: str, file_coverage_pct: float, row_full_pct: float
) -> str:
    """Per spec: typed_value stats enable skipping only when field.value is always null."""
    if spec_status == "NOT_SHREDDED":
        return "NONE"
    if spec_status == "FULL" and file_coverage_pct >= 100.0:
        return "HIGH"
    if spec_status == "FULL" and file_coverage_pct < 100.0:
        return "MEDIUM"
    if spec_status == "PARTIAL" and row_full_pct >= 90.0:
        return "MEDIUM"
    if spec_status == "PARTIAL":
        return "LOW"
    return "NONE"


def build_path_qualities(table: TableAudit, variant_col: str) -> list[PathQuality]:
    if not table.files:
        return []

    num_files = len(table.files)
    path_file_count: dict[str, int] = defaultdict(int)
    for f in table.files:
        for p in f.shredded_paths:
            path_file_count[p] += 1

    qualities: list[PathQuality] = []
    for path in sorted(table.all_shredded_paths):
        fc = path_file_count[path]
        file_cov = 100.0 * fc / num_files
        st = field_value_stats(table, variant_col, path)
        row_full = st.full_pct
        row_partial = st.partial_pct

        if st.total == 0 and "[]" in path:
            spec_status = "FULL"
        elif row_partial == 0.0:
            spec_status = "FULL"
        else:
            spec_status = "PARTIAL"

        qualities.append(
            PathQuality(
                path=path,
                spec_status=spec_status,
                file_coverage_pct=file_cov,
                row_full_pct=row_full,
                row_partial_pct=row_partial,
                query_benefit=classify_query_benefit(spec_status, file_cov, row_full),
            )
        )
    return qualities


def build_unshredded_paths(
    data_counts: Counter[str], shredded: set[str], total_rows: int
) -> list[tuple[str, int, float]]:
    rows: list[tuple[str, int, float]] = []
    for path, count in data_counts.items():
        if path_is_covered_by_shredded(path, shredded):
            continue
        pct = 100.0 * count / total_rows if total_rows else 0.0
        rows.append((path, count, pct))
    rows.sort(key=lambda x: (-x[1], x[0]))
    return rows


def aggregate_nested_partials(table: TableAudit, variant_col: str) -> list[tuple[str, int, float]]:
    """Paths where nested .value still has binary (partial shred at that object)."""
    agg: dict[str, ValueColStats] = {}
    for f in table.files:
        for path, st in f.value_stats.items():
            if path == f"{variant_col}.value":
                continue
            # Parquet null_count on repeated (list) columns counts elements, not rows
            if ".list." in path:
                continue
            if st.non_null_rows == 0:
                continue
            if path not in agg:
                agg[path] = ValueColStats(path=path)
            agg[path].null_rows += st.null_rows
            agg[path].non_null_rows += st.non_null_rows

    rows: list[tuple[str, int, float]] = []
    for path, st in agg.items():
        if st.non_null_rows <= 0:
            continue
        lp = logical_path_from_parquet_path(path.split("."), variant_col)
        label = lp if lp else path
        rows.append((label, st.non_null_rows, st.partial_pct))
    rows.sort(key=lambda x: (-x[1], x[0]))
    return rows


def print_scan_scope(table: TableAudit, variant_col: str, data_rows_scanned: int) -> None:
    inspected = len(table.files)
    available = table.total_files_available or inspected
    complete = inspected >= available
    print("=" * 110)
    print("VARIANT SHRED COVERAGE REPORT")
    print("=" * 110)
    print(f"  Location:      {table.location}")
    print(f"  Variant col:   {variant_col}")
    scope = "COMPLETE" if complete else f"PARTIAL ({inspected}/{available} files)"
    print(f"  Parquet scan:  {scope} — {inspected} file(s), {table.total_rows:,} rows")
    print("  Method:        Parquet metadata + column null statistics (not row sampling)")
    if data_rows_scanned > 0:
        print(f"  B2 add-on:     SAMPLED — {data_rows_scanned:,} rows (--scan-rows)")
    else:
        print("  B2 add-on:     off (unshredded path list requires --scan-rows)")
    print()


def print_query_benefit_report(
    table: TableAudit,
    variant_col: str,
    qualities: list[PathQuality],
    unshredded: list[tuple[str, int, float]] | None,
    data_rows_scanned: int,
    include_scope: bool = True,
) -> None:
    total = table.total_rows
    num_files = len(table.files)
    full_paths = [q for q in qualities if q.spec_status == "FULL"]
    partial_paths = [q for q in qualities if q.spec_status == "PARTIAL"]
    high = [q for q in qualities if q.query_benefit == "HIGH"]
    medium = [q for q in qualities if q.query_benefit == "MEDIUM"]
    low = [q for q in qualities if q.query_benefit == "LOW"]

    if include_scope:
        print_scan_scope(table, variant_col, data_rows_scanned)

    print("=" * 110)
    print("A — SPEC SUMMARY (Parquet VariantShredding.md)")
    print("=" * 110)
    print("  FULLY SHREDDED field  → field.value is NULL, field.typed_value holds the value")
    print("                         → enables column projection + stats-based data skipping")
    print("  PARTIALLY SHREDDED    → field.value has binary on some rows (type mismatch / fallback)")
    print("                         OR parent object.value has binary (un-inferred subfields)")
    print("  NOT SHREDDED          → no typed_value column; value lives only in variant binary")
    print()
    print(f"  Files inspected:           {num_files}")
    print(f"  Rows inspected (Parquet):  {total:,}")
    print(f"  Shredded paths in schema:  {len(qualities)}")
    print(f"    FULL (spec):             {len(full_paths)}  ({100*len(full_paths)/len(qualities):.0f}% of shredded)" if qualities else "    FULL (spec):             0")
    print(f"    PARTIAL (spec):          {len(partial_paths)}  ({100*len(partial_paths)/len(qualities):.0f}% of shredded)" if qualities else "    PARTIAL (spec):          0")
    if total:
        print(
            f"  Root variant FULL:         {100*table.root_full_rows/total:.1f}% rows "
            f"({variant_col}.value NULL — object fields in typed_value)"
        )

    print()
    print("=" * 110)
    print("B — SHREDDED PATHS: FULL vs PARTIAL (all files, spec-aligned)")
    print("=" * 110)
    print(f"{'JSON PATH':<45} {'FILES':>6} {'ROW FULL%':>10} {'SPEC':>10} {'QUERY':>8}")
    print("-" * 110)
    for q in qualities:
        fc = int(q.file_coverage_pct * num_files / 100)
        print(
            f"{q.path:<45} {fc:>4}/{num_files} {q.row_full_pct:>9.1f}% "
            f"{q.spec_status:>10} {q.query_benefit:>8}"
        )

    if unshredded is not None and data_rows_scanned > 0:
        print()
        print("=" * 110)
        print(f"B2 — NOT SHREDDED PATHS (no typed_value column; sample {data_rows_scanned:,} rows)")
        print("=" * 110)
        print(f"{'JSON PATH':<55} {'ROWS':>8} {'FREQ %':>8} {'QUERY':>8}")
        print("-" * 110)
        for path, count, pct in unshredded[:30]:
            print(f"{path:<55} {count:>8,} {pct:>7.1f}% {'NONE':>8}")
        if len(unshredded) > 30:
            print(f"  ... and {len(unshredded) - 30} more unshredded paths")
        total_known = len(unshredded) + len(qualities)
        if total_known:
            print()
            print(
                f"  Unshredded distinct paths: {len(unshredded)} "
                f"({100*len(unshredded)/total_known:.0f}% of distinct paths in sample)"
            )
    elif data_rows_scanned == 0:
        print()
        print("  (B2 skipped — pass --table + --scan-rows N or --json-file for unshredded path frequency)")

    print()
    print("=" * 110)
    print("C — QUERY BENEFIT SUMMARY (which filters/projections win)")
    print("=" * 110)
    print(f"  HIGH benefit ({len(high)} paths) — use these in WHERE / SELECT (typed_value + stats skipping):")
    for q in high[:15]:
        print(f"    • variant_get({variant_col}, '$.{q.path}', ...)  [{q.row_full_pct:.0f}% row FULL, {q.file_coverage_pct:.0f}% files]")
    if len(high) > 15:
        print(f"    ... +{len(high)-15} more")

    if medium:
        print(f"\n  MEDIUM benefit ({len(medium)} paths) — column exists, partial fallback or not all files:")
        for q in medium[:8]:
            print(f"    • $.{q.path}  (FULL {q.row_full_pct:.0f}%, files {q.file_coverage_pct:.0f}%)")

    if partial_paths:
        print(f"\n  LOW benefit ({len(low)} paths) — shredded column but heavy binary fallback:")
        for q in low:
            print(f"    • $.{q.path}  ({q.row_partial_pct:.1f}% rows still in field.value binary)")

    if unshredded:
        top = unshredded[:5]
        print(f"\n  NO benefit ({len(unshredded)}+ paths) — NOT SHREDDED, must parse variant binary:")
        for path, count, pct in top:
            print(f"    • $.{path}  ({pct:.1f}% of sampled rows)")

    print()
    print("  Rule of thumb:")
    print("    HIGH   → filter/project on this path; Iceberg/Parquet can skip row groups")
    print("    NONE   → variant_get still works logically, but reads full variant binary")
    print("    Prefer HIGH paths for dashboard filters; avoid NONE paths in hot queries")


def is_unshredded_table(table: TableAudit, variant_col: str) -> bool:
    if table.all_shredded_paths:
        return False
    if not table.files:
        return False
    for f in table.files:
        if f.shredded_paths:
            return False
        root = f.root_value
        if root and root.partial_pct >= 99.9:
            continue
        if f.mode == "UNSHREDDED":
            continue
        return False
    return True


def print_unshredded_report(
    table: TableAudit,
    variant_col: str,
    data_rows_scanned: int,
) -> None:
    total = table.total_rows
    cols = table.files[0].parquet_columns if table.files else 0

    print_scan_scope(table, variant_col, data_rows_scanned)

    print("=" * 110)
    print("RESULT: NOT SHREDDED")
    print("=" * 110)
    print(f"  No typed_value columns found for variant column '{variant_col}'.")
    print(f"  Parquet layout: id, {variant_col}.metadata, {variant_col}.value  ({cols} columns)")
    if total:
        print(f"  All {total:,} rows store the full JSON payload in {variant_col}.value binary.")
    print()
    print("  Likely cause:")
    print("    write.parquet.shred-variants = false  (or shredding not enabled for this write)")
    print()
    print("  Query impact:")
    print("    variant_get() works logically, but every access reads the full variant binary")
    print("    No column projection or Parquet stats skipping on JSON sub-fields")
    print("    Query benefit: NONE for all paths")
    print()
    print("  To enable shredding on new writes:")
    print("    ALTER TABLE <table> SET TBLPROPERTIES (")
    print("      'format-version' = '3',")
    print("      'write.parquet.shred-variants' = 'true'")
    print("    )")
    print()
    if table.files:
        print("PER FILE")
        print("-" * 110)
        print(f"{'FILE':<46} {'ROWS':>10} {'COLS':>6} {'MODE':>12}")
        print("-" * 110)
        for f in table.files:
            print(
                f"{short_name(f.path):<46} {f.record_count:>10,} {f.parquet_columns:>6} "
                f"{'UNSHREDDED':>12}"
            )


def print_single_report(
    table: TableAudit,
    variant_col: str,
    data_counts: Counter[str] | None = None,
    data_rows_scanned: int = 0,
) -> None:
    if not table.files:
        print(f"No Parquet files in {table.location}")
        return

    if is_unshredded_table(table, variant_col):
        print_unshredded_report(table, variant_col, data_rows_scanned)
        return

    print_scan_scope(table, variant_col, data_rows_scanned)

    path_file_count: dict[str, int] = defaultdict(int)
    for f in table.files:
        for p in f.shredded_paths:
            path_file_count[p] += 1

    all_paths = table.all_shredded_paths
    total = table.total_rows

    print(f"  Parquet columns:  {sum(f.parquet_columns for f in table.files) // len(table.files)}")
    print(f"  Shredded paths:   {len(all_paths)}")
    if total:
        print(
            f"  Root FULL rows:   {table.root_full_rows:,} "
            f"({100 * table.root_full_rows / total:.1f}%)  ← {variant_col}.value is NULL"
        )
        print(
            f"  Root PARTIAL rows: {table.root_partial_rows:,} "
            f"({100 * table.root_partial_rows / total:.1f}%)  ← {variant_col}.value has binary"
        )

    print()
    print("SHREDDED PATHS (extracted to typed_value columns)")
    print("-" * 100)
    print(f"{'JSON PATH':<45} {'FILES':>8} {'COVERAGE':>10} {'STATUS':>12}")
    print("-" * 100)
    if not all_paths:
        print(f"{'(none)':<45} {'—':>8} {'—':>10} {'NOT SHREDDED':>12}")
    else:
        for p in sorted(all_paths):
            fc = path_file_count[p]
            cov = f"{100 * fc / len(table.files):.0f}%"
            print(f"{p:<45} {fc:>8} {cov:>10} {'SHREDDED':>12}")

    nested = aggregate_nested_partials(table, variant_col)
    if nested:
        print()
        print("NESTED PARTIAL SHRED (object still has leftover binary in .value)")
        print("-" * 100)
        print(f"{'JSON PATH / OBJECT':<45} {'PARTIAL ROWS':>14} {'PARTIAL %':>10}")
        print("-" * 100)
        for label, partial_rows, pct in nested[:40]:
            print(f"{label:<45} {partial_rows:>14,} {pct:>9.1f}%")
        if len(nested) > 40:
            print(f"  ... and {len(nested) - 40} more")

    print()
    print("PER FILE")
    print("-" * 100)
    print(f"{'FILE':<46} {'ROWS':>8} {'COLS':>6} {'ROOT FULL%':>11} {'#PATHS':>8} {'MODE':>14}")
    print("-" * 100)
    for f in table.files:
        root = f.root_value
        full = root.full_pct if root else 0.0
        print(
            f"{short_name(f.path):<46} {f.record_count:>8,} {f.parquet_columns:>6} "
            f"{full:>10.1f}% {len(f.shredded_paths):>8} {f.mode:>14}"
        )

    qualities = build_path_qualities(table, variant_col)
    unshredded = None
    if data_counts and data_rows_scanned > 0:
        unshredded = build_unshredded_paths(data_counts, table.all_shredded_paths, data_rows_scanned)

    print_query_benefit_report(
        table, variant_col, qualities, unshredded, data_rows_scanned, include_scope=False
    )


def print_compare_report(no_shred: TableAudit, with_shred: TableAudit, variant_col: str) -> None:
    print("=" * 110)
    print("VARIANT SHRED COVERAGE REPORT — COMPARE")
    print("=" * 110)
    print(f"Table 1 (NO shredding):   {no_shred.location}")
    print(f"Table 2 (WITH shredding): {with_shred.location}")
    print(f"Variant column:           {variant_col}")
    print()

    n_rows = no_shred.total_rows
    w_rows = with_shred.total_rows
    n_paths = len(no_shred.all_shredded_paths)
    w_paths = len(with_shred.all_shredded_paths)
    n_cols = no_shred.files[0].parquet_columns if no_shred.files else 0
    w_cols = with_shred.files[0].parquet_columns if with_shred.files else 0

    print(f"{'METRIC':<35} {'NO SHREDDING':>22} {'WITH SHREDDING':>22}")
    print("-" * 110)
    print(f"{'Rows':<35} {n_rows:>22,} {w_rows:>22,}")
    print(f"{'Parquet columns (per file)':<35} {n_cols:>22} {w_cols:>22}")
    print(f"{'Shredded JSON paths':<35} {n_paths:>22} {w_paths:>22}")
    if n_rows:
        print(
            f"{'Root FULL shred (value=NULL)':<35} "
            f"{100 * no_shred.root_full_rows / n_rows:>21.1f}% "
            f"{100 * with_shred.root_full_rows / w_rows:>21.1f}%"
        )
        print(
            f"{'Root PARTIAL (value=binary)':<35} "
            f"{100 * no_shred.root_partial_rows / n_rows:>21.1f}% "
            f"{100 * with_shred.root_partial_rows / w_rows:>21.1f}%"
        )

    print()
    print("=" * 110)
    print("TABLE 1 — NO SHREDDING (all JSON stays in variant binary)")
    print("=" * 110)
    print("  Status: NOT SHREDDED — only v.metadata + v.value columns, no typed_value")
    print(f"  Parquet layout: id, {variant_col}.metadata, {variant_col}.value")
    if n_rows and no_shred.root_partial_rows == n_rows:
        print(f"  All {n_rows:,} rows store full JSON in {variant_col}.value binary")

    print()
    print("=" * 110)
    print("TABLE 2 — WITH SHREDDING (typed_value columns + optional leftover binary)")
    print("=" * 110)

    only_in_with = with_shred.all_shredded_paths - no_shred.all_shredded_paths
    path_file_count: dict[str, int] = defaultdict(int)
    for f in with_shred.files:
        for p in f.shredded_paths:
            path_file_count[p] += 1

    print(f"{'JSON PATH':<45} {'IN PARQUET?':>14} {'FILE COV':>10} {'STORAGE':>18}")
    print("-" * 110)
    for p in sorted(only_in_with):
        fc = path_file_count[p]
        cov = f"{100 * fc / len(with_shred.files):.0f}%" if with_shred.files else "—"
        print(f"{p:<45} {'YES':>14} {cov:>10} {'typed_value col':>18}")

    nested = aggregate_nested_partials(with_shred, variant_col)
    if nested:
        print()
        print("NESTED PARTIAL — fields/objects still using .value binary (not fully extracted)")
        print("-" * 110)
        print(f"{'JSON PATH / OBJECT':<45} {'PARTIAL ROWS':>14} {'PARTIAL %':>10}")
        print("-" * 110)
        for label, partial_rows, pct in nested:
            print(f"{label:<45} {partial_rows:>14,} {pct:>9.1f}%")

    print()
    print("=" * 110)
    print("TL;DR")
    print("=" * 110)
    if n_paths == 0 and w_paths > 0:
        print(f"  • NO-shred table: 0 typed_value columns → entire variant in binary")
        print(f"  • WITH-shred table: {w_paths} JSON paths extracted to Parquet columns")
    if w_rows:
        root_full = 100 * with_shred.root_full_rows / w_rows
        root_part = 100 * with_shred.root_partial_rows / w_rows
        print(
            f"  • Root-level: {root_full:.1f}% rows FULLY shredded, "
            f"{root_part:.1f}% rows still have top-level binary"
        )
    if nested:
        heavy = [x for x in nested if x[2] >= 50]
        if heavy:
            names = ", ".join(x[0] for x in heavy[:5])
            print(f"  • Heaviest nested partial objects: {names}")
    print()
    print("  FULL row    → that level's .value column is NULL (fields in typed_value)")
    print("  PARTIAL row → .value column has binary (some fields not extracted)")
    print("  SHREDDED path → appears as typed_value column in Parquet schema")
    print("  NOT SHREDDED  → no typed_value column; data only in variant binary")


def main() -> None:
    args = parse_args()
    pq = import_pyarrow()
    spark_config = build_spark_config(args)

    data_counts: Counter[str] | None = None
    data_rows_scanned = 0
    if args.json_file and args.scan_rows:
        data_counts, data_rows_scanned = scan_json_file(args.json_file, args.scan_rows)

    if args.no_shred_dir and args.with_shred_dir:
        no_table = audit_table(pq, "NO SHREDDING", args.no_shred_dir, args.variant_col, args.max_files)
        with_table = audit_table(
            pq, "WITH SHREDDING", args.with_shred_dir, args.variant_col, args.max_files
        )
        print_compare_report(no_table, with_table, args.variant_col)
        qualities = build_path_qualities(with_table, args.variant_col)
        unshredded = None
        if data_counts and data_rows_scanned:
            unshredded = build_unshredded_paths(
                data_counts, with_table.all_shredded_paths, data_rows_scanned
            )
        print_query_benefit_report(
            with_table, args.variant_col, qualities, unshredded, data_rows_scanned
        )
        return

    if args.table:
        if args.scan_rows and not data_counts:
            data_counts, data_rows_scanned = scan_table_paths(
                args.table, args.variant_col, spark_config, args.scan_rows
            )
        file_list, total_available = discover_via_spark(
            args.table, spark_config, args.max_files
        )
        files = [audit_file(pq, path, args.variant_col, rc) for path, rc in file_list]
        table = TableAudit(
            label=args.table,
            location=args.table,
            files=files,
            total_files_available=total_available,
        )
        print_single_report(table, args.variant_col, data_counts, data_rows_scanned)
        return

    if args.parquet_dir:
        table = audit_table(pq, args.parquet_dir, args.parquet_dir, args.variant_col, args.max_files)
        print_single_report(table, args.variant_col, data_counts, data_rows_scanned)
        return

    sys.exit(
        "Provide --parquet-dir, --table, or both --no-shred-dir and --with-shred-dir.\n"
        "Run with -h for examples (spark-submit and local modes)."
    )


if __name__ == "__main__":
    main()
