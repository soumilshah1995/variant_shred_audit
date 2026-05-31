# Variant Shred Coverage Report

Audit **Iceberg v3 VARIANT** shredding from Parquet files. Shows which JSON paths are **FULLY** shredded, **PARTIALLY** shredded, or **NOT** shredded — and which `variant_get` paths benefit query engines most.

Based on the [Parquet Variant Shredding spec](https://github.com/apache/parquet-format/blob/master/VariantShredding.md).

## Tools in this repo

| File | Purpose |
|------|---------|
| `variant_shred_audit.py` | **Coverage report** — FULL / PARTIAL / query benefit (HIGH/MEDIUM/LOW/NONE) |

## Requirements

```bash
pip install pyarrow
```

For `--table` mode: **PySpark 4.x**, **Java 17+**, Iceberg 1.11+ Spark runtime.

## Quick start — Full report (recommended)

Use `--full` for all sections (A/B/C/D) including no-benefit path sampling:

```bash
python3 variant_shred_audit.py \
  --parquet-dir /path/to/warehouse/demo/my_table/data \
  --variant-col v \
  --full
```

`--full` auto-discovers a JSON source for Section D via `VARIANT_SHRED_JSON_FILE` or `github_archive.json.gz` near the table/warehouse.

## Quick start — Parquet only (sections A/B/C)

Point at your Iceberg table `data/` directory:

```bash
python3 variant_shred_audit.py \
  --parquet-dir /path/to/warehouse/demo/my_table/data \
  --variant-col v
```

**Not shredded example output:**

```
RESULT: NOT SHREDDED
  Parquet layout: id, v.metadata, v.value  (3 columns)
  Query benefit: NONE for all paths
```

**Shredded example output:**

```
  Parquet columns:  80
  Shredded paths:   35
  Root FULL rows:   100.0%

  HIGH benefit paths → use in variant_get() filters:
    • variant_get(v, '$.type', ...)
    • variant_get(v, '$.actor.login', ...)
```

## Iceberg table mode (spark-submit)

```bash
export ICEBERG_WAREHOUSE=file:///path/to/warehouse
export ICEBERG_PACKAGES=org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0
export ICEBERG_CATALOG=dev

spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
  --conf spark.sql.catalog.dev=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.dev.type=hadoop \
  --conf spark.sql.catalog.dev.warehouse=file:///path/to/warehouse \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  variant_shred_audit.py \
  --use-existing-spark \
  --table dev.demo.my_table \
  --variant-col v
```

## Compare two tables

```bash
python3 variant_shred_audit.py \
  --no-shred-dir /path/to/github_no_shredding/data \
  --with-shred-dir /path/to/github_with_shredding/data \
  --variant-col v
```

## Report sections

| Section | What it tells you |
|---------|-------------------|
| **A** | Spec summary — FULL vs PARTIAL vs NOT SHREDDED counts |
| **B** | Per-path ROW FULL %, SPEC status, QUERY benefit |
| **B2** | Sampled list of unshredded paths (`--full` or `--scan-rows`) |
| **C** | Paths that **benefit** from shredding (HIGH / MEDIUM / LOW) |
| **D** | Paths with **no benefit** (not shredded — full binary parse) |

## Cursor skill

Install for Agent chat: copy `.cursor/skills/variant-shred-coverage/` to `~/.cursor/skills/`, or use it from this repo.

```
@variant-shred-coverage
Run report on /path/to/warehouse/demo/my_table/data
```

The skill always runs with `--full`.

## Accuracy

Sections **A/B/C** scan **all Parquet files** (default) using column `null_count` statistics — **not row sampling**.

Section **B2** is optional and sampled (only with `--scan-rows`).

## Enable shredding on your table

```sql
ALTER TABLE my_table SET TBLPROPERTIES (
  'format-version' = '3',
  'write.parquet.shred-variants' = 'true',
  'write.parquet.variant-inference-buffer-size' = '10000'
);
```

## Environment variables

| Variable | Purpose |
|----------|---------|
| `ICEBERG_WAREHOUSE` | Warehouse URI |
| `ICEBERG_CATALOG` | Catalog name (default: `dev`) |
| `ICEBERG_PACKAGES` | Iceberg Spark runtime Maven coords |
| `VARIANT_SHRED_JSON_FILE` | JSON/JSON.GZ source for Section D sampling |
| `VARIANT_SHRED_AUDIT_SCRIPT` | Override path to `variant_shred_audit.py` |

## License

Apache License 2.0 — see [LICENSE](LICENSE).
