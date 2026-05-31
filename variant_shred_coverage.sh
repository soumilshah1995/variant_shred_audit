#!/usr/bin/env bash
# Variant Shred Coverage Report — spark-submit wrapper
#
# Configure via environment variables (all optional):
#   ICEBERG_PACKAGES   Maven coords for Iceberg Spark runtime
#   ICEBERG_WAREHOUSE  Warehouse URI, e.g. file:///path/to/warehouse
#   ICEBERG_CATALOG    Catalog name (default: dev)
#   SPARK_MASTER       Spark master (default: local[*])
#
# Examples:
#   ICEBERG_WAREHOUSE=file:///data/warehouse \
#   ICEBERG_PACKAGES=org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
#     ./variant_shred_coverage.sh --table dev.demo.my_table --variant-col v
#
#   ./variant_shred_coverage.sh --parquet-dir /path/to/table/data --variant-col v
#
# Parquet-dir mode uses python3 directly (no Spark). Table mode uses spark-submit.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUDIT_PY="${SCRIPT_DIR}/variant_shred_audit.py"

CATALOG="${ICEBERG_CATALOG:-dev}"
WAREHOUSE="${ICEBERG_WAREHOUSE:-}"
PACKAGES="${ICEBERG_PACKAGES:-org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"

usage() {
  cat <<EOF
Usage: $(basename "$0") [spark-submit options] -- [audit options]

Runs Variant Shred Coverage Report.

Parquet mode (no Spark):
  $(basename "$0") --parquet-dir /path/to/table/data [--variant-col v]

Iceberg table mode (spark-submit):
  ICEBERG_WAREHOUSE=file:///path/to/warehouse \\
  ICEBERG_PACKAGES=org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \\
    $(basename "$0") --table catalog.db.table [--variant-col v]

Environment:
  ICEBERG_WAREHOUSE   Warehouse URI (required for --table)
  ICEBERG_CATALOG     Catalog name (default: dev)
  ICEBERG_PACKAGES    Iceberg Spark runtime Maven package
  SPARK_MASTER        Spark master (default: local[*])

Pass-through: any spark-submit flags before "--" are forwarded.
Audit flags after "--" or directly if no "--" present.

Examples:
  $(basename "$0") --parquet-dir ./warehouse/demo/github_with_shredding/data
  ICEBERG_WAREHOUSE=file:///tmp/wh $(basename "$0") --table dev.demo.events --scan-rows 5000
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

# Split spark-submit args from audit args at "--"
SPARK_ARGS=()
AUDIT_ARGS=()
SEEN_SEP=false
for arg in "$@"; do
  if [[ "$arg" == "--" && "$SEEN_SEP" == false ]]; then
    SEEN_SEP=true
    continue
  fi
  if [[ "$SEEN_SEP" == true ]]; then
    AUDIT_ARGS+=("$arg")
  else
    SPARK_ARGS+=("$arg")
  fi
done

if [[ ${#AUDIT_ARGS[@]} -eq 0 ]]; then
  AUDIT_ARGS=("${SPARK_ARGS[@]}")
  SPARK_ARGS=()
fi

USES_TABLE=false
USES_PARQUET=false
for arg in "${AUDIT_ARGS[@]}"; do
  [[ "$arg" == "--table" ]] && USES_TABLE=true
  [[ "$arg" == "--parquet-dir" ]] && USES_PARQUET=true
done

if $USES_PARQUET && ! $USES_TABLE; then
  exec python3 "$AUDIT_PY" "${AUDIT_ARGS[@]}"
fi

if $USES_TABLE; then
  if [[ -z "$WAREHOUSE" ]]; then
    echo "Error: ICEBERG_WAREHOUSE must be set for --table mode" >&2
    echo "  export ICEBERG_WAREHOUSE=file:///path/to/warehouse" >&2
    exit 1
  fi

  exec spark-submit \
    --master "$SPARK_MASTER" \
    --packages "$PACKAGES" \
    --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
    --conf "spark.sql.catalog.${CATALOG}=org.apache.iceberg.spark.SparkCatalog" \
    --conf "spark.sql.catalog.${CATALOG}.type=hadoop" \
    --conf "spark.sql.catalog.${CATALOG}.warehouse=${WAREHOUSE}" \
    --conf "spark.sql.defaultCatalog=${CATALOG}" \
    "${SPARK_ARGS[@]}" \
    "$AUDIT_PY" \
    --use-existing-spark \
    --catalog "$CATALOG" \
    --warehouse "$WAREHOUSE" \
    "${AUDIT_ARGS[@]}"
fi

echo "Error: provide --parquet-dir or --table" >&2
usage
exit 1
