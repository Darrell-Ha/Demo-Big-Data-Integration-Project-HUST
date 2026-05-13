#!/bin/bash

export CMD_BIN='/home/airflow/.local/bin'
export PYTHONWARNINGS="ignore:Unverified HTTPS request"

# dbt run \
#     --select tag:silver \
#     --vars '{
#         "run_date": "2025-01-01"
#     }'

# =========================
# Usage
# =========================
usage() {
  echo "Usage:"
  echo ""
  echo "  Daily run:"
  echo "    ./run_dbt.sh daily --run-date YYYY-MM-DD [dbt args]"
  echo ""
  echo "  Backfill run:"
  echo "    ./run_dbt.sh backfill --start-date YYYY-MM-DD --end-date YYYY-MM-DD [dbt args]"
  echo ""
  echo "Examples:"
  echo "  ./run_dbt.sh daily --run-date 2026-01-22 --select tag:gold"
  echo "  ./run_dbt.sh backfill --start-date 2025-01-01 --end-date 2025-03-31 --full-refresh"
  exit 1
}

# =========================
# Mode
# =========================
MODE=${1:-}
shift || true

# =========================
# Vars
# =========================
RUN_DATE=""
START_DATE=""
END_DATE=""

# =========================
# dbt passthrough args
# =========================
DBT_ARGS=()

# =========================
# Parse args
# =========================
while [[ $# -gt 0 ]]; do
  case $1 in
    --run-date)
      RUN_DATE="$2"
      shift 2
      ;;
    --start-date)
      START_DATE="$2"
      shift 2
      ;;
    --end-date)
      END_DATE="$2"
      shift 2
      ;;
    *)
      # Everything else goes to dbt directly
      DBT_ARGS+=("$1")
      shift
      ;;
  esac
done

# =========================
# Validate + build vars
# =========================
if [[ "$MODE" == "daily" ]]; then
  if [[ -z "$RUN_DATE" ]]; then
    echo "❌ daily mode requires --run-date"
    usage
  fi

  DBT_VARS=$(cat <<EOF
{
  "run_date": "$RUN_DATE"
}
EOF
)

elif [[ "$MODE" == "backfill" ]]; then
  if [[ -z "$START_DATE" || -z "$END_DATE" ]]; then
    echo "❌ backfill mode requires --start-date and --end-date"
    usage
  fi

  DBT_VARS=$(cat <<EOF
{
  "start_date": "$START_DATE",
  "end_date": "$END_DATE"
}
EOF
)

else
  echo "❌ Mode must be 'daily' or 'backfill'"
  usage
fi

# =========================
# Echo context
# =========================
echo "🚀 dbt mode       : $MODE"
echo "📦 dbt vars       : $DBT_VARS"
echo "⚙️  dbt arguments : ${DBT_ARGS[*]:-(none)}"

# =========================
# dbt execution
# =========================
# ${CMD_BIN}/dbt deps

${CMD_BIN}/dbt run \
  --vars "$DBT_VARS" \
  "${DBT_ARGS[@]}"

echo "✅ dbt $MODE run completed"