#!/bin/bash

export CMD_BIN='/home/airflow/.local/bin'
export PYTHONWARNINGS="ignore:Unverified HTTPS request"

# =========================
# Usage
# =========================
usage() {
  cat <<EOF
Usage:
  ./dbt-test.sh [dbt args]

Description:
  Run dbt deps + dbt test with passthrough arguments.

Examples:
  ./dbt-test.sh --select tag:gold
  ./dbt-test.sh --select tag:silver
  ./dbt-test.sh --select model_name
EOF
  exit 1
}

# =========================
# Help
# =========================
if [[ "${1:-}" =~ ^(-h|--help)$ ]]; then
  usage
fi

# =========================
# dbt passthrough args
# =========================
DBT_ARGS=("$@")

# =========================
# Validate dbt binary
# =========================
if [[ ! -x "${CMD_BIN}/dbt" ]]; then
  echo "❌ dbt binary not found: ${CMD_BIN}/dbt"
  exit 1
fi

# =========================
# dbt deps
# =========================
# echo "📦 Running dbt deps..."
# "${CMD_BIN}/dbt" deps

# =========================
# dbt test
# =========================
echo "🧪 Running dbt test..."
"${CMD_BIN}/dbt" test "${DBT_ARGS[@]}"

echo "✅ dbt test completed successfully"