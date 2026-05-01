#!/usr/bin/env bash
set -Eeuo pipefail

export DEBIAN_FRONTEND=noninteractive
export PYTHONUNBUFFERED=1

WAVE_ID="${WAVE_ID:-qqq_365d_canonical_20260501T2250Z}"
GCS_PREFIX="${GCS_PREFIX:-gs://codexalpaca-control-us/research_results/qqq_365d_canonical_20260501T2250Z}"
SOURCE_ARCHIVE_URI="${SOURCE_ARCHIVE_URI:-${GCS_PREFIX}/inputs/source/codexalpaca_repo_source.tar.gz}"
INITIAL_CASH="${INITIAL_CASH:-25000}"
TARGET_EQUITY="${TARGET_EQUITY:-300000}"

STOCK_URI="${STOCK_URI:-gs://codexalpaca-data-us/research_stock_data/qqq_365d_next_trading_day_5x5_20260428/stock_ref_silver/stock_bars/}"
CONTRACTS_URI="${CONTRACTS_URI:-gs://codexalpaca-control-us/research_results/qqq_365d_next_trading_day_5x5_20260428/research_wave/qqq_365d_next_trading_day_5x5_20260428/dense_universe/selected_option_contracts/}"
BARS_URI="${BARS_URI:-gs://codexalpaca-data-us/research_option_data/qqq_365d_next_trading_day_5x5_20260428/option_bars_silver/option_bars/underlying=QQQ/}"

WORKROOT="${WORKROOT:-/mnt/codexalpaca-qqq365}"
REPO_DIR="${WORKROOT}/repo"
DATA_DIR="${WORKROOT}/data"
EMPTY_OPTION_TRADES="${WORKROOT}/empty_option_trades"

mkdir -p "${WORKROOT}" "${DATA_DIR}/stock" "${DATA_DIR}/contracts" "${DATA_DIR}/option_bars" "${EMPTY_OPTION_TRADES}"
exec > >(tee -a "${WORKROOT}/startup.log") 2>&1

now_utc() {
  date -u '+%Y-%m-%dT%H:%M:%SZ'
}

write_status() {
  local phase="$1"
  local detail="${2:-}"
  python3 - "$phase" "$detail" > "${WORKROOT}/qqq_365d_status.json" <<'PY'
import json
import sys
from datetime import UTC, datetime

phase, detail = sys.argv[1:3]
print(json.dumps({
    "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
    "wave_id": "__WAVE_ID__",
    "phase": phase,
    "detail": detail,
    "gcs_prefix": "__GCS_PREFIX__",
    "broker_facing": False,
    "paper_orders": False,
    "live_manifest_effect": "none",
    "risk_policy_effect": "none",
    "hard_rules": [
        "Do not start trading.",
        "Do not submit paper orders.",
        "Do not modify live manifests.",
        "Do not change risk policy.",
        "Do not lower fill_coverage >= 0.90.",
        "Promotion means governed validation review only.",
    ],
}, indent=2, sort_keys=True))
PY
  sed -i -e "s|__WAVE_ID__|${WAVE_ID}|g" -e "s|__GCS_PREFIX__|${GCS_PREFIX}|g" "${WORKROOT}/qqq_365d_status.json"
  gcloud storage cp "${WORKROOT}/qqq_365d_status.json" "${GCS_PREFIX}/status/qqq_365d_status.json" || true
  gcloud storage cp "${WORKROOT}/startup.log" "${GCS_PREFIX}/status/startup.log" || true
}

run_profile() {
  local profile="$1"
  local timing_mode="$2"
  local entry_offset="$3"
  local exit_offset="$4"
  local entry_lag="$5"
  local exit_lag="$6"
  local selector="$7"
  local run_id="qqq365_${profile}_${selector}"
  local output_dir="reports/research_wave/${run_id}"
  local command=(
    python scripts/run_qqq_option_native_tournament.py
    --stock-bars-path "${DATA_DIR}/stock"
    --selected-contracts-root "${DATA_DIR}/contracts"
    --option-bars-root "${DATA_DIR}/option_bars"
    --option-trades-root "${EMPTY_OPTION_TRADES}"
    --regime-labels-csv reports/research_wave/qqq_regime_labels/qqq_regime_labels.csv
    --symbol QQQ
    --entry-timing-mode "${timing_mode}"
    --entry-offset-minutes "${entry_offset}"
    --exit-offset-minutes "${exit_offset}"
    --max-entry-lag-minutes "${entry_lag}"
    --max-exit-lag-minutes "${exit_lag}"
    --test-date-count 20
    --initial-cash "${INITIAL_CASH}"
    --allocation-fraction 0.05
    --slippage-bps 10
    --fee-per-contract 0.65
    --contract-selection-method "${selector}"
    --output-dir "${output_dir}"
    --run-id "${run_id}"
  )
  printf '%q ' "${command[@]}" >> "${WORKROOT}/command.txt"
  printf '\n' >> "${WORKROOT}/command.txt"
  echo "run_started_utc=${run_id}:$(now_utc)"
  write_status "running_profile" "${run_id}"
  "${command[@]}"
  echo "run_completed_utc=${run_id}:$(now_utc)"
  gcloud storage cp --recursive "${output_dir}" "${GCS_PREFIX}/workers/qqq_365d_canonical/reports/research_wave/${run_id}/" || true
}

echo "startup_utc=$(now_utc)"
echo "wave_id=${WAVE_ID}"
echo "gcs_prefix=${GCS_PREFIX}"
write_status "startup" "installing_dependencies"

apt-get update
apt-get install -y python3 python3-venv python3-pip ca-certificates

write_status "staging_source" "copying_source_archive"
gcloud storage cp "${SOURCE_ARCHIVE_URI}" "${WORKROOT}/source.tar.gz"
rm -rf "${REPO_DIR}"
mkdir -p "${REPO_DIR}"
tar -xzf "${WORKROOT}/source.tar.gz" -C "${REPO_DIR}"

cd "${REPO_DIR}"
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[gcp]"

write_status "staging_data" "copying_qqq_365d_dense_dataset"
gcloud storage cp --recursive "${STOCK_URI}" "${DATA_DIR}/stock/"
gcloud storage cp --recursive "${CONTRACTS_URI}" "${DATA_DIR}/contracts/"
gcloud storage cp --recursive "${BARS_URI}" "${DATA_DIR}/option_bars/"

mkdir -p reports/research_wave
write_status "building_regime_labels" "QQQ"
python scripts/build_qqq_regime_labels.py \
  --stock-bars-path "${DATA_DIR}/stock" \
  --output-dir reports/research_wave/qqq_regime_labels \
  --symbol QQQ
gcloud storage cp --recursive reports/research_wave/qqq_regime_labels "${GCS_PREFIX}/regime_labels/"

run_profile fixed_e330_x390_lag15_15 fixed_offset 330 390 15 15 nearest_contract
run_profile first_common_e330_x390_lag15_15 first_common_within_cutoff 330 390 15 15 nearest_contract
run_profile fixed_e300_x390_lag30_60 fixed_offset 300 390 30 60 nearest_contract
run_profile first_common_e300_x390_lag30_60 first_common_within_cutoff 300 390 30 60 nearest_contract
run_profile fixed_e330_x390_lag15_15 fixed_offset 330 390 15 15 entry_liquidity_first_research_only
run_profile first_common_e330_x390_lag15_15 first_common_within_cutoff 330 390 15 15 entry_liquidity_first_research_only

write_status "building_portfolio_report" "all_profiles_complete"
python scripts/build_research_portfolio_report.py \
  --replay-root reports/research_wave \
  --output-dir reports/research_wave/qqq_365d_portfolio_report \
  --fill-coverage-gate 0.90 \
  --min-option-trades 20 \
  --min-test-net-pnl 0 \
  --max-positions 8 \
  --max-strategies-per-symbol 2 \
  --max-symbol-weight 0.20 \
  --initial-cash "${INITIAL_CASH}" \
  --candidate-identity-mode variant_profile

python scripts/build_research_promotion_review_packet.py \
  --portfolio-report-json reports/research_wave/qqq_365d_portfolio_report/research_portfolio_report.json \
  --output-dir reports/research_wave/qqq_365d_promotion_packet

python scripts/build_portfolio_growth_projection.py \
  --portfolio-report-json reports/research_wave/qqq_365d_portfolio_report/research_portfolio_report.json \
  --replay-root reports/research_wave \
  --output-dir reports/research_wave/qqq_365d_growth_projection \
  --initial-cash "${INITIAL_CASH}" \
  --target-equity "${TARGET_EQUITY}" \
  --backtest-allocation-fraction 0.05 \
  --bootstrap-runs 2000

gcloud storage cp --recursive reports/research_wave/qqq_365d_portfolio_report "${GCS_PREFIX}/portfolio_report/"
gcloud storage cp --recursive reports/research_wave/qqq_365d_promotion_packet "${GCS_PREFIX}/promotion_packet/"
gcloud storage cp --recursive reports/research_wave/qqq_365d_growth_projection "${GCS_PREFIX}/growth_projection/"

python - <<'PY' > "${WORKROOT}/artifacts_manifest.json"
import json
from pathlib import Path

root = Path("reports/research_wave")
files = sorted(str(path) for path in root.rglob("*") if path.is_file())
print(json.dumps({"file_count": len(files), "files": files[:1000]}, indent=2))
PY

gcloud storage cp "${WORKROOT}/command.txt" "${GCS_PREFIX}/status/command.txt" || true
gcloud storage cp "${WORKROOT}/artifacts_manifest.json" "${GCS_PREFIX}/status/artifacts_manifest.json" || true
write_status "completed" "qqq_365d_canonical_outputs_uploaded"
echo "completed_utc=$(now_utc)"
