#!/usr/bin/env bash
set -Eeuo pipefail

export DEBIAN_FRONTEND=noninteractive
export PYTHONUNBUFFERED=1

metadata_value() {
  local key="$1"
  local default_value="${2:-}"
  curl -fs -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/attributes/${key}" \
    2>/dev/null || printf '%s' "${default_value}"
}

SYMBOL="$(metadata_value symbol)"
WORKER_ID="$(metadata_value worker_id "ticker365_${SYMBOL,,}")"
WAVE_ID="$(metadata_value wave_id ticker_365d_all_available_20260501T2300Z)"
GCS_PREFIX="$(metadata_value gcs_prefix gs://codexalpaca-control-us/research_results/ticker_365d_all_available_20260501T2300Z)"
SOURCE_ARCHIVE_URI="$(metadata_value source_archive_uri "${GCS_PREFIX}/inputs/source/codexalpaca_repo_source.tar.gz")"
INPUT_VARIANTS_URI="$(metadata_value input_variants_uri gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/inputs/portfolio_overnight_variants.jsonl)"
INPUT_QUEUE_URI="$(metadata_value input_queue_uri gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501/inputs/portfolio_overnight_option_queue.json)"
STOCK_URI="$(metadata_value stock_uri)"
CONTRACTS_URI="$(metadata_value contracts_uri)"
BARS_URI="$(metadata_value bars_uri)"
INITIAL_CASH="$(metadata_value initial_cash 25000)"
TOP_N="$(metadata_value top_n 40)"
TEST_DATE_COUNT="$(metadata_value test_date_count 20)"
ALLOCATION_FRACTION="$(metadata_value allocation_fraction 0.05)"
SLIPPAGE_BPS="$(metadata_value slippage_bps 10)"
FEE_PER_CONTRACT="$(metadata_value fee_per_contract 0.65)"
SELECTORS_CSV="$(metadata_value selectors "nearest_contract,entry_liquidity_first_research_only")"
LAG_PROFILES_CSV="$(metadata_value lag_profiles "10:10")"
ENTRY_BAR_LOOKUP_MODE="$(metadata_value entry_bar_lookup_mode "first_bar_at_or_after_entry_within_lag")"
MAX_ENTRY_STALENESS_MINUTES="$(metadata_value max_entry_staleness_minutes 5)"
EXIT_BAR_LOOKUP_MODE="$(metadata_value exit_bar_lookup_mode "first_bar_at_or_after_exit_within_lag")"
SELECTORS_CSV="${SELECTORS_CSV//;/,}"
LAG_PROFILES_CSV="${LAG_PROFILES_CSV//;/,}"

if [[ -z "${SYMBOL}" || -z "${STOCK_URI}" || -z "${CONTRACTS_URI}" || -z "${BARS_URI}" ]]; then
  echo "missing_required_metadata symbol=${SYMBOL} stock_uri=${STOCK_URI} contracts_uri=${CONTRACTS_URI} bars_uri=${BARS_URI}" >&2
  exit 2
fi

WORKROOT="${WORKROOT:-/mnt/codexalpaca-ticker365}"
REPO_DIR="${WORKROOT}/repo"
DATA_DIR="${WORKROOT}/data"
EMPTY_OPTION_TRADES="${WORKROOT}/empty_option_trades"
WORKER_PREFIX="${GCS_PREFIX}/workers/${WORKER_ID}"

mkdir -p "${WORKROOT}" "${DATA_DIR}/stock" "${DATA_DIR}/contracts" "${DATA_DIR}/option_bars" "${EMPTY_OPTION_TRADES}"
exec > >(tee -a "${WORKROOT}/startup.log") 2>&1
MONITOR_PID=""

now_utc() {
  date -u '+%Y-%m-%dT%H:%M:%SZ'
}

write_status() {
  local phase="$1"
  local detail="${2:-}"
  python3 - "$phase" "$detail" > "${WORKROOT}/ticker_365d_status.json" <<'PY'
import json
import sys
from datetime import UTC, datetime

phase, detail = sys.argv[1:3]
print(json.dumps({
    "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
    "wave_id": "__WAVE_ID__",
    "worker_id": "__WORKER_ID__",
    "symbol": "__SYMBOL__",
    "phase": phase,
    "detail": detail,
    "gcs_prefix": "__GCS_PREFIX__",
    "worker_prefix": "__WORKER_PREFIX__",
    "broker_facing": False,
    "paper_orders": False,
    "live_manifest_effect": "none",
    "risk_policy_effect": "none",
}, indent=2, sort_keys=True))
PY
  sed -i \
    -e "s|__WAVE_ID__|${WAVE_ID}|g" \
    -e "s|__WORKER_ID__|${WORKER_ID}|g" \
    -e "s|__SYMBOL__|${SYMBOL}|g" \
    -e "s|__GCS_PREFIX__|${GCS_PREFIX}|g" \
    -e "s|__WORKER_PREFIX__|${WORKER_PREFIX}|g" \
    "${WORKROOT}/ticker_365d_status.json"
  gcloud storage cp "${WORKROOT}/ticker_365d_status.json" "${WORKER_PREFIX}/ticker_365d_status.json" || true
  gcloud storage cp "${WORKROOT}/startup.log" "${WORKER_PREFIX}/startup.log" || true
}

start_runtime_monitor() {
  (
    while true; do
      {
        echo "monitor_utc=$(now_utc)"
        echo "--- ps ---"
        ps -eo pid,ppid,pcpu,pmem,etime,cmd --sort=-pcpu | head -30 || true
        echo "--- memory ---"
        free -h || true
        echo "--- disk ---"
        df -h "${WORKROOT}" || true
        echo "--- report_files ---"
        if [[ -d "${REPO_DIR}/reports/research_wave" ]]; then
          find "${REPO_DIR}/reports/research_wave" -maxdepth 4 -type f | head -80 || true
        fi
        echo
      } >> "${WORKROOT}/runtime_monitor.log"
      gcloud storage cp "${WORKROOT}/runtime_monitor.log" "${WORKER_PREFIX}/runtime_monitor.log" || true
      gcloud storage cp "${WORKROOT}/startup.log" "${WORKER_PREFIX}/startup.log" || true
      sleep 120
    done
  ) &
  MONITOR_PID="$!"
}

cleanup_runtime_monitor() {
  if [[ -n "${MONITOR_PID}" ]]; then
    kill "${MONITOR_PID}" 2>/dev/null || true
  fi
}
trap cleanup_runtime_monitor EXIT

safe_slug() {
  local value="$1"
  value="${value//[^A-Za-z0-9]/_}"
  printf '%s' "${value}"
}

run_selector() {
  local selector="$1"
  local entry_lag="$2"
  local exit_lag="$3"
  local selector_slug
  local entry_slug
  local exit_slug
  selector_slug="$(safe_slug "${selector}")"
  entry_slug="$(safe_slug "${entry_lag}")"
  exit_slug="$(safe_slug "${exit_lag}")"
  local run_id="${WORKER_ID}_${SYMBOL,,}_e${entry_slug}_x${exit_slug}_${selector_slug}"
  local output_dir="reports/research_wave/${run_id}"
  local command=(
    python -u scripts/run_option_aware_research_backtest.py
    --queue-json inputs/portfolio_overnight_option_queue.json
    --variants-jsonl inputs/portfolio_overnight_variants.jsonl
    --stock-bars-path "${DATA_DIR}/stock"
    --selected-contracts-root "${DATA_DIR}/contracts"
    --option-bars-root "${DATA_DIR}/option_bars"
    --option-trades-root "${EMPTY_OPTION_TRADES}"
    --output-dir "${output_dir}"
    --run-id "${run_id}"
    --top-n "${TOP_N}"
    --symbol-filter "${SYMBOL}"
    --max-entry-lag-minutes "${entry_lag}"
    --entry-bar-lookup-mode "${ENTRY_BAR_LOOKUP_MODE}"
    --max-entry-staleness-minutes "${MAX_ENTRY_STALENESS_MINUTES}"
    --max-exit-lag-minutes "${exit_lag}"
    --exit-bar-lookup-mode "${EXIT_BAR_LOOKUP_MODE}"
    --test-date-count "${TEST_DATE_COUNT}"
    --initial-cash "${INITIAL_CASH}"
    --allocation-fraction "${ALLOCATION_FRACTION}"
    --slippage-bps "${SLIPPAGE_BPS}"
    --fee-per-contract "${FEE_PER_CONTRACT}"
    --contract-selection-method "${selector}"
  )
  printf '%q ' "${command[@]}" >> "${WORKROOT}/command.txt"
  printf '\n' >> "${WORKROOT}/command.txt"
  echo "run_started_utc=${run_id}:$(now_utc)"
  "${command[@]}"
  echo "run_completed_utc=${run_id}:$(now_utc)"
  echo "completed_run_id=${run_id}"
  gcloud storage cp --recursive "${output_dir}" "${WORKER_PREFIX}/reports/research_wave/${run_id}/" || true
}

echo "startup_utc=$(now_utc)"
echo "wave_id=${WAVE_ID}"
echo "worker_id=${WORKER_ID}"
echo "symbol=${SYMBOL}"
echo "gcs_prefix=${GCS_PREFIX}"
echo "top_n=${TOP_N}"
echo "test_date_count=${TEST_DATE_COUNT}"
echo "allocation_fraction=${ALLOCATION_FRACTION}"
echo "selectors=${SELECTORS_CSV}"
echo "lag_profiles=${LAG_PROFILES_CSV}"
echo "entry_bar_lookup_mode=${ENTRY_BAR_LOOKUP_MODE}"
echo "max_entry_staleness_minutes=${MAX_ENTRY_STALENESS_MINUTES}"
echo "exit_bar_lookup_mode=${EXIT_BAR_LOOKUP_MODE}"
start_runtime_monitor
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

mkdir -p inputs reports/research_wave
gcloud storage cp "${INPUT_VARIANTS_URI}" inputs/portfolio_overnight_variants.jsonl
gcloud storage cp "${INPUT_QUEUE_URI}" inputs/portfolio_overnight_option_queue.json

write_status "staging_data" "copying_${SYMBOL}_365d_dataset"
gcloud storage cp --recursive "${STOCK_URI}" "${DATA_DIR}/stock/"
gcloud storage cp --recursive "${CONTRACTS_URI}" "${DATA_DIR}/contracts/"
gcloud storage cp --recursive "${BARS_URI}" "${DATA_DIR}/option_bars/"

write_status "running_selectors" "selectors=${SELECTORS_CSV} lag_profiles=${LAG_PROFILES_CSV}"
IFS=',' read -r -a SELECTORS <<< "${SELECTORS_CSV}"
IFS=',' read -r -a LAG_PROFILES <<< "${LAG_PROFILES_CSV}"
for lag_profile in "${LAG_PROFILES[@]}"; do
  lag_profile="$(echo "${lag_profile}" | xargs)"
  if [[ -z "${lag_profile}" ]]; then
    continue
  fi
  if [[ "${lag_profile}" != *:* ]]; then
    write_status "failed" "invalid_lag_profile=${lag_profile}"
    exit 2
  fi
  entry_lag="${lag_profile%%:*}"
  exit_lag="${lag_profile##*:}"
  echo "lag_profile_started=${lag_profile} entry=${entry_lag} exit=${exit_lag} utc=$(now_utc)"
  pids=()
  labels=()
  for selector in "${SELECTORS[@]}"; do
    selector="$(echo "${selector}" | xargs)"
    if [[ -z "${selector}" ]]; then
      continue
    fi
    run_selector "${selector}" "${entry_lag}" "${exit_lag}" &
    pids+=("$!")
    labels+=("${selector}")
  done
  profile_status=0
  for index in "${!pids[@]}"; do
    if ! wait "${pids[$index]}"; then
      echo "selector_failed selector=${labels[$index]} lag_profile=${lag_profile}"
      profile_status=1
    fi
  done
  if [[ "${profile_status}" -ne 0 ]]; then
    write_status "failed" "lag_profile=${lag_profile}"
    exit 1
  fi
  echo "lag_profile_completed=${lag_profile} utc=$(now_utc)"
done

write_status "building_symbol_report" "selectors_complete"
python scripts/build_research_portfolio_report.py \
  --replay-root reports/research_wave \
  --output-dir reports/research_wave/${WORKER_ID}_portfolio_report \
  --fill-coverage-gate 0.90 \
  --min-option-trades 20 \
  --min-test-net-pnl 0 \
  --max-positions 4 \
  --max-strategies-per-symbol 2 \
  --max-symbol-weight 0.20 \
  --initial-cash "${INITIAL_CASH}" \
  --candidate-identity-mode variant_profile

python scripts/build_research_promotion_review_packet.py \
  --portfolio-report-json reports/research_wave/${WORKER_ID}_portfolio_report/research_portfolio_report.json \
  --output-dir reports/research_wave/${WORKER_ID}_promotion_packet

gcloud storage cp --recursive "reports/research_wave/${WORKER_ID}_portfolio_report" "${WORKER_PREFIX}/portfolio_report/" || true
gcloud storage cp --recursive "reports/research_wave/${WORKER_ID}_promotion_packet" "${WORKER_PREFIX}/promotion_packet/" || true

python - <<'PY' > "${WORKROOT}/artifacts_manifest.json"
import json
from pathlib import Path

root = Path("reports/research_wave")
files = sorted(str(path) for path in root.rglob("*") if path.is_file())
print(json.dumps({"file_count": len(files), "files": files[:500]}, indent=2))
PY

gcloud storage cp "${WORKROOT}/command.txt" "${WORKER_PREFIX}/command.txt" || true
gcloud storage cp "${WORKROOT}/artifacts_manifest.json" "${WORKER_PREFIX}/artifacts_manifest.json" || true
write_status "completed" "ticker_365d_outputs_uploaded"
echo "completed_utc=$(now_utc)"

INSTANCE_NAME="$(curl -fs -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/name 2>/dev/null || true)"
INSTANCE_ZONE_PATH="$(curl -fs -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/zone 2>/dev/null || true)"
INSTANCE_ZONE="${INSTANCE_ZONE_PATH##*/}"
if [[ -n "${INSTANCE_NAME}" && -n "${INSTANCE_ZONE}" ]]; then
  echo "self_stop_requested_utc=$(now_utc) instance=${INSTANCE_NAME} zone=${INSTANCE_ZONE}"
  gcloud compute instances stop "${INSTANCE_NAME}" --zone "${INSTANCE_ZONE}" --quiet || true
fi
