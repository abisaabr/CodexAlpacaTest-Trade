#!/usr/bin/env bash
set -Eeuo pipefail

export DEBIAN_FRONTEND=noninteractive
export PYTHONUNBUFFERED=1

WAVE_ID="${WAVE_ID:-portfolio_overnight_12h_20260501}"
GCS_PREFIX="${GCS_PREFIX:-gs://codexalpaca-control-us/research_results/portfolio_overnight_12h_20260501}"
SOURCE_ARCHIVE_URI="${SOURCE_ARCHIVE_URI:-${GCS_PREFIX}/inputs/source/codexalpaca_repo_source.tar.gz}"
AGG_SUBDIR="${AGG_SUBDIR:-aggregate_post_completion_20260501T161922Z}"
CHECK_INTERVAL_SECONDS="${CHECK_INTERVAL_SECONDS:-900}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-21600}"
COMPLETION_INCREMENT_SUMMARIES="${COMPLETION_INCREMENT_SUMMARIES:-4}"
COMPLETION_INCREMENT_SYMBOLS="${COMPLETION_INCREMENT_SYMBOLS:-2}"
FILL_COVERAGE_GATE="${FILL_COVERAGE_GATE:-0.90}"
MIN_OPTION_TRADES="${MIN_OPTION_TRADES:-20}"
MIN_TEST_NET_PNL="${MIN_TEST_NET_PNL:-0}"
MAX_POSITIONS="${MAX_POSITIONS:-8}"
MAX_STRATEGIES_PER_SYMBOL="${MAX_STRATEGIES_PER_SYMBOL:-2}"
MAX_SYMBOL_WEIGHT="${MAX_SYMBOL_WEIGHT:-0.20}"
INITIAL_CASH="${INITIAL_CASH:-25000}"
CANDIDATE_IDENTITY_MODE="${CANDIDATE_IDENTITY_MODE:-variant_profile}"

WORKROOT="${WORKROOT:-/mnt/codexalpaca-postagg}"
REPO_DIR="${WORKROOT}/repo"
WORKER_OUTPUTS="${WORKROOT}/worker_outputs"
MONITOR_DIR="${WORKROOT}/monitor"

mkdir -p "${WORKROOT}" "${MONITOR_DIR}"
exec > >(tee -a "${WORKROOT}/startup.log") 2>&1

now_utc() {
  date -u '+%Y-%m-%dT%H:%M:%SZ'
}

write_state() {
  local phase="$1"
  local reason="${2:-}"
  local summary_count="${3:-0}"
  local symbol_count="${4:-0}"
  local running_count="${5:-0}"
  local elapsed="${6:-0}"
  python3 - "$phase" "$reason" "$summary_count" "$symbol_count" "$running_count" "$elapsed" > "${MONITOR_DIR}/post_completion_aggregate_watch_status.json" <<'PY'
import json
import sys
from datetime import UTC, datetime

phase, reason, summary_count, symbol_count, running_count, elapsed = sys.argv[1:7]
payload = {
    "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
    "phase": phase,
    "reason": reason,
    "wave_id": "__WAVE_ID__",
    "gcs_prefix": "__GCS_PREFIX__",
    "aggregate_output_subdir": "__AGG_SUBDIR__",
    "standard_summary_count": int(summary_count),
    "standard_unique_symbol_count": int(symbol_count),
    "running_all_ticker_worker_count": int(running_count),
    "elapsed_seconds": int(elapsed),
    "hard_rules": [
        "Do not start trading.",
        "Do not submit paper orders.",
        "Do not modify live manifests.",
        "Do not change risk policy.",
        "Do not lower fill_coverage >= 0.90.",
        "Promotion means governed validation review only.",
    ],
}
print(json.dumps(payload, indent=2, sort_keys=True))
PY
  sed -i \
    -e "s|__WAVE_ID__|${WAVE_ID}|g" \
    -e "s|__GCS_PREFIX__|${GCS_PREFIX}|g" \
    -e "s|__AGG_SUBDIR__|${AGG_SUBDIR}|g" \
    "${MONITOR_DIR}/post_completion_aggregate_watch_status.json"
  gcloud storage cp "${MONITOR_DIR}/post_completion_aggregate_watch_status.json" "${GCS_PREFIX}/${AGG_SUBDIR}/monitor/post_completion_aggregate_watch_status.json" || true
  gcloud storage cp "${WORKROOT}/startup.log" "${GCS_PREFIX}/${AGG_SUBDIR}/monitor/startup.log" || true
}

standard_summaries() {
  gcloud storage ls --recursive "${GCS_PREFIX}/workers/**" 2>/dev/null \
    | grep 'option_aware_candidate_summary\.json$' \
    | grep -E '/workers/(fastlane_top40_|option_aware_core_)' \
    || true
}

summary_count() {
  local paths="$1"
  if [[ -z "${paths}" ]]; then
    echo 0
  else
    printf '%s\n' "${paths}" | grep -c . || true
  fi
}

symbol_count() {
  local paths="$1"
  if [[ -z "${paths}" ]]; then
    echo 0
  else
    printf '%s\n' "${paths}" | python3 -c "import re, sys; symbols=set(); [symbols.add(m.group(1).upper()) for line in sys.stdin for m in [re.search(r'_([a-z]{1,5})_(?:nearest_contract|entry_liquidity_first_research_only)/', line)] if m]; print(len(symbols))"
  fi
}

running_all_ticker_workers() {
  gcloud compute instances list \
    --filter="name~'portfolio-overnight-12h-20260501-(fastlane-top40|option-aware-core)' AND status=RUNNING" \
    --format='value(name)' 2>/dev/null \
    | grep -E '^portfolio-overnight-12h-20260501-(fastlane-top40-[abcd]|option-aware-core-[cd])$' \
    | grep -c . \
    || true
}

echo "watch_started_utc=$(now_utc)"
echo "wave_id=${WAVE_ID}"
echo "gcs_prefix=${GCS_PREFIX}"
echo "aggregate_output_subdir=${AGG_SUBDIR}"
echo "check_interval_seconds=${CHECK_INTERVAL_SECONDS}"
echo "max_wait_seconds=${MAX_WAIT_SECONDS}"

baseline_paths="$(standard_summaries)"
baseline_summary_count="$(summary_count "${baseline_paths}")"
baseline_symbol_count="$(symbol_count "${baseline_paths}")"
echo "baseline_standard_summary_count=${baseline_summary_count}"
echo "baseline_standard_unique_symbol_count=${baseline_symbol_count}"

start_epoch="$(date -u +%s)"
trigger_reason=""

while true; do
  current_paths="$(standard_summaries)"
  current_summary_count="$(summary_count "${current_paths}")"
  current_symbol_count="$(symbol_count "${current_paths}")"
  running_count="$(running_all_ticker_workers)"
  now_epoch="$(date -u +%s)"
  elapsed="$((now_epoch - start_epoch))"

  echo "monitor_utc=$(now_utc) standard_summaries=${current_summary_count} unique_symbols=${current_symbol_count} running_all_ticker_workers=${running_count} elapsed_seconds=${elapsed}"
  write_state "waiting_for_more_worker_outputs" "" "${current_summary_count}" "${current_symbol_count}" "${running_count}" "${elapsed}"

  if [[ "${running_count}" -eq 0 ]]; then
    trigger_reason="all_all_ticker_workers_finished"
    break
  fi
  if [[ "${current_summary_count}" -ge $((baseline_summary_count + COMPLETION_INCREMENT_SUMMARIES)) ]]; then
    trigger_reason="summary_increment_threshold_reached"
    break
  fi
  if [[ "${current_symbol_count}" -ge $((baseline_symbol_count + COMPLETION_INCREMENT_SYMBOLS)) ]]; then
    trigger_reason="unique_symbol_increment_threshold_reached"
    break
  fi
  if [[ "${elapsed}" -ge "${MAX_WAIT_SECONDS}" ]]; then
    trigger_reason="max_wait_elapsed_snapshot"
    break
  fi

  sleep "${CHECK_INTERVAL_SECONDS}"
done

current_paths="$(standard_summaries)"
current_summary_count="$(summary_count "${current_paths}")"
current_symbol_count="$(symbol_count "${current_paths}")"
running_count="$(running_all_ticker_workers)"
elapsed="$(($(date -u +%s) - start_epoch))"
echo "aggregate_triggered_utc=$(now_utc) reason=${trigger_reason} standard_summaries=${current_summary_count} unique_symbols=${current_symbol_count} running_all_ticker_workers=${running_count}"
write_state "aggregate_triggered" "${trigger_reason}" "${current_summary_count}" "${current_symbol_count}" "${running_count}" "${elapsed}"

apt-get update
apt-get install -y python3 python3-venv python3-pip ca-certificates

gcloud storage cp "${SOURCE_ARCHIVE_URI}" "${WORKROOT}/source.tar.gz"
rm -rf "${REPO_DIR}" "${WORKER_OUTPUTS}"
mkdir -p "${REPO_DIR}" "${WORKER_OUTPUTS}"
tar -xzf "${WORKROOT}/source.tar.gz" -C "${REPO_DIR}"

cd "${REPO_DIR}"
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[gcp]"

gcloud storage cp --recursive "${GCS_PREFIX}/workers/" "${WORKER_OUTPUTS}/" || true

python scripts/build_research_portfolio_report.py \
  --replay-root "${WORKER_OUTPUTS}" \
  --output-dir reports/research_wave/portfolio_overnight_12h_aggregate \
  --fill-coverage-gate "${FILL_COVERAGE_GATE}" \
  --min-option-trades "${MIN_OPTION_TRADES}" \
  --min-test-net-pnl "${MIN_TEST_NET_PNL}" \
  --max-positions "${MAX_POSITIONS}" \
  --max-strategies-per-symbol "${MAX_STRATEGIES_PER_SYMBOL}" \
  --max-symbol-weight "${MAX_SYMBOL_WEIGHT}" \
  --initial-cash "${INITIAL_CASH}" \
  --candidate-identity-mode "${CANDIDATE_IDENTITY_MODE}"

python scripts/build_research_promotion_review_packet.py \
  --portfolio-report-json reports/research_wave/portfolio_overnight_12h_aggregate/research_portfolio_report.json \
  --output-dir reports/research_wave/portfolio_overnight_12h_promotion_packet

gcloud storage cp --recursive reports/research_wave/portfolio_overnight_12h_aggregate "${GCS_PREFIX}/${AGG_SUBDIR}/portfolio_report/"
gcloud storage cp --recursive reports/research_wave/portfolio_overnight_12h_promotion_packet "${GCS_PREFIX}/${AGG_SUBDIR}/promotion_packet/"
gcloud storage cp "${WORKROOT}/startup.log" "${GCS_PREFIX}/${AGG_SUBDIR}/monitor/startup.log" || true

write_state "aggregate_completed" "${trigger_reason}" "${current_summary_count}" "${current_symbol_count}" "${running_count}" "${elapsed}"
echo "aggregate_completed_utc=$(now_utc)"
