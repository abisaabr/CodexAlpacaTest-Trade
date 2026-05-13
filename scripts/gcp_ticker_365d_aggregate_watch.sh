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

WAVE_ID="${WAVE_ID:-$(metadata_value wave_id ticker_365d_all_available_20260501T2300Z)}"
GCS_PREFIX="${GCS_PREFIX:-$(metadata_value gcs_prefix gs://codexalpaca-control-us/research_results/ticker_365d_all_available_20260501T2300Z)}"
SOURCE_ARCHIVE_URI="${SOURCE_ARCHIVE_URI:-$(metadata_value source_archive_uri "${GCS_PREFIX}/inputs/source/codexalpaca_repo_source.tar.gz")}"
EXPECTED_SUMMARY_COUNT="${EXPECTED_SUMMARY_COUNT:-$(metadata_value expected_summary_count 40)}"
CHECK_INTERVAL_SECONDS="${CHECK_INTERVAL_SECONDS:-$(metadata_value check_interval_seconds 900)}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-$(metadata_value max_wait_seconds 43200)}"
INITIAL_CASH="${INITIAL_CASH:-$(metadata_value initial_cash 25000)}"
TARGET_EQUITY="${TARGET_EQUITY:-$(metadata_value target_equity 300000)}"

WORKROOT="${WORKROOT:-/mnt/codexalpaca-ticker365-agg}"
REPO_DIR="${WORKROOT}/repo"
WORKER_OUTPUTS="${WORKROOT}/worker_outputs"
mkdir -p "${WORKROOT}"
exec > >(tee -a "${WORKROOT}/startup.log") 2>&1

now_utc() {
  date -u '+%Y-%m-%dT%H:%M:%SZ'
}

summary_count() {
  gcloud storage ls --recursive "${GCS_PREFIX}/workers/**" 2>/dev/null \
    | grep 'option_aware_candidate_summary\.json$' \
    | grep -c . \
    || true
}

write_status() {
  local phase="$1"
  local detail="${2:-}"
  local count="${3:-0}"
  local elapsed="${4:-0}"
  python3 - \
    "$phase" \
    "$detail" \
    "$count" \
    "$elapsed" \
    "$WAVE_ID" \
    "$GCS_PREFIX" \
    "$EXPECTED_SUMMARY_COUNT" \
    > "${WORKROOT}/ticker_365d_aggregate_status.json" <<'PY'
import json
import sys
from datetime import UTC, datetime

phase, detail, count, elapsed, wave_id, gcs_prefix, expected_summary_count = sys.argv[1:8]
print(json.dumps({
    "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
    "wave_id": wave_id,
    "phase": phase,
    "detail": detail,
    "gcs_prefix": gcs_prefix,
    "summary_count": int(count),
    "expected_summary_count": int(expected_summary_count),
    "elapsed_seconds": int(elapsed),
    "broker_facing": False,
    "paper_orders": False,
    "live_manifest_effect": "none",
    "risk_policy_effect": "none",
}, indent=2, sort_keys=True))
PY
  gcloud storage cp "${WORKROOT}/ticker_365d_aggregate_status.json" "${GCS_PREFIX}/aggregate/status/ticker_365d_aggregate_status.json" || true
  gcloud storage cp "${WORKROOT}/startup.log" "${GCS_PREFIX}/aggregate/status/startup.log" || true
}

echo "aggregate_watch_started_utc=$(now_utc)"
echo "wave_id=${WAVE_ID}"
echo "gcs_prefix=${GCS_PREFIX}"
echo "expected_summary_count=${EXPECTED_SUMMARY_COUNT}"

start_epoch="$(date -u +%s)"
trigger_reason=""
while true; do
  count="$(summary_count)"
  elapsed="$(($(date -u +%s) - start_epoch))"
  echo "monitor_utc=$(now_utc) summary_count=${count} expected=${EXPECTED_SUMMARY_COUNT} elapsed=${elapsed}"
  write_status "waiting_for_ticker_shards" "" "${count}" "${elapsed}"
  if [[ "${count}" -ge "${EXPECTED_SUMMARY_COUNT}" ]]; then
    trigger_reason="expected_summary_count_reached"
    break
  fi
  if [[ "${elapsed}" -ge "${MAX_WAIT_SECONDS}" ]]; then
    trigger_reason="max_wait_elapsed_snapshot"
    break
  fi
  sleep "${CHECK_INTERVAL_SECONDS}"
done

count="$(summary_count)"
elapsed="$(($(date -u +%s) - start_epoch))"
write_status "aggregate_triggered" "${trigger_reason}" "${count}" "${elapsed}"

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

PROJECTION_CALENDAR_DIR="reports/research_wave/ticker_365d_all_available_projection_calendar"
mkdir -p "${PROJECTION_CALENDAR_DIR}"
gcloud storage cp \
  "${GCS_PREFIX}/inputs/ticker_365d_all_available_launch_rows.json" \
  "${PROJECTION_CALENDAR_DIR}/launch_rows.json"
python scripts/build_gcs_projection_calendar.py \
  --launch-rows-json "${PROJECTION_CALENDAR_DIR}/launch_rows.json" \
  --output-dir "${PROJECTION_CALENDAR_DIR}"

python scripts/build_research_portfolio_report.py \
  --replay-root "${WORKER_OUTPUTS}" \
  --output-dir reports/research_wave/ticker_365d_all_available_portfolio_report \
  --fill-coverage-gate 0.90 \
  --min-option-trades 20 \
  --min-test-net-pnl 0 \
  --max-positions 12 \
  --max-strategies-per-symbol 2 \
  --max-symbol-weight 0.20 \
  --initial-cash "${INITIAL_CASH}" \
  --candidate-identity-mode variant_profile

python scripts/build_research_promotion_review_packet.py \
  --portfolio-report-json reports/research_wave/ticker_365d_all_available_portfolio_report/research_portfolio_report.json \
  --output-dir reports/research_wave/ticker_365d_all_available_promotion_packet

python scripts/build_portfolio_growth_projection.py \
  --portfolio-report-json reports/research_wave/ticker_365d_all_available_portfolio_report/research_portfolio_report.json \
  --replay-root "${WORKER_OUTPUTS}" \
  --output-dir reports/research_wave/ticker_365d_all_available_growth_projection \
  --calendar-csv "${PROJECTION_CALENDAR_DIR}/projection_calendar.csv" \
  --calendar-date-column trade_date \
  --initial-cash "${INITIAL_CASH}" \
  --target-equity "${TARGET_EQUITY}" \
  --backtest-allocation-fraction 0.05 \
  --bootstrap-runs 2000

gcloud storage cp --recursive "${PROJECTION_CALENDAR_DIR}" "${GCS_PREFIX}/aggregate/projection_calendar/"
gcloud storage cp --recursive reports/research_wave/ticker_365d_all_available_portfolio_report "${GCS_PREFIX}/aggregate/portfolio_report/"
gcloud storage cp --recursive reports/research_wave/ticker_365d_all_available_promotion_packet "${GCS_PREFIX}/aggregate/promotion_packet/"
gcloud storage cp --recursive reports/research_wave/ticker_365d_all_available_growth_projection "${GCS_PREFIX}/aggregate/growth_projection/"

write_status "aggregate_completed" "${trigger_reason}" "${count}" "${elapsed}"
echo "aggregate_completed_utc=$(now_utc)"

INSTANCE_NAME="$(curl -fs -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/name 2>/dev/null || true)"
INSTANCE_ZONE_PATH="$(curl -fs -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/zone 2>/dev/null || true)"
INSTANCE_ZONE="${INSTANCE_ZONE_PATH##*/}"
if [[ -n "${INSTANCE_NAME}" && -n "${INSTANCE_ZONE}" ]]; then
  echo "self_stop_requested_utc=$(now_utc) instance=${INSTANCE_NAME} zone=${INSTANCE_ZONE}"
  gcloud compute instances stop "${INSTANCE_NAME}" --zone "${INSTANCE_ZONE}" --quiet || true
fi
