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

WAVE_ID="$(metadata_value wave_id microstructure_event_replay)"
WORKER_ID="$(metadata_value worker_id microstructure_worker)"
GCS_PREFIX="$(metadata_value gcs_prefix gs://codexalpaca-control-us/research_results/microstructure_event_replay)"
SOURCE_ARCHIVE_URI="$(metadata_value source_archive_uri "${GCS_PREFIX}/inputs/source/codexalpaca_repo_source.tar.gz")"
EVENTS_JSONL_URI="$(metadata_value events_jsonl_uri)"
GRID_JSONL_URI="$(metadata_value grid_jsonl_uri "${GCS_PREFIX}/inputs/microstructure_research_grid.jsonl")"
GRID_START_INDEX="$(metadata_value grid_start_index 1)"
GRID_COUNT="$(metadata_value grid_count 128)"
UNDERLYINGS="$(metadata_value underlyings QQQ,SPY,IWM)"
UNDERLYINGS="${UNDERLYINGS//;/,}"
MAX_CONTRACTS="$(metadata_value max_contracts 0)"
MAX_CONTRACTS_PER_UNDERLYING="$(metadata_value max_contracts_per_underlying 0)"
FEE_PER_CONTRACT="$(metadata_value fee_per_contract 0.65)"
PROCESSES="$(metadata_value processes "$(nproc)")"
PROGRESS_UPLOAD_INTERVAL_SECONDS="$(metadata_value progress_upload_interval_seconds 60)"

if [[ -z "${EVENTS_JSONL_URI}" ]]; then
  echo "missing_required_metadata events_jsonl_uri" >&2
  exit 2
fi

WORKROOT="${WORKROOT:-/mnt/codexalpaca-microstructure}"
REPO_DIR="${WORKROOT}/repo"
OUTPUT_DIR="${WORKROOT}/outputs"
WORKER_PREFIX="${GCS_PREFIX}/workers/${WORKER_ID}"

mkdir -p "${WORKROOT}" "${OUTPUT_DIR}"
exec > >(tee -a "${WORKROOT}/startup.log") 2>&1

now_utc() {
  date -u '+%Y-%m-%dT%H:%M:%SZ'
}

write_status() {
  local phase="$1"
  local detail="${2:-}"
  python3 - "$phase" "$detail" > "${WORKROOT}/microstructure_status.json" <<'PY'
import json
import sys
from datetime import UTC, datetime

phase, detail = sys.argv[1:3]
print(json.dumps({
    "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
    "phase": phase,
    "detail": detail,
    "wave_id": "__WAVE_ID__",
    "worker_id": "__WORKER_ID__",
    "gcs_prefix": "__GCS_PREFIX__",
    "worker_prefix": "__WORKER_PREFIX__",
    "events_jsonl_uri": "__EVENTS_JSONL_URI__",
    "grid_jsonl_uri": "__GRID_JSONL_URI__",
    "grid_start_index": "__GRID_START_INDEX__",
    "grid_count": "__GRID_COUNT__",
    "underlyings": "__UNDERLYINGS__",
    "max_contracts": "__MAX_CONTRACTS__",
    "max_contracts_per_underlying": "__MAX_CONTRACTS_PER_UNDERLYING__",
    "processes": "__PROCESSES__",
    "progress_upload_interval_seconds": "__PROGRESS_UPLOAD_INTERVAL_SECONDS__",
    "broker_facing": False,
    "paper_orders": False,
    "live_manifest_effect": "none",
    "risk_policy_effect": "none",
}, indent=2, sort_keys=True))
PY
  sed -i \
    -e "s|__WAVE_ID__|${WAVE_ID}|g" \
    -e "s|__WORKER_ID__|${WORKER_ID}|g" \
    -e "s|__GCS_PREFIX__|${GCS_PREFIX}|g" \
    -e "s|__WORKER_PREFIX__|${WORKER_PREFIX}|g" \
    -e "s|__EVENTS_JSONL_URI__|${EVENTS_JSONL_URI}|g" \
    -e "s|__GRID_JSONL_URI__|${GRID_JSONL_URI}|g" \
    -e "s|__GRID_START_INDEX__|${GRID_START_INDEX}|g" \
    -e "s|__GRID_COUNT__|${GRID_COUNT}|g" \
    -e "s|__UNDERLYINGS__|${UNDERLYINGS}|g" \
    -e "s|__MAX_CONTRACTS__|${MAX_CONTRACTS}|g" \
    -e "s|__MAX_CONTRACTS_PER_UNDERLYING__|${MAX_CONTRACTS_PER_UNDERLYING}|g" \
    -e "s|__PROCESSES__|${PROCESSES}|g" \
    -e "s|__PROGRESS_UPLOAD_INTERVAL_SECONDS__|${PROGRESS_UPLOAD_INTERVAL_SECONDS}|g" \
    "${WORKROOT}/microstructure_status.json"
  gcloud storage cp "${WORKROOT}/microstructure_status.json" "${WORKER_PREFIX}/microstructure_status.json" || true
  gcloud storage cp "${WORKROOT}/startup.log" "${WORKER_PREFIX}/startup.log" || true
}

write_status "booting" "installing_system_dependencies"
apt-get update -y
apt-get install -y python3 python3-venv python3-pip ca-certificates curl git tar gzip

write_status "downloading_source" "source_archive"
mkdir -p "${REPO_DIR}"
gcloud storage cp "${SOURCE_ARCHIVE_URI}" "${WORKROOT}/source.tar.gz"
tar -xzf "${WORKROOT}/source.tar.gz" -C "${REPO_DIR}"
cd "${REPO_DIR}"

python3 -m venv "${WORKROOT}/venv"
source "${WORKROOT}/venv/bin/activate"
python -m pip install --upgrade pip

write_status "running_replay" "grid_start=${GRID_START_INDEX} grid_count=${GRID_COUNT}"
cat > "${WORKROOT}/command.txt" <<EOF
python scripts/run_microstructure_event_replay_shard.py --events-jsonl "${EVENTS_JSONL_URI}" --grid-jsonl "${GRID_JSONL_URI}" --output-dir "${OUTPUT_DIR}" --wave-id "${WAVE_ID}" --worker-id "${WORKER_ID}" --grid-start-index "${GRID_START_INDEX}" --grid-count "${GRID_COUNT}" --underlyings "${UNDERLYINGS}" --max-contracts "${MAX_CONTRACTS}" --max-contracts-per-underlying "${MAX_CONTRACTS_PER_UNDERLYING}" --fee-per-contract "${FEE_PER_CONTRACT}" --processes "${PROCESSES}"
EOF

set +e
python scripts/run_microstructure_event_replay_shard.py \
  --events-jsonl "${EVENTS_JSONL_URI}" \
  --grid-jsonl "${GRID_JSONL_URI}" \
  --output-dir "${OUTPUT_DIR}" \
  --wave-id "${WAVE_ID}" \
  --worker-id "${WORKER_ID}" \
  --grid-start-index "${GRID_START_INDEX}" \
  --grid-count "${GRID_COUNT}" \
  --underlyings "${UNDERLYINGS}" \
  --max-contracts "${MAX_CONTRACTS}" \
  --max-contracts-per-underlying "${MAX_CONTRACTS_PER_UNDERLYING}" \
  --fee-per-contract "${FEE_PER_CONTRACT}" \
  --processes "${PROCESSES}" &
REPLAY_PID="$!"
while kill -0 "${REPLAY_PID}" 2>/dev/null; do
  if [[ -f "${OUTPUT_DIR}/microstructure_replay_progress.json" ]]; then
    gcloud storage cp "${OUTPUT_DIR}/microstructure_replay_progress.json" "${WORKER_PREFIX}/microstructure_replay_progress.json" || true
  fi
  sleep "${PROGRESS_UPLOAD_INTERVAL_SECONDS}"
done
wait "${REPLAY_PID}"
REPLAY_STATUS="$?"
if [[ -f "${OUTPUT_DIR}/microstructure_replay_progress.json" ]]; then
  gcloud storage cp "${OUTPUT_DIR}/microstructure_replay_progress.json" "${WORKER_PREFIX}/microstructure_replay_progress.json" || true
fi
set -e
if [[ "${REPLAY_STATUS}" -ne 0 ]]; then
  write_status "failed" "replay_exit_code=${REPLAY_STATUS}"
  exit "${REPLAY_STATUS}"
fi

write_status "uploading_outputs" "replay_complete"
gcloud storage cp "${WORKROOT}/command.txt" "${WORKER_PREFIX}/command.txt" || true
gcloud storage cp --recursive "${OUTPUT_DIR}" "${WORKER_PREFIX}/outputs/" || true
gcloud storage cp "${WORKROOT}/startup.log" "${WORKER_PREFIX}/startup.log" || true
write_status "completed" "outputs_uploaded"

INSTANCE_NAME="$(curl -fs -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/name 2>/dev/null || true)"
INSTANCE_ZONE_PATH="$(curl -fs -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/zone 2>/dev/null || true)"
INSTANCE_ZONE="${INSTANCE_ZONE_PATH##*/}"
if [[ -n "${INSTANCE_NAME}" && -n "${INSTANCE_ZONE}" ]]; then
  echo "self_stop_requested_utc=$(now_utc) instance=${INSTANCE_NAME} zone=${INSTANCE_ZONE}"
  gcloud compute instances stop "${INSTANCE_NAME}" --zone "${INSTANCE_ZONE}" --quiet || true
fi
