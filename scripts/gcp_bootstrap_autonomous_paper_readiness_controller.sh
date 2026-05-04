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

REPO_URL="$(metadata_value repo_url "https://github.com/abisaabr/CodexAlpacaTest-Trade.git")"
BRANCH="$(metadata_value branch "codex/phase2-fill-semantics-20260430")"
SOURCE_ARCHIVE_URI="$(metadata_value source_archive_uri "gs://codexalpaca-control-us/research_results/autonomous_paper_readiness_20260504/inputs/source/codexalpaca_repo_source.tar.gz")"
STATUS_PREFIX="$(metadata_value status_prefix "gs://codexalpaca-control-us/research_results/autonomous_paper_readiness_20260504/controller")"
CONTROLLER_ARGS="$(metadata_value controller_args "--max-launches-per-pass 16 --allow-delete-terminated")"
LOOP_SLEEP_SECONDS="$(metadata_value loop_sleep_seconds "900")"

WORKROOT="${WORKROOT:-/mnt/codexalpaca-autonomous-controller}"
REPO_DIR="${WORKROOT}/repo"
mkdir -p "${WORKROOT}"
exec > >(tee -a "${WORKROOT}/startup.log") 2>&1

now_utc() {
  date -u '+%Y-%m-%dT%H:%M:%SZ'
}

publish_log() {
  gcloud storage cp "${WORKROOT}/startup.log" "${STATUS_PREFIX}/controller_vm_startup.log" || true
}

echo "controller_bootstrap_started_utc=$(now_utc)"
echo "repo_url=${REPO_URL}"
echo "branch=${BRANCH}"
echo "source_archive_uri=${SOURCE_ARCHIVE_URI}"
echo "status_prefix=${STATUS_PREFIX}"
echo "controller_args=${CONTROLLER_ARGS}"
echo "loop_sleep_seconds=${LOOP_SLEEP_SECONDS}"

apt-get update
apt-get install -y git python3 python3-venv python3-pip ca-certificates

rm -rf "${REPO_DIR}"
if git clone --branch "${BRANCH}" --depth 1 "${REPO_URL}" "${REPO_DIR}"; then
  echo "repo_clone_success branch=${BRANCH}"
else
  echo "repo_clone_failed_falling_back_to_source_archive"
  mkdir -p "${REPO_DIR}"
  gcloud storage cp "${SOURCE_ARCHIVE_URI}" "${WORKROOT}/source.tar.gz"
  tar -xzf "${WORKROOT}/source.tar.gz" -C "${REPO_DIR}"
fi

cd "${REPO_DIR}"
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[gcp]"
sha256sum pyproject.toml > "${WORKROOT}/last_pyproject.sha256"

publish_log
while true; do
  if [[ -d .git ]]; then
    git fetch origin "${BRANCH}" || true
    git reset --hard "origin/${BRANCH}" || true
  fi
  source .venv/bin/activate
  if ! sha256sum --check --status "${WORKROOT}/last_pyproject.sha256"; then
    echo "dependency_fingerprint_changed_utc=$(now_utc)"
    python -m pip install -e ".[gcp]"
    sha256sum pyproject.toml > "${WORKROOT}/last_pyproject.sha256"
  fi
  echo "controller_loop_started_utc=$(now_utc)"
  # shellcheck disable=SC2086
  python -u scripts/gcp_autonomous_paper_readiness_controller.py \
    --gcloud gcloud \
    --source-archive-uri "${SOURCE_ARCHIVE_URI}" \
    --status-prefix "${STATUS_PREFIX}" \
    ${CONTROLLER_ARGS}
  publish_log
  sleep "${LOOP_SLEEP_SECONDS}"
done
