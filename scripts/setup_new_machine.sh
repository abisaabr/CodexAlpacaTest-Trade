#!/usr/bin/env bash
set -euo pipefail

MODE="docker"
START_SERVICES="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="$2"
      shift 2
      ;;
    --start-services)
      START_SERVICES="true"
      shift
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

mkdir -p data reports

if [[ ! -f ".env" ]]; then
  cp .env.example .env
  echo "Created .env from .env.example"
fi

if [[ "$MODE" == "docker" ]]; then
  docker compose build
  if [[ "$START_SERVICES" == "true" ]]; then
    docker compose up -d portfolio-trader portfolio-watchdog portfolio-close-guard
  fi
  echo
  echo "Docker setup is ready."
  echo "Next steps:"
  echo "  1. Fill .env with Alpaca paper credentials and your ntfy topic."
  echo "  2. If this is a standby machine, run 'docker compose run --rm portfolio-trader python scripts/run_multi_ticker_standby_failover_check.py'."
  echo "  3. Run 'docker compose up -d portfolio-trader portfolio-watchdog portfolio-close-guard' if you did not pass --start-services."
  echo "  4. Check 'docker compose ps' and 'docker compose logs -f portfolio-trader'."
  exit 0
fi

if [[ "$(uname -s)" == "Darwin" ]]; then
  bash ./scripts/bootstrap_mac.sh
else
  bash ./scripts/bootstrap_linux.sh
fi

echo
echo "Native shell setup is ready."
echo "Next steps:"
echo "  1. Activate .venv with 'source .venv/bin/activate'"
echo "  2. Fill .env with Alpaca paper credentials and your ntfy topic."
echo "  3. If this is a standby machine, run 'python scripts/run_multi_ticker_standby_failover_check.py'"
echo "  4. Run 'python scripts/doctor.py --skip-connectivity'"
echo "  5. Run 'python -m pytest'"
echo "  6. For the always-on trader on macOS/Linux, prefer 'docker compose up -d portfolio-trader portfolio-watchdog portfolio-close-guard'."
