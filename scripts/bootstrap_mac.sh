#!/usr/bin/env bash
set -euo pipefail

echo "Bootstrap is running in a child shell process."
echo "Any activation done inside this script ends when the script exits."
echo "This repo is locked to Alpaca paper trading only."
echo "Keep ALPACA_PAPER_TRADE=true, LIVE_TRADING=false, and APCA_API_BASE_URL unset or set to https://paper-api.alpaca.markets."
echo

if [ ! -d ".venv" ]; then
  python3 -m venv .venv
fi

./.venv/bin/python -m pip install --upgrade pip
./.venv/bin/python -m pip install -e .[dev]

if [ ! -f ".env" ]; then
  cp .env.example .env
fi

./.venv/bin/python scripts/doctor.py --skip-connectivity

echo
echo "Next commands to run in your current shell:"
echo "  source .venv/bin/activate"
echo "  which python"
echo "  python scripts/doctor.py --skip-connectivity"
echo "  python -m pytest"
echo "  python scripts/run_sample_backtest.py --synthetic"
