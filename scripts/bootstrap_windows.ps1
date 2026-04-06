$ErrorActionPreference = "Stop"

Write-Host "Creating or reusing .venv..."
if (-not (Test-Path ".venv")) {
  python -m venv .venv
}

Write-Host "Activating virtual environment..."
. .\.venv\Scripts\Activate.ps1

Write-Host "Upgrading pip and installing project dependencies..."
python -m pip install --upgrade pip
python -m pip install -e .[dev]

if (-not (Test-Path ".env")) {
  Copy-Item .env.example .env
  Write-Host "Created .env from .env.example"
}

Write-Host "Running doctor..."
python scripts/doctor.py --skip-connectivity
