$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $RepoRoot

$BasePython = (Get-Command python -ErrorAction Stop).Source
$VenvPath = Join-Path $RepoRoot ".venv"
$VenvPython = Join-Path $VenvPath "Scripts\python.exe"

Write-Host "Bootstrap is running in a child PowerShell process."
Write-Host "Any activation done inside this script would end when the script exits."
Write-Host "After bootstrap finishes, activate .venv in the PowerShell session you want to keep using."
Write-Host "This repo is locked to Alpaca paper trading only."
Write-Host "Keep ALPACA_PAPER_TRADE=true, LIVE_TRADING=false, and APCA_API_BASE_URL unset or set to https://paper-api.alpaca.markets."
Write-Host ""
Write-Host "Base python for venv creation: $BasePython"
Write-Host "Creating or reusing .venv..."
if (-not (Test-Path $VenvPath)) {
  & $BasePython -m venv $VenvPath
}

if (-not (Test-Path $VenvPython)) {
  throw "Expected virtualenv interpreter was not created: $VenvPython"
}

Write-Host "Using project virtualenv interpreter: $VenvPython"
Write-Host "Upgrading pip and installing project dependencies with the .venv interpreter..."
& $VenvPython -m pip install --upgrade pip
& $VenvPython -m pip install -e .[dev]

if (-not (Test-Path ".env")) {
  Copy-Item .env.example .env
  Write-Host "Created .env from .env.example"
}

Write-Host "Running doctor with the .venv interpreter..."
& $VenvPython .\scripts\doctor.py --skip-connectivity

Write-Host ""
Write-Host "Next commands to run in your current shell:"
Write-Host "  .\.venv\Scripts\Activate.ps1"
Write-Host "  where python"
Write-Host "  python scripts\doctor.py --skip-connectivity"
Write-Host "  python -m pytest"
Write-Host "  python scripts\run_sample_backtest.py --synthetic"
