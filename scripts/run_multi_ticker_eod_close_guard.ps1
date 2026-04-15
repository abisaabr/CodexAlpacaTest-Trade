param()

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$pythonPath = Join-Path $repoRoot ".venv\\Scripts\\python.exe"
if (-not (Test-Path $pythonPath)) {
    throw "Missing virtualenv python at $pythonPath. Run scripts\\bootstrap_windows.ps1 first."
}

$scriptPath = Join-Path $repoRoot "scripts\\run_multi_ticker_eod_close_guard.py"
$portfolioConfig = Join-Path $repoRoot "config\\multi_ticker_paper_portfolio.yaml"

Push-Location $repoRoot
try {
    & $pythonPath $scriptPath "--portfolio-config" $portfolioConfig "--submit-paper-orders"
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
