param()

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$pythonPath = Join-Path $repoRoot ".venv\\Scripts\\python.exe"
$scriptPath = Join-Path $repoRoot "scripts\\run_multi_ticker_health_check.py"

if (-not (Test-Path $pythonPath)) {
    throw "Missing virtualenv python at $pythonPath. Run scripts\\bootstrap_windows.ps1 first."
}

if (-not (Test-Path $scriptPath)) {
    throw "Missing health-check script at $scriptPath"
}

Push-Location $repoRoot
try {
    & $pythonPath $scriptPath --restart-if-needed
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
