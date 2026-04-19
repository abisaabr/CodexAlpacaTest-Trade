param()

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$pythonPath = Join-Path $repoRoot ".venv\\Scripts\\python.exe"
$scriptPath = Join-Path $repoRoot "scripts\\run_multi_ticker_github_sync.py"

if (-not (Test-Path $pythonPath)) {
    throw "Missing virtualenv python at $pythonPath. Run scripts\\bootstrap_windows.ps1 first."
}

if (-not (Test-Path $scriptPath)) {
    throw "Missing GitHub-sync script at $scriptPath"
}

Push-Location $repoRoot
try {
    & $pythonPath $scriptPath
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
