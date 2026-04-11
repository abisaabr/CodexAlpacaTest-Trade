param(
    [switch]$RunOnce
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$pythonPath = Join-Path $repoRoot ".venv\\Scripts\\python.exe"
if (-not (Test-Path $pythonPath)) {
    throw "Missing virtualenv python at $pythonPath. Run scripts\\bootstrap_windows.ps1 first."
}

$scriptPath = Join-Path $repoRoot "scripts\\run_multi_ticker_portfolio_paper_trader.py"
$portfolioConfig = Join-Path $repoRoot "config\\multi_ticker_paper_portfolio.yaml"
$args = @($scriptPath, "--portfolio-config", $portfolioConfig, "--submit-paper-orders")
if ($RunOnce) {
    $args += "--run-once"
}

Push-Location $repoRoot
try {
    & $pythonPath @args
}
finally {
    Pop-Location
}
