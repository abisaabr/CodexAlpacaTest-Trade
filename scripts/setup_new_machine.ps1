param(
    [ValidateSet("docker", "native")]
    [string]$Mode = "docker",
    [switch]$StartServices,
    [switch]$InstallTasks,
    [string]$TaskStartTime = "09:20"
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $repoRoot

foreach ($path in @("data", "reports")) {
    if (-not (Test-Path $path)) {
        New-Item -ItemType Directory -Path $path | Out-Null
    }
}

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example"
}

if ($Mode -eq "docker") {
    $null = Get-Command docker -ErrorAction Stop
    docker compose build
    if ($StartServices) {
        docker compose up -d portfolio-trader portfolio-watchdog portfolio-close-guard
    }
    Write-Host ""
    Write-Host "Docker setup is ready."
    Write-Host "Next steps:"
    Write-Host "  1. Fill .env with Alpaca paper credentials and your ntfy topic."
    Write-Host "  2. Run 'docker compose up -d portfolio-trader portfolio-watchdog portfolio-close-guard' if you did not pass -StartServices."
    Write-Host "  3. Check 'docker compose ps' and 'docker compose logs -f portfolio-trader'."
    exit 0
}

$bootstrapScript = Join-Path $repoRoot "scripts\bootstrap_windows.ps1"
& $bootstrapScript

if ($InstallTasks) {
    & (Join-Path $repoRoot "scripts\install_multi_ticker_paper_task.ps1") -StartTime $TaskStartTime
    & (Join-Path $repoRoot "scripts\install_multi_ticker_health_check_task.ps1")
    & (Join-Path $repoRoot "scripts\install_multi_ticker_eod_close_guard_task.ps1")
}

Write-Host ""
Write-Host "Native Windows setup is ready."
Write-Host "Next steps:"
Write-Host "  1. Activate .venv in this shell with .\.venv\Scripts\Activate.ps1"
Write-Host "  2. Fill .env with Alpaca paper credentials and your ntfy topic."
Write-Host "  3. Run python scripts\doctor.py --skip-connectivity"
Write-Host "  4. Run python -m pytest"
if (-not $InstallTasks) {
    Write-Host "  5. Optional: rerun this script with -Mode native -InstallTasks to install the weekday scheduler."
}
