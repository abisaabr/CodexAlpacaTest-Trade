param(
    [string]$TaskName = "Multi-Ticker Portfolio Health Check",
    [string]$StartTime = "06:05"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$runnerPath = Join-Path $repoRoot "scripts\\run_multi_ticker_health_check.ps1"
if (-not (Test-Path $runnerPath)) {
    throw "Missing health-check runner at $runnerPath"
}

$taskCommand = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$runnerPath`""

schtasks.exe /Create `
    /TN $TaskName `
    /TR $taskCommand `
    /SC HOURLY `
    /MO 1 `
    /ST $StartTime `
    /F | Out-Null

Write-Output "Installed scheduled task '$TaskName' to run hourly from $StartTime using $runnerPath"
