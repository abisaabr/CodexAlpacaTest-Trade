param(
    [string]$TaskName = "Multi-Ticker Portfolio GitHub Sync",
    [string]$StartTime = "06:10"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$runnerPath = Join-Path $repoRoot "scripts\\run_multi_ticker_github_sync.ps1"
if (-not (Test-Path $runnerPath)) {
    throw "Missing GitHub-sync runner at $runnerPath"
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
