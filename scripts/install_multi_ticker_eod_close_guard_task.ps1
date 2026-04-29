param(
    [string]$TaskName = "Multi-Ticker Portfolio EOD Close Guard",
    [string]$StartTime = "15:58"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$runnerPath = Join-Path $repoRoot "scripts\\run_multi_ticker_eod_close_guard.ps1"
if (-not (Test-Path $runnerPath)) {
    throw "Missing EOD close guard runner at $runnerPath"
}

$taskActionArgs = "-NoProfile -ExecutionPolicy Bypass -File `"$runnerPath`""
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $taskActionArgs -WorkingDirectory $repoRoot
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At $StartTime
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 45) `
    -RestartCount 2 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null
Set-ScheduledTask -TaskName $TaskName -Settings $settings | Out-Null

Write-Output "Installed scheduled task '$TaskName' to run weekdays at $StartTime from $repoRoot"
