param(
    [string]$TaskName = "Multi-Ticker Portfolio Paper Trader",
    [string]$StartTime = "09:20",
    [string]$LegacyTaskName = "QQQ Portfolio Paper Trader",
    [switch]$RunOnce
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$runnerPath = Join-Path $repoRoot "scripts\\run_multi_ticker_portfolio_session.ps1"
if (-not (Test-Path $runnerPath)) {
    throw "Missing runner script at $runnerPath"
}

$taskActionArgs = "-NoProfile -ExecutionPolicy Bypass -File `"$runnerPath`""
if ($RunOnce) {
    $taskActionArgs += " -RunOnce"
}

$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $taskActionArgs -WorkingDirectory $repoRoot
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At $StartTime
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Hours 8) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null

if ($LegacyTaskName -and $LegacyTaskName -ne $TaskName) {
    $legacyTask = Get-ScheduledTask -TaskName $LegacyTaskName -ErrorAction SilentlyContinue
    if ($null -ne $legacyTask) {
        Unregister-ScheduledTask -TaskName $LegacyTaskName -Confirm:$false
    }
}

Write-Output "Installed scheduled task '$TaskName' to run weekdays at $StartTime from $repoRoot"
