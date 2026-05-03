$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$LogDir = Join-Path $RepoRoot "logs"
$LogPath = Join-Path $LogDir "ticker365_timing_rescue_readiness_watchdog.log"
$Python = "C:\Users\rabisaab\AppData\Local\Programs\Python\Python312\python.exe"
$Gcloud = "C:\Users\rabisaab\Downloads\google-cloud-sdk-local\google-cloud-sdk\bin\gcloud.cmd"
$GcsPrefix = "gs://codexalpaca-control-us/research_results/ticker365_timing_rescue_20260503T1906Z"
$OutputDir = "reports/gcp_research/ticker365_timing_rescue_paper_readiness_20260503"

New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
Set-Location $RepoRoot

$Utf8NoBom = New-Object System.Text.UTF8Encoding $false
function Add-WatchdogLogLine {
    param([string]$Line)
    [System.IO.File]::AppendAllText($LogPath, $Line + [Environment]::NewLine, $Utf8NoBom)
}

$env:CLOUDSDK_PYTHON = $Python
$env:GOOGLE_CLOUD_PROJECT = "codexalpaca"
$KeyPath = "C:\Users\rabisaab\Downloads\codexalpaca-7bcb9ac9a02d.json"
if (Test-Path $KeyPath) {
    $env:GOOGLE_APPLICATION_CREDENTIALS = $KeyPath
}

$stamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
Add-WatchdogLogLine "===== ticker365_timing_rescue_readiness_watchdog run $stamp ====="

& $Python "scripts\monitor_ticker365_paper_readiness.py" `
    --gcloud-bin $Gcloud `
    --gcs-prefix $GcsPrefix `
    --output-dir $OutputDir `
    --once 2>&1 |
    ForEach-Object {
        $line = $_.ToString()
        Write-Output $line
        Add-WatchdogLogLine $line
    }

exit $LASTEXITCODE
