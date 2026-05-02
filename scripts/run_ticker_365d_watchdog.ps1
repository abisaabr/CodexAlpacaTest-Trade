$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$LogDir = Join-Path $RepoRoot "logs"
$LogPath = Join-Path $LogDir "ticker_365d_watchdog.log"
$Python = "C:\Users\rabisaab\AppData\Local\Programs\Python\Python312\python.exe"
$Gcloud = "C:\Users\rabisaab\Downloads\google-cloud-sdk-local\google-cloud-sdk\bin\gcloud.cmd"

New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
Set-Location $RepoRoot

$env:CLOUDSDK_PYTHON = $Python
$env:GOOGLE_CLOUD_PROJECT = "codexalpaca"
$KeyPath = "C:\Users\rabisaab\Downloads\codexalpaca-7bcb9ac9a02d.json"
if (Test-Path $KeyPath) {
    $env:GOOGLE_APPLICATION_CREDENTIALS = $KeyPath
}

$stamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
Add-Content -Path $LogPath -Value "===== ticker_365d_watchdog run $stamp ====="

& $Python "scripts\watch_ticker_365d_wave.py" --gcloud $Gcloud 2>&1 |
    Tee-Object -FilePath $LogPath -Append

exit $LASTEXITCODE
