$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$LogDir = Join-Path $RepoRoot "logs"
$LogPath = Join-Path $LogDir "ticker365_timing_rescue_watchdog.log"
$LockPath = Join-Path $LogDir "ticker365_timing_rescue_watchdog.lock"
$Python = "C:\Users\rabisaab\AppData\Local\Programs\Python\Python312\python.exe"
$Gcloud = "C:\Users\rabisaab\Downloads\google-cloud-sdk-local\google-cloud-sdk\bin\gcloud.cmd"
$WaveId = "ticker365_timing_rescue_20260503T1906Z"
$GcsPrefix = "gs://codexalpaca-control-us/research_results/ticker365_timing_rescue_20260503T1906Z"

New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
Set-Location $RepoRoot

try {
    $LockHandle = [System.IO.File]::Open($LockPath, [System.IO.FileMode]::OpenOrCreate, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
} catch {
    $stamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    "===== ticker365_timing_rescue_watchdog skipped_locked $stamp =====" | Out-File -FilePath $LogPath -Append -Encoding utf8
    exit 0
}

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
Add-WatchdogLogLine "===== ticker365_timing_rescue_watchdog run $stamp ====="

& $Python "scripts\watch_ticker365_fill_repair_wave.py" `
    --gcloud $Gcloud `
    --wave-id $WaveId `
    --gcs-prefix $GcsPrefix `
    --source-archive-uri "$GcsPrefix/inputs/source/codexalpaca_repo_source.tar.gz" `
    --instance-suffix "20260503c" `
    --top-n 40 `
    --lag-profiles "30:120,60:180,120:240,180:390" `
    --selectors "nearest_contract,entry_liquidity_first_research_only" `
    --fallback-zones "us-central1-a,us-east1-b,us-west1-a,us-east4-a" `
    --aggregate-fallback-zones "us-central1-a,us-east1-b,us-west1-a,us-east4-a" `
    --prefer-fallback-zones `
    --delete-completed-worker-instances `
    --max-launches-per-run 8 2>&1 |
    ForEach-Object {
        $line = $_.ToString()
        Write-Output $line
        Add-WatchdogLogLine $line
    }

$LockHandle.Close()
exit $LASTEXITCODE
