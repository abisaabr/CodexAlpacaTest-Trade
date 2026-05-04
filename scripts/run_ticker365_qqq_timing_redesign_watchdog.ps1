$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$LogDir = Join-Path $RepoRoot "logs"
$LogPath = Join-Path $LogDir "ticker365_qqq_timing_redesign_watchdog.log"
$LockPath = Join-Path $LogDir "ticker365_qqq_timing_redesign_watchdog.lock"
$Python = "C:\Users\rabisaab\AppData\Local\Programs\Python\Python312\python.exe"
$Gcloud = "C:\Users\rabisaab\Downloads\google-cloud-sdk-local\google-cloud-sdk\bin\gcloud.cmd"
$WaveId = "ticker365_qqq_timing_redesign_20260504T1245Z"
$GcsPrefix = "gs://codexalpaca-control-us/research_results/ticker365_qqq_timing_redesign_20260504T1245Z"
$InputDir = Join-Path $RepoRoot "docs\gcp_research\qqq_timing_redesign_20260504\inputs"

New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
Set-Location $RepoRoot

try {
    $LockHandle = [System.IO.File]::Open($LockPath, [System.IO.FileMode]::OpenOrCreate, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
} catch {
    $stamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    "===== ticker365_qqq_timing_redesign_watchdog skipped_locked $stamp =====" | Out-File -FilePath $LogPath -Append -Encoding utf8
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

try {
    $stamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    Add-WatchdogLogLine "===== ticker365_qqq_timing_redesign_watchdog run $stamp ====="

    & $Gcloud storage cp (Join-Path $InputDir "qqq_timing_redesign_variants.jsonl") "$GcsPrefix/inputs/qqq_timing_redesign_variants.jsonl" 2>&1 |
        ForEach-Object { $line = $_.ToString(); Write-Output $line; Add-WatchdogLogLine $line }
    & $Gcloud storage cp (Join-Path $InputDir "qqq_timing_redesign_option_queue.json") "$GcsPrefix/inputs/qqq_timing_redesign_option_queue.json" 2>&1 |
        ForEach-Object { $line = $_.ToString(); Write-Output $line; Add-WatchdogLogLine $line }
    & $Gcloud storage cp (Join-Path $InputDir "qqq_timing_redesign_launch_rows.json") "$GcsPrefix/inputs/qqq_timing_redesign_launch_rows.json" 2>&1 |
        ForEach-Object { $line = $_.ToString(); Write-Output $line; Add-WatchdogLogLine $line }

    & $Python "scripts\watch_ticker365_fill_repair_wave.py" `
        --gcloud $Gcloud `
        --wave-id $WaveId `
        --gcs-prefix $GcsPrefix `
        --source-archive-uri "$GcsPrefix/inputs/source/codexalpaca_repo_source.tar.gz" `
        --input-variants-uri "$GcsPrefix/inputs/qqq_timing_redesign_variants.jsonl" `
        --input-queue-uri "$GcsPrefix/inputs/qqq_timing_redesign_option_queue.json" `
        --launch-rows-uri "$GcsPrefix/inputs/qqq_timing_redesign_launch_rows.json" `
        --instance-suffix "20260504q" `
        --top-n 126 `
        --lag-profiles "0:60,15:120,30:180,60:240,120:390" `
        --selectors "nearest_contract,entry_liquidity_first_research_only" `
        --entry-bar-lookup-mode "first_bar_at_or_after_entry_within_lag" `
        --max-entry-staleness-minutes 0 `
        --fallback-zones "us-east1-b,us-central1-a,us-west1-a,us-east4-a" `
        --aggregate-fallback-zones "us-east1-b,us-central1-a,us-west1-a,us-east4-a" `
        --prefer-fallback-zones `
        --delete-completed-worker-instances `
        --max-launches-per-run 1 2>&1 |
        ForEach-Object {
            $line = $_.ToString()
            Write-Output $line
            Add-WatchdogLogLine $line
        }
    exit $LASTEXITCODE
} finally {
    if ($null -ne $LockHandle) {
        $LockHandle.Close()
    }
}
