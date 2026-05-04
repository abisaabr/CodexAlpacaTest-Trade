param(
    [int]$MinMicroSummariesForExpansion = 72,
    [int]$MaxExpansionLaunches = 8
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$LogDir = Join-Path $RepoRoot "logs"
$LogPath = Join-Path $LogDir "ticker365_qqq_fill_squash_controller.log"
$Python = "C:\Users\rabisaab\AppData\Local\Programs\Python\Python312\python.exe"
$Gcloud = "C:\Users\rabisaab\Downloads\google-cloud-sdk-local\google-cloud-sdk\bin\gcloud.cmd"
$Project = "codexalpaca"
$MicroWaveId = "ticker365_qqq_fill_squash_micro_20260504T1730Z"
$MicroGcsPrefix = "gs://codexalpaca-control-us/research_results/$MicroWaveId"
$HeatmapDir = Join-Path $RepoRoot "reports\gcp_research\qqq_fill_squash_micro_20260504"
$ControllerStatusPath = Join-Path $HeatmapDir "qqq_fill_squash_controller_status.json"
$ExpansionScript = Join-Path $RepoRoot "scripts\run_ticker365_qqq_fill_squash_expand_best_profile.ps1"
$ControllerMutexName = "Global\CodexAlpacaTicker365QQQFillSquashController"

New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
New-Item -ItemType Directory -Path $HeatmapDir -Force | Out-Null
Set-Location $RepoRoot

$env:CLOUDSDK_PYTHON = $Python
$env:GOOGLE_CLOUD_PROJECT = $Project
$KeyPath = "C:\Users\rabisaab\Downloads\codexalpaca-7bcb9ac9a02d.json"
if (Test-Path $KeyPath) {
    $env:GOOGLE_APPLICATION_CREDENTIALS = $KeyPath
}

$Utf8NoBom = New-Object System.Text.UTF8Encoding $false
function Add-LogLine {
    param([string]$Line)
    [System.IO.File]::AppendAllText($LogPath, $Line + [Environment]::NewLine, $Utf8NoBom)
}

function Invoke-GcloudLogged {
    param([string[]]$Arguments)
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & $Gcloud @Arguments 2>&1 | ForEach-Object {
            $line = $_.ToString()
            [Console]::Out.WriteLine($line)
            Add-LogLine $line
        }
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousPreference
    }
    return [int]$exitCode
}

function Get-GcsObjectCount {
    param([string]$Uri)
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $rows = & $Gcloud storage ls --recursive $Uri --project $Project 2>$null
        if ($LASTEXITCODE -ne 0 -or $null -eq $rows) {
            return 0
        }
        return @($rows | Where-Object { $_ -and $_.Trim() }).Count
    } catch {
        return 0
    } finally {
        $ErrorActionPreference = $previousPreference
    }
}

function Get-RunningQqqFsCount {
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $rows = & $Gcloud compute instances list `
            --project $Project `
            --filter "name~'qqqfs-' AND status=RUNNING" `
            --format "value(name)" 2>$null
        if ($LASTEXITCODE -ne 0 -or $null -eq $rows) {
            return 0
        }
        return @($rows | Where-Object { $_ -and $_.Trim() }).Count
    } catch {
        return 0
    } finally {
        $ErrorActionPreference = $previousPreference
    }
}

$ControllerMutex = New-Object System.Threading.Mutex($false, $ControllerMutexName)
if (-not $ControllerMutex.WaitOne(0)) {
    Add-LogLine "controller_skipped mutex_held=$ControllerMutexName"
    exit 0
}

try {
    $stamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    Add-LogLine "===== qqq_fill_squash_controller run $stamp min_micro_summaries=$MinMicroSummariesForExpansion ====="

    $summaryCount = Get-GcsObjectCount "$MicroGcsPrefix/**/option_aware_candidate_summary.json"
    $progressCount = Get-GcsObjectCount "$MicroGcsPrefix/**/candidate_summary_progress.jsonl"
    $runningMicroCount = Get-RunningQqqFsCount

    & $Python "scripts\build_qqq_fill_squash_heatmap.py" `
        --gcs-prefix $MicroGcsPrefix `
        --output-dir $HeatmapDir

    Invoke-GcloudLogged @(
        "storage", "cp", "--recursive", $HeatmapDir,
        "$MicroGcsPrefix/heatmap_partial/",
        "--project", $Project
    ) | Out-Null

    $expanded = $false
    $nextAction = "continue_micro_wave"
    if ($summaryCount -ge $MinMicroSummariesForExpansion) {
        $nextAction = "launch_or_continue_full126_expansion"
        Add-LogLine "micro_summary_threshold_met summaries=$summaryCount launching_expansion"
        powershell.exe -NoProfile -ExecutionPolicy Bypass `
            -File $ExpansionScript `
            -MaxLaunches $MaxExpansionLaunches
        $expanded = $true
    } else {
        Add-LogLine "micro_summary_threshold_not_met summaries=$summaryCount threshold=$MinMicroSummariesForExpansion running_micro=$runningMicroCount"
    }

    $status = [PSCustomObject]@{
        generated_at_utc = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
        micro_wave_id = $MicroWaveId
        micro_gcs_prefix = $MicroGcsPrefix
        micro_summary_count = $summaryCount
        micro_progress_file_count = $progressCount
        running_micro_vm_count = $runningMicroCount
        min_micro_summaries_for_expansion = $MinMicroSummariesForExpansion
        expansion_triggered_this_run = $expanded
        next_action = $nextAction
        hard_rules = @(
            "Do not start trading.",
            "Do not submit paper orders.",
            "Do not modify live manifests.",
            "Do not change risk policy.",
            "Do not lower fill_coverage >= 0.90."
        )
    }
    $status | ConvertTo-Json -Depth 8 | Set-Content -Path $ControllerStatusPath -Encoding utf8
    Invoke-GcloudLogged @(
        "storage", "cp", $ControllerStatusPath,
        "$MicroGcsPrefix/controller/qqq_fill_squash_controller_status.json",
        "--project", $Project
    ) | Out-Null
} finally {
    $ControllerMutex.ReleaseMutex()
    $ControllerMutex.Dispose()
}
