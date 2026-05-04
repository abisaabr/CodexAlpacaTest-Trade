param(
    [int]$MaxLaunches = 8,
    [switch]$PrepareOnly
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$LogDir = Join-Path $RepoRoot "logs"
$LogPath = Join-Path $LogDir "ticker365_qqq_fill_squash_micro_shards.log"
$Python = "C:\Users\rabisaab\AppData\Local\Programs\Python\Python312\python.exe"
$Gcloud = "C:\Users\rabisaab\Downloads\google-cloud-sdk-local\google-cloud-sdk\bin\gcloud.cmd"
$Project = "codexalpaca"
$ServiceAccount = "ramzi-service-account@codexalpaca.iam.gserviceaccount.com"
$WaveId = "ticker365_qqq_fill_squash_micro_20260504T1730Z"
$InstanceSuffix = "20260504qs"
$GcsPrefix = "gs://codexalpaca-control-us/research_results/$WaveId"
$SourceArchiveUri = "$GcsPrefix/inputs/source/codexalpaca_repo_source.tar.gz"
$InputVariantsUri = "$GcsPrefix/inputs/qqq_timing_redesign_variants.jsonl"
$InputQueueUri = "$GcsPrefix/inputs/qqq_timing_redesign_option_queue.json"
$InputLaunchRowsUri = "$GcsPrefix/inputs/qqq_timing_redesign_launch_rows.json"
$StockUri = "gs://codexalpaca-data-us/research_stock_data/qqq_365d_next_trading_day_5x5_20260428/stock_ref_silver/stock_bars/"
$ContractsUri = "gs://codexalpaca-control-us/research_results/qqq_365d_next_trading_day_5x5_20260428/research_wave/qqq_365d_next_trading_day_5x5_20260428/dense_universe/selected_option_contracts/"
$BarsUri = "gs://codexalpaca-data-us/research_option_data/qqq_365d_next_trading_day_5x5_20260428/option_bars_silver/option_bars/underlying=QQQ/"
$StartupScript = Join-Path $RepoRoot "scripts\gcp_single_ticker_365d_shard.sh"
$InputDir = Join-Path $RepoRoot "docs\gcp_research\qqq_timing_redesign_20260504\inputs"
$Selectors = @("nearest_contract", "entry_liquidity_first_research_only")
$SelectorMetadata = ($Selectors -join ";")
$FallbackZones = @("us-east1-b", "us-central1-a", "us-west1-a", "us-east4-a")
$TopN = 42
$ExpectedSummaryCount = 72
$LaunchMutexName = "Global\CodexAlpacaTicker365QQQFillSquashMicroShardWatchdog"

$Profiles = @(
    [PSCustomObject]@{
        Name = "strict-e0x60"
        LagProfile = "0:60"
        EntryMode = "first_bar_at_or_after_entry_within_lag"
        EntryStaleness = "0"
        ExitMode = "first_bar_at_or_after_exit_within_lag"
        StockSessionFilter = "option_rth_same_day"
        PreferredZone = "us-east1-b"
    },
    [PSCustomObject]@{
        Name = "strict-e15x120"
        LagProfile = "15:120"
        EntryMode = "first_bar_at_or_after_entry_within_lag"
        EntryStaleness = "0"
        ExitMode = "first_bar_at_or_after_exit_within_lag"
        StockSessionFilter = "option_rth_same_day"
        PreferredZone = "us-central1-a"
    },
    [PSCustomObject]@{
        Name = "strict-e30x180"
        LagProfile = "30:180"
        EntryMode = "first_bar_at_or_after_entry_within_lag"
        EntryStaleness = "0"
        ExitMode = "first_bar_at_or_after_exit_within_lag"
        StockSessionFilter = "option_rth_same_day"
        PreferredZone = "us-west1-a"
    },
    [PSCustomObject]@{
        Name = "asof-e5s1-x120"
        LagProfile = "5:120"
        EntryMode = "first_bar_at_or_after_or_asof_entry_within_lag"
        EntryStaleness = "1"
        ExitMode = "first_bar_at_or_after_exit_within_lag"
        StockSessionFilter = "option_rth_same_day"
        PreferredZone = "us-east4-a"
    },
    [PSCustomObject]@{
        Name = "diag-priorexit-e15x120"
        LagProfile = "15:120"
        EntryMode = "first_bar_at_or_after_entry_within_lag"
        EntryStaleness = "0"
        ExitMode = "first_bar_at_or_after_or_prior_exit_within_lag"
        StockSessionFilter = "option_rth_same_day"
        PreferredZone = "us-east1-b"
    },
    [PSCustomObject]@{
        Name = "diag-nosession-e15x120"
        LagProfile = "15:120"
        EntryMode = "first_bar_at_or_after_entry_within_lag"
        EntryStaleness = "0"
        ExitMode = "first_bar_at_or_after_exit_within_lag"
        StockSessionFilter = "none"
        PreferredZone = "us-central1-a"
    }
)

$CandidateChunks = @(
    [PSCustomObject]@{ Name = "c001-007"; Start = 1; Count = 7 },
    [PSCustomObject]@{ Name = "c008-014"; Start = 8; Count = 7 },
    [PSCustomObject]@{ Name = "c015-021"; Start = 15; Count = 7 },
    [PSCustomObject]@{ Name = "c022-028"; Start = 22; Count = 7 },
    [PSCustomObject]@{ Name = "c029-035"; Start = 29; Count = 7 },
    [PSCustomObject]@{ Name = "c036-042"; Start = 36; Count = 7 }
)
$ProfileLagProfilesCsv = (($Profiles | ForEach-Object { $_.LagProfile } | Sort-Object -Unique) -join ",")
$SelectorsCsv = ($Selectors -join ",")

New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
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

function ConvertTo-MetadataArg {
    param([hashtable]$Metadata)
    return (($Metadata.GetEnumerator() | Sort-Object Name | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join ",")
}

function Test-GcsObject {
    param([string]$Uri)
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & $Gcloud storage ls $Uri --project $Project 2>$null | Out-Null
        return ($LASTEXITCODE -eq 0)
    } catch {
        return $false
    } finally {
        $ErrorActionPreference = $previousPreference
    }
}

function Get-InstanceRows {
    param([string]$InstanceName)
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $rows = & $Gcloud compute instances list `
            --project $Project `
            --filter "name=$InstanceName" `
            --format "csv[no-heading](name,zone.basename(),status)" 2>$null
        if ($LASTEXITCODE -ne 0 -or $null -eq $rows) {
            return @()
        }
        return @($rows | Where-Object { $_ -and $_.Trim() })
    } catch {
        return @()
    } finally {
        $ErrorActionPreference = $previousPreference
    }
}

function Get-ActiveInstanceRows {
    param([string]$InstanceName)
    $activeRows = @()
    foreach ($row in Get-InstanceRows $InstanceName) {
        $parts = $row.Split(",")
        if ($parts.Count -ge 3 -and $parts[2] -ne "TERMINATED") {
            $activeRows += $row
        }
    }
    return $activeRows
}

function Get-RunningCpuUsage {
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $rows = & $Gcloud compute instances list `
            --project $Project `
            --filter "status=RUNNING" `
            --format "csv[no-heading](machineType.basename())" 2>$null
        if ($LASTEXITCODE -ne 0 -or $null -eq $rows) {
            return 0
        }
        $total = 0
        foreach ($row in @($rows | Where-Object { $_ -and $_.Trim() })) {
            if ($row -match "-standard-(\d+)$" -or $row -match "-highmem-(\d+)$" -or $row -match "-highcpu-(\d+)$") {
                $total += [int]$Matches[1]
            } elseif ($row -match "-micro$" -or $row -match "-small$") {
                $total += 1
            } else {
                $total += 2
            }
        }
        return $total
    } catch {
        return 0
    } finally {
        $ErrorActionPreference = $previousPreference
    }
}

function Test-MicroShardComplete {
    param([string]$WorkerId, [string]$LagProfile)
    $entry = ($LagProfile).Split(":")[0]
    $exit = ($LagProfile).Split(":")[1]
    foreach ($selector in $Selectors) {
        $selectorSlug = ($selector -replace "[^A-Za-z0-9]", "_")
        $runId = "${WorkerId}_qqq_e${entry}_x${exit}_${selectorSlug}"
        $summaryUris = @(
            "$GcsPrefix/workers/$WorkerId/reports/research_wave/$runId/option_aware_candidate_summary.json",
            "$GcsPrefix/workers/$WorkerId/reports/research_wave/$runId/**/option_aware_candidate_summary.json"
        )
        $found = $false
        foreach ($summaryUri in $summaryUris) {
            if (Test-GcsObject $summaryUri) {
                $found = $true
                break
            }
        }
        if (-not $found) {
            return $false
        }
    }
    return $true
}

function Remove-TerminatedInstance {
    param([string]$InstanceName)
    foreach ($row in Get-InstanceRows $InstanceName) {
        $parts = $row.Split(",")
        if ($parts.Count -lt 3) {
            continue
        }
        if ($parts[2] -eq "TERMINATED") {
            Add-LogLine "deleting_terminated_micro_shard instance=$InstanceName zone=$($parts[1])"
            Invoke-GcloudLogged @("compute", "instances", "delete", $InstanceName, "--project", $Project, "--zone", $parts[1], "--quiet") | Out-Null
        }
    }
}

function Sync-InputsAndSource {
    Add-LogLine "syncing_qqq_fill_squash_micro_inputs gcs_prefix=$GcsPrefix"
    Invoke-GcloudLogged @("storage", "cp", (Join-Path $InputDir "qqq_timing_redesign_variants.jsonl"), $InputVariantsUri, "--project", $Project) | Out-Null
    Invoke-GcloudLogged @("storage", "cp", (Join-Path $InputDir "qqq_timing_redesign_option_queue.json"), $InputQueueUri, "--project", $Project) | Out-Null
    Invoke-GcloudLogged @("storage", "cp", (Join-Path $InputDir "qqq_timing_redesign_launch_rows.json"), $InputLaunchRowsUri, "--project", $Project) | Out-Null

    & $Python "scripts\watch_ticker365_fill_repair_wave.py" `
        --gcloud $Gcloud `
        --wave-id $WaveId `
        --gcs-prefix $GcsPrefix `
        --source-archive-uri $SourceArchiveUri `
        --input-variants-uri $InputVariantsUri `
        --input-queue-uri $InputQueueUri `
        --launch-rows-uri $InputLaunchRowsUri `
        --instance-suffix $InstanceSuffix `
        --top-n $TopN `
        --expected-summary-count-override $ExpectedSummaryCount `
        --lag-profiles $ProfileLagProfilesCsv `
        --selectors $SelectorsCsv `
        --entry-bar-lookup-mode "first_bar_at_or_after_entry_within_lag" `
        --exit-bar-lookup-mode "first_bar_at_or_after_exit_within_lag" `
        --stock-session-filter "option_rth_same_day" `
        --max-entry-staleness-minutes 0 `
        --fallback-zones ($FallbackZones -join ",") `
        --aggregate-fallback-zones ($FallbackZones -join ",") `
        --prefer-fallback-zones `
        --delete-completed-worker-instances `
        --max-launches-per-run 0 2>&1 |
        ForEach-Object {
            $line = $_.ToString()
            Write-Output $line
            Add-LogLine $line
        }
}

function Start-MicroShardWorker {
    param([pscustomobject]$Profile, [pscustomobject]$Chunk)

    $chunkSlug = $Chunk.Name
    $profileSlug = ($Profile.Name.ToLower() -replace "[^a-z0-9-]", "-")
    $workerChunk = ($chunkSlug -replace "-", "_")
    $workerProfile = ($Profile.Name.ToLower() -replace "[^a-z0-9]", "_")
    $instanceName = "qqqfs-$profileSlug-$chunkSlug-$InstanceSuffix"
    $workerId = "qqqfillsquash_qqq_${workerProfile}_${workerChunk}"

    if (Test-MicroShardComplete $workerId $Profile.LagProfile) {
        Add-LogLine "micro_shard_complete worker_id=$workerId profile=$($Profile.Name) chunk=$chunkSlug"
        Remove-TerminatedInstance $instanceName
        return $false
    }

    $activeRows = @(Get-ActiveInstanceRows $instanceName)
    if ($activeRows.Count -gt 0) {
        Add-LogLine "micro_shard_instance_exists instance=$instanceName rows=$($activeRows -join ';')"
        return $false
    }
    $existingRows = Get-InstanceRows $instanceName
    if ($existingRows.Count -gt 0) {
        Remove-TerminatedInstance $instanceName
    }

    $metadata = @{
        symbol = "QQQ"
        profile_name = $Profile.Name
        worker_id = $workerId
        wave_id = $WaveId
        gcs_prefix = $GcsPrefix
        source_archive_uri = $SourceArchiveUri
        input_variants_uri = $InputVariantsUri
        input_queue_uri = $InputQueueUri
        stock_uri = $StockUri
        contracts_uri = $ContractsUri
        bars_uri = $BarsUri
        initial_cash = "25000"
        top_n = "$TopN"
        candidate_start_index = "$($Chunk.Start)"
        candidate_count = "$($Chunk.Count)"
        test_date_count = "20"
        allocation_fraction = "0.05"
        slippage_bps = "10"
        fee_per_contract = "0.65"
        selectors = $SelectorMetadata
        lag_profiles = $Profile.LagProfile
        entry_bar_lookup_mode = $Profile.EntryMode
        max_entry_staleness_minutes = $Profile.EntryStaleness
        exit_bar_lookup_mode = $Profile.ExitMode
        stock_session_filter = $Profile.StockSessionFilter
    }
    $metadataArg = ConvertTo-MetadataArg $metadata
    $labels = "wave=qqq-fill-squash,role=qqq-micro,symbol=qqq,dataset=qqq-dense-365d"
    $zones = @($Profile.PreferredZone) + ($FallbackZones | Where-Object { $_ -ne $Profile.PreferredZone })

    foreach ($zone in $zones) {
        $latestActiveRows = @(Get-ActiveInstanceRows $instanceName)
        if ($latestActiveRows.Count -gt 0) {
            Add-LogLine "micro_shard_instance_exists_after_recheck instance=$instanceName rows=$($latestActiveRows -join ';')"
            return $false
        }
        Add-LogLine "launching_micro_shard worker_id=$workerId instance=$instanceName zone=$zone profile=$($Profile.Name) chunk=$chunkSlug"
        $args = @(
            "compute", "instances", "create", $instanceName,
            "--project", $Project,
            "--zone", $zone,
            "--machine-type", "e2-standard-4",
            "--image-family", "debian-12",
            "--image-project", "debian-cloud",
            "--service-account", $ServiceAccount,
            "--scopes", "cloud-platform",
            "--provisioning-model", "SPOT",
            "--instance-termination-action", "STOP",
            "--boot-disk-size", "200GB",
            "--boot-disk-type", "pd-standard",
            "--labels", $labels,
            "--metadata", $metadataArg,
            "--metadata-from-file", "startup-script=$StartupScript"
        )
        $exitCode = Invoke-GcloudLogged $args
        if ($exitCode -eq 0) {
            return $true
        }
        Add-LogLine "micro_shard_launch_failed worker_id=$workerId instance=$instanceName zone=$zone exit_code=$exitCode"
    }
    return $false
}

$LaunchMutex = New-Object System.Threading.Mutex($false, $LaunchMutexName)
if (-not $LaunchMutex.WaitOne(0)) {
    Add-LogLine "micro_shard_launch_skipped mutex_held=$LaunchMutexName"
    exit 0
}

try {
    $stamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    Add-LogLine "===== qqq_fill_squash_micro_shards run $stamp max_launches=$MaxLaunches prepare_only=$PrepareOnly ====="
    Sync-InputsAndSource

    if (-not $PrepareOnly) {
        $launched = 0
        foreach ($chunk in $CandidateChunks) {
            foreach ($profile in $Profiles) {
                if ($launched -ge $MaxLaunches) {
                    break
                }
                $freeCpu = 32 - (Get-RunningCpuUsage)
                if ($freeCpu -lt 4) {
                    Add-LogLine "micro_shard_launch_paused quota_free_cpu=$freeCpu"
                    break
                }
                if (Start-MicroShardWorker $profile $chunk) {
                    $launched += 1
                }
            }
            if ($launched -ge $MaxLaunches) {
                break
            }
        }
        Add-LogLine "micro_shard_launch_pass_complete launched=$launched max_launches=$MaxLaunches"
    }

    & $Python "scripts\watch_ticker365_fill_repair_wave.py" `
        --gcloud $Gcloud `
        --wave-id $WaveId `
        --gcs-prefix $GcsPrefix `
        --source-archive-uri $SourceArchiveUri `
        --input-variants-uri $InputVariantsUri `
        --input-queue-uri $InputQueueUri `
        --launch-rows-uri $InputLaunchRowsUri `
        --instance-suffix $InstanceSuffix `
        --top-n $TopN `
        --expected-summary-count-override $ExpectedSummaryCount `
        --lag-profiles $ProfileLagProfilesCsv `
        --selectors $SelectorsCsv `
        --entry-bar-lookup-mode "first_bar_at_or_after_entry_within_lag" `
        --exit-bar-lookup-mode "first_bar_at_or_after_exit_within_lag" `
        --stock-session-filter "option_rth_same_day" `
        --max-entry-staleness-minutes 0 `
        --fallback-zones ($FallbackZones -join ",") `
        --aggregate-fallback-zones ($FallbackZones -join ",") `
        --prefer-fallback-zones `
        --delete-completed-worker-instances `
        --max-launches-per-run 0 `
        --no-refresh-inputs 2>&1 |
        ForEach-Object {
            $line = $_.ToString()
            Write-Output $line
            Add-LogLine $line
        }
} finally {
    $LaunchMutex.ReleaseMutex()
    $LaunchMutex.Dispose()
}
