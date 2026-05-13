param(
    [int]$MaxLaunches = 8,
    [switch]$PrepareOnly,
    [switch]$RunBothSelectors
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$LogDir = Join-Path $RepoRoot "logs"
$LogPath = Join-Path $LogDir "ticker365_qqq_fill_squash_full126_expansion.log"
$Python = "C:\Users\rabisaab\AppData\Local\Programs\Python\Python312\python.exe"
$Gcloud = "C:\Users\rabisaab\Downloads\google-cloud-sdk-local\google-cloud-sdk\bin\gcloud.cmd"
$Project = "codexalpaca"
$ServiceAccount = "ramzi-service-account@codexalpaca.iam.gserviceaccount.com"
$MicroWaveId = "ticker365_qqq_fill_squash_micro_20260504T1730Z"
$MicroGcsPrefix = "gs://codexalpaca-control-us/research_results/$MicroWaveId"
$WaveId = "ticker365_qqq_fill_squash_full126_20260504T1900Z"
$InstanceSuffix = "20260504qx"
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
$HeatmapDir = Join-Path $RepoRoot "reports\gcp_research\qqq_fill_squash_micro_20260504"
$HeatmapCsv = Join-Path $HeatmapDir "qqq_fill_squash_heatmap.csv"
$AllSelectors = @("nearest_contract", "entry_liquidity_first_research_only")
$Selectors = @()
$SelectorMetadata = ""
$SelectorsCsv = ""
$FallbackZones = @("us-east1-b", "us-central1-a", "us-west1-a", "us-east4-a")
$TopN = 126
$ChunkSize = 7
$ExpectedSummaryCount = 0
$MachineType = "e2-standard-2"
$MachineCpu = 2
$LaunchMutexName = "Global\CodexAlpacaTicker365QQQFillSquashFull126Expansion"

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

function Get-CandidateChunks {
    $chunks = @()
    for ($start = 1; $start -le $TopN; $start += $ChunkSize) {
        $end = [Math]::Min($start + $ChunkSize - 1, $TopN)
        $chunks += [PSCustomObject]@{
            Name = ("c{0:D3}-{1:D3}" -f $start, $end)
            Start = $start
            Count = ($end - $start + 1)
        }
    }
    return $chunks
}

function Set-ActiveSelectors {
    param([string[]]$SelectedSelectors)
    $script:Selectors = @($SelectedSelectors | Where-Object { $_ -and $_.Trim() })
    if ($script:Selectors.Count -eq 0) {
        throw "No active selectors were selected for full126 expansion"
    }
    $script:SelectorMetadata = ($script:Selectors -join ";")
    $script:SelectorsCsv = ($script:Selectors -join ",")
    $script:ExpectedSummaryCount = (Get-CandidateChunks).Count * $script:Selectors.Count
    Add-LogLine "full126_active_selectors selectors=$script:SelectorsCsv expected_summary_count=$script:ExpectedSummaryCount"
}

function Get-StrictProfileMap {
    return @{
        strict_e0x60 = [PSCustomObject]@{
            Name = "strict-e0x60"
            Slug = "strict_e0x60"
            LagProfile = "0:60"
            EntryMode = "first_bar_at_or_after_entry_within_lag"
            EntryStaleness = "0"
            ExitMode = "first_bar_at_or_after_exit_within_lag"
            StockSessionFilter = "option_rth_same_day"
            PreferredZone = "us-east1-b"
        }
        strict_e15x120 = [PSCustomObject]@{
            Name = "strict-e15x120"
            Slug = "strict_e15x120"
            LagProfile = "15:120"
            EntryMode = "first_bar_at_or_after_entry_within_lag"
            EntryStaleness = "0"
            ExitMode = "first_bar_at_or_after_exit_within_lag"
            StockSessionFilter = "option_rth_same_day"
            PreferredZone = "us-central1-a"
        }
        strict_e30x180 = [PSCustomObject]@{
            Name = "strict-e30x180"
            Slug = "strict_e30x180"
            LagProfile = "30:180"
            EntryMode = "first_bar_at_or_after_entry_within_lag"
            EntryStaleness = "0"
            ExitMode = "first_bar_at_or_after_exit_within_lag"
            StockSessionFilter = "option_rth_same_day"
            PreferredZone = "us-east1-b"
        }
    }
}

function Select-BestStrictProfile {
    if (-not (Test-Path $HeatmapCsv)) {
        & $Python "scripts\build_qqq_fill_squash_heatmap.py" `
            --gcs-prefix $MicroGcsPrefix `
            --output-dir $HeatmapDir
    }
    $rows = Import-Csv $HeatmapCsv
    $strictRows = @(
        $rows | Where-Object {
            $_.profile -like "strict_*" -and
            $_.entry_lookup_mode -eq "first_bar_at_or_after_entry_within_lag" -and
            $_.exit_lookup_mode -eq "first_bar_at_or_after_exit_within_lag" -and
            $_.source_session_filter -eq "option_rth_same_day"
        }
    )
    if ($strictRows.Count -eq 0) {
        throw "No legitimate strict profile rows found in $HeatmapCsv"
    }
    $best = $strictRows | Sort-Object `
        @{ Expression = { [int]$_.research_gate_passes }; Descending = $true }, `
        @{ Expression = { [int]$_.fill_gate_passes }; Descending = $true }, `
        @{ Expression = { [double]$_.mean_fill }; Descending = $true }, `
        @{ Expression = { [double]$_.max_test_net_pnl }; Descending = $true }, `
        @{ Expression = { [double]$_.mean_net_pnl }; Descending = $true } |
        Select-Object -First 1
    $map = Get-StrictProfileMap
    if (-not $map.ContainsKey($best.profile)) {
        throw "Selected strict profile $($best.profile) has no expansion profile map"
    }
    $profile = $map[$best.profile]
    $selectedSelectors = @($best.contract_selection_method)
    if ($RunBothSelectors) {
        $selectedSelectors = @($AllSelectors)
    }
    Set-ActiveSelectors $selectedSelectors
    $selection = [PSCustomObject]@{
        generated_at_utc = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
        micro_wave_id = $MicroWaveId
        expansion_wave_id = $WaveId
        selected_profile = $profile.Name
        selected_profile_slug = $profile.Slug
        selected_micro_selector = $best.contract_selection_method
        selected_expansion_selectors = $Selectors
        run_both_selectors = [bool]$RunBothSelectors
        expected_summary_count = $ExpectedSummaryCount
        lag_profile = $profile.LagProfile
        entry_bar_lookup_mode = $profile.EntryMode
        max_entry_staleness_minutes = $profile.EntryStaleness
        exit_bar_lookup_mode = $profile.ExitMode
        source_session_filter = $profile.StockSessionFilter
        micro_candidate_rows = [int]$best.candidate_rows
        micro_unique_candidates = [int]$best.unique_candidates
        micro_chunks = [int]$best.chunks
        micro_mean_fill = [double]$best.mean_fill
        micro_min_fill = [double]$best.min_fill
        micro_max_fill = [double]$best.max_fill
        micro_fill_gate_passes = [int]$best.fill_gate_passes
        micro_research_gate_passes = [int]$best.research_gate_passes
        micro_mean_test_net_pnl = [double]$best.mean_test_net_pnl
        micro_max_test_net_pnl = [double]$best.max_test_net_pnl
        hard_rules = @(
            "Do not start trading.",
            "Do not submit paper orders.",
            "Do not modify live manifests.",
            "Do not change risk policy.",
            "Do not lower fill_coverage >= 0.90.",
            "Expansion is research-only before promotion review."
        )
    }
    $selectionPath = Join-Path $HeatmapDir "qqq_fill_squash_selected_strict_profile.json"
    $selection | ConvertTo-Json -Depth 8 | Set-Content -Path $selectionPath -Encoding utf8
    Invoke-GcloudLogged @(
        "storage", "cp", $selectionPath,
        "$GcsPrefix/selection/qqq_fill_squash_selected_strict_profile.json",
        "--project", $Project
    ) | Out-Null
    return $profile
}

function Test-FullShardComplete {
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
            Add-LogLine "deleting_terminated_full126_shard instance=$InstanceName zone=$($parts[1])"
            Invoke-GcloudLogged @("compute", "instances", "delete", $InstanceName, "--project", $Project, "--zone", $parts[1], "--quiet") | Out-Null
        }
    }
}

function Sync-InputsAndSource {
    param([pscustomobject]$Profile)
    Add-LogLine "syncing_qqq_fill_squash_full126_inputs gcs_prefix=$GcsPrefix profile=$($Profile.Name)"
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
        --lag-profiles $Profile.LagProfile `
        --selectors $SelectorsCsv `
        --entry-bar-lookup-mode $Profile.EntryMode `
        --exit-bar-lookup-mode $Profile.ExitMode `
        --stock-session-filter $Profile.StockSessionFilter `
        --max-entry-staleness-minutes ([double]$Profile.EntryStaleness) `
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

function Start-FullShardWorker {
    param([pscustomobject]$Profile, [pscustomobject]$Chunk)

    $chunkSlug = $Chunk.Name
    $workerChunk = ($chunkSlug -replace "-", "_")
    $instanceName = "qqqfull-$($Profile.Name)-$chunkSlug-$InstanceSuffix"
    $workerId = "qqqfillfull_qqq_$($Profile.Slug)_$workerChunk"

    if (Test-FullShardComplete $workerId $Profile.LagProfile) {
        Add-LogLine "full126_shard_complete worker_id=$workerId profile=$($Profile.Name) chunk=$chunkSlug"
        Remove-TerminatedInstance $instanceName
        return $false
    }

    $activeRows = @(Get-ActiveInstanceRows $instanceName)
    if ($activeRows.Count -gt 0) {
        Add-LogLine "full126_shard_instance_exists instance=$instanceName rows=$($activeRows -join ';')"
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
    $labels = "wave=qqq-fill-full,role=qqq-full126,symbol=qqq,dataset=qqq-dense-365d"
    $zones = @($Profile.PreferredZone) + ($FallbackZones | Where-Object { $_ -ne $Profile.PreferredZone })

    foreach ($zone in $zones) {
        $latestActiveRows = @(Get-ActiveInstanceRows $instanceName)
        if ($latestActiveRows.Count -gt 0) {
            Add-LogLine "full126_shard_instance_exists_after_recheck instance=$instanceName rows=$($latestActiveRows -join ';')"
            return $false
        }
        Add-LogLine "launching_full126_shard worker_id=$workerId instance=$instanceName zone=$zone profile=$($Profile.Name) chunk=$chunkSlug"
        $args = @(
            "compute", "instances", "create", $instanceName,
            "--project", $Project,
            "--zone", $zone,
            "--machine-type", $MachineType,
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
        Add-LogLine "full126_shard_launch_failed worker_id=$workerId instance=$instanceName zone=$zone exit_code=$exitCode"
    }
    return $false
}

$LaunchMutex = New-Object System.Threading.Mutex($false, $LaunchMutexName)
if (-not $LaunchMutex.WaitOne(0)) {
    Add-LogLine "full126_launch_skipped mutex_held=$LaunchMutexName"
    exit 0
}

try {
    $stamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    Add-LogLine "===== qqq_fill_squash_full126_expansion run $stamp max_launches=$MaxLaunches prepare_only=$PrepareOnly ====="
    $profile = Select-BestStrictProfile
    Sync-InputsAndSource $profile

    if (-not $PrepareOnly) {
        $launched = 0
        foreach ($chunk in Get-CandidateChunks) {
            if ($launched -ge $MaxLaunches) {
                break
            }
            $freeCpu = 32 - (Get-RunningCpuUsage)
            if ($freeCpu -lt $MachineCpu) {
                Add-LogLine "full126_shard_launch_paused quota_free_cpu=$freeCpu"
                break
            }
            if (Start-FullShardWorker $profile $chunk) {
                $launched += 1
            }
        }
        Add-LogLine "full126_shard_launch_pass_complete launched=$launched max_launches=$MaxLaunches"
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
        --lag-profiles $profile.LagProfile `
        --selectors $SelectorsCsv `
        --entry-bar-lookup-mode $profile.EntryMode `
        --exit-bar-lookup-mode $profile.ExitMode `
        --stock-session-filter $profile.StockSessionFilter `
        --max-entry-staleness-minutes ([double]$profile.EntryStaleness) `
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
