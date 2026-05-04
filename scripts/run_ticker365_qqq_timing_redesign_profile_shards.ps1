$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$LogDir = Join-Path $RepoRoot "logs"
$LogPath = Join-Path $LogDir "ticker365_qqq_timing_redesign_profile_shards.log"
$Python = "C:\Users\rabisaab\AppData\Local\Programs\Python\Python312\python.exe"
$Gcloud = "C:\Users\rabisaab\Downloads\google-cloud-sdk-local\google-cloud-sdk\bin\gcloud.cmd"
$Project = "codexalpaca"
$ServiceAccount = "ramzi-service-account@codexalpaca.iam.gserviceaccount.com"
$WaveId = "ticker365_qqq_timing_redesign_20260504T1245Z"
$GcsPrefix = "gs://codexalpaca-control-us/research_results/ticker365_qqq_timing_redesign_20260504T1245Z"
$SourceArchiveUri = "$GcsPrefix/inputs/source/codexalpaca_repo_source.tar.gz"
$InputVariantsUri = "$GcsPrefix/inputs/qqq_timing_redesign_variants.jsonl"
$InputQueueUri = "$GcsPrefix/inputs/qqq_timing_redesign_option_queue.json"
$StockUri = "gs://codexalpaca-data-us/research_stock_data/qqq_365d_next_trading_day_5x5_20260428/stock_ref_silver/stock_bars/"
$ContractsUri = "gs://codexalpaca-control-us/research_results/qqq_365d_next_trading_day_5x5_20260428/research_wave/qqq_365d_next_trading_day_5x5_20260428/dense_universe/selected_option_contracts/"
$BarsUri = "gs://codexalpaca-data-us/research_option_data/qqq_365d_next_trading_day_5x5_20260428/option_bars_silver/option_bars/underlying=QQQ/"
$StartupScript = Join-Path $RepoRoot "scripts\gcp_single_ticker_365d_shard.sh"
$Selectors = @("nearest_contract", "entry_liquidity_first_research_only")
$SelectorMetadata = ($Selectors -join ";")
$FallbackZones = @("us-east1-b", "us-central1-a", "us-west1-a", "us-east4-a")

$Shards = @(
    [PSCustomObject]@{ Name = "e0x60"; LagProfile = "0:60"; PreferredZone = "us-east1-b" },
    [PSCustomObject]@{ Name = "e15x120"; LagProfile = "15:120"; PreferredZone = "us-central1-a" },
    [PSCustomObject]@{ Name = "e30x180"; LagProfile = "30:180"; PreferredZone = "us-west1-a" },
    [PSCustomObject]@{ Name = "e60x240"; LagProfile = "60:240"; PreferredZone = "us-east4-a" },
    [PSCustomObject]@{ Name = "e120x390"; LagProfile = "120:390"; PreferredZone = "us-east1-b" }
)

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

function Test-ShardComplete {
    param([string]$WorkerId, [string]$ShardName)
    $entry = ($ShardName -replace "^e", "").Split("x")[0]
    $exit = ($ShardName -replace "^e", "").Split("x")[1]
    foreach ($selector in $Selectors) {
        $selectorSlug = ($selector -replace "[^A-Za-z0-9]", "_")
        $runId = "${WorkerId}_qqq_e${entry}_x${exit}_${selectorSlug}"
        $summaryUri = "$GcsPrefix/workers/$WorkerId/reports/research_wave/$runId/option_aware_candidate_summary.json"
        if (-not (Test-GcsObject $summaryUri)) {
            return $false
        }
    }
    return $true
}

function Remove-CompletedShardInstance {
    param([string]$InstanceName)
    foreach ($row in Get-InstanceRows $InstanceName) {
        $parts = $row.Split(",")
        if ($parts.Count -lt 3) {
            continue
        }
        $zone = $parts[1]
        $status = $parts[2]
        if ($status -eq "TERMINATED") {
            Add-LogLine "deleting_completed_profile_shard instance=$InstanceName zone=$zone"
            Invoke-GcloudLogged @("compute", "instances", "delete", $InstanceName, "--project", $Project, "--zone", $zone, "--quiet") | Out-Null
        }
    }
}

function Start-ShardWorker {
    param([pscustomobject]$Shard)
    $instanceName = "ticker365-repair-qqq-$($Shard.Name)-20260504qs"
    $workerId = "ticker365fillrepair_qqq_$($Shard.Name)"

    if (Test-ShardComplete $workerId $Shard.Name) {
        Add-LogLine "profile_shard_complete worker_id=$workerId shard=$($Shard.Name)"
        Remove-CompletedShardInstance $instanceName
        return
    }

    $existingRows = Get-InstanceRows $instanceName
    if ($existingRows.Count -gt 0) {
        $activeRows = @()
        foreach ($row in $existingRows) {
            $parts = $row.Split(",")
            if ($parts.Count -lt 3) {
                continue
            }
            if ($parts[2] -ne "TERMINATED") {
                $activeRows += $row
            }
        }
        if ($activeRows.Count -gt 0) {
            Add-LogLine "profile_shard_instance_exists instance=$instanceName rows=$($activeRows -join ';')"
            return
        }
        foreach ($row in $existingRows) {
            $parts = $row.Split(",")
            if ($parts.Count -lt 3) {
                continue
            }
            Add-LogLine "deleting_incomplete_terminated_profile_shard instance=$instanceName zone=$($parts[1])"
            Invoke-GcloudLogged @("compute", "instances", "delete", $instanceName, "--project", $Project, "--zone", $parts[1], "--quiet") | Out-Null
        }
    }

    $metadata = @{
        symbol = "QQQ"
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
        top_n = "126"
        test_date_count = "20"
        allocation_fraction = "0.05"
        slippage_bps = "10"
        fee_per_contract = "0.65"
        selectors = $SelectorMetadata
        lag_profiles = $Shard.LagProfile
        entry_bar_lookup_mode = "first_bar_at_or_after_entry_within_lag"
        max_entry_staleness_minutes = "0"
    }
    $metadataArg = ConvertTo-MetadataArg $metadata
    $labels = "wave=ticker365-fillrepair,role=ticker365-repair,symbol=qqq,dataset=qqq-dense-365d"
    $zones = @($Shard.PreferredZone) + ($FallbackZones | Where-Object { $_ -ne $Shard.PreferredZone })

    foreach ($zone in $zones) {
        Add-LogLine "launching_profile_shard shard=$($Shard.Name) worker_id=$workerId instance=$instanceName zone=$zone"
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
            return
        }
        Add-LogLine "profile_shard_launch_failed shard=$($Shard.Name) instance=$instanceName zone=$zone exit_code=$exitCode"
    }
}

$stamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
Add-LogLine "===== ticker365_qqq_timing_redesign_profile_shards run $stamp ====="
foreach ($shard in $Shards) {
    Start-ShardWorker $shard
}

& $Python "scripts\watch_ticker365_fill_repair_wave.py" `
    --gcloud $Gcloud `
    --wave-id $WaveId `
    --gcs-prefix $GcsPrefix `
    --source-archive-uri $SourceArchiveUri `
    --input-variants-uri $InputVariantsUri `
    --input-queue-uri $InputQueueUri `
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
    --max-launches-per-run 0 `
    --no-refresh-inputs 2>&1 |
    ForEach-Object {
        $line = $_.ToString()
        Write-Output $line
        Add-LogLine $line
    }
