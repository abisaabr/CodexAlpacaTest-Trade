param(
    [Parameter(Mandatory = $true)]
    [string]$WaveId,
    [string[]]$Symbols = @("QQQ", "SPY", "IWM"),
    [string]$InstanceSuffix = "20260507g1",
    [int]$StartCandidateIndex = 1,
    [int]$CandidateCountPerWorker = 28,
    [int]$MaxLaunchesPerSymbol = 1,
    [string]$MachineType = "e2-standard-2",
    [string]$LagProfiles = "0:60,10:60,30:120",
    [string]$Selectors = "entry_delta_target_research_only",
    [switch]$PrepareOnly
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Project = "codexalpaca"
$ServiceAccount = "ramzi-service-account@codexalpaca.iam.gserviceaccount.com"
$StartupScript = Join-Path $RepoRoot "scripts\gcp_single_ticker_365d_shard.sh"
$GcsPrefix = "gs://codexalpaca-control-us/research_results/$WaveId"
$SourceArchiveUri = "$GcsPrefix/inputs/source/codexalpaca_repo_source.tar.gz"
$InputVariantsUri = "$GcsPrefix/inputs/greek_strategy_variants.jsonl"
$InputQueueUri = "$GcsPrefix/inputs/greek_strategy_option_queue.json"
$ReportDir = Join-Path $RepoRoot "reports\gcp_research\$WaveId"
$InputsDir = Join-Path $ReportDir "inputs"
$LaunchRowsPath = Join-Path $ReportDir "greek_strategy_launch_rows.json"
$SourceArchivePath = Join-Path $env:TEMP "$WaveId-codexalpaca_repo_source.tar.gz"
$Zones = @("us-central1-a", "us-west1-a", "us-east4-a", "us-east1-b")

Set-Location $RepoRoot
New-Item -ItemType Directory -Path $InputsDir -Force | Out-Null
$env:GOOGLE_CLOUD_PROJECT = $Project

function Invoke-Gcloud {
    param([string[]]$Arguments)
    & gcloud @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "gcloud failed: $($Arguments -join ' ')"
    }
}

function Invoke-GcloudCreateInstance {
    param([string[]]$Arguments)
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = & gcloud @Arguments 2>&1
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousPreference
    }
    $output | ForEach-Object { Write-Host $_ }
    if ($exitCode -eq 0) {
        return "created"
    }
    $message = ($output | Out-String)
    if ($message -match "CPUS_ALL_REGIONS" -or $message -match "Quota .* exceeded") {
        Write-Host "capacity_or_quota_limit_reached=true"
        return "quota_limited"
    }
    if ($message -match "ZONE_RESOURCE_POOL_EXHAUSTED" -or $message -match "RESOURCE_POOL_EXHAUSTED" -or $message -match "IN_USE_ADDRESSES") {
        Write-Host "zone_capacity_limit_reached=true"
        return "zone_capacity_limited"
    }
    throw "gcloud failed: $($Arguments -join ' ')"
}

function ConvertTo-MetadataArg {
    param([hashtable]$Metadata)
    return (($Metadata.GetEnumerator() | Sort-Object Name | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join ",")
}

function Get-DatasetUris {
    param([string]$Symbol)
    $Symbol = $Symbol.ToUpperInvariant()
    if ($Symbol -eq "QQQ") {
        $Root = "option_fill_ladder_next10_20260429"
    } else {
        $Root = "option_fill_ladder_20260429"
    }
    return @{
        Stock = "gs://codexalpaca-data-us/research_stock_data/$Root/$Symbol/365d_5x5/stock_ref_silver/stock_bars/"
        Contracts = "gs://codexalpaca-control-us/research_results/$Root/$Symbol/365d_5x5/research_wave/dense_universe/selected_option_contracts/"
        Bars = "gs://codexalpaca-data-us/research_option_data/$Root/$Symbol/365d_5x5/option_bars_silver/option_bars/"
    }
}

$SelectedSymbols = @(
    $Symbols |
        ForEach-Object { $_ -split "," } |
        ForEach-Object { $_.Trim().ToUpperInvariant() } |
        Where-Object { $_ }
)
if ($SelectedSymbols.Count -eq 0) {
    throw "No symbols selected"
}

python scripts\build_greek_strategy_research_inputs.py `
    --symbols ($SelectedSymbols -join ",") `
    --wave-id $WaveId `
    --output-dir $InputsDir
if ($LASTEXITCODE -ne 0) {
    throw "Greek input builder failed"
}

$QueuePath = Join-Path $InputsDir "greek_strategy_option_queue.json"
$VariantPath = Join-Path $InputsDir "greek_strategy_variants.jsonl"
$ManifestPath = Join-Path $InputsDir "greek_strategy_manifest.json"
$Queue = Get-Content -Raw -Path $QueuePath | ConvertFrom-Json
$CountsBySymbol = @{}
foreach ($item in $Queue.queue_items) {
    $symbol = [string]$item.symbol
    if (-not $CountsBySymbol.ContainsKey($symbol)) {
        $CountsBySymbol[$symbol] = 0
    }
    $CountsBySymbol[$symbol] += 1
}

git archive --format=tar.gz --output $SourceArchivePath HEAD
Invoke-Gcloud @("storage", "cp", $SourceArchivePath, $SourceArchiveUri, "--project", $Project)
Invoke-Gcloud @("storage", "cp", $VariantPath, $InputVariantsUri, "--project", $Project)
Invoke-Gcloud @("storage", "cp", $QueuePath, $InputQueueUri, "--project", $Project)
Invoke-Gcloud @("storage", "cp", $ManifestPath, "$GcsPrefix/inputs/greek_strategy_manifest.json", "--project", $Project)

$launchRows = @()
foreach ($Symbol in $SelectedSymbols) {
    $totalCandidates = [int]$CountsBySymbol[$Symbol]
    for ($candidateStart = $StartCandidateIndex; $candidateStart -le $totalCandidates; $candidateStart += $CandidateCountPerWorker) {
        $candidateEnd = [Math]::Min($candidateStart + $CandidateCountPerWorker - 1, $totalCandidates)
        $startSlug = "{0:D3}" -f $candidateStart
        $endSlug = "{0:D3}" -f $candidateEnd
        $symbolSlug = $Symbol.ToLowerInvariant()
        $launchRows += [PSCustomObject]@{
            broker_facing = $false
            candidate_count = ($candidateEnd - $candidateStart + 1)
            candidate_start_index = $candidateStart
            gcs_prefix = $GcsPrefix
            instance_name = "$symbolSlug-greek-c$startSlug-$endSlug-$InstanceSuffix"
            live_manifest_effect = "none"
            risk_policy_effect = "none"
            symbol = $Symbol
            top_n = $totalCandidates
            worker_id = "$symbolSlug`_greek_c${startSlug}_${endSlug}"
            zone = $Zones[$launchRows.Count % $Zones.Count]
        }
    }
}
$launchRows | ConvertTo-Json -Depth 5 | Set-Content -Path $LaunchRowsPath -Encoding utf8
Invoke-Gcloud @("storage", "cp", $LaunchRowsPath, "$GcsPrefix/ops/greek_strategy_launch_rows.json", "--project", $Project)

Write-Output "wave_id=$WaveId"
Write-Output "symbols=$($SelectedSymbols -join ',')"
Write-Output "launch_rows=$LaunchRowsPath"
Write-Output "gcs_prefix=$GcsPrefix"
Write-Output "research_only=true"
Write-Output "selector=$Selectors"
if ($PrepareOnly) {
    exit 0
}

$metadataSelectors = $Selectors.Replace(",", ";")
$metadataLagProfiles = $LagProfiles.Replace(",", ";")
foreach ($Symbol in $SelectedSymbols) {
    $symbolRows = @($launchRows | Where-Object { $_.symbol -eq $Symbol } | Select-Object -First $MaxLaunchesPerSymbol)
    foreach ($row in $symbolRows) {
        $Uris = Get-DatasetUris $Symbol
        $metadata = @{
            allocation_fraction = "0.05"
            bars_uri = $Uris.Bars
            candidate_count = "$($row.candidate_count)"
            candidate_selection_mode = "priority_order"
            candidate_start_index = "$($row.candidate_start_index)"
            contracts_uri = $Uris.Contracts
            entry_bar_lookup_mode = "first_bar_at_or_after_entry_within_lag"
            exit_bar_lookup_mode = "first_bar_at_or_after_exit_within_lag"
            fee_per_contract = "0.65"
            gcs_prefix = $GcsPrefix
            initial_cash = "25000"
            input_queue_uri = $InputQueueUri
            input_variants_uri = $InputVariantsUri
            lag_profiles = $metadataLagProfiles
            max_entry_staleness_minutes = "0"
            profile_name = "$($Symbol.ToLowerInvariant())-greek-delta-target"
            regime_balance_order = "bull;bear;choppy;unclassified"
            selectors = $metadataSelectors
            slippage_bps = "10"
            source_archive_uri = $SourceArchiveUri
            stock_session_filter = "option_rth_same_day"
            stock_uri = $Uris.Stock
            symbol = $Symbol
            test_date_count = "20"
            top_n = "$($row.top_n)"
            wave_id = $WaveId
            worker_id = $row.worker_id
        }
        $created = $false
        $candidateZones = @($row.zone) + @($Zones | Where-Object { $_ -ne $row.zone })
        foreach ($zone in $candidateZones) {
            $status = Invoke-GcloudCreateInstance @(
                "compute", "instances", "create", $row.instance_name,
                "--project", $Project,
                "--zone", $zone,
                "--machine-type", $MachineType,
                "--image-family", "debian-12",
                "--image-project", "debian-cloud",
                "--boot-disk-size", "120GB",
                "--boot-disk-type", "pd-standard",
                "--service-account", $ServiceAccount,
                "--scopes", "https://www.googleapis.com/auth/cloud-platform",
                "--labels", "app=codexalpaca,env=research,role=greek-strategy,wave=greek-strategy,symbol=$($Symbol.ToLowerInvariant())",
                "--metadata", (ConvertTo-MetadataArg $metadata),
                "--metadata-from-file", "startup-script=$StartupScript",
                "--quiet"
            )
            if ($status -eq "created") {
                Write-Output "launched_instance=$($row.instance_name) symbol=$Symbol candidate_start=$($row.candidate_start_index) candidate_count=$($row.candidate_count) zone=$zone"
                $created = $true
                break
            }
            if ($status -eq "quota_limited") {
                break
            }
        }
        if (-not $created) {
            Write-Output "launch_deferred=$($row.instance_name)"
        }
    }
}
