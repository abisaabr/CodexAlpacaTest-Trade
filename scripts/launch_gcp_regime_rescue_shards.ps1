param(
    [Parameter(Mandatory = $true)]
    [string]$Symbol,
    [Parameter(Mandatory = $true)]
    [string]$WaveId,
    [Parameter(Mandatory = $true)]
    [string]$StockUri,
    [Parameter(Mandatory = $true)]
    [string]$ContractsUri,
    [Parameter(Mandatory = $true)]
    [string]$BarsUri,
    [string]$InstanceSuffix = "20260505a",
    [int]$StartCandidateIndex = 1,
    [int]$CandidateCountPerWorker = 12,
    [int]$TotalCandidates = 0,
    [int]$TopN = 0,
    [int]$MaxLaunches = 8,
    [string[]]$Zones = @("us-central1-a", "us-west1-a", "us-east4-a", "us-east1-b"),
    [string]$MachineType = "e2-standard-2",
    [string]$Selectors = "entry_liquidity_first_research_only",
    [string]$LagProfiles = "0:60,10:60,30:120",
    [int]$MaxEntryStalenessMinutes = 0,
    [string]$TargetRegimes = "bear,choppy",
    [ValidateSet("baseline", "momentum_refine")]
    [string]$BullProfileSet = "baseline",
    [string]$ChoppyFamilies = "",
    [string]$ChoppySignalDelayBars = "0",
    [ValidateSet("rescue", "timewindow_refine", "timewindow_micro_exit", "timewindow_quality_filter")]
    [string]$ChoppyProfileSet = "rescue",
    [ValidateSet("rescue", "signal_window_refine")]
    [string]$BearProfileSet = "rescue",
    [ValidateSet("priority_order", "regime_balanced")]
    [string]$CandidateSelectionMode = "regime_balanced",
    [string]$RegimeBalanceOrder = "bear,choppy,bull,unclassified",
    [switch]$PrepareOnly
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Project = "codexalpaca"
$ServiceAccount = "ramzi-service-account@codexalpaca.iam.gserviceaccount.com"
$Symbol = $Symbol.ToUpperInvariant()
$SymbolSlug = $Symbol.ToLowerInvariant()
$OutputPrefix = "$SymbolSlug`_regime_rescue"
$GcsPrefix = "gs://codexalpaca-control-us/research_results/$WaveId"
$SourceArchiveUri = "$GcsPrefix/inputs/source/codexalpaca_repo_source.tar.gz"
$InputVariantsUri = "$GcsPrefix/inputs/$OutputPrefix`_variants.jsonl"
$InputQueueUri = "$GcsPrefix/inputs/$OutputPrefix`_option_queue.json"
$LaunchRowsUri = "$GcsPrefix/ops/$OutputPrefix`_launch_rows.json"
$StartupScript = Join-Path $RepoRoot "scripts\gcp_single_ticker_365d_shard.sh"
$ReportDir = Join-Path $RepoRoot "reports\gcp_research\$WaveId"
$InputsDir = Join-Path $ReportDir "inputs"
$LaunchRowsPath = Join-Path $ReportDir "$OutputPrefix`_launch_rows.json"
$SourceArchivePath = Join-Path $env:TEMP "$WaveId-codexalpaca_repo_source.tar.gz"
$VariantPath = Join-Path $InputsDir "$OutputPrefix`_variants.jsonl"
$QueuePath = Join-Path $InputsDir "$OutputPrefix`_option_queue.json"
$ManifestPath = Join-Path $InputsDir "$OutputPrefix`_manifest.json"
$MetadataSelectors = $Selectors.Replace(",", ";")
$MetadataLagProfiles = $LagProfiles.Replace(",", ";")
$MetadataRegimeBalanceOrder = $RegimeBalanceOrder.Replace(",", ";")

Set-Location $RepoRoot
New-Item -ItemType Directory -Path $InputsDir -Force | Out-Null
$env:GOOGLE_CLOUD_PROJECT = $Project
$Zones = @(
    $Zones |
        ForEach-Object { $_ -split "," } |
        ForEach-Object { $_.Trim() } |
        Where-Object { $_ }
)
if ($Zones.Count -eq 0) {
    throw "at least one zone is required"
}
if ($StartCandidateIndex -lt 1) {
    throw "StartCandidateIndex must be positive"
}

function Invoke-Gcloud {
    param([string[]]$Arguments)
    & gcloud @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "gcloud failed: $($Arguments -join ' ')"
    }
}

function Test-GcsObject {
    param([string]$Uri)
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & gcloud storage ls $Uri --project $Project *> $null
        return ($LASTEXITCODE -eq 0)
    } finally {
        $ErrorActionPreference = $previousPreference
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
    if ($message -match "IN_USE_ADDRESSES") {
        Write-Host "regional_address_capacity_limit_reached=true"
        Write-Host "regional_address_capacity_pause_reason=$($message.Trim() -replace '\s+', ' ')"
        return "zone_capacity_limited"
    }
    if ($message -match "CPUS_ALL_REGIONS" -or $message -match "Quota .* exceeded") {
        Write-Host "capacity_or_quota_limit_reached=true"
        Write-Host "capacity_or_quota_pause_reason=$($message.Trim() -replace '\s+', ' ')"
        return "quota_limited"
    }
    if ($message -match "ZONE_RESOURCE_POOL_EXHAUSTED" -or $message -match "RESOURCE_POOL_EXHAUSTED") {
        Write-Host "zone_capacity_limit_reached=true"
        Write-Host "zone_capacity_pause_reason=$($message.Trim() -replace '\s+', ' ')"
        return "zone_capacity_limited"
    }
    throw "gcloud failed: $($Arguments -join ' ')"
}

function ConvertTo-MetadataArg {
    param([hashtable]$Metadata)
    return (($Metadata.GetEnumerator() | Sort-Object Name | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join ",")
}

function Get-InstanceRows {
    param([string]$InstanceName)
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $rows = & gcloud compute instances list `
            --project $Project `
            --filter "name=$InstanceName" `
            --format "csv[no-heading](name,zone.basename(),status)" 2>$null
        if ($LASTEXITCODE -ne 0 -or $null -eq $rows) {
            return @()
        }
        return @($rows | Where-Object { $_ -and $_.Trim() })
    } finally {
        $ErrorActionPreference = $previousPreference
    }
}

function Test-ActiveInstance {
    param([string]$InstanceName)
    foreach ($row in Get-InstanceRows $InstanceName) {
        $parts = $row.Split(",")
        if ($parts.Count -ge 3 -and $parts[2] -ne "TERMINATED") {
            return $true
        }
    }
    return $false
}

function Remove-TerminatedInstance {
    param([string]$InstanceName)
    foreach ($row in Get-InstanceRows $InstanceName) {
        $parts = $row.Split(",")
        if ($parts.Count -ge 3 -and $parts[2] -eq "TERMINATED") {
            Invoke-Gcloud @("compute", "instances", "delete", $InstanceName, "--project", $Project, "--zone", $parts[1], "--quiet")
        }
    }
}

$builderArgs = @(
    "scripts\build_regime_rescue_research_inputs.py",
    "--symbol", $Symbol,
    "--wave-id", $WaveId,
    "--output-dir", $InputsDir,
    "--output-prefix", $OutputPrefix,
    "--target-regimes", $TargetRegimes,
    "--bull-profile-set", $BullProfileSet,
    "--choppy-signal-delay-bars", $ChoppySignalDelayBars,
    "--choppy-profile-set", $ChoppyProfileSet,
    "--bear-profile-set", $BearProfileSet
)
if ($ChoppyFamilies.Trim()) {
    $builderArgs += @("--choppy-families", $ChoppyFamilies)
}
& python @builderArgs
if ($LASTEXITCODE -ne 0) {
    throw "input builder failed"
}
if (-not (Test-Path $VariantPath) -or -not (Test-Path $QueuePath) -or -not (Test-Path $ManifestPath)) {
    throw "input builder did not create expected files"
}

$manifest = Get-Content -Raw -Path $ManifestPath | ConvertFrom-Json
if ($TotalCandidates -le 0) {
    $TotalCandidates = [int]$manifest.template_count
}
if ($TopN -le 0) {
    $TopN = $TotalCandidates
}
if ($StartCandidateIndex -gt $TotalCandidates) {
    throw "StartCandidateIndex must be between 1 and TotalCandidates"
}

if (Test-GcsObject $SourceArchiveUri) {
    Write-Output "source_archive_reused=$SourceArchiveUri"
} else {
    git archive --format=tar.gz --output $SourceArchivePath HEAD
    Invoke-Gcloud @("storage", "cp", $SourceArchivePath, $SourceArchiveUri, "--project", $Project)
}
Invoke-Gcloud @("storage", "cp", $VariantPath, $InputVariantsUri, "--project", $Project)
Invoke-Gcloud @("storage", "cp", $QueuePath, $InputQueueUri, "--project", $Project)
Invoke-Gcloud @("storage", "cp", $ManifestPath, "$GcsPrefix/inputs/$OutputPrefix`_manifest.json", "--project", $Project)

$launchRows = @()
for ($candidateStart = $StartCandidateIndex; $candidateStart -le $TotalCandidates; $candidateStart += $CandidateCountPerWorker) {
    $candidateEnd = [Math]::Min($candidateStart + $CandidateCountPerWorker - 1, $TotalCandidates)
    $zone = $Zones[(($launchRows.Count) % $Zones.Count)]
    $startSlug = "{0:D3}" -f $candidateStart
    $endSlug = "{0:D3}" -f $candidateEnd
    $workerId = "$SymbolSlug`_regime_rescue_c${startSlug}_${endSlug}"
    $instanceName = "$SymbolSlug-rescue-c$startSlug-$endSlug-$InstanceSuffix"
    $launchRows += [PSCustomObject]@{
        broker_facing = $false
        candidate_count = ($candidateEnd - $candidateStart + 1)
        candidate_selection_mode = $CandidateSelectionMode
        candidate_start_index = $candidateStart
        gcs_prefix = $GcsPrefix
        instance_name = $instanceName
        lag_profiles = $MetadataLagProfiles
        live_manifest_effect = "none"
        machine_type = $MachineType
        regime_balance_order = $MetadataRegimeBalanceOrder
        risk_policy_effect = "none"
        selectors = $MetadataSelectors
        symbol = $Symbol
        top_n = $TopN
        worker_id = $workerId
        zone = $zone
    }
}
$launchRows | ConvertTo-Json -Depth 5 | Set-Content -Path $LaunchRowsPath -Encoding utf8
Invoke-Gcloud @("storage", "cp", $LaunchRowsPath, $LaunchRowsUri, "--project", $Project)

if ($PrepareOnly) {
    Write-Output "prepared_wave=$WaveId"
    Write-Output "symbol=$Symbol"
    Write-Output "total_candidates=$TotalCandidates"
    Write-Output "launch_rows=$LaunchRowsPath"
    Write-Output "gcs_prefix=$GcsPrefix"
    exit 0
}

$launched = 0
foreach ($row in $launchRows) {
    if ($launched -ge $MaxLaunches) {
        break
    }
    Remove-TerminatedInstance $row.instance_name
    if (Test-ActiveInstance $row.instance_name) {
        Write-Output "instance_already_active=$($row.instance_name)"
        continue
    }

    $metadata = @{
        allocation_fraction = "0.05"
        bars_uri = $BarsUri
        bull_profile_set = $BullProfileSet
        candidate_count = "$($row.candidate_count)"
        candidate_selection_mode = "$($row.candidate_selection_mode)"
        candidate_start_index = "$($row.candidate_start_index)"
        contracts_uri = $ContractsUri
        entry_bar_lookup_mode = "first_bar_at_or_after_entry_within_lag"
        exit_bar_lookup_mode = "first_bar_at_or_after_exit_within_lag"
        fee_per_contract = "0.65"
        gcs_prefix = $GcsPrefix
        initial_cash = "25000"
        input_queue_uri = $InputQueueUri
        input_variants_uri = $InputVariantsUri
        lag_profiles = $MetadataLagProfiles
        max_entry_staleness_minutes = "$MaxEntryStalenessMinutes"
        profile_name = "$SymbolSlug-regime-rescue-$CandidateSelectionMode-e${MaxEntryStalenessMinutes}"
        regime_balance_order = $MetadataRegimeBalanceOrder
        selectors = $MetadataSelectors
        slippage_bps = "10"
        source_archive_uri = $SourceArchiveUri
        stock_session_filter = "option_rth_same_day"
        stock_uri = $StockUri
        symbol = $Symbol
        test_date_count = "20"
        top_n = "$($row.top_n)"
        wave_id = $WaveId
        worker_id = $row.worker_id
    }

    $candidateZones = @($row.zone) + @($Zones | Where-Object { $_ -ne $row.zone })
    $created = $false
    foreach ($candidateZone in $candidateZones) {
        $createStatus = Invoke-GcloudCreateInstance @(
            "compute", "instances", "create", $row.instance_name,
            "--project", $Project,
            "--zone", $candidateZone,
            "--machine-type", $MachineType,
            "--image-family", "debian-12",
            "--image-project", "debian-cloud",
            "--boot-disk-size", "160GB",
            "--boot-disk-type", "pd-standard",
            "--service-account", $ServiceAccount,
            "--scopes", "https://www.googleapis.com/auth/cloud-platform",
            "--labels", "app=codexalpaca,env=research,role=regime-rescue,wave=regime-rescue,symbol=$SymbolSlug",
            "--metadata", (ConvertTo-MetadataArg $metadata),
            "--metadata-from-file", "startup-script=$StartupScript",
            "--quiet"
        )
        if ($createStatus -eq "quota_limited") {
            $created = $false
            break
        }
        if ($createStatus -eq "zone_capacity_limited") {
            Write-Output "zone_retry_next candidate_start=$($row.candidate_start_index) failed_zone=$candidateZone"
            continue
        }
        $launched += 1
        $created = $true
        Write-Output "launched_instance=$($row.instance_name) candidate_start=$($row.candidate_start_index) candidate_count=$($row.candidate_count) zone=$candidateZone"
        break
    }
    if (-not $created -and $createStatus -eq "quota_limited") {
        break
    }
}

Write-Output "wave_id=$WaveId"
Write-Output "symbol=$Symbol"
Write-Output "gcs_prefix=$GcsPrefix"
Write-Output "start_candidate_index=$StartCandidateIndex"
Write-Output "total_candidates=$TotalCandidates"
Write-Output "launched=$launched"
