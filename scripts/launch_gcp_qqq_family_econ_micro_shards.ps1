param(
    [string]$WaveId = "ticker365_qqq_family_econ_micro_20260505T0135Z",
    [string]$InstanceSuffix = "20260505a",
    [int[]]$CandidateIndices = @(6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
    [int]$MaxLaunches = 15,
    [int]$TopN = 20,
    [string[]]$Zones = @("us-east1-b", "us-central1-a", "us-west1-a", "us-east4-a"),
    [string]$MachineType = "e2-standard-2",
    [string]$VariantPath = "",
    [string]$QueuePath = "",
    [switch]$PrepareOnly
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Python = "C:\Users\rabisaab\AppData\Local\Programs\Python\Python312\python.exe"
$Gcloud = "C:\Users\rabisaab\Downloads\google-cloud-sdk-local\google-cloud-sdk\bin\gcloud.cmd"
$Project = "codexalpaca"
$ServiceAccount = "ramzi-service-account@codexalpaca.iam.gserviceaccount.com"
$KeyPath = "C:\Users\rabisaab\Downloads\codexalpaca-7bcb9ac9a02d.json"
$GcsPrefix = "gs://codexalpaca-control-us/research_results/$WaveId"
$SourceArchiveUri = "$GcsPrefix/inputs/source/codexalpaca_repo_source.tar.gz"
$InputVariantsUri = "$GcsPrefix/inputs/portfolio_overnight_variants.jsonl"
$InputQueueUri = "$GcsPrefix/inputs/portfolio_overnight_option_queue.json"
$LaunchRowsUri = "$GcsPrefix/inputs/qqq_family_econ_micro_launch_rows.json"
$StockUri = "gs://codexalpaca-data-us/research_stock_data/qqq_365d_next_trading_day_5x5_20260428/stock_ref_silver/stock_bars/"
$ContractsUri = "gs://codexalpaca-control-us/research_results/qqq_365d_next_trading_day_5x5_20260428/research_wave/qqq_365d_next_trading_day_5x5_20260428/dense_universe/selected_option_contracts/"
$BarsUri = "gs://codexalpaca-data-us/research_option_data/qqq_365d_next_trading_day_5x5_20260428/option_bars_silver/option_bars/underlying=QQQ/"
$StartupScript = Join-Path $RepoRoot "scripts\gcp_single_ticker_365d_shard.sh"
if (-not $VariantPath) {
    $VariantPath = Join-Path $RepoRoot "reports\gcp_research\portfolio_overnight_12h_20260501\inputs\portfolio_overnight_variants.jsonl"
}
if (-not $QueuePath) {
    $QueuePath = Join-Path $RepoRoot "reports\gcp_research\portfolio_overnight_12h_20260501\inputs\portfolio_overnight_option_queue.json"
}
$ReportDir = Join-Path $RepoRoot "reports\gcp_research\$WaveId"
$LaunchRowsPath = Join-Path $ReportDir "qqq_family_econ_micro_launch_rows.json"
$SourceArchivePath = Join-Path $env:TEMP "$WaveId-codexalpaca_repo_source.tar.gz"

Set-Location $RepoRoot
New-Item -ItemType Directory -Path $ReportDir -Force | Out-Null
$MaxCandidateIndex = ($CandidateIndices | Measure-Object -Maximum).Maximum
$EffectiveTopN = [Math]::Max($TopN, $MaxCandidateIndex)

$env:CLOUDSDK_PYTHON = $Python
$env:GOOGLE_CLOUD_PROJECT = $Project
if (Test-Path $KeyPath) {
    $env:GOOGLE_APPLICATION_CREDENTIALS = $KeyPath
}

function Invoke-Gcloud {
    param([string[]]$Arguments)
    & $Gcloud @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "gcloud failed: $($Arguments -join ' ')"
    }
}

function Invoke-GcloudCreateInstance {
    param([string[]]$Arguments)
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $output = & $Gcloud @Arguments 2>&1
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
        Write-Host "quota_limit_reached=true"
        Write-Host "quota_pause_reason=$($message.Trim() -replace '\s+', ' ')"
        return "quota_limited"
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
        $rows = & $Gcloud compute instances list `
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

function Get-ExistingInstanceZone {
    param([string]$InstanceName)
    foreach ($row in Get-InstanceRows $InstanceName) {
        $parts = $row.Split(",")
        if ($parts.Count -ge 3 -and $parts[2] -ne "TERMINATED") {
            return $parts[1]
        }
    }
    return $null
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

if (-not (Test-Path $VariantPath)) {
    throw "Missing variants input: $VariantPath"
}
if (-not (Test-Path $QueuePath)) {
    throw "Missing queue input: $QueuePath"
}

git archive --format=tar.gz --output $SourceArchivePath HEAD
Invoke-Gcloud @("storage", "cp", $SourceArchivePath, $SourceArchiveUri, "--project", $Project)
Invoke-Gcloud @("storage", "cp", $VariantPath, $InputVariantsUri, "--project", $Project)
Invoke-Gcloud @("storage", "cp", $QueuePath, $InputQueueUri, "--project", $Project)

$launchRows = @()
foreach ($candidateIndex in $CandidateIndices) {
    $zone = $Zones[(($candidateIndex - 6) % $Zones.Count)]
    $candidateSlug = "{0:D3}" -f $candidateIndex
    $workerId = "qqqfamilymicro_qqq_e0x60_c$candidateSlug"
    $instanceName = "qqqfam-micro-c$candidateSlug-$InstanceSuffix"
    $existingZone = Get-ExistingInstanceZone $instanceName
    if ($existingZone) {
        $zone = $existingZone
    }
    $launchRows += [PSCustomObject]@{
        broker_facing = $false
        candidate_count = 1
        candidate_start_index = $candidateIndex
        gcs_prefix = $GcsPrefix
        instance_name = $instanceName
        lag_profiles = "0:60"
        live_manifest_effect = "none"
        machine_type = $MachineType
        risk_policy_effect = "none"
        symbol = "QQQ"
        top_n = $EffectiveTopN
        worker_id = $workerId
        zone = $zone
    }
}
$launchRows | ConvertTo-Json -Depth 5 | Set-Content -Path $LaunchRowsPath -Encoding utf8
Invoke-Gcloud @("storage", "cp", $LaunchRowsPath, $LaunchRowsUri, "--project", $Project)

if ($PrepareOnly) {
    Write-Output "prepared_wave=$WaveId"
    Write-Output "launch_rows=$LaunchRowsPath"
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
        candidate_count = "$($row.candidate_count)"
        candidate_start_index = "$($row.candidate_start_index)"
        contracts_uri = $ContractsUri
        entry_bar_lookup_mode = "first_bar_at_or_after_entry_within_lag"
        exit_bar_lookup_mode = "first_bar_at_or_after_exit_within_lag"
        fee_per_contract = "0.65"
        gcs_prefix = $GcsPrefix
        initial_cash = "25000"
        input_queue_uri = $InputQueueUri
        input_variants_uri = $InputVariantsUri
        lag_profiles = "0:60"
        max_entry_staleness_minutes = "0"
        profile_name = "family-aware-econ-micro-strict-e0x60"
        selectors = "entry_liquidity_first_research_only"
        slippage_bps = "10"
        source_archive_uri = $SourceArchiveUri
        stock_session_filter = "option_rth_same_day"
        stock_uri = $StockUri
        symbol = "QQQ"
        test_date_count = "20"
        top_n = "$EffectiveTopN"
        wave_id = $WaveId
        worker_id = $row.worker_id
    }

    $createStatus = Invoke-GcloudCreateInstance @(
        "compute", "instances", "create", $row.instance_name,
        "--project", $Project,
        "--zone", $row.zone,
        "--machine-type", $MachineType,
        "--image-family", "debian-12",
        "--image-project", "debian-cloud",
        "--boot-disk-size", "160GB",
        "--service-account", $ServiceAccount,
        "--scopes", "https://www.googleapis.com/auth/cloud-platform",
        "--metadata", (ConvertTo-MetadataArg $metadata),
        "--metadata-from-file", "startup-script=$StartupScript",
        "--quiet"
    )
    if ($createStatus -eq "quota_limited") {
        break
    }
    $launched += 1
    Write-Output "launched_instance=$($row.instance_name) candidate_index=$($row.candidate_start_index) zone=$($row.zone)"
}

Write-Output "wave_id=$WaveId"
Write-Output "gcs_prefix=$GcsPrefix"
Write-Output "launched=$launched"
