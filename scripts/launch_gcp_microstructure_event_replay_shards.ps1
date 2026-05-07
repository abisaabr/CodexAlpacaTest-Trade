param(
    [string]$WaveId = "microstructure_event_replay_20260507T1435Z",
    [string]$EventsJsonlUri = "gs://codexalpaca-control-us/research_results/multi_symbol_governed_realtime_20260507/microstructure_shadow_stream_fastwriter_20260507T1340ET/microstructure_shadow_stream_fastwriter_20260507T1340ET/realtime_shadow_events.jsonl",
    [string]$Underlyings = "QQQ,SPY,IWM",
    [ValidateSet("smoke", "liquid_exhaustive_v1")]
    [string]$Profile = "liquid_exhaustive_v1",
    [string]$InstanceSuffix = "20260507m1",
    [int]$ChunkSize = 128,
    [int]$MaxLaunches = 8,
    [int]$MaxContracts = 160,
    [string[]]$Zones = @("us-central1-a", "us-west1-a", "us-east4-a", "us-east1-b"),
    [string]$MachineType = "e2-standard-4",
    [switch]$PrepareOnly
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Project = "codexalpaca"
$ServiceAccount = "ramzi-service-account@codexalpaca.iam.gserviceaccount.com"
$GcsPrefix = "gs://codexalpaca-control-us/research_results/$WaveId"
$SourceArchiveUri = "$GcsPrefix/inputs/source/codexalpaca_repo_source.tar.gz"
$GridJsonlUri = "$GcsPrefix/inputs/microstructure_research_grid.jsonl"
$GridManifestUri = "$GcsPrefix/inputs/microstructure_research_grid_manifest.json"
$LaunchRowsUri = "$GcsPrefix/ops/microstructure_event_replay_launch_rows.json"
$StartupScript = Join-Path $RepoRoot "scripts\gcp_microstructure_event_replay_shard.sh"
$ReportDir = Join-Path $RepoRoot "reports\gcp_research\$WaveId"
$InputsDir = Join-Path $ReportDir "inputs"
$LaunchRowsPath = Join-Path $ReportDir "microstructure_event_replay_launch_rows.json"
$SourceArchivePath = Join-Path $env:TEMP "$WaveId-codexalpaca_repo_source.tar.gz"
$MetadataUnderlyings = $Underlyings.Replace(",", ";")

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

python scripts\build_microstructure_research_grid.py `
    --wave-id $WaveId `
    --output-dir $InputsDir `
    --underlyings $Underlyings `
    --profile $Profile `
    --chunk-size $ChunkSize
if ($LASTEXITCODE -ne 0) {
    throw "microstructure grid builder failed"
}

$GridPath = Join-Path $InputsDir "microstructure_research_grid.jsonl"
$ManifestPath = Join-Path $InputsDir "microstructure_research_grid_manifest.json"
$Manifest = Get-Content -Raw -Path $ManifestPath | ConvertFrom-Json

git archive --format=tar.gz --output $SourceArchivePath HEAD
Invoke-Gcloud @("storage", "cp", $SourceArchivePath, $SourceArchiveUri, "--project", $Project)
Invoke-Gcloud @("storage", "cp", $GridPath, $GridJsonlUri, "--project", $Project)
Invoke-Gcloud @("storage", "cp", $ManifestPath, $GridManifestUri, "--project", $Project)

$launchRows = @()
foreach ($chunk in $Manifest.chunks) {
    $startSlug = "{0:D5}" -f [int]$chunk.grid_start_index
    $endSlug = "{0:D5}" -f ([int]$chunk.grid_start_index + [int]$chunk.grid_count - 1)
    $workerId = "micro_event_c${startSlug}_${endSlug}"
    $instanceName = "micro-event-c$startSlug-$endSlug-$InstanceSuffix"
    $zone = $Zones[$launchRows.Count % $Zones.Count]
    $launchRows += [PSCustomObject]@{
        broker_facing = $false
        events_jsonl_uri = $EventsJsonlUri
        gcs_prefix = $GcsPrefix
        grid_count = [int]$chunk.grid_count
        grid_start_index = [int]$chunk.grid_start_index
        instance_name = $instanceName
        live_manifest_effect = "none"
        machine_type = $MachineType
        max_contracts = $MaxContracts
        risk_policy_effect = "none"
        underlyings = $MetadataUnderlyings
        worker_id = $workerId
        zone = $zone
    }
}
$launchRows | ConvertTo-Json -Depth 5 | Set-Content -Path $LaunchRowsPath -Encoding utf8
Invoke-Gcloud @("storage", "cp", $LaunchRowsPath, $LaunchRowsUri, "--project", $Project)

Write-Output "wave_id=$WaveId"
Write-Output "profile=$Profile"
Write-Output "grid_count=$($Manifest.grid_count)"
Write-Output "chunk_count=$($Manifest.chunks.Count)"
Write-Output "gcs_prefix=$GcsPrefix"
Write-Output "launch_rows=$LaunchRowsPath"
Write-Output "research_only=true"
if ($PrepareOnly) {
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
        events_jsonl_uri = $EventsJsonlUri
        fee_per_contract = "0.65"
        gcs_prefix = $GcsPrefix
        grid_count = "$($row.grid_count)"
        grid_jsonl_uri = $GridJsonlUri
        grid_start_index = "$($row.grid_start_index)"
        max_contracts = "$MaxContracts"
        source_archive_uri = $SourceArchiveUri
        underlyings = $Underlyings
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
            "--boot-disk-size", "80GB",
            "--boot-disk-type", "pd-standard",
            "--service-account", $ServiceAccount,
            "--scopes", "https://www.googleapis.com/auth/cloud-platform",
            "--labels", "app=codexalpaca,env=research,role=microstructure,wave=microstructure",
            "--metadata", (ConvertTo-MetadataArg $metadata),
            "--metadata-from-file", "startup-script=$StartupScript",
            "--quiet"
        )
        if ($status -eq "created") {
            Write-Output "launched_instance=$($row.instance_name) worker=$($row.worker_id) grid_start=$($row.grid_start_index) grid_count=$($row.grid_count) zone=$zone"
            $created = $true
            $launched += 1
            break
        }
        if ($status -eq "quota_limited") {
            Write-Output "quota_limited_after_launch_count=$launched"
            break
        }
    }
    if (-not $created) {
        Write-Output "not_launched_instance=$($row.instance_name)"
    }
}
Write-Output "launched_count=$launched"
