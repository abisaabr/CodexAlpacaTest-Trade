param(
    [string]$Project = "codexalpaca",
    [string]$Zone = "us-central1-a",
    [string]$InstanceName = "paper-ready-controller-20260504qa",
    [string]$MachineType = "e2-standard-2",
    [string]$Branch = "codex/phase2-fill-semantics-20260430",
    [string]$RepoUrl = "https://github.com/abisaabr/CodexAlpacaTest-Trade.git",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Python = "C:\Users\rabisaab\AppData\Local\Programs\Python\Python312\python.exe"
$Gcloud = "C:\Users\rabisaab\Downloads\google-cloud-sdk-local\google-cloud-sdk\bin\gcloud.cmd"
$ServiceAccount = "ramzi-service-account@codexalpaca.iam.gserviceaccount.com"
$WaveId = "autonomous_paper_readiness_20260504"
$GcsRoot = "gs://codexalpaca-control-us/research_results/$WaveId"
$SourceArchiveUri = "$GcsRoot/inputs/source/codexalpaca_repo_source.tar.gz"
$StatusPrefix = "$GcsRoot/controller"
$StartupScript = Join-Path $RepoRoot "scripts\gcp_bootstrap_autonomous_paper_readiness_controller.sh"
$ArchivePath = Join-Path $RepoRoot "reports\gcp_research\autonomous_paper_readiness_20260504\codexalpaca_repo_source.tar.gz"

Set-Location $RepoRoot
$env:CLOUDSDK_PYTHON = $Python
$env:GOOGLE_CLOUD_PROJECT = $Project
$KeyPath = "C:\Users\rabisaab\Downloads\codexalpaca-7bcb9ac9a02d.json"
if (Test-Path $KeyPath) {
    $env:GOOGLE_APPLICATION_CREDENTIALS = $KeyPath
}

New-Item -ItemType Directory -Path (Split-Path -Parent $ArchivePath) -Force | Out-Null

@'
import subprocess
import tarfile
from pathlib import Path

repo = Path.cwd()
archive = Path(r"__ARCHIVE_PATH__")
archive.parent.mkdir(parents=True, exist_ok=True)
tracked = subprocess.run(["git", "ls-files"], cwd=repo, text=True, stdout=subprocess.PIPE, check=True).stdout.splitlines()
with tarfile.open(archive, "w:gz") as tar:
    for rel in tracked:
        path = repo / rel
        if path.is_file():
            tar.add(path, arcname=rel)
'@.Replace("__ARCHIVE_PATH__", $ArchivePath.Replace("\", "\\")) | & $Python -

if ($DryRun) {
    Write-Output "dry_run would upload $ArchivePath to $SourceArchiveUri"
} else {
    & $Gcloud storage cp $ArchivePath $SourceArchiveUri --project $Project
    & $Gcloud storage cp $StartupScript "$GcsRoot/inputs/startup/gcp_bootstrap_autonomous_paper_readiness_controller.sh" --project $Project
}

$ControllerArgs = "--loop --sleep-seconds 900 --max-launches-per-pass 16 --allow-delete-terminated"
$Metadata = @(
    "repo_url=$RepoUrl",
    "branch=$Branch",
    "source_archive_uri=$SourceArchiveUri",
    "status_prefix=$StatusPrefix",
    "controller_args=$ControllerArgs"
) -join ","

$Labels = "role=paper-ready-controller,wave=autonomous-paper-readiness,symbol=qqq"
$CreateArgs = @(
    "compute", "instances", "create", $InstanceName,
    "--project", $Project,
    "--zone", $Zone,
    "--machine-type", $MachineType,
    "--image-family", "debian-12",
    "--image-project", "debian-cloud",
    "--service-account", $ServiceAccount,
    "--scopes", "cloud-platform",
    "--boot-disk-size", "80GB",
    "--boot-disk-type", "pd-standard",
    "--labels", $Labels,
    "--metadata", $Metadata,
    "--metadata-from-file", "startup-script=$StartupScript"
)

if ($DryRun) {
    Write-Output "dry_run gcloud $($CreateArgs -join ' ')"
} else {
    & $Gcloud @CreateArgs
    $status = [PSCustomObject]@{
        generated_at_utc = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
        controller_instance = $InstanceName
        zone = $Zone
        branch = $Branch
        repo_url = $RepoUrl
        gcs_root = $GcsRoot
        status_prefix = $StatusPrefix
        source_archive_uri = $SourceArchiveUri
        broker_facing = $false
        paper_orders = $false
        live_manifest_effect = "none"
        risk_policy_effect = "none"
    }
    $StatusPath = Join-Path $RepoRoot "reports\gcp_research\autonomous_paper_readiness_20260504\controller_launch_status.json"
    $status | ConvertTo-Json -Depth 6 | Set-Content -Path $StatusPath -Encoding utf8
    & $Gcloud storage cp $StatusPath "$StatusPrefix/controller_launch_status.json" --project $Project
}
