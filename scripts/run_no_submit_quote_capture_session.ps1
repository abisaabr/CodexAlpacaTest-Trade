param(
    [string]$PortfolioConfig = "config\multi_symbol_governed_realtime_paper_portfolio_20260513_armed.yaml",
    [string]$OutputRoot = "D:\codexalpaca_runtime\quote_capture\multi_symbol_governed_realtime_20260513",
    [int]$DurationSeconds = 23400,
    [int]$MaxOptionSymbols = 1200,
    [string]$Underlyings = "",
    [switch]$SkipSidecarBuild
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $RepoRoot

$RunId = Get-Date -Format "yyyyMMddTHHmmss"
$OutputDir = Join-Path $OutputRoot "capture_$RunId"
$SidecarDir = Join-Path $OutputDir "quote_sidecars"
New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

$MonitorArgs = @(
    "scripts\run_multi_ticker_realtime_shadow_monitor.py",
    "--portfolio-config", $PortfolioConfig,
    "--output-dir", $OutputDir,
    "--max-option-symbols", "$MaxOptionSymbols",
    "--stream",
    "--duration-seconds", "$DurationSeconds",
    "--include-stock-quotes",
    "--include-option-trades",
    "--no-trade-updates"
)

$RequestedUnderlyings = @(
    $Underlyings -split "," |
        ForEach-Object { $_.Trim().ToUpperInvariant() } |
        Where-Object { $_ }
)
foreach ($Underlying in $RequestedUnderlyings) {
    $MonitorArgs += @("--underlying", $Underlying)
}

Write-Output "quote_capture_mode=no_submit_shadow"
Write-Output "broker_facing=false"
Write-Output "paper_orders=false"
Write-Output "portfolio_config=$PortfolioConfig"
Write-Output "output_dir=$OutputDir"
Write-Output "duration_seconds=$DurationSeconds"
Write-Output "max_option_symbols=$MaxOptionSymbols"

python @MonitorArgs
if ($LASTEXITCODE -ne 0) {
    throw "Realtime shadow quote capture failed"
}

if (-not $SkipSidecarBuild) {
    $EventsPath = Join-Path $OutputDir "realtime_shadow_events.jsonl"
    $SidecarArgs = @(
        "scripts\build_realtime_quote_quality_sidecar.py",
        "--events-jsonl", $EventsPath,
        "--output-dir", $SidecarDir
    )
    if ($RequestedUnderlyings.Count -gt 0) {
        $SidecarArgs += @("--underlyings", ($RequestedUnderlyings -join ","))
    }
    python @SidecarArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Realtime quote sidecar build failed"
    }
    Write-Output "sidecar_dir=$SidecarDir"
}

Write-Output "quote_capture_complete=true"
