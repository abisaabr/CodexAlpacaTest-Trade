param(
    [Parameter(Mandatory = $true)]
    [string]$WaveId,
    [string[]]$Symbols = @("AAPL", "NVDA", "INTC", "META", "MU", "NFLX", "ORCL", "PLTR", "XLE", "XOM"),
    [string]$InstanceSuffix = "20260507rt",
    [int]$StartCandidateIndex = 1,
    [int]$CandidateCountPerWorker = 36,
    [int]$MaxLaunchesPerSymbol = 3,
    [int]$MaxSymbols = 4,
    [string]$MachineType = "e2-standard-2",
    [string]$LagProfiles = "0:60,10:60,30:120",
    [string]$Selectors = "entry_liquidity_first_research_only",
    [switch]$PrepareOnly
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Launcher = Join-Path $RepoRoot "scripts\launch_gcp_regime_rescue_shards.ps1"
$TopLadder = @("AAPL", "AMD", "AMZN", "INTC", "IWM", "META", "MSFT", "NVDA", "SPY", "TSLA")
$NextLadder = @("AVGO", "GOOGL", "MU", "NFLX", "ORCL", "PLTR", "QQQ", "TSM", "XLE", "XOM")
$ZoneOrders = @(
    @("us-central1-a", "us-west1-a", "us-east4-a", "us-east1-b"),
    @("us-west1-a", "us-east4-a", "us-east1-b", "us-central1-a"),
    @("us-east4-a", "us-east1-b", "us-central1-a", "us-west1-a"),
    @("us-east1-b", "us-central1-a", "us-west1-a", "us-east4-a")
)

function Get-DatasetUris {
    param([string]$Symbol)
    $Symbol = $Symbol.ToUpperInvariant()
    if ($TopLadder -contains $Symbol) {
        $Root = "option_fill_ladder_20260429"
    } elseif ($NextLadder -contains $Symbol) {
        $Root = "option_fill_ladder_next10_20260429"
    } else {
        throw "No 365d dense dataset root is configured for symbol=$Symbol"
    }
    return @{
        Stock = "gs://codexalpaca-data-us/research_stock_data/$Root/$Symbol/365d_5x5/stock_ref_silver/stock_bars/"
        Contracts = "gs://codexalpaca-control-us/research_results/$Root/$Symbol/365d_5x5/research_wave/dense_universe/selected_option_contracts/"
        Bars = "gs://codexalpaca-data-us/research_option_data/$Root/$Symbol/365d_5x5/option_bars_silver/option_bars/"
    }
}

Set-Location $RepoRoot

$SelectedSymbols = @(
    $Symbols |
        ForEach-Object { $_ -split "," } |
        ForEach-Object { $_.Trim().ToUpperInvariant() } |
        Where-Object { $_ } |
        Select-Object -First $MaxSymbols
)

if ($SelectedSymbols.Count -eq 0) {
    throw "No symbols selected"
}

Write-Output "wave_id=$WaveId"
Write-Output "selected_symbols=$($SelectedSymbols -join ',')"
Write-Output "research_only=true"
Write-Output "broker_facing=false"
Write-Output "live_manifest_effect=none"
Write-Output "risk_policy_effect=none"
Write-Output "selectors=$Selectors"
Write-Output "lag_profiles=$LagProfiles"
Write-Output "start_candidate_index=$StartCandidateIndex"
Write-Output "candidate_count_per_worker=$CandidateCountPerWorker"
Write-Output "max_launches_per_symbol=$MaxLaunchesPerSymbol"

for ($i = 0; $i -lt $SelectedSymbols.Count; $i++) {
    $Symbol = $SelectedSymbols[$i]
    $Uris = Get-DatasetUris $Symbol
    $Zones = $ZoneOrders[$i % $ZoneOrders.Count]
    $LaunchParams = @{
        Symbol = $Symbol
        WaveId = $WaveId
        StockUri = $Uris.Stock
        ContractsUri = $Uris.Contracts
        BarsUri = $Uris.Bars
        InstanceSuffix = $InstanceSuffix
        StartCandidateIndex = $StartCandidateIndex
        CandidateCountPerWorker = $CandidateCountPerWorker
        MaxLaunches = $MaxLaunchesPerSymbol
        MachineType = $MachineType
        Zones = $Zones
        TargetRegimes = "bull,bear,choppy"
        BullProfileSet = "momentum_refine"
        BearProfileSet = "signal_window_refine"
        ChoppyProfileSet = "timewindow_quality_filter"
        Selectors = $Selectors
        LagProfiles = $LagProfiles
        CandidateSelectionMode = "regime_balanced"
    }
    if ($PrepareOnly) {
        $LaunchParams["PrepareOnly"] = $true
    }
    Write-Output "launch_symbol=$Symbol"
    Write-Output "stock_uri=$($Uris.Stock)"
    Write-Output "contracts_uri=$($Uris.Contracts)"
    Write-Output "bars_uri=$($Uris.Bars)"
    & $Launcher @LaunchParams
    if ($LASTEXITCODE -ne 0) {
        throw "Failed launching symbol=$Symbol"
    }
}
