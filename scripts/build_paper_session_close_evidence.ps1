param(
    [string]$PortfolioConfig = "config\multi_symbol_governed_realtime_paper_portfolio_20260513_armed.yaml",
    [string]$TradeDate = (Get-Date -Format "yyyy-MM-dd"),
    [string]$EvidenceRoot = "",
    [string[]]$QuoteEventsJsonl = @(),
    [string]$Underlyings = "",
    [switch]$SkipBrokerHealth,
    [switch]$UpdateStrategyLedgers
)

$ErrorActionPreference = "Stop"

if (-not $EvidenceRoot) {
    $EvidenceRoot = Join-Path "reports\paper_session_evidence" ("session_close_" + $TradeDate)
}

New-Item -ItemType Directory -Force -Path $EvidenceRoot | Out-Null

$healthPath = Join-Path $EvidenceRoot ("session_health_" + $TradeDate + ".json")
$healthArgs = @(
    "scripts\build_multi_ticker_session_health_snapshot.py",
    "--portfolio-config", $PortfolioConfig,
    "--trade-date", $TradeDate,
    "--output-json", $healthPath
)
if ($SkipBrokerHealth) {
    $healthArgs += "--skip-broker"
}
python @healthArgs | Out-Null

$quoteReportRoot = Join-Path $EvidenceRoot "session_quote_fields"
python scripts\build_paper_session_quote_evidence_report.py `
    --portfolio-config $PortfolioConfig `
    --trade-date $TradeDate `
    --run-root $quoteReportRoot | Out-Null

$bundleSummaryPath = $null
if ($QuoteEventsJsonl.Count -gt 0) {
    $bundleRoot = Join-Path $EvidenceRoot "quote_backed_bundle"
    $bundleArgs = @(
        "scripts\build_paper_session_evidence_bundle.py",
        "--portfolio-config", $PortfolioConfig,
        "--trade-date", $TradeDate,
        "--output-dir", $bundleRoot
    )
    foreach ($eventsPath in $QuoteEventsJsonl) {
        $bundleArgs += @("--quote-events-jsonl", $eventsPath)
    }
    if ($Underlyings) {
        $bundleArgs += @("--underlyings", $Underlyings)
    }
    python @bundleArgs | Out-Null
    $bundleSummaryPath = Join-Path $bundleRoot "paper_session_evidence_bundle_summary.json"
}

$postmortemRoot = Join-Path $EvidenceRoot "postmortem"
if ($UpdateStrategyLedgers) {
    $postmortemRoot = ""
}
$postmortemArgs = @(
    "scripts\build_multi_ticker_paper_postmortem.py",
    "--portfolio-config", $PortfolioConfig,
    "--trade-date", $TradeDate
)
if ($postmortemRoot) {
    $postmortemArgs += @("--run-root", $postmortemRoot)
}
if ($bundleSummaryPath -and (Test-Path $bundleSummaryPath)) {
    $postmortemArgs += @("--quote-evidence-json", $bundleSummaryPath)
}
python @postmortemArgs | Out-Null

$summary = [ordered]@{
    status = "paper_session_close_evidence_complete"
    generated_at = (Get-Date).ToString("o")
    portfolio_config = $PortfolioConfig
    trade_date = $TradeDate
    evidence_root = $EvidenceRoot
    health_snapshot_json = $healthPath
    session_quote_fields_root = $quoteReportRoot
    quote_backed_bundle_summary_json = $bundleSummaryPath
    postmortem_root = $(if ($postmortemRoot) { $postmortemRoot } else { "portfolio_config_run_root" })
    quote_events_jsonl = $QuoteEventsJsonl
    update_strategy_ledgers = [bool]$UpdateStrategyLedgers
}
$summaryPath = Join-Path $EvidenceRoot ("session_close_evidence_summary_" + $TradeDate + ".json")
$summary | ConvertTo-Json -Depth 6 | Set-Content -Path $summaryPath -Encoding UTF8
$summary | ConvertTo-Json -Depth 6
