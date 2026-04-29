param(
    [switch]$RunOnce,
    [string]$ResearchGatePath = "",
    [int]$ResearchGatePollSeconds = 60
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$pythonPath = Join-Path $repoRoot ".venv\\Scripts\\python.exe"
if (-not (Test-Path $pythonPath)) {
    throw "Missing virtualenv python at $pythonPath. Run scripts\\bootstrap_windows.ps1 first."
}

$scriptPath = Join-Path $repoRoot "scripts\\run_multi_ticker_portfolio_paper_trader.py"
$portfolioConfig = Join-Path $repoRoot "config\\multi_ticker_paper_portfolio.yaml"
$pythonArgs = @($scriptPath, "--portfolio-config", $portfolioConfig, "--submit-paper-orders")
if ($RunOnce) {
    $pythonArgs += "--run-once"
}

if ([string]::IsNullOrWhiteSpace($ResearchGatePath)) {
    $downloadsRoot = Split-Path -Parent $repoRoot
    $ResearchGatePath = Join-Path $downloadsRoot "qqq_options_30d_cleanroom\\output\\paper_runner_gate.json"
}

function Get-EasternNow {
    $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
    return [System.TimeZoneInfo]::ConvertTime([datetimeoffset]::UtcNow, $tz)
}

function Get-SessionCutoff([datetimeoffset]$nowEt) {
    return [datetimeoffset]::new($nowEt.Year, $nowEt.Month, $nowEt.Day, 16, 10, 0, $nowEt.Offset)
}

function Write-SupervisorLog([string]$message) {
    $nowEt = Get-EasternNow
    $logDir = Join-Path $repoRoot "reports\\multi_ticker_portfolio\\supervisor"
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    $logPath = Join-Path $logDir ("session_{0}.log" -f $nowEt.ToString("yyyy-MM-dd"))
    Add-Content -Path $logPath -Value ("[{0}] {1}" -f $nowEt.ToString("o"), $message)
}

function Get-ResearchGateDecision([datetimeoffset]$nowEt) {
    if ([string]::IsNullOrWhiteSpace($ResearchGatePath) -or -not (Test-Path $ResearchGatePath)) {
        return [pscustomobject]@{
            block = $false
            message = "No overnight research gate file present."
        }
    }

    $payload = $null
    try {
        $payload = Get-Content -Path $ResearchGatePath -Raw | ConvertFrom-Json
    }
    catch {
        return [pscustomobject]@{
            block = $true
            message = ("Overnight research gate file is unreadable: {0}" -f $_.Exception.Message)
        }
    }

    $targetTradeDate = [string]$payload.target_trade_date
    if (-not [string]::IsNullOrWhiteSpace($targetTradeDate)) {
        $todayEt = $nowEt.ToString("yyyy-MM-dd")
        if ($targetTradeDate -lt $todayEt) {
            return [pscustomobject]@{
                block = $false
                message = ("Ignoring stale overnight research gate for {0}." -f $targetTradeDate)
            }
        }
        if ($targetTradeDate -gt $todayEt) {
            return [pscustomobject]@{
                block = $false
                message = ("Ignoring overnight research gate for future trade date {0}." -f $targetTradeDate)
            }
        }
    }

    $status = [string]$payload.status
    $phase = [string]$payload.phase
    $message = [string]$payload.message
    if ($status -eq "completed") {
        return [pscustomobject]@{
            block = $false
            message = ("Overnight research gate cleared ({0})." -f $phase)
        }
    }

    if ([string]::IsNullOrWhiteSpace($message)) {
        $message = "Overnight research gate is not complete yet."
    }
    return [pscustomobject]@{
        block = $true
        message = ("Holding runner for overnight research gate status '{0}' ({1}). {2}" -f $status, $phase, $message)
    }
}

function Invoke-Trader {
    Push-Location $repoRoot
    try {
        Write-SupervisorLog ("Launching trader: {0} {1}" -f $pythonPath, ($pythonArgs -join " "))
        & $pythonPath @pythonArgs
        return $LASTEXITCODE
    }
    finally {
        Pop-Location
    }
}

if ($RunOnce) {
    $gateDecision = Get-ResearchGateDecision (Get-EasternNow)
    if ($gateDecision.block) {
        Write-SupervisorLog $gateDecision.message
        exit 75
    }
    exit (Invoke-Trader)
}

$restartCount = 0
Write-SupervisorLog "Starting supervised multi-ticker portfolio runner."
while ($true) {
    $nowEt = Get-EasternNow
    if ($nowEt -ge (Get-SessionCutoff $nowEt)) {
        Write-SupervisorLog "Stopping supervisor because the session cutoff has passed."
        break
    }

    $gateDecision = Get-ResearchGateDecision $nowEt
    if ($gateDecision.block) {
        Write-SupervisorLog $gateDecision.message
        Start-Sleep -Seconds $ResearchGatePollSeconds
        continue
    }

    $exitCode = 0
    try {
        $exitCode = Invoke-Trader
    }
    catch {
        $exitCode = 1
        Write-SupervisorLog ("Runner threw an exception: {0}" -f $_.Exception.Message)
    }

    $nowEt = Get-EasternNow
    if ($nowEt -ge (Get-SessionCutoff $nowEt)) {
        Write-SupervisorLog ("Runner exited with code {0} after the session cutoff." -f $exitCode)
        break
    }

    if ($exitCode -eq 42) {
        Write-SupervisorLog "Runner is in standby because another machine currently owns the portfolio lease. Sleeping for 120 seconds before re-checking."
        Start-Sleep -Seconds 120
        continue
    }

    $restartCount += 1
    Write-SupervisorLog ("Runner exited early with code {0}; restarting in 30 seconds (attempt {1})." -f $exitCode, $restartCount)
    Start-Sleep -Seconds 30
}
