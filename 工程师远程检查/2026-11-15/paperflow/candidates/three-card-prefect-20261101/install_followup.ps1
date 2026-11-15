$ErrorActionPreference = 'Stop'
$root = (Get-Location).Path
$candidate = Join-Path $root 'candidates\three-card-prefect-20261101'
$intake = Join-Path $root 'candidates\local-intake-live-link-20261031'
$release = Join-Path $root 'candidates\postprocess-prefect-20261031\server_release_next'
$deployment = Join-Path $root 'candidates\postprocess-prefect-20261031\deployment-live-20261101'
$oldWrapper = Join-Path $deployment 'run_mainline_observer_three_card_20261101.ps1'
$newWrapper = Join-Path $deployment 'run_mainline_observer_three_card_v2_20261101.ps1'
$backup = Join-Path $candidate 'before-stopped-retry'
$task05 = 'Paperflow-LocalIntake-Api-14205'
$task06 = 'Paperflow-Postprocessing-Observer-14206'

function Hash-File([string]$path) { return (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant() }
function Stop-Observer([string]$name, [int]$port) {
    $connections = @(Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue)
    if ($connections.Count -gt 1) { throw "More than one listener on $port" }
    $listenerPid = if ($connections.Count) { $connections[0].OwningProcess } else { $null }
    if ($listenerPid) {
        $process = Get-CimInstance Win32_Process -Filter ("ProcessId=" + $listenerPid)
        if ($process.CommandLine -notlike '*run_waiting_service.py*' -or $process.CommandLine -notlike ("*" + $port + "*")) {
            throw "Unexpected listener on $port"
        }
    }
    Stop-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue
    Start-Sleep -Milliseconds 500
    if ($listenerPid -and (Get-Process -Id $listenerPid -ErrorAction SilentlyContinue)) { Stop-Process -Id $listenerPid }
}

$expected = Get-Content -LiteralPath (Join-Path $candidate 'followup-baseline.json') -Raw -Encoding UTF8 | ConvertFrom-Json
foreach ($entry in $expected.PSObject.Properties) {
    if ((Hash-File $entry.Name) -cne $entry.Value) { throw "Baseline changed: $($entry.Name)" }
}
$observerTask = Get-ScheduledTask -TaskName $task06
if (@($observerTask.Actions).Count -ne 1) { throw 'Unexpected observer action' }
$oldAction = $observerTask.Actions[0]
if ($oldAction.Arguments -notmatch '-EncodedCommand\s+([A-Za-z0-9+/=]+)') { throw 'Unexpected observer launcher' }
$script = [Text.Encoding]::Unicode.GetString([Convert]::FromBase64String($Matches[1]))
$oldHash = Hash-File $oldWrapper
$newHash = Hash-File (Join-Path $candidate 'observer-wrapper-v2.ps1')
if (-not $script.Contains($oldWrapper) -or -not $script.Contains($oldHash)) { throw 'Observer task changed' }
$updated = $script.Replace($oldWrapper, $newWrapper).Replace($oldHash, $newHash)
$encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($updated))
$args06 = $oldAction.Arguments -replace '-EncodedCommand\s+[A-Za-z0-9+/=]+', ('-EncodedCommand ' + $encoded)
$newAction = New-ScheduledTaskAction -Execute $oldAction.Execute -Argument $args06 -WorkingDirectory $oldAction.WorkingDirectory
Export-ScheduledTask -TaskName $task06 | Set-Content -LiteralPath (Join-Path $backup 'observer-task.xml') -Encoding UTF8

$changed = $false
try {
    Stop-Observer $task05 14205
    Stop-Observer $task06 14206
    $changed = $true
    Copy-Item -LiteralPath (Join-Path $candidate 'llm_queue_summary.py') -Destination (Join-Path $intake 'llm_queue_summary.py') -Force
    Copy-Item -LiteralPath (Join-Path $candidate 'release-inventory-three-card-v2-20261101.json') -Destination $release -Force
    Copy-Item -LiteralPath (Join-Path $candidate 'observer-wrapper-v2.ps1') -Destination $newWrapper -Force
    Set-ScheduledTask -TaskName $task06 -Action $newAction | Out-Null
    Start-ScheduledTask -TaskName $task05
    Start-ScheduledTask -TaskName $task06
    $ready = $false
    for ($i = 0; $i -lt 25; $i++) {
        Start-Sleep -Seconds 1
        $p05 = @(Get-NetTCPConnection -LocalPort 14205 -State Listen -ErrorAction SilentlyContinue)
        $p06 = @(Get-NetTCPConnection -LocalPort 14206 -State Listen -ErrorAction SilentlyContinue)
        if ($p05.Count -eq 1 -and $p06.Count -eq 1) { $ready = $true; break }
    }
    if (-not $ready) { throw 'Read-only observers did not both start' }
    Write-Output 'Stopped-retry display change installed; read-only observers loaded.'
}
catch {
    $cause = $_.Exception.Message
    if ($changed) {
        Stop-Observer $task05 14205
        Stop-Observer $task06 14206
        Copy-Item -LiteralPath (Join-Path $backup 'llm_queue_summary.py') -Destination (Join-Path $intake 'llm_queue_summary.py') -Force
        Set-ScheduledTask -TaskName $task06 -Action $oldAction | Out-Null
    }
    Start-ScheduledTask -TaskName $task05 -ErrorAction SilentlyContinue
    Start-ScheduledTask -TaskName $task06 -ErrorAction SilentlyContinue
    throw "Display update failed; prior file and observer action restored. $cause"
}
