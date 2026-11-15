$ErrorActionPreference = 'Stop'
$statePath = Join-Path $PSScriptRoot 'sandbox\processes.json'
if (-not (Test-Path -LiteralPath $statePath -PathType Leaf)) { throw '找不到候选进程记录，没有停止任何进程。' }
$entries = @(Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json)
$projectRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..')).Path
$firstPython = Join-Path $projectRoot 'prefect-first-step-independent\.venv\Scripts\python.exe'
$env:PAPERFLOW_CONFIG = Join-Path $PSScriptRoot 'first-step\config.json'
& $firstPython (Join-Path $PSScriptRoot 'first-step\check_candidate_stopped.py')
if ($LASTEXITCODE -ne 0) { throw '候选尚未安全暂停，没有停止进程。' }
$worker = $entries | Where-Object { $_.name -eq 'first-worker' } | Select-Object -First 1
if ($worker) {
    $process = Get-Process -Id $worker.id -ErrorAction SilentlyContinue
    if ($process -and $process.StartTime.ToUniversalTime().Ticks -eq $worker.startTicks) {
        $children = @(Get-CimInstance Win32_Process -Filter "ParentProcessId = $($worker.id)" -ErrorAction Stop)
        if ($children.Count -gt 0) { throw '候选执行器仍有子进程，请稍后再停止。' }
    }
}
foreach ($entry in $entries) {
    $process = Get-Process -Id $entry.id -ErrorAction SilentlyContinue
    if (-not $process -or $process.StartTime.ToUniversalTime().Ticks -ne $entry.startTicks) { continue }
    $details = Get-CimInstance Win32_Process -Filter "ProcessId = $($entry.id)" -ErrorAction SilentlyContinue
    if (-not $details -or -not $details.CommandLine.Contains([string]$entry.marker)) { continue }
    Stop-Process -Id $entry.id
}
Write-Host '已请求停止身份匹配的候选进程；候选数据和工单记录已保留。'
