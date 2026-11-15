$ErrorActionPreference = 'Stop'
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..')).Path
$liveOrders = Join-Path $root 'candidates\local-intake-live-link-20261031\prefect_orders.py'
$liveScheduler = Join-Path $root 'candidates\first-step-control-20261029\paperflow_prefect_v2\first_step_scheduler.py'
$candidateOrders = Join-Path $PSScriptRoot 'prefect_orders_no_placeholder.py'
$candidateScheduler = Join-Path $PSScriptRoot 'control_candidate\first_step_scheduler.py'
$python = Join-Path $root 'prefect-first-step-independent\.venv\Scripts\python.exe'
$controlDb = Join-Path $root 'candidates\first-step-control-20261029\runtime\workspace\output\group-task-summaries\summary-index.sqlite3'
$expected = @{
    $liveOrders = 'F5DBB07087A564762DF1D96AF97AD7763AA6BB05E6BEE554E070426713A6BEA5'
    $liveScheduler = '7C79034AF2A80DA432D9ACA17808BA66634504B8940CA70CC378931C270F07D8'
}
$services = @(
    @{ Name = 'Paperflow-LocalIntake-Receiver-14205'; Marker = 'run_waiting_service.py' },
    @{ Name = 'Paperflow-LocalIntake-Api-14205'; Marker = 'run_waiting_service.py' },
    @{ Name = 'Paperflow-FirstStep-API-14203'; Marker = 'managed_api_only.py' }
)
$startOrder = @($services)
[array]::Reverse($startOrder)
function Assert-NoTasks {
    $count = & $python -X utf8 -c "import sqlite3,sys; from pathlib import Path; db=sqlite3.connect(Path(sys.argv[1]).resolve().as_uri()+'?mode=ro',uri=True); print(db.execute('select count(*) from tasks').fetchone()[0])" $controlDb
    if ($LASTEXITCODE -ne 0 -or $count -ne '0') { throw '当前有任务或无法核验任务状态，未同步' }
}
foreach ($target in $expected.Keys) {
    if ((Get-FileHash -Algorithm SHA256 -LiteralPath $target).Hash -ne $expected[$target]) {
        throw "运行文件已变化，未覆盖: $target"
    }
}
foreach ($service in $services) {
    $task = Get-ScheduledTask -TaskName $service.Name
    if ($task.State -ne 'Running' -or @($task.Actions).Count -ne 1 -or
        $task.Actions[0].Arguments -notlike "*$($service.Marker)*") {
        throw "服务运行身份不可核验: $($service.Name)"
    }
}
Assert-NoTasks
$backup = Join-Path $root ('prefect-paperflow\runtime\installations\remove-placeholder-' +
    (Get-Date -Format 'yyyyMMdd-HHmmss') + '-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
[void][IO.Directory]::CreateDirectory($backup)
Copy-Item -LiteralPath $liveOrders -Destination (Join-Path $backup 'prefect_orders.py')
Copy-Item -LiteralPath $liveScheduler -Destination (Join-Path $backup 'first_step_scheduler.py')
$stopped = [System.Collections.Generic.List[string]]::new()
$changesStarted = $false
try {
    foreach ($service in $services) {
        Stop-ScheduledTask -TaskName $service.Name
        $stopped.Add($service.Name)
    }
    $deadline = [DateTime]::UtcNow.AddSeconds(20)
    do {
        $running = @($services | Where-Object { (Get-ScheduledTask -TaskName $_.Name).State -eq 'Running' })
        $listeners = @(Get-NetTCPConnection -State Listen -LocalPort 14203,14205 -ErrorAction SilentlyContinue)
        if (-not $running.Count -and -not $listeners.Count) { break }
        Start-Sleep -Milliseconds 400
    } while ([DateTime]::UtcNow -lt $deadline)
    if ($running.Count -or $listeners.Count) { throw '原服务尚未完全停止，未覆盖运行文件' }
    Assert-NoTasks
    foreach ($target in $expected.Keys) {
        if ((Get-FileHash -Algorithm SHA256 -LiteralPath $target).Hash -ne $expected[$target]) {
            throw "服务停止期间文件发生变化，未覆盖: $target"
        }
    }
    $changesStarted = $true
    Copy-Item -LiteralPath $candidateOrders -Destination $liveOrders -Force
    Copy-Item -LiteralPath $candidateScheduler -Destination $liveScheduler -Force
    foreach ($service in $startOrder) {
        Start-ScheduledTask -TaskName $service.Name
    }
    $deadline = [DateTime]::UtcNow.AddSeconds(35)
    do {
        try {
            $health = Invoke-RestMethod -Uri 'http://127.0.0.1:14203/health' -TimeoutSec 2
            $waiting = Invoke-RestMethod -Uri 'http://127.0.0.1:14205/api/v1/local-intake/llm/waiting' -TimeoutSec 2
            $allRunning = @($services | Where-Object { (Get-ScheduledTask -TaskName $_.Name).State -ne 'Running' }).Count -eq 0
            if ($health -and $waiting.available -and $allRunning) { break }
        } catch { }
        Start-Sleep -Milliseconds 500
    } while ([DateTime]::UtcNow -lt $deadline)
    if (-not ($health -and $waiting.available -and $allRunning)) { throw '同步后服务未全部恢复' }
    Write-Output ("backup=" + $backup)
    Write-Output ("waiting_llm=" + $waiting.waiting_count)
    Write-Output 'placeholder_creation_removed=true'
} catch {
    $failure = $_
    if ($changesStarted) {
        foreach ($service in $services) {
            if ((Get-ScheduledTask -TaskName $service.Name).State -eq 'Running') {
                Stop-ScheduledTask -TaskName $service.Name
            }
        }
        if ((Get-FileHash -Algorithm SHA256 -LiteralPath $liveOrders).Hash -eq
             (Get-FileHash -Algorithm SHA256 -LiteralPath $candidateOrders).Hash) {
            Copy-Item -LiteralPath (Join-Path $backup 'prefect_orders.py') -Destination $liveOrders -Force
        }
        if ((Get-FileHash -Algorithm SHA256 -LiteralPath $liveScheduler).Hash -eq
             (Get-FileHash -Algorithm SHA256 -LiteralPath $candidateScheduler).Hash) {
            Copy-Item -LiteralPath (Join-Path $backup 'first_step_scheduler.py') -Destination $liveScheduler -Force
        }
    }
    foreach ($service in $startOrder) {
        if ((Get-ScheduledTask -TaskName $service.Name).State -ne 'Running') {
            Start-ScheduledTask -TaskName $service.Name
        }
    }
    throw $failure
}
