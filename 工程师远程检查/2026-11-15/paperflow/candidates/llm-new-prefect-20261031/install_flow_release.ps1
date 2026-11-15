$ErrorActionPreference = 'Stop'
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..')).Path
$stage = Join-Path $root 'candidates\llm-flow-staged-20261031-v4'
$manifest = Get-Content -LiteralPath (Join-Path $stage 'stage-manifest.json') -Raw -Encoding UTF8 | ConvertFrom-Json -AsHashtable
$firstPython = Join-Path $root 'prefect-first-step-independent\.venv\Scripts\python.exe'
$prefectPython = Join-Path $root 'prefect-paperflow\.venv\Scripts\python.exe'
$limitsScript = Join-Path $PSScriptRoot 'activate_flow_limits.py'
$controlDb = Join-Path $root 'candidates\first-step-control-20261029\runtime\workspace\output\group-task-summaries\summary-index.sqlite3'
$services = @(
    @{ Name = 'Paperflow-LocalIntake-Receiver-14205'; Marker = 'run_waiting_service.py' },
    @{ Name = 'Paperflow-LocalIntake-Api-14205'; Marker = 'run_waiting_service.py' },
    @{ Name = 'Paperflow-FirstStep-API-14203'; Marker = 'managed_api_only.py' },
    @{ Name = 'Paperflow-Unified-Folder-Worker'; Marker = 'run_unified_folder_worker.py' }
)
$maps = [System.Collections.Generic.List[object]]::new()
function Add-Mapping([string]$source, [string]$target, [string]$before) {
    $maps.Add(@{ Source=$source; Target=$target; Before=$before })
}
Add-Mapping 'local_preprocessing.py' 'prefect-paperflow\paperflow_prefect\local_preprocessing.py' 'F53D78ACE65A40B885F5E89A32D9E5CF1E862C5BE0BB9302116FB17F53E33ABF'
Add-Mapping 'run_unified_folder_worker.py' 'prefect-paperflow\run_unified_folder_worker.py' '608803BA253DA3EFDE4E2FF89837D5D654D4D78BC740519D8E09C1128613291D'
Add-Mapping 'intake\prefect_orders.py' 'candidates\local-intake-live-link-20261031\prefect_orders.py' '5216E8247AA6DECBEA93314DAAB387A19D14EC10A09B8E75084ABD52DCC3366C'
Add-Mapping 'intake\waiting_api.py' 'candidates\local-intake-live-link-20261031\waiting_api.py' 'F30CA68A231945EB698B76BDA324C0F27F740B290AC527E5B8D8D472A2024ED2'
Add-Mapping 'intake\llm_queue_summary.py' 'candidates\local-intake-live-link-20261031\llm_queue_summary.py' ''
Add-Mapping 'control\first_step_control.py' 'candidates\first-step-control-20261029\paperflow_prefect_v2\first_step_control.py' '931C6EE40F0FE554A2FA77B7DB3D6FB4C6DDE44823580A2A5E969458657935F3'
Add-Mapping 'control\first_step_delete.py' 'candidates\first-step-control-20261029\paperflow_prefect_v2\first_step_delete.py' '10825274870C225D2B00CF54DB6C3FFC797424C8FA7B4F701956ACAF7C5DED44'
Add-Mapping 'control\group_summary_store.py' 'candidates\first-step-control-20261029\paperflow_prefect_v2\group_summary_store.py' '4B75EA88A3018EB564A672731480EA07A3867DF6591CFD0A0D75229E8F16A543'
Add-Mapping 'control\llm_admission_state.py' 'candidates\first-step-control-20261029\paperflow_prefect_v2\llm_admission_state.py' ''
Add-Mapping 'ui\app.js' 'candidates\first-step-preview-8795\app.js' '18AAAB2EF207D4B36A7C39475B5B917DBB05B9E0FCFDB0CFCA1B601AD977DC85'
Add-Mapping 'ui\index.html' 'candidates\first-step-preview-8795\index.html' '4B50801BDF937B216E093DCA1970E890F1C2C574725360B91997196C474FDF2C'
Add-Mapping 'ui\prefect-ticket-counts.js' 'candidates\first-step-preview-8795\prefect-ticket-counts.js' '812CC2FDE50059511BEBD5CD4DA0EAC790F5E42DFE00B0B2D0F1F2692BC95763'
foreach ($file in (Get-ChildItem -LiteralPath (Join-Path $stage 'llm_queue') -File)) {
    Add-Mapping ('llm_queue\' + $file.Name) ('prefect-paperflow\paperflow_prefect\llm_queue\' + $file.Name) ''
}
function Assert-NoTasks {
    $count = & $firstPython -X utf8 -c "import sqlite3,sys; from pathlib import Path; db=sqlite3.connect(Path(sys.argv[1]).resolve().as_uri()+'?mode=ro',uri=True); print(db.execute('select count(*) from tasks').fetchone()[0])" $controlDb
    if ($LASTEXITCODE -ne 0 -or $count -ne '0') { throw '当前有任务或无法核验任务状态，未同步' }
}
function Assert-Files {
    foreach ($map in $maps) {
        $source = Join-Path $stage $map.Source
        $target = Join-Path $root $map.Target
        $key = $map.Source.Replace('/', '\')
        $savedHash = $manifest.staged[$key]
        if (-not $savedHash -or (Get-FileHash -Algorithm SHA256 -LiteralPath $source).Hash -ne $savedHash) {
            throw "待同步文件已变化: $($map.Source)"
        }
        if ($map.Before) {
            if (-not (Test-Path -LiteralPath $target -PathType Leaf) -or
                (Get-FileHash -Algorithm SHA256 -LiteralPath $target).Hash -ne $map.Before) {
                throw "运行文件已变化，未覆盖: $($map.Target)"
            }
        } elseif (Test-Path -LiteralPath $target) {
            throw "新文件位置已经存在，未覆盖: $($map.Target)"
        }
    }
    $scheduler = Join-Path $root 'candidates\first-step-control-20261029\paperflow_prefect_v2\first_step_scheduler.py'
    if ((Get-FileHash -Algorithm SHA256 -LiteralPath $scheduler).Hash -ne 'E161A0E34590E1411AD82F8C19054A774B630A90236813CF265552DC34785DE8') {
        throw '文件夹工单删除控制已变化，未覆盖'
    }
}
function Run-Limits([string]$operation, [string]$receipt) {
    & $prefectPython -X utf8 -B $limitsScript $operation $receipt
    if ($LASTEXITCODE -ne 0) { throw "Prefect 并发切换未完成: $operation" }
}
Assert-NoTasks
Assert-Files
foreach ($service in $services) {
    $task = Get-ScheduledTask -TaskName $service.Name
    if ($task.State -ne 'Running' -or @($task.Actions).Count -ne 1 -or
        $task.Actions[0].Arguments -notlike "*$($service.Marker)*") {
        throw "服务运行身份不可核验: $($service.Name)"
    }
}
$backup = Join-Path $root ('prefect-paperflow\runtime\installations\llm-flow-' +
    (Get-Date -Format 'yyyyMMdd-HHmmss') + '-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
[void][IO.Directory]::CreateDirectory($backup)
foreach ($map in $maps) {
    if ($map.Before) {
        $target = Join-Path $backup (Join-Path 'before' $map.Target)
        [void][IO.Directory]::CreateDirectory((Split-Path -Parent $target))
        Copy-Item -LiteralPath (Join-Path $root $map.Target) -Destination $target
    }
}
Copy-Item -LiteralPath (Join-Path $stage 'stage-manifest.json') -Destination (Join-Path $backup 'stage-manifest.json')
$receipt = Join-Path $backup 'prefect-limits-before.json'
$startedCopy = $false
$limitsPrepared = $false
$released = $false
try {
    foreach ($service in $services) { Stop-ScheduledTask -TaskName $service.Name }
    $deadline = [DateTime]::UtcNow.AddSeconds(25)
    do {
        $running = @($services | Where-Object { (Get-ScheduledTask -TaskName $_.Name).State -eq 'Running' })
        $listeners = @(Get-NetTCPConnection -State Listen -LocalPort 14203,14205 -ErrorAction SilentlyContinue)
        if (-not $running.Count -and -not $listeners.Count) { break }
        Start-Sleep -Milliseconds 400
    } while ([DateTime]::UtcNow -lt $deadline)
    if ($running.Count -or $listeners.Count) { throw '原服务尚未停止，未覆盖运行文件' }
    Assert-NoTasks
    Assert-Files
    try {
        Run-Limits 'prepare' $receipt
    } finally {
        # A partial remote update still has a receipt and needs rollback.
        $limitsPrepared = Test-Path -LiteralPath $receipt -PathType Leaf
    }
    $startedCopy = $true
    foreach ($map in $maps) {
        $target = Join-Path $root $map.Target
        [void][IO.Directory]::CreateDirectory((Split-Path -Parent $target))
        Copy-Item -LiteralPath (Join-Path $stage $map.Source) -Destination $target -Force
    }
    foreach ($map in $maps) {
        if ((Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $root $map.Target)).Hash -ne
            (Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $stage $map.Source)).Hash) {
            throw "同步文件的内容未确认: $($map.Target)"
        }
    }
    Start-ScheduledTask -TaskName 'Paperflow-FirstStep-API-14203'
    Start-ScheduledTask -TaskName 'Paperflow-LocalIntake-Api-14205'
    Run-Limits 'release' $receipt
    $released = $true
    Start-ScheduledTask -TaskName 'Paperflow-Unified-Folder-Worker'
    Start-ScheduledTask -TaskName 'Paperflow-LocalIntake-Receiver-14205'
    # Observe the existing service manager, without running any task or test.
    Start-Sleep -Seconds 5
    $deadline = [DateTime]::UtcNow.AddSeconds(20)
    do {
        $notRunning = @($services | Where-Object { (Get-ScheduledTask -TaskName $_.Name).State -ne 'Running' })
        if (-not $notRunning.Count) { break }
        Start-Sleep -Milliseconds 400
    } while ([DateTime]::UtcNow -lt $deadline)
    if ($notRunning.Count) { throw '部分服务未保持运行，请核对服务状态' }
    Write-Output ('backup=' + $backup)
    Write-Output 'llm_files_installed=true'
    Write-Output 'real_model_requests_started_by_installer=0'
    Write-Output 'postprocessing_started_by_installer=0'
} catch {
    $failure = $_
    try {
        # Never stop or overwrite a task that appeared during deployment.
        Assert-NoTasks
        foreach ($service in $services) {
            if ((Get-ScheduledTask -TaskName $service.Name).State -eq 'Running') {
                Stop-ScheduledTask -TaskName $service.Name
            }
        }
        $deadline = [DateTime]::UtcNow.AddSeconds(20)
        do {
            $running = @($services | Where-Object { (Get-ScheduledTask -TaskName $_.Name).State -eq 'Running' })
            if (-not $running.Count) { break }
            Start-Sleep -Milliseconds 400
        } while ([DateTime]::UtcNow -lt $deadline)
        if ($running.Count) { throw '服务尚未停止，未覆盖文件或调整并发' }
        if ($limitsPrepared) { Run-Limits 'rollback' $receipt }
        if ($startedCopy) {
            foreach ($map in $maps) {
                if (-not $map.Before) { continue }
                $target = Join-Path $root $map.Target
                $after = (Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $stage $map.Source)).Hash
                $current = (Get-FileHash -Algorithm SHA256 -LiteralPath $target).Hash
                if ($current -eq $after) {
                    Copy-Item -LiteralPath (Join-Path $backup (Join-Path 'before' $map.Target)) -Destination $target -Force
                } elseif ($current -ne $map.Before) {
                    throw "运行文件已被其他操作修改，未覆盖: $($map.Target)"
                }
            }
        }
        foreach ($name in @('Paperflow-FirstStep-API-14203','Paperflow-LocalIntake-Api-14205',
                             'Paperflow-Unified-Folder-Worker','Paperflow-LocalIntake-Receiver-14205')) {
            if ((Get-ScheduledTask -TaskName $name).State -ne 'Running') { Start-ScheduledTask -TaskName $name }
        }
    } catch {
        Write-Warning ('自动回退未完成：' + $_.Exception.Message + '；备份：' + $backup)
    }
    throw $failure
}
