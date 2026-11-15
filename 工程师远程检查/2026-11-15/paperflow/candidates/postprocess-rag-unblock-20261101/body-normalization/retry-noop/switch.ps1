$ErrorActionPreference = 'Stop'
$candidate = $PSScriptRoot
$base = Split-Path (Split-Path $candidate -Parent) -Parent
$old = Get-Content -LiteralPath (Join-Path $base 'bindings.json') -Raw -Encoding UTF8 | ConvertFrom-Json
$new = Get-Content -LiteralPath (Join-Path $candidate 'bindings.json') -Raw -Encoding UTF8 | ConvertFrom-Json
$task = Invoke-RestMethod -Uri 'http://127.0.0.1:14203/api/v1/first-step/tasks/6cb5a766-94f9-4613-aa5e-17a883be9016?page_size=1' -TimeoutSec 8
if ($task.desired_state -ne 'paused' -or $task.inflight_requests -ne 0) { throw 'The original task is not paused and idle.' }
$oldConfig = Get-Content -LiteralPath $old.remote_config -Raw -Encoding UTF8 | ConvertFrom-Json
$newConfig = Get-Content -LiteralPath $new.remote_config -Raw -Encoding UTF8 | ConvertFrom-Json
$liveStage = Join-Path $oldConfig.runtime_dir 'stage_runtime.py'
if ((Get-FileHash -LiteralPath $liveStage -Algorithm SHA256).Hash.ToLowerInvariant() -ne $oldConfig.runtime_sources.'stage_runtime.py') { throw 'Current stage source changed.' }
if ((Get-FileHash -LiteralPath (Join-Path $candidate 'stage_runtime.py') -Algorithm SHA256).Hash.ToLowerInvariant() -ne $newConfig.runtime_sources.'stage_runtime.py') { throw 'Candidate stage source changed.' }
foreach ($binding in @($old, $new)) {
    foreach ($key in @('remote_config', 'release_inventory', 'observer')) {
        if ((Get-FileHash -LiteralPath $binding.$key -Algorithm SHA256).Hash.ToLowerInvariant() -ne $binding."${key}_sha256") { throw "Binding changed: $key" }
    }
}
$backup = Join-Path $candidate ('before-switch-' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
New-Item -ItemType Directory -Path $backup -ErrorAction Stop | Out-Null
Copy-Item -LiteralPath $liveStage -Destination (Join-Path $backup 'stage_runtime.py')
Copy-Item -LiteralPath (Join-Path $base 'bindings.json') -Destination (Join-Path $backup 'bindings.json')
Export-ScheduledTask -TaskName 'Paperflow-Remote-Postprocess-Coordinator' | Set-Content -LiteralPath (Join-Path $backup 'coordinator.xml') -Encoding UTF8
Export-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206' | Set-Content -LiteralPath (Join-Path $backup 'observer.xml') -Encoding UTF8
$workerTask = Get-ScheduledTask -TaskName 'Paperflow-Remote-Postprocess-Coordinator'
$python = $workerTask.Actions[0].Execute
Stop-ScheduledTask -TaskName 'Paperflow-Remote-Postprocess-Coordinator'
Stop-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206'
$processes = @(Get-CimInstance Win32_Process | Where-Object {
    $_.Name -eq 'pythonw.exe' -and $_.CommandLine -and
    (($_.CommandLine.Contains('run_remote_postprocess_worker.py') -and $_.CommandLine.Contains($old.remote_config)) -or
     ($_.CommandLine.Contains('run_waiting_service.py') -and $_.CommandLine.Contains('14206')))
})
$processes | Sort-Object ParentProcessId -Descending | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
Copy-Item -LiteralPath (Join-Path $candidate 'stage_runtime.py') -Destination $liveStage -Force
Copy-Item -LiteralPath (Join-Path $candidate 'bindings.json') -Destination (Join-Path $base 'bindings.json') -Force
$worker = Join-Path $newConfig.prefect_project 'run_remote_postprocess_worker.py'
$workerArgs = '-X utf8 -B "{0}" --config "{1}" --config-sha256 {2}' -f $worker, $new.remote_config, $new.remote_config_sha256
Set-ScheduledTask -TaskName 'Paperflow-Remote-Postprocess-Coordinator' -Action (New-ScheduledTaskAction -Execute $python -Argument $workerArgs) | Out-Null
$bootstrap = @'
$ErrorActionPreference = 'Stop'
$wrapper = '__WRAPPER__'
$expected = '__HASH__'
$bytes = [IO.File]::ReadAllBytes($wrapper)
$hasher = [Security.Cryptography.SHA256]::Create()
try { $actual = -join ($hasher.ComputeHash($bytes) | ForEach-Object { $_.ToString('x2') }) }
finally { $hasher.Dispose() }
if ($actual -cne $expected) { throw 'Observer wrapper source changed' }
& ([scriptblock]::Create([Text.Encoding]::UTF8.GetString($bytes)))
exit $LASTEXITCODE
'@
$bootstrap = $bootstrap.Replace('__WRAPPER__', $new.observer.Replace("'", "''")).Replace('__HASH__', $new.observer_sha256)
$encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($bootstrap))
Set-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206' -Action (New-ScheduledTaskAction -Execute "$env:WINDIR\System32\WindowsPowerShell\v1.0\powershell.exe" -Argument "-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -EncodedCommand $encoded") | Out-Null
Start-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206'
Start-ScheduledTask -TaskName 'Paperflow-Remote-Postprocess-Coordinator'
Write-Output "Updated existing postprocessing runtime; task remains paused. Backup: $backup"
