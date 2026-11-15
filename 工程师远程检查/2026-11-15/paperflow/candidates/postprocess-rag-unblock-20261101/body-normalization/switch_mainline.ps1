$ErrorActionPreference = 'Stop'
$parent = Split-Path $PSScriptRoot -Parent
$old = Get-Content -LiteralPath (Join-Path $parent 'bindings.json') -Raw -Encoding UTF8 | ConvertFrom-Json
$new = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'bindings-mainline.json') -Raw -Encoding UTF8 | ConvertFrom-Json
$task = Invoke-RestMethod -Uri 'http://127.0.0.1:14203/api/v1/first-step/tasks/6cb5a766-94f9-4613-aa5e-17a883be9016' -TimeoutSec 5
if ($task.desired_state -ne 'paused' -or $task.inflight_requests -ne 0) { throw 'The parent task is not paused and idle.' }
if ((Get-FileHash -LiteralPath $old.remote_config -Algorithm SHA256).Hash.ToLowerInvariant() -ne $old.remote_config_sha256) { throw 'Current configuration changed.' }
if ((Get-FileHash -LiteralPath $new.remote_config -Algorithm SHA256).Hash.ToLowerInvariant() -ne $new.remote_config_sha256) { throw 'Candidate configuration changed.' }
$backup = Join-Path $PSScriptRoot 'mainline-before-switch'
New-Item -ItemType Directory -Path $backup -ErrorAction Stop | Out-Null
Copy-Item -LiteralPath (Join-Path $parent 'bindings.json') -Destination (Join-Path $backup 'bindings.json')
Copy-Item -LiteralPath (Join-Path $parent 'receipt_access.py') -Destination (Join-Path $backup 'receipt_access.py')
Export-ScheduledTask -TaskName 'Paperflow-Remote-Postprocess-Coordinator' | Set-Content -LiteralPath (Join-Path $backup 'coordinator.xml') -Encoding UTF8
Export-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206' | Set-Content -LiteralPath (Join-Path $backup 'observer.xml') -Encoding UTF8
$workerTask = Get-ScheduledTask -TaskName 'Paperflow-Remote-Postprocess-Coordinator'
$python = $workerTask.Actions[0].Execute
Stop-ScheduledTask -TaskName 'Paperflow-Remote-Postprocess-Coordinator'
Stop-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206'
$oldProcesses = @(Get-CimInstance Win32_Process | Where-Object {
    $_.Name -eq 'pythonw.exe' -and $_.CommandLine -and
    (($_.CommandLine.Contains('run_remote_postprocess_worker.py') -and $_.CommandLine.Contains($old.remote_config)) -or
     ($_.CommandLine.Contains('run_waiting_service.py') -and $_.CommandLine.Contains('14206')))
})
$oldProcesses | Sort-Object ParentProcessId -Descending | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'receipt_access.py') -Destination (Join-Path $parent 'receipt_access.py') -Force
Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'bindings-mainline.json') -Destination (Join-Path $parent 'bindings.json') -Force
$project = (Get-Content -LiteralPath $new.remote_config -Raw -Encoding UTF8 | ConvertFrom-Json).prefect_project
$worker = Join-Path $project 'run_remote_postprocess_worker.py'
$workerArguments = '-X utf8 -B "{0}" --config "{1}" --config-sha256 {2}' -f $worker, $new.remote_config, $new.remote_config_sha256
Set-ScheduledTask -TaskName 'Paperflow-Remote-Postprocess-Coordinator' -Action (New-ScheduledTaskAction -Execute $python -Argument $workerArguments) | Out-Null
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
$observerAction = New-ScheduledTaskAction -Execute "$env:WINDIR\System32\WindowsPowerShell\v1.0\powershell.exe" -Argument "-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -EncodedCommand $encoded"
Set-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206' -Action $observerAction | Out-Null
Start-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206'
Start-ScheduledTask -TaskName 'Paperflow-Remote-Postprocess-Coordinator'
Write-Output 'Switched postprocessing revision while the parent stayed paused; RAG barrier unchanged.'
