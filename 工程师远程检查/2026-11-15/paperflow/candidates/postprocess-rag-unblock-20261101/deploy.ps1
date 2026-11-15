$ErrorActionPreference = 'Stop'
$binding = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'bindings.json') -Raw -Encoding UTF8 | ConvertFrom-Json
$project = (Get-Content -LiteralPath $binding.remote_config -Raw -Encoding UTF8 | ConvertFrom-Json).prefect_project
$root = Split-Path $project -Parent
$worker = Join-Path $project 'run_remote_postprocess_worker.py'
$runtime = Join-Path $root 'candidates\postprocess-prefect-20261031\runtime_next\stage_runtime.py'
$gates = Join-Path $root 'candidates\postprocess-prefect-20261031\stage_gates.py'
$originals = @{ $worker = 'run_remote_postprocess_worker.py'; $runtime = 'stage_runtime.py'; $gates = 'stage_gates.py' }
foreach ($target in $originals.Keys) {
    $expected = $binding.old_sources.$target
    if ((Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash.ToLowerInvariant() -ne $expected) {
        throw "Original source changed: $target"
    }
}
$backup = Join-Path $PSScriptRoot 'original'
New-Item -ItemType Directory -Path $backup -Force | Out-Null
foreach ($target in $originals.Keys) {
    Copy-Item -LiteralPath $target -Destination (Join-Path $backup $originals[$target])
}
Export-ScheduledTask -TaskName 'Paperflow-Remote-Postprocess-Coordinator' | Set-Content -LiteralPath (Join-Path $backup 'coordinator-task.xml') -Encoding UTF8
Export-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206' | Set-Content -LiteralPath (Join-Path $backup 'observer-task.xml') -Encoding UTF8
$workerTask = Get-ScheduledTask -TaskName 'Paperflow-Remote-Postprocess-Coordinator'
$python = $workerTask.Actions[0].Execute
Stop-ScheduledTask -TaskName 'Paperflow-Remote-Postprocess-Coordinator'
Stop-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206'
$oldConfig = Join-Path $root 'candidates\postprocess-server-20261101\optimization-20261101\final\remote-config.json'
$old = @(Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'pythonw.exe' -and $_.CommandLine -and (($_.CommandLine.Contains('run_remote_postprocess_worker.py') -and $_.CommandLine.Contains($oldConfig)) -or $_.CommandLine -like '*--port*14206*') })
$old | Sort-Object ParentProcessId -Descending | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
foreach ($target in $originals.Keys) {
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot $originals[$target]) -Destination $target -Force
}
$workerArguments = '-X utf8 -B "{0}" --config "{1}" --config-sha256 {2}' -f $worker, $binding.remote_config, $binding.remote_config_sha256
$workerAction = New-ScheduledTaskAction -Execute $python -Argument $workerArguments
Set-ScheduledTask -TaskName 'Paperflow-Remote-Postprocess-Coordinator' -Action $workerAction | Out-Null
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
$bootstrap = $bootstrap.Replace('__WRAPPER__', $binding.observer.Replace("'", "''")).Replace('__HASH__', $binding.observer_sha256)
$encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($bootstrap))
$observerAction = New-ScheduledTaskAction -Execute "$env:WINDIR\System32\WindowsPowerShell\v1.0\powershell.exe" -Argument "-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -EncodedCommand $encoded"
Set-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206' -Action $observerAction | Out-Null
Start-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206'
Start-ScheduledTask -TaskName 'Paperflow-Remote-Postprocess-Coordinator'
Write-Output 'Updated postprocessing dependencies; RAG barrier and task state were not changed.'
