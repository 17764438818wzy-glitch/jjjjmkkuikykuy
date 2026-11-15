$ErrorActionPreference = 'Stop'
$root = 'F:\论文预处理系统\预处理系统2'
$base = Join-Path $root 'candidates\postprocess-rag-unblock-20261101'
$deploy = Join-Path $base 'receipt-reuse\deployment'
$server = Join-Path $root 'candidates\postprocess-server-20261101'
$release = Join-Path $root 'candidates\postprocess-prefect-20261031\server_release_next'
$site = Join-Path $release 'site-live-release-20261101'
$workerPath = Join-Path $root 'prefect-paperflow\run_remote_postprocess_worker.py'
$bindingPath = Join-Path $base 'bindings.json'
$old = Get-Content -LiteralPath $bindingPath -Raw -Encoding UTF8 | ConvertFrom-Json
$new = Get-Content -LiteralPath (Join-Path $deploy 'bindings.json') -Raw -Encoding UTF8 | ConvertFrom-Json
$task = Invoke-RestMethod 'http://127.0.0.1:14203/api/v1/first-step/tasks/6cb5a766-94f9-4613-aa5e-17a883be9016?page_size=1' -TimeoutSec 8
if ($task.desired_state -ne 'paused' -or $task.inflight_requests -ne 0) { throw 'The original task is not paused and idle.' }
foreach ($binding in @($old, $new)) {
    foreach ($key in @('remote_config','release_inventory','observer')) {
        if ((Get-FileHash -LiteralPath $binding.$key -Algorithm SHA256).Hash.ToLowerInvariant() -ne $binding."${key}_sha256") { throw "Binding differs: $key" }
    }
}
$oldManifest = Get-Content -LiteralPath $old.release_inventory -Raw -Encoding UTF8 | ConvertFrom-Json
foreach ($entry in $oldManifest.files.PSObject.Properties) {
    if ((Get-FileHash -LiteralPath $entry.Name -Algorithm SHA256).Hash.ToLowerInvariant() -ne $entry.Value) { throw "Live source differs: $($entry.Name)" }
}
$replacements = @(
    @{ target = (Join-Path $server 'remote_coordinator.py'); source = (Join-Path $base 'receipt-reuse\remote_coordinator.py'); backup = 'remote_coordinator.py' },
    @{ target = (Join-Path $server 'remote_runtime_adapter.py'); source = (Join-Path $base 'receipt-reuse\remote_runtime_adapter.py'); backup = 'remote_runtime_adapter.py' },
    @{ target = (Join-Path $server 'remote_server_transport.py'); source = (Join-Path $base 'receipt-reuse\remote_server_transport.py'); backup = 'remote_server_transport.py' },
    @{ target = (Join-Path $release 'ticket_positions.py'); source = (Join-Path $base 'ticket_positions.py'); backup = 'ticket_positions.py' },
    @{ target = $workerPath; source = (Join-Path $deploy 'run_remote_postprocess_worker.py'); backup = 'run_remote_postprocess_worker.py' },
    @{ target = (Join-Path $site 'index.html'); source = (Join-Path $base 'index.html'); backup = 'index.html' },
    @{ target = (Join-Path $site 'app.js'); source = (Join-Path $base 'app.js'); backup = 'app.js' },
    @{ target = (Join-Path $site 'prefect-ticket-counts-server-20261101.js'); source = (Join-Path $base 'prefect-ticket-counts-server-20261101.js'); backup = 'prefect-ticket-counts-server-20261101.js' },
    @{ target = (Join-Path $site 'three-card-positions-20261101.js'); source = (Join-Path $base 'three-card-positions-20261101.js'); backup = 'three-card-positions-20261101.js' }
)
$newManifest = Get-Content -LiteralPath $new.release_inventory -Raw -Encoding UTF8 | ConvertFrom-Json
foreach ($item in $replacements) {
    if (-not (Test-Path -LiteralPath $item.target -PathType Leaf) -or -not (Test-Path -LiteralPath $item.source -PathType Leaf)) { throw "Replacement unavailable: $($item.backup)" }
    $expected = $newManifest.files.($item.target.ToLowerInvariant())
    if ($expected -and (Get-FileHash -LiteralPath $item.source -Algorithm SHA256).Hash.ToLowerInvariant() -ne $expected) { throw "Candidate differs: $($item.backup)" }
}
$prepared = Get-Content -LiteralPath (Join-Path $deploy 'preparation-live-hashes.json') -Raw -Encoding UTF8 | ConvertFrom-Json
foreach ($entry in $prepared.PSObject.Properties) {
    if ((Get-FileHash -LiteralPath $entry.Name -Algorithm SHA256).Hash.ToLowerInvariant() -ne $entry.Value) { throw "Live worker or page changed since preparation: $($entry.Name)" }
}
$backup = Join-Path $deploy ('before-switch-' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
New-Item -ItemType Directory -Path $backup -ErrorAction Stop | Out-Null
foreach ($item in $replacements) { Copy-Item -LiteralPath $item.target -Destination (Join-Path $backup $item.backup) }
Copy-Item -LiteralPath $bindingPath -Destination (Join-Path $backup 'bindings.json')
Export-ScheduledTask -TaskName 'Paperflow-Remote-Postprocess-Coordinator' | Set-Content -LiteralPath (Join-Path $backup 'coordinator.xml') -Encoding UTF8
Export-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206' | Set-Content -LiteralPath (Join-Path $backup 'observer.xml') -Encoding UTF8
$workerTask = Get-ScheduledTask -TaskName 'Paperflow-Remote-Postprocess-Coordinator'
$pythonExe = $workerTask.Actions[0].Execute
Stop-ScheduledTask -TaskName 'Paperflow-Remote-Postprocess-Coordinator'
Stop-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206'
$processes = @(Get-CimInstance Win32_Process | Where-Object {
    $_.Name -eq 'pythonw.exe' -and $_.CommandLine -and
    (($_.CommandLine.Contains('run_remote_postprocess_worker.py') -and $_.CommandLine.Contains($old.remote_config)) -or
     ($_.CommandLine.Contains('run_waiting_service.py') -and $_.CommandLine.Contains('14206')))
})
$processes | Sort-Object ParentProcessId -Descending | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
foreach ($item in $replacements) { Copy-Item -LiteralPath $item.source -Destination $item.target -Force }
Copy-Item -LiteralPath (Join-Path $deploy 'bindings.json') -Destination $bindingPath -Force
$batchConfig = Join-Path $root 'candidates\rag-nas-prefect-20261031\batch-309.json'
$workerArgs = '-X utf8 -B "{0}" --config "{1}" --config-sha256 {2} --rag-nas-batch-config "{3}"' -f $workerPath, $new.remote_config, $new.remote_config_sha256, $batchConfig
Set-ScheduledTask -TaskName 'Paperflow-Remote-Postprocess-Coordinator' -Action (New-ScheduledTaskAction -Execute $pythonExe -Argument $workerArgs) | Out-Null
$bootstrap = @(
    '$ErrorActionPreference = ''Stop'''
    '$wrapper = ''__WRAPPER__'''
    '$expected = ''__HASH__'''
    '$bytes = [IO.File]::ReadAllBytes($wrapper)'
    '$hasher = [Security.Cryptography.SHA256]::Create()'
    'try { $actual = -join ($hasher.ComputeHash($bytes) | ForEach-Object { $_.ToString(''x2'') }) }'
    'finally { $hasher.Dispose() }'
    'if ($actual -cne $expected) { throw ''Observer wrapper source changed'' }'
    '& ([scriptblock]::Create([Text.Encoding]::UTF8.GetString($bytes)))'
    'exit $LASTEXITCODE'
) -join [Environment]::NewLine
$bootstrap = $bootstrap.Replace('__WRAPPER__', $new.observer.Replace("'", "''")).Replace('__HASH__', $new.observer_sha256)
$encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($bootstrap))
Set-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206' -Action (New-ScheduledTaskAction -Execute "$env:WINDIR\System32\WindowsPowerShell\v1.0\powershell.exe" -Argument "-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -EncodedCommand $encoded") | Out-Null
Start-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206'
Start-ScheduledTask -TaskName 'Paperflow-Remote-Postprocess-Coordinator'
Write-Output "Updated the existing worker and observer while paused. Backup: $backup"
