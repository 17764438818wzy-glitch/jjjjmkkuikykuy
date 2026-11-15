$ErrorActionPreference = 'Stop'
$root = 'F:\论文预处理系统\预处理系统2'
$base = Join-Path $root 'candidates\postprocess-rag-unblock-20261101'
$candidate = Join-Path $base 'frontend-positions-freshness'
$targetDir = Join-Path $root 'candidates\postprocess-prefect-20261031\server_release_next'
$names = @('waiting_api.py', 'ticket_positions.py')
$bindingPath = Join-Path $base 'bindings.json'
$old = Get-Content -LiteralPath $bindingPath -Raw -Encoding UTF8 | ConvertFrom-Json
$new = Get-Content -LiteralPath (Join-Path $candidate 'bindings.json') -Raw -Encoding UTF8 | ConvertFrom-Json
$oldManifest = Get-Content -LiteralPath $old.release_inventory -Raw -Encoding UTF8 | ConvertFrom-Json
$newManifest = Get-Content -LiteralPath $new.release_inventory -Raw -Encoding UTF8 | ConvertFrom-Json
foreach ($entry in $oldManifest.files.PSObject.Properties) {
    if ((Get-FileHash -LiteralPath $entry.Name -Algorithm SHA256).Hash.ToLowerInvariant() -ne $entry.Value) {
        throw 'Existing observer release source changed.'
    }
}
foreach ($name in $names) {
    $newKey = (Join-Path $targetDir $name).ToLowerInvariant()
    if ((Get-FileHash -LiteralPath (Join-Path $candidate $name) -Algorithm SHA256).Hash.ToLowerInvariant() -ne $newManifest.files.$newKey) {
        throw 'Candidate position source differs from its release inventory.'
    }
}
foreach ($entry in @(@($new.release_inventory, $new.release_inventory_sha256), @($new.observer, $new.observer_sha256))) {
    if ((Get-FileHash -LiteralPath $entry[0] -Algorithm SHA256).Hash.ToLowerInvariant() -ne $entry[1]) {
        throw 'Prepared observer release differs.'
    }
}
$backup = Join-Path $candidate ('before-switch-' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
New-Item -ItemType Directory -Path $backup | Out-Null
foreach ($name in $names) { Copy-Item -LiteralPath (Join-Path $targetDir $name) -Destination (Join-Path $backup $name) }
Copy-Item -LiteralPath $bindingPath -Destination (Join-Path $backup 'bindings.json')
Export-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206' | Set-Content -LiteralPath (Join-Path $backup 'observer.xml') -Encoding UTF8
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
Stop-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206'
$observers = @(Get-CimInstance Win32_Process | Where-Object {
    $_.Name -eq 'pythonw.exe' -and $_.CommandLine -and
    $_.CommandLine.Contains('run_waiting_service.py') -and $_.CommandLine.Contains('14206')
})
$observers | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
foreach ($name in $names) { Copy-Item -LiteralPath (Join-Path $candidate $name) -Destination (Join-Path $targetDir $name) -Force }
Copy-Item -LiteralPath (Join-Path $candidate 'bindings.json') -Destination $bindingPath -Force
Set-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206' -Action (New-ScheduledTaskAction -Execute "$env:WINDIR\System32\WindowsPowerShell\v1.0\powershell.exe" -Argument "-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -EncodedCommand $encoded") | Out-Null
Start-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206'
Write-Output "Only the existing read-only observer was restarted. Backup: $backup"
