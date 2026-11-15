$ErrorActionPreference = 'Stop'
$taskRoot = $PSScriptRoot
$runtimeDir = Join-Path $taskRoot 'runtime'
New-Item -ItemType Directory -Path $runtimeDir -Force | Out-Null
$processRecord = Join-Path $runtimeDir 'runner-process.json'
if (Test-Path -LiteralPath $processRecord) {
    $record = Get-Content -LiteralPath $processRecord -Raw -Encoding UTF8 | ConvertFrom-Json
    $existing = Get-CimInstance Win32_Process -Filter "ProcessId = $($record.pid)"
    if ($existing -and $existing.CommandLine -like '*manage.py*serve*') {
        Write-Output "Runner already running: $($record.pid)"
        exit 0
    }
}
$runner = Start-Process -FilePath (Join-Path $taskRoot '.venv\Scripts\python.exe') `
    -ArgumentList @('-X', 'utf8', 'manage.py', 'serve') -WorkingDirectory $taskRoot `
    -RedirectStandardOutput (Join-Path $runtimeDir 'runner.stdout.log') `
    -RedirectStandardError (Join-Path $runtimeDir 'runner.stderr.log') -WindowStyle Hidden -PassThru
Write-Output "Started runner launcher: $($runner.Id)"
