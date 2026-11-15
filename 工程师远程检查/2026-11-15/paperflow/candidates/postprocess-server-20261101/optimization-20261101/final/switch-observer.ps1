$ErrorActionPreference = 'Stop'
$binding = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'bindings.json') -Raw -Encoding UTF8 | ConvertFrom-Json
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
$bootstrap = $bootstrap.Replace('__WRAPPER__', $binding.wrapper_path.Replace("'", "''")).Replace('__HASH__', $binding.wrapper_sha256)
$encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($bootstrap))
$action = New-ScheduledTaskAction -Execute "$env:WINDIR\System32\WindowsPowerShell\v1.0\powershell.exe" -Argument "-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -EncodedCommand $encoded"
Set-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206' -Action $action | Out-Null
Stop-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206'
$old = @(Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'pythonw.exe' -and $_.CommandLine -like '*--port*14206*' })
$old | Sort-Object ParentProcessId -Descending | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
Start-ScheduledTask -TaskName 'Paperflow-Postprocessing-Observer-14206'
Write-Output 'observer action updated and scheduled'
