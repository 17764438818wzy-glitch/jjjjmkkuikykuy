$ErrorActionPreference = 'Stop'
$binding = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'bindings.json') -Raw -Encoding UTF8 | ConvertFrom-Json
$config = Get-Content -LiteralPath $binding.config_path -Raw -Encoding UTF8 | ConvertFrom-Json
$taskName = 'Paperflow-Remote-Postprocess-Coordinator'
$current = Get-ScheduledTask -TaskName $taskName
$runner = Join-Path $config.prefect_project 'run_remote_postprocess_worker.py'
$arguments = '-X utf8 -B "{0}" --config "{1}" --config-sha256 {2}' -f $runner, $binding.config_path, $binding.config_sha256
$action = New-ScheduledTaskAction -Execute $current.Actions[0].Execute -Argument $arguments
Set-ScheduledTask -TaskName $taskName -Action $action | Out-Null
Stop-ScheduledTask -TaskName $taskName
$oldConfig = Join-Path (Split-Path $PSScriptRoot -Parent) 'remote-config.json'
$old = @(Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'pythonw.exe' -and $_.CommandLine -and $_.CommandLine.Contains($oldConfig) -and $_.CommandLine.Contains('run_remote_postprocess_worker.py') })
$old | Sort-Object ParentProcessId -Descending | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
Start-ScheduledTask -TaskName $taskName
Write-Output 'coordinator action updated and scheduled'
