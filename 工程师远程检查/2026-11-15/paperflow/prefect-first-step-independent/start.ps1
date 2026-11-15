$ErrorActionPreference = 'Stop'
$taskRoot = $PSScriptRoot
$runtimeDir = Join-Path $taskRoot 'runtime'
$python = Join-Path $taskRoot '.venv\Scripts\python.exe'
$env:PREFECT_HOME = Join-Path $runtimeDir 'prefect-home'
$env:PREFECT_API_URL = 'http://127.0.0.1:4201/api'
$dbPath = (Join-Path $runtimeDir 'prefect-first-step.db').Replace('\', '/')
$env:PREFECT_SERVER_DATABASE_CONNECTION_URL = 'sqlite+aiosqlite:///' + $dbPath
$env:PREFECT_SERVER_ANALYTICS_ENABLED = 'false'
$env:PYTHONIOENCODING = 'utf-8'

foreach ($port in 4201, 4203) {
    if (Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue) {
        throw "端口 $port 已被占用；未启动新实例。"
    }
}

& (Join-Path $taskRoot 'start-nimble.ps1')

$server = Start-Process -FilePath $python -ArgumentList @('-X', 'utf8', '-m', 'prefect', 'server', 'start', '--host', '127.0.0.1', '--port', '4201', '--analytics-off', '--no-scheduler') `
    -WorkingDirectory $taskRoot -RedirectStandardOutput (Join-Path $runtimeDir 'server.stdout.log') `
    -RedirectStandardError (Join-Path $runtimeDir 'server.stderr.log') -WindowStyle Hidden -PassThru
for ($i = 0; $i -lt 45; $i++) {
    try {
        $health = Invoke-WebRequest -Uri 'http://127.0.0.1:4201/api/health' -TimeoutSec 2 -UseBasicParsing
        if ($health.StatusCode -eq 200) { break }
    } catch { Start-Sleep -Seconds 1 }
}
if (-not $health -or $health.StatusCode -ne 200) { throw '新 Prefect 尚未就绪，请查看独立日志。' }

$api = Start-Process -FilePath $python -ArgumentList @('-X', 'utf8', '-m', 'uvicorn', 'summary_api:app', '--host', '127.0.0.1', '--port', '4203') `
    -WorkingDirectory $taskRoot -RedirectStandardOutput (Join-Path $runtimeDir 'api.stdout.log') `
    -RedirectStandardError (Join-Path $runtimeDir 'api.stderr.log') -WindowStyle Hidden -PassThru
$runner = Start-Process -FilePath $python -ArgumentList @('-X', 'utf8', 'manage.py') `
    -WorkingDirectory $taskRoot -RedirectStandardOutput (Join-Path $runtimeDir 'runner.stdout.log') `
    -RedirectStandardError (Join-Path $runtimeDir 'runner.stderr.log') -WindowStyle Hidden -PassThru
@{server=$server.Id; runner=$runner.Id; summary_api=$api.Id; started_at=(Get-Date).ToString('o')} |
    ConvertTo-Json | Set-Content -LiteralPath (Join-Path $runtimeDir 'processes.json') -Encoding UTF8
Write-Output "新实例已启动：Prefect 127.0.0.1:4201，统计接口 127.0.0.1:4203。"
