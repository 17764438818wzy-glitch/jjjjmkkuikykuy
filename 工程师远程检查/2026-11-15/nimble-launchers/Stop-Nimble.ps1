$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot
$recordPath = Join-Path $root 'logs\service.json'
if (-not (Test-Path -LiteralPath $recordPath)) { Write-Host 'No Nimble service record exists.'; return }
$record = Get-Content -LiteralPath $recordPath -Raw -Encoding UTF8 | ConvertFrom-Json
$process = Get-Process -Id $record.pid -ErrorAction SilentlyContinue
if (-not $process) { Write-Host 'Nimble Ollama is already stopped.'; return }
$expectedExe = Join-Path $root 'runtime\ollama.exe'
if ($process.Path -ne $expectedExe -or $process.StartTime.ToUniversalTime().ToString('o') -ne $record.startedUtc) { throw 'The saved process identity changed; no process was stopped.' }
$env:OLLAMA_HOST = '127.0.0.1:11434'
$loaded = Invoke-RestMethod 'http://127.0.0.1:11434/api/ps' -TimeoutSec 5
foreach ($model in $loaded.models) {
    & $expectedExe stop $model.name
    if ($LASTEXITCODE -ne 0) { throw "Could not unload $($model.name); service was not stopped." }
}
Stop-Process -Id $process.Id -ErrorAction Stop
Write-Host 'Nimble Ollama stopped. Downloaded models are retained.'
