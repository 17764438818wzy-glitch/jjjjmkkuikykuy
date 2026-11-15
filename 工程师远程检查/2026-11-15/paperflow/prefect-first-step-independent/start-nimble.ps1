$ErrorActionPreference = 'Stop'
$taskRoot = $PSScriptRoot
$runtimeDir = Join-Path $taskRoot 'runtime'
$exe = 'F:\nimble\runtime\ollama.exe'
$models = 'F:\nimble\models'
$recordPath = Join-Path $runtimeDir 'nimble-process.json'
$listener = Get-NetTCPConnection -State Listen -LocalPort 11435 -ErrorAction SilentlyContinue | Select-Object -First 1
if ($listener) {
    if (-not (Test-Path -LiteralPath $recordPath)) { throw 'Port 11435 is occupied by an unrecognized process.' }
    $record = Get-Content -Raw -LiteralPath $recordPath -Encoding UTF8 | ConvertFrom-Json
    $owner = Get-Process -Id $listener.OwningProcess -ErrorAction Stop
    if ($owner.Id -ne $record.pid -or $owner.Path -ne $exe) { throw 'Port 11435 belongs to another process.' }
    $version = Invoke-RestMethod 'http://127.0.0.1:11435/api/version' -TimeoutSec 5
    Write-Output "Independent Nimble is already running (PID $($owner.Id))."
    return
}
if (-not (Test-Path -LiteralPath $exe) -or -not (Test-Path -LiteralPath $models)) {
    throw 'Existing Nimble executable or model directory is missing.'
}
$env:OLLAMA_HOST = '127.0.0.1:11435'
$env:OLLAMA_MODELS = $models
$env:OLLAMA_CONTEXT_LENGTH = '4096'
$env:OLLAMA_NUM_PARALLEL = '1'
$env:OLLAMA_MAX_LOADED_MODELS = '1'
$env:OLLAMA_KEEP_ALIVE = '5m'
$env:OLLAMA_FLASH_ATTENTION = '1'
$env:OLLAMA_NO_CLOUD = '1'
$env:OLLAMA_NOPRUNE = '1'
$env:NO_PROXY = '127.0.0.1,localhost,::1'
$process = Start-Process -FilePath $exe -ArgumentList 'serve' -WorkingDirectory 'F:\nimble' `
    -RedirectStandardOutput (Join-Path $runtimeDir 'nimble.stdout.log') `
    -RedirectStandardError (Join-Path $runtimeDir 'nimble.stderr.log') -WindowStyle Hidden -PassThru
@{pid=$process.Id; started_at=(Get-Date).ToString('o'); endpoint='http://127.0.0.1:11435'; parallel=1} |
    ConvertTo-Json | Set-Content -LiteralPath $recordPath -Encoding UTF8
for ($i = 0; $i -lt 40; $i++) {
    if ($process.HasExited) { throw 'Independent Nimble failed to start; inspect its runtime logs.' }
    try {
        $version = Invoke-RestMethod 'http://127.0.0.1:11435/api/version' -TimeoutSec 1
        Write-Output "Independent Nimble started on port 11435 (PID $($process.Id))."
        return
    } catch { Start-Sleep -Milliseconds 250 }
}
throw 'Independent Nimble did not become ready; inspect its runtime logs.'
