param([switch]$Quiet)
$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot
$exe = Join-Path $root 'runtime\ollama.exe'
$baseUrl = 'http://127.0.0.1:11434'
if (-not (Test-Path -LiteralPath $exe)) { throw "Ollama executable is missing: $exe" }
$listener = Get-NetTCPConnection -State Listen -LocalPort 11434 -ErrorAction SilentlyContinue | Select-Object -First 1
if ($listener) {
    $owner = Get-Process -Id $listener.OwningProcess -ErrorAction Stop
    if ($owner.Path -ne $exe) { throw "Port 11434 belongs to another program (PID $($owner.Id))." }
    $version = Invoke-RestMethod "$baseUrl/api/version" -TimeoutSec 5
    if (-not $Quiet) { Write-Host "Nimble Ollama is already running at $baseUrl (version $($version.version))." }
    return
}
$env:OLLAMA_HOST = '127.0.0.1:11434'
$env:OLLAMA_MODELS = Join-Path $root 'models'
$env:OLLAMA_CONTEXT_LENGTH = '4096'
$env:OLLAMA_NUM_PARALLEL = '1'
$env:OLLAMA_MAX_LOADED_MODELS = '1'
$env:OLLAMA_KEEP_ALIVE = '5m'
$env:OLLAMA_FLASH_ATTENTION = '1'
$env:OLLAMA_NO_CLOUD = '1'
$env:NO_PROXY = '127.0.0.1,localhost,::1'
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss-fff'
$stdout = Join-Path $root "logs\ollama-$stamp.stdout.log"
$stderr = Join-Path $root "logs\ollama-$stamp.stderr.log"
$process = Start-Process -FilePath $exe -ArgumentList 'serve' -WorkingDirectory $root -WindowStyle Hidden -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru
$record = [ordered]@{ pid = $process.Id; startedUtc = $process.StartTime.ToUniversalTime().ToString('o'); executable = $exe; url = $baseUrl; stdout = $stdout; stderr = $stderr }
$record | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $root 'logs\service.json') -Encoding UTF8
for ($attempt = 0; $attempt -lt 80; $attempt++) {
    if ($process.HasExited) { throw "Ollama exited. Read $stderr" }
    try {
        $version = Invoke-RestMethod "$baseUrl/api/version" -TimeoutSec 1
        if (-not $Quiet) { Write-Host "Nimble Ollama started at $baseUrl (version $($version.version))." }
        return
    } catch { Start-Sleep -Milliseconds 250 }
}
throw "Ollama did not become ready. Read $stderr"
