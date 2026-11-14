param([switch]$WaitForServer)

$ErrorActionPreference = 'Stop'
$env:PYTHONUTF8 = '1'

$candidateRoot = $PSScriptRoot
$workspaceRoot = Split-Path (Split-Path $candidateRoot -Parent) -Parent
$pythonPath = Join-Path $workspaceRoot 'prefect-paperflow\.venv\Scripts\python.exe'
$configPath = Join-Path $workspaceRoot 'prefect-paperflow\config.json'
$logRoot = Join-Path $candidateRoot 'runtime\desktop'
$pageUrl = 'http://127.0.0.1:8796/'
New-Item -ItemType Directory -Path $logRoot -Force | Out-Null

# Reopening the shortcut should reuse a running UI and leave its task state alone.
function Test-PaperflowReady {
    try {
        $apiConfig = Invoke-RestMethod -Uri ($pageUrl + 'api-config.json') -TimeoutSec 2
        return $apiConfig.control_api_url -eq ($pageUrl + 'api/v1/prefect')
    }
    catch [System.Net.WebException] {
        return $false
    }
}

try {
    if (-not (Test-PaperflowReady)) {
        # Networking can become ready after Windows signs in.
        $settings = Get-Content -LiteralPath $configPath -Raw -Encoding UTF8 | ConvertFrom-Json
        $healthUrl = $settings.prefect_api_url.TrimEnd('/') + '/health'
        $networkDeadline = if ($WaitForServer) { [datetime]::MaxValue } else { (Get-Date).AddMinutes(2) }
        $prefectReady = $false
        do {
            try {
                $health = Invoke-WebRequest -Uri $healthUrl -UseBasicParsing -TimeoutSec 3
                $prefectReady = $health.StatusCode -eq 200
            }
            catch [System.Net.WebException] {
                $prefectReady = $false
            }
            if (-not $prefectReady) { Start-Sleep -Seconds $(if ($WaitForServer) { 15 } else { 2 }) }
        } until ($prefectReady -or (Get-Date) -ge $networkDeadline)
        if (-not $prefectReady) { throw 'Prefect is unavailable. Start the server, then reopen the desktop shortcut.' }

        # The user may have opened the app while this startup entry was waiting.
        if (-not (Test-PaperflowReady)) {
            $serviceTask = Get-ScheduledTask -TaskName 'Paperflow-V3-Control-8796'
            if ($serviceTask.State -ne 'Running') {
                Start-ScheduledTask -TaskName 'Paperflow-V3-Control-8796'
            }
            $startupDeadline = (Get-Date).AddMinutes(2)
            do {
                if (Test-PaperflowReady) { break }
                if ((Get-Date) -ge $startupDeadline) { throw 'Paperflow is still starting. See the service logs, then reopen the shortcut.' }
                Start-Sleep -Seconds 2
            } while ($true)
        }
    }

    Start-Process $pageUrl
}
catch {
    $message = "Paperflow could not open.`r`n$($_.Exception.Message)`r`n`r`nLogs: $logRoot"
    $message | Set-Content -LiteralPath (Join-Path $logRoot 'startup-error.log') -Encoding UTF8
    $popup = New-Object -ComObject WScript.Shell
    $null = $popup.Popup($message, 60, 'Paperflow', 16)
    exit 1
}

