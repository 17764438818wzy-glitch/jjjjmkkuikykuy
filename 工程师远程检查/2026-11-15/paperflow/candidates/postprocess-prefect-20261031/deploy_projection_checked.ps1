$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot
$workspace = (Resolve-Path -LiteralPath (Join-Path $root '../..')).Path
$live = Join-Path $workspace 'candidates/local-intake-live-link-20261031'
$service = 'Paperflow-LocalIntake-Api-14205'
$receiver = 'Paperflow-LocalIntake-Receiver-14205'
$items = @(
    @{name='waiting_api.py'; source=(Join-Path $root 'api_next/waiting_api.py'); old='2d2c5e05f251bd78bbe69efa38bf2312961c01b5795dae52cd657c7efe6795db'; new='7a62ac20aefe4915c8a3a941743de34581d4425749c6fc2414f99468e0e9c088'},
    @{name='waiting_cli.py'; source=(Join-Path $root 'projection_next/waiting_cli.py'); old='c6e5dc5e13bca1b15a5606f099982ffb164cb74cc26d8becdf6ed95d3f7433c7'; new='7f0337c8526e17757516c9c74cbfa6d9d46d61a95fa757e95228744e641e5086'}
)

function Hash($path) { (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash }
function ApiProcesses {
    @(Get-CimInstance Win32_Process | Where-Object {
        $_.Name -match '^pythonw?\.exe$' -and $_.CommandLine -match 'run_waiting_service\.py' -and
        $_.CommandLine -match '\sserve\s' -and $_.CommandLine -match '--port\s+14205'
    })
}
function Wait-PortFree {
    $until = (Get-Date).AddSeconds(25)
    do {
        $listen = @(Get-NetTCPConnection -LocalPort 14205 -State Listen -ErrorAction SilentlyContinue)
        if ($listen.Count -eq 0 -and @(ApiProcesses).Count -eq 0 -and (Get-ScheduledTask -TaskName $service).State -ne 'Running') { return }
        Start-Sleep -Milliseconds 300
    } while ((Get-Date) -lt $until)
    throw '14205 API did not stop'
}
function Wait-Api {
    $until = (Get-Date).AddSeconds(30)
    do {
        try {
            $value = Invoke-RestMethod -Uri 'http://127.0.0.1:14205/api/v1/local-intake/preprocessing/summary' -TimeoutSec 3
            if ((Get-ScheduledTask -TaskName $service).State -eq 'Running' -and $null -ne $value.receiver_status) { return $value }
        } catch { }
        Start-Sleep -Milliseconds 500
    } while ((Get-Date) -lt $until)
    throw '14205 API did not respond after restart'
}

if ((Get-ScheduledTask -TaskName $service).State -ne 'Running' -or (Get-ScheduledTask -TaskName $receiver).State -ne 'Running') { throw 'API or receiver was not originally running' }
$listener = @(Get-NetTCPConnection -LocalPort 14205 -State Listen -ErrorAction Stop)
if ($listener.Count -ne 1 -or $listener[0].LocalAddress -ne '127.0.0.1') { throw '14205 listener is unexpected' }
$listenerPid = $listener[0].OwningProcess
$process = Get-CimInstance Win32_Process -Filter ('ProcessId=' + $listenerPid)
if ($null -eq $process -or $process.CommandLine -notmatch 'run_waiting_service\.py' -or $process.CommandLine -notmatch '\sserve\s') { throw '14205 listener does not match API service' }
foreach ($item in $items) {
    if ((Hash (Join-Path $live $item.name)) -ne $item.old -or (Hash $item.source) -ne $item.new) { throw ('File baseline changed: ' + $item.name) }
}
$projection = Join-Path $root 'projection_next/projections.py'
if ((Hash $projection) -ne '5205036975773e2b8f9e21bca5373c6295ef57cc58130eedd16060eafd8a2bab') { throw 'Projection baseline changed' }
$backup = Join-Path $root ('deployment-backups/projection-api-' + (Get-Date -Format 'yyyyMMdd-HHmmss-ffffff'))
New-Item -ItemType Directory -Path $backup | Out-Null
foreach ($item in $items) {
    Copy-Item -LiteralPath (Join-Path $live $item.name) -Destination (Join-Path $backup $item.name)
    if ((Hash (Join-Path $backup $item.name)) -ne $item.old) { throw ('Backup hash mismatch: ' + $item.name) }
}
$touched = $false
$attempted = @()
try {
    $touched = $true
    Stop-ScheduledTask -TaskName $service
    Wait-PortFree
    if ((Get-ScheduledTask -TaskName $receiver).State -ne 'Running') { throw 'Receiver was interrupted' }
    foreach ($item in $items) {
        $target = Join-Path $live $item.name
        if ((Hash $target) -ne $item.old -or (Hash $item.source) -ne $item.new) { throw ('File changed after stop: ' + $item.name) }
        $temporary = Join-Path $live ('.projection-update-' + $item.name + '.tmp')
        Copy-Item -LiteralPath $item.source -Destination $temporary
        $attempted += $item
        Move-Item -LiteralPath $temporary -Destination $target -Force
        if ((Hash $target) -ne $item.new) { throw ('Installed file mismatch: ' + $item.name) }
    }
    Start-ScheduledTask -TaskName $service
    $value = Wait-Api
    if ($null -eq $value.postprocessing_results) { throw 'New API projection field is missing' }
    if ((Get-ScheduledTask -TaskName $receiver).State -ne 'Running') { throw 'Receiver is not running after API restart' }
    [pscustomobject]@{status='installed';backup=$backup;receiver_status=$value.receiver_status;waiting_available=$value.waiting_available} | ConvertTo-Json -Compress
} catch {
    $cause = $_.Exception.Message
    $recovery = 'not attempted'
    if ($touched) {
        try {
            foreach ($item in $items) {
                $actual = Hash (Join-Path $live $item.name)
                $wasAttempted = @($attempted | Where-Object { $_.name -eq $item.name }).Count -gt 0
                if ($actual -ne $item.old -and (-not $wasAttempted -or $actual -ne $item.new)) { throw ('Conflicting file change: ' + $item.name) }
                if ((Hash (Join-Path $backup $item.name)) -ne $item.old) { throw ('Backup changed: ' + $item.name) }
            }
            if ((Get-ScheduledTask -TaskName $service).State -eq 'Running') { Stop-ScheduledTask -TaskName $service }
            Wait-PortFree
            foreach ($item in $attempted) {
                if ((Hash (Join-Path $live $item.name)) -eq $item.new) {
                    Copy-Item -LiteralPath (Join-Path $backup $item.name) -Destination (Join-Path $live $item.name) -Force
                }
            }
            Start-ScheduledTask -TaskName $service
            $null = Wait-Api
            $recovery = 'original API files restored and service responded'
        } catch { $recovery = 'not confirmed: ' + $_.Exception.Message }
    }
    throw ('Projection update failed: ' + $cause + '; recovery ' + $recovery)
}
