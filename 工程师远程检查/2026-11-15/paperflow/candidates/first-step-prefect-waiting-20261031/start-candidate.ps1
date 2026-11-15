$ErrorActionPreference = 'Stop'
$candidateRoot = $PSScriptRoot
$sandboxRoot = Join-Path $candidateRoot 'sandbox'
$firstRoot = Join-Path $candidateRoot 'first-step'
$intakeRoot = Join-Path $candidateRoot 'intake'
$webRoot = Join-Path $candidateRoot 'web'
$projectRoot = (Resolve-Path -LiteralPath (Join-Path $candidateRoot '..\..')).Path
$firstPython = Join-Path $projectRoot 'prefect-first-step-independent\.venv\Scripts\python.exe'
$intakePython = Join-Path $projectRoot 'prefect-paperflow\.venv\Scripts\python.exe'
$tusd = Join-Path $projectRoot 'candidates\first-step-uppy-tusd-20261029\tusd\tusd.exe'
$config = Join-Path $firstRoot 'config.json'
$settings = Get-Content -LiteralPath $config -Raw | ConvertFrom-Json
$prefectApi = $settings.prefect_api_url.TrimEnd('/')
$receiptRoot = $settings.receipt_root
$dataRoot = $settings.data_root
$queueDb = $settings.local_intake_queue_db
$index = Join-Path $settings.classification_root 'group-task-summaries\summary-index.sqlite3'
$logRoot = Join-Path $sandboxRoot ('logs\' + (Get-Date -Format 'yyyyMMdd-HHmmss-ffff'))
$statePath = Join-Path $sandboxRoot 'processes.json'

foreach ($file in @($firstPython, $intakePython, $tusd, $config)) {
    if (-not (Test-Path -LiteralPath $file -PathType Leaf)) { throw "缺少候选运行依赖：$file" }
}
if (Test-Path -LiteralPath $statePath -PathType Leaf) {
    foreach ($entry in @(Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json)) {
        $process = Get-Process -Id $entry.id -ErrorAction SilentlyContinue
        if ($process -and $process.StartTime.ToUniversalTime().Ticks -eq $entry.startTicks) {
            $details = Get-CimInstance Win32_Process -Filter "ProcessId = $($entry.id)" -ErrorAction Stop
            if ($details -and $details.CommandLine.Contains([string]$entry.marker)) {
                throw '仍有已记录的候选进程在运行，请先安全停止候选。'
            }
        }
    }
}
foreach ($port in @(15180, 15203, 15205, 18795)) {
    if (Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue) {
        throw "端口 $port 已被占用，没有启动或修改该进程。"
    }
}
foreach ($dir in @($sandboxRoot, $logRoot, $settings.library_root, $dataRoot,
                   (Join-Path $dataRoot 'tusd'), $receiptRoot, (Split-Path -Parent $queueDb))) {
    New-Item -ItemType Directory -Path $dir -Force | Out-Null
}
$env:PAPERFLOW_CONFIG = $config
$env:PREFECT_API_URL = $prefectApi
$env:PREFECT_API_REQUEST_TIMEOUT = '3'
$env:PREFECT_CLIENT_MAX_RETRIES = '0'
$env:PYTHONUTF8 = '1'

# The explicit setup registers a separate deployment and a paused work pool;
# it does not execute a folder or attach a worker to that pool.
& $intakePython (Join-Path $intakeRoot 'register_waiting_deployment.py') --prefect-api $prefectApi
if ($LASTEXITCODE -ne 0) { throw '等待工单部署登记失败，候选服务未启动。' }

$started = [System.Collections.Generic.List[object]]::new()
function Start-CandidateRole([string]$name, [string]$file, [string[]]$processArgs,
                             [string]$workingDirectory, [string]$marker) {
    $process = Start-Process -FilePath $file -ArgumentList $processArgs -WorkingDirectory $workingDirectory `
        -WindowStyle Hidden -RedirectStandardOutput (Join-Path $logRoot "$name.out.log") `
        -RedirectStandardError (Join-Path $logRoot "$name.err.log") -PassThru
    $started.Add([pscustomobject]@{ name=$name; id=$process.Id; marker=$marker;
                                   startTicks=$process.StartTime.ToUniversalTime().Ticks })
}
try {
    Start-CandidateRole 'tusd' $tusd @('-host','127.0.0.1','-port','15180','-upload-dir',
        (Join-Path $dataRoot 'tusd'),'-hooks-http',
        'http://127.0.0.1:15203/api/v1/first-step/internal/tusd-hooks','-hooks-enabled-events',
        'pre-create,pre-finish','-disable-download','-disable-termination','-disable-cors') $firstRoot '15180'
    Start-CandidateRole 'first-api' $firstPython @('-m','uvicorn','summary_api:app',
        '--host','127.0.0.1','--port','15203','--no-access-log') $firstRoot 'summary_api:app'
    Start-CandidateRole 'first-worker' $firstPython @((Join-Path $firstRoot 'serve_controlled.py')) `
        $firstRoot 'serve_controlled.py'
    $common = @('--db',$queueDb,'--prefect-api',$prefectApi,'--first-step-index',$index,
                '--first-step-data-root',$dataRoot)
    Start-CandidateRole 'intake-watch' $intakePython (@((Join-Path $intakeRoot 'waiting_cli.py'),'watch') +
        $common + @('--receipt-root',$receiptRoot,'--interval-seconds','2')) $intakeRoot 'waiting_cli.py'
    Start-CandidateRole 'waiting-api' $intakePython (@((Join-Path $intakeRoot 'waiting_cli.py'),'serve') +
        $common + @('--port','15205')) $intakeRoot 'waiting_cli.py'
    Start-CandidateRole 'page' $intakePython @('-m','http.server','18795','--bind','127.0.0.1') `
        $webRoot '18795'
    $started | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $statePath -Encoding utf8
    Write-Host '已请求启动隔离候选；是否就绪需要由您自行核验。'
    Write-Host '候选页面：http://127.0.0.1:18795/'
    Write-Host '候选日志位于 sandbox\logs；未清理或启动任何旧任务。'
} catch {
    foreach ($item in $started) {
        $process = Get-Process -Id $item.id -ErrorAction SilentlyContinue
        if ($process -and $process.StartTime.ToUniversalTime().Ticks -eq $item.startTicks) {
            Stop-Process -Id $item.id -ErrorAction SilentlyContinue
        }
    }
    throw
}
