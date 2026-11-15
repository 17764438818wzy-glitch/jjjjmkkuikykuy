$ErrorActionPreference='Stop'
$env:PYTHONUTF8='1'
$env:PYTHONDONTWRITEBYTECODE='1'
$candidateRoot=$PSScriptRoot
$workspaceRoot=Split-Path (Split-Path $candidateRoot -Parent) -Parent
$pythonPath=Join-Path $workspaceRoot 'prefect-paperflow\.venv\Scripts\python.exe'
$logs=Join-Path $candidateRoot 'runtime\desktop'
New-Item -ItemType Directory -Path $logs -Force|Out-Null
Set-Location -LiteralPath $candidateRoot
$serviceProcess=Start-Process -FilePath $pythonPath -ArgumentList @('-m','paperflow_v3.control.launch') -WorkingDirectory $candidateRoot -WindowStyle Hidden -RedirectStandardOutput (Join-Path $logs 'service.stdout.log') -RedirectStandardError (Join-Path $logs 'service.stderr.log') -Wait -PassThru
exit $serviceProcess.ExitCode
