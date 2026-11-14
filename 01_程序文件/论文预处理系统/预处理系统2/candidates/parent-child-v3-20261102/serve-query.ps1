$ErrorActionPreference='Stop'
$env:PYTHONUTF8='1'
$env:PYTHONDONTWRITEBYTECODE='1'
$candidateRoot=$PSScriptRoot
$projectRoot=Join-Path (Split-Path (Split-Path $candidateRoot -Parent) -Parent) 'prefect-paperflow'
$pythonPath=Join-Path $projectRoot '.venv\Scripts\python.exe'
$projectConfig=Join-Path $projectRoot 'config.json'
$settings=Get-Content -LiteralPath $projectConfig -Raw -Encoding UTF8 | ConvertFrom-Json
$env:PREFECT_API_URL=$settings.prefect_api_url
$env:PREFECT_SERVER_ALLOW_EPHEMERAL_MODE='false'
$env:PREFECT_SERVER_ANALYTICS_ENABLED='false'
$logs=Join-Path $candidateRoot 'runtime\dashboard-query'
$cache=Join-Path $logs 'current'
New-Item -ItemType Directory -Path $logs -Force | Out-Null
Set-Location -LiteralPath $candidateRoot
$arguments=@('-m','paperflow_v3.control.dashboard_api','--prefect-api-url',$settings.prefect_api_url,
  '--cache-dir',('"'+$cache+'"'),'--project-config',('"'+$projectConfig+'"'),
  '--core-root',('"'+$projectRoot+'"'),'--port','8826')
$serviceProcess=Start-Process -FilePath $pythonPath -ArgumentList $arguments -WorkingDirectory $candidateRoot -WindowStyle Hidden -RedirectStandardOutput (Join-Path $logs 'stdout.log') -RedirectStandardError (Join-Path $logs 'stderr.log') -Wait -PassThru
exit $serviceProcess.ExitCode
