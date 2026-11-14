param([switch]$TasksPaused)
$ErrorActionPreference='Stop'
$bundleRoot=Split-Path $PSScriptRoot -Parent
$pythonPath=Join-Path $bundleRoot '01_程序文件\论文预处理系统\预处理系统2\prefect-paperflow\.venv\Scripts\python.exe'
$env:PYTHONUTF8='1'
$env:PYTHONDONTWRITEBYTECODE='1'
$env:PYTHONNOUSERSITE='1'
if (-not (Test-Path -LiteralPath $pythonPath)) { throw '请先执行00_初始化新电脑.ps1' }
$extra=@()
if ($TasksPaused) { $extra+= '--tasks-paused' }
& $pythonPath -X utf8 -B (Join-Path $PSScriptRoot 'service_tools.py') stop @extra
if ($LASTEXITCODE -ne 0) { throw '停止未完成，请检查进程记录' }
