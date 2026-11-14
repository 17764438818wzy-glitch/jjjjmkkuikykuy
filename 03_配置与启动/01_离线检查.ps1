$ErrorActionPreference='Stop'
$bundleRoot=Split-Path $PSScriptRoot -Parent
$pythonPath=Join-Path $bundleRoot '01_程序文件\论文预处理系统\预处理系统2\prefect-paperflow\.venv\Scripts\python.exe'
$env:PYTHONUTF8='1'
$env:PYTHONDONTWRITEBYTECODE='1'
$env:PYTHONNOUSERSITE='1'
if (-not (Test-Path -LiteralPath $pythonPath)) { throw '请先执行00_初始化新电脑.ps1' }
& $pythonPath -X utf8 -B (Join-Path $PSScriptRoot 'bundle_tools.py') 'offline-check'
if ($LASTEXITCODE -ne 0) { throw '步骤失败，请按错误修正，勿继续启动' }
