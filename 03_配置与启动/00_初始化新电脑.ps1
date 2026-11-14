$ErrorActionPreference='Stop'
$bundleRoot=Split-Path $PSScriptRoot -Parent
$pythonPath=Join-Path $bundleRoot '02_运行环境\Python314\python.exe'
$env:PYTHONUTF8='1'
$env:PYTHONDONTWRITEBYTECODE='1'
$env:PYTHONNOUSERSITE='1'
& $pythonPath -X utf8 -B (Join-Path $PSScriptRoot 'bundle_tools.py') configure
if ($LASTEXITCODE -ne 0) { throw '初始化失败，请保留错误信息检查' }
