$ErrorActionPreference='Stop'
$bundleRoot=Split-Path $PSScriptRoot -Parent
$pythonPath=Join-Path $bundleRoot '02_运行环境\Python314\python.exe'
& $pythonPath -X utf8 -B -s (Join-Path $PSScriptRoot 'bundle_tools.py') verify-payload
if ($LASTEXITCODE -ne 0) { throw '交付文件与清单不同；若已初始化请区分预期配置改动与复制损坏' }
