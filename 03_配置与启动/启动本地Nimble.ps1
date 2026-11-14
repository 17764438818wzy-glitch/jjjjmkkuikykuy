$ErrorActionPreference='Stop'
$bundleRoot=Split-Path $PSScriptRoot -Parent
& (Join-Path $bundleRoot '02_运行环境\nimble\Start-Nimble.ps1')
