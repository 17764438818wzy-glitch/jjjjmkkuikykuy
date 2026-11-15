param([string]$Text)
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
& (Join-Path $PSScriptRoot 'Start-Nimble.ps1') -Quiet
$payload = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'examples\classification-zh.json') -Raw -Encoding UTF8 | ConvertFrom-Json
if ($Text) { $payload.state = $Text }
$body = [Text.Encoding]::UTF8.GetBytes(($payload | ConvertTo-Json -Depth 12))
$timer = [Diagnostics.Stopwatch]::StartNew()
$result = Invoke-RestMethod -Uri 'http://127.0.0.1:11434/v1/systemone' -Method Post -ContentType 'application/json; charset=utf-8' -Body $body -TimeoutSec 300
$timer.Stop()
$result | ConvertTo-Json -Depth 16 | Set-Content -LiteralPath (Join-Path $PSScriptRoot 'examples\last-result.json') -Encoding UTF8
$result | ConvertTo-Json -Depth 16
Write-Host ('Elapsed: {0:N2} seconds. Saved to examples\last-result.json.' -f $timer.Elapsed.TotalSeconds)
