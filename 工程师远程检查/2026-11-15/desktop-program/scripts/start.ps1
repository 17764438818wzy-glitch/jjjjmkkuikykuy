$ErrorActionPreference = 'Stop'


$projectRoot = Split-Path -Parent $PSScriptRoot


$paths = Get-Content -LiteralPath (Join-Path $projectRoot 'config\paths.json') -Raw -Encoding UTF8 | ConvertFrom-Json


$pythonwPath = Join-Path (Split-Path -Parent $paths.python) 'pythonw.exe'


if (-not (Test-Path -LiteralPath $pythonwPath -PathType Leaf)) {


    throw '未找到配置的 Python，请检查 config\paths.json 中的 python 路径。'


}


$entryPath = Join-Path $PSScriptRoot 'archive_launcher.py'


$entryArgument = '"' + $entryPath + '"'


$env:PYTHONUTF8 = '1'


$env:PYTHONDONTWRITEBYTECODE = '1'


Start-Process -FilePath $pythonwPath -ArgumentList @('-X', 'utf8', '-B', $entryArgument) -WorkingDirectory $projectRoot -WindowStyle Hidden


