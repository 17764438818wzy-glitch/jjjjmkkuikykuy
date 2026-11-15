$ErrorActionPreference = 'Stop'
Push-Location $PSScriptRoot
try {
@'
from pathlib import Path
import json
import psutil
record = json.loads(Path('runtime/runner-process.json').read_text(encoding='utf8'))
try:
    process = psutil.Process(record['pid'])
except psutil.NoSuchProcess:
    print('Runner already stopped')
    raise SystemExit(0)
if abs(process.create_time() - record['create_time']) > 0.01 or not any('manage.py' in arg for arg in process.cmdline()):
    raise RuntimeError('Process identity changed; will not stop another process')
if process.children(recursive=True):
    raise RuntimeError('Runner has child processes; finish or cancel the current Prefect run first')
process.terminate()
process.wait(timeout=15)
print('This project runner stopped')
'@ | & '.\.venv\Scripts\python.exe' -X utf8 -
    if ($LASTEXITCODE -ne 0) { throw 'Runner was not stopped; inspect the reason above' }
} finally {
    Pop-Location
}
