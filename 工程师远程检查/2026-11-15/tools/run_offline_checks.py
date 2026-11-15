from pathlib import Path
import sys,os,socket,json,types,tempfile
sys.dont_write_bytecode=True
stage=Path(__file__).resolve().parents[1]
_temporary=tempfile.TemporaryDirectory(prefix='paperflow-handoff-check-')
work=Path(_temporary.name)
v3=stage/'paperflow/candidates/parent-child-v3-20261102'
os.environ['PYTEST_DISABLE_PLUGIN_AUTOLOAD']='1'
os.environ['PREFECT_API_URL']='http://127.0.0.1:9/api'
os.environ['PAPERFLOW_CONFIG']=str(stage/'paperflow/prefect-paperflow/config.example.json')
sys.path[:0]=list(map(str,[
 v3,stage/'paperflow/prefect-paperflow/paperflow_prefect/clean_overlay',
 stage/'desktop-program/updates/unified-prefect-20261029-material-r02/desktop',
 stage/'desktop-program/desktop',
 stage/'desktop-program/updates/unified-prefect-20261029-material-r02/core',
 stage/'paperflow/prefect-paperflow']))
attempts=[]
def denied_connect(sock,address):
 attempts.append(repr(address))
 raise RuntimeError('Network access denied in source export checks')
socket.socket.connect=denied_connect
socket.socket.connect_ex=denied_connect
# Only these two unused path constants are needed to import the one-shot client.
# Do not load machine-bound config or any production directories for these tests.
project_paths=types.ModuleType('project_paths')
project_paths.LEGACY_ROOT=work/'offline-not-configured'
project_paths.OFFICE_CONVERTER=work/'offline-not-configured/converter.py'
sys.modules['project_paths']=project_paths
import pytest
os.chdir(v3)
args=['--noconftest','-q','-p','no:cacheprovider',
 'tests/stages/test_unknown_transport_preservation.py',
 'tests/delivery/test_transport_journal.py',
 'tests/delivery/test_owned_work.py',
 '--basetemp',str(work/'offline-check-tmp')]
status=int(pytest.main(args))
(work/'offline-check-result.json').write_text(json.dumps({'exit_code':status,'tests':[a for a in args if a.startswith('tests/')],'network_attempts':len(attempts),'scope':'exported source copies; no conftest or external network'},ensure_ascii=False,indent=2),'utf-8')
sys.exit(status)
