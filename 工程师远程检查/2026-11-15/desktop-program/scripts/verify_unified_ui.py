"""Use the existing offline acceptance isolation with a chosen immutable release."""
import sys
sys.dont_write_bytecode = True
import hashlib
import importlib
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WORK = Path(__file__).resolve().parent
baseline = '--baseline' in sys.argv
files = [value for value in sys.argv[1:] if value != '--baseline']
UPDATE = ROOT/'updates'/('shutdown-fix-20261022' if baseline else 'ui-unified-20261022')
RUN = ROOT/'_verification/ui-unified-20261022'/str(os.getpid())
(RUN/'temp').mkdir(parents=True)
os.environ.update(PAPER_UPSTREAM_DATA_DIR=str(RUN/'data'), TEMP=str(RUN/'temp'),
    TMP=str(RUN/'temp'), PYTHONDONTWRITEBYTECODE='1', PYTHONUTF8='1')


def audit(event, args):
    if event in {'socket.connect', 'socket.connect_ex', 'socket.bind'}:
        address = args[1]
        if not isinstance(address, tuple) or address[0] not in {'127.0.0.1','::1','localhost'}:
            raise RuntimeError('Offline acceptance permits only loopback fixture connections')
    paths = []
    if event == 'open':
        mode, flags = args[1:3]
        if ((isinstance(mode,str) and any(c in mode for c in 'wax+')) or
                (isinstance(flags,int) and flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND))):
            paths.append(args[0])
    elif event in {'os.mkdir','os.remove','os.rmdir','os.chmod','os.utime'}: paths.append(args[0])
    elif event in {'os.rename','os.replace'}: paths.extend(args[:2])
    for path in paths:
        if not isinstance(path,(str,bytes,os.PathLike)): continue
        if os.fsdecode(path).casefold() in {'nul',r'\\.\nul'}: continue
        resolved = str(Path(os.fsdecode(path)).resolve())
        if resolved.startswith('\\\\?\\UNC\\'): resolved = '\\\\'+resolved[8:]
        elif resolved.startswith('\\\\?\\'): resolved = resolved[4:]
        if not Path(resolved).is_relative_to((ROOT/'_verification').resolve()):
            raise RuntimeError('Acceptance rejected a write outside the isolated workspace: '+resolved)


sys.addaudithook(audit)
sys.path[:0] = [str(UPDATE/'desktop'),str(ROOT/'desktop'),str(ROOT/'core'),str(ROOT/'scripts')]
manifest = json.loads((UPDATE/'manifest.json').read_text(encoding='utf8'))
origins = {}
for row in manifest['files']:
    source = UPDATE/row['file']
    assert hashlib.sha256(source.read_bytes()).hexdigest() == row['sha256'], source
    module = importlib.import_module(source.stem)
    assert Path(module.__file__).resolve() == source.resolve(), module.__file__
    origins[source.stem] = str(source)

import pytest
class Isolation:
    @pytest.fixture(autouse=True)
    def private_indexes(self,tmp_path,monkeypatch):
        import preextract_pipeline
        monkeypatch.setattr(preextract_pipeline,'DATA_DIR',tmp_path/'isolated-app/data')
        yield
        import gc
        gc.collect()

files = files or [str(UPDATE/'tests/test_unified_task_ui.py')]
args = ['-q','--tb=short','-p','no:cacheprovider','--basetemp',str(RUN/'pytest'),
        '--junitxml',str(RUN/'results.xml')]
args.extend(str(ROOT/name) if not Path(name).is_absolute() else name for name in files)
code = pytest.main(args,plugins=[Isolation()])
report = dict(exit_code=int(code),baseline=baseline,candidate=str(UPDATE),module_origins=origins,
    local_fixtures_only=True,production_writes_allowed=False,artifacts=str(RUN))
(RUN/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps({k:v for k,v in report.items() if k!='module_origins'},ensure_ascii=False))
raise SystemExit(code)
