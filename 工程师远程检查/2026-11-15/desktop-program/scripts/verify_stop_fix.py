"""Isolated stop acceptance: local fixtures only, no production writes."""
import sys
sys.dont_write_bytecode = True
import hashlib
import importlib
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BASELINE = '--baseline' in sys.argv
FILES = [x for x in sys.argv[1:] if x != '--baseline']
UPDATE = ROOT / ('updates/token-saving-20261022' if BASELINE else 'updates/stop-fix-20261022')
RUN = ROOT / '_verification/stop-fix-20261022' / str(os.getpid())
(RUN / 'temp').mkdir(parents=True)
os.environ.update(PAPER_UPSTREAM_DATA_DIR=str(RUN/'data'), TEMP=str(RUN/'temp'), TMP=str(RUN/'temp'),
                  PYTHONDONTWRITEBYTECODE='1', PYTHONUTF8='1')


def audit(event, args):
    if event in {'socket.connect', 'socket.connect_ex', 'socket.bind'}:
        address = args[1]
        if not isinstance(address, tuple) or address[0] not in {'127.0.0.1', '::1', 'localhost'}:
            raise RuntimeError('Acceptance permits only loopback fixture connections')
    paths = []
    if event == 'open':
        mode, flags = args[1:3]
        if ((isinstance(mode,str) and any(c in mode for c in 'wax+')) or
                (isinstance(flags,int) and flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND))):
            paths.append(args[0])
    elif event in {'os.mkdir', 'os.remove', 'os.rmdir', 'os.chmod', 'os.utime'}: paths.append(args[0])
    elif event in {'os.rename', 'os.replace'}: paths.extend(args[:2])
    for path in paths:
        if not isinstance(path, (str,bytes,os.PathLike)): continue
        if os.fsdecode(path).casefold() in {'nul', r'\\.\nul'}: continue
        resolved = str(Path(os.fsdecode(path)).resolve())
        if resolved.startswith('\\\\?\\UNC\\'): resolved = '\\\\' + resolved[8:]
        elif resolved.startswith('\\\\?\\'): resolved = resolved[4:]
        if not Path(resolved).is_relative_to((ROOT/'_verification').resolve()):
            raise RuntimeError('Acceptance rejected a write outside the isolated workspace')


sys.addaudithook(audit)
sys.path[:0] = [str(UPDATE/'desktop'), str(ROOT/'desktop'), str(ROOT/'core'), str(ROOT/'scripts')]
manifest = json.loads((UPDATE/'manifest.json').read_text(encoding='utf8'))
for row in manifest['files']:
    assert hashlib.sha256((UPDATE/row['file']).read_bytes()).hexdigest() == row['sha256']
for name in ('agent_client', 'associated_materials', 'auto_pipeline', 'app', 'preextract_request',
             'preextract_images', 'preextract_partition_run', 'preextract_workbench', 'office_wps', 'intake_image_ocr'):
    module = importlib.import_module(name)
    if (UPDATE/'desktop'/f'{name}.py').exists(): assert Path(module.__file__).parent == UPDATE/'desktop'

import pytest
class Isolation:
    @pytest.fixture(autouse=True)
    def private_indexes(self, tmp_path, monkeypatch):
        import preextract_pipeline
        monkeypatch.setattr(preextract_pipeline, 'DATA_DIR', tmp_path/'isolated-app/data')
        yield
        import gc
        gc.collect()

files = FILES or ['desktop/test_processing_stop.py', 'desktop/test_model_cancellation.py',
    'desktop/test_material_conversion.py', 'desktop/test_intake_progress_fixes.py',
    'desktop/test_associated_materials.py', 'desktop/test_preextract_request.py',
    'desktop/test_concurrency.py']
args = ['-q', '--tb=short', '-p', 'no:cacheprovider', '--basetemp', str(RUN/'pytest'),
        '--junitxml', str(RUN/'results.xml')]
args.extend(str(ROOT/name) if not Path(name).is_absolute() else name for name in files)
code = pytest.main(args, plugins=[Isolation()])
report = dict(exit_code=int(code), candidate=str(UPDATE), baseline=BASELINE,
              local_fixtures_only=True, production_writes_allowed=False, artifacts=str(RUN))
(RUN/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps(report, ensure_ascii=False))
raise SystemExit(code)
