"""Reuse stop-fix acceptance isolation for the purpose overlay; no live traffic."""
import sys
sys.dont_write_bytecode = True
import hashlib
import importlib
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
UPDATE = ROOT / 'updates/purpose-review-20261022'
RUN = ROOT / '_verification/purpose-review-20261022' / ('offline-' + str(os.getpid()))
(RUN / 'temp').mkdir(parents=True)
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
        if resolved.startswith('\\\\?\\UNC\\'): resolved = '\\\\' + resolved[8:]
        elif resolved.startswith('\\\\?\\'): resolved = resolved[4:]
        if not Path(resolved).is_relative_to((ROOT/'_verification').resolve()):
            raise RuntimeError('Acceptance rejected a write outside the isolated workspace')


sys.addaudithook(audit)
sys.path[:0] = [str(UPDATE/'desktop'),str(ROOT/'desktop'),str(ROOT/'core'),str(ROOT/'scripts')]
# pytest prepends desktop/ while collecting old suites. Preload changed modules
# before collection so the regression run actually exercises this overlay.
origins = {}
for name in ('material_roles','purpose_evidence','purpose_review','purpose_cleanup',
             'associated_materials','associated_delivery','auto_config','auto_pipeline','auto_ui',
             'preextract_workbench','preextract_request','preextract_throttle',
             'preextract_images','preextract_partition_run','preextract_catalog',
             'processing_stop','stop_http','agent_client','office_wps','intake_image_ocr'):
    module = importlib.import_module(name)
    assert Path(module.__file__).parent == UPDATE/'desktop', module.__file__
    origins[name] = str(module.__file__)

import pytest
class Isolation:
    @pytest.fixture(autouse=True)
    def private_indexes(self,tmp_path,monkeypatch):
        import preextract_pipeline
        monkeypatch.setattr(preextract_pipeline,'DATA_DIR',tmp_path/'isolated-app/data')
        yield
        import gc
        gc.collect()

files = sys.argv[1:] or [
    'updates/purpose-review-20261022/tests/test_purpose_review.py',
    'updates/purpose-review-20261022/tests/test_purpose_integration.py',
    'desktop/test_associated_materials.py','desktop/test_intake_progress_fixes.py',
    'desktop/test_triage_intake.py','desktop/test_material_seams.py',
    'desktop/test_material_pipeline.py','desktop/test_processing_stop.py',
    'desktop/test_model_cancellation.py','desktop/test_material_conversion.py',
    'desktop/test_concurrency.py']
args = ['-q','--tb=short','-p','no:cacheprovider','--basetemp',str(RUN/'pytest'),
        '--junitxml',str(RUN/'results.xml')]
args.extend(str(ROOT/name) if not Path(name).is_absolute() else name for name in files)
code = pytest.main(args,plugins=[Isolation()])
report = dict(exit_code=int(code),candidate=str(UPDATE),module_origins=origins,
              local_fixtures_only=True,production_writes_allowed=False,artifacts=str(RUN))
(RUN/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps(report,ensure_ascii=False))
raise SystemExit(code)
