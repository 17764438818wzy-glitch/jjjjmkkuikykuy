"""Offline update acceptance; neither network nor live data writes are allowed."""
import sys
sys.dont_write_bytecode = True
import os, json
from pathlib import Path

HERE=Path(__file__).resolve().parent
PROJECT=HERE.parent
UPDATE=PROJECT/'updates/token-saving-20261022'
SCRATCH=PROJECT/'_verification/token-suite'
RUN=SCRATCH/str(os.getpid())
RUN.mkdir(parents=True,exist_ok=False)
(RUN/'temp').mkdir()
os.environ.update(PAPER_UPSTREAM_DATA_DIR=str(RUN/'data'), TEMP=str(RUN/'temp'), TMP=str(RUN/'temp'),
                  PYTHONDONTWRITEBYTECODE='1', PYTHONUTF8='1')
ALLOWED=((PROJECT/'_verification').resolve(),)

def audit(event,args):
    if event in {'socket.connect','socket.connect_ex','socket.bind'} or event.startswith(('subprocess.','os.system','os.posix_spawn')):
        raise RuntimeError('Acceptance forbids network connections and subprocesses')
    paths=[]
    if event=='open':
        mode,flags=args[1:3]
        if ((isinstance(mode,str) and any(c in mode for c in 'wax+')) or
            (isinstance(flags,int) and flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND))):paths.append(args[0])
    elif event in {'os.mkdir','os.remove','os.rmdir','os.chmod','os.utime'}:paths.append(args[0])
    elif event in {'os.rename','os.replace'}:paths.extend(args[:2])
    for p in paths:
        if not isinstance(p,(str,bytes,os.PathLike)):continue
        if os.fsdecode(p).casefold() in {'nul',r'\\.\nul'}:continue
        resolved=str(Path(os.fsdecode(p)).resolve())
        if resolved.startswith('\\\\?\\UNC\\'):resolved='\\\\'+resolved[8:]
        elif resolved.startswith('\\\\?\\'):resolved=resolved[4:]
        target=Path(resolved)
        if not any(target.is_relative_to(root) for root in ALLOWED):
            with (RUN/'denied-writes.jsonl').open('a',encoding='utf-8') as log:
                log.write(json.dumps(dict(event=event,target=str(target)))+'\n')
            raise RuntimeError('Acceptance rejected a write outside its isolated workspace')

sys.addaudithook(audit)
sys.path[:0]=[str(UPDATE/'desktop'),str(PROJECT/'desktop'),str(PROJECT/'core'),str(PROJECT/'scripts')]
# Pytest's collection of legacy tests must not prepend the live source before
# the overlay. Preload precisely the modules being accepted, never app/main.
import preextract_request,preextract_images,preextract_partition_run,preextract_workbench
for module in (preextract_request,preextract_images,preextract_partition_run,preextract_workbench):
    assert Path(module.__file__).resolve().is_relative_to((UPDATE/'desktop').resolve())
import pytest
class GuiIsolation:
    def pytest_collection_modifyitems(self,config,items):
        old=PROJECT/'desktop/test_preextract_reuse_repairs.py'
        ported=[i for i in items if i.name=='test_saved_answer_validation_has_its_own_label_and_retry_eligibility'
                and Path(i.path).resolve()==old.resolve()]
        if ported:
            config.hook.pytest_deselected(items=ported)
            items[:]=[i for i in items if i not in ported]
    @pytest.fixture(autouse=True)
    def collect_gui_cycles(self,tmp_path,monkeypatch):
        # Earlier tests patched ROOT before the consolidated DATA_DIR constant
        # existed. Bind that index per test too, avoiding cross-fixture reuse.
        import preextract_pipeline
        monkeypatch.setattr(preextract_pipeline,'DATA_DIR',tmp_path/'isolated-app/data')
        yield
        import gc
        gc.collect()
files=sys.argv[1:] or [str(UPDATE/'tests'), 'test_preextract_request.py','test_preextract_images.py',
    'test_preextract_pipeline.py','test_preextract_partition.py','test_preextract_start_pacing.py',
    'test_preextract_reuse_repairs.py','test_preextract_workbench_flow.py']
args=['-q','--tb=short','-p','no:cacheprovider','--basetemp',str(RUN/'pytest'),
      '--log-file',str(RUN/'pytest.log'),'--junitxml',str(RUN/'results.xml')]
# The old two-sentence text fixture is now correctly intercepted by the already
# deployed intake rule. The same status scenario is ported with explicit body
# intake evidence in test_token_resume_compatibility; production rules stay intact.
args.extend(str(PROJECT/'desktop'/name) if not Path(name).is_absolute() else name for name in files)
code=pytest.main(args,plugins=[GuiIsolation()])
print(json.dumps(dict(test_exit_code=int(code),artifacts=str(RUN),network_allowed=False,production_writes_allowed=False),ensure_ascii=False))
raise SystemExit(code)
