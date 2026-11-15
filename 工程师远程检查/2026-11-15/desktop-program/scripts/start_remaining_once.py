"""One authorized launch using the native selector, GUI, mutex and source overlay.

No recovery, classification override, uploader restart or alternative processor.
The ordinary daily entry remains unified_launcher.py.
"""
import sys
sys.dont_write_bytecode=True
import hashlib,json,os,time
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parent.parent
TASK=ROOT.parent.parent
DATA=Path(r'E:\uv\ragflow_upstream\data')
OUT=ROOT/'_verification/activation-remaining-20261022'
LATEST='AUTO_20261022_121452_a6a23b'
sys.path[:0]=[str(ROOT/'scripts'),str(ROOT/'desktop'),str(ROOT/'core')]
os.environ['PAPERFLOW_LEGACY_APP']=str(ROOT/'desktop')

def read(path):return json.loads(Path(path).read_text(encoding='utf-8-sig'))
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def save(name,value):
    OUT.mkdir(parents=True,exist_ok=True)
    target=OUT/name;temporary=target.with_suffix(target.suffix+'.tmp')
    temporary.write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf8');os.replace(temporary,target)

class ReadOnlyStore:
    def load(self,run_id):return read(DATA/'runs'/run_id/'manifest.json')

def prepare():
    from archive_identity import record_key
    from app import retry_sources
    from script_task import task_snapshot
    latest=ReadOnlyStore().load(LATEST)
    if latest.get('status')!='completed' or latest.get('processed_sources')!=134:
        raise RuntimeError('Original 379 continuation is not complete')
    merged=task_snapshot(ReadOnlyStore(),latest)
    original=read(TASK/'work/continuation_inspection.json')['rows']
    excluded={record_key(r['record']) for r in original}
    if len(excluded)!=379:raise RuntimeError('Original body scope is not exact')
    native=retry_sources(merged,scope='script')
    selected=[r for r in native if record_key(r) not in excluded]
    if len(native)!=6237 or len(selected)!=6197 or any(not Path(r['path']).is_file() for r in selected):
        raise RuntimeError('Remaining source scope changed; no automatic launch')
    bindings={};current=latest
    while current:
        rid=current['run_id'];bindings[rid]=sha(DATA/'runs'/rid/'manifest.json')
        current=ReadOnlyStore().load(current['retry_of']) if current.get('retry_of') else None
    save('selection.json',selected)
    save('plan.json',dict(schema='authorized-native-remaining/1',previous_run=LATEST,scope=6197,
        excluded_original_bodies=379,native_retry_count=6237,selection_sha256=sha(OUT/'selection.json'),
        historical_manifests=bindings,operation='retry_failed',model_request_limit=20,
        production_recovery=False,uploader_intervention=False,experimental_profile_grouping=False))
    print(json.dumps(dict(prepared_sources=len(selected),excluded_bodies=len(excluded),historical_runs=len(bindings)),ensure_ascii=False))

def main():
    if sys.argv[1:]==['--prepare']:prepare();return
    if sys.argv[1:]:raise ValueError('Only --prepare or a normal one-shot launch is supported')
    from unified_launcher import running_owners
    if running_owners():raise RuntimeError('Existing frontend or model owner remains; no duplicate launch')
    plan=read(OUT/'plan.json');selected=read(OUT/'selection.json')
    if sha(OUT/'selection.json')!=plan['selection_sha256'] or len(selected)!=plan['scope']:
        raise RuntimeError('Frozen selection changed')
    if not all(sha(DATA/'runs'/rid/'manifest.json')==value for rid,value in plan['historical_manifests'].items()):
        raise RuntimeError('Historical batch changed since selection')
    from source_update import activate_source_update
    overlay=activate_source_update(ROOT)
    import preextract_request,preextract_images,preextract_partition_run,preextract_workbench
    modules=(preextract_request,preextract_images,preextract_partition_run,preextract_workbench)
    origins={m.__name__:m.__file__ for m in modules}
    if any(Path(m.__file__).parent!=overlay for m in modules):raise RuntimeError('Selected source was not loaded')
    import app
    state=dict(pid=os.getpid(),phase='opening',scope=plan['scope'],source_overlay=str(overlay),
               loaded_modules=origins,original_379_excluded=True,upload_worker_untouched=True)
    def record(**values):
        state.update(values,updated_at=datetime.now(ZoneInfo('America/Los_Angeles')).isoformat(timespec='seconds'))
        save('runtime.json',state)
    base=app.Workbench
    class SelectedWorkbench(base):
        def __init__(self,root,data_root,*,recover=True):
            # Keep old manifests and the live upload pool exactly where they are.
            super().__init__(root,data_root,recover=False)
            self.current=self.store.load(plan['previous_run'])
            self._pending_operation='retry_failed'
            root.title('论文知识库预处理 · 已启用Token节约 · 剩余6197个来源')
            def start():
                from auto_ui import read_ai_form
                config,_=read_ai_form(self)
                if not config['concurrency']['enabled'] or config['concurrency']['global_llm']!=20:
                    raise RuntimeError('Existing 20-request configuration changed')
                record(phase='starting_native_batch',model_request_limit=20)
                self.start_automatic(selected)
                record(phase='running' if self.busy else 'not_started',visible=bool(root.winfo_viewable()))
            root.after(500,start)
        def show_script_progress(self,value):
            super().show_script_progress(value)
            update={k:value[k] for k in ('run_id','status','processed_sources','source_count','intake_progress') if k in value}
            if value.get('pipeline_concurrency'):update['concurrency']=value['pipeline_concurrency']
            stage=(value.get('progress') or {}).get('stage')
            if stage:update['stage']=stage
            if time.monotonic()-getattr(self,'_activation_saved',0)>=2 or state.get('run_id')!=value.get('run_id'):
                record(phase='running',**update);self._activation_saved=time.monotonic()
        def finished(self,manifest):
            super().finished(manifest)
            record(phase='finished',run_id=manifest['run_id'],status=manifest['status'],processed_sources=manifest['processed_sources'])
        def error(self,message):
            record(phase='error',message=str(message)[:250])
            super().error(message)
    app.Workbench=SelectedWorkbench
    record()
    # Retain the original data-dir mutex and complete six-tab native application.
    app.main(['--data-dir',str(DATA)])

if __name__=='__main__':
    try:main()
    except Exception:
        import traceback
        save('startup-error.json',dict(error=traceback.format_exc()))
        raise
