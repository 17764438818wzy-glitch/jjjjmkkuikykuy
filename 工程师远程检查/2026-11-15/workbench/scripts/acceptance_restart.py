"""只在独立测试库执行真实写入后退出、重启去重和原稿新版切换验收。"""
from pathlib import Path
import os,shutil,subprocess,sys,time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from paperflow.config import Config
from paperflow.graph import Workflow
from paperflow.store import Store
from paperflow.package import load,save,sha,seal_package,validate_package
from paperflow.producer import PUBLIC_NAMES
from paperflow.rag import RagPublisher,native_client
from paperflow.service import InstanceLock

project=Path(__file__).resolve().parents[1];home=project/'runtime/integration-live'
c=Config.load(home/'config.json');s=Store(home/'state.sqlite3')

if '--kill-worker' in sys.argv:
    task=s.get(sys.argv[-1]);client=native_client(c)
    class KillAfterWrite:
        def __getattr__(self,name):return getattr(client,name)
        def request(self,method,endpoint,body=None,content_type=None):
            options={'content_type':content_type} if content_type is not None else {}
            result=client.request(method,endpoint,body,**options)
            if method=='POST' and endpoint.endswith('/chunks'):
                chunk=result.get('chunk',result)
                save(home/'process-kill-proof.json',dict(task_id=task['task_id'],
                    point='server_write_succeeded_before_local_ack',chunk_id=chunk.get('id'),endpoint=endpoint))
                os._exit(86)
            return result
    with InstanceLock(home/'controller.lock'):
        f=Workflow(c,s,publisher=RagPublisher(KillAfterWrite()))
        try:f.step(task)
        finally:f.close()
    raise SystemExit('kill point was not reached')

report_path=home/'restart-version-report.json'
if report_path.exists():
    report=load(report_path)
    if report.get('passed'):print('restart_version_acceptance_already_passed');raise SystemExit(0)
old=load(home/'status-last.json')
if old['stage']!='complete':raise SystemExit('wait for real model repair completion')
plan_path=home/'restart-plan.json'
if '--new-fixture' in sys.argv and plan_path.exists():
    previous=load(plan_path)
    shutil.copy2(plan_path,home/('restart-plan-prior-'+str(time.time_ns())+'.json'))
    s.update(previous['task_id'],stage='code_error',error_kind='controller',error='isolated_test_wrapper_corrected')
    plan_path.unlink() # 仅删除本脚本的已备份测试计划，不删除图检查点或原稿。
if plan_path.exists():plan=load(plan_path);task=s.get(plan['task_id'])
else:
    source=Path(old['source_path']);old_hash=old['source_sha256']
    source.write_text(source.read_text(encoding='utf-8')+'\n## 合成验收第二版\n这是同一教学材料的版本更新标记，用于测试检索可见性。\n',encoding='utf-8')
    task=s.register_source(source,sha(source))
    root=home/'fixtures'/task['task_id'];root.mkdir(parents=True,exist_ok=True)
    for name in PUBLIC_NAMES:
        shutil.copy2(Path(old['package_dir'])/name,root/name)
        if name.endswith('.json'):
            obj=load(root/name)
            for k in ('task_id','source_id','source_sha256'):obj[k]=task[k]
            save(root/name,obj)
    note='\n## 合成验收第二版\n这是同一教学材料的版本更新标记，用于测试检索可见性。\n'
    body=(root/'article.redacted.md').read_text(encoding='utf-8')+note
    (root/'article.redacted.md').write_text(body,encoding='utf-8')
    chunks=load(root/'chunks.json');[x for x in chunks['chunks'] if x['kind']=='body'][-1]['content']+=note;save(root/'chunks.json',chunks)
    profile=load(root/'profile.json');profile['article']['title']+='（合成验收第二版）';save(root/'profile.json',profile)
    seal_package(root,task,task['revision']);validate_package(root,task,c.catalog)
    task=s.update(task['task_id'],stage='validate',package_dir=str(root),manifest_sha256=sha(root/'manifest.json'))
    plan=dict(task_id=task['task_id'],old_task=old['task_id'],old_revision=old['revision'],old_source_sha256=old_hash,
              purpose='prepared synthetic fixture for technical restart test; no claim of LLM reprocessing')
    save(plan_path,plan)

if not (home/'process-kill-proof.json').exists():
    with InstanceLock(home/'controller.lock'):
        f=Workflow(c,s)
        try:
            if task['stage'] in ('join_targets','code_error'):
                s.request(task['task_id'],stage='ingest',error=None,error_kind=None)
                f.apply_pending_requests();task=s.get(task['task_id'])
            while task['stage']!='ingest_target':
                task=f.step(s.get(task['task_id']))
                if task['stage'] in ('parked','code_error','held'):raise RuntimeError('fixture cannot reach kill point')
        finally:f.close()
    killed=subprocess.run([sys.executable,'-X','utf8','-B',__file__,'--kill-worker',task['task_id']],
        cwd=project,capture_output=True,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
    if killed.returncode!=86:raise SystemExit('unexpected child exit '+str(killed.returncode))
    print('actual_process_exit_86_after_server_write',flush=True)

with InstanceLock(home/'controller.lock'):
    s.recover_leases();f=Workflow(c,s)
    try:
        for _ in range(24):
            task=f.step(s.get(task['task_id']))
            print('restart_phase',task['stage'],task.get('error_kind'),flush=True)
            if task['stage'] in ('complete','parked','code_error','held') or task.get('error_kind')=='infrastructure':break
    finally:f.close()
if task['stage']!='complete':raise SystemExit('restart did not complete')
p=RagPublisher(native_client(c));receipts=s.target_receipts(task['task_id'],1)
packet=validate_package(task['package_dir'],task,c.catalog)
counts={}
for code,target in receipts.items():
    for role,records in p.records(packet).items():
        if not records:continue
        doc=target[role];rows=p.chunks(doc['dataset_id'],doc['document_id'])
        assert len(rows)==len(records) and len({x['content'] for x in rows})==len(rows)
        counts[code+'/'+role]=len(rows)
old_receipts=s.target_receipts(plan['old_task'],plan['old_revision']);old_hidden=True
for target in old_receipts.values():
    for role in ('body','images'):
        obj=target.get(role)
        if not obj:continue
        doc=p.documents(obj['dataset_id'],id=obj['document_id'])[0]
        assert str(doc['status'])=='0'
        data=p.client.request('POST','/retrieval',dict(question='指标分解研究流程',dataset_ids=[obj['dataset_id']],
            document_ids=[obj['document_id']],similarity_threshold=0,vector_similarity_weight=1,top_k=64,
            rerank_candidates_count=64,page_size=64))
        old_hidden=old_hidden and not data.get('chunks')
assert old_hidden
save(report_path,dict(passed=True,process_killed_after_real_write=True,restarted_without_duplicates=True,
    new_active=s.active_for(task['source_path'])==task['task_id'],old_disabled_and_retrieval_hidden=old_hidden,
    task_id=task['task_id'],previous_task=plan['old_task'],chunk_counts=counts,at=time.time()))
print('restart_version_acceptance_passed',flush=True)
