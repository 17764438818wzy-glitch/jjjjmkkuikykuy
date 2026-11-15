"""独立测试库内的有限吞吐标定；不调用LLM，不更改服务器配置。"""
from concurrent.futures import ThreadPoolExecutor,as_completed
from pathlib import Path
import copy,hashlib,json,sys,time,uuid
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from paperflow.config import Config
from paperflow.rag import RagPublisher,native_client
from paperflow.package import load,save,validate_package
from paperflow.store import Store

project=Path(__file__).resolve().parents[1];home=project/'runtime/integration-live'
c=Config.load(home/'config.json');s=Store(home/'state.sqlite3')
plan=load(home/'restart-plan.json');base=s.get(plan['task_id'])
packet=validate_package(base['package_dir'],base,c.catalog)
report_path=home/'calibration-report.json'
if report_path.exists() and load(report_path).get('complete'):print('calibration_already_complete');raise SystemExit(0)
run=uuid.uuid4().hex[:12];results=[];previous_rate=0
for concurrency in (1,2,4,8):
    receipts=[];durations=[];failures=[];started=time.monotonic()
    def job(index):
        task=dict(base,task_id='PF_CAL_'+run+'_'+str(concurrency)+'_'+str(index),revision=1,
            source_sha256=hashlib.sha256((run+str(concurrency)+str(index)).encode()).hexdigest())
        current=dict(packet,manifest_sha256=hashlib.sha256((task['task_id']+'manifest').encode()).hexdigest())
        own={};receipts.append(own);p=RagPublisher(native_client(c));begin=time.monotonic()
        try:
            p.publish(task,current,own);p.verify(task,current,own)
            return time.monotonic()-begin
        except Exception as exc:
            return dict(type=type(exc).__name__)
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for future in as_completed([pool.submit(job,i) for i in range(8)]):
            result=future.result()
            if isinstance(result,dict):failures.append(result)
            else:durations.append(result)
    elapsed=time.monotonic()-started
    # 不发布检索可见性：本测试只测写入/精确读回链路，完整版检索另有实测。
    p=RagPublisher(native_client(c));cleaned=0
    for own in receipts:
        for target in own.values():
            for role in ('body','images'):
                obj=target.get(role)
                if not obj or not obj.get('document_id'):continue
                docs=p.documents(obj['dataset_id'],id=obj['document_id'])
                meta=docs[0].get('meta_fields',{})
                if not str(meta.get('paperflow_task','')).startswith('PF_CAL_'+run+'_'):raise RuntimeError('calibration cleanup ownership conflict')
                p.client.request('DELETE',f"/datasets/{obj['dataset_id']}/documents",{'ids':[obj['document_id']]})
                if any(d['id']==obj['document_id'] for d in p.documents(obj['dataset_id'])):raise RuntimeError('calibration cleanup unconfirmed')
                cleaned+=1
    row=dict(concurrency=concurrency,tasks=8,verified=len(durations),failures=failures,seconds=round(elapsed,3),
        verified_per_minute=round(60*len(durations)/elapsed,3),mean_task_seconds=round(sum(durations)/len(durations),3) if durations else None,
        max_task_seconds=round(max(durations),3) if durations else None,deleted_test_documents=cleaned)
    results.append(row);save(report_path,dict(complete=False,run=run,measurements=results))
    print(json.dumps(row,ensure_ascii=False),flush=True)
    rate=row['verified_per_minute']
    if failures or (concurrency>=4 and rate<previous_rate*1.08):break
    previous_rate=rate
stable=[r for r in results if not r['failures'] and r['verified']==8]
best=max(stable,key=lambda r:r['verified_per_minute']) if stable else None
save(report_path,dict(complete=True,run=run,measurements=results,recommended_rag_workers=best['concurrency'] if best else None,
    scope='8 copies per level of a small real LLM-prepared package; write plus exact readback; no model throughput or unlimited-load claim'))
print('calibration_complete',flush=True)
