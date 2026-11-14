"""Exercise the real create/retention code using an isolated MySQL probe table."""
import argparse
import importlib.util
import json
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from types import SimpleNamespace

from api.db.db_models import DB, PipelineOperationLog
from common.constants import PipelineTaskType, TaskStatus

parser=argparse.ArgumentParser();parser.add_argument('--source',required=True)
parser.add_argument('--workers',type=int,default=4);parser.add_argument('--result',required=True)
parser.add_argument('--real-dsl',action='store_true')
parser.add_argument('--lose-session',action='store_true')
args=parser.parse_args()
module_spec=importlib.util.spec_from_file_location('education_log_probe',args.source)
module=importlib.util.module_from_spec(module_spec);module_spec.loader.exec_module(module)
table='codex_edu_log_probe_'+uuid.uuid4().hex[:16]
class ProbeLog(PipelineOperationLog):
    class Meta:
        database=DB
        table_name=table
kb='ecf88dc4b82b11f1a8846d29005ec004'
payload='{}'
if args.real_dsl:
    with DB.connection_context():
        value=PipelineOperationLog.select(PipelineOperationLog.dsl).where(
            (PipelineOperationLog.kb_id==kb)&(PipelineOperationLog.operation_status=='3')
        ).order_by(PipelineOperationLog.create_time.desc()).first().dsl
        payload=json.dumps(value) if isinstance(value,dict) else value
timings=[]
execute_sql=DB.execute_sql
def timed_sql(sql,*a,**kw):
    begin=time.monotonic()
    try:return execute_sql(sql,*a,**kw)
    finally:
        timings.append({'statement':sql.split()[0], 'bytes':len(sql),'seconds':round(time.monotonic()-begin,3)})
DB.execute_sql=timed_sql
document=SimpleNamespace(id='a'*32,kb_id=kb,parser_id='naive',thumbnail='',name='probe.md',
    run=TaskStatus.DONE.value,progress=1,progress_msg='Probe indexing complete',
    process_begin_at=datetime.now(),process_duration=1,suffix='md',type='doc',source_type='local')
document.to_dict=lambda:vars(document)
module.DocumentService=SimpleNamespace(get_by_id=lambda *_:(True,document),update_progress_immediately=lambda *_:None)
module.KnowledgebaseService=SimpleNamespace(get_by_id=lambda *_:(True,SimpleNamespace(tenant_id='b'*32)))
barrier=threading.Barrier(args.workers);local=threading.local();saved_ids={};attempts={};guard=threading.Lock()
original_begin=DB.begin
def begin_with_loss():
    if args.lose_session and hasattr(local,'worker') and not getattr(local,'lost_session',False):
        local.lost_session=True
        DB.manual_close()
        DB.connect()
    return original_begin()
DB.begin=begin_with_loss
class ProbeService(module.PipelineOperationLogService):
    model=ProbeLog
    @classmethod
    def save(cls,**kwargs):
        number=getattr(local,'number',0)+1;local.number=number
        with guard:
            attempts[local.worker]=number
            saved_ids.setdefault(local.worker,set()).add(kwargs['id'])
        result=super().save(**kwargs)
        if number==1:time.sleep(.05)
        return result

def writer(index):
    local.worker=index
    try:
        barrier.wait(timeout=15)
        value=ProbeService.create(document_id='a'*32,pipeline_id=None,task_type=PipelineTaskType.PARSE,dsl=payload)
        return {'worker':index,'success':value==1}
    except Exception as exc:
        return {'worker':index,'success':False,'error':type(exc).__name__,
            'error_code':exc.args[0] if exc.args and isinstance(exc.args[0],int) else None}

result={}
try:
    with DB.connection_context():
        if DB.table_exists(table):raise RuntimeError('Unexpected probe table collision')
        DB.create_tables([ProbeLog])
        seed={'document_id':'a'*32,'tenant_id':'b'*32,'kb_id':kb,'parser_id':'naive',
            'document_name':'probe.md','document_suffix':'md','document_type':'doc','source_from':'local',
            'progress':1,'process_duration':1,'task_type':PipelineTaskType.PARSE,'operation_status':TaskStatus.DONE.value,'dsl':{}}
        ProbeLog.insert_many([dict(seed,id=f'{n:032x}',create_time=n) for n in range(1000)]).execute()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:rows=list(pool.map(writer,range(args.workers)))
    with DB.connection_context():
        persisted={i:ProbeLog.select().where(ProbeLog.id.in_(list(ids))).count() for i,ids in saved_ids.items()}
        total=ProbeLog.select().count()
    result={'source':args.source,'workers':args.workers,'results':rows,'attempts':attempts,
        'ids_per_worker':{i:len(ids) for i,ids in saved_ids.items()},'persisted':persisted,'retained':total,
        'payload_bytes':len(payload.encode()),'slow_sql':[r for r in timings if r['seconds']>.05]}
    result['passed']=(all(r['success'] for r in rows) and all(n==1 for n in persisted.values())
        and all(len(ids)==1 for ids in saved_ids.values()) and total==1000)
    if args.lose_session:
        result['passed']=(not any(r['success'] for r in rows) and not attempts and total==1000)
finally:
    with DB.connection_context():
        if not table.startswith('codex_edu_log_probe_') or ProbeLog._meta.table_name!=table:
            raise RuntimeError('Probe cleanup identity changed')
        DB.drop_tables([ProbeLog],safe=True)
    with open(args.result,'w') as out:json.dump(result,out,indent=2)
print(json.dumps(result),flush=True)
raise SystemExit(0 if result.get('passed') else 1)
