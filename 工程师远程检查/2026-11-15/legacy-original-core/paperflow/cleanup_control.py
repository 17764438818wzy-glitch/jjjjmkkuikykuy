"""Cleanup uses the existing manager gate, IPC commit path and specialty owner."""
import hashlib
import json
from pathlib import Path
import time
import uuid
from urllib.parse import quote,urlsplit,unquote

from .cleanup_store import CleanupLedger
from .delivery_control import DeliveryCommits,ControlledRagClient
from .handoff_store import canonical
from .package import load,save,sha
from .scheduling_policy import ActionDeferred


class CleanupActions:
    def __init__(self,client,config,item_id):
        self.client,self.config,self.item_id=client,config,item_id
        self.execution_id=client.execution_id

    def run(self,kind,resource,call,*,total_timeout=150):
        result=[]
        def invoke(cancel):
            if cancel.is_set():raise ValueError('cleanup stopped')
            result.append(call(cancel));return dict(completed=True)
        self.client.perform(kind,dict(item_id=self.item_id,**resource),invoke,
            total_timeout=total_timeout,expected_generation=0)
        return result[0]

    def commit(self,operation,payload):
        def invoke(cancel):
            path=self.config.root/'delivery-commits'/self.execution_id/(uuid.uuid4().hex+'.json')
            path.parent.mkdir(parents=True,exist_ok=True);save(path,dict(item_id=self.item_id,**payload))
            return self.client.rpc('cleanup_commit',dict(operation=operation,descriptor=dict(path=str(path),sha256=sha(path))))
        return self.run('local',dict(operation=operation),invoke,total_timeout=120)


class CleanupClient(ControlledRagClient):
    def __init__(self,native,actions):
        self.native,self.actions,self.base_url=native,actions,native.base_url

    def call(self,kind,method,endpoint,body,fn):
        if method not in ('GET','DELETE'):raise ValueError('cleanup only reads or deletes')
        resource=dict(operation='http',method=method,endpoint=endpoint,ids=body.get('ids') if isinstance(body,dict) else None,
            write_operation='document_delete' if method=='DELETE' else 'read')
        return self.actions.run('cleanup_write' if method=='DELETE' else 'rag_read',resource,lambda cancel:self.invoke_native(fn))


class CleanupCommits(DeliveryCommits):
    worker_kind='cleanup'

    def __init__(self,manager,config,*,isolation_root):
        self.manager,self.config=manager,config
        self.isolation_root=Path(isolation_root).resolve()
        for path in (manager.root,manager.store.path,config.root,config.handoff_root):
            if not Path(path).resolve().is_relative_to(self.isolation_root):raise ValueError('cleanup outside isolation root')
        self.ledger=CleanupLedger(manager.store,config)
        manager.cleanup=self

    @staticmethod
    def key(item):return canonical([item['source_id'],item['specialty_code']]) if item['kind']=='remote' else canonical(['local_cleanup',item['task_id']])

    def prepare(self,item_id):
        item=self.ledger.item(item_id);key=self.key(item)
        scope='unit:'+canonical(['target',key])
        with self.manager.gate_lock:
            if any(ex.scope==scope and (not ex.exit_saved or ex.execution_id in self.manager.publications) for ex in self.manager.executions.values()):
                raise ActionDeferred('specialty already owned')
        return key

    def check_action(self,c,execution_id,kind,resource):
        ex=self.manager.executions[execution_id]
        if ex.kind!='cleanup' or not isinstance(resource,dict):raise ValueError('cleanup owner required')
        row=c.execute('SELECT * FROM cleanup_items WHERE item_id=?',(resource.get('item_id'),)).fetchone()
        if not row or ex.key!=self.key(row):raise ValueError('cleanup item ownership mismatch')
        operation=resource.get('operation')
        if kind=='local':
            if operation not in ('load','begin','result','delete_local'):raise ValueError('unknown cleanup action')
            if operation=='delete_local':
                if row['kind']!='local' or row['state']!='deleting':raise ValueError('local delete intent required')
                self.ledger.local_guard(c,json.loads(row['resource']),row['manifest_sha256'])
            return False
        if row['kind']!='remote' or operation!='http':raise ValueError('remote cleanup item required')
        obj=json.loads(row['resource'])['object'];ds,did=obj['dataset_id'],obj['document_id']
        path=unquote(urlsplit(resource['endpoint']).path)
        base=f'/datasets/{ds}/documents'
        reads=[base,base+'/'+did+'/chunks','/pf-stage4/identity']
        if self.config.production_pilot:reads.append('/paperflow/deployment')
        if kind=='rag_read' and resource['method']=='GET' and path in reads:return True
        if kind!='cleanup_write' or resource['method']!='DELETE' or path!=base or resource.get('ids')!=[did]:
            raise ValueError('cleanup DELETE outside exact registered object')
        if row['state']!='deleting':raise ValueError('remote delete intent required')
        self.ledger.remote_guard(c,row)
        return False

    def handle(self,execution_id,operation,descriptor):
        m=self.manager
        with m.gate_lock:
            ex,action,ticket=self.guard(execution_id,operation)
            if execution_id in m.publications:raise ValueError('cleanup commit pending')
            m.publications.add(execution_id)
        try:
            if set(descriptor)!={'path','sha256'}:raise ValueError('invalid cleanup descriptor')
            path=Path(descriptor['path']).resolve()
            if not path.is_relative_to((self.config.root/'delivery-commits'/execution_id).resolve()) or path.stat().st_size>8*1024*1024:
                raise ValueError('cleanup evidence outside execution')
            if sha(path)!=descriptor['sha256']:raise ValueError('cleanup evidence changed')
            payload=load(path)
            if payload['item_id']!=ticket['request']['resource']['item_id']:raise ValueError('cleanup item changed')
            def write(c):
                self.check_action(c,execution_id,'local',ticket['request']['resource'])
                m.ledger.check(c,ticket['snapshot'],execution_id,'local',0)
                if operation=='begin':return self.ledger.begin_sql(c,payload['item_id'])
                if operation=='result':
                    item=c.execute('SELECT kind FROM cleanup_items WHERE item_id=?',(payload['item_id'],)).fetchone()
                    if payload['outcome']=='deleted' and item['kind']=='remote':
                        m.ledger.check(c,ticket['snapshot'],execution_id,'cleanup_write',0)
                    return self.ledger.result_sql(c,payload['item_id'],payload['outcome'],payload['evidence'])
                raise ValueError('unknown cleanup commit')
            return m.ledger._write(write,still_valid=lambda:m._valid(ticket['snapshot'],execution_id),
                before_commit=lambda:self.qualify(execution_id,action,ticket))
        finally:
            with m.gate_lock:m.publications.discard(execution_id)
