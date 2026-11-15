"""Small delivery extension to the existing manager/IPC admission protocol."""
from pathlib import Path
from contextlib import contextmanager
import hashlib
import json
import re
import time
import uuid
from urllib.parse import quote,urlsplit,unquote
from urllib.request import Request
from urllib.error import HTTPError

from .delivery_store import DeliveryLedger,target_key
from .package import load,save,sha
from .handoff_store import canonical
from .scheduling_policy import ServiceConfigurationError


class ResponseHeaders:
    def __init__(self,opener):self.opener=opener;self.retry_after=None
    def open(self,*args,**kwargs):
        try:return self.opener.open(*args,**kwargs)
        except HTTPError as exc:
            self.retry_after=exc.headers.get('Retry-After')
            raise


class DeliveryActions:
    def __init__(self,client,config,identity):
        self.client,self.config,self.identity=client,config,identity
        self.execution_id=client.execution_id

    def run(self,kind,resource,call,*,total_timeout=150):
        result=[]
        def invoke(cancel):
            if cancel.is_set():raise ValueError('delivery stopped')
            value=call(cancel);result.append(value)
            digest=hashlib.sha256(value if isinstance(value,bytes) else canonical(value).encode()).hexdigest()
            return dict(response_received=True,response_sha256=digest)
        self.client.perform(kind,dict(identity=self.identity,**resource),invoke,total_timeout=total_timeout,
                            expected_generation=self.identity['generation'])
        return result[0]

    def commit(self,operation,payload):
        def invoke(cancel):
            path=self.config.root/'delivery-commits'/self.execution_id/(uuid.uuid4().hex+'.json')
            path.parent.mkdir(parents=True,exist_ok=True);save(path,dict(identity=self.identity,**payload))
            return self.client.rpc('delivery_commit',dict(operation=operation,descriptor=dict(path=str(path),sha256=sha(path))))
        return self.run('local',dict(operation=operation),invoke,total_timeout=120)


class ControlledRagClient:
    """Every HTTP request, including downloads and read-only POST, has one permit."""
    def __init__(self,client,actions):
        self.native,self.actions=client,actions
        self.base_url=client.base_url
        self.object_manifest=actions.identity['manifest_sha256']
        self.recovery=False

    @contextmanager
    def version(self,digest,*,recovery=False):
        old=self.object_manifest,self.recovery
        self.object_manifest,self.recovery=digest,recovery
        try:yield
        finally:self.object_manifest,self.recovery=old

    def call(self,kind,method,endpoint,body,fn):
        effect=({k:body[k] for k in ('enabled','available') if k in body} if isinstance(body,dict) else {})
        resource=dict(operation='http',method=method,endpoint=endpoint,object_manifest=self.object_manifest,
            recovery=self.recovery,effect=effect,body_fields=sorted(body) if isinstance(body,dict) else [],
            body_sha256=hashlib.sha256(body if isinstance(body,bytes) else canonical(body).encode()).hexdigest())
        path=urlsplit(endpoint).path
        resource['write_operation']=('chunk_add' if method=='POST' and path.endswith('/chunks') else
            'upload' if method=='POST' and path.endswith('/documents') else
            'chunk_patch' if method=='PATCH' and '/chunks/' in path else
            'document_patch' if method=='PATCH' else 'read')
        if endpoint=='/retrieval':resource['datasets']=body.get('dataset_ids',[])
        return self.actions.run(kind,resource,lambda cancel:self.invoke_native(fn),total_timeout=150)

    def invoke_native(self,fn):
        original=getattr(self.native,'opener',None)
        headers=ResponseHeaders(original) if original else None
        if headers:self.native.opener=headers
        try:return fn()
        except Exception as exc:
            if headers and headers.retry_after is not None:exc.retry_after=headers.retry_after
            if str(exc).startswith('RAGFlow 认证失败'):
                raise ServiceConfigurationError('RAGFlow authentication rejected') from None
            raise
        finally:
            if headers:self.native.opener=original

    def request(self,method,endpoint,body=None,content_type='application/json'):
        kind='rag_read' if method=='GET' or endpoint=='/retrieval' else 'rag_write'
        return self.call(kind,method,endpoint,body,lambda:self._once(method,endpoint,body,content_type))

    def _once(self,method,endpoint,body=None,content_type='application/json'):
        if not hasattr(self.native,'_request_once'):return self.native.request(method,endpoint,body,content_type)
        data=body if isinstance(body,bytes) else json.dumps(body).encode() if body is not None else None
        request=Request(self.base_url+'/api/v1'+endpoint,data=data,method=method,
            headers={'Authorization':'Bearer '+self.native.api_key,'Content-Type':content_type})
        return self.native._request_once(request,timeout=15 if method=='GET' else 120)

    def ingest(self,dataset_id,document_id,pipeline_id,*,timeout=600):
        """One admitted write: submit once, then only observe that same in-flight job.

        Pause never admits another job. Manager's existing 30-second remote grace
        bounds collection; lost responses, process death and nonterminal outcomes
        retain the ordinary unresolved-write barrier.
        """
        if not 0 < timeout <= 3600:raise ValueError('invalid native ingest timeout')
        body=dict(doc_ids=[document_id],run='1',delete=False)
        resource=dict(operation='http',method='POST',endpoint='/documents/ingest',
            object_manifest=self.object_manifest,recovery=self.recovery,effect={},
            body_fields=sorted(body),body_sha256=hashlib.sha256(canonical(body).encode()).hexdigest(),
            write_operation='native_ingest',dataset_id=dataset_id,document_id=document_id,pipeline_id=pipeline_id)
        def execute(cancel):
            started=time.monotonic()
            self.invoke_native(lambda:self._once('POST','/documents/ingest',body))
            endpoint=f'/datasets/{quote(dataset_id,safe="")}/documents?id={quote(document_id,safe="")}&page=1&page_size=100'
            polls=0
            while time.monotonic()-started < timeout:
                data=self.invoke_native(lambda:self._once('GET',endpoint));polls+=1
                docs=data.get('docs',[]) if isinstance(data,dict) else data
                if not isinstance(docs,list) or len(docs)!=1 or docs[0].get('id')!=document_id:
                    raise ValueError('native terminal document identity missing')
                doc=docs[0]
                if doc.get('pipeline_id')!=pipeline_id or str(doc.get('status'))!='0':
                    raise ValueError('native job pipeline or availability changed')
                if str(doc.get('run')) in ('3','DONE') and float(doc.get('progress',0))>=1:
                    return dict(document_id=document_id,pipeline_id=pipeline_id,run=doc['run'],
                        progress=doc['progress'],polls=polls,elapsed_seconds=time.monotonic()-started)
                if str(doc.get('run')) in ('FAIL','4','-1','2','CANCEL') or float(doc.get('progress',0))<0:
                    raise RuntimeError('native job not successful; requires reconciliation')
                time.sleep(.25)
            raise TimeoutError('native job terminal readback deadline')
        return self.actions.run('rag_write',resource,execute,total_timeout=timeout+30)

    def upload_bytes(self,ds,name,data):
        return self.call('rag_write','POST',f'/datasets/{quote(ds,safe="")}/documents',data,
            lambda:self.native.upload_bytes(ds,name,data))

    def download_bytes(self,ds,did):
        endpoint=f'/datasets/{quote(ds,safe="")}/documents/{quote(did,safe="")}'
        return self.call('rag_read','GET',endpoint,None,lambda:self._download('/api/v1'+endpoint))

    def download_image(self,image_id):
        endpoint='/documents/images/'+quote(image_id,safe='')
        return self.call('rag_read','GET',endpoint,None,lambda:self._download('/api/v1'+endpoint,image=True))

    def _download(self,path,*,image=False):
        request=Request(self.base_url+path,headers={'Authorization':'Bearer '+self.native.api_key})
        with self.native.opener.open(request,timeout=30) as response:
            if image and not response.headers.get('Content-Type','').lower().startswith('image/'):
                raise ConnectionError('image response is not image bytes')
            return response.read()

    def deployment_identity(self):
        if getattr(self.actions.config,'production_pilot',None):
            return self.request('GET','/paperflow/deployment')
        return self.call('rag_read','GET','/pf-stage4/identity',None,lambda:json.loads(self._download('/pf-stage4/identity')))


class DeliveryCommits:
    worker_kind='delivery'
    def __init__(self,manager,config):
        self.manager,self.config=manager,config
        self.ledger=DeliveryLedger(manager.store)
        from .repair_store import RepairLedger
        self.repairs=RepairLedger(manager.store)
        self.boundary=None  # No deployed end-to-end bound has been verified; never infer one from HTTP timeouts.
        self.clock_anchor=(time.time(),time.monotonic())
        self.read_windows={}
        from .attachment_delivery import AttachmentControl
        self.attachments=AttachmentControl(self)
        manager.delivery=self

    def freeze_active(self,source_id,specialty_code,reason):
        """Durably freeze the selected active version, including while globally paused."""
        key=canonical([source_id,specialty_code]);scope='unit:'+canonical(['target',key]);result=[]
        def freeze(c):
            row=c.execute('SELECT data FROM specialty_active WHERE source_id=? AND specialty_code=?',
                          (source_id,specialty_code)).fetchone()
            if not row:
                previous=c.execute('SELECT manifest_sha256 FROM specialty_safety WHERE source_id=? AND specialty_code=? ORDER BY updated DESC LIMIT 1',
                                   (source_id,specialty_code)).fetchone()
                result.append(self.ledger.safety_sql(c,source_id,specialty_code,previous['manifest_sha256']) if previous else None)
                return
            active=json.loads(row['data']);digest=active['identity']['manifest_sha256']
            c.execute('INSERT OR IGNORE INTO package_freezes VALUES(?,?,?)',(digest,specialty_code,str(reason)[:600]))
            result.append(self.ledger.plan_safety_sql(c,active,str(reason)[:600]))
        self.manager.change_admission_state(scope,uuid.uuid4().hex,{},mutate=freeze)
        if result[0] is None:raise ValueError('no active version to freeze')
        return result[0]

    def check_action(self,c,execution_id,kind,resource):
        ex=self.manager.executions[execution_id]
        if ex.kind!='delivery':raise ValueError('delivery controller cannot authorize another worker kind')
        if not isinstance(resource,dict) or not isinstance(resource.get('identity'),dict):raise ValueError('complete delivery identity required')
        if ex.key.startswith('attachment:'):
            return self.attachments.check_action(c,execution_id,kind,resource)
        identity=resource['identity']
        if ex.key!=target_key(identity):raise ValueError('specialty ownership mismatch')
        operation=resource.get('operation')
        binding=getattr(ex,'run_id',None)
        readback_object=(binding.removeprefix('legacy-readback:') if isinstance(binding,str) and binding.startswith('legacy-readback:') else None)
        if readback_object:
            from .legacy_readback import subject_sql
            subject_sql(self.ledger,c,identity,readback_object)
            if kind not in ('local','rag_read') or (kind=='local' and operation!='legacy_readback'):
                raise ValueError('legacy readback execution is read-only')
            if kind=='rag_read' and resource.get('object_manifest')!=readback_object:
                raise ValueError('legacy readback object changed')
        if kind=='rag_write':
            from .handoff_store import legacy_write_guard
            legacy_write_guard(c,identity['source_id'],identity['specialty_code'])
        legacy=c.execute('SELECT 1 FROM legacy_targets WHERE manifest_sha256=? AND specialty_code=?',
            (identity['manifest_sha256'],identity['specialty_code'])).fetchone() is not None
        legacy_safety=legacy and self.ledger.legacy_safety_allowed_sql(c,identity)
        if legacy and not readback_object:
            if kind=='local' and not (operation=='legacy_readback' or (operation=='safety' and legacy_safety)):
                raise ValueError('legacy target permits only bounded readback or safety')
            if kind not in ('local','rag_read') and not (kind=='rag_write' and legacy_safety):
                raise ValueError('legacy target permits only bounded readback or safety')
        recovery=operation in ('withdraw','safety','reconcile','read_start','load','repair_report','late_repair_report') or (operation=='http' and resource.get('recovery') is True)
        allow_frozen=kind=='rag_read' or recovery
        ctx=self.ledger.check_identity(c,identity,allow_frozen=allow_frozen,allow_legacy=legacy)
        if kind=='local':
            if operation=='legacy_readback' and (legacy or readback_object):return True
            if operation not in ('save','switch','withdraw','safety','reconcile','read_start','load','repair_report','late_repair_report'):raise ValueError('unknown delivery action')
            return allow_frozen
        if operation!='http':raise ValueError('delivery network action requires HTTP descriptor')
        method,path=resource['method'],urlsplit(resource['endpoint']).path
        digest=resource['object_manifest'];version=c.execute('SELECT * FROM delivery_packages WHERE manifest_sha256=?',(digest,)).fetchone()
        mapping=c.execute('SELECT mapping_json FROM specialty_tasks WHERE manifest_sha256=? AND specialty_code=?',(digest,identity['specialty_code'])).fetchone()
        if not version or not mapping or json.loads(version['identity_json'])['source_id']!=identity['source_id']:
            raise ValueError('object version identity mismatch')
        route=json.loads(mapping['mapping_json']);datasets={route.get('paper_dataset_id'),route.get('image_dataset_id')}-{None,''}
        if path=='/documents/ingest':
            if (kind!='rag_write' or method!='POST' or recovery or legacy or digest!=identity['manifest_sha256']
                or not self.config.native_pipeline_id or resource.get('pipeline_id')!=self.config.native_pipeline_id
                or resource.get('write_operation')!='native_ingest'):
                raise ValueError('native ingest requires current configured write owner')
            progress=self.ledger.progress_sql(c,digest,identity['specialty_code']) or {}
            obj=progress.get('receipts',{}).get(identity['specialty_code'],{}).get('body',{})
            batch=obj.get('native_batch',{})
            did=resource.get('document_id');ds=resource.get('dataset_id')
            expected=dict(doc_ids=[did],run='1',delete=False)
            if (not did or ds!=route.get('paper_dataset_id') or obj.get('dataset_id')!=ds or obj.get('document_id')!=did
                or batch.get('state')!='intent' or batch.get('pipeline_id')!=self.config.native_pipeline_id
                or resource.get('body_fields')!=sorted(expected)
                or resource.get('body_sha256')!=hashlib.sha256(canonical(expected).encode()).hexdigest()):
                raise ValueError('native ingest outside durable body intent')
            for row in c.execute("SELECT request FROM control_actions WHERE kind='rag_write' AND state IN ('possible','complete','unresolved','reconciled')"):
                previous=json.loads(row[0])['resource']
                if (previous.get('write_operation')=='native_ingest' and previous.get('dataset_id')==ds
                    and previous.get('document_id')==did):
                    raise ValueError('native ingest already possibly submitted; no replay')
            return False
        if kind=='rag_read':
            if method=='GET' and (path=='/pf-stage4/identity' or
                (path=='/paperflow/deployment' and self.config.production_pilot)):return True
            if method=='POST' and path=='/retrieval' and resource.get('datasets') and set(resource['datasets'])<=datasets:return True
            if method=='GET' and path.startswith('/documents/images/') and any(unquote(path).split('/')[-1].startswith(ds+'-') for ds in datasets):return True
        match=re.fullmatch(r'/datasets/([^/]+)/documents(?:/([^/]+))?(?:/chunks(?:/([^/]+))?)?',path)
        if not match or unquote(match[1]) not in datasets or match[2] in ('parse','upload_info'):
            raise ValueError('delivery endpoint outside specialty datasets')
        if kind=='rag_read':
            if method!='GET':raise ValueError('read action method mismatch')
            return True
        if kind!='rag_write' or method not in ('POST','PATCH'):raise ValueError('unsupported delivery write')
        effect=resource.get('effect',{})
        switching=c.execute('SELECT * FROM specialty_switches WHERE source_id=? AND specialty_code=?',
            (identity['source_id'],identity['specialty_code'])).fetchone()
        plan=self.ledger.safety_sql(c,identity['source_id'],identity['specialty_code'],digest)
        safety_disable=(recovery and plan and plan['state']=='pending' and method=='PATCH' and match[2]
                        and effect in ({'enabled':0},{'available':False}))
        if legacy and not safety_disable:raise ValueError('legacy writes are limited to recorded safety disable')
        if safety_disable:
            expected_effect={'available':False} if match[3] else {'enabled':0}
            if effect!=expected_effect or (not match[3] and path.endswith('/chunks')):
                raise ValueError('safety availability endpoint mismatch')
        if recovery or digest!=identity['manifest_sha256']:
            if method=='PATCH' and resource.get('body_fields')!=sorted(effect):
                raise ValueError('recovery may only change availability')
            if safety_disable:pass
            elif not switching or switching['phase'] in ('committed','withdrawn') or method!='PATCH' or not match[2]:
                raise ValueError('no unfinished switch authorizes recovery write')
            else:
                previous=json.loads(switching['previous_json']) if switching['previous_json'] else None
                previous_digest=previous['identity']['manifest_sha256'] if previous else None
                disabled=effect in ({'enabled':0},{'available':False})
                enabled=effect in ({'enabled':1},{'available':True})
                if not ((digest==switching['candidate'] and disabled and recovery) or
                        (digest==previous_digest and (disabled or (enabled and recovery)))):
                    raise ValueError('recovery may only withdraw candidate or restore previous version')
                if enabled and self.ledger.frozen_sql(c,dict(identity,manifest_sha256=digest)):
                    raise ValueError('previous version is frozen')
        if match[2]:
            progress=self.ledger.progress_sql(c,digest,identity['specialty_code'])
            objects=(plan['data'] if safety_disable else (progress or {})).get('receipts',{}).get(identity['specialty_code'],{})
            if not any(obj.get('dataset_id')==unquote(match[1]) and obj.get('document_id')==unquote(match[2])
                for key,obj in objects.items() if key in ('body','images')):raise ValueError('document is outside recorded delivery ownership')
            if safety_disable and match[3] and not any(
                obj.get('dataset_id')==unquote(match[1]) and obj.get('document_id')==unquote(match[2])
                and any(ch.get('chunk_id')==unquote(match[3]) for ch in obj.get('chunks',{}).values())
                for key,obj in objects.items() if key in ('body','images')):
                raise ValueError('chunk is outside recorded safety ownership')
        return recovery

    def guard(self,execution_id,operation):
        m=self.manager
        with m.gate_lock:
            ex=m.executions.get(execution_id);action=ex.active_action if ex else None
            if not ex or ex.kind!=self.worker_kind or not action or action['kind']!='local':raise ValueError('delivery commit requires local action')
            ticket=m.tickets[action['action_id']]
            if ticket['request']['resource'].get('operation')!=operation or ticket.get('qualified'):raise ValueError('delivery commit action mismatch')
            if not m._valid(ticket['snapshot'],execution_id) or time.monotonic()>=action['deadline']:raise ValueError('delivery commit lost qualification')
            return ex,action,ticket

    def qualify(self,execution_id,action,ticket):
        m=self.manager
        with m.gate_lock:
            if (not m._valid(ticket['snapshot'],execution_id) or m.executions[execution_id].active_action is not action
                or ticket.get('qualified') or time.monotonic()>=action['deadline']):raise ValueError('final delivery qualification denied')
            ticket['qualified']=True
            m._event('delivery_result_qualified',execution_id=execution_id,action_id=action['action_id'])

    def stage(self,execution_id,payload):
        path=self.config.root/'delivery-commits'/execution_id/(uuid.uuid4().hex+'.json')
        path.parent.mkdir(parents=True,exist_ok=True);save(path,payload)
        return dict(path=str(path),sha256=sha(path))

    def handle(self,execution_id,operation,descriptor):
        m=self.manager
        # Capture the end of the network read before any file/SQL waiting.
        finished=time.monotonic()
        with m.gate_lock:
            ex,action,ticket=self.guard(execution_id,operation)
            if execution_id in m.publications:raise ValueError('delivery commit already pending')
            m.publications.add(execution_id)
        try:
            if set(descriptor)!={'path','sha256'}:raise ValueError('invalid delivery descriptor')
            path=Path(descriptor['path']).resolve()
            if not path.is_relative_to((self.config.root/'delivery-commits'/execution_id).resolve()) or path.stat().st_size>8*1024*1024:
                raise ValueError('delivery evidence outside execution')
            if sha(path)!=descriptor['sha256']:raise ValueError('delivery evidence changed')
            payload=load(path)
            if payload['identity']!=ticket['request']['resource']['identity']:raise ValueError('delivery evidence identity mismatch')
            legacy=0
            if operation in ('repair_report','late_repair_report'):
                from .repair_control import legacy_count
                legacy=legacy_count(self.config,m.store,payload['identity'])
            if operation=='reconcile':
                with m.gate_lock:window=self.read_windows.pop(execution_id,None)
                if not window or window['read_id']!=payload.get('read_id') or window['epoch']!=m.epoch:
                    raise ValueError('current manager read marker required')
                wall,monotonic=self.clock_anchor
                payload['evidence']=dict(payload['evidence'],clock_epoch=m.epoch,
                    clock_reliable=abs((time.time()-wall)-(time.monotonic()-monotonic))<.01,
                    read_window=dict(window,finished=finished),epoch_started=monotonic)
            def write(c):
                allow_frozen=self.check_action(c,execution_id,'local',ticket['request']['resource'])
                m.ledger.check(c,ticket['snapshot'],execution_id,'local',ticket['request']['expected_generation'],allow_frozen=allow_frozen)
                if ex.key.startswith('attachment:'):
                    return self.attachments.commit_sql(c,execution_id,operation,payload)
                if operation in ('switch','withdraw') or (operation=='safety' and payload.get('phase')=='disabled'):
                    m.ledger.check(c,ticket['snapshot'],execution_id,'rag_write',ticket['request']['expected_generation'],allow_frozen=allow_frozen)
                if operation=='save':return self.ledger.save_sql(c,payload)
                if operation=='legacy_readback':
                    from .legacy_readback import commit_readback
                    binding=getattr(ex,'run_id',None)
                    subject=payload.get('source_manifest',payload['identity']['manifest_sha256'])
                    if binding and binding!='legacy-readback:'+subject:raise ValueError('legacy readback binding changed')
                    return commit_readback(self.ledger,c,payload,epoch=m.epoch)
                if operation in ('repair_report','late_repair_report'):
                    fields={'identity','stage','problem_type','evidence'}
                    if operation=='late_repair_report':fields.add('source_manifest')
                    if set(payload)!=fields:raise ValueError('invalid content report')
                    source=(self.ledger.report_source_sql(c,payload['identity'],payload['source_manifest'])
                            if operation=='late_repair_report' else payload['identity'])
                    return self.repairs.request_sql(c,source,payload['stage'],payload['problem_type'],
                        payload['identity']['specialty_code'],payload['evidence'],legacy_count=legacy)
                if operation=='switch':return self.ledger.switch_sql(c,payload)
                if operation=='withdraw':return self.ledger.withdraw_sql(c,payload)
                if operation=='safety':return self.ledger.safety_commit_sql(c,payload)
                if operation=='reconcile':return self.ledger.reconcile_sql(c,payload,self.boundary)
                if operation=='read_start':
                    # No deployed protocol proves downstream terminality from an HTTP response.
                    return {}
                raise ValueError('unknown delivery commit')
            result=m.ledger._write(write,still_valid=lambda:m._valid(ticket['snapshot'],execution_id),
                before_commit=lambda:self.qualify(execution_id,action,ticket))
            if operation=='read_start':
                window=dict(read_id=uuid.uuid4().hex,epoch=m.epoch,started=time.monotonic(),**result)
                with m.gate_lock:self.read_windows[execution_id]=window
                return dict(read_id=window['read_id'])
            return result
        finally:
            with m.gate_lock:m.publications.discard(execution_id)
