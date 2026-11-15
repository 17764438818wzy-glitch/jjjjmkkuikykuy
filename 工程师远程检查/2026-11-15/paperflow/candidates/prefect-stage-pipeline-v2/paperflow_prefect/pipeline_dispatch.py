"""Registered handoff: durable result -> intent -> apply_async -> confirmed IDs.

Prefect owns queues/capacity. A lost apply_async reply is queried using canonical
operation/sequence tags; an uncertain query never becomes permission to resend.
"""
import asyncio
from contextlib import contextmanager
import json
import os
from pathlib import Path
import time
from urllib.parse import urlparse
import urllib.request
from filelock import FileLock
from .pipeline_contracts import ArtifactRef, StageEnvelope, StageResult, STAGES, operation_key, ready_stages
from .pipeline_ledger import StageLedger, EvidenceChanged, OperationBusy, OutcomeUnknown, owner_alive
from .pipeline_runtime import artifact, verify_ref, configured_path
from .records import read_json, write_json, identity, sha_file


class SubmissionUncertain(RuntimeError): pass


def _http(url,data=None):
    request=urllib.request.Request(url,data=None if data is None else json.dumps(data).encode(),
        headers={'Content-Type':'application/json'})
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request,timeout=5) as response:
        return json.loads(response.read())


class NativeGateway:
    def __init__(self,pipeline,config_sha256,tasks=None):
        self.pipeline=pipeline; self.config_sha256=config_sha256
        if tasks is None:
            from .pipeline_tasks import TASKS
            tasks=TASKS
        self.tasks=tasks

    def task(self,env):
        return self.tasks['model_recovery' if env.dispatch_kind.value=='recovery' else env.stage.value]

    def task_key(self,env): return self.task(env).task_key

    @contextmanager
    def settings(self):
        from prefect.settings import temporary_settings, PREFECT_API_URL, PREFECT_SERVER_ALLOW_EPHEMERAL_MODE
        from prefect.settings import PREFECT_TASK_SCHEDULING_DEFAULT_STORAGE_BLOCK
        with temporary_settings({PREFECT_API_URL:self.pipeline['prefect_api_url'],
                PREFECT_SERVER_ALLOW_EPHEMERAL_MODE:False,
                PREFECT_TASK_SCHEDULING_DEFAULT_STORAGE_BLOCK:'local-file-system/'+self.pipeline['parameter_storage_block']}):
            yield

    def tags(self,env):
        return ['pipeline-operation:'+env.operation_key,'pipeline-dispatch:'+str(env.dispatch_seq)]

    def find(self,env):
        from prefect.client.orchestration import get_client
        from prefect.client.schemas.filters import TaskRunFilter, TaskRunFilterTags
        with self.settings(),get_client(sync_client=True) as client:
            runs=client.read_task_runs(task_run_filter=TaskRunFilter(tags=TaskRunFilterTags(all_=self.tags(env))))
        rows=[]
        for run in runs:
            if run.task_key!=self.task_key(env): raise EvidenceChanged('tagged task key mismatch')
            rows.append(dict(id=str(run.id),state=run.state_type.value if run.state_type else None,
                task_key=run.task_key,created=run.created.timestamp()))
        return rows

    def submit(self,env):
        from prefect.context import TagsContext
        from prefect.filesystems import LocalFileSystem
        from .pipeline_tasks import no_cache
        # A child must not inherit an ancestor's operation tags: tag intersection
        # is our accepted-before-local-ID lookup protocol, not a provenance chain.
        with self.settings(),TagsContext(current_tags=set(self.tags(env))):
            block=LocalFileSystem(basepath=self.pipeline['storage_root'])
            block.save(self.pipeline['parameter_storage_block'],overwrite=True,_sync=True)
            task=self.task(env).with_options(result_storage=block,cache_key_fn=no_cache,
                persist_result=True,cache_result_in_memory=False)
            return str(task.apply_async(kwargs={'envelope':env.to_dict()}).task_run_id)

    def availability(self,env,run_ids):
        """Native /status joined to PID/create/config/client_id and Server identity.

        An unqueryable live matching Worker makes ownership uncertain. Native
        available is observation, never a second custom capacity counter.
        """
        key=self.task_key(env); workers=[]; owned=False; certain=True; idle=False
        server=_http(self.pipeline['prefect_api_url'].rstrip('/')+'/task_workers/filter',{})
        for path in (Path(self.pipeline['ledger_root'])/'workers').glob('*.json'):
            row=read_json(path)
            if key not in row.get('task_keys',[]) or not owner_alive(row): continue
            if (row.get('schema')!='pipeline-worker/1' or row.get('instance_id')!=self.pipeline['instance_id']
                    or row.get('config_sha256')!=self.config_sha256):
                certain=False; continue
            parsed=urlparse(row.get('status_url',''))
            if parsed.scheme!='http' or parsed.hostname!='127.0.0.1' or parsed.path!='/status':
                certain=False; continue
            try:
                from datetime import datetime
                status=_http(row['status_url']); started=datetime.fromisoformat(status['started_at']).timestamp()
                matches=[w for w in server if w['identifier']==row.get('client_id') and key in w['task_keys']]
                if (status['client_id']!=row.get('client_id') or started<row['process_create_time']-2
                        or set(status['tasks'])!=set(row['task_keys']) or not matches):
                    certain=False; continue
                if status['stopping']: certain=False; continue
                active={r for values in status['in_flight'].values() for r in values}
                owned |= bool(active.intersection(run_ids)); idle |= status['available']>0
                workers.append(dict(pid=row['pid'],client_id=status['client_id'],available=status['available']))
            except (OSError,ValueError,KeyError,TypeError): certain=False
        return dict(idle=idle,certain=certain,owned=owned,workers=workers)


def _slot(stage,item=None): return stage+':'+(item or '')


class DispatchCoordinator:
    def __init__(self,ledger,gateway,*,instance_id,local_outcome=None,remote_outcome=None,fence=None):
        self.ledger=ledger; self.gateway=gateway; self.instance_id=instance_id
        self.local_outcome=local_outcome; self.remote_outcome=remote_outcome
        self.fence=fence or (lambda env: None)

    def _scope_path(self,batch): return self.ledger.root/'scopes'/(identity(batch)+'.json')

    def _lock(self,kind,key):
        path=self.ledger.root/'locks'/kind/(identity(key)+'.lock'); path.parent.mkdir(parents=True,exist_ok=True)
        return FileLock(str(path),timeout=0)

    def register(self,scope_manifest,request_id):
        from .stage3_handoff import read_receipt
        if not isinstance(request_id,str) or not request_id.strip(): raise ValueError('request_id required')
        reference=artifact(scope_manifest); value=read_json(reference.path)
        if value.get('schema')!='pipeline-scope/1': raise ValueError('pipeline-scope/1 required')
        if set(value)!={'schema','batch_id','contracts','control_generation','papers'}:
            raise ValueError('scope accepts frozen inputs only, never caller success claims')
        batch=value['batch_id']; contracts=value['contracts']; generation=value['control_generation']
        if not isinstance(batch,str) or not batch.strip(): raise ValueError('batch_id required')
        if set(contracts)!=set(STAGES) or any(not isinstance(c,str) or not c.strip() for c in contracts.values()):
            raise ValueError('all stage contracts must be explicit')
        papers=[]; seen=set()
        for row in value['papers']:
            if set(row)!={'receipt_ref'}: raise ValueError('paper registration accepts only a verified receipt reference')
            receipt_ref=ArtifactRef.from_dict(row['receipt_ref']); verify_ref(receipt_ref)
            receipt,paper=read_receipt(receipt_ref.path)
            if not paper or paper['paper_id'] in seen: raise ValueError('missing or duplicate admitted paper')
            seen.add(paper['paper_id'])
            for source in ([paper['body']] if paper.get('body') else [])+paper['attachments']:
                verify_ref(ArtifactRef(source['path'],source['sha256']))
            papers.append(dict(paper_id=paper['paper_id'],receipt_ref=receipt_ref.to_dict(),paper=paper,
                whole_receipt_verified=receipt['state']=='已接收',
                inventory_complete=receipt['upstream']['classification_complete'] is True,operations={}))
        if not papers: raise ValueError('scope must contain admitted papers')
        record=dict(schema='pipeline-registration/1',batch_id=batch,request_id=request_id,
            scope_ref=reference.to_dict(),instance_id=self.instance_id,contracts=contracts,
            control_generation=generation,papers=papers)
        self._build(record,papers[0],'intake',None,{})  # Validate generation via canonical contract.
        request_path=self.ledger.root/'registrations'/(identity(request_id)+'.json')
        with self._lock('registration','all'):
            if request_path.exists() and read_json(request_path)!={'batch_id':batch,'scope_sha256':reference.sha256}:
                raise EvidenceChanged('request_id already binds a different scope')
            path=self._scope_path(batch)
            if path.exists():
                old=read_json(path)
                if old['scope_ref']!=reference.to_dict() or old['request_id']!=request_id:
                    raise EvidenceChanged('batch registration is immutable')
            else: write_json(path,record)
            write_json(request_path,dict(batch_id=batch,scope_sha256=reference.sha256))
        return dict(batch_id=batch,request_id=request_id,registered=len(papers))

    def _read_scope(self,batch):
        record=read_json(self._scope_path(batch))
        if record.get('schema')!='pipeline-registration/1' or record['batch_id']!=batch:
            raise EvidenceChanged('invalid registered scope')
        verify_ref(ArtifactRef.from_dict(record['scope_ref']))
        if record['instance_id']!=self.instance_id: raise EvidenceChanged('runtime instance mismatch')
        return record

    def envelopes(self,batch):
        return [StageEnvelope.from_dict(e) for p in self._read_scope(batch)['papers'] for e in p['operations'].values()]

    def intent_path(self,env): return self.ledger.root/'handoffs'/env.operation_key/(str(env.dispatch_seq)+'.json')

    def intent(self,env):
        path=self.intent_path(env); return read_json(path) if path.exists() else None

    def _snapshot(self,registration,paper):
        snapshot=dict(paper_id=paper['paper_id'],batch_id=registration['batch_id'],
            body_admitted=paper['paper'].get('body') is not None,
            whole_receipt_verified=paper['whole_receipt_verified'],stages={},
            attachments=dict(inventory_state='complete' if paper['inventory_complete'] else 'pending',
                items={r['file_id']:{'required':True} for r in paper['paper']['attachments']}))
        results={}
        for slot,data in paper['operations'].items():
            env=StageEnvelope.from_dict(data)
            try: status=self.ledger.dispatch_status(env)
            except OperationBusy:
                # One active attachment cannot prevent a ready body successor.
                status=dict(record={'state':'running'},result=None)
            state=status['record']['state']; result=status['result']; row=dict(state=state)
            if result:
                results[slot]=StageResult.from_dict(result)
                if state=='skipped' and env.item_id is None and not paper['paper']['attachments']:
                    proofs=[read_json(r.path) for r in results[slot].evidence_refs if Path(r.path).suffix=='.json']
                    if not any(p.get('reason')=='empty_inventory' for p in proofs):
                        raise EvidenceChanged('empty inventory skip proof missing')
                    row['reason']='empty_inventory'
            elif state in {'complete','passed','verified','skipped'}:
                raise EvidenceChanged('unverified success in ledger')
            if env.item_id is None: snapshot['stages'][env.stage.value]=row
            else: snapshot['attachments']['items'][env.item_id][env.stage.value]=row
        return snapshot,results

    def _build(self,registration,paper,stage,item,results):
        sources=paper['paper']; source=sources.get('body')
        if item is not None: source=next(r for r in sources['attachments'] if r['file_id']==item)
        digest=source['sha256'] if source else identity(sources); refs=[]
        if stage in {'prepare','attachment'} and source: refs.append(ArtifactRef(source['path'],source['sha256']))
        # Intake is a readiness gate; its receipt/attachment inventory is NOT a body input.
        prerequisites={'intake':[], 'prepare':[], 'profile':[_slot('prepare')], 'attachment':[],
            'package':[_slot('profile')], 'rag_body':[_slot('package')],
            'rag_attachment':[_slot('attachment',item),_slot('rag_body')],
            'archive':[_slot('rag_body')]+([_slot('rag_attachment',r['file_id']) for r in sources['attachments']]
                         or [_slot('rag_attachment')]),'cleanup':[_slot('archive')],'finish':[_slot('cleanup')]}[stage]
        for dependency in prerequisites:
            if dependency not in results: raise EvidenceChanged('required predecessor result missing')
            selected=results[dependency].evidence_refs
            if stage=='profile':
                selected=tuple(r for r in selected if Path(r.path).name=='material.json')
                if len(selected)!=1: raise EvidenceChanged('prepare must bind exactly one body material.json')
            refs.extend(selected)
        refs=tuple(dict.fromkeys(refs))
        key=operation_key(dict(paper_id=paper['paper_id'],source_sha256=digest,stage=stage,item_id=item,
            part_id=None,input_refs=refs,contract_version=registration['contracts'][stage]))
        return StageEnvelope(registration['batch_id'],paper['paper_id'],ArtifactRef.from_dict(paper['receipt_ref']),
            key,stage,item,None,refs,registration['contracts'][stage],registration['control_generation'],0,'normal')

    def _eligible(self,snapshot,stage,item):
        if stage not in ready_stages(snapshot): return False
        if item is not None:
            row=snapshot['attachments']['items'][item]
            if row.get(stage,{}).get('state','pending')!='pending': return False
            if stage=='rag_attachment' and row.get('attachment',{}).get('state') not in {'passed','verified'}: return False
        return True

    def _registered(self,env):
        registration=self._read_scope(env.batch_id)
        paper=next(p for p in registration['papers'] if p['paper_id']==env.paper_id)
        if paper['operations'].get(_slot(env.stage.value,env.item_id))!=env.to_dict():
            raise EvidenceChanged('envelope is not the registered current dispatch')
        return registration,paper

    def _save_envelope(self,env):
        with self._lock('scope',env.batch_id):
            registration=self._read_scope(env.batch_id)
            paper=next(p for p in registration['papers'] if p['paper_id']==env.paper_id)
            previous=paper['operations'].get(_slot(env.stage.value,env.item_id))
            if previous and (previous['operation_key']!=env.operation_key or previous['dispatch_seq']>env.dispatch_seq):
                raise EvidenceChanged('cannot overwrite canonical operation or newer dispatch')
            paper['operations'][_slot(env.stage.value,env.item_id)]=env.to_dict()
            write_json(self._scope_path(env.batch_id),registration)

    def dispatch_ready(self,env):
        if isinstance(env,dict): env=StageEnvelope.from_dict(env)
        with self._lock('handoff',env.operation_key):
            registration,paper=self._registered(env); self.fence(env); status=self.ledger.dispatch_status(env)
            if status['result'] is not None: raise EvidenceChanged('already completed; no new delivery')
            if status['live']: raise OperationBusy('known live owner')
            if status['sent'] or status['record']['state']=='unknown': raise OutcomeUnknown('sent operation needs reconciliation')
            snapshot,_=self._snapshot(registration,paper)
            if not self._eligible(snapshot,env.stage.value,env.item_id): raise EvidenceChanged('dependencies not ready')
            intent=self.intent(env)
            if intent is None:
                self.ledger.set_dispatch(env)
                intent=dict(schema='pipeline-handoff/1',envelope=env.to_dict(),phase='prepared',
                    prepared_at=time.time(),task_run_ids=[],task_key=self.gateway.task_key(env))
                write_json(self.intent_path(env),intent)
            runs=self.gateway.find(env)
            if runs:
                intent.update(phase='confirmed',task_run_ids=sorted(r['id'] for r in runs)); write_json(self.intent_path(env),intent)
                return intent['task_run_ids'][0]
            if intent['phase']!='prepared': raise SubmissionUncertain('submission may have been accepted')
            intent.update(phase='sending',sending_at=time.time()); write_json(self.intent_path(env),intent)
            identifier=self.gateway.submit(env)
            intent.update(phase='confirmed',task_run_ids=[identifier]); write_json(self.intent_path(env),intent)
            return identifier

    def reconcile_now(self,batch):
        from .pipeline_reconcile import reconcile_registered
        return reconcile_registered(self,batch)

    async def reconcile_scope(self,batch): return await asyncio.to_thread(self.reconcile_now,batch)


def configured_coordinator():
    path=configured_path(); digest=os.environ.get('PAPERFLOW_PIPELINE_CONFIG_SHA256')
    if not digest or sha_file(path)!=digest: raise EvidenceChanged('configuration binding changed')
    pipeline=read_json(path)['pipeline']; ledger=StageLedger(Path(pipeline['ledger_root']))
    def fence(env):
        if sha_file(path)!=digest: raise EvidenceChanged('configuration changed')
        current=read_json(pipeline['control_path'])
        if current.get('allowed') is not True or current.get('generation')!=env.control_generation:
            raise InterruptedError('control fence closed')
    return DispatchCoordinator(ledger,NativeGateway(pipeline,digest),instance_id=pipeline['instance_id'],fence=fence)


def dispatch_ready(envelope: StageEnvelope) -> str:
    return configured_coordinator().dispatch_ready(envelope)
