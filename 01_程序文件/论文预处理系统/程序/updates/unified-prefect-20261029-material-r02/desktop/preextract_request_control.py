"""Bind the native transport to existing Manager permits and scope authority."""
from pathlib import Path
import json
import time
import uuid

from paperflow.prepared_protocol import fingerprint
from preextract_material import file_sha,write_json
from preextract_scheduler_adapter import request_observation,body_permit


class RequestControl:
    def __init__(self,client,registry,scope_id,generation,task_id,record,observation,*,purpose_config,limit=4,timeout=1800,disk=None):
        self.client,self.registry=client,registry
        self.scope_id,self.generation,self.task_id=scope_id,generation,task_id
        self.record,self.observation,self.purpose_config=record,observation,purpose_config
        self.limit,self.timeout=limit,timeout
        self.disk=disk

    def dispatch(self,send,descriptor,*,purpose=False):
        relative=Path(descriptor['work_dir']).resolve().relative_to(Path(self.observation.evidence['work_dir']).resolve()/'model') if not purpose else None
        stage='purpose' if purpose else 'body' if not relative.parts else 'part:'+relative.parts[0]
        correction=not purpose and bool(relative.parts) and relative.parts[0].startswith('content-repair-')
        if correction:stage='body'  # Corrections share the original body attempt budget.
        request_key=fingerprint(dict(task=self.task_id,directory=str(Path(descriptor['work_dir']).resolve()),
                                    request=descriptor['request_sha256']))
        with self.registry.store.db() as c:
            old=c.execute('SELECT state FROM prepared_http WHERE request_key=?',(request_key,)).fetchone()
        if old and old['state']=='not_sent':request_key+=':'+uuid.uuid4().hex
        result=[];entered=[False]
        def permitted(cancel):
            entered[0]=True
            self.registry.assert_running(self.scope_id,self.generation)
            if cancel.is_set():raise InterruptedError('scope stopped before HTTP')
            if not purpose and not body_permit(self.record,self.observation,self.purpose_config)['allowed']:
                raise ValueError('body purpose proof required before HTTP')
            from preextract_disk_pressure import DiskPressure
            if not (self.disk or DiskPressure(Path(descriptor['work_dir']))).ready():
                from paperflow.scheduling_policy import ActionDeferred
                raise ActionDeferred('disk')
            historical=unregistered_attempts(self,descriptor,stage,purpose=purpose)
            if correction:
                self._check_correction_budget()
            if historical>=self.limit:raise ValueError('actual HTTP budget exhausted by retained attempts')
            self.registry.begin_http(self.scope_id,self.generation,self.task_id,request_key,stage,
                                     descriptor['request_sha256'],limit=max(1,self.limit-historical),evidence=dict(work_dir=descriptor['work_dir']))
            try:
                if cancel.is_set():
                    self.registry.finish_http(request_key,'not_sent',dict(reason='stopped_before_transport',work_dir=descriptor['work_dir']))
                    raise InterruptedError('stopped before transport')
                result.append(send())
            except BaseException:
                # Native request_once writes its exception facts while unwinding.
                # Unknown stays conservative until the worker observes that journal.
                with self.registry.store.db() as c:
                    row=c.execute('SELECT state FROM prepared_http WHERE request_key=?',(request_key,)).fetchone()
                if row and row['state']=='possible':
                    outcome='not_sent' if descriptor.get('http_invoked') is False else 'unknown'
                    self.registry.finish_http(request_key,outcome,dict(work_dir=descriptor['work_dir']))
                raise
            proof=dict(work_dir=descriptor['work_dir'],response_sha256=file_sha(Path(descriptor['work_dir'])/'response.txt'))
            self.registry.finish_http(request_key,'complete',proof)
            return proof  # Large answer never crosses IPC.
        try:
            self.client.perform('llm',dict(operation='prepared_purpose' if purpose else 'prepared_body',
                task_id=self.task_id,request_key=request_key),permitted,total_timeout=self.timeout)
        except BaseException:
            if not entered[0]:
                self.registry.record_not_sent(self.scope_id,self.generation,self.task_id,request_key+':denied:'+uuid.uuid4().hex,
                    stage,descriptor['request_sha256'],descriptor['work_dir'])
            raise
        return result[0]

    def _check_correction_budget(self):
        from preextract_content_recovery import _ancestors,_read
        roots=list(_ancestors(self.record['path'],Path(self.observation.evidence['work_dir'])))
        paths=[p for root in roots for p in (root/'model').glob('content-repair-*/request-state.json')]
        count=sum(1 for p in paths if _read(p).get('http_invoked') is not False and
                  (_read(p).get('http_started_at') or _read(p).get('client_http_requests')))
        if count>=2:raise ValueError('content correction budget exhausted')
        for root in roots:
            path=root/'model/partition-plan.json'
            if path.exists():
                plan=_read(path);state=_read(root/'model/partition-state.json')
                if len(state.get('requests',[]))+len(state.get('recoveries',[]))+count>=plan['max_model_requests']:
                    raise ValueError('partition total request budget exhausted by correction')
                break

    def reconcile_requests(self):
        reconcile_saved_requests(self.registry,self.task_id)

    @property
    def marker(self):
        return dict(schema='prepared-scope-binding/1',database=str(self.registry.store.path),
            scope_id=self.scope_id,generation=self.generation,task_id=self.task_id)

    def purpose_directory(self,directory):
        self.registry.assert_running(self.scope_id,self.generation)
        root=Path(directory).resolve();task=self.registry.store.get(self.task_id)
        selected=Path(task.get('prepared_purpose_directory') or root)
        if selected!=root and not selected.is_relative_to(root/'revisions'):selected=root
        phase,step,_=request_observation(selected)
        if step=='reconcile':raise ValueError('purpose outcome unknown')
        if step=='adopt_saved_response':adopt_saved_response(selected)
        if step=='recover':
            target=root/'revisions'/uuid.uuid4().hex;target.mkdir(parents=True)
            write_json(target/'prepared-purpose-revision.json',dict(parent=str(selected)))
            selected=target
        self.registry.store.update(self.task_id,prepared_purpose_directory=str(selected))
        return selected


def reconcile_saved_requests(registry,task_id):
    with registry.store.db() as c:
        rows=[dict(r) for r in c.execute("SELECT * FROM prepared_http WHERE task_id=? AND state IN ('possible','unknown')",(task_id,))]
    for row in rows:
        prior=json.loads(row['result'] or '{}');directory=prior.get('work_dir')
        if not directory:continue
        try:phase,step,evidence=request_observation(directory)
        except (OSError,ValueError,KeyError,TypeError):continue
        if evidence.get('request_sha256')!=row['request_sha256']:continue
        if phase=='response_complete':registry.finish_http(row['request_key'],'complete',evidence)
        elif phase=='failed_ended':registry.finish_http(row['request_key'],'failed',evidence)


def not_sent_proof(registry,task_id,directory):
    directory=Path(directory).resolve();state=json.loads((directory/'request-state.json').read_text(encoding='utf8'))
    if state.get('http_invoked') is not False or not state.get('finished_at'):raise ValueError('transport absence not proved')
    with registry.store.db() as connection:
        rows=connection.execute("SELECT * FROM prepared_http WHERE task_id=? AND request_sha256=? AND state='not_sent'",
            (task_id,state.get('request_sha256'))).fetchall()
    for row in rows:
        evidence=json.loads(row['result'] or '{}')
        if evidence.get('work_dir') and Path(evidence['work_dir']).resolve()==directory:
            return dict(database=str(registry.store.path),task_id=task_id,request_key=row['request_key'],
                request_sha256=row['request_sha256'],state_sha256=file_sha(directory/'request-state.json'))
    raise ValueError('durable not-sent proof missing')


def verify_not_sent(proof,directory):
    from preextract_scope_guard import registry_for
    registry=registry_for(dict(database=proof['database']))
    current=not_sent_proof(registry,proof['task_id'],directory)
    if current!=proof:raise ValueError('not-sent recovery proof changed')


def registered_not_sent(work,directory,request_sha256):
    from preextract_scope_guard import binding,registry_for
    marker=binding(work)
    if not marker:return False
    registry=registry_for(marker)
    with registry.store.db() as connection:
        rows=connection.execute('SELECT * FROM prepared_http WHERE task_id=? ORDER BY started DESC',(marker['task_id'],)).fetchall()
    for row in rows:
        evidence=json.loads(row['result'] or '{}')
        if evidence.get('work_dir') and Path(evidence['work_dir']).resolve()==Path(directory).resolve():
            return row['state']=='not_sent' and row['request_sha256']==request_sha256
    return False


def adopt_saved_response(directory):
    """Append verified recovery evidence. Original intent, response and events stay intact."""
    directory=Path(directory)
    phase,step,proof=request_observation(directory)
    if phase!='response_complete':raise ValueError('complete response evidence required')
    if step=='postprocess':return
    path=directory/'adopted-response.json'
    value=dict(schema='verified-saved-response/1',request_state_sha256=proof['request_state_sha256'],
        request_sha256=proof['request_sha256'],response_sha256=proof['response_sha256'],events_sha256=proof.get('events_sha256'))
    if path.exists():
        if json.loads(path.read_text(encoding='utf8'))!=value:raise ValueError('adoption proof changed')
    else:
        with path.open('x',encoding='utf8') as handle:
            json.dump(value,handle,ensure_ascii=False);handle.flush()
            import os
            os.fsync(handle.fileno())


def verified_saved_state(directory):
    """Read a complete native state or its independently bound adoption receipt."""
    directory=Path(directory);state=json.loads((directory/'request-state.json').read_text(encoding='utf8'))
    if state.get('status')=='response_complete':return state
    path=directory/'adopted-response.json'
    if not path.is_file():return state
    proof=json.loads(path.read_text(encoding='utf8'))
    phase,_,current=request_observation(directory)
    if (proof.get('schema')!='verified-saved-response/1' or phase!='response_complete'
            or any(proof.get(k)!=current.get(k) for k in ('request_state_sha256','request_sha256','response_sha256','events_sha256'))):
        raise ValueError('adoption proof no longer matches saved evidence')
    return dict(state,status='response_complete',response_sha256=proof['response_sha256'])


def unregistered_attempts(control,descriptor,stage,*,purpose=False):
    """Count native attempts from before scope adoption; revision creation cannot reset it."""
    if purpose:
        current=Path(descriptor['work_dir']).resolve();seen=set();paths=[]
        while True:
            if current in seen:raise ValueError('purpose revision parent cycle')
            seen.add(current);paths.append(current/'request-state.json')
            proof=current/'prepared-purpose-revision.json'
            if not proof.exists():break
            parent=Path(json.loads(proof.read_text(encoding='utf8'))['parent']).resolve()
            root=current.parent.parent if current.parent.name=='revisions' else current
            if parent!=root and not parent.is_relative_to(root/'revisions'):raise ValueError('purpose revision parent outside root')
            current=parent
    else:
        work=Path(control.observation.evidence['work_dir']);roots=[];seen=set()
        while True:
            resolved=work.resolve()
            if resolved in seen:raise ValueError('revision parent cycle')
            seen.add(resolved);roots.append(work)
            revision=work/'revision.json'
            if not revision.exists():break
            work=Path(json.loads(revision.read_text(encoding='utf8'))['parent'])
        if stage=='body':
            paths=[p/'model/request-state.json' for p in roots]
            paths.extend(p for root in roots for p in (root/'model').glob('content-repair-*/request-state.json'))
        else:
            child=stage.removeprefix('part:')
            paths=[]
            for root in roots:
                base=root/'model'/child
                paths.append(base/'request-state.json')
                if (base/'revisions').is_dir():paths.extend((base/'revisions').glob('*/request-state.json'))
    with control.registry.store.db() as c:
        known={r[0] for r in c.execute('SELECT request_key FROM prepared_http WHERE task_id=?',(control.task_id,))}
    count=0
    for path in paths:
        if not path.is_file():continue
        state=json.loads(path.read_text(encoding='utf8'))
        key=fingerprint(dict(task=control.task_id,directory=str(path.parent.resolve()),request=state.get('request_sha256')))
        if key not in known and state.get('http_invoked') is not False:count+=int(bool(state.get('http_started_at') or state.get('client_http_requests')))
    return count
