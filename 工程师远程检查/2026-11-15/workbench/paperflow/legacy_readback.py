"""Read-only native legacy receipt verification under the existing Job/IPC owner."""
import copy
import hashlib
import json
import time

from .delivery import DeliveryWorkflow,DeliveryPublisher
from .handoff_store import canonical


def legacy_row(c,identity):
    row=c.execute('SELECT * FROM legacy_targets WHERE manifest_sha256=? AND specialty_code=?',
        (identity['manifest_sha256'],identity['specialty_code'])).fetchone()
    if not row:raise ValueError('legacy reconciliation target missing')
    return dict(row)


def subject_sql(ledger,c,actor,digest):
    """Qualify the current actor separately from immutable historical provenance."""
    ledger.check_identity(c,actor,allow_frozen=True,allow_legacy=True)
    package=c.execute('SELECT * FROM delivery_packages WHERE manifest_sha256=?',(digest,)).fetchone()
    if not package or package['lifecycle']!='retained':raise ValueError('retained legacy package required')
    identity=json.loads(package['identity_json']);code=actor['specialty_code']
    if identity['source_id']!=actor['source_id']:raise ValueError('legacy object belongs to another source')
    row=legacy_row(c,dict(manifest_sha256=digest,specialty_code=code))
    target=c.execute('SELECT mapping_version FROM specialty_tasks WHERE manifest_sha256=? AND specialty_code=?',(digest,code)).fetchone()
    origin=c.execute('SELECT i.data FROM handoff_results h JOIN publication_intents i '
        'ON i.thread_id=h.thread_id AND i.input_fingerprint=h.input_fingerprint WHERE h.manifest_sha256=? LIMIT 1',(digest,)).fetchone()
    if not target or not origin:raise ValueError('legacy registration provenance missing')
    # This is the generation granted by the original registration, not permission
    # to resume it. All action and commit admission continues to use actor.
    generation=json.loads(origin[0])['expected_targets'][code]['generation']+1
    identity.update(manifest_sha256=digest,specialty_code=code,generation=generation,mapping_version=target['mapping_version'])
    return dict(row,identity=identity)


def audit_active_version(config,ledger,actions,client,identity):
    """Use the current owner to inspect an older active version; never replay it."""
    active=ledger.active(identity['source_id'],identity['specialty_code'])
    if not active or active.get('state')=='frozen_pending':raise ValueError('no healthy active version to audit')
    digest=active['identity']['manifest_sha256'];receipts=copy.deepcopy(active['receipts'])
    flow=DeliveryWorkflow(config,ledger,actions,client,identity);publisher=DeliveryPublisher(client)
    with client.version(digest,recovery=True):
        task,packet=flow.packet(digest)
        publisher.verify(task,packet,receipts);publisher.states(receipts,True);publisher.verify_retrieval(task,packet,receipts)
    if ledger.active(identity['source_id'],identity['specialty_code'])!=active:raise ValueError('active changed while reading')
    return dict(state='active_readback',active_identity=active['identity'],expected_identity=identity,
        content_images_availability_retrieval=True,remote_writes=0)


def readback(config,ledger,actions,client,identity,*,source_manifest=None):
    code=identity['specialty_code']
    digest=source_manifest or identity['manifest_sha256']
    with ledger.store.db() as c:row=subject_sql(ledger,c,identity,digest)
    receipts={code:json.loads(row['receipt'])}
    publisher=DeliveryPublisher(client)
    flow=DeliveryWorkflow(config,ledger,actions,client,identity)
    evidence=dict(snapshot_id=row['snapshot_id'],endpoint=client.base_url,observed_at=time.time(),checks=[],objects=[])
    try:
        with client.version(digest,recovery=True):
            task,packet=flow.packet(digest)
            if receipts[code].get('legacy_images'):raise ValueError('separate legacy image documents need historical adapter')
            publisher.verify(task,packet,receipts);evidence['checks'].append('exact_content_metadata_images')
            states=publisher.states(receipts,True);evidence['checks'].append('document_and_chunks_enabled')
            publisher.verify_retrieval(task,packet,receipts);evidence['checks'].append('version_retrieval')
        for obj in states:
            evidence['objects'].append(dict(dataset_id=obj['dataset_id'],document_id=obj['document']['id'],
                status=obj['document']['status'],chunks=[dict(id=ch['id'],available=ch['available'],
                    content_sha256=hashlib.sha256(ch['content'].encode()).hexdigest()) for ch in obj['chunks']]))
        evidence['outcome']='verified_available'
    except Exception as exc:
        evidence.update(outcome='pending_reconcile',error=type(exc).__name__,message=str(exc)[:250])
    return actions.commit('legacy_readback',dict(source_manifest=digest,evidence=evidence,receipts=receipts))


def commit_readback(ledger,c,payload,*,epoch):
    if set(payload) not in ({'identity','evidence','receipts'},{'identity','source_manifest','evidence','receipts'}):raise ValueError('invalid legacy readback fields')
    actor=payload['identity'];code=actor['specialty_code'];digest=payload.get('source_manifest',actor['manifest_sha256'])
    row=subject_sql(ledger,c,actor,digest);identity=row['identity'];evidence=payload['evidence'];receipts=payload['receipts']
    if evidence.get('snapshot_id')!=row['snapshot_id'] or set(receipts)!={code}:raise ValueError('legacy readback provenance mismatch')
    original=json.loads(row['receipt'])
    def strip_flags(v):return {k:x for k,x in v.items() if k not in ('verified','retrieval_verified')}
    if strip_flags(receipts[code])!=strip_flags(original):raise ValueError('legacy readback changed document ownership')
    prior=c.execute('SELECT data FROM specialty_active WHERE source_id=? AND specialty_code=?',(identity['source_id'],code)).fetchone()
    prior=json.loads(prior[0]) if prior else None
    retired=c.execute('SELECT 1 FROM specialty_retirements WHERE source_id=? AND specialty_code=? AND manifest_sha256=?',
        (identity['source_id'],code,digest)).fetchone()
    safety=ledger.safety_sql(c,identity['source_id'],code,digest)
    cannot_activate=(ledger.frozen_sql(c,identity) or retired or (safety and safety['state']=='disabled')
                     or (prior and prior['identity']['manifest_sha256']!=digest))
    # A later diagnostic read of an already retired/closed version does not
    # invent a new unknown write or change the current active pointer.
    state=(row['state'] if row['state'] in ('active','reconciled') and cannot_activate else 'pending_reconcile')
    if evidence.get('outcome')=='verified_available':
        if evidence.get('checks')!=['exact_content_metadata_images','document_and_chunks_enabled','version_retrieval']:
            raise ValueError('incomplete legacy readback')
        expected={(obj['dataset_id'],obj['document_id']) for role,obj in original.items() if role in ('body','images')}
        objects=evidence.get('objects',[])
        if not expected or len(objects)!=len(expected) or {(x['dataset_id'],x['document_id']) for x in objects}!=expected:
            raise ValueError('legacy document set mismatch')
        for obj in objects:
            receipt=next(v for k,v in original.items() if k in ('body','images') and v['document_id']==obj['document_id'] and v['dataset_id']==obj['dataset_id'])
            expected_chunks={v['chunk_id']:v['content_sha256'] for v in receipt['chunks'].values()}
            if (str(obj['status'])!='1' or not obj['chunks'] or any(ch['available'] is not True for ch in obj['chunks'])
                    or len(obj['chunks'])!=len(expected_chunks) or {ch['id']:ch['content_sha256'] for ch in obj['chunks']}!=expected_chunks):
                raise ValueError('legacy content/availability proof mismatch')
        if receipts[code].get('verified') is not True or receipts[code].get('retrieval_verified') is not True:raise ValueError('legacy read flags missing')
        run=c.execute('SELECT original FROM legacy_runs WHERE snapshot_id=? AND task_id=?',(row['snapshot_id'],identity['task_id'])).fetchone()
        # No end-to-end B has been verified. An interrupted legacy writer may
        # still finish remotely, even after its local process tree has exited.
        known_closed=run and json.loads(run[0])['task']['stage']=='complete'
        state=('reconciled' if cannot_activate else 'active') if known_closed else 'observed_pending'
        if state=='active':
            if prior:identity=prior['identity']
            result=dict(identity=identity,receipts=receipts,activated_at=time.time(),switch_id='legacy:'+row['snapshot_id'],historical=True)
            c.execute('INSERT INTO specialty_active VALUES(?,?,?,?) ON CONFLICT(source_id,specialty_code) DO UPDATE SET data=excluded.data',
                (identity['source_id'],code,digest,canonical(result)))
            current=ledger.progress_sql(c,digest,code)
            c.execute('INSERT INTO specialty_progress VALUES(?,?,?,?,?,?,?) ON CONFLICT(manifest_sha256,specialty_code) '
                'DO UPDATE SET seq=excluded.seq,state=excluded.state,receipts_json=excluded.receipts_json,updated=excluded.updated',
                (digest,code,(current['seq'] if current else 0)+1,'active',canonical(identity),canonical(receipts),time.time()))
            ledger._reference(c,digest,'active_specialty',canonical([identity['source_id'],code]),'package',digest)
    elif evidence.get('outcome')!='pending_reconcile':raise ValueError('unknown legacy readback outcome')
    evidence=dict(evidence,manager_epoch=epoch,verified_by=actor,source_manifest=digest)
    c.execute('UPDATE legacy_targets SET state=?,evidence=? WHERE manifest_sha256=? AND specialty_code=?',(state,canonical(evidence),digest,code))
    return dict(state=state,evidence=evidence,remote_writes=0)


def article_status(store,task_id,cleanup_status):
    """Availability, desired completion and closeout are separate facts (§10)."""
    task=store.get(task_id)
    if not task:raise ValueError('unknown article')
    with store.db() as c:
        c.execute('BEGIN')
        rows=c.execute('SELECT v.*,t.status,t.mapping_issue FROM target_versions v JOIN specialty_tasks t '
            'ON t.manifest_sha256=v.manifest_sha256 AND t.specialty_code=v.specialty_code WHERE v.task_id=?',(task_id,)).fetchall()
        targets=[]
        for row in rows:
            code=row['specialty_code']
            saved=c.execute('SELECT data FROM specialty_active WHERE source_id=? AND specialty_code=?',(task['source_id'],code)).fetchone()
            active=json.loads(saved[0]) if saved else None
            legacy=c.execute('SELECT state FROM legacy_targets WHERE manifest_sha256=? AND specialty_code=?',(row['manifest_sha256'],code)).fetchone()
            frozen=c.execute("SELECT 1 FROM package_freezes WHERE manifest_sha256=? AND specialty_code IN ('*',?)",(row['manifest_sha256'],code)).fetchone()
            active_digest=active['identity']['manifest_sha256'] if active else None
            active_frozen=c.execute("SELECT 1 FROM package_freezes WHERE manifest_sha256=? AND specialty_code IN ('*',?)",(active_digest,code)).fetchone()
            active_legacy=c.execute('SELECT state FROM legacy_targets WHERE manifest_sha256=? AND specialty_code=?',(active_digest,code)).fetchone()
            pending_scope=c.execute('SELECT 1 FROM legacy_pending_scopes WHERE source_id=? AND specialty_code=?',(task['source_id'],code)).fetchone()
            switching=c.execute('SELECT phase FROM specialty_switches WHERE source_id=? AND specialty_code=?',(task['source_id'],code)).fetchone()
            # After retrieval the next remote action can disable the old version
            # before old_disabled is committed. Count that window conservatively.
            switching_unconfirmed=switching and switching['phase'] not in ('intent','enabled','committed','withdrawn')
            available=bool(active and active.get('state') in (None,'active') and not active_frozen
                and not switching_unconfirmed and not pending_scope and (not active_legacy or active_legacy[0]=='active'))
            complete=available and not frozen and not row['mapping_issue'] and active_digest==row['manifest_sha256']
            targets.append(dict(code=code,available=available,expected_complete=complete,
                expected_manifest=row['manifest_sha256'],active_manifest=active['identity']['manifest_sha256'] if active else None,
                pending_reconcile=bool(pending_scope or (legacy and legacy[0] not in ('active','reconciled'))),
                held=bool(row['mapping_issue'] or row['status']=='frozen')))
    excluded=task.get('disposition')=='exclude'
    return dict(task_id=task_id,processing=task['stage'],excluded=excluded,targets=targets,
        target_count=len(targets),available=sum(x['available'] for x in targets),expected_complete=sum(x['expected_complete'] for x in targets),
        pending_reconcile=sum(x['pending_reconcile'] for x in targets),cleanup=cleanup_status,
        complete=bool(not excluded and targets and all(x['expected_complete'] for x in targets) and not cleanup_status.get('closeout_pending',0)))
