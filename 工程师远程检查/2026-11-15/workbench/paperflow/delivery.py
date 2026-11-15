"""One article x specialty attempt, ending at its own active-version commit."""
import copy
import hashlib
import json
import time
import uuid
from pathlib import Path

from .package import load,validate_package
from .rag import RagPublisher,RemoteConflict
from .handoff_store import canonical
from .scheduling_policy import ActionDeferred


class DeliveryPublisher(RagPublisher):
    def states(self,receipts,enabled=None):
        rows=[]
        for target in receipts.values():
            for role in ('body','images'):
                obj=target.get(role)
                if not obj:continue
                ds,did=obj['dataset_id'],obj['document_id']
                docs=self.documents(ds,id=did)
                if len(docs)!=1 or any((docs[0].get('meta_fields') or {}).get(k)!=v for k,v in obj['identity'].items()):
                    raise RemoteConflict('switch document identity conflict')
                chunks=self.chunks(ds,did)
                if enabled is not None and (str(docs[0].get('status'))!=str(int(enabled)) or
                    any(ch.get('available') is not bool(enabled) for ch in chunks)):
                    raise RemoteConflict('document and chunk availability not converged')
                rows.append(dict(dataset_id=ds,document=docs[0],chunks=chunks))
        return rows

    def set_enabled(self,receipts,enabled):
        # Verify ownership before writing, then confirm both SQL status and index flags.
        states=self.states(receipts)
        for item in states:
            ds,doc=item['dataset_id'],item['document'];did=doc['id']
            if str(doc.get('status'))!=str(int(enabled)):
                self.client.request('PATCH',f'/datasets/{ds}/documents/{did}',dict(enabled=int(enabled)))
            elif any(ch.get('available') is not bool(enabled) for ch in item['chunks']):
                # Recover a status update interrupted after its SQL change.
                for ch in item['chunks']:
                    if ch.get('available') is not bool(enabled):
                        self.client.request('PATCH',f'/datasets/{ds}/documents/{did}/chunks/{ch["id"]}',dict(available=bool(enabled)))
        for attempt in range(10):
            try:return self.states(receipts,enabled)
            except RemoteConflict:
                if attempt==9:raise
                time.sleep(.2)


class DeliveryWorkflow:
    def __init__(self,config,ledger,actions,client,identity):
        self.config,self.ledger,self.actions,self.client,self.identity=config,ledger,actions,client,identity
        self.code=identity['specialty_code'];self.digest=identity['manifest_sha256']
        self.fault=lambda point:None
        self.seq=0;self.receipts={};self.publisher=DeliveryPublisher(client,self.save_receipts)
        if config.native_pipeline_id:
            from .native_delivery import NativeDeliveryPublisher
            self.publisher=NativeDeliveryPublisher(client,self.save_receipts,config)
        self.deployment_fingerprint='unverified'

    def report_problem(self,stage,problem_type,evidence,*,source_manifest=None):
        """A definite content diagnosis, never a catch-all for network/readback errors.

        Reports persist a request only. The manager starts its separate graph run;
        this specialty worker must not call a model or hold graph ownership.
        source_manifest refers to retained evidence from an older package. It
        does not replace this worker's current, independently qualified identity.
        """
        payload=dict(stage=stage,problem_type=problem_type,evidence=evidence)
        if source_manifest is not None:payload['source_manifest']=source_manifest
        return self.actions.commit('late_repair_report' if source_manifest is not None else 'repair_report',payload)

    def packet(self,digest):
        row=self.ledger.registration(digest)
        if not row or row['lifecycle']!='retained':raise ValueError('retained sealed package required')
        identity=json.loads(row['identity_json'])
        targets=json.loads(row['targets_json'])['targets']
        catalog=dict(majors=[target['mapping'] for target in targets if not target['issue']])
        packet=validate_package(row['package_dir'],identity,catalog,strict_seal=True)
        if packet['manifest_sha256']!=digest or (Path(packet['root'])/'manifest.json').read_bytes()!=row['manifest_bytes']:
            raise ValueError('registered package bytes changed')
        packet['routes']=[r for r in packet['routes'] if r['code']==self.code]
        if len(packet['routes'])!=1:raise ValueError('one registered specialty required')
        task=dict(identity,revision=int(row['revision'].removeprefix('r')))
        return task,packet

    def save_receipts(self,receipts):
        self.receipts=copy.deepcopy(receipts)
        self.save('preparing')

    def save(self,state):
        result=self.actions.commit('save',dict(state=state,receipts=self.receipts,expected_seq=self.seq))
        self.seq=result['seq'];return result

    def check_deployment(self):
        if not self.config.delivery_acceptance_report:raise ValueError('candidate visibility acceptance required')
        report=load(self.config.delivery_acceptance_report)
        if report.get('passed') is not True or report.get('endpoint')!=self.client.base_url:raise ValueError('deployment acceptance endpoint mismatch')
        if self.config.native_pipeline_id and report.get('native_pipeline_id')!=self.config.native_pipeline_id:
            raise ValueError('native pipeline visibility acceptance mismatch')
        observed=self.client.deployment_identity()
        if observed!=report['identity']:raise ValueError('deployment configuration/fingerprint changed')
        self.deployment_fingerprint=hashlib.sha256(canonical(observed).encode()).hexdigest()
        for role in ('a_body','a_images'):
            probe=report['probes'][role]
            if not probe['passed'] or probe['index']['available_int']!=0 or not probe['index']['vectors']:
                raise ValueError('complete creation isolation evidence required')
            if probe['query'].get('document_ids') or len(probe['disabled_reads'])<2:raise ValueError('ordinary retrieval evidence required')
        ctx=self.ledger.context(self.digest,self.code)
        if any(ds not in observed['dataset_ids'] for ds in [ctx['route']['paper_dataset_id'],ctx['route'].get('image_dataset_id')] if ds):
            raise ValueError('target outside accepted isolated datasets')

    def inventory(self):
        manifests={self.digest}
        for row in self.ledger.unresolved(self.identity['source_id'],self.code):
            resource=json.loads(row['request'])['resource']
            if resource.get('object_manifest'):manifests.add(resource['object_manifest'])
        switching=self.ledger.switching(self.identity['source_id'],self.code)
        if switching:
            manifests.add(switching['candidate'])
            if switching['previous']:manifests.add(switching['previous']['identity']['manifest_sha256'])
        active=self.ledger.active(self.identity['source_id'],self.code)
        if active:manifests.add(active['identity']['manifest_sha256'])
        result=[];reader=RagPublisher(self.client)
        for digest in sorted(manifests):
            task,packet=self.packet(digest);route=packet['routes'][0]
            with self.client.version(digest,recovery=True):
                for role,records in reader.records(packet).items():
                    if not records:continue
                    ds=route['paper_dataset_id' if role=='body' else 'image_dataset_id']
                    name=f'PF_{task["source_sha256"][:16]}_{digest[:20]}_{self.code}_{role}.md'
                    docs=[d for d in reader.documents(ds) if d['name']==name]
                    if len(docs)>1:raise RemoteConflict('ambiguous document during reconciliation')
                    items=[]
                    for doc in docs:
                        identity=dict(paperflow_source_sha256=task['source_sha256'],paperflow_manifest_sha256=digest,
                            paperflow_revision=str(task['revision']),paperflow_specialty=self.code,paperflow_role=role,paperflow_task=task['task_id'])
                        meta=doc.get('meta_fields') or {}
                        if meta:
                            if any(meta.get(k)!=v for k,v in identity.items()):raise RemoteConflict('reconciliation identity conflict')
                        else:
                            from .rag import compact
                            if reader._download(ds,doc['id'])!=self.publisher.document_bytes(task,packet,role,identity):raise RemoteConflict('unbound upload identity conflict')
                        chunks=sorted(reader.chunks(ds,doc['id']),key=lambda r:r['id'])
                        images={ch['image_id']:hashlib.sha256(reader.download_image(ch['image_id'])).hexdigest() for ch in chunks if ch.get('image_id')}
                        items.append(dict(id=doc['id'],name=doc['name'],status=doc.get('status'),meta_fields=meta,chunks=chunks,images=images))
                    result.append(dict(manifest_sha256=digest,role=role,dataset_id=ds,documents=items))
        return result

    def reconcile(self,unresolved):
        # Persist each whole read under a fresh local permit. Never sleep while holding SQL or a permit.
        last={}
        for attempt in range(3):
            marker=self.actions.commit('read_start',{})
            inventory=self.inventory()
            evidence=dict(complete_inventory=True,inventory=inventory,action_ids=sorted(r['action_id'] for r in unresolved),
                endpoint=self.client.base_url,configuration=self.deployment_fingerprint)
            last=self.actions.commit('reconcile',dict(evidence=evidence,read_id=marker['read_id']))
            if last.get('released'):return True
            if attempt==0:continue
            delay=last.get('wait_seconds',0)
            if not 0<delay<=2:break  # Longer verified windows are due-time retries, not a sleeping worker.
            time.sleep(delay)
        return False

    def withdraw(self,switching):
        if not switching or switching['phase'] in ('committed','withdrawn'):return
        if self.ledger.unresolved(self.identity['source_id'],self.code):return
        self.actions.commit('withdraw',dict(switch_id=switching['switch_id'],phase='withdraw_pending'))
        verifier=DeliveryPublisher(self.client)
        # A rejected candidate must stop being visible even when the old version is broken.
        progress=self.ledger.progress(switching['candidate'],self.code)
        if progress:
            with self.client.version(switching['candidate'],recovery=True):verifier.set_enabled(progress['receipts'],False)
        old=switching['previous']
        if old and not self.ledger.frozen(old['identity']):
            with self.client.version(old['identity']['manifest_sha256'],recovery=True):
                task,packet=self.packet(old['identity']['manifest_sha256'])
                verifier.verify(task,packet,copy.deepcopy(old['receipts']))
                verifier.set_enabled(old['receipts'],True)
                verifier.verify_retrieval(task,packet,copy.deepcopy(old['receipts']))
        self.actions.commit('withdraw',dict(switch_id=switching['switch_id']))

    def plan_frozen_active(self):
        active=self.ledger.active(self.identity['source_id'],self.code)
        if active and self.ledger.frozen(active['identity']):
            self.actions.commit('safety',dict(phase='intent',object_manifest=active['identity']['manifest_sha256']))
        return self.ledger.pending_safety(self.identity['source_id'],self.code)

    def disable_frozen(self,plan):
        digest=plan['manifest_sha256']
        try:
            with self.client.version(digest,recovery=True):
                # Ownership and availability matter here; bad content must not prevent disabling it.
                DeliveryPublisher(self.client).set_enabled(plan['data']['receipts'],False)
        except Exception as exc:
            if isinstance(exc,ActionDeferred):raise
            try:self.actions.commit('safety',dict(phase='pending',object_manifest=digest,error=type(exc).__name__+': '+str(exc)))
            except ValueError:pass  # Pause revoked permission; the durable pending intent already exists.
            return dict(state='frozen_pending',manifest_sha256=digest,error=type(exc).__name__)
        self.fault('safety_disabled')
        self.actions.commit('safety',dict(phase='disabled',object_manifest=digest))
        return dict(state='frozen',manifest_sha256=digest,safety='disabled')

    def run_safety(self):
        """Historical versions can close an existing plan, never resume delivery."""
        self.check_deployment()
        plans=self.plan_frozen_active()
        unresolved=self.ledger.unresolved(self.identity['source_id'],self.code)
        if unresolved:return dict(state='frozen_pending',unresolved=[r['action_id'] for r in unresolved])
        for plan in plans:
            result=self.disable_frozen(plan)
            if result['state']=='frozen_pending':return result
        return dict(state='frozen')

    def run(self):
        with self.ledger.store.db() as c:
            legacy=c.execute('SELECT 1 FROM legacy_targets WHERE manifest_sha256=? AND specialty_code=?',
                             (self.digest,self.code)).fetchone()
            if legacy and not self.ledger.legacy_safety_allowed_sql(c,self.identity):
                raise ValueError('legacy target requires readback or recorded frozen safety plan')
        if legacy:return self.run_safety()
        self.check_deployment()
        plans=self.plan_frozen_active()
        progress=self.ledger.progress(self.digest,self.code)
        if progress:self.seq,self.receipts=progress['seq'],progress['receipts']
        unresolved=self.ledger.unresolved(self.identity['source_id'],self.code)
        if unresolved:
            # A matching read alone never proves an earlier request has ended.
            if not self.reconcile(unresolved):
                return dict(state='frozen_pending' if plans else 'blocked',unresolved=[r['action_id'] for r in unresolved])
        for plan in plans:
            result=self.disable_frozen(plan)
            if result['state']=='frozen_pending':return result
        task,packet=self.actions.run('local',dict(operation='load'),lambda cancel:self.packet(self.digest))
        switching=self.ledger.switching(self.identity['source_id'],self.code)
        frozen=self.ledger.frozen(self.identity)
        if switching and switching['phase'] not in ('committed','withdrawn') and (
            switching['candidate']!=self.digest or frozen or switching['phase']=='withdraw_pending'):
            self.withdraw(switching)
            switching=self.ledger.switching(self.identity['source_id'],self.code)
        if frozen:return dict(state='frozen')
        if switching and switching['phase']=='withdrawn' and switching['candidate']==self.digest:return dict(state='withdrawn')
        active=self.ledger.active(self.identity['source_id'],self.code)
        # Use a verifier without callbacks: checking an active version is read-only.
        verifier=DeliveryPublisher(self.client)
        if active and active['identity']==self.identity:
            verifier.verify(task,packet,active['receipts']);verifier.states(active['receipts'],True)
            verifier.verify_retrieval(task,packet,active['receipts'])
            # Freeze can arrive during read-only verification, whose permits allow frozen reads.
            plans=self.plan_frozen_active()
            if plans:return self.disable_frozen(plans[0])
            return dict(state='active',already_active=True)
        if switching and switching['phase'] not in ('committed','withdrawn'):
            # Re-read all sealed content before resuming any previously admitted switch.
            try:verifier.verify(task,packet,self.receipts)
            except RemoteConflict:
                # Only a confirmed mismatch rejects a candidate. Admission
                # contention and read outages retain this durable switch phase.
                self.withdraw(switching)
                raise
        else:
            self.publisher.publish(task,packet,self.receipts)
            self.publisher.verify(task,packet,self.receipts)
            verifier.states(self.receipts,False)
            self.save('verified')
            result=self.actions.commit('switch',dict(phase='intent',switch_id=uuid.uuid4().hex))
            switching=self.ledger.switching(self.identity['source_id'],self.code)
            self.fault('switch_intent')
        switch_id=switching['switch_id']
        verifier.set_enabled(self.receipts,True)
        self.actions.commit('switch',dict(phase='enabled',switch_id=switch_id));self.fault('enabled')
        try:verifier.verify_retrieval(task,packet,self.receipts)
        except RemoteConflict:
            self.withdraw(switching)
            raise
        self.save('switching')
        self.actions.commit('switch',dict(phase='retrieved',switch_id=switch_id));self.fault('retrieved')
        old=switching['previous']
        if old:
            with self.client.version(old['identity']['manifest_sha256']):verifier.set_enabled(old['receipts'],False)
        self.actions.commit('switch',dict(phase='old_disabled',switch_id=switch_id));self.fault('old_disabled')
        self.actions.commit('switch',dict(phase='committed',switch_id=switch_id))
        return dict(state='active',already_active=False)
