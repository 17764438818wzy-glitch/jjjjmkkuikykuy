"""Durable handoff breakpoints; all files stay under the candidate lab."""
import asyncio
from dataclasses import replace
import os
from pathlib import Path
import tempfile
import time
import unittest
import subprocess
import sys
import uuid
from unittest.mock import patch

import psutil
from paperflow_prefect.pipeline_contracts import StageEnvelope, StageResult, STAGES
from paperflow_prefect.pipeline_dispatch import DispatchCoordinator, SubmissionUncertain
from paperflow_prefect.pipeline_ledger import StageLedger, StaleDispatch, OutcomeUnknown
from paperflow_prefect.pipeline_runtime import artifact
from paperflow_prefect.records import read_json, write_json
from test_receipt_h21 import handoff
from paperflow_prefect.receipts import ReceiptStore
from paperflow_prefect.intake_handoff import receive_handoff

ROOT = Path(__file__).resolve().parents[1]
LAB = ROOT/'runtime/development/pipeline-v2/handoff-unit'


def owner():
    return dict(instance_id='handoff-test', pid=os.getpid(),
                process_create_time=psutil.Process().create_time(), task_run_id='test')


class Gateway:
    """Only submission protocol is simulated; ledger is the real durable implementation."""
    def __init__(self):
        self.runs = []; self.submissions = []; self.fail_query = False
        self.accept_then_crash = False; self.idle = True; self.in_flight = False

    def task_key(self, env): return env.stage.value

    def find(self, env):
        if self.fail_query: raise ConnectionError('query unavailable')
        return [r for r in self.runs if r['operation_key']==env.operation_key and r['dispatch_seq']==env.dispatch_seq]

    def submit(self, env):
        identifier = str(len(self.submissions)+1)
        self.submissions.append(env)
        self.runs.append(dict(id=identifier, operation_key=env.operation_key, dispatch_seq=env.dispatch_seq,
                              state='SCHEDULED', created=time.time(), task_key=env.stage.value))
        if self.accept_then_crash: raise ConnectionError('accepted; response lost')
        return identifier

    def availability(self, env, run_ids):
        return dict(idle=self.idle, owned=self.in_flight, certain=True, workers=[])


class HandoffTests(unittest.TestCase):
    def setUp(self):
        LAB.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=LAB)
        self.addCleanup(self.temp.cleanup); self.root=Path(self.temp.name)
        library=self.root/'library'; library.mkdir()
        write_json(self.root/'handoff.json', handoff(library, library/'person', attachments=[]))
        receipt=receive_handoff(self.root/'handoff.json', ReceiptStore(self.root/'receipts'))
        self.paper=receipt['inventory']; self.batch='batch-test'
        self.scope=self.root/'scope.json'
        write_json(self.scope,dict(schema='pipeline-scope/1',batch_id=self.batch,
            control_generation=dict(global_=0), papers=[dict(receipt_ref=artifact(receipt['record_path']).to_dict())],
            contracts={s:'test-v1' for s in STAGES}))
        value=read_json(self.scope); value['control_generation']={'global':0,'batch':0,'runtime':0}; write_json(self.scope,value)
        self.gateway=Gateway(); self.ledger=StageLedger(self.root/'ledger')
        self.coordinator=DispatchCoordinator(self.ledger, self.gateway, instance_id='handoff-test')
        self.coordinator.register(self.scope, 'request-1')

    def reconcile(self): return asyncio.run(self.coordinator.reconcile_scope(self.batch))

    def operation(self, stage):
        return next(env for env in self.coordinator.envelopes(self.batch) if env.stage.value==stage)

    def complete(self, env, state='complete'):
        path=self.root/'outputs'/env.operation_key/('material.json' if env.stage.value=='prepare' else 'evidence.json')
        write_json(path,dict(operation_key=env.operation_key, reason='empty_inventory' if state=='skipped' else 'fixture'))
        result=StageResult(env.operation_key,state,(artifact(path),),())
        with self.ledger.claim(env,owner()): self.ledger.commit_result(env,result)
        return result

    def test_crash_after_result_before_successor_intent(self):
        self.reconcile(); intake=self.operation('intake'); saved=self.complete(intake)
        # A completely fresh coordinator reconstructs successors from durable refs.
        self.coordinator=DispatchCoordinator(StageLedger(self.root/'ledger'),self.gateway,instance_id='handoff-test')
        self.reconcile(); prepare=self.operation('prepare')
        self.assertNotIn(saved.evidence_refs[0], prepare.input_refs)  # Intake is not a body input.
        self.assertEqual([e.stage.value for e in self.gateway.submissions].count('prepare'),1)
        self.reconcile()
        self.assertEqual([e.stage.value for e in self.gateway.submissions].count('prepare'),1)

    def test_crash_after_prefect_accept_before_local_id(self):
        self.gateway.accept_then_crash=True
        self.reconcile()
        self.assertEqual(len(self.gateway.submissions),1)
        self.gateway.accept_then_crash=False; self.reconcile()
        env=self.operation('intake'); intent=self.coordinator.intent(env)
        self.assertEqual(intent['task_run_ids'],['1'])
        self.assertEqual(len(self.gateway.submissions),1)

    def test_server_restart_lost_queue_fences_old_delivery(self):
        self.reconcile(); old=self.operation('intake')
        self.gateway.runs[0]['created']-=121
        # SQL still Scheduled but queue lost. Native test below proves real consumption.
        self.reconcile(); fresh=self.operation('intake')
        self.assertEqual(fresh.dispatch_seq,old.dispatch_seq+1)
        with self.assertRaises(StaleDispatch):
            with self.ledger.claim(old,owner()): self.fail('old task executed')
        self.complete(fresh)
        self.assertEqual(len([e for e in self.gateway.submissions if e.stage.value=='intake']),2)

    def test_dead_pending_running_owner_recovers_without_repeat_side_effect(self):
        self.reconcile(); old=self.operation('intake')
        record=self.ledger.read_operation(old.operation_key)
        record.update(state='running',owner=dict(owner(),pid=2147483647))
        write_json(self.ledger.root/'operations'/(old.operation_key+'.json'),record)
        self.gateway.runs[0]['state']='RUNNING'
        self.gateway.runs[0]['created']-=121
        self.reconcile(); new=self.operation('intake')
        self.assertEqual(new.dispatch_seq,1)
        self.complete(new)
        with self.ledger.claim(new,owner()) as claim: self.assertFalse(claim['execute'])

    def test_query_failure_busy_or_missing_worker_never_requeues(self):
        self.reconcile(); old=self.operation('intake'); self.gateway.runs[0]['created']-=121
        self.gateway.fail_query=True; self.reconcile()
        self.assertEqual(len(self.gateway.submissions),1)
        self.gateway.fail_query=False; self.gateway.idle=False; self.reconcile()
        self.assertEqual(len(self.gateway.submissions),1)
        self.gateway.idle=True; self.gateway.in_flight=True; self.reconcile()
        self.assertEqual(self.operation('intake').dispatch_seq,0)

    def test_lost_submit_and_empty_query_stays_uncertain(self):
        self.gateway.accept_then_crash=True; self.reconcile()
        self.gateway.runs.clear(); self.gateway.accept_then_crash=False
        self.reconcile(); self.reconcile()
        self.assertEqual(len(self.gateway.submissions),1)
        self.assertEqual(self.coordinator.intent(self.operation('intake'))['phase'],'sending')

    def test_live_owner_or_owned_child_cannot_be_replaced(self):
        self.reconcile(); env=self.operation('intake'); self.gateway.runs[0]['created']-=121
        with self.ledger.claim(env,owner()):
            self.reconcile(); self.assertEqual(self.operation('intake').dispatch_seq,0)
        write_json(self.ledger.root/'processes'/env.operation_key/'one/process.json',
            dict(schema='pipeline-stage-process/1',processes=[owner()],status='running'))
        self.reconcile(); self.assertEqual(self.operation('intake').dispatch_seq,0)
        self.assertEqual(len(self.gateway.submissions),1)

    def test_sent_orphan_checks_local_then_remote_and_never_resends(self):
        self.reconcile(); env=self.operation('intake')
        with self.ledger.claim(env,owner()):
            self.ledger.reserve_send(env.paper_id,None,None,'a'*64,False)
        calls=[]
        self.coordinator.local_outcome=lambda *args:calls.append('local')
        self.coordinator.remote_outcome=lambda *args:calls.append('remote')
        self.reconcile()
        self.assertEqual(calls,['local','remote'])
        self.assertEqual(self.ledger.read_operation(env.operation_key)['state'],'unknown')
        self.assertEqual(len(self.gateway.submissions),1)
        with self.assertRaises(OutcomeUnknown): self.ledger.advance_unsent_dispatch(env)

    def test_scope_cannot_supply_success_claims(self):
        value=read_json(self.scope); value['papers'][0]['stages']={'profile':{'state':'passed'}}
        other=self.root/'forged.json'; write_json(other,value)
        with self.assertRaises(ValueError): self.coordinator.register(other,'forged')

    def test_only_registered_scope_is_examined(self):
        write_json(self.root/'unregistered/scope.json',dict(schema='pipeline-scope/1',batch_id='foreign'))
        self.reconcile()
        self.assertEqual(len(self.gateway.submissions),1)
        with self.assertRaises(FileNotFoundError): self.coordinator.reconcile_now('foreign')

    def test_dynamic_profile_uses_material_only_and_cross_batch_added_attachment_reuses_body(self):
        self.reconcile(); self.complete(self.operation('intake')); self.reconcile()
        prepared=self.complete(self.operation('prepare')); self.reconcile()
        profile=self.operation('profile')
        self.assertEqual(profile.input_refs,prepared.evidence_refs)
        self.complete(profile,'passed')
        old_profile_calls=sum(e.stage.value=='profile' for e in self.gateway.submissions)
        library=self.root/'library'
        write_json(self.root/'handoff2.json',handoff(library,library/'person',attachments=[('附件/new.txt',b'new')]))
        receipt=receive_handoff(self.root/'handoff2.json',ReceiptStore(self.root/'receipts'))
        self.assertEqual(receipt['inventory']['paper_id'],self.paper['paper_id'])
        value=read_json(self.scope); value.update(batch_id='second-batch',papers=[dict(receipt_ref=artifact(receipt['record_path']).to_dict())])
        path=self.root/'scope2.json'; write_json(path,value); self.coordinator.register(path,'request-2')
        for _ in range(4): self.coordinator.reconcile_now('second-batch')
        reused=next(e for e in self.coordinator.envelopes('second-batch') if e.stage.value=='profile')
        self.assertEqual(profile.operation_key,reused.operation_key)
        self.assertEqual(old_profile_calls,sum(e.stage.value=='profile' for e in self.gateway.submissions))

    def test_changed_predecessor_evidence_blocks_successor(self):
        self.reconcile(); self.complete(self.operation('intake')); self.reconcile()
        prepared=self.complete(self.operation('prepare')); Path(prepared.evidence_refs[0].path).write_text('changed')
        report=self.reconcile()
        self.assertTrue(report['errors']); self.assertFalse(any(e.stage.value=='profile' for e in self.gateway.submissions))

    def test_crash_after_sequence_fence_before_scope_pointer_write(self):
        self.reconcile(); old=self.operation('intake')
        fresh=self.ledger.advance_unsent_dispatch(old)
        self.assertEqual(self.operation('intake').dispatch_seq,0)
        self.reconcile()
        self.assertEqual(self.operation('intake'),fresh)
        self.assertEqual(len(self.gateway.submissions),2)

    def test_active_attachment_does_not_block_body_and_item_rag_predicate_is_exact(self):
        library=self.root/'library'
        write_json(self.root/'items-handoff.json',handoff(library,library/'person',
            attachments=[('附件/a.txt',b'a'),('附件/b.txt',b'b')]))
        receipt=receive_handoff(self.root/'items-handoff.json',ReceiptStore(self.root/'receipts'))
        value=read_json(self.scope); value.update(batch_id='items',papers=[dict(receipt_ref=artifact(receipt['record_path']).to_dict())])
        path=self.root/'items-scope.json'; write_json(path,value); self.coordinator.register(path,'items-request')
        self.batch='items'; self.reconcile(); self.complete(self.operation('intake')); self.reconcile()
        attachments=[e for e in self.coordinator.envelopes(self.batch) if e.stage.value=='attachment']
        self.complete(self.operation('prepare'))
        with self.ledger.claim(attachments[0],owner()):
            self.reconcile(); self.assertTrue(any(e.stage.value=='profile' for e in self.gateway.submissions))
        self.complete(self.operation('profile'),'passed'); self.reconcile()
        self.complete(self.operation('package')); self.reconcile()
        self.complete(self.operation('rag_body'),'verified')
        self.complete(attachments[0],'passed'); self.complete(attachments[1],'unknown'); self.reconcile()
        rag=[e for e in self.gateway.submissions if e.stage.value=='rag_attachment']
        self.assertEqual([e.item_id for e in rag],[attachments[0].item_id])

    def test_native_availability_binds_status_server_process_and_runtime(self):
        from types import SimpleNamespace
        from paperflow_prefect.pipeline_dispatch import NativeGateway
        self.reconcile(); env=self.operation('intake')
        pipeline=dict(ledger_root=str(self.ledger.root),instance_id='handoff-test',prefect_api_url='http://127.0.0.1:1/api')
        gateway=NativeGateway(pipeline,'a'*64,{'intake':SimpleNamespace(task_key='intake')})
        row=dict(owner(),schema='pipeline-worker/1',config_sha256='a'*64,task_keys=['intake'],
            client_id='test-client',status_url='http://127.0.0.1:2/status')
        path=self.ledger.root/'workers/test.json'; write_json(path,row)
        from datetime import datetime,timezone
        status=dict(client_id='test-client',started_at=datetime.now(timezone.utc).isoformat(),stopping=False,
            available=1,tasks=['intake'],in_flight={'intake':{}})
        def read(url,data=None): return [dict(identifier='test-client',task_keys=['intake'])] if data is not None else status
        with patch('paperflow_prefect.pipeline_dispatch._http',side_effect=read):
            self.assertTrue(gateway.availability(env,['run'])['idle'])
            status['in_flight']['intake']['run']='now'
            self.assertTrue(gateway.availability(env,['run'])['owned'])
            status['client_id']='unrelated'
            self.assertFalse(gateway.availability(env,['run'])['certain'])
            status['client_id']='test-client'; row['config_sha256']='b'*64; write_json(path,row)
            self.assertFalse(gateway.availability(env,['run'])['certain'])
            row['config_sha256']='a'*64; row['process_create_time']-=100; write_json(path,row)
            self.assertFalse(gateway.availability(env,['run'])['idle'])


class NativeHandoffTests(unittest.TestCase):
    @classmethod
    def record_owned(cls):
        for process in cls.lab.children:
            if process.poll() is not None: continue
            try:
                parent=psutil.Process(process.pid)
                for p in [parent,*parent.children(recursive=True)]:
                    cls.owned[(p.pid,p.create_time())]=dict(pid=p.pid,process_create_time=p.create_time())
            except psutil.NoSuchProcess: pass

    @classmethod
    def setUpClass(cls):
        from pipeline_lab import start_lab,_port
        cls.lab=start_lab(ROOT/'runtime/development/pipeline-v2/handoff-native')
        cls.envpatch=patch.dict(os.environ,cls.lab.env); cls.envpatch.start()
        cls.config=cls.lab.root/'pipeline-config.json'
        cls.pipeline=dict(instance_id='handoff-native',ledger_root=str(cls.lab.root/'ledger'),
            storage_root=str(cls.lab.storage_root),parameter_storage_block='handoff-shared',
            prefect_api_url=cls.lab.api_url,control_path=str(cls.lab.root/'control.json'),
            worker_status_ports=dict(fixture=_port()))
        write_json(cls.pipeline['control_path'],dict(allowed=True,generation={'global':0,'batch':0,'runtime':0}))
        write_json(cls.config,dict(pipeline=cls.pipeline))
        os.environ.update(PAPERFLOW_PIPELINE_CONFIG=str(cls.config),PAPERFLOW_PIPELINE_CONFIG_SHA256=artifact(cls.config).sha256)
        cls.observations={}
        cls.owned={}; cls.record_owned()

    @classmethod
    def tearDownClass(cls):
        cls.record_owned()
        cls.lab.close()
        from paperflow_prefect.pipeline_ledger import owner_alive
        identities=list(cls.owned.values())
        cls.observations['closeout']=dict(launcher_exit_codes=[p.poll() for p in cls.lab.children],
            all_owned_launchers_stopped=all(p.poll() is not None for p in cls.lab.children),
            owned_identities=identities,survivors=[r for r in identities if owner_alive(r)])
        write_json(cls.lab.root/'handoff-observations.json',cls.observations)
        cls.envpatch.stop()

    def launch(self,mode,*args):
        token=uuid.uuid4().hex; log=(self.lab.root/(mode+'-'+token+'.log')).open('w',encoding='utf8')
        self.lab.logs.append(log)
        p=subprocess.Popen([sys.executable,'-u',str(ROOT/'tests/pipeline_handoff_probe.py'),mode,
            str(self.config),*map(str,args)],env=self.lab.env,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        self.lab.children.append(p); return p

    def fixture(self,name):
        root=self.lab.root/name; root.mkdir(); library=root/'library'; library.mkdir()
        write_json(root/'handoff.json',handoff(library,library/'person'))
        receipt=receive_handoff(root/'handoff.json',ReceiptStore(root/'receipts'))
        scope=root/'scope.json'; write_json(scope,dict(schema='pipeline-scope/1',batch_id=name,
            papers=[dict(receipt_ref=artifact(receipt['record_path']).to_dict())],contracts={s:'native-fixture-v1' for s in STAGES},
            control_generation={'global':0,'batch':0,'runtime':0}))
        return scope

    def coordinator(self):
        from paperflow_prefect.pipeline_dispatch import configured_coordinator
        return configured_coordinator()

    def submit(self,name,mode='submit'):
        scope=self.fixture(name); out=self.lab.root/(name+'-submitted.json')
        p=self.launch(mode,scope,'request-'+name,out); p.wait(timeout=40)
        self.assertEqual(p.returncode,27 if mode=='submit-crash' else 0)
        return self.coordinator().envelopes(name)[0]

    def reconcile(self,name):
        out=self.lab.root/(name+'-reconciled-'+uuid.uuid4().hex+'.json')
        p=self.launch('reconcile',name,out); p.wait(timeout=45)
        self.assertEqual(p.returncode,0)
        return read_json(out)

    def worker(self):
        token=uuid.uuid4().hex; p=self.launch('worker',token)
        who=self.lab.wait_file(self.lab.root/('worker-'+token+'.json'))
        self.record_owned()
        deadline=time.monotonic()+30
        while time.monotonic()<deadline:
            try:
                from paperflow_prefect.pipeline_dispatch import _http
                if _http(who['status_url'])['started_at']: return p,who
            except Exception: time.sleep(.2)
        self.fail('native status did not become ready')

    def wait_complete(self,name,stage='finish',timeout=45):
        deadline=time.monotonic()+timeout; coordinator=self.coordinator()
        while time.monotonic()<deadline:
            env=next((e for e in coordinator.envelopes(name) if e.stage.value==stage),None)
            if env:
                record=coordinator.ledger.read_operation(env.operation_key)
                if record.get('result'): return env,record
            time.sleep(.2)
        self.fail('registered successor not actually consumed: '+name+'/'+stage)

    def test_real_server_restart_and_all_four_crash_boundaries(self):
        coordinator=self.coordinator(); old=self.submit('restart-lost')
        original=coordinator.intent(old)['task_run_ids'][0]
        before=self.lab.task_state(original); self.assertEqual(before['state_type'],'SCHEDULED')
        self.record_owned()
        self.lab.restart_server()
        self.record_owned()
        replacement,who=self.worker()
        # Required positive control is consumed by THIS exact replacement Worker.
        control_initial=self.submit('positive-control',mode='flow-submit'); control,record=self.wait_complete('positive-control')
        self.assertEqual(read_json(record['result']['evidence_refs'][0]['path'])['pid'],who['pid'])
        # Descendant tags must not pollute accepted-but-unconfirmed ancestor lookup.
        self.assertEqual(len(coordinator.gateway.find(control_initial)),1)
        self.assertEqual(self.lab.task_state(original)['state_type'],'SCHEDULED')
        self.assertNotIn('result',coordinator.ledger.read_operation(old.operation_key))
        created=coordinator.gateway.find(old)[0]['created']
        while time.time()-created<=121: time.sleep(.5)
        report=self.reconcile('restart-lost')
        fresh,record=self.wait_complete('restart-lost','intake')
        self.assertEqual(fresh.dispatch_seq,1)
        self.assertEqual(read_json(record['result']['evidence_refs'][0]['path'])['pid'],who['pid'])
        new_id=coordinator.intent(fresh)['task_run_ids'][0]
        independent=self.lab.result(new_id)
        self.assertEqual(independent['operation_key'],old.operation_key)
        self.assertNotEqual(independent['reader_pid'],who['pid'])
        self.wait_complete('restart-lost')
        self.observations['server_restart']=dict(original_task_run_id=original,replacement_task_run_id=new_id,
            replacement_worker=who,positive_control_worker_pid=read_json(record['result']['evidence_refs'][0]['path'])['pid'],
            queued_seconds=time.time()-created,report=report,independent_result=independent)
        # Deliver the obsolete envelope after successful replacement; ledger fencing prevents effects.
        oldfile=self.lab.root/'old-envelope.json'; write_json(oldfile,old.to_dict())
        duplicate=self.lab.root/'duplicate.json'; p=self.launch('duplicate',oldfile,duplicate); p.wait(timeout=35)
        self.assertEqual(p.returncode,0); duplicate_id=read_json(duplicate)['task_run_id']
        deadline=time.monotonic()+30
        while self.lab.task_state(duplicate_id)['state_type'] not in {'FAILED','COMPLETED'} and time.monotonic()<deadline: time.sleep(.2)
        self.assertEqual(self.lab.task_state(duplicate_id)['state_type'],'FAILED')
        calls=[read_json(p)['body'] for p in self.lab.root.glob('fake-request-*.json')]
        self.assertEqual(sum(r['reference']==old.operation_key for r in calls),1)
        # Accepted response lost: an actual submitter exits before local confirmation.
        accepted=self.submit('accepted-crash','submit-crash')
        self.reconcile('accepted-crash'); self.wait_complete('accepted-crash')
        self.assertEqual(len(coordinator.gateway.find(accepted)),1)
        self.assertEqual(coordinator.intent(accepted)['phase'],'confirmed')
        # Actual Worker death after durable prepare result, before successor submission.
        self.lab.stop(replacement)
        crash=self.lab.root/'result-crash-crash-after-result'; crash.touch()
        self.submit('result-crash'); crashed,deadwho=self.worker()
        crashed.wait(timeout=45); self.assertEqual(crashed.returncode,29)
        self.assertFalse(any(e.stage.value=='profile' for e in coordinator.envelopes('result-crash')))
        crash.unlink(); replacement,who=self.worker(); self.reconcile('result-crash')
        profile,record=self.wait_complete('result-crash','profile')
        self.assertEqual(Path(profile.input_refs[0].path).name,'material.json')
        self.assertEqual(read_json(record['result']['evidence_refs'][0]['path'])['pid'],who['pid'])
        self.wait_complete('result-crash')
        # Running orphan: kill actual Worker before the first fake side effect.
        block=self.lab.root/'owner-crash-block'; block.touch()
        orphan=self.submit('owner-crash'); claimed=self.lab.wait_file(self.lab.root/'owner-crash-claimed.json')
        self.assertEqual(claimed['pid'],who['pid']); self.lab.stop(replacement)
        block.unlink(); replacement,who=self.worker(); report=self.reconcile('owner-crash')
        fresh,record=self.wait_complete('owner-crash','intake')
        self.assertEqual(fresh.dispatch_seq,1); self.wait_complete('owner-crash')
        calls=[read_json(p)['body'] for p in self.lab.root.glob('fake-request-*.json')]
        self.assertEqual(sum(r['reference']==orphan.operation_key for r in calls),1)
        self.observations['crashes']=dict(accepted_local_id_restored=True,durable_prepare_successor_consumed=True,
            orphan_owner=claimed,replacement_worker=who,orphan_report=report,
            stale_duplicate_state=self.lab.task_state(duplicate_id)['state_type'],actual_orphan_side_effects=1)


if __name__=='__main__': unittest.main()
