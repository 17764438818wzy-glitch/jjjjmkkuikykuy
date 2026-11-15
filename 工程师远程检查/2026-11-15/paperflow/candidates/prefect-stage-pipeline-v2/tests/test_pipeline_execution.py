import asyncio
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import tempfile
import unittest
import psutil
from paperflow_prefect.pipeline_contracts import ArtifactRef, StageEnvelope, StageResult, operation_key
from paperflow_prefect.pipeline_ledger import StageLedger
# Prefect imports occur before IsolatedAsyncioTestCase starts the event loop.
from paperflow_prefect.pipeline_tasks import TASKS, WORKERS
from paperflow_prefect.records import write_json, sha_file, read_json


def ref(path): return ArtifactRef(str(path.resolve()), sha_file(path))
def envelope(root, stage='profile', item=None):
    receipt=root/'receipt.json'
    if not receipt.exists(): write_json(receipt, {'fixture':True})
    inputs=dict(paper_id='paper',source_sha256='a'*64,stage=stage,item_id=item,part_id=None,input_refs=[],contract_version='v1')
    return StageEnvelope('batch','paper',ref(receipt),operation_key(inputs),stage,item,None,(),'v1',{'global':0,'batch':0,'runtime':0},0,'normal')
def owner(name): return dict(instance_id=name,pid=os.getpid(),process_create_time=psutil.Process().create_time(),task_run_id=name)

class ExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.root=Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
    async def test_ledger_claims_are_async_context_isolated(self):
        ledger=StageLedger(self.root/'ledger'); first=envelope(self.root); second=envelope(self.root,'attachment','item')
        ready=asyncio.Event(); observed=[]
        async def a():
            with ledger.claim(first,owner('a')):
                ready.set(); await asyncio.sleep(.04)
                observed.append(set(ledger._claims()))
        async def b():
            await ready.wait()
            with ledger.claim(second,owner('b')):
                observed.append(set(ledger._claims())); await asyncio.sleep(.08)
        await asyncio.gather(a(),b())
        self.assertEqual(observed,[{second.operation_key},{first.operation_key}])

    async def test_business_tasks_empty_attachment_rag_skips_without_remote_call(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        from paperflow_prefect.pipeline_runtime import StageContext
        from paperflow_prefect.pipeline_execution import execute_stage
        env=envelope(self.root,'rag_attachment'); ledger=StageLedger(self.root/'ledger')
        ctx=StageContext(env,ledger,None,None,lambda:None,
            settings=SimpleNamespace(values={'pipeline':{'instance_id':'empty-fixture'}}),
            backend=SimpleNamespace(paper_root=lambda paper:self.root),paper={'attachments':[]})
        with patch('paperflow_prefect.pipeline_execution.load_stage_context',return_value=ctx), \
             patch('paperflow_prefect.pipeline_execution.configured_path',return_value=self.root/'config.json'), \
             patch('paperflow_prefect.stage_process.run_stage_process',side_effect=AssertionError('remote call')):
            result=await execute_stage(env)
        self.assertEqual(result.state,'skipped')
        self.assertEqual(read_json(result.evidence_refs[0].path)['reason'],'empty_inventory')

    async def test_transport_stores_native_complete_and_bound_reservation(self):
        from paperflow_prefect.model_transport import send_model, save_request
        from paperflow_prefect.pipeline_runtime import StageContext, active_context
        env=envelope(self.root); ledger=StageLedger(self.root/'ledger')
        calls=[]
        async def server(reader,writer):
            header=await reader.readuntil(b'\r\n\r\n'); length=int(next(x.split(b':')[1] for x in header.split(b'\r\n') if x.lower().startswith(b'content-length:')))
            calls.append(await reader.readexactly(length))
            payload=b'data: {"choices":[{"delta":{"content":"{}"},"finish_reason":"stop"}],"usage":{"total_tokens":4}}\n\ndata: [DONE]\n\n'
            writer.write(b'HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: '+str(len(payload)).encode()+b'\r\n\r\n'+payload); await writer.drain(); writer.close()
        srv=await asyncio.start_server(server,'127.0.0.1',0); self.addAsyncCleanup(srv.wait_closed); self.addCleanup(srv.close)
        connection=dict(base_url=f'http://127.0.0.1:{srv.sockets[0].getsockname()[1]}/v1',api_key='<REDACTED_SECRET>',model='test')
        @asynccontextmanager
        async def scope(name): yield
        ctx=StageContext.for_transport(env,ledger,connection,scope,lambda:None)
        token=active_context.set(ctx)
        try:
            with ledger.claim(env,owner('send')):
                request=await save_request({'model':'test','messages':[],'stream':True},self.root/'model',env)
                result=await send_model(request,env)
                self.assertIsInstance(result,ArtifactRef)
                state=read_json(self.root/'model/request-state.json')
                self.assertEqual(state['status'],'response_complete')
                self.assertEqual(state['usage']['total_tokens'],4)
                self.assertTrue(state['done_received'])
                self.assertEqual((self.root/'model/response.txt').read_text(),'{}')
                reservation=ledger.send_budget(env.paper_id)['reservations'][0]
                self.assertEqual(reservation['operation_key'],env.operation_key)
                self.assertEqual(reservation['owner']['task_run_id'],'send')
                self.assertEqual(len(calls),1)
        finally: active_context.reset(token)

    async def test_business_tasks_have_distinct_keys_and_explicit_no_cache(self):
        from paperflow_prefect.pipeline_tasks import TASKS, WORKERS
        self.assertEqual(len({t.task_key for t in TASKS.values()}),11)
        for task in TASKS.values():
            self.assertEqual(task.task_key,task.with_options(persist_result=True).task_key)
            self.assertTrue(task.persist_result)
            self.assertIsNone(task.cache_key_fn(None,{}))
            self.assertIsNone(task.cache_policy)
            self.assertTrue(task.refresh_cache)
            self.assertEqual(task.result_storage_key,'pipeline-task-results/{task_run.id}')
        self.assertEqual(WORKERS['body']['limit'],128)
        self.assertEqual(WORKERS['model_recovery']['limit'],32)
        self.assertNotEqual(WORKERS['body']['tasks'],WORKERS['model_recovery']['tasks'])


class TransportAdversityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.root=Path(self.temp.name); self.addCleanup(self.temp.cleanup)
        self.clients=[]; self.servers=[]; self.hit=asyncio.Event(); self.closed=asyncio.Event(); self.calls=0
    async def asyncTearDown(self):
        for server in self.servers: server.close(); await server.wait_closed()
        for writer in self.clients: writer.close()
    async def start(self,payload=None,hang=False,status=200):
        async def handle(reader,writer):
            self.clients.append(writer)
            try:
                header=await reader.readuntil(b'\r\n\r\n')
                size=int(next(x.split(b':')[1] for x in header.split(b'\r\n') if x.lower().startswith(b'content-length:')))
                await reader.readexactly(size); self.calls+=1
                writer.write(f'HTTP/1.1 {status} Response\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n'.encode())
                if payload: writer.write(payload)
                await writer.drain(); self.hit.set()
                if hang: await reader.read()
            finally: writer.close(); self.closed.set()
        server=await asyncio.start_server(handle,'127.0.0.1',0); self.servers.append(server)
        return dict(base_url=f'http://127.0.0.1:{server.sockets[0].getsockname()[1]}/v1',api_key='test',model='test')
    async def transmit(self,connection):
        from paperflow_prefect.model_transport import send_model,save_request
        from paperflow_prefect.pipeline_runtime import StageContext,active_context
        env=envelope(self.root); self.ledger=StageLedger(self.root/'ledger')
        @asynccontextmanager
        async def scope(name): yield
        ctx=StageContext.for_transport(env,self.ledger,connection,scope,lambda:None)
        token=active_context.set(ctx)
        try:
            with self.ledger.claim(env,owner('transmit')):
                request=await save_request(dict(model='test',messages=[],stream=True),self.root/'model',env)
                return await send_model(request,env)
        finally: active_context.reset(token)
    async def test_hung_stream_cancellation_retains_partial_and_unknown(self):
        payload=b'data: {"choices":[{"delta":{"content":"partial"}}]}\n\n'
        connection=await self.start(payload,True)
        task=asyncio.create_task(self.transmit(connection)); await self.hit.wait(); await asyncio.sleep(.03)
        start=asyncio.get_running_loop().time(); task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        self.assertLess(asyncio.get_running_loop().time()-start,2)
        await asyncio.wait_for(self.closed.wait(),2)
        self.assertEqual((self.root/'model/response-events.jsonl').read_bytes(),payload)
        self.assertEqual((self.root/'model/response.txt').read_text(),'partial')
        self.assertEqual(self.ledger.read_operation(envelope(self.root).operation_key)['state'],'unknown')
        state=read_json(self.root/'model/request-state.json'); self.assertNotEqual(state['status'],'response_complete')
        self.assertIsNone(state['usage'])
    async def test_blocked_rag_process_does_not_hold_body_execution(self):
        import sys
        from paperflow_prefect.stage_process import supervise
        capsule=self.root/'rag-envelope.json'; record=self.root/'rag-process.json'
        write_json(capsule,dict(sleep=True))
        blocked=asyncio.create_task(supervise([sys.executable,str(Path(__file__).with_name('pipeline_execution_probe.py'))],capsule,record))
        for _ in range(100):
            if Path(str(capsule)+'.go.json').exists(): break
            await asyncio.sleep(.02)
        connection=await self.start(b'data: {"choices":[{"delta":{"content":"{}"},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n')
        try:
            result=await asyncio.wait_for(self.transmit(connection),3)
            self.assertIsInstance(result,ArtifactRef); self.assertFalse(blocked.done())
        finally:
            blocked.cancel()
            with self.assertRaises(asyncio.CancelledError): await blocked

    async def test_http_failure_keeps_native_error_body_without_fake_response(self):
        connection=await self.start(b'{"error":{"code":"rate_limit_error"}}',status=429)
        with self.assertRaises(RuntimeError): await self.transmit(connection)
        state=read_json(self.root/'model/request-state.json')
        self.assertEqual(state['http_status'],429)
        self.assertEqual(state['error_response_sha256'],sha_file(self.root/'model/http-error.txt'))
        self.assertFalse((self.root/'model/response.txt').exists())

    async def test_stop_without_done_is_truncated_and_stream_error_not_success(self):
        for index,payload in enumerate((b'data: {"choices":[{"delta":{"content":"x"},"finish_reason":"stop"}]}\n\n',
                b'data: {"error":{"code":"upstream_stream_error"}}\n\ndata: [DONE]\n\n')):
            with self.subTest(index=index):
                target=self.root/str(index); target.mkdir(); prior=self.root; self.root=target
                try:
                    connection=await self.start(payload)
                    with self.assertRaises(RuntimeError): await self.transmit(connection)
                    state=read_json(self.root/'model/request-state.json')
                    self.assertNotEqual(state['status'],'response_complete')
                    self.assertEqual((self.root/'model/response-events.jsonl').read_bytes(),payload)
                finally: self.root=prior
    async def test_many_small_events_cancel_with_offloop_bounded_flushes(self):
        from unittest.mock import patch
        from paperflow_prefect.model_transport import EvidenceWriter
        import threading
        payload=b'data: {"choices":[{"delta":{"content":"x"}}]}\n\n'*30000
        connection=await self.start(payload,True)
        thread_id=threading.get_ident(); writes=[]; original=EvidenceWriter.append
        def append(writer,raw,text):
            writes.append((len(raw),threading.get_ident()))
            return original(writer,raw,text)
        with patch.object(EvidenceWriter,'append',append):
            task=asyncio.create_task(self.transmit(connection)); await self.hit.wait()
            for _ in range(100):
                if len(writes)>2: break
                await asyncio.sleep(.01)
            start=asyncio.get_running_loop().time(); task.cancel()
            with self.assertRaises(asyncio.CancelledError): await task
            self.assertLess(asyncio.get_running_loop().time()-start,2)
        raw=(self.root/'model/response-events.jsonl').read_bytes()
        self.assertGreater(len(raw),65536); self.assertEqual(raw,payload[:len(raw)])
        self.assertLess(len(writes),100)
        self.assertTrue(all(t!=thread_id for _,t in writes))

    async def test_unowned_transport_never_enters_http(self):
        from paperflow_prefect.model_transport import save_request,send_model
        from paperflow_prefect.pipeline_runtime import StageContext,active_context
        connection=await self.start(b'')
        env=envelope(self.root)
        @asynccontextmanager
        async def scope(name): yield
        token=active_context.set(StageContext.for_transport(env,StageLedger(self.root/'ledger'),connection,scope,lambda:None))
        try:
            request=await save_request(dict(model='test'),self.root/'model',env)
            from paperflow_prefect.pipeline_ledger import EvidenceChanged
            with self.assertRaises(EvidenceChanged):
                await send_model(request,env)
            self.assertEqual(self.calls,0)
        finally: active_context.reset(token)

class ProcessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.root=Path(self.temp.name); self.addCleanup(self.temp.cleanup)
    async def test_new_process_executes_from_envelope_and_checks_result_hash(self):
        from paperflow_prefect.stage_process import supervise
        import sys
        env=envelope(self.root,'prepare'); capsule=self.root/'capsule.json'; record=self.root/'process.json'
        write_json(capsule,dict(envelope=env.to_dict()))
        result=await supervise([sys.executable,str(Path(__file__).with_name('pipeline_execution_probe.py'))],capsule,record)
        self.assertEqual(result.state,'complete'); self.assertEqual(result.operation_key,env.operation_key)
        journal=read_json(record)
        self.assertIn('process_create_time',journal['child'])
        self.assertIn(journal['child'],journal['processes'])
        self.assertGreaterEqual(len(journal['processes']),2 if os.name=='nt' else 1)
    async def test_cancel_terminates_recorded_launcher_and_actual_children(self):
        from paperflow_prefect.stage_process import supervise
        import sys
        capsule=self.root/'capsule.json'; record=self.root/'process.json'
        write_json(capsule,dict(spawn_child=True))
        task=asyncio.create_task(supervise([sys.executable,str(Path(__file__).with_name('pipeline_execution_probe.py'))],capsule,record))
        for _ in range(200):
            if (self.root/'grandchild.json').exists(): break
            await asyncio.sleep(.02)
        self.assertTrue((self.root/'grandchild.json').exists()); await asyncio.sleep(.1)
        start=asyncio.get_running_loop().time(); task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        self.assertLess(asyncio.get_running_loop().time()-start,5)
        journal=read_json(record); self.assertEqual(journal['status'],'terminated')
        for identity in journal['processes']:
            if psutil.pid_exists(identity['pid']):
                self.assertNotEqual(psutil.Process(identity['pid']).create_time(),identity['process_create_time'])

class NativeAdapterTests(unittest.TestCase):
    def test_crash_after_complete_response_can_parse_offline_without_resend(self):
        import subprocess,sys
        with tempfile.TemporaryDirectory() as directory:
            command=[sys.executable,str(Path(__file__).with_name('pipeline_native_probe.py')),directory]
            first=subprocess.run(command+['crash'],capture_output=True,text=True,encoding='utf8',errors='replace',timeout=60,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
            self.assertEqual(first.returncode,23,first.stderr)
            state=read_json(next((Path(directory)/'data').glob('papers/*/body/*/model/request-state.json')))
            self.assertEqual(state['status'],'response_complete')
            second=subprocess.run(command+['offline'],capture_output=True,text=True,encoding='utf8',errors='replace',timeout=60,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
            self.assertEqual(second.returncode,0,second.stderr)
            self.assertEqual(read_json(Path(directory)/'offline-result.json')['added_http_calls'],0)

    def test_actual_native_partition_and_synthesis_keep_part_identities(self):
        import subprocess,sys
        with tempfile.TemporaryDirectory() as directory:
            result=subprocess.run([sys.executable,str(Path(__file__).with_name('pipeline_native_probe.py')),directory,'partition'],capture_output=True,text=True,encoding='utf8',errors='replace',timeout=90,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
            self.assertEqual(read_json(Path(directory)/'native-result.json')['loopback_http_calls'],4)

    def test_actual_native_body_attachment_and_fresh_prepare_process(self):
        import subprocess,sys
        with tempfile.TemporaryDirectory() as directory:
            result=subprocess.run([sys.executable,str(Path(__file__).with_name('pipeline_native_probe.py')),directory],
                capture_output=True,text=True,encoding='utf8',errors='replace',timeout=90,
                creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
            report=read_json(Path(directory)/'native-result.json')
            self.assertEqual(report['profile'],'passed'); self.assertEqual(report['attachment'],'passed')
            self.assertEqual(report['loopback_http_calls'],2)
            self.assertEqual(report['source_config_mismatch_calls'],0)

class ItemDispatchTests(unittest.TestCase):
    def test_single_attachment_publish_keeps_parent_identity_and_does_not_publish_sibling(self):
        from paperflow_prefect.unified import UnifiedBackend
        from unittest.mock import patch
        from paperflow_prefect.records import Records
        with tempfile.TemporaryDirectory() as directory:
            backend=object.__new__(UnifiedBackend); backend.root=Path(directory)
            paper=dict(paper_id='p',revision='r',body={'sha256':'a'*64},attachments=[{'file_id':'a'},{'file_id':'b'}])
            class Settings:
                def require_execution(self): pass
            backend.settings=Settings()
            received=[]
            def publish(b,p,*,items=None):
                received.append((p,items))
                return dict(status='verified',attachments=1,receipt_paths=[],targets=[])
            with patch('paperflow_prefect.unified_attachments.publish_cards',publish):
                self.assertTrue(hasattr(backend,'publish_attachment'),'single item RAG adapter missing')
                backend.publish_attachment(paper,paper['attachments'][0])
                self.assertIsNone(Records(backend.paper_root(paper)).cached('rag-attachments'))
                backend.publish_attachment(paper,paper['attachments'][1])
            self.assertEqual([x[1] for x in received],[[paper['attachments'][0]],[paper['attachments'][1]]])
            self.assertTrue(all(x[0] is paper for x in received))
            self.assertEqual(Records(backend.paper_root(paper)).cached('rag-attachments')['attachments'],2)

class DriftCompatibilityTests(unittest.TestCase):
    def test_attachment_retry_selector_is_used_before_root_response(self):
        from unittest.mock import patch
        from paperflow_prefect.profile_recovery import _selected_model
        with tempfile.TemporaryDirectory() as directory:
            work=Path(directory); intent=work/'private/attachment-recovery/retry-intent.json'
            write_json(intent,dict(schema='attachment-engineering-attempt/1'))
            # The selector must reject malformed intent, rather than silently use root/model.
            with self.assertRaises(Exception): _selected_model(work)


class NativeCacheAdoptionTests(unittest.TestCase):
    def test_shared_body_sync_api_preserves_fresh_and_cached_safety(self):
        import subprocess,sys
        with tempfile.TemporaryDirectory() as directory:
            result=subprocess.run([sys.executable,str(Path(__file__).with_name('pipeline_native_probe.py')),directory,'shared-body'],capture_output=True,text=True,encoding='utf8',errors='replace',timeout=90,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
            self.assertEqual(read_json(Path(directory)/'shared-result.json')['new_injected_requests'],1)

    def check_adoption(self,mode):
        import subprocess,sys
        with tempfile.TemporaryDirectory() as directory:
            result=subprocess.run([sys.executable,str(Path(__file__).with_name('pipeline_native_probe.py')),directory,mode],capture_output=True,text=True,encoding='utf8',errors='replace',timeout=90,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
            report=read_json(Path(directory)/'adoption-result.json')
            self.assertEqual(report['added_http_calls'],0)
            self.assertTrue(report['preserved_native_bytes'])
    def test_first_single_cache_adoption_validates_original_provenance(self): self.check_adoption('adoption')
    def test_first_partition_cache_adoption_validates_every_child_and_synthesis(self): self.check_adoption('adoption-partition')
    def test_first_recovery_cache_adoption_validates_parent_and_selected_child(self): self.check_adoption('adoption-recovery')

if __name__=='__main__': unittest.main()
