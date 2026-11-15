import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from paperflow_prefect.unified import UnifiedBackend, execute_unified
from paperflow_prefect.records import Records,read_json,write_json
from paperflow_prefect.settings import Settings
from test_receipt_h21 import handoff
from paperflow_prefect.intake_handoff import receive_handoff
from paperflow_prefect.receipts import ReceiptStore
from rag_fixture import native_fixture
from local_rag import LocalRag


class ResumeEntryTests(unittest.IsolatedAsyncioTestCase):
    async def test_resume_rejects_changed_frozen_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);library=root/'library';library.mkdir()
            h=handoff(library,library/'person');write_json(root/'handoff.json',h)
            r=receive_handoff(root/'handoff.json',ReceiptStore(root/'receipts'))
            cfg=dict(Settings.load().values,execution_enabled=True,data_root=str(root/'data'),library_root=str(library))
            backend=UnifiedBackend(Settings(cfg));paper=backend.receive(r['record_path'])
            changed=read_json(r['record_path']);changed['inventory']['attachments']=[];changed['paper_id']='changed'
            write_json(r['record_path'],changed)
            with self.assertRaises(ValueError):backend.receipt_for(paper)
            with self.assertRaises(ValueError):backend.receive(r['record_path'])

    async def test_resume_finishes_original_objects_and_whole_paper_cleanup_without_models(self):
        f=Path(native_fixture()['root']);prepared=read_json(f/'body/prepared-publication.json')
        source=(f/'library/school/fixture/正文/fixture.docx').read_bytes()
        client=LocalRag();client.lose_chunk=True
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);library=root/'library';library.mkdir();data=root/'data';data.mkdir()
            h=handoff(library,library/'person',body=source);write_json(root/'handoff.json',h)
            r=receive_handoff(root/'handoff.json',ReceiptStore(root/'receipts'))
            cfg=dict(Settings.load().values,execution_enabled=True,local_cleanup_enabled=True,
                     data_root=str(data),library_root=str(library),nas_destination=str(root/'nas'),nas_reserve_bytes=0)
            backend=UnifiedBackend(Settings(cfg));paper=backend.receive(r['record_path']);work=backend.body_work(paper)
            write_json(work/'prepared-publication.json',prepared)
            from preextract_publish import deliver_publication
            with self.assertRaises(TimeoutError):deliver_publication(prepared,config=None,receipt_path=work/'delivery-receipt.json',client=client)
            calls=[]
            async def call(stage,current,row=None):
                calls.append(stage);self.assertIn(stage,{'recover_body','publish_attachments','archive','cleanup','finish'})
                return getattr(backend,stage)(current)
            with patch('paperflow_prefect.legacy.rag_client',return_value=client), \
                 patch('paperflow_prefect.legacy.connection',side_effect=AssertionError('model replay')):
                result=await execute_unified(paper,backend.receipt_for(paper),call,resume_rag=True)
            self.assertEqual(result['status'],'complete',result)
            self.assertEqual(client.uploads,1)
            self.assertEqual(client.posts,sum(len(c) for c in client.chunks.values()))
            self.assertEqual(result['archive_status'],'verified')
            self.assertFalse(Path(paper['body']['path']).exists())
            self.assertTrue((work/'prepared-publication.json').exists())

    async def test_missing_resume_artifacts_fail_without_upstream_calls(self):
        calls=[]
        async def call(stage,paper,row=None):
            calls.append(stage)
            if stage=='recover_body':raise FileNotFoundError('missing original receipt')
            self.fail('later phase reached')
        result=await execute_unified({'paper_id':'p','body':{},'attachments':[]},{'state':'已接收'},call,resume_rag=True)
        self.assertEqual(calls,['recover_body']);self.assertEqual(result['status'],'incomplete')

    async def test_empty_attachment_inventory_does_not_hide_missing_descriptions(self):
        from paperflow_prefect import flows
        # Non-empty input must keep RAG admission even if identification is missing.
        visited = []
        class Gate:
            async def __aenter__(self): visited.append('quota'); raise RuntimeError('no description')
            async def __aexit__(self, *a): pass
        with tempfile.TemporaryDirectory() as directory:
            from types import SimpleNamespace
            paper = {'paper_id':'p','revision':'r','attachments':[{'file_id':'a'}]}
            backend = SimpleNamespace(root=Path(directory), settings=Settings.load(),
                stops={'p':threading.Event()}, paper_root=lambda p: Path(directory)/'p')
            flows._batches['nonempty'] = {'backend':backend,'papers':{'p':paper},'gates':{'rag':Gate()}}
            try:
                with self.assertRaises(flows.StageFailed):
                    await flows.business_stage.fn('nonempty', 'p', 'publish_attachments')
                self.assertEqual(visited, ['quota'])
            finally: flows._batches.pop('nonempty', None)


if __name__ == '__main__': unittest.main()
