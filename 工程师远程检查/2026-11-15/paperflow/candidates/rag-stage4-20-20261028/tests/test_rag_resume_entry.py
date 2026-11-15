import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import shutil
import tempfile
import threading
import unittest
from unittest.mock import patch

from paperflow_prefect.backend import Backend
from paperflow_prefect.intake import inspect_person
from paperflow_prefect.records import Records, read_json, write_json
from paperflow_prefect.settings import PROJECT, Settings
from local_rag import LocalRag
from rag_fixture import native_fixture


class ResumeEntryTests(unittest.IsolatedAsyncioTestCase):
    async def test_resume_rejects_changed_or_unattested_frozen_inventory(self):
        fixture = native_fixture()
        fixture_root = Path(fixture['root'])
        prepared = read_json(fixture_root / 'body/prepared-publication.json')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); person = root / 'library/school/person'
            (person / '正文').mkdir(parents=True); (person / '附件').mkdir()
            shutil.copyfile(fixture_root / 'library/school/fixture/正文/fixture.docx', person / '正文/fixture.docx')
            (person / '附件/source.py').write_text('print(1)', encoding='utf8')
            cfg = dict(Settings.load().values, execution_enabled=True, data_root=str(root/'data'),
                       library_root=str(root/'library'))
            backend = Backend(Settings(cfg)); paper = backend.intake(person)
            work = backend.body_work(paper)
            write_json(work / 'prepared-publication.json', prepared)
            write_json(work / 'delivery-receipt.json', {})
            inventory = backend.paper_root(paper) / 'inventory.json'
            changed = dict(paper, attachments=[])
            write_json(inventory, changed)
            with self.assertRaisesRegex(ValueError, 'artifact changed|冻结清单'):
                backend.load_rag_resume(inventory)
            write_json(inventory, paper)
            # An old or incomplete intake record without the inventory hash
            # cannot authorize an empty-attachment shortcut on recovery.
            records = Records(backend.paper_root(paper))
            intake = read_json(records.path('intake')); intake['artifacts'] = {}
            write_json(records.path('intake'), intake)
            with self.assertRaisesRegex(ValueError, '冻结清单'):
                backend.load_rag_resume(inventory)

    async def test_resume_entry_finishes_rag_then_archives_without_upstream_calls(self):
        from paperflow_prefect import flows
        fixture = native_fixture()
        fixture_root = Path(fixture['root'])
        prepared = read_json(fixture_root / 'body/prepared-publication.json')
        client = LocalRag(); client.lose_chunk = True
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); person = root / 'library/school/person'
            (person / '正文').mkdir(parents=True); (person / '附件').mkdir()
            shutil.copyfile(fixture_root / 'library/school/fixture/正文/fixture.docx', person / '正文/fixture.docx')
            cfg = dict(Settings.load().values, execution_enabled=True, data_root=str(root/'data'),
                library_root=str(root/'library'), nas_destination=str(root/'nas'), nas_reserve_bytes=0)
            backend = Backend(Settings(cfg))
            paper = backend.intake(person)
            work = backend.body_work(paper)
            write_json(work / 'prepared-publication.json', prepared)
            shutil.copyfile(fixture_root / 'body/material.json', work / 'material.json')
            from preextract_publish import deliver_publication
            with self.assertRaises(TimeoutError):
                deliver_publication(prepared, config=None, receipt_path=work/'delivery-receipt.json', client=client)
            inventory = backend.paper_root(paper) / 'inventory.json'
            forbidden = lambda *a, **k: self.fail('resume entered upstream/model stage')
            @asynccontextmanager
            async def local_quota(*a, **k): yield
            async def run_resume(batch_id, inventory_path):
                return await flows.connected_rag_resume.fn(batch_id, inventory_path)
            async def stage_call(*args, **kwargs): return await flows.business_stage.fn(*args, **kwargs)
            # The external orchestration and RAG service are substituted; actual
            # workflow, backend, native publisher, records, locks and NAS copy run.
            with patch.object(flows.Settings, 'load', return_value=Settings(cfg)), \
                 patch.object(flows, 'verify_limits', return_value={}), \
                 patch.object(flows, 'async_concurrency', local_quota), \
                 patch.object(flows.Backend, 'prepare', forbidden), \
                 patch.object(flows.Backend, 'profile', forbidden), \
                 patch.object(flows.Backend, 'package', forbidden), \
                 patch.object(flows.Backend, 'attachment', forbidden), \
                 patch('paperflow_prefect.legacy.connection', forbidden), \
                 patch('preextract_request.request_once', forbidden), \
                 patch('paperflow_prefect.legacy.rag_client', return_value=client):
                self.assertTrue(hasattr(flows, 'connected_rag_resume'), 'separate RAG resume entry missing')
                original_resume = flows.connected_rag_resume
                async def run_resume(batch_id, path): return await original_resume.fn(batch_id, path)
                with patch.object(flows, 'connected_rag_resume', run_resume), \
                     patch.object(flows.business_stage, 'with_options', return_value=stage_call):
                    result = await flows.paperflow_batch.fn(run_mode='resume-rag', resume_records=[str(inventory)])
            self.assertEqual(result['complete'], 1)
            self.assertEqual(client.uploads, 1)
            self.assertEqual(client.posts, sum(len(c) for c in client.chunks.values()))
            completed = read_json(backend.paper_root(paper) / 'completed.json')
            self.assertTrue(completed['local_folder_retained'])
            self.assertEqual(completed['nas']['status'], 'verified')
            self.assertTrue((person / '正文/fixture.docx').exists())

    async def test_missing_resume_artifacts_fail_without_model_or_input_scan(self):
        from paperflow_prefect.workflow import execute_rag_resume
        calls = []
        async def call(stage, paper, row=None):
            calls.append(stage)
            if stage == 'recover_body': raise FileNotFoundError('missing original receipt')
            self.fail('later phase reached')
        result = await execute_rag_resume({'paper_id':'p','attachments':[]}, call)
        self.assertEqual(calls, ['recover_body'])
        self.assertEqual(result['status'], 'incomplete')

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
