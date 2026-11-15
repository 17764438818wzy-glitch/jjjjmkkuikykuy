import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from paperflow_prefect import flows
from paperflow_prefect.backend import Backend
from paperflow_prefect.intake import inspect_person
from paperflow_prefect.records import Records
from paperflow_prefect.settings import Settings


def batch_for(backend, papers, limits):
    return {'backend': backend, 'papers': {p['paper_id']: p for p in papers},
            'gates': {k: asyncio.Semaphore(v) for k, v in limits.items()}}


class RagStageTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_keeps_rag_slot_until_worker_exits(self):
        started, release = threading.Event(), threading.Event()
        occupied = 0
        @asynccontextmanager
        async def global_gate(*args, **kwargs):
            nonlocal occupied
            occupied += 1
            try: yield
            finally: occupied -= 1
        with tempfile.TemporaryDirectory() as directory:
            paper = {'paper_id':'cancel', 'revision':'r', 'attachments':[]}
            def publish(current):
                started.set()
                if not release.wait(5): raise TimeoutError('fixture release missing')
            backend = SimpleNamespace(root=Path(directory), settings=Settings.load(), publish_body=publish,
                stops={'cancel':threading.Event()}, paper_root=lambda p: Path(directory)/'paper')
            batch = batch_for(backend, [paper], Settings.load()['limits'])
            flows._batches['cancel'] = batch
            with patch.object(flows, 'async_concurrency', global_gate):
                job = asyncio.create_task(flows.business_stage.fn('cancel', 'cancel', 'publish_body'))
                try:
                    deadline = time.monotonic() + 1
                    while not started.is_set() and time.monotonic() < deadline: await asyncio.sleep(.01)
                    self.assertTrue(started.is_set())
                    job.cancel(); await asyncio.sleep(.05)
                    self.assertFalse(job.done())
                    self.assertEqual(occupied, 1)
                    self.assertEqual(batch['rag_progress'].snapshot()['counts']['occupied'], 1)
                finally:
                    release.set()
                    results = await asyncio.gather(job, return_exceptions=True)
                    flows._batches.pop('cancel', None)
            self.assertIsInstance(results[0], asyncio.CancelledError)
            self.assertEqual(occupied, 0)
            self.assertEqual(batch['rag_progress'].snapshot()['counts']['occupied'], 0)
            self.assertEqual(batch['rag_progress'].snapshot()['counts']['verified_papers'], 0)

    async def test_two_batches_share_twenty_and_twenty_first_waits(self):
        limits = Settings.load()['limits']
        pool = asyncio.Semaphore(20)
        release = threading.Event()
        entered = []
        current = peak = 0

        @asynccontextmanager
        async def global_gate(name, *, occupy, strict):
            nonlocal current, peak
            self.assertEqual(name, 'paperflow-connected-rag')
            self.assertEqual(occupy, 1)
            self.assertTrue(strict)
            async with pool:
                current += 1; peak = max(peak, current)
                try: yield
                finally: current -= 1

        with tempfile.TemporaryDirectory() as directory:
            papers = [{'paper_id': f'p{i}', 'revision': 'r', 'attachments': []} for i in range(25)]
            def publish(paper):
                entered.append(paper['paper_id'])
                if not release.wait(5): raise TimeoutError('fixture release missing')
                return {'status': 'verified'}
            backend = SimpleNamespace(stops={p['paper_id']: threading.Event() for p in papers},
                publish_body=publish, root=Path(directory), settings=Settings.load(),
                paper_root=lambda p: Path(directory) / p['paper_id'])
            flows._batches['a'] = batch_for(backend, papers[:13], limits)
            flows._batches['b'] = batch_for(backend, papers[13:], limits)
            tasks = []
            patched = patch.object(flows, 'async_concurrency', global_gate)
            patched.start()
            try:
                tasks = [asyncio.create_task(flows.business_stage.fn(
                    'a' if i < 13 else 'b', p['paper_id'], 'publish_body')) for i, p in enumerate(papers)]
                deadline = time.monotonic() + 1.5
                while len(entered) < 20 and time.monotonic() < deadline: await asyncio.sleep(.01)
                self.assertEqual(len(entered), 20, '20 ready stages must enter before the release')
                self.assertEqual(peak, 20)
                self.assertEqual(sum(t.done() for t in tasks), 0)
            finally:
                release.set()
                if tasks: await asyncio.gather(*tasks, return_exceptions=True)
                patched.stop()
                flows._batches.pop('a', None); flows._batches.pop('b', None)
            self.assertEqual(current, 0)
            self.assertEqual(len(entered), 25)

    async def test_empty_attachments_complete_before_any_rag_gate(self):
        class ForbiddenGate:
            async def __aenter__(self): raise AssertionError('empty attachment requested RAG slot')
            async def __aexit__(self, *args): pass
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); person = root / 'library/school/person'
            (person / '正文').mkdir(parents=True); (person / '附件').mkdir()
            (person / '正文/a.docx').write_bytes(b'fixture body')
            paper = inspect_person(person, root / 'library')
            cfg = dict(Settings.load().values, execution_enabled=True,
                       library_root=str(root / 'library'), data_root=str(root / 'data'))
            backend = Backend(Settings(cfg))
            backend.stops[paper['paper_id']] = threading.Event()
            records = Records(backend.paper_root(paper))
            records.complete('rag-body', {'status': 'verified', 'targets': {}})
            records.complete('attachment-admission', {})
            batch = batch_for(backend, [paper], cfg['limits']); batch['gates']['rag'] = ForbiddenGate()
            flows._batches['empty'] = batch
            try:
                with patch.object(flows, 'async_concurrency', side_effect=AssertionError('global quota called')):
                    with patch('paperflow_prefect.legacy.rag_client', side_effect=AssertionError('RAG client created')):
                        try:
                            await asyncio.wait_for(flows.business_stage.fn('empty', paper['paper_id'], 'publish_attachments'), 1)
                        except flows.StageFailed as exc:
                            self.fail('empty attachment must bypass quota: ' + str(exc))
                result = records.cached('rag-attachments')
                self.assertEqual(result['status'], 'verified')
                self.assertEqual(result['attachments'], 0)
                self.assertEqual(result['targets'], [])
            finally: flows._batches.pop('empty', None)

    async def test_one_paper_archives_while_another_is_still_uploading(self):
        from paperflow_prefect.workflow import execute_paper
        blocked, release, archived = asyncio.Event(), asyncio.Event(), asyncio.Event()
        async def call(stage, paper, row=None):
            if stage == 'publish_body' and paper['paper_id'] == 'b':
                blocked.set(); await release.wait()
            if stage == 'archive' and paper['paper_id'] == 'a': archived.set()
            return {'status': 'complete' if stage == 'finish' else 'verified'}
        slow = asyncio.create_task(execute_paper({'paper_id': 'b', 'attachments': []}, call))
        fast = asyncio.create_task(execute_paper({'paper_id': 'a', 'attachments': []}, call))
        try:
            await asyncio.wait_for(blocked.wait(), 1)
            await asyncio.wait_for(archived.wait(), 1)
            self.assertFalse(slow.done())
            self.assertEqual((await fast)['status'], 'complete')
        finally:
            release.set(); await slow


if __name__ == '__main__': unittest.main()
