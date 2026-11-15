"""Retain one auditable end-to-end fixture and its real per-file receipts."""
import asyncio
from pathlib import Path
import unittest
from cleanup_fixture import Crash
from unified_fixture import Fixture
from paperflow_prefect.records import Records, sha_file, write_json
from paperflow_prefect.settings import Settings


class CleanupAcceptance(unittest.TestCase):
    def test_native_archive_crash_recovery_and_repeat_scan_with_durable_evidence(self):
        from paperflow_prefect.unified import execute_unified
        f = Fixture()
        # Keep this generated fixture for the user's review; no real library data.
        f.temp._finalizer.detach()
        backend=f.backend;paper=f.original;archive=f.archive
        # Freeze by the actual backend root, before any original is removed.
        manager = f.manager()
        manager.freeze(f.paper, f.work)
        original_manifest = [{'path': row['path'], 'sha256': row['sha256'], 'bytes': row['bytes']}
                             for row in paper['archive_files']]
        protected = dict(f.protected)
        for name in ('original-total-papers.txt', 'old-library.txt', 'original-mixed.zip'):
            path = f.root / name
            path.write_bytes(b'protected outside library sentinel')
            protected[str(path)] = sha_file(path)
        def interrupt(stage, path):
            if stage == 'after_unlink':
                raise Crash('intent persisted; unlink happened; result not yet persisted')
        with self.assertRaises(Crash):
            manager.run(f.work, checkpoint=interrupt)
        interrupted = manager.inspect(f.work)
        self.assertEqual(len(interrupted['pending_intents']), 1)
        traces = []
        async def rescan():
            recovered = backend.receive(f.receipt_path)
            self.assertEqual(recovered['revision'], paper['revision'])
            async def call(stage, current, row=None):
                traces.append(stage)
                self.assertIn(stage, {'archive','cleanup', 'finish'})
                return getattr(backend, stage)(current)
            return await execute_unified(recovered,backend.receipt_for(recovered),call,resume_stage5=True)
        first = asyncio.run(rescan())
        second = asyncio.run(rescan())
        self.assertEqual(first['status'], 'complete')
        self.assertEqual(second['status'], 'complete')
        cleanup = manager.inspect(f.work)
        self.assertEqual(cleanup['removed_files'], 1)
        self.assertEqual(cleanup['reconciled_absent_files'], 1)
        actual_protected = {path: sha_file(path) for path in protected}
        self.assertEqual(actual_protected, protected)
        report = {'schema': 'isolated-cleanup-acceptance/1', 'status': 'passed',
            'fixture_root': str(f.root), 'paper_id': paper['paper_id'], 'revision': paper['revision'],
            'record_root': str(f.work), 'nas_fixture': str(f.nas), 'original_inventory': original_manifest,
            'nas_status': archive['status'], 'nas_manifest_sha256': archive['manifest_sha256'],
            'interruption': interrupted, 'cleanup_result': cleanup, 'rescan_stage_calls': traces,
            'first_rescan': first, 'second_rescan': second,
            'preserved_hashes_before': protected, 'preserved_hashes_after': actual_protected,
            'model_requests': 0, 'rag_writes': 0, 'rag_evidence': 'synthetic fixture receipts',
            'nas_readback': 'native engine, local generated fixture', 'production_acceptance': False,
            'real_new_library_deleted_files': 0, 'deployed': False}
        write_json(Path(__file__).resolve().parents[1] / 'evidence/acceptance-receipt.json', report)
