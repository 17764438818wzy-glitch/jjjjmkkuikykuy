"""Recovery behavior through the actual batch entry; all inputs are synthetic."""
import asyncio
from pathlib import Path
import unittest
from unittest.mock import patch
import test_stage5_handoff as existing
from cleanup_fixture import Crash, Fixture
from test_stage5_scope import cache_item, change_owner
from paperflow_prefect.records import Records, identity, read_json, sha_file, write_json
from paperflow_prefect.intake import object_fingerprint


class HandoffRecoveryTests(unittest.TestCase):
    setUp = existing.Stage5HandoffTests.setUp
    receive_archive = existing.Stage5HandoffTests.receive_archive

    def run_default_batch(self):
        from paperflow_prefect import flows, unified_flows as unified
        from paperflow_prefect.unified import UnifiedBackend
        from paperflow_prefect.settings import Settings
        settings=Settings(dict(Settings.load().values,execution_enabled=True,local_cleanup_enabled=True,
            library_root=str(self.root/'library'),data_root=str(self.data),nas_destination=str(self.nas),
            receipt_root=str(self.root/'batch-receipts'),acceptance_isolation_root=str(self.root),nas_reserve_bytes=0))
        backend=UnifiedBackend(settings);paper=backend.receive(self.f['intake_receipt_path'])
        work=next((self.data/'stage5'/paper['paper_id']/paper['revision']).iterdir())
        binding=dict(status='ready',work=str(work));path=backend.paper_root(paper)/'stage5-binding.json'
        write_json(path,binding);Records(backend.paper_root(paper)).complete('stage5-binding',binding,[path])
        calls=[]
        async def intake(parameters):
            from paperflow_prefect.unified_inputs import receive_inputs
            return receive_inputs(settings,**parameters)
        async def business(batch_id,paper_id,stage,attachment_id=''):
            self.assertIn(stage,{'archive','cleanup','finish'});calls.append(stage)
            batch=flows._batches[batch_id]
            return getattr(batch['backend'],stage)(batch['papers'][paper_id])
        async def artifact(**kwargs):return None
        connected=unified.unified_paper.fn
        with patch.object(flows.Settings,'load',return_value=settings), \
             patch.object(flows,'verify_limits'), \
             patch.object(unified,'input_stage',new=intake), \
             patch.object(unified,'create_markdown_artifact',new=artifact), \
             patch.object(flows.business_stage,'with_options',return_value=business), \
             patch.object(unified,'unified_paper',new=connected), \
             patch.object(flows,'invoke_model',side_effect=AssertionError('model replay')):
            result=asyncio.run(unified.paperflow_batch.fn(run_mode='resume-stage5',receipt_paths=[self.f['intake_receipt_path']]))
        return result,calls

    def test_flat_h21_interruption_and_completion_found_by_explicit_receipt(self):
        # Rebuild the synthetic input before accepting it, with no legacy body
        # directory. This layout is allowed by the H2.1 receipt contract.
        from nas_handoff_fixture import fixture, build
        from receipt_h21_fixture import handoff as old_handoff, version
        def flat_handoff(library, person, **kwargs):
            value = old_handoff(library, person, **kwargs)
            body = value['body']
            old = Path(body['path']); new = person / 'body.txt'
            old.rename(new); old.parent.rmdir()
            body.update(path=str(new), relative='body.txt',
                        file_id=identity([value['group_id'], 'body.txt'])[:24])
            rows = sorted([body, *value['attachments']], key=lambda r:r['relative'].casefold())
            value['handoff_id'] = identity(['group-handoff/1', value['group_id'],
                [(r['relative'], r['bytes'], r['sha256']) for r in rows]])[:32]
            return version(value)
        flat = self.root / 'flat'
        with patch('nas_handoff_fixture.handoff', side_effect=flat_handoff):
            self.f = fixture(flat)
        self.root = flat
        self.handoff = build(self.f)
        from paperflow_prefect.stage5_handoff import Stage5Receiver
        self.data, self.nas = flat / 'stage5-records', flat / 'nas'
        self.receiver = Stage5Receiver(self.data, flat / 'library', self.nas,
            enabled=True, isolation_root=flat, reserve_bytes=0)
        work = self.receive_archive()
        def crash(stage, path):
            if stage == 'after_unlink': raise Crash()
        with self.assertRaises(Crash): self.receiver.cleanup(work, checkpoint=crash)
        for _ in range(2):
            result, calls = self.run_default_batch()
            self.assertEqual(result['total'], 1)
            self.assertEqual(result['complete'], 1)
            self.assertEqual(calls, ['archive','cleanup','finish'])
            self.assertEqual(result['papers'][0]['paper_id'], self.handoff['paper']['paper_id'])

    def test_repeated_receive_does_not_change_frozen_admission(self):
        work = self.receive_archive()
        self.receiver.coordinator.freeze(read_json(work / 'inventory.json'), work)
        paths = [work / 'stage4/admission.json', Records(work).path('stage4-admission')]
        before = {str(p): sha_file(p) for p in paths}
        self.receiver.receive(self.handoff)
        self.assertEqual({str(p): sha_file(p) for p in paths}, before)
        self.assertEqual(self.receiver.cleanup(work)['status'], 'complete')
        self.assertEqual(self.receiver.resume(self.root / 'library/person')['status'], 'complete')

    def test_corrupt_existing_admission_is_not_silently_recreated(self):
        work = self.receive_archive()
        path = Records(work).path('stage4-admission')
        path.write_text('{}', encoding='utf8')
        with self.assertRaises(ValueError): self.receiver.receive(self.handoff)
        self.assertEqual(path.read_text(encoding='utf8'), '{}')

    def test_retained_source_result_matches_existing_flow_failure_contract(self):
        work = self.receive_archive()
        self.receiver.cleanup(work)
        extra = self.root / 'library/person/new.txt'
        extra.write_bytes(b'keep new material')
        result = self.receiver.resume(self.root / 'library/person')
        self.assertEqual(result['status'], 'incomplete')
        self.assertEqual(result.get('failures'), {'cleanup': 'retained'})
        from paperflow_prefect import flows
        with self.assertRaises(flows.BatchIncomplete): self.run_default_batch()
        reports = list((self.data / 'batches').glob('*.json'))
        self.assertEqual(len(reports), 1)
        self.assertEqual(read_json(reports[0])['papers'][0]['error_type'], 'UnifiedIncomplete')
        self.assertTrue(extra.exists())


class EvidenceScopeTests(unittest.TestCase):
    def test_txt_producer_is_protected_even_when_listed_before_its_consumer(self):
        f = Fixture(); self.addCleanup(f.close)
        producer = cache_item(f, 'owner.txt')
        consumer = cache_item(f, 'cache.txt')
        # A is a valid cache candidate and also stores B's ownership proof.
        proof = read_json(consumer['producer_record']['path'])
        write_json(producer['path'], proof)
        producer.update(bytes=Path(producer['path']).stat().st_size,
                        sha256=sha_file(producer['path']),
                        object_fingerprint=object_fingerprint(Path(producer['path']).stat()))
        change_owner(producer, files=[{k:v for k,v in producer.items() if k != 'producer_record'}])
        consumer['producer_record'] = dict(path=producer['path'], sha256=producer['sha256'])
        association = f.manager().freeze(f.paper, f.work, intermediates=[producer, consumer])
        self.assertEqual([r['path'] for r in association['intermediates']], [consumer['path']])
        self.assertFalse(set(association['evidence']) & {r['path'] for r in association['intermediates']})
        result = f.manager().run(f.work)
        self.assertEqual(result['removed_files'], 3)
        self.assertTrue(Path(producer['path']).exists())
        self.assertFalse(Path(consumer['path']).exists())
        self.assertEqual(f.manager().lookup(f.person)['action'], 'resume_cleanup')
