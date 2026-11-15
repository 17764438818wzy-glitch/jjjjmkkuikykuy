"""Pure contract tests: no Prefect import, filesystem fixture, or HTTP call."""
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
import unittest

from paperflow_prefect import pipeline_contracts as contracts


def evidence():
    return {
        'paper_id': 'paper-1', 'batch_id': 'batch-1', 'whole_receipt_verified': True,
        'body_admitted': True,
        'submission': {'state': 'complete'},
        'stages': {'intake': {'state': 'verified'}},
        'attachments': {'inventory_state': 'complete', 'items': {
            'attachment-1': {'required': True, 'attachment': {'state': 'pending'},
                             'rag_attachment': {'state': 'pending'}}}},
    }


def identity_inputs():
    return {'paper_id': 'paper-1', 'source_sha256': 'a' * 64,
            'stage': 'profile', 'item_id': None, 'part_id': None,
            'input_refs': [{'path': 'cache/body.md', 'sha256': 'b' * 64}],
            'contract_version': 'body-profile/1'}


class DependencyTests(unittest.TestCase):
    def test_missing_body_blocks_body_stages_but_not_attachment_identification(self):
        row = evidence()
        del row['body_admitted']
        self.assertNotIn('prepare', contracts.ready_stages(row))
        self.assertIn('attachment', contracts.ready_stages(row))
        row['body_admitted'] = False
        row['stages']['prepare'] = {'state': 'complete'}
        self.assertNotIn('profile', contracts.ready_stages(row))
        row['stages']['profile'] = {'state': 'passed'}
        self.assertNotIn('package', contracts.ready_stages(row))
        row['stages']['package'] = {'state': 'complete'}
        self.assertNotIn('rag_body', contracts.ready_stages(row))
        row['stages']['rag_body'] = {'state': 'verified'}
        row['attachments']['items']['attachment-1']['attachment']['state'] = 'passed'
        self.assertNotIn('rag_attachment', contracts.ready_stages(row))
        row['attachments']['items']['attachment-1']['rag_attachment']['state'] = 'verified'
        row['stages'].update(archive={'state': 'verified'}, cleanup={'state': 'verified'},
                             finish={'state': 'verified'})
        self.assertNotEqual(contracts.paper_status(row), 'completed')

    def test_confirmed_body_proceeds_before_whole_receipt_verified(self):
        row = evidence()
        row['whole_receipt_verified'] = False
        row['stages']['intake']['state'] = 'complete'
        self.assertIn('prepare', contracts.ready_stages(row))
        row['stages'].update(profile={'state': 'passed'}, rag_body={'state': 'verified'})
        row['attachments']['items']['attachment-1']['attachment']['state'] = 'passed'
        row['attachments']['items']['attachment-1']['rag_attachment']['state'] = 'verified'
        self.assertNotIn('archive', contracts.ready_stages(row))
        row['stages'].update(archive={'state': 'verified'}, cleanup={'state': 'verified'},
                             finish={'state': 'verified'})
        self.assertNotEqual(contracts.paper_status(row), 'completed')

    def test_attachment_failure_does_not_block_body(self):
        row = evidence()
        row['attachments']['items']['attachment-1']['attachment']['state'] = 'failed'
        self.assertIn('prepare', contracts.ready_stages(row))
        row['stages']['prepare'] = {'state': 'complete'}
        self.assertIn('profile', contracts.ready_stages(row))
        row['stages']['profile'] = {'state': 'passed'}
        self.assertIn('package', contracts.ready_stages(row))
        row['stages']['package'] = {'state': 'complete'}
        self.assertIn('rag_body', contracts.ready_stages(row))
        self.assertNotIn('archive', contracts.ready_stages(row))

    def test_empty_attachment_inventory_has_explicit_skip(self):
        row = evidence()
        row['attachments']['items'] = {}
        row['stages'].update(profile={'state': 'passed'}, rag_body={'state': 'verified'})
        self.assertIn('attachment', contracts.ready_stages(row))
        self.assertNotIn('archive', contracts.ready_stages(row))
        row['stages']['attachment'] = {'state': 'skipped', 'reason': 'empty_inventory'}
        self.assertIn('rag_attachment', contracts.ready_stages(row))
        row['stages']['rag_attachment'] = {'state': 'skipped', 'reason': 'empty_inventory'}
        self.assertIn('archive', contracts.ready_stages(row))
        self.assertEqual(contracts.paper_status(row), 'rag_verified')

    def test_unknown_inventory_cannot_mean_no_attachments(self):
        row = evidence()
        row['attachments'] = {'inventory_state': 'unknown', 'items': {}}
        row['stages'].update(profile={'state': 'passed'}, rag_body={'state': 'verified'})
        self.assertNotIn('attachment', contracts.ready_stages(row))
        self.assertNotIn('archive', contracts.ready_stages(row))
        self.assertNotEqual(contracts.paper_status(row), 'rag_verified')

    def test_submission_complete_is_not_paper_complete(self):
        self.assertEqual(contracts.paper_status(evidence()), 'submitted')
        row = evidence()
        row['stages']['finish'] = {'state': 'verified'}
        self.assertNotEqual(contracts.paper_status(row), 'completed')

    def test_attachment_fanout_waits_only_its_own_paper_and_item(self):
        row = evidence()
        row['attachments']['items']['attachment-1']['attachment']['state'] = 'passed'
        row['attachments']['items']['attachment-2'] = {
            'required': True, 'attachment': {'state': 'failed'},
            'rag_attachment': {'state': 'pending'}}
        self.assertNotIn('rag_attachment', contracts.ready_stages(row))
        row['stages']['rag_body'] = {'state': 'verified'}
        self.assertIn('rag_attachment', contracts.ready_stages(row))
        row['other_papers'] = {'paper-2': {'state': 'failed'}}
        row['other_batches'] = {'batch-2': {'state': 'running'}}
        self.assertIn('rag_attachment', contracts.ready_stages(row))

    def test_sent_unknown_failed_and_running_are_never_normal_resubmitted(self):
        for state in ('submitted', 'unknown', 'running', 'failed', 'retryable_failed',
                      'needs_attention', 'cancelled'):
            with self.subTest(state=state):
                row = evidence()
                row['stages']['prepare'] = {'state': state}
                self.assertNotIn('prepare', contracts.ready_stages(row))
        row = evidence()
        row['stages']['prepare'] = {'state': 'pending'}
        self.assertIn('prepare', contracts.ready_stages(row))

    def test_empty_skip_cannot_override_nonempty_required_inventory(self):
        row = evidence()
        row['stages'].update(profile={'state': 'passed'}, rag_body={'state': 'verified'},
            attachment={'state': 'skipped', 'reason': 'empty_inventory'},
            rag_attachment={'state': 'skipped', 'reason': 'empty_inventory'})
        self.assertNotIn('archive', contracts.ready_stages(row))

    def test_llm_rag_nas_and_whole_paper_are_different_milestones(self):
        row = evidence()
        row['stages']['profile'] = {'state': 'passed'}
        row['attachments']['items']['attachment-1']['attachment']['state'] = 'passed'
        self.assertEqual(contracts.paper_status(row), 'llm_passed')
        row['stages']['rag_body'] = {'state': 'submitted'}
        self.assertEqual(contracts.paper_status(row), 'llm_passed')
        row['stages']['rag_body'] = {'state': 'verified'}
        row['attachments']['items']['attachment-1']['rag_attachment']['state'] = 'verified'
        self.assertEqual(contracts.paper_status(row), 'rag_verified')
        self.assertIn('archive', contracts.ready_stages(row))
        row['stages']['archive'] = {'state': 'complete'}
        self.assertNotIn('cleanup', contracts.ready_stages(row))
        row['stages']['archive'] = {'state': 'verified'}
        self.assertEqual(contracts.paper_status(row), 'nas_verified')
        self.assertIn('cleanup', contracts.ready_stages(row))
        row['stages']['cleanup'] = {'state': 'verified'}
        self.assertIn('finish', contracts.ready_stages(row))
        row['stages']['finish'] = {'state': 'verified'}
        self.assertEqual(contracts.paper_status(row), 'completed')
        self.assertNotIn('finish', contracts.ready_stages(row))


class IdentityTests(unittest.TestCase):
    def test_same_source_contract_reused_across_batch_name_paths_and_scheduler(self):
        original = identity_inputs()
        changed = deepcopy(original)
        changed.update(batch_id='batch-2', display_name='new title', dispatch_seq=7,
                       dispatch_kind='recovery', scheduler_version='next', worker_pid=123)
        changed['input_refs'][0]['path'] = 'other/path/body.md'
        self.assertRegex(contracts.operation_key(original), r'^op-[0-9a-f]{64}$')
        self.assertEqual(contracts.operation_key(original), contracts.operation_key(changed))

    def test_source_stage_contract_part_and_result_hash_change_identity(self):
        original = identity_inputs()
        for field, value in [('source_sha256', 'c' * 64), ('stage', 'attachment'),
                             ('contract_version', 'body-profile/2'), ('part_id', 'part-2'),
                             ('item_id', 'attachment-2'), ('paper_id', 'paper-2')]:
            with self.subTest(field=field):
                changed = deepcopy(original)
                changed[field] = value
                self.assertNotEqual(contracts.operation_key(original), contracts.operation_key(changed))
        changed = deepcopy(original)
        changed['input_refs'][0]['sha256'] = 'd' * 64
        self.assertNotEqual(contracts.operation_key(original), contracts.operation_key(changed))

    def test_adding_attachment_does_not_invalidate_passed_body(self):
        original = identity_inputs()
        changed = dict(original, attachments=[{'item_id': 'new', 'sha256': 'c' * 64}],
                       receipt_ref={'path': 'new-receipt.json', 'sha256': 'd' * 64})
        self.assertRegex(contracts.operation_key(changed), r'^op-[0-9a-f]{64}$')
        self.assertEqual(contracts.operation_key(original), contracts.operation_key(changed))


class NamingTests(unittest.TestCase):
    def test_title_names_preserve_original_mapping_and_number_collisions(self):
        result = contracts.delivery_names('paper-1', '论文题目', '张三.pdf', [
            {'item_id': 'a', 'original_name': '数据.xlsx'},
            {'item_id': 'b', 'original_name': '数据.xlsx'}], title_confirmed=True)
        self.assertEqual(result['display_title'], '论文题目')
        self.assertEqual(result['body']['delivery_name'], '论文题目.pdf')
        self.assertEqual(result['body']['original_name'], '张三.pdf')
        self.assertEqual(result['attachments']['a']['delivery_name'], '论文题目_数据.xlsx')
        self.assertEqual(result['attachments']['b']['delivery_name'], '论文题目_数据_2.xlsx')
        self.assertEqual(result['attachments']['b']['original_name'], '数据.xlsx')

    def test_unconfirmed_title_uses_internal_id_not_person_name(self):
        result = contracts.delivery_names('paper-17', '张三', '张三.pdf', [], title_confirmed=False)
        self.assertEqual(result['display_title'], '待补题目')
        self.assertEqual(result['body']['delivery_name'], 'paper-17.pdf')

    def test_windows_names_invalid_reserved_case_collisions(self):
        for title, expected in [('CON', '_CON.pdf'), ('a:b?c. ', 'a_b_c.pdf'),
                                ('COM1.txt', '_COM1.txt.pdf')]:
            with self.subTest(title=title):
                result = contracts.delivery_names('paper-1', title, 'original.pdf', [],
                                                 title_confirmed=True)
                self.assertEqual(result['body']['delivery_name'], expected)
        result = contracts.delivery_names('paper-1', 'Thesis', 'original.PDF', [],
                                         title_confirmed=True, existing_names=['THESIS.pdf'])
        self.assertEqual(result['body']['delivery_name'], 'Thesis_2.PDF')


class SerializationTests(unittest.TestCase):
    def envelope(self):
        self.assertTrue(hasattr(contracts, 'StageEnvelope'), 'missing serializable stage contract')
        return contracts.StageEnvelope(
            batch_id='batch-1', paper_id='paper-1',
            receipt_ref={'path': 'receipts/1.json', 'sha256': 'a' * 64},
            operation_key=contracts.operation_key(identity_inputs()), stage='profile',
            item_id=None, part_id='part-1',
            input_refs=[{'path': 'cache/body.md', 'sha256': 'b' * 64}],
            contract_version='body-profile/1',
            control_generation={'global': 1, 'batch': 2, 'runtime': 3},
            dispatch_seq=0, dispatch_kind='normal')

    def test_envelope_survives_json_process_boundary_with_typed_refs(self):
        envelope = self.envelope()
        payload = json.loads(json.dumps(envelope.to_dict()))
        restored = contracts.StageEnvelope.from_dict(payload)
        self.assertEqual(restored.receipt_ref.path, 'receipts/1.json')
        self.assertEqual(restored.input_refs[0].sha256, 'b' * 64)
        self.assertEqual(restored.control_generation, {'global': 1, 'batch': 2, 'runtime': 3})
        self.assertEqual(restored.to_dict(), payload)
        self.assertNotIn('body', payload)
        self.assertNotIn('credentials', payload)

    def test_separate_python_process_reads_envelope_and_returns_stage_result(self):
        payload = self.envelope().to_dict()
        completed = subprocess.run([sys.executable, '-c',
            'import json,sys; '
            'from paperflow_prefect.pipeline_contracts import StageEnvelope,StageResult; '
            'e=StageEnvelope.from_dict(json.load(sys.stdin)); '
            'r=StageResult(e.operation_key,"passed",e.input_refs,["package"]); '
            'print(json.dumps(r.to_dict()))'],
            cwd=Path(__file__).resolve().parents[1], input=json.dumps(payload),
            text=True, capture_output=True, check=True)
        result = json.loads(completed.stdout)
        self.assertEqual(result['operation_key'], payload['operation_key'])
        self.assertEqual(result['evidence_refs'], [{'path': 'cache/body.md', 'sha256': 'b' * 64}])
        self.assertEqual(result['next_stages'], ['package'])

    def test_recovery_retains_original_business_stage_and_operation(self):
        original = self.envelope().to_dict()
        recovered = dict(original, dispatch_kind='recovery', dispatch_seq=1)
        restored = contracts.StageEnvelope.from_dict(recovered)
        self.assertEqual(restored.stage, 'profile')
        self.assertEqual(restored.operation_key, original['operation_key'])
        with self.assertRaises(ValueError):
            contracts.StageEnvelope.from_dict(dict(original, stage='model_recovery'))
        with self.assertRaises(ValueError):
            contracts.StageEnvelope.from_dict(dict(recovered, stage='package'))

    def test_invalid_hash_generation_and_dispatch_rejected_before_dispatch(self):
        original = self.envelope().to_dict()
        for patch in [
            {'control_generation': {'global': 1, 'batch': 2}},
            {'control_generation': {'global': 1, 'batch': 2, 'runtime': -1}},
            {'control_generation': {'global': True, 'batch': 2, 'runtime': 3}},
            {'dispatch_seq': -1}, {'dispatch_seq': True}, {'dispatch_kind': 'other'},
            {'receipt_ref': {'path': 'x', 'sha256': 'invalid'}},
            {'input_refs': [{'path': 'x', 'sha256': 'invalid'}]},
            {'operation_key': 'path/unsafe'}, {'item_id': ''},
        ]:
            with self.subTest(patch=patch), self.assertRaises(ValueError):
                contracts.StageEnvelope.from_dict(dict(original, **patch))
        with self.assertRaises(TypeError):
            contracts.StageEnvelope.from_dict(dict(original, body_text='do not persist'))

    def test_stage_result_is_lightweight_and_does_not_create_recovery_stage(self):
        self.assertTrue(hasattr(contracts, 'StageResult'), 'missing lightweight result contract')
        result = contracts.StageResult(contracts.operation_key(identity_inputs()), 'passed',
            [{'path': 'results/profile.json', 'sha256': 'c' * 64}], ['package'])
        restored = contracts.StageResult.from_dict(json.loads(json.dumps(result.to_dict())))
        self.assertEqual(restored.evidence_refs[0].path, 'results/profile.json')
        self.assertEqual(tuple(restored.next_stages), ('package',))
        with self.assertRaises(ValueError):
            contracts.StageResult(result.operation_key, 'invented', [], [])
        with self.assertRaises(ValueError):
            contracts.StageResult(result.operation_key, 'passed', [], ['model_recovery'])

    def test_identity_rejects_missing_business_inputs_and_normalizes_hex(self):
        original = identity_inputs()
        changed = deepcopy(original)
        changed['source_sha256'] = 'A' * 64
        self.assertEqual(contracts.operation_key(original), contracts.operation_key(changed))
        for patch in [{'source_sha256': 'not-a-hash'}, {'contract_version': ''},
                      {'stage': 'model_recovery'}, {'paper_id': ''}]:
            with self.subTest(patch=patch), self.assertRaises(ValueError):
                contracts.operation_key(dict(original, **patch))


class EvidenceValidationTests(unittest.TestCase):
    def test_nonboolean_required_cannot_bypass_archive_or_paper_completion(self):
        for required in (None, 0, ''):
            for consumer in (contracts.ready_stages, contracts.paper_status):
                with self.subTest(required=required, consumer=consumer.__name__):
                    row = evidence()
                    row['stages'].update(profile={'state': 'passed'}, rag_body={'state': 'verified'})
                    item = row['attachments']['items']['attachment-1']
                    item['required'] = required
                    item['attachment']['state'] = 'failed'
                    if consumer is contracts.paper_status:
                        row['stages'].update(archive={'state': 'verified'},
                            cleanup={'state': 'verified'}, finish={'state': 'verified'})
                    with self.assertRaises(ValueError):
                        consumer(row)

    def test_missing_true_and_false_required_have_explicit_dependency_semantics(self):
        for required, can_complete in (('missing', False), (True, False), (False, True)):
            with self.subTest(required=required):
                row = evidence()
                row['stages'].update(profile={'state': 'passed'}, rag_body={'state': 'verified'})
                item = row['attachments']['items']['attachment-1']
                if required == 'missing':
                    del item['required']
                else:
                    item['required'] = required
                item['attachment']['state'] = 'failed'
                self.assertEqual('archive' in contracts.ready_stages(row), can_complete)
                row['stages'].update(archive={'state': 'verified'}, cleanup={'state': 'verified'},
                                     finish={'state': 'verified'})
                self.assertEqual(contracts.paper_status(row),
                                 'completed' if can_complete else 'needs_attention')

    def test_foreign_paper_or_batch_cannot_supply_a_dependency(self):
        for foreign in [{'paper_id': 'paper-2'}, {'batch_id': 'batch-2'}]:
            with self.subTest(foreign=foreign):
                row = evidence()
                row['stages']['prepare'] = dict(state='complete', **foreign)
                with self.assertRaises(ValueError):
                    contracts.ready_stages(row)

    def test_invented_state_cannot_silently_block_work(self):
        row = evidence()
        row['stages']['prepare'] = {'state': 'complte'}
        with self.assertRaises(ValueError):
            contracts.ready_stages(row)

    def test_input_snapshot_is_not_mutated(self):
        row = evidence()
        prior = deepcopy(row)
        contracts.ready_stages(row)
        contracts.paper_status(row)
        self.assertEqual(row, prior)


if __name__ == '__main__':
    unittest.main()
