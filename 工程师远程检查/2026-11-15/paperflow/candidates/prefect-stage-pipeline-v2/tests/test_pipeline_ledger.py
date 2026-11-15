"""Behavioral crash/race tests. Every 'send' is a local counter; no HTTP."""
from dataclasses import replace
import hashlib
import multiprocessing as mp
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import psutil
from paperflow_prefect.pipeline_contracts import ArtifactRef, StageEnvelope, StageResult, operation_key
from paperflow_prefect.pipeline_ledger import (StageLedger, OperationBusy, StaleDispatch,
    EvidenceChanged, SendBudgetExceeded, SendAlreadyReserved, OutcomeUnknown)
from paperflow_prefect.records import read_json, write_json


def owner(task='task-1'):
    return {'instance_id': 'worker-test', 'pid': os.getpid(),
            'process_create_time': psutil.Process().create_time(), 'task_run_id': task}


def envelope(root, part=None, seq=0, kind='normal'):
    source = Path(root) / 'input.txt'
    if not source.exists(): source.write_bytes(b'fixture-input')
    ref = ArtifactRef(str(source.resolve()), hashlib.sha256(source.read_bytes()).hexdigest())
    key = operation_key({'paper_id': 'paper-1', 'source_sha256': ref.sha256,
        'stage': 'profile', 'part_id': part, 'input_refs': [ref], 'contract_version': 'v2'})
    return StageEnvelope('batch-1', 'paper-1', ref, key, 'profile', None, part,
        (ref,), 'v2', {'global': 0, 'batch': 0, 'runtime': 0}, seq, kind)


def result(env):
    return StageResult(env.operation_key, 'passed', env.input_refs, ('package',))


def competing_send(root, env_dict, ready, start, output):
    env = StageEnvelope.from_dict(env_dict)
    ledger = StageLedger(Path(root) / 'ledger')
    ready.put(True); start.wait(10)
    try:
        with ledger.claim(env, owner(str(os.getpid()))) as claim:
            if claim['execute']:
                ledger.reserve_send(env.paper_id, env.item_id, env.part_id, 'a'*64, False)
                with (Path(root) / 'sends.txt').open('a') as stream: stream.write('send\n')
                ledger.commit_result(env, result(env))
                output.put('sent')
            else: output.put('cached')
    except OperationBusy: output.put('busy')


def budget_racer(root, ready, start, output, part):
    ledger = StageLedger(Path(root))
    ready.put(True); start.wait(10)
    try:
        reservation = ledger.reserve_send('paper-1', None, part, 'a'*64, True)
        output.put(reservation['reservation_id'])
    except SendBudgetExceeded: output.put('exhausted')


def crash_during_replace(path, reached):
    import paperflow_prefect.records as records
    def hold_replace(src, dst):
        reached.set()
        # Parent terminates us while the flushed temp file exists.
        mp.Event().wait(30)
    records.os.replace = hold_replace
    records.write_json(path, {'version': 'new', 'payload': 'y'*100000})


def crash_recovery_preflight(root, env_dict):
    ledger = StageLedger(Path(root))
    with ledger.claim(StageEnvelope.from_dict(env_dict), owner('preflight-crash')):
        os._exit(24)


def crash_recovery_reservation(root):
    StageLedger(Path(root)).reserve_send('paper-1', None, 'p1', 'a'*64, True)
    os._exit(23)


def crash_after_reservation(root, env_dict):
    env = StageEnvelope.from_dict(env_dict)
    ledger = StageLedger(Path(root))
    with ledger.claim(env, owner()) as claim:
        ledger.reserve_send(env.paper_id, None, None, 'a'*64, False)
        os._exit(19)


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.ledger = StageLedger(self.root / 'ledger')
        self.env = envelope(self.root)

    def test_two_processes_perform_only_one_actual_send(self):
        ctx = mp.get_context('spawn'); ready = ctx.Queue(); output = ctx.Queue(); start = ctx.Event()
        children = [ctx.Process(target=competing_send, args=(str(self.root), self.env.to_dict(), ready, start, output)) for _ in range(2)]
        for child in children: child.start()
        for _ in children: self.assertTrue(ready.get(timeout=15))
        start.set()
        for child in children: child.join(20); self.assertEqual(child.exitcode, 0)
        statuses = [output.get(timeout=5) for _ in children]
        self.assertEqual((self.root/'sends.txt').read_text().splitlines(), ['send'])
        self.assertEqual(statuses.count('sent'), 1)
        print('MULTIPROCESS_SEND_EVIDENCE actual_send_count=1 contenders=2 outcomes=' + ','.join(sorted(statuses)))

    def test_duplicate_verified_result_makes_zero_model_calls(self):
        with self.ledger.claim(self.env, owner()): self.ledger.commit_result(self.env, result(self.env))
        calls = 0
        with StageLedger(self.root/'ledger').claim(self.env, owner('new-task')) as claim:
            if claim['execute']: calls += 1
            self.assertEqual(claim['result'], result(self.env).to_dict())
        self.assertEqual(calls, 0)

    def test_cached_artifact_is_rehashed(self):
        with self.ledger.claim(self.env, owner()): self.ledger.commit_result(self.env, result(self.env))
        Path(self.env.input_refs[0].path).write_bytes(b'changed')
        with self.assertRaises(EvidenceChanged):
            with self.ledger.claim(self.env, owner('new-task')): pass

    def test_commit_requires_matching_identity_and_owned_claim(self):
        with self.assertRaises(OperationBusy): self.ledger.commit_result(self.env, result(self.env))
        with self.ledger.claim(self.env, owner()):
            with self.assertRaises(EvidenceChanged):
                self.ledger.commit_result(self.env, replace(result(self.env), operation_key='op-'+'b'*64))

    def test_changed_result_evidence_cannot_commit(self):
        with self.ledger.claim(self.env, owner()):
            bad = replace(result(self.env), evidence_refs=(ArtifactRef(self.env.input_refs[0].path, 'b'*64),))
            with self.assertRaises(EvidenceChanged): self.ledger.commit_result(self.env, bad)

    def test_success_without_evidence_is_not_accepted(self):
        with self.ledger.claim(self.env, owner()):
            with self.assertRaises(EvidenceChanged):
                self.ledger.commit_result(self.env, replace(result(self.env), evidence_refs=()))

    def test_stale_dispatch_exits_before_send(self):
        self.ledger.set_dispatch(replace(self.env, dispatch_seq=2))
        with self.assertRaises(StaleDispatch):
            with self.ledger.claim(self.env, owner()): self.fail('stale task may not send')
        with self.ledger.claim(replace(self.env, dispatch_seq=2), owner()): pass

    def test_owner_contains_verified_process_and_worker_identity(self):
        with self.ledger.claim(self.env, owner()) as claim:
            self.assertTrue(claim['execute'])
            record = self.ledger.read_operation(self.env.operation_key)
            self.assertEqual(record['owner'], owner())
        bad_owner = {**owner(), 'process_create_time': 1}
        with self.assertRaises(ValueError):
            with self.ledger.claim(self.env, bad_owner): pass

    def test_other_operations_run_while_one_stage_is_claimed(self):
        other = envelope(self.root, 'part-2')
        with self.ledger.claim(self.env, owner()):
            with StageLedger(self.root/'ledger').claim(other, owner('second')) as claimed:
                self.assertTrue(claimed['execute'])

    def test_normal_request_cannot_be_reserved_twice(self):
        self.ledger.reserve_send('paper-1', None, None, 'a'*64, False)
        with self.assertRaises(SendAlreadyReserved): self.ledger.reserve_send('paper-1', None, None, 'a'*64, False)

    def test_budget_survives_restarts_and_is_shared_across_parts(self):
        first = self.ledger.reserve_send('paper-1', None, 'part-1', 'a'*64, True)
        self.assertEqual(first['additional_number'], 1)
        second = StageLedger(self.root/'ledger').reserve_send('paper-1', 'attachment-1', None, 'b'*64, True)
        self.assertEqual(second['additional_number'], 2)
        with self.assertRaises(SendBudgetExceeded):
            StageLedger(self.root/'ledger').reserve_send('paper-1', None, 'new-part', 'c'*64, True)

    def test_crashed_reservation_consumes_budget(self):
        ctx = mp.get_context('spawn')
        child = ctx.Process(target=crash_recovery_reservation, args=(str(self.root/'ledger'),))
        child.start(); child.join(20); self.assertEqual(child.exitcode, 23)
        self.ledger.reserve_send('paper-1', None, 'p2', 'b'*64, True)
        self.assertEqual(StageLedger(self.root/'ledger').send_budget('paper-1')['additional_used'], 2)

    def test_budget_race_allows_at_most_two_additional_sends(self):
        ctx = mp.get_context('spawn'); ready = ctx.Queue(); output = ctx.Queue(); start = ctx.Event()
        children = [ctx.Process(target=budget_racer, args=(str(self.root/'ledger'), ready, start, output, 'part-'+str(i))) for i in range(5)]
        for child in children: child.start()
        for _ in children: ready.get(timeout=15)
        start.set()
        for child in children: child.join(20); self.assertEqual(child.exitcode, 0)
        rows = [output.get(timeout=5) for _ in children]
        self.assertEqual(rows.count('exhausted'), 3)
        self.assertEqual(self.ledger.send_budget('paper-1')['additional_used'], 2)

    def test_crashed_sent_claim_is_unknown_and_never_automatically_resends(self):
        ctx = mp.get_context('spawn')
        child = ctx.Process(target=crash_after_reservation, args=(str(self.root/'ledger'), self.env.to_dict()))
        child.start(); child.join(20); self.assertEqual(child.exitcode, 19)
        with self.assertRaises(OutcomeUnknown):
            with self.ledger.claim(self.env, owner('replacement')): self.fail('unknown send replay')
        self.assertEqual(self.ledger.read_operation(self.env.operation_key)['state'], 'unknown')

    def test_interrupted_atomic_write_never_exposes_partial_json(self):
        path = self.root/'atomic.json'; old = {'version': 'old', 'payload': 'x'*100000}
        write_json(path, old)
        ctx = mp.get_context('spawn'); reached = ctx.Event()
        child = ctx.Process(target=crash_during_replace, args=(str(path), reached))
        child.start(); self.assertTrue(reached.wait(15))
        try:
            for _ in range(30): self.assertEqual(read_json(path), old)
        finally: child.terminate(); child.join(10)
        self.assertEqual(read_json(path), old)
        write_json(path, {'version': 'new'})
        self.assertEqual(read_json(path), {'version': 'new'})

    def test_failed_atomic_replace_preserves_old_json_and_removes_temporary(self):
        path = self.root/'atomic.json'; write_json(path, {'old': True})
        with patch('paperflow_prefect.records.os.replace', side_effect=OSError('test failure')):
            with self.assertRaises(OSError): write_json(path, {'new': True})
        self.assertEqual(read_json(path), {'old': True})
        self.assertEqual(list(self.root.glob('atomic.json.*.tmp')), [])

    def reconciliation(self, state, **changes):
        reservation = self.ledger.send_budget('paper-1')['reservations'][-1]
        proof = {'schema': 'pipeline-reconciliation/1', 'operation_key': self.env.operation_key,
            'paper_id': self.env.paper_id, 'item_id': None, 'part_id': None,
            'reservation_id': reservation['reservation_id'], 'request_sha256': reservation['request_sha256'],
            'outcome': state}
        proof.update(changes)
        path = self.root/'reconciliation.json'; write_json(path, proof)
        return replace(result(self.env), state=state,
            evidence_refs=(ArtifactRef(str(path.resolve()), hashlib.sha256(path.read_bytes()).hexdigest()),))

    def test_wrong_reconciliation_identity_preserves_unknown(self):
        with self.ledger.claim(self.env, owner()):
            self.ledger.reserve_send('paper-1', None, None, 'a'*64, False)
        for changes in ({'operation_key': 'op-'+'b'*64}, {'reservation_id': 'other'},
                        {'request_sha256': 'b'*64}, {'outcome': 'passed'}):
            with self.subTest(changes=changes):
                proof = self.reconciliation('retryable_failed', **changes)
                with self.assertRaises(EvidenceChanged): self.ledger.reconcile_result(self.env, proof, owner('reconciler'))
                self.assertEqual(self.ledger.read_operation(self.env.operation_key)['state'], 'unknown')

    def test_reconciliation_records_known_failure_without_sending(self):
        with self.ledger.claim(self.env, owner()):
            self.ledger.reserve_send('paper-1', None, None, 'a'*64, False)
        recovery = replace(self.env, dispatch_seq=1, dispatch_kind='recovery')
        self.ledger.set_dispatch(recovery)
        with self.assertRaises(OutcomeUnknown):
            with self.ledger.claim(recovery, owner('replacement')): pass
        failed = self.reconciliation('retryable_failed')
        self.ledger.reconcile_result(recovery, failed, owner('reconciler'))
        history = self.ledger.read_operation(self.env.operation_key)['reconciliations']
        self.assertEqual(history[-1]['previous_state'], 'unknown')
        with self.ledger.claim(recovery, owner('replacement')) as claimed:
            self.assertTrue(claimed['execute'])
            reservation = self.ledger.reserve_send('paper-1', None, None, 'a'*64, True)
            self.assertEqual(reservation['additional_number'], 1)
            self.ledger.commit_result(recovery, result(recovery))

    def test_reconciliation_requires_hashed_evidence(self):
        with self.ledger.claim(self.env, owner()):
            self.ledger.reserve_send('paper-1', None, None, 'a'*64, False)
        with self.assertRaises(EvidenceChanged):
            self.ledger.reconcile_result(self.env, replace(result(self.env), state='retryable_failed', evidence_refs=()), owner('reconciler'))

    def test_recovery_that_exits_before_reservation_retains_known_failure(self):
        with self.ledger.claim(self.env, owner()):
            self.ledger.reserve_send('paper-1', None, None, 'a'*64, False)
            self.ledger.commit_result(self.env, replace(result(self.env), state='retryable_failed'))
        recovery = replace(self.env, dispatch_seq=1, dispatch_kind='recovery')
        self.ledger.set_dispatch(recovery)
        with self.ledger.claim(recovery, owner('recovery-preflight')): pass
        self.assertEqual(self.ledger.read_operation(self.env.operation_key)['state'], 'retryable_failed')
        self.assertEqual(self.ledger.send_budget('paper-1')['additional_used'], 0)
        with self.ledger.claim(recovery, owner('recovery-next')): pass

    def test_recovery_crash_before_new_reservation_preserves_known_failure(self):
        with self.ledger.claim(self.env, owner()):
            self.ledger.reserve_send('paper-1', None, None, 'a'*64, False)
            self.ledger.commit_result(self.env, replace(result(self.env), state='retryable_failed'))
        recovery = replace(self.env, dispatch_seq=1, dispatch_kind='recovery')
        self.ledger.set_dispatch(recovery)
        ctx = mp.get_context('spawn')
        child = ctx.Process(target=crash_recovery_preflight, args=(str(self.root/'ledger'), recovery.to_dict()))
        child.start(); child.join(20); self.assertEqual(child.exitcode, 24)
        with self.ledger.claim(recovery, owner('replacement')) as claim:
            self.assertTrue(claim['execute'])
        self.assertEqual(self.ledger.send_budget('paper-1')['additional_used'], 0)

    def test_different_request_hash_cannot_reset_spent_recovery_budget(self):
        self.ledger.reserve_send('paper-1', None, None, 'a'*64, True)
        self.ledger.reserve_send('paper-1', None, None, 'b'*64, True)
        with self.assertRaises(SendBudgetExceeded): self.ledger.reserve_send('paper-1', None, None, 'c'*64, True)

    def test_budget_rechecks_new_legacy_revision_after_registration(self):
        work = self.legacy()
        self.ledger.inherit_legacy_retries('paper-1', [work])
        write_json(work/'revisions/late/revision.json', {
            'schema': 'preextract-authorized-revision/1', 'parent': str(work.resolve()), 'mode': 'rerun'})
        with self.assertRaises(SendBudgetExceeded): self.ledger.reserve_send('paper-1', None, None, 'a'*64, True)

    def legacy(self, name='legacy'):
        work = self.root/name; work.mkdir()
        write_json(work/'private/profile-recovery/retry-intent.json', {
            'schema': 'profile-engineering-attempt/1', 'root_work': str(work.resolve()),
            'attempt': 1, 'max_additional_attempts': 1, 'parent_request_sha256': 'a'*64,
            'source_sha256': 'd'*64,
            'parent_hashes': {'material.json': 'b'*64, 'model/request.json': 'c'*64}})
        write_json(work/'revisions/profile-recovery-1/revision.json', {
            'schema': 'preextract-authorized-revision/1', 'parent': str(work.resolve()),
            'mode': 'rerun', 'recover_failed': True, 'source_sha256': 'd'*64,
            'parent_hashes': {'material.json': 'b'*64, 'model/request.json': 'c'*64}})
        return work

    def test_legacy_intent_and_linked_revision_charge_once_idempotently(self):
        work = self.legacy()
        self.ledger.inherit_legacy_retries('paper-1', [work])
        self.ledger.inherit_legacy_retries('paper-1', [work])
        self.assertEqual(self.ledger.send_budget('paper-1')['additional_used'], 1)
        self.ledger.reserve_send('paper-1', None, 'p2', 'b'*64, True)
        with self.assertRaises(SendBudgetExceeded): self.ledger.reserve_send('paper-1', None, 'p3', 'c'*64, True)

    def test_same_named_unrelated_rerun_cannot_hide_an_additional_send(self):
        work = self.legacy()
        path = work/'revisions/profile-recovery-1/revision.json'
        unrelated = read_json(path)
        unrelated.update(recover_failed=False, parent_hashes={'model/request.json': 'f'*64})
        write_json(path, unrelated)
        with self.assertRaises(EvidenceChanged): self.ledger.inherit_legacy_retries('paper-1', [work])
        with self.assertRaises(EvidenceChanged): self.ledger.reserve_send('paper-1', None, None, 'a'*64, True)

    def test_missing_or_wrong_revision_linkage_cannot_deduplicate(self):
        cases = [('recover_failed', None), ('recover_failed', False),
                 ('parent_hashes', None), ('parent_hashes', {}),
                 ('parent_hashes', {'model/request.json': 'e'*64}),
                 ('source_sha256', None), ('source_sha256', 'e'*64),
                 ('refresh_material', True)]
        for i, (field, replacement) in enumerate(cases):
            with self.subTest(field=field, replacement=replacement):
                paper = 'case-'+str(i); work = self.legacy(paper)
                path = work/'revisions/profile-recovery-1/revision.json'
                value = read_json(path)
                if replacement is None: value.pop(field, None)
                else: value[field] = replacement
                write_json(path, value)
                with self.assertRaises(EvidenceChanged): self.ledger.inherit_legacy_retries(paper, [work])
                with self.assertRaises(EvidenceChanged): self.ledger.reserve_send(paper, None, None, 'a'*64, True)

    def test_missing_intent_binding_cannot_make_same_name_revision_linked(self):
        for i, field in enumerate(('source_sha256', 'parent_hashes')):
            with self.subTest(field=field):
                paper = 'missing-intent-'+str(i); work = self.legacy(paper)
                path = work/'private/profile-recovery/retry-intent.json'
                value = read_json(path); value.pop(field)
                write_json(path, value)
                with self.assertRaises(EvidenceChanged): self.ledger.inherit_legacy_retries(paper, [work])
                with self.assertRaises(EvidenceChanged): self.ledger.reserve_send(paper, None, None, 'a'*64, True)

    def test_independent_revision_consumes_another_slot(self):
        work = self.legacy()
        write_json(work/'revisions/other/revision.json', {
            'schema': 'preextract-authorized-revision/1', 'parent': str(work.resolve()), 'mode': 'rerun'})
        self.ledger.inherit_legacy_retries('paper-1', [work])
        with self.assertRaises(SendBudgetExceeded): self.ledger.reserve_send('paper-1', None, None, 'a'*64, True)

    def test_partition_revision_is_charged_and_candidate_import_is_free(self):
        work = self.root/'legacy'; work.mkdir()
        write_json(work/'model/part-1/revisions/recovery-1/revision.json', {
            'id': 'part-1', 'directory': 'part-1/revisions/recovery-1',
            'previous': 'part-1', 'evidence': {}, 'dispatch_started': False})
        write_json(work/'revisions/import/revision.json', {
            'schema': 'preextract-authorized-revision/1', 'parent': str(work.resolve()), 'mode': 'candidate_import'})
        self.ledger.inherit_legacy_retries('paper-1', [work])
        self.assertEqual(self.ledger.send_budget('paper-1')['additional_used'], 1)

    def test_dangling_active_revision_does_not_allow_free_budget(self):
        work = self.root/'legacy'; work.mkdir()
        write_json(work/'active-revision.json', {'schema': 'preextract-active-revision/1',
            'work_dir': str(work/'revisions/deleted'), 'revision_sha256': 'a'*64})
        with self.assertRaises(EvidenceChanged): self.ledger.inherit_legacy_retries('paper-1', [work])

    def test_partition_state_with_missing_revision_blocks_recovery(self):
        work = self.root/'legacy'; work.mkdir()
        write_json(work/'model/partition-state.json', {
            'recoveries': [{'directory': 'part-1/revisions/deleted'}]})
        with self.assertRaises(EvidenceChanged): self.ledger.inherit_legacy_retries('paper-1', [work])

    def test_deleted_inherited_history_cannot_reset_budget(self):
        work = self.legacy(); self.ledger.inherit_legacy_retries('paper-1', [work])
        (work/'private/profile-recovery/retry-intent.json').unlink()
        with self.assertRaises(EvidenceChanged): self.ledger.reserve_send('paper-1', None, None, 'a'*64, True)

    def test_unreadable_legacy_history_fails_closed(self):
        work = self.legacy()
        (work/'private/profile-recovery/retry-intent.json').write_bytes(b'{')
        with self.assertRaises(EvidenceChanged): self.ledger.inherit_legacy_retries('paper-1', [work])
        with self.assertRaises(EvidenceChanged): self.ledger.reserve_send('paper-1', None, None, 'a'*64, True)


if __name__ == '__main__': unittest.main()
