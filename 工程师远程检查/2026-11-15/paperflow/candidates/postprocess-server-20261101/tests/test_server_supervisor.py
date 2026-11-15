"""Synthetic supervision only; no server, business data, or model dispatch.

The controllable scope double represents unavailable privileged kernel scope
operations. POSIX tests additionally exercise real PID/start-time identities and
a real parent exiting while its detached child remains alive.
"""
import copy
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import hashlib
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from uuid import UUID, uuid4, uuid5

from remote_jobs import RemoteJobs, JobConflict, CapacityFull


def make_job(n=1):
    identity = dict(task_id=str(uuid4()), group_id=f'{n:032x}', paper_id='synthetic',
                    receipt_id='a' * 64, attempt_id='synthetic',
                    parent_run_id=str(uuid4()), task_run_id=str(uuid4()))
    return dict(job_id=str(uuid5(UUID('5e9b7732-458e-50cb-8916-6b53007c204f'), identity['task_run_id'])),
                claim_id=str(uuid4()), identity=identity, generation=1,
                manifest_sha256='b' * 64, snapshot_sha256='c' * 64, code_sha256='d' * 64)


class SyntheticScope:
    """A controllable kernel boundary; tests assert the real durable ledger."""
    scopes = {}

    def __init__(self, parent, job_id, *, worker_uid, worker_gid):
        self.name = str(Path(parent) / job_id)
        self.populated = True
        self.identity = dict(boot_id='synthetic-boot', pid=123, start_ticks=456)
        self.error = False
        self.scopes[self.name] = self

    def launch(self, argv, *, cwd=None, env=None):
        self.output = Path(argv[0])
        return copy.deepcopy(self.identity)

    def release_gate(self):
        # A real synthetic side effect catches duplicate business launches.
        with self.output.open('a') as stream:
            stream.write('one execution\n')

    def snapshot(self):
        if self.error:
            raise OSError('scope unavailable')
        return dict(populated=self.populated, boot_id='synthetic-boot', scope_id=[12, 34])

    def leader_alive(self, identity):
        return identity == self.identity

    @classmethod
    def reopen(cls, parent, job_id, *, worker_uid, worker_gid):
        return cls.scopes[str(Path(parent) / job_id)]


class SupervisorTests(unittest.TestCase):
    def setUp(self):
        try:
            self.module = importlib.import_module('server_supervisor')
        except ImportError:
            self.fail('server supervisor has not been implemented')
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = RemoteJobs(self.root / 'ledger.sqlite3', capacity=1)
        self.store.initialize()
        self.item = make_job()
        self.store.reserve(self.item)
        self.clock = [100.0]
        self.monotonic = self.clock
        self.boot = ['synthetic-boot']
        self.patcher = patch.object(self.module, 'LinuxScope', SyntheticScope)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
        self.supervisor = self.create()
        self.supervisor.initialize()

    def create(self, validator=None):
        supervisor = self.module.ServerSupervisor(self.store, cgroup_parent=self.root / 'scopes',
            worker_uid=65534, worker_gid=65534,
            receipt_validator=validator or self.validate_receipt, clock=lambda: self.clock[0],
            monotonic_clock=lambda: self.monotonic[0], boot_id_provider=lambda: self.boot[0])
        return supervisor

    def receipt(self):
        output = self.root / 'artifact.txt'
        output.write_text('synthetic verified bytes')
        return dict(schema='paperflow-postprocessing-receipt/1', identity=copy.deepcopy(self.item['identity']),
                    claim_id=self.item['claim_id'], manifest_sha256=self.item['manifest_sha256'],
                    validation_status='passed', artifacts=[dict(path=str(output),
                    sha256=hashlib.sha256(output.read_bytes()).hexdigest())])

    def validate_receipt(self, item, receipt):
        # This is a real synthetic artifact verifier, not unconditional True.
        return all(hashlib.sha256(Path(ref['path']).read_bytes()).hexdigest() == ref['sha256']
                   for ref in receipt['artifacts']) and bool(receipt['artifacts'])

    def launch(self, timeout=20):
        return self.supervisor.launch(self.item, [str(self.root / 'launches.txt')], timeout_seconds=timeout)

    def scope(self):
        return SyntheticScope.scopes[str(self.root / 'scopes' / self.item['job_id'])]

    def test_missing_actual_receipt_validator_refuses_construction(self):
        with self.assertRaises(ValueError):
            self.module.ServerSupervisor(self.store, cgroup_parent=self.root,
                worker_uid=65534, worker_gid=65534, receipt_validator=None)

    def test_same_claim_launches_once_across_recreated_supervisor(self):
        self.launch()
        second = self.create()
        result = second.launch(self.item, [str(self.root / 'launches.txt')], timeout_seconds=20)
        self.assertFalse(result['started'])
        self.assertEqual((self.root / 'launches.txt').read_text(), 'one execution\n')
        self.assertEqual(self.store.occupied(), 1)

    def test_explicit_stderr_is_forwarded_to_the_launched_scope(self):
        class LoggingScope(SyntheticScope):
            def launch(scope, argv, *, cwd=None, env=None, stderr=None):
                self.assertIsNotNone(stderr)
                stderr.write(b'synthetic early worker failure\n')
                return super().launch(argv, cwd=cwd, env=env)

        output = self.root / 'diagnostic.log'
        with patch.object(self.module, 'LinuxScope', LoggingScope), output.open('wb') as stream:
            result = self.supervisor.launch(self.item, [str(self.root / 'launches.txt')],
                                            timeout_seconds=20, stderr=stream)
        self.assertTrue(result['started'])
        self.assertEqual(output.read_bytes(), b'synthetic early worker failure\n')

    def test_concurrent_submissions_run_the_business_command_once(self):
        with ThreadPoolExecutor(max_workers=12) as pool:
            results = list(pool.map(lambda _: self.launch(), range(12)))
        self.assertEqual(sum(result['started'] for result in results), 1)
        self.assertEqual((self.root / 'launches.txt').read_text(), 'one execution\n')

    def test_failed_scope_creation_records_unknown_and_never_relaunches(self):
        with patch.object(self.module, 'LinuxScope', side_effect=OSError('delegation unavailable')):
            with self.assertRaises(OSError):
                self.launch()
        self.assertEqual(self.store.status(self.item['job_id'])['state'], 'unknown')
        self.assertEqual(self.store.occupied(), 1)
        self.assertFalse(self.launch()['started'])
        self.assertFalse((self.root / 'launches.txt').exists())

    def test_expired_launch_intent_never_opens_business_gate(self):
        original = SyntheticScope.launch
        def slow_launch(scope, *args, **kwargs):
            result = original(scope, *args, **kwargs)
            self.clock[0] = 103.0
            return result
        with patch.object(SyntheticScope, 'launch', slow_launch):
            with self.assertRaises(self.module.SupervisionUnavailable):
                self.launch(timeout=2)
        self.assertFalse((self.root / 'launches.txt').exists())
        self.assertEqual(self.store.status(self.item['job_id'])['state'], 'unknown')
        self.assertEqual(self.store.occupied(), 1)

    def test_cross_process_poll_during_identity_persistence_does_not_abort_launch(self):
        original = SyntheticScope.launch
        observations = []
        def poll_before_identity(scope, *args, **kwargs):
            source = ('import json,sys; from remote_jobs import RemoteJobs; '
                'from server_supervisor import ServerSupervisor; '
                'ledger=RemoteJobs(sys.argv[1],capacity=1,ledger_id=sys.argv[2]); '
                'supervisor=ServerSupervisor(ledger,cgroup_parent=sys.argv[3],'
                'worker_uid=65534,worker_gid=65534,receipt_validator=lambda *args:False,clock=lambda:100.0,'
                'monotonic_clock=lambda:100.0,boot_id_provider=lambda:"synthetic-boot"); '
                'print(json.dumps(supervisor.poll(json.loads(sys.argv[4]))))')
            result = subprocess.run([sys.executable, '-c', source, str(self.store.database),
                self.store.ledger_id, str(self.root / 'scopes'), json.dumps(self.item)],
                capture_output=True, text=True, timeout=10, check=True)
            observations.append(json.loads(result.stdout))
            return original(scope, *args, **kwargs)
        with patch.object(SyntheticScope, 'launch', poll_before_identity):
            try:
                self.launch()
            except self.module.SupervisionUnavailable:
                pass
        self.assertEqual(observations[0]['state'], 'running')
        self.assertIsNone(observations[0]['server_alive'])
        self.assertEqual(observations[0]['detail'], 'launch_preparing')
        self.assertEqual((self.root / 'launches.txt').read_text(), 'one execution\n')

    def test_cross_process_unknown_writer_cannot_commit_between_check_and_gate(self):
        original = SyntheticScope.release_gate
        attempts = []
        def contend_at_gate(scope):
            # The subprocess is a real competing ledger writer, with timeout=0
            # so the interleaving is deterministic and requires no sleep.
            source = ('import sqlite3,sys; db=sqlite3.connect(sys.argv[1],timeout=0);\n'
                'try:\n db.execute("BEGIN IMMEDIATE")\n'
                'except sqlite3.OperationalError:\n print("blocked")\n'
                'else:\n db.execute("UPDATE executor_jobs SET state=\'unknown\' WHERE job_id=?",'
                '(sys.argv[2],)); db.commit(); print("committed_unknown")\n')
            result = subprocess.run([sys.executable, '-c', source, str(self.store.database),
                self.item['job_id']], capture_output=True, text=True, timeout=10, check=True)
            attempts.append(result.stdout.strip())
            original(scope)
        with patch.object(SyntheticScope, 'release_gate', contend_at_gate):
            self.launch()
        self.assertEqual(attempts, ['blocked'])
        self.assertEqual(self.store.status(self.item['job_id'])['state'], 'running')
        self.assertEqual((self.root / 'launches.txt').read_text(), 'one execution\n')

    def test_unknown_committed_before_gate_transaction_prevents_business_execution(self):
        original = SyntheticScope.launch
        def unknown_before_gate(scope, *args, **kwargs):
            source = ('import sys; from remote_jobs import RemoteJobs; '
                'ledger=RemoteJobs(sys.argv[1],capacity=1,ledger_id=sys.argv[2]); '
                'ledger.mark_unknown(sys.argv[3],sys.argv[4])')
            subprocess.run([sys.executable, '-c', source, str(self.store.database),
                self.store.ledger_id, self.item['job_id'], self.item['claim_id']],
                capture_output=True, text=True, timeout=10, check=True)
            return original(scope, *args, **kwargs)
        with patch.object(SyntheticScope, 'launch', unknown_before_gate):
            with self.assertRaises(self.module.SupervisionUnavailable):
                self.launch()
        self.assertFalse((self.root / 'launches.txt').exists())
        self.assertEqual(self.store.status(self.item['job_id'])['state'], 'unknown')
        self.assertFalse(self.launch()['started'])
        self.assertEqual(self.store.occupied(), 1)

    def test_process_identity_survives_supervisor_death_after_gate_before_commit(self):
        # os._exit bypasses exception handling and connection context managers;
        # SQLite itself must roll back the unfinished gate transaction.
        source = ('import json,os,sys; '
            'sys.path.insert(0,sys.argv[5]); '
            'from test_server_supervisor import SyntheticScope; '
            'import server_supervisor; from remote_jobs import RemoteJobs;\n'
            'class CrashAtGate(SyntheticScope):\n'
            ' def release_gate(self):\n'
            '  super().release_gate(); os._exit(99)\n'
            'server_supervisor.LinuxScope=CrashAtGate\n'
            'ledger=RemoteJobs(sys.argv[1],capacity=1,ledger_id=sys.argv[2])\n'
            'supervisor=server_supervisor.ServerSupervisor(ledger,cgroup_parent=sys.argv[3],'
            'worker_uid=65534,worker_gid=65534,receipt_validator=lambda *args:False,clock=lambda:100.0,'
            'monotonic_clock=lambda:100.0,boot_id_provider=lambda:"synthetic-boot")\n'
            'supervisor.launch(json.loads(sys.argv[4]),[sys.argv[6]],timeout_seconds=20)\n')
        result = subprocess.run([sys.executable, '-c', source, str(self.store.database),
            self.store.ledger_id, str(self.root / 'scopes'), json.dumps(self.item),
            str(Path(__file__).parent), str(self.root / 'launches.txt')],
            capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 99, result.stderr)
        self.assertEqual((self.root / 'launches.txt').read_text(), 'one execution\n')
        with closing(self.store._connect()) as db:
            row = db.execute('SELECT process_json,scope_json FROM supervisor_jobs WHERE job_id=?',
                             (self.item['job_id'],)).fetchone()
        self.assertIsNotNone(row['process_json'])
        self.assertEqual(json.loads(row['process_json']),
                         dict(boot_id='synthetic-boot', pid=123, start_ticks=456))
        self.assertIsNotNone(row['scope_json'])
        self.assertEqual(self.store.status(self.item['job_id'])['state'], 'running')
        self.assertEqual(self.store.occupied(), 1)
        self.assertFalse(self.launch()['started'])

    def test_observing_reserved_job_does_not_block_its_first_authorized_launch(self):
        result = self.supervisor.poll(self.item)
        self.assertEqual(result['state'], 'reserved')
        self.assertIsNone(result['server_alive'])
        self.assertEqual(result['detail'], 'not_started')
        self.assertTrue(self.launch()['started'])

    def test_parent_exit_or_valid_receipt_does_not_release_live_descendant(self):
        self.launch()
        receipt = self.receipt()
        result = self.supervisor.poll(self.item, receipt=receipt)
        self.assertEqual(result['state'], 'running')
        self.assertTrue(result['server_alive'])
        self.assertEqual(self.store.occupied(), 1)
        self.scope().populated = False
        result = self.supervisor.poll(self.item, receipt=receipt)
        self.assertEqual(result['state'], 'finished')
        self.assertEqual(self.store.occupied(), 0)

    def test_empty_scope_without_receipt_keeps_slot(self):
        self.launch()
        self.scope().populated = False
        result = self.supervisor.poll(self.item)
        self.assertEqual(result['state'], 'running')
        self.assertFalse(result['server_alive'])
        self.assertEqual(self.store.occupied(), 1)
        with self.assertRaises(CapacityFull):
            self.store.reserve(make_job(2))

    def test_finished_receipt_bytes_survive_supervisor_restart(self):
        self.launch()
        self.scope().populated = False
        receipt = self.receipt()
        self.supervisor.poll(self.item, receipt=receipt)
        held = self.create().held_receipt(self.item)
        self.assertEqual(json.loads(held['receipt_json']), receipt)
        self.assertEqual(hashlib.sha256(held['receipt_json'].encode('utf8')).hexdigest(),
                         self.store.status(self.item['job_id'])['receipt_sha256'])
        self.assertEqual(held['receipt_sha256'], self.store.status(self.item['job_id'])['receipt_sha256'])

    def test_unfinished_job_has_no_delivery_receipt(self):
        self.launch()
        self.assertIsNone(self.supervisor.held_receipt(self.item))

    def test_digest_string_is_not_a_verified_receipt(self):
        self.launch()
        self.scope().populated = False
        with self.assertRaises(JobConflict):
            self.supervisor.poll(self.item, receipt='f' * 64)
        self.assertEqual(self.store.occupied(), 1)

    def test_invalid_artifact_or_different_identity_cannot_finish(self):
        self.launch()
        self.scope().populated = False
        receipt = self.receipt()
        Path(receipt['artifacts'][0]['path']).write_text('tampered')
        with self.assertRaises(JobConflict):
            self.supervisor.poll(self.item, receipt=receipt)
        receipt = self.receipt()
        receipt['claim_id'] = str(uuid4())
        with self.assertRaises(JobConflict):
            self.supervisor.poll(self.item, receipt=receipt)
        self.assertEqual(self.store.occupied(), 1)

    def test_timeout_remains_unknown_even_after_late_receipt_and_empty_scope(self):
        self.launch(timeout=2)
        self.clock[0] = 103.0
        result = self.supervisor.poll(self.item)
        self.assertEqual(result['state'], 'unknown')
        self.assertTrue(result['server_alive'])
        self.scope().populated = False
        self.supervisor.poll(self.item, receipt=self.receipt())
        self.assertEqual(self.store.occupied(), 1)
        self.assertEqual(self.store.status(self.item['job_id'])['state'], 'unknown')
        self.assertFalse(self.launch()['started'])

    def test_wall_clock_rollback_does_not_extend_persisted_monotonic_deadline(self):
        self.monotonic = [100.0]
        self.clock[0] = 1000.0
        self.launch(timeout=20)
        self.clock[0] = 900.0
        self.monotonic[0] = 121.0
        result = self.create().poll(self.item)
        self.assertEqual(result['state'], 'unknown')
        self.assertEqual(self.store.occupied(), 1)

    def test_wall_clock_forward_jump_does_not_expire_running_work(self):
        self.monotonic = [100.0]
        self.clock[0] = 1000.0
        self.launch(timeout=20)
        self.clock[0] = 100000.0
        self.monotonic[0] = 101.0
        self.assertEqual(self.create().poll(self.item)['state'], 'running')

    def test_different_boot_never_finishes_even_with_empty_scope(self):
        self.launch()
        self.scope().populated = False
        self.boot[0] = 'different-boot'
        result = self.create().poll(self.item, receipt=self.receipt())
        self.assertEqual(result['state'], 'unknown')
        self.assertEqual(self.store.occupied(), 1)

    def test_missing_boot_never_finishes_even_with_empty_scope(self):
        self.launch()
        self.scope().populated = False
        self.boot[0] = None
        result = self.create().poll(self.item, receipt=self.receipt())
        self.assertEqual(result['state'], 'unknown')
        self.assertEqual(self.store.occupied(), 1)

    def test_duplicate_start_after_reboot_reports_unknown_without_relaunch(self):
        self.launch()
        self.boot[0] = 'different-boot'
        self.assertEqual(self.create().launch(self.item,
            [str(self.root / 'launches.txt')], timeout_seconds=20)['state'], 'unknown')
        self.assertEqual((self.root / 'launches.txt').read_text(), 'one execution\n')

    def test_cached_activity_from_another_boot_is_unknown(self):
        self.launch()
        self.supervisor.poll(self.item)
        self.boot[0] = 'different-boot'
        result = self.create().last_observation(self.item)
        self.assertEqual(result['state'], 'unknown')
        self.assertIsNone(result['server_alive'])
        self.assertFalse(result['fresh'])

    def test_monotonic_clock_regression_is_unknown(self):
        self.monotonic = [100.0]
        self.launch(timeout=20)
        self.monotonic[0] = 90.0
        self.assertEqual(self.create().poll(self.item)['state'], 'unknown')
        self.assertEqual(self.store.occupied(), 1)

    def crash_after_holding_receipt(self):
        self.launch()
        self.scope().populated = False
        receipt = self.receipt()
        source = ('import hashlib,json,os,pathlib,sys; '
            'sys.path.insert(0,sys.argv[5]); '
            'from test_server_supervisor import SyntheticScope; '
            'import server_supervisor; from remote_jobs import RemoteJobs;\n'
            'class FinishedScope(SyntheticScope):\n'
            ' @classmethod\n'
            ' def reopen(cls,parent,job_id,**kwargs):\n'
            '  scope=cls(parent,job_id,**kwargs); scope.populated=False; return scope\n'
            'def verify(item,receipt):\n'
            ' return bool(receipt["artifacts"]) and all(hashlib.sha256(pathlib.Path(ref["path"]).read_bytes()).hexdigest()==ref["sha256"] for ref in receipt["artifacts"])\n'
            'server_supervisor.LinuxScope=FinishedScope\n'
            'ledger=RemoteJobs(sys.argv[1],capacity=1,ledger_id=sys.argv[2])\n'
            'supervisor=server_supervisor.ServerSupervisor(ledger,cgroup_parent=sys.argv[3],'
            'worker_uid=65534,worker_gid=65534,receipt_validator=verify,clock=lambda:100.0)\n'
            'supervisor.monotonic_clock=lambda:100.0\n'
            'supervisor.boot_id_provider=lambda:"synthetic-boot"\n'
            'supervisor._finish_held=lambda *args,**kwargs:os._exit(98)\n'
            'supervisor.poll(json.loads(sys.argv[4]),receipt=json.loads(sys.argv[6]))\n')
        result = subprocess.run([sys.executable, '-c', source, str(self.store.database),
            self.store.ledger_id, str(self.root / 'scopes'), json.dumps(self.item),
            str(Path(__file__).parent), json.dumps(receipt)],
            capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 98, result.stderr)
        self.assertEqual(self.store.status(self.item['job_id'])['state'], 'running')
        self.assertEqual(self.store.occupied(), 1)
        return receipt

    def test_recover_held_receipt_after_process_crash_revalidates_without_rerunning(self):
        receipt = self.crash_after_holding_receipt()
        artifact = Path(receipt['artifacts'][0]['path'])
        before = artifact.stat().st_mtime_ns
        result = self.create().recover_held_receipt(self.item)
        self.assertEqual(result['state'], 'finished')
        self.assertEqual(self.store.occupied(), 0)
        self.assertEqual(json.loads(self.create().held_receipt(self.item)['receipt_json']), receipt)
        self.assertEqual(artifact.stat().st_mtime_ns, before)
        self.assertEqual((self.root / 'launches.txt').read_text(), 'one execution\n')

    def test_recovery_of_tampered_evidence_keeps_slot(self):
        receipt = self.crash_after_holding_receipt()
        Path(receipt['artifacts'][0]['path']).write_text('tampered')
        with self.assertRaises(JobConflict):
            self.create().recover_held_receipt(self.item)
        self.assertEqual(self.store.occupied(), 1)

    def test_recovery_rejects_changed_held_receipt_bytes(self):
        self.crash_after_holding_receipt()
        with closing(self.store._connect()) as db, db:
            db.execute('UPDATE supervisor_jobs SET receipt_json=? WHERE job_id=?',
                       ('{}', self.item['job_id']))
        with self.assertRaises(JobConflict):
            self.create().recover_held_receipt(self.item)
        self.assertEqual(self.store.occupied(), 1)

    def test_recovery_after_monotonic_timeout_never_releases(self):
        self.crash_after_holding_receipt()
        self.clock = [90.0]
        self.monotonic = [121.0]
        result = self.create().recover_held_receipt(self.item)
        self.assertEqual(result['state'], 'unknown')
        self.assertEqual(self.store.occupied(), 1)

    def test_recovery_after_reboot_never_releases(self):
        self.crash_after_holding_receipt()
        self.boot[0] = 'another-boot'
        result = self.create().recover_held_receipt(self.item)
        self.assertEqual(result['state'], 'unknown')
        self.assertEqual(self.store.occupied(), 1)

    def test_recovery_without_previously_held_receipt_is_refused(self):
        self.launch()
        self.scope().populated = False
        with self.assertRaises(JobConflict):
            self.create().recover_held_receipt(self.item)
        self.assertEqual(self.store.occupied(), 1)

    def test_recovery_never_releases_unknown_or_running_descendants(self):
        self.crash_after_holding_receipt()
        self.scope().populated = True
        self.assertEqual(self.create().recover_held_receipt(self.item)['state'], 'running')
        self.store.mark_unknown(self.item['job_id'], self.item['claim_id'])
        self.scope().populated = False
        def forbidden_validator(*args):
            self.fail('unknown recovery must not validate or release the job')
        result = self.create(forbidden_validator).recover_held_receipt(self.item)
        self.assertEqual(result['state'], 'unknown')
        self.assertEqual(self.store.occupied(), 1)

    def test_unavailable_scope_never_means_process_dead_and_never_retries(self):
        self.launch()
        self.scope().error = True
        result = self.supervisor.poll(self.item)
        self.assertEqual(result['state'], 'unknown')
        self.assertIsNone(result['server_alive'])
        self.assertEqual(self.store.occupied(), 1)
        self.assertFalse(self.launch()['started'])

    def test_scope_identity_replacement_is_unknown_not_empty(self):
        self.launch()
        with patch.object(self.scope(), 'snapshot', return_value=dict(
                populated=False, boot_id='different-boot', scope_id=[12, 34])):
            result = self.supervisor.poll(self.item, receipt=self.receipt())
        self.assertEqual(result['state'], 'unknown')
        self.assertEqual(self.store.occupied(), 1)

    def test_receipt_validator_mutation_is_rejected(self):
        self.launch()
        self.scope().populated = False
        def mutates(item, receipt):
            receipt['manifest_sha256'] = 'e' * 64
            return True
        self.supervisor.receipt_validator = mutates
        with self.assertRaises(JobConflict):
            self.supervisor.poll(self.item, receipt=self.receipt())
        self.assertEqual(self.store.occupied(), 1)

    def test_scope_is_rechecked_after_artifact_validator(self):
        self.launch()
        self.scope().populated = False
        def verifier(item, receipt):
            valid = self.validate_receipt(item, receipt)
            self.scope().populated = True
            return valid
        self.supervisor.receipt_validator = verifier
        with self.assertRaises(JobConflict):
            self.supervisor.poll(self.item, receipt=self.receipt())
        self.assertEqual(self.store.occupied(), 1)

    def test_expensive_validator_cannot_finish_after_deadline(self):
        self.launch(timeout=2)
        self.scope().populated = False
        def verifier(item, receipt):
            self.clock[0] = 103.0
            return self.validate_receipt(item, receipt)
        self.supervisor.receipt_validator = verifier
        with self.assertRaises(JobConflict):
            self.supervisor.poll(self.item, receipt=self.receipt())
        self.assertEqual(self.store.occupied(), 1)
        self.assertEqual(self.store.status(self.item['job_id'])['state'], 'unknown')

    def test_final_scope_check_holds_ledger_write_lock_and_rechecks_deadline(self):
        self.launch(timeout=2)
        self.scope().populated = False
        original = self.scope().snapshot
        probes, contenders = [], []
        def scope_probe():
            probes.append(True)
            if len(probes) == 2:
                source = ('import sqlite3,sys; db=sqlite3.connect(sys.argv[1],timeout=0);\n'
                    'try:\n db.execute("BEGIN IMMEDIATE")\n'
                    'except sqlite3.OperationalError:\n print("blocked")\n'
                    'else:\n db.execute("UPDATE executor_settings SET value=value"); '
                    'db.commit(); print("committed")\n')
                result = subprocess.run([sys.executable, '-c', source, str(self.store.database)],
                    capture_output=True, text=True, timeout=10, check=True)
                contenders.append(result.stdout.strip())
                self.clock[0] = 103.0
            return original()
        with patch.object(self.scope(), 'snapshot', scope_probe):
            try:
                self.supervisor.poll(self.item, receipt=self.receipt())
            except JobConflict:
                pass
        self.assertEqual(contenders, ['blocked'])
        self.assertEqual(self.store.status(self.item['job_id'])['state'], 'unknown')
        self.assertEqual(self.store.occupied(), 1)

    def test_scope_observation_crossing_deadline_marks_unknown_immediately(self):
        self.launch(timeout=2)
        original = self.scope().snapshot
        def slow_probe():
            self.clock[0] = 103.0
            return original()
        with patch.object(self.scope(), 'snapshot', slow_probe):
            result = self.supervisor.poll(self.item)
        self.assertEqual(result['state'], 'unknown')
        self.assertEqual(self.store.occupied(), 1)

    def test_stale_observation_does_not_turn_reused_leader_pid_into_running(self):
        self.launch()
        self.scope().identity['start_ticks'] += 1
        result = self.supervisor.poll(self.item)
        self.assertFalse(result['leader_alive'])
        self.assertTrue(result['server_alive'])
        self.assertEqual(self.store.occupied(), 1)

    def test_activity_expires_and_does_not_claim_remote_completion(self):
        self.launch()
        result = self.supervisor.poll(self.item, freshness_seconds=5)
        self.assertEqual(result['observed_at'], 100.0)
        self.assertEqual(result['valid_until'], 105.0)
        self.clock[0] = 106.0
        result = self.supervisor.last_observation(self.item)
        self.assertIsNone(result['server_alive'])
        self.assertFalse(result['fresh'])
        self.assertEqual(self.store.occupied(), 1)

    def test_wall_clock_rollback_cannot_revive_stale_activity(self):
        self.monotonic = [100.0]
        self.clock[0] = 1000.0
        self.launch(timeout=100)
        self.supervisor.poll(self.item, freshness_seconds=5)
        self.clock[0] = 1004.0
        self.monotonic[0] = 104.0
        self.assertTrue(self.create().last_observation(self.item)['fresh'])
        self.clock[0] = 1002.0
        self.monotonic[0] = 110.0
        result = self.create().last_observation(self.item)
        self.assertFalse(result['fresh'])
        self.assertIsNone(result['server_alive'])
        self.assertEqual(result['state'], 'running')

    def test_slow_validation_cannot_publish_expired_activity_as_fresh(self):
        self.monotonic = [100.0]
        self.launch(timeout=100)
        self.scope().populated = False
        def slow_validator(item, receipt):
            self.monotonic[0] = 106.0
            return self.validate_receipt(item, receipt)
        self.supervisor.receipt_validator = slow_validator
        result = self.supervisor.poll(self.item, receipt=self.receipt(), freshness_seconds=5)
        self.assertEqual(result['state'], 'finished')
        self.assertFalse(result['fresh'])
        self.assertIsNone(result['server_alive'])

    def test_unrecorded_running_job_is_unknown_never_adopted_or_restarted(self):
        self.store.start(self.item['job_id'], self.item['claim_id'])
        result = self.launch()
        self.assertFalse(result['started'])
        self.assertEqual(result['state'], 'unknown')
        self.assertFalse((self.root / 'launches.txt').exists())


@unittest.skipUnless(sys.platform == 'linux', 'Linux /proc identity tests only')
class LinuxIdentityTests(unittest.TestCase):
    def test_gated_real_child_failure_reaches_supplied_stderr(self):
        # This test isolates file-descriptor inheritance: its ordinary temporary
        # directory only stands in for cgroup.procs, not for kernel containment.
        module = importlib.import_module('server_supervisor')
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'cgroup.procs').write_text('')
            scope = object.__new__(module.LinuxScope)
            scope.path, scope.uid, scope.gid = root, os.geteuid(), os.getegid()
            scope.gate, scope.process = None, None
            output = root / 'worker.stderr.log'
            with output.open('wb') as stream:
                scope.launch([sys.executable, '-c', 'raise RuntimeError("synthetic early failure")'],
                             stderr=stream)
                scope.release_gate()
                self.assertNotEqual(scope.process.wait(timeout=5), 0)
            self.assertIn(b'synthetic early failure', output.read_bytes())

    def test_real_start_ticks_detect_pid_reuse(self):
        module = importlib.import_module('server_supervisor')
        identity = module.process_identity(os.getpid())
        self.assertEqual(identity['pid'], os.getpid())
        self.assertGreater(identity['start_ticks'], 0)
        self.assertTrue(module.same_process(identity))
        identity['start_ticks'] += 1
        self.assertFalse(module.same_process(identity))

    def test_real_parent_exit_does_not_prove_detached_child_exit(self):
        module = importlib.import_module('server_supervisor')
        with tempfile.TemporaryDirectory() as temp:
            pidfile = Path(temp) / 'pid'
            code = ('import os,sys,time; pid=os.fork(); '
                    'sys.exit(0) if pid else None; os.setsid(); '
                    'open(sys.argv[1],"w").write(str(os.getpid())); time.sleep(30)')
            parent = subprocess.Popen([sys.executable, '-c', code, str(pidfile)])
            child = None
            try:
                parent.wait(timeout=5)
                deadline = time.monotonic() + 5
                while not pidfile.exists() and time.monotonic() < deadline:
                    time.sleep(.01)
                child = int(pidfile.read_text())
                identity = module.process_identity(child)
                self.assertTrue(module.same_process(identity))
                # A process-group-only strategy must never qualify as containment.
                with self.assertRaises(module.SupervisionUnavailable):
                    module.LinuxScope(Path(temp), str(uuid4()),
                        worker_uid=max(os.geteuid(), 1), worker_gid=max(os.getegid(), 1))
            finally:
                if child is not None:
                    os.kill(child, 9)

    def test_gate_eof_never_executes_business_command(self):
        module = importlib.import_module('server_supervisor')
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / 'must-not-exist'
            read_fd, write_fd = os.pipe()
            process = subprocess.Popen([sys.executable, module.__file__, '--gate',
                str(read_fd), str(os.geteuid()), str(os.getegid()), sys.executable,
                '-c', 'import pathlib,sys; pathlib.Path(sys.argv[1]).touch()', str(output)],
                pass_fds=(read_fd,))
            os.close(read_fd)
            os.close(write_fd)
            self.assertEqual(process.wait(timeout=5), 125)
            self.assertFalse(output.exists())


if __name__ == '__main__':
    unittest.main()
