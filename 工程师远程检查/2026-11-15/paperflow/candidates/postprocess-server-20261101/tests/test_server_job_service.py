"""Single-job service tests use disposable data and never execute Wine."""
from contextlib import closing, redirect_stderr, redirect_stdout
import copy
import hashlib
import importlib
import io
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest.mock import patch

from remote_jobs import RemoteJobs
from test_server_supervisor import make_job


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()


class ConfigTests(unittest.TestCase):
    def module(self):
        try:
            return importlib.import_module('server_job_service')
        except ImportError:
            self.fail('single-job service is not implemented')

    def test_automatic_capacity_release_requires_recorded_terminated_scope(self):
        from unittest.mock import Mock
        module, job = self.module(), make_job()
        ledger, config = Mock(), {'original': 'config'}
        cleanup = {'terminated': True, 'scope_removed': True}
        self.assertTrue(module._release_terminated_slot(ledger, job, config, cleanup, None))
        ledger.release_terminated_capacity.assert_called_once_with(job['job_id'], job['claim_id'], config)
        ledger.reset_mock()
        self.assertFalse(module._release_terminated_slot(ledger, job, config, {'terminated': False}, None))
        self.assertFalse(module._release_terminated_slot(ledger, job, config, cleanup, 'ledger-error'))
        ledger.release_terminated_capacity.assert_not_called()
        ledger.release_terminated_capacity.side_effect = ValueError('proof unavailable')
        self.assertFalse(module._release_terminated_slot(ledger, job, config, cleanup, None))

    def test_configuration_requires_independent_digest(self):
        module = self.module()
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'config.json'; path.write_bytes(b'{}')
            with self.assertRaises(module.ServiceError):
                module.load_config(path, 'f' * 64)

    def test_absent_config_is_never_initialized_or_replaced(self):
        module = self.module()
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'absent.json'
            with self.assertRaises(module.ServiceError):
                module.load_config(path, 'f' * 64)
            self.assertFalse(path.exists())

    def test_cli_success_outputs_only_small_safe_status(self):
        module, job = self.module(), make_job()
        private = 'synthetic-private-content-and-path'
        result = dict(job={**job, 'extra': private}, state='finished', started=True,
            held_receipt={'receipt_json': private, 'receipt_sha256': 'a' * 64},
            cleanup={'terminated': True, 'scope_removed': True, 'prefix_removed': False,
                     'private_path': private}, rag_admission='not_granted')
        output, errors = io.StringIO(), io.StringIO()
        with patch.object(module.sys, 'version_info', (3, 11)), patch.object(module.os, 'umask'), \
                patch.object(module.signal, 'signal'), patch.object(module, 'load_config', return_value={}), \
                patch.object(module, 'run_job', return_value=result), \
                redirect_stdout(output), redirect_stderr(errors):
            code = module.main([job['job_id'], '--config-sha256', 'b' * 64])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output.getvalue()), dict(job_id=job['job_id'], state='finished',
            started=True, receipt_sha256='a' * 64,
            cleanup={'terminated': True, 'scope_removed': True, 'prefix_removed': False}))
        self.assertNotIn(private, output.getvalue() + errors.getvalue())

    def test_cli_failure_outputs_error_types_without_sensitive_exception_text(self):
        module, job = self.module(), make_job()
        private = 'synthetic-private-content-and-path'
        def fail(*args, **kwargs):
            try:
                raise ValueError(private)
            except ValueError as cause:
                raise module.ServiceError(private) from cause
        output, errors = io.StringIO(), io.StringIO()
        with patch.object(module.sys, 'version_info', (3, 11)), patch.object(module.os, 'umask'), \
                patch.object(module.signal, 'signal'), patch.object(module, 'load_config', return_value={}), \
                patch.object(module, 'run_job', side_effect=fail), \
                redirect_stdout(output), redirect_stderr(errors):
            code = module.main([job['job_id'], '--config-sha256', 'b' * 64])
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(errors.getvalue()), dict(job_id=job['job_id'],
            error_types=['ServiceError', 'ValueError']))
        self.assertEqual(output.getvalue(), '')
        self.assertNotIn(private, errors.getvalue())

    def test_cli_duplicate_finished_status_includes_saved_digest_only(self):
        module, job = self.module(), make_job()
        result = dict(job=job, state='finished', started=False, receipt_sha256='c' * 64)
        output = io.StringIO()
        with patch.object(module.sys, 'version_info', (3, 11)), patch.object(module.os, 'umask'), \
                patch.object(module.signal, 'signal'), patch.object(module, 'load_config', return_value={}), \
                patch.object(module, 'run_job', return_value=result), redirect_stdout(output):
            code = module.main([job['job_id'], '--config-sha256', 'b' * 64])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output.getvalue())['receipt_sha256'], 'c' * 64)


@unittest.skipUnless(sys.platform == 'linux', 'Linux no-follow service and scope semantics')
class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.module = ConfigTests().module()
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.ledger = RemoteJobs(self.root / 'ledger.sqlite3', capacity=8)
        self.ledger.initialize()
        self.job = make_job()
        self.jobdir = self.root / 'jobs' / self.job['job_id']
        self.jobdir.mkdir(parents=True, mode=0o700)
        self.snapshot = self.root / 'staging' / self.job['job_id']; self.snapshot.mkdir(parents=True, mode=0o700)
        (self.snapshot / 'drives/f').mkdir(parents=True)
        (self.jobdir / 'job.json').write_bytes(encoded(self.job))
        self.manifest = dict(schema='paperflow-remote-snapshot/1', **self.job,
                             output_root_windows=r'F:\out', files=[])
        (self.snapshot / 'transport-manifest.json').write_bytes(encoded(self.manifest))
        self.config = dict(schema='paperflow-job-service/1',
            server_identity=dict(server_id='synthetic-server', ledger_id=self.ledger.ledger_id),
            ledger=dict(database=str(self.ledger.database), capacity=8), jobs_root=str(self.root / 'jobs'),
            staging_root=str(self.root / 'staging'), initialize_delegation=True,
            base=str(self.root / 'base'), cgroup_parent=str(self.root / 'cgroup'),
            release=dict(code_root=str(self.root / 'release'), code_manifest=str(self.root / 'code.json'),
                template=str(self.root / 'template'), template_manifest=str(self.root / 'template.json')),
            native_manifest=str(self.root / 'native.json'), native_manifest_sha256='e' * 64,
            limits=dict(timeout_seconds=20, poll_seconds=.01, cleanup_seconds=.1, disk_poll_seconds=5,
                memory_bytes=128 * 1024 * 1024, pids=32, file_bytes=1024 * 1024,
                open_files=128, max_job_bytes=1024 * 1024 * 10, min_free_bytes=1024 * 1024),
            worker_uid=os.getuid(), worker_gid=os.getgid())
        self.prepares, self.starts, self.kills = [], [], []
        self.native = dict(roles={'python': sys.executable,
            'worker_script': str(self.root / 'server_wine_worker.py'),
            'service_script': str(Path(self.module.__file__).resolve())}, files=[])
        script = Path(self.native['roles']['service_script'])
        self.native['files'] = [dict(path=str(script), bytes=script.stat().st_size,
                                    sha256=hashlib.sha256(script.read_bytes()).hexdigest())]
        template_raw = encoded(dict(entries=[]))
        (self.root / 'template.json').write_bytes(template_raw)
        self.manifest['runtime_template_sha256'] = hashlib.sha256(template_raw).hexdigest()
        code = self.snapshot / 'drives/f/synthetic_worker.py'; code.write_bytes(b'synthetic code never executed')
        self.verified = dict(job=copy.deepcopy(self.job), manifest=copy.deepcopy(self.manifest),
            snapshot=str(self.snapshot), prefix=str(self.jobdir / 'prefix'), native=self.native,
            code_files=[dict(relative='drives/f/synthetic_worker.py', bytes=code.stat().st_size,
                            sha256=hashlib.sha256(code.read_bytes()).hexdigest())])
        self.patches = [patch.object(self.module, 'verify_job', side_effect=lambda *a, **k: copy.deepcopy(self.verified)),
            patch.object(self.module, 'verify_native_runtime', side_effect=lambda *a, **k: copy.deepcopy(self.native)),
            patch.object(self.module, 'prepare_environment', side_effect=self.prepare),
            patch.object(self.module, 'inspect_receipt', side_effect=self.inspect),
            patch.object(self.module, '_delegate_parent', side_effect=lambda *a: dict(
                path=self.config['cgroup_parent'], identity=[1, 2], boot_id='synthetic')),
            patch.object(self.module, '_assert_installed_config', return_value=None),
            patch.object(self.module, 'ServerSupervisor', side_effect=self.supervisor),
            patch.object(self.module, '_cleanup_scope', side_effect=self.cleanup)]
        for item in self.patches:
            item.start(); self.addCleanup(item.stop)
        self.module.initialize_service_state(self.config)

    def prepare(self, **kwargs):
        self.prepares.append(copy.deepcopy(kwargs))
        Path(kwargs['prefix']).mkdir(mode=0o700)
        return dict(job=copy.deepcopy(self.job), prefix=str(kwargs['prefix']),
                    template_identity_verified=True, native_runtime_identity_verified=False)

    def inspect(self, **kwargs):
        # Inspector integration is a kernel-filesystem boundary tested separately.
        receipt = kwargs['receipt']
        for ref in receipt['artifacts']:
            if hashlib.sha256(Path(ref['path']).read_bytes()).hexdigest() != ref['sha256']:
                raise ValueError('synthetic artifact changed')
        return dict(integrity_status='passed', job=kwargs['job'],
                    receipt_canonical_sha256=hashlib.sha256(encoded(receipt)).hexdigest())

    def write_result(self):
        output = self.snapshot / 'artifact'; output.write_bytes(b'synthetic validated artifact')
        receipt = dict(schema='paperflow-postprocessing-receipt/1', identity=self.job['identity'],
            claim_id=self.job['claim_id'], manifest_sha256=self.job['manifest_sha256'],
            validation_status='passed', artifacts=[dict(path=str(output),
            sha256=hashlib.sha256(output.read_bytes()).hexdigest())])
        result = dict(server_validation='passed', receipt=receipt,
            receipt_json=encoded(receipt).decode(), receipt_sha256=hashlib.sha256(encoded(receipt)).hexdigest())
        (self.snapshot / 'drives/f/wire-result.json').write_bytes(encoded(result))
        (self.snapshot / 'tokenizer-runtime.json').write_bytes(encoded(dict(
            vocab_only=True, context_created=False, inference_requests=0, unified=True)))
        return result

    def supervisor(self, ledger, **kwargs):
        outer = self
        class Scope:
            def snapshot(self):
                return dict(populated=False, boot_id='synthetic', scope_id=[1, 2])
        class Supervisor:
            _scopes = {outer.job['job_id']: Scope()}
            def initialize(self):
                pass
            def launch(self, job, argv, **options):
                outer.starts.append(copy.deepcopy(job))
                if options.get('stderr') is not None:
                    options['stderr'].write(b'synthetic worker failure detail\n')
                    options['stderr'].flush()
                ledger.start(job['job_id'], job['claim_id'])
                outer.write_result()
                return dict(started=True, state='running')
            def poll(self, job, receipt=None, **options):
                if receipt is not None and kwargs['receipt_validator'](job, receipt) is True:
                    digest = hashlib.sha256(encoded(receipt)).hexdigest()
                    ledger.finish(job['job_id'], job['claim_id'], digest, confirm=lambda *a: True)
                return dict(state=ledger.status(job['job_id'])['state'], server_alive=False)
            def held_receipt(self, job):
                value = json.loads((outer.snapshot / 'drives/f/wire-result.json').read_bytes())
                return dict(receipt_json=value['receipt_json'], receipt_sha256=value['receipt_sha256'])
        return Supervisor()

    def cleanup(self, *args, **kwargs):
        self.kills.append(kwargs.get('kill', False))
        return dict(terminated=True, scope_removed=True)

    def run_job(self):
        return self.module.run_job(self.job['job_id'], self.config)

    def test_same_admitted_job_prepares_and_launches_only_once(self):
        self.ledger.reserve(self.job)
        result = self.run_job()
        self.assertEqual(result['state'], 'finished')
        self.assertEqual(self.ledger.occupied(), 0)
        second = self.run_job()
        self.assertEqual(second['state'], 'finished')
        self.assertEqual(second['receipt_sha256'], result['held_receipt']['receipt_sha256'])
        self.assertEqual(len(self.prepares), 1)
        self.assertEqual(len(self.starts), 1)
        self.assertFalse((self.jobdir / 'prefix').exists())
        self.assertTrue((self.snapshot / 'artifact').is_file())

    def test_explicit_install_creates_real_supervisor_and_service_state_tables(self):
        from server_supervisor import ServerSupervisor
        fresh = RemoteJobs(self.root / 'fresh.sqlite3', capacity=8); fresh.initialize()
        config = copy.deepcopy(self.config)
        config['ledger']['database'] = str(fresh.database)
        config['server_identity']['ledger_id'] = fresh.ledger_id
        with patch.object(self.module, 'ServerSupervisor', ServerSupervisor):
            self.module.initialize_service_state(config)
        with closing(fresh._connect()) as db:
            names = {row['name'] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertTrue({'executor_jobs', 'supervisor_jobs', 'service_jobs'} <= names)
        self.assertEqual(fresh.occupied(), 0)

    def test_second_service_during_first_preparation_cannot_start_another_worker(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Event
        self.ledger.reserve(self.job)
        entered, proceed = Event(), Event()
        original = self.prepare
        def preparing(**kwargs):
            entered.set()
            if not proceed.wait(5):
                raise AssertionError('test preparation gate was never released')
            return original(**kwargs)
        with patch.object(self.module, 'prepare_environment', preparing), ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.run_job)
            try:
                self.assertTrue(entered.wait(5))
                second = pool.submit(self.run_job).result(timeout=5)
                self.assertFalse(second['started'])
                self.assertEqual(self.starts, [])
            finally:
                proceed.set()
            self.assertEqual(first.result(timeout=5)['state'], 'finished')
        self.assertEqual(len(self.prepares), 1)
        self.assertEqual(len(self.starts), 1)

    def test_unreserved_job_does_not_prepare_or_launch(self):
        with self.assertRaises(self.module.ServiceError):
            self.run_job()
        self.assertEqual(self.prepares, [])
        self.assertEqual(self.starts, [])

    def test_unknown_job_is_never_prepared_or_restarted(self):
        self.ledger.reserve(self.job)
        self.ledger.mark_unknown(self.job['job_id'], self.job['claim_id'])
        result = self.run_job()
        self.assertEqual(result['state'], 'unknown')
        self.assertEqual(self.prepares, [])
        self.assertEqual(self.starts, [])
        self.assertEqual(self.ledger.occupied(), 1)

    def test_prepare_failure_keeps_unknown_slot_and_original_snapshot(self):
        self.ledger.reserve(self.job)
        with patch.object(self.module, 'prepare_environment', side_effect=ValueError('bad release')):
            with self.assertRaises(self.module.ServiceError):
                self.run_job()
        self.assertEqual(self.ledger.status(self.job['job_id'])['state'], 'unknown')
        self.assertFalse((self.jobdir / 'prefix').exists())
        self.assertEqual(self.ledger.occupied(), 1)
        self.assertTrue((self.snapshot / 'transport-manifest.json').exists())
        self.assertEqual(self.starts, [])

    def test_stop_before_preparation_never_starts_business(self):
        self.ledger.reserve(self.job)
        (self.jobdir / 'STOP.json').write_bytes(encoded(dict(job=self.job, reason='synthetic pause')))
        result = self.run_job()
        self.assertEqual(result['state'], 'unknown')
        self.assertEqual(self.prepares, [])
        self.assertEqual(self.starts, [])
        self.assertEqual(self.ledger.occupied(), 1)

    def test_result_digest_or_tokenizer_identity_cannot_be_faked(self):
        self.ledger.reserve(self.job)
        real_write = self.write_result
        def bad_result():
            result = real_write(); result['receipt_sha256'] = 'f' * 64
            (self.snapshot / 'drives/f/wire-result.json').write_bytes(encoded(result))
        with patch.object(self, 'write_result', bad_result):
            with self.assertRaises(self.module.ServiceError):
                self.run_job()
        self.assertEqual(self.ledger.occupied(), 1)
        self.assertEqual(self.ledger.status(self.job['job_id'])['state'], 'unknown')
        self.assertTrue((self.jobdir / 'prefix').is_dir())

    def test_worker_exit_without_business_receipt_does_not_finish(self):
        self.ledger.reserve(self.job)
        with patch.object(self, 'write_result', lambda: None):
            with self.assertRaises(self.module.ServiceError):
                self.run_job()
        self.assertEqual(self.ledger.occupied(), 1)
        self.assertEqual(self.ledger.status(self.job['job_id'])['state'], 'unknown')

    def test_worker_failure_preserves_private_stderr_and_never_retries(self):
        self.ledger.reserve(self.job)
        with patch.object(self, 'write_result', lambda: None):
            with self.assertRaises(self.module.ServiceError) as caught:
                self.run_job()
        log = self.jobdir / 'worker.stderr.log'
        self.assertEqual(log.read_bytes(), b'synthetic worker failure detail\n')
        self.assertEqual(stat.S_IMODE(log.stat().st_mode), 0o600)
        self.assertEqual(log.stat().st_nlink, 1)
        self.assertNotIn('synthetic worker failure detail', str(caught.exception))
        self.assertEqual(self.ledger.status(self.job['job_id'])['state'], 'unknown')
        self.assertEqual(self.ledger.occupied(), 1)
        self.assertFalse(self.run_job()['started'])
        self.assertEqual(len(self.starts), 1)
        self.assertEqual(log.read_bytes(), b'synthetic worker failure detail\n')

    def test_existing_stderr_file_is_never_overwritten_or_reused(self):
        log = self.jobdir / 'worker.stderr.log'
        log.write_bytes(b'original diagnostic')
        self.ledger.reserve(self.job)
        with self.assertRaises(self.module.ServiceError):
            self.run_job()
        self.assertEqual(log.read_bytes(), b'original diagnostic')
        self.assertEqual(self.starts, [])
        self.assertEqual(self.ledger.status(self.job['job_id'])['state'], 'unknown')
        self.assertEqual(self.ledger.occupied(), 1)

    def test_existing_stderr_symlink_is_never_followed(self):
        target = self.root / 'protected-synthetic-file'
        target.write_bytes(b'original bytes')
        (self.jobdir / 'worker.stderr.log').symlink_to(target)
        self.ledger.reserve(self.job)
        with self.assertRaises(self.module.ServiceError):
            self.run_job()
        self.assertEqual(target.read_bytes(), b'original bytes')
        self.assertEqual(self.starts, [])
        self.assertEqual(self.ledger.status(self.job['job_id'])['state'], 'unknown')

    def test_changed_bundle_owner_cannot_prepare_existing_reserved_job(self):
        self.ledger.reserve(self.job)
        changed = copy.deepcopy(self.job); changed['claim_id'] = '00000000-0000-4000-8000-000000000000'
        self.verified['job'] = changed
        with self.assertRaises(self.module.ServiceError):
            self.run_job()
        self.assertEqual(self.prepares, [])
        self.assertEqual(self.starts, [])

    def test_prefix_collection_never_follows_f_drive_into_originals(self):
        self.ledger.reserve(self.job)
        original = self.prepare
        def with_drive(**kwargs):
            result = original(**kwargs)
            devices = Path(kwargs['prefix']) / 'dosdevices'; devices.mkdir()
            (devices / 'f:').symlink_to(self.snapshot / 'drives/f', target_is_directory=True)
            return result
        with patch.object(self.module, 'prepare_environment', with_drive):
            self.assertEqual(self.run_job()['state'], 'finished')
        self.assertFalse((self.jobdir / 'prefix').exists())
        self.assertTrue((self.snapshot / 'drives/f/wire-result.json').is_file())

    def test_post_finish_artifact_revalidation_failure_preserves_temporary_prefix(self):
        self.ledger.reserve(self.job)
        def cleanup(*args, **kwargs):
            (self.snapshot / 'artifact').write_bytes(b'synthetic changed artifact after receipt persistence')
            return dict(terminated=True, scope_removed=True)
        with patch.object(self.module, '_cleanup_scope', cleanup):
            result = self.run_job()
        self.assertFalse(result['cleanup']['prefix_removed'])
        self.assertEqual(result['cleanup']['prefix_error'], 'ValueError')
        self.assertTrue((self.jobdir / 'prefix').is_dir())
        self.assertTrue((self.snapshot / 'artifact').is_file())

    def test_low_global_disk_reserve_refuses_preparation(self):
        self.ledger.reserve(self.job)
        self.config['limits']['min_free_bytes'] = 10 ** 30
        with self.assertRaises(self.module.ServiceError):
            self.run_job()
        self.assertEqual(self.prepares, [])
        self.assertEqual(self.starts, [])
        self.assertEqual(self.ledger.occupied(), 1)

    def test_stop_after_start_marks_unknown_before_scope_kill_and_keeps_prefix(self):
        self.ledger.reserve(self.job)
        seen = []
        def stop():
            return bool(self.starts)
        def cleanup(*args, **kwargs):
            seen.append((kwargs['kill'], self.ledger.status(self.job['job_id'])['state']))
            return dict(terminated=True, scope_removed=True)
        with patch.object(self.module, '_cleanup_scope', cleanup):
            with self.assertRaises(self.module.ServiceError):
                self.module.run_job(self.job['job_id'], self.config, stopped=stop)
        self.assertEqual(seen, [(True, 'unknown')])
        self.assertTrue((self.jobdir / 'prefix').is_dir())
        self.assertEqual(self.ledger.occupied(), 1)

    def test_accounting_failure_does_not_skip_controlled_scope_cleanup(self):
        self.ledger.reserve(self.job)
        import sqlite3
        with patch.object(self.module, '_mark_unknown', side_effect=sqlite3.OperationalError('synthetic unavailable ledger')):
            with self.assertRaises(self.module.ServiceError):
                self.module.run_job(self.job['job_id'], self.config, stopped=lambda: bool(self.starts))
        self.assertEqual(self.kills, [True])
        self.assertTrue((self.jobdir / 'prefix').is_dir())
        self.assertEqual(self.ledger.occupied(), 1)

    def test_preflight_is_heavy_but_final_admission_does_not_rehash_or_reverify_job(self):
        receipt = self.module.preflight_job(self.job, self.config)
        calls = []
        with patch.object(self.module, 'verify_job', side_effect=AssertionError('heavy verification under admission')), \
             patch.object(self.module, 'verify_native_runtime', side_effect=AssertionError('native hashing under admission')), \
             patch.object(self.module, 'launch_systemd', side_effect=lambda job, **kwargs: calls.append(job) or True):
            result = self.module.admit_job(self.job, self.config,
                preflight_id=receipt['preflight_id'], preflight_sha256=receipt['preflight_sha256'])
            self.assertTrue(result['accepted'])
            duplicate = self.module.admit_job(self.job, self.config,
                preflight_id=receipt['preflight_id'], preflight_sha256=receipt['preflight_sha256'])
            self.assertFalse(duplicate['accepted'])
        self.assertEqual(calls, [self.job])

    def test_full_capacity_returns_definite_no_start_without_launch(self):
        receipt = self.module.preflight_job(self.job, self.config)
        for number in range(2, 10):
            self.ledger.reserve(make_job(number))
        with patch.object(self.module, 'launch_systemd', side_effect=AssertionError('full capacity launched')):
            result = self.module.admit_job(self.job, self.config,
                preflight_id=receipt['preflight_id'], preflight_sha256=receipt['preflight_sha256'])
        self.assertEqual(result, dict(job=self.job, server_identity=self.config['server_identity'],
            accepted=False, reason='capacity_full', this_attempt_started=False))
        self.assertEqual(self.ledger.occupied(), 8)

    def test_quick_admission_refuses_changed_preflight_and_contended_ledger_promptly(self):
        receipt = self.module.preflight_job(self.job, self.config)
        with self.assertRaises(self.module.ServiceError):
            self.module.admit_job(self.job, self.config,
                preflight_id=receipt['preflight_id'], preflight_sha256='f' * 64)
        import sqlite3, time
        with closing(sqlite3.connect(self.ledger.database)) as blocker:
            blocker.execute('BEGIN IMMEDIATE')
            started = time.monotonic()
            with patch.object(self.module, 'launch_systemd', side_effect=AssertionError('contended admission dispatched')):
                result = self.module.admit_job(self.job, self.config,
                    preflight_id=receipt['preflight_id'], preflight_sha256=receipt['preflight_sha256'])
                self.assertEqual(result, dict(job=self.job, server_identity=self.config['server_identity'],
                    accepted=False, reason='admission_busy', this_attempt_started=False))
            self.assertLess(time.monotonic() - started, 1.0)
        self.assertEqual(self.ledger.occupied(), 0)

    def test_disk_tree_is_sampled_at_separate_low_frequency(self):
        self.ledger.reserve(self.job)
        original_factory = self.supervisor
        def factory(ledger, **kwargs):
            instance = original_factory(ledger, **kwargs)
            original_poll, probes = instance.poll, []
            def poll(job, receipt=None, **options):
                if receipt is None and len(probes) < 3:
                    probes.append(True)
                    return dict(state='running', server_alive=True)
                return original_poll(job, receipt=receipt, **options)
            instance.poll = poll
            return instance
        real_usage, samples = self.module._disk_usage, []
        def usage(*args):
            samples.append(True)
            return real_usage(*args)
        with patch.object(self.module, 'ServerSupervisor', factory), patch.object(self.module, '_disk_usage', usage):
            self.assertEqual(self.run_job()['state'], 'finished')
        self.assertEqual(len(samples), 1)

    def test_limited_worker_rejects_unexpected_path_before_verification_or_execution(self):
        with patch.dict(os.environ, PATH='/unreviewed/bin'), patch.object(self.module, 'verify_job',
                side_effect=AssertionError('unexpected PATH reached heavy verification')):
            with self.assertRaises(self.module.ServiceError):
                self.module._limited_worker(self.job['job_id'], self.config)


@unittest.skipUnless(sys.platform == 'linux', 'Linux delegated cgroup protocol')
class DelegationTests(unittest.TestCase):
    def setUp(self):
        self.module = ConfigTests().module()
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.job = make_job()
        self.parent = Path(self.temp.name) / ('paperflow-pp-' + self.job['job_id'] + '.service')
        self.parent.mkdir(mode=0o700)
        (self.parent / 'cgroup.procs').write_text(str(os.getpid()))
        (self.parent / 'cgroup.controllers').write_text('memory pids cpu')
        (self.parent / 'cgroup.subtree_control').write_text('')
        self.config = dict(initialize_delegation=True, cgroup_parent=str(self.parent))

    def kernel_write(self, path, value):
        if path.name == 'cgroup.procs':
            (self.parent / 'cgroup.procs').write_text('')
            path.write_text(value)
        elif path.name == 'cgroup.subtree_control':
            path.write_text('memory pids')

    def test_own_single_pid_unit_moves_to_control_before_enabling_controllers(self):
        with patch.object(self.module, '_current_cgroup', return_value=self.parent), \
             patch.object(self.module.LinuxScope, '_check_directory', return_value=None), \
             patch.object(self.module, '_cgroup_write', side_effect=self.kernel_write):
            result = self.module._delegate_parent(self.job, self.config)
        self.assertEqual(result['path'], str(self.parent))
        self.assertEqual((self.parent / 'supervisor-control/cgroup.procs').read_text(), str(os.getpid()))
        self.assertEqual((self.parent / 'cgroup.procs').read_text(), '')
        self.assertEqual((self.parent / 'cgroup.subtree_control').read_text(), 'memory pids')
        self.assertNotEqual(Path(result['path']).name, 'supervisor-control')

    def test_additional_internal_process_fails_before_any_migration(self):
        (self.parent / 'cgroup.procs').write_text(str(os.getpid()) + '\n999999')
        with patch.object(self.module, '_current_cgroup', return_value=self.parent), \
             patch.object(self.module.LinuxScope, '_check_directory', return_value=None):
            with self.assertRaises(self.module.ServiceError):
                self.module._delegate_parent(self.job, self.config)
        self.assertFalse((self.parent / 'supervisor-control').exists())

    def test_uncontrolled_or_nonkernel_scope_is_never_modified(self):
        with patch.object(self.module, '_current_cgroup', return_value=self.parent):
            with self.assertRaises(Exception):
                self.module._delegate_parent(self.job, self.config)
        self.assertFalse((self.parent / 'supervisor-control').exists())


@unittest.skipUnless(sys.platform == 'linux', 'Linux scope cleanup evidence')
class ScopeCleanupTests(unittest.TestCase):
    def test_cleanup_records_original_process_and_last_empty_kernel_scope(self):
        import sqlite3
        from types import SimpleNamespace
        module = ConfigTests().module()
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            database = root / 'ledger.sqlite3'
            job = make_job()
            process = dict(pid=12345, boot_id=module._boot_id(), start_ticks=999)
            expected = dict(boot_id=module._boot_id(), scope_id=[1, 2], populated=True)
            final = dict(boot_id=expected['boot_id'], scope_id=[1, 2], populated=False)
            def connect():
                db = sqlite3.connect(database); db.row_factory = sqlite3.Row
                return db
            with closing(connect()) as db, db:
                db.execute('CREATE TABLE supervisor_jobs(job_id TEXT, scope_json TEXT, process_json TEXT)')
                db.execute('INSERT INTO supervisor_jobs VALUES(?,?,?)',
                    (job['job_id'], json.dumps(expected), json.dumps(process)))
            parent = root / 'parent'; parent.mkdir()
            scope_path = parent / job['job_id']; scope_path.mkdir()
            scope = SimpleNamespace(path=scope_path, snapshot=lambda: copy.deepcopy(final))
            supervisor = SimpleNamespace(ledger=SimpleNamespace(_connect=connect))
            config = dict(cgroup_parent=str(parent), worker_uid=os.getuid(), worker_gid=os.getgid(),
                          limits=dict(cleanup_seconds=.1, poll_seconds=.01))
            with patch.object(module.LinuxScope, 'reopen', return_value=scope):
                result = module._cleanup_scope(supervisor, job, config, kill=False)
            self.assertEqual(result['process_identity'], process)
            self.assertEqual(result['scope_identity'], expected)
            self.assertEqual(result['final_scope'], final)
            self.assertEqual(result['parent'], str(parent))
            self.assertTrue(result['terminated'] and result['scope_removed'])
            self.assertFalse(scope_path.exists())

@unittest.skipUnless(sys.platform == 'linux', 'Linux runtime source and mapped-loader inventory')
class NativeInventoryTests(unittest.TestCase):
    def setUp(self):
        self.module = ConfigTests().module()
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.base = self.root / 'base'
        roles = dict(python=sys.executable, service_script=str(Path(self.module.__file__).resolve()),
            worker_script=str(Path(self.module.__file__).with_name('server_wine_worker.py').resolve()),
            wine64=str(self.base / 'wine-runtime/usr/lib/wine/wine64'),
            counter_config=str(self.base / 'tokenizer/server-counter-config.json'),
            counter_env=str(Path('/usr/bin/env').resolve(strict=True)),
            counter_runtime_manifest=str(self.base / 'tokenizer/runtime-manifest.json'))
        for name in ('model', 'libllama', 'cl100k'):
            path = self.base / name; path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(('synthetic-' + name).encode()); roles[name] = str(path)
        wine = Path(roles['wine64']); wine.parent.mkdir(parents=True); wine.write_bytes(b'never-executed synthetic Wine')
        counter = dict(counter=dict(command=['env', 'LD_LIBRARY_PATH=' + str(Path(roles['libllama']).parent),
                    sys.executable, '-B'], runtime_manifest=roles['counter_runtime_manifest']),
            model_budget=2000, policy=dict(pipeline_limit=8000, adapter_limit=8192,
            **{name + '_sha256': hashlib.sha256(Path(roles[name]).read_bytes()).hexdigest()
               for name in ('model', 'libllama', 'cl100k')}))
        Path(roles['counter_config']).parent.mkdir(parents=True)
        Path(roles['counter_config']).write_bytes(encoded(counter))
        Path(roles['counter_runtime_manifest']).write_bytes(encoded(dict(tiktoken='0.12.0', files=[{'synthetic': True}])))
        paths = {Path(value).resolve() for value in roles.values()}
        paths.update(Path(value) for value in self.module._mapped_executables())
        if sys.prefix != sys.base_prefix:
            # The target server invokes this suite through its actual venv.
            # Its configuration is part of that environment's identity.
            paths.add((Path(sys.prefix) / 'pyvenv.cfg').resolve(strict=True))
        directory = Path(self.module.__file__).parent
        for name in ('remote_jobs.py', 'server_termination_proof.py', 'server_admission.py', 'server_release.py', 'server_supervisor.py',
                     'server_receipt_inspect.py', 'snapshot_import.py', 'snapshot_wire.py',
                     'windows_snapshot.py', 'tokenizer_service.py', 'server_path_guard.py',
                     'server_service_descriptor.py', 'server_receipt_query.py', 'server_rpc.py'):
            paths.add(directory / name)
        self.native = dict(schema='paperflow-native-runtime/1', reviewed_dependency_closure=False,
            verification_scope='pinned-known-files',
            python_environment=dict(prefix=sys.prefix, base_prefix=sys.base_prefix),
            roles=roles, files=[dict(path=str(path), bytes=path.stat().st_size,
                sha256=hashlib.sha256(path.read_bytes()).hexdigest()) for path in sorted(paths)])
        self.config = dict(base=str(self.base), native_manifest=str(self.root / 'native.json'))
        self.counter = counter
        self.save()

    def save(self):
        raw = encoded(self.native)
        Path(self.config['native_manifest']).write_bytes(raw)
        self.config['native_manifest_sha256'] = hashlib.sha256(raw).hexdigest()

    def save_counter(self):
        path = Path(self.native['roles']['counter_config'])
        raw = encoded(self.counter); path.write_bytes(raw)
        ref = next(row for row in self.native['files'] if row['path'] == str(path))
        ref.update(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
        self.save()

    def test_actual_native_files_and_mapped_libraries_match_inventory(self):
        result = self.module.verify_native_runtime(self.config)
        self.assertEqual(result['roles'], self.native['roles'])

    def test_pinned_known_files_scope_does_not_claim_complete_dependency_closure(self):
        result = self.module.verify_native_runtime(self.config)
        self.assertIs(result['reviewed_dependency_closure'], False)
        self.assertEqual(result['verification_scope'], 'pinned-known-files')

    def test_unscoped_or_falsely_complete_inventory_is_rejected(self):
        for scope, reviewed in [(None, False), ('other', False), ('pinned-known-files', True)]:
            with self.subTest(scope=scope, reviewed=reviewed):
                self.native['verification_scope'] = scope
                self.native['reviewed_dependency_closure'] = reviewed
                self.save()
                with self.assertRaises(self.module.ServiceError):
                    self.module.verify_native_runtime(self.config)

    def test_counter_env_must_be_the_actual_executable_resolved_on_the_service_path(self):
        alternate = self.base / 'different-env'; alternate.write_bytes(b'synthetic never executed env')
        alternate.chmod(0o700)
        self.native['roles']['counter_env'] = str(alternate)
        self.native['files'].append(dict(path=str(alternate), bytes=alternate.stat().st_size,
            sha256=hashlib.sha256(alternate.read_bytes()).hexdigest()))
        self.save()
        with self.assertRaises(self.module.ServiceError):
            self.module.verify_native_runtime(self.config)

    def test_counter_cannot_use_another_python_alias_or_environment_override(self):
        alias = self.base / 'another-python'; alias.symlink_to(Path(sys.executable).resolve())
        for command in (
            ['env', 'LD_LIBRARY_PATH=' + str(self.base), str(alias), '-B'],
            ['env', 'PATH=/unreviewed', sys.executable, '-B'],
            ['env', '-S', sys.executable, '-B'],
        ):
            with self.subTest(command=command):
                self.counter['counter']['command'] = command; self.save_counter()
                with self.assertRaises(self.module.ServiceError):
                    self.module.verify_native_runtime(self.config)

    def test_relative_env_uses_service_path_not_interactive_shell_path(self):
        with patch.dict(os.environ, PATH='/unreviewed/interactive-shell-only'):
            self.module.verify_native_runtime(self.config)
        self.counter['counter']['command'][0] = '/usr/bin/env'; self.save_counter()
        self.module.verify_native_runtime(self.config)

    def test_actual_counter_runtime_manifest_must_be_bound_to_the_fixed_role(self):
        alternate = self.base / 'not-in-inventory.json'; alternate.write_bytes(b'{}')
        self.counter['counter']['runtime_manifest'] = str(alternate); self.save_counter()
        with self.assertRaises(self.module.ServiceError):
            self.module.verify_native_runtime(self.config)

    def test_counter_env_and_runtime_manifest_roles_cannot_be_omitted(self):
        for name in ('counter_env', 'counter_runtime_manifest'):
            original = self.native['roles'].pop(name); self.save()
            with self.subTest(role=name), self.assertRaises(self.module.ServiceError):
                self.module.verify_native_runtime(self.config)
            self.native['roles'][name] = original

    def test_counter_manifest_body_drift_is_detected_by_native_inventory(self):
        Path(self.native['roles']['counter_runtime_manifest']).write_bytes(b'{"changed":true}')
        with self.assertRaises(self.module.ServiceError):
            self.module.verify_native_runtime(self.config)

    def test_another_alias_of_same_python_binary_does_not_preserve_invocation_environment(self):
        alias = self.root / 'other-python'; alias.symlink_to(Path(sys.executable).resolve())
        self.native['roles']['python'] = str(alias); self.save()
        with self.assertRaises(self.module.ServiceError):
            self.module.verify_native_runtime(self.config)

    def test_actual_python_prefix_must_match_reviewed_environment(self):
        self.native['python_environment']['prefix'] = str(self.root / 'different-environment'); self.save()
        with self.assertRaises(self.module.ServiceError):
            self.module.verify_native_runtime(self.config)

    def test_virtual_environment_requires_pinned_pyvenv_configuration(self):
        prefix = self.root / 'venv'; (prefix / 'bin').mkdir(parents=True)
        executable = prefix / 'bin/python'; executable.symlink_to(Path(sys.executable).resolve())
        self.native['roles']['python'] = str(executable)
        self.native['python_environment']['prefix'] = str(prefix)
        self.counter['counter']['command'][-2] = str(executable)
        self.save_counter()
        cfg = prefix / 'pyvenv.cfg'; cfg.write_bytes(b'home = /usr/bin\n')
        self.save()
        with patch.object(sys, 'prefix', str(prefix)), patch.object(sys, 'executable', str(executable)):
            with self.assertRaises(self.module.ServiceError):
                self.module.verify_native_runtime(self.config)
            self.native['files'].append(dict(path=str(cfg), bytes=cfg.stat().st_size,
                sha256=hashlib.sha256(cfg.read_bytes()).hexdigest()))
            self.save()
            self.module.verify_native_runtime(self.config)

    def test_tampered_native_dependency_is_rejected_before_execution(self):
        Path(self.native['roles']['model']).write_bytes(b'tampered')
        with self.assertRaises(self.module.ServiceError):
            self.module.verify_native_runtime(self.config)

    def test_missing_service_source_or_mapped_library_is_rejected(self):
        path = str(Path(self.module.__file__).with_name('server_supervisor.py'))
        self.native['files'] = [row for row in self.native['files'] if row['path'] != path]
        self.save()
        with self.assertRaises(self.module.ServiceError):
            self.module.verify_native_runtime(self.config)

    def test_original_counter_budget_is_required_and_must_match_static_worker_config(self):
        config = json.loads(Path(self.native['roles']['counter_config']).read_bytes())
        contract = dict(embedding_model_budget=2048, cl_limit=8000,
            **{key: config['policy'][key] for key in ('model_sha256', 'libllama_sha256', 'cl100k_sha256')})
        with self.assertRaises(self.module.ServiceError):
            self.module._verify_counter_contract(dict(counter_contract=contract), self.native)
        contract['embedding_model_budget'] = 2000
        self.module._verify_counter_contract(dict(counter_contract=contract), self.native)
        with self.assertRaises(self.module.ServiceError):
            self.module._verify_counter_contract({}, self.native)


if __name__ == '__main__':
    unittest.main()
