"""Native contract probes; adverse native behavior is evidence, not repaired here.

Break caught: replacing shared storage with per-process defaults loses submissions;
assuming DB persistence rehydrates the task queue loses restart consumption;
assuming retry frees a worker / crash frees a lease overstates capacity.
"""
import importlib.util
import json
import os
from pathlib import Path
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
LAB = ROOT / 'runtime/development/pipeline-v2/native-lab'


class PipelineInterfaces(unittest.TestCase):
    def test_baseline_implementation_exists(self):
        self.assertTrue((ROOT / 'tools/pipeline_baseline.py').is_file(),
                        'baseline exporter must exist before scope can be frozen')

    def test_lab_implementation_exists(self):
        self.assertTrue((ROOT / 'tests/pipeline_lab.py').is_file(),
                        'isolated subprocess lab must exist')

    def test_network_guard_denies_unlisted_loopback_service(self):
        import socket
        import subprocess
        import sys
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            listener.listen()
            port = listener.getsockname()[1]
            script = (
                'import socket,sys;sys.path.insert(0,sys.argv[1]);'
                'from pipeline_lab import _deny_external_network;_deny_external_network();'
                '\ntry: socket.socket().connect(("127.0.0.1",int(sys.argv[2])))'
                '\nexcept PermissionError: print("denied")'
                '\nelse: print("allowed")')
            result = subprocess.run([sys.executable, '-c', script, str(ROOT / 'tests'), str(port)],
                capture_output=True, text=True, env=dict(os.environ,
                    PREFECT_API_URL='http://127.0.0.1:1/api', PIPELINE_FAKE_URL='http://127.0.0.1:2',
                    PYTHONDONTWRITEBYTECODE='1'))
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), 'denied',
                             'unlisted local services must be unreachable from lab roles')


@unittest.skipUnless((ROOT / 'tests/pipeline_lab.py').is_file(), 'lab not implemented yet')
class PrefectNativeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pipeline_lab import start_lab
        cls.lab = start_lab(LAB)
        cls.observations = {'schema': 'paperflow-prefect-capability/1',
                            'sdk_version': cls.lab.sdk_version,
                            'api_url': cls.lab.api_url,
                            'storage_root': str(cls.lab.storage_root),
                            'allowed_network': cls.lab.allowed_network,
                            'real_model_requests': 0, 'real_rag_writes': 0}

    @classmethod
    def tearDownClass(cls):
        cls.observations['production_ready'] = False
        cls.observations['next_tasks'] = [4, 7]
        cls.observations['red_lights'] = [
            {'task': 4, 'case': 'native_retry_wait_occupies_worker_capacity',
             'observed': cls.observations.get('native_retry', {}).get('retry_holds_worker_capacity')},
            {'task': 7, 'case': 'server_restart_does_not_rehydrate_scheduled_task_queue',
             'observed': not cls.observations.get('server_restart', {}).get('consumed', True)},
            {'task': 7, 'case': 'memory_lease_records_lost_across_server_restart',
             'observed': cls.observations.get('server_restart_held_lease', {}).get('orphaned_db_slot')},
        ]
        cls.lab.close()
        (LAB.parent / 'prefect-capability.json').write_text(
            json.dumps(cls.observations, indent=2), encoding='utf-8')

    def test_process_independent_parameters_and_results(self):
        submitted = self.lab.submit('echo', 'process-independent', 17)
        self.assertEqual(submitted['exit_code'], 0)
        worker = self.lab.worker('echo')
        try:
            result = self.lab.result(submitted['task_run_id'])
            self.assertEqual(result['value'], 34)
            self.assertNotEqual(result['worker_pid'], submitted['submitter_pid'])
            self.assertNotEqual(result['worker_pid'], result['reader_pid'])
            self.assertEqual(result['token'], 'process-independent')
            self.observations['cross_process_storage'] = result
        finally:
            self.lab.stop(worker)

    def test_worker_restart_consumes_queued_work(self):
        first = self.lab.worker('echo')
        identity = self.lab.wait_worker(first)
        first_pid = identity['pid']
        self.lab.stop(first)
        submitted = self.lab.submit('echo', 'worker-restart', 3)
        replacement = self.lab.worker('echo')
        try:
            result = self.lab.result(submitted['task_run_id'])
            self.assertEqual(result['value'], 6)
            self.assertNotEqual(first_pid, result['worker_pid'])
            self.observations['worker_restart'] = {'consumed': True, **result}
        finally:
            self.lab.stop(replacement)

    def test_server_restart_does_not_imply_queue_rehydration(self):
        from pipeline_lab import _http
        submitted = self.lab.submit('echo', 'server-restart', 5)
        before = self.lab.task_state(submitted['task_run_id'])
        self.assertEqual(before['state_type'], 'SCHEDULED')
        _http(self.lab.api_url + '/v2/concurrency_limits/', 'POST',
              {'name': 'lab-server-lease', 'limit': 1, 'active': True})
        lease = _http(self.lab.api_url + '/v2/concurrency_limits/increment-with-lease',
                      'POST', {'names': ['lab-server-lease'], 'slots': 1,
                               'mode': 'concurrency', 'lease_duration': 60})
        lease_started = time.monotonic()
        self.assertEqual(_http(self.lab.api_url + '/v2/concurrency_limits/lab-server-lease')['active_slots'], 1)
        self.lab.restart_server()
        replacement = self.lab.worker('echo')
        try:
            # Observe actual execution, not just a surviving SQL task_run row.
            result = self.lab.result(submitted['task_run_id'], timeout=25, required=False)
            after = self.lab.task_state(submitted['task_run_id'])
            observed = {'task_run_id': submitted['task_run_id'],
                        'before': before['state_type'], 'after': after['state_type'],
                        'observation_seconds': 25, 'consumed': result is not None,
                        'result': result}
            self.observations['server_restart'] = observed
            self.assertIn(after['state_type'], ['SCHEDULED', 'COMPLETED'])
            if result is not None:
                self.assertEqual(result['value'], 10)
            # The default memory backend can lose the lease while retaining its
            # active_slots in SQL. Do not decrement/patch it to hide the loss.
            samples = []
            while time.monotonic() - lease_started < 75:
                elapsed = time.monotonic() - lease_started
                slots = _http(self.lab.api_url + '/v2/concurrency_limits/lab-server-lease')['active_slots']
                samples.append({'seconds': round(elapsed, 3), 'active_slots': slots})
                time.sleep(1)
            final_slots = _http(self.lab.api_url + '/v2/concurrency_limits/lab-server-lease')['active_slots']
            self.observations['server_restart_held_lease'] = {
                'lease_seconds': 60, 'observation_seconds': round(time.monotonic() - lease_started, 3),
                'lease_id': lease['lease_id'], 'final_active_slots': final_slots,
                'orphaned_db_slot': final_slots == 1, 'samples': samples,
                'required_followup': 'Task 7: evaluate native filesystem lease backend and recovery'}
        finally:
            self.lab.stop(replacement)

    def test_native_retry_wait_capacity_is_measured(self):
        worker = self.lab.worker('retry')
        try:
            first = self.lab.submit('retry', 'retry-first', 1)
            failed_attempt = self.lab.wait_event('retry-first', 'attempt-1')
            second = self.lab.submit('retry', 'retry-second', 2)
            result1 = self.lab.result(first['task_run_id'])
            result2 = self.lab.result(second['task_run_id'])
            first_retry = self.lab.wait_event('retry-first', 'attempt-2')
            second_start = self.lab.wait_event('retry-second', 'attempt-1')
            held = second_start['at'] >= first_retry['at']
            observation = {'worker_limit': 1, 'retry_delay_seconds': 12,
                           'retry_holds_worker_capacity': held,
                           'first_failure': failed_attempt, 'first_retry': first_retry,
                           'second_start': second_start,
                           'http_slots_during_wait': self.lab.http_wait_slots()}
            self.observations['native_retry'] = observation
            self.assertEqual(result1['value'], 2)
            self.assertEqual(result2['value'], 4)
            self.assertGreaterEqual(first_retry['at'] - failed_attempt['at'], 10)
        finally:
            self.lab.stop(worker)

    def test_60_second_lease_reclaims_after_holder_crash(self):
        observation = self.lab.measure_lease_recycle(lease_seconds=60, timeout=100)
        self.observations['lease'] = observation
        self.assertEqual(observation['slots_after_acquire'], 1)
        self.assertTrue(observation['holder_killed'])
        # A failure is preserved as a blocking capability, never patched in SQL.
        self.assertTrue(observation['reclaimed'], json.dumps(observation))
        self.assertGreaterEqual(observation['recycle_seconds'], 55)
        self.assertEqual(observation['new_holder_slots'], 1)

    def test_loopback_fake_http_is_the_only_business_endpoint(self):
        response = self.lab.fake_request({'reference': 'fixture-only'})
        self.assertEqual(response['reference'], 'fixture-only')
        self.assertEqual(response['kind'], 'fake-model-rag-storage')
        self.assertTrue(self.lab.storage_root.is_absolute())
        self.assertTrue(self.lab.storage_root.is_relative_to(ROOT))
        self.assertTrue(all('127.0.0.1' in item for item in self.lab.allowed_network))

    def test_baseline_freezes_membership_and_indexes_each_paper(self):
        import sys
        sys.path.insert(0, str(ROOT / 'tools'))
        from pipeline_baseline import capture_baseline
        fixture = self.lab.root / 'baseline-fixture'
        fixture.mkdir()
        documents = {
            'config.json': {'prefect_api_url': self.lab.api_url,
                            'limits': {'body_llm': 100}, 'api_key': '<REDACTED_SECRET>'},
            'scope.json': {'groups': 2, 'files': [
                {'group': 'person-A', 'relative': 'person-A/paper.pdf', 'sha256': 'a' * 64},
                {'group': 'person-B', 'relative': 'person-B/paper.docx', 'sha256': 'b' * 64}]},
            'diagnosis.json': {'papers': [{'person': 'person-A', 'paper_id': 'paper-A',
                                         'category': 'interrupted_outcome_unknown'}]},
            'closed.json': {'status': 'passed', 'summary': {'total': 100}},
        }
        for name, data in documents.items():
            (fixture / name).write_text(json.dumps(data), encoding='utf-8')
        manifest = fixture / 'freeze.json'
        manifest.write_text(json.dumps({'production_root': str(fixture),
            'copy_manifest': str(fixture / 'scope.json'),
            'diagnosis': str(fixture / 'diagnosis.json'),
            'morning_closeout': str(fixture / 'closed.json')}), encoding='utf-8')
        output = fixture / 'out'
        report = capture_baseline(manifest, output)
        data = json.loads(report.read_text(encoding='utf-8'))
        self.assertEqual(data['scope']['groups'], 2)
        index = json.loads((output / 'paper-evidence-index.json').read_text(encoding='utf-8'))
        self.assertEqual([row['person'] for row in index], ['person-A', 'person-B'])
        self.assertEqual(index[0]['historical_category'], 'interrupted_outcome_unknown')
        self.assertEqual(index[1]['historical_category'], 'not_in_diagnostic_snapshot')
        self.assertEqual(data['morning_100']['exclude_from_reexecution'], True)
        self.assertEqual(data['production_api']['server_version']['data'], '3.9.0')
        self.assertNotIn('secret-must-not-export', report.read_text(encoding='utf-8'))
