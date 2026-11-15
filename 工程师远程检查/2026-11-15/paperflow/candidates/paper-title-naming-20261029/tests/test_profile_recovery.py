import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from paperflow_prefect.records import read_json, write_json, sha_file
from paperflow_prefect.settings import Settings


class ProfileRecoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from paperflow_prefect.legacy import bind
        bind(Settings.load())

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.work = self.root / 'paper/body/original'
        self.work.mkdir(parents=True)
        self.source = self.root / 'source.docx'
        self.source.write_bytes(b'original source')
        self.template = self.root / 'template.md'
        self.template.write_text('unchanged template', encoding='utf8')
        material = {'source': {'file': str(self.source), 'source_sha256': sha_file(self.source),
                    'text_sha256': 'text', 'index_sha256': 'index', 'catalog_sha256': 'catalog'}}
        write_json(self.work / 'material.json', material)
        self.request = {'model': 'fixture', 'messages': [
            {'role': 'system', 'content': self.template.read_text()},
            {'role': 'user', 'content': [{'type': 'text', 'text': json.dumps({'material': material})}]}],
            'max_tokens': 100}
        write_json(self.work / 'model/request.json', self.request)
        from preextract_scheduler_adapter import _request_hash
        self.state = {'status': 'response_complete', 'request_sha256': _request_hash(self.request),
            'stored_request_sha256': sha_file(self.work / 'model/request.json'),
            'started_at': 1, 'finished_at': 2, 'http_invoked': True, 'client_http_requests': 1,
            'finish_reason': 'stop'}
        self.connection = {'model': 'fixture', 'base_url': 'http://127.0.0.1:9/v1',
                           'api_key': 'fixture-only', 'timeout': 1}

    def response(self, text, **state):
        p = self.work / 'model/response.txt'
        p.write_text(text, encoding='utf8')
        event = {'choices': [{'message': {'content': text}, 'finish_reason': 'stop'}]}
        write_json(self.work / 'model/response-events.jsonl', event)
        self.state.update(response_sha256=sha_file(p), **state)
        write_json(self.work / 'model/request-state.json', self.state)

    def failure(self, *, http_status=429):
        payload = b'{"error":{"code":"rate_limited"}}'
        (self.work / 'model/http-error.txt').write_bytes(payload)
        self.state.update(status='request_failed_or_uncertain', error_type='HTTPError',
            http_status=http_status, error_response_sha256=sha_file(self.work / 'model/http-error.txt'))
        self.state.pop('finish_reason', None)
        write_json(self.work / 'model/request-state.json', self.state)

    def original_hashes(self):
        return {p.name: sha_file(p) for p in (self.work / 'model').iterdir() if p.is_file()}

    def api(self):
        from paperflow_prefect import profile_recovery
        return profile_recovery

    def fake_success(self, request, connection, directory):
        from preextract_request import request_once
        class Reply(io.BytesIO): headers = {'Content-Type': 'application/json'}
        class Opener:
            def open(self, *a, **kw):
                return Reply(b'{"choices":[{"message":{"content":"{\\"value\\":1}"},"finish_reason":"stop"}]}')
        return request_once(request, connection, directory, opener=Opener())

    def native_http_failure(self, directory, status=503):
        from preextract_request import request_once
        class Opener:
            def open(self, *a, **kw):
                raise HTTPError('http://fixture.invalid', status, 'fixture', {}, io.BytesIO(b'{"error":"busy"}'))
        with patch('preextract_throttle.record_rate_limit'), self.assertRaises(HTTPError) as raised:
            request_once(self.request, self.connection, directory, opener=Opener())
        raised.exception.close()

    def test_native_syntax_repair_preserves_original_and_saves_diff_without_http(self):
        self.response('{"value": "keep every word", "amount": 17,}')
        before = self.original_hashes()
        with patch('urllib.request.OpenerDirector.open', side_effect=AssertionError('network forbidden')):
            report = self.api().repair_saved_profile(self.work)
        self.assertEqual(report['status'], 'parsed')
        self.assertEqual(read_json(report['candidate']), {'value': 'keep every word', 'amount': 17})
        self.assertIn('trailing_comma_only', [r['policy'] for r in report['repairs']])
        self.assertTrue(Path(report['diff']).read_text(encoding='utf8'))
        self.assertEqual(report['model_requests_added'], 0)
        self.assertEqual(self.original_hashes(), before)

    def test_ambiguous_quote_is_reported_without_guessing_or_model_call(self):
        self.response('{"value":"before "unescaped words" after"}')
        before = self.original_hashes()
        report = self.api().repair_saved_profile(self.work)
        self.assertEqual(report['status'], 'format_error')
        self.assertEqual(report['error']['code'], 'profile_format_error')
        self.assertGreater(report['error']['column'], 0)
        self.assertNotIn('candidate', report)
        self.assertEqual(self.original_hashes(), before)

    def test_gateway_reset_and_idle_timeout_remain_unknown_even_with_stop(self):
        for cause, code in [('ECONNRESET', 'upstream_interrupted'), ('IDLE_TIMEOUT', 'timeout_unknown')]:
            with self.subTest(cause=cause):
                self.response('partial', status='request_failed_or_uncertain',
                    error_kind='explicit_stream_error', error_type='ModelExplicitError',
                    model_error_code='upstream_stream_error')
                event = {'error': {'code': 'upstream_stream_error', 'message': cause}}
                (self.work / 'model/response-events.jsonl').write_text('data: '+json.dumps(event)+'\n', encoding='utf8')
                observation = self.api().inspect_request(self.work)
                self.assertEqual(observation['outcome'], 'unknown')
                self.assertFalse(observation['retry_allowed'])
                self.assertEqual(observation['code'], code)
                sent = []
                with self.assertRaises(self.api().ProfileRecoveryError):
                    self.api().retry_saved_request(self.work, self.source, self.template,
                        self.connection, lambda *args: sent.append(args))
                self.assertEqual(sent, [])
                self.assertFalse((self.work / 'revisions').exists())

    def test_local_timeout_without_gateway_event_is_unknown(self):
        self.state.update(status='request_failed_or_uncertain', error_type='TimeoutError')
        write_json(self.work / 'model/request-state.json', self.state)
        observed = self.api().inspect_request(self.work)
        self.assertEqual(observed['code'], 'timeout_unknown')
        self.assertEqual(observed['outcome'], 'unknown')

    def test_complete_saved_event_is_adopted_without_rewriting_request_state(self):
        self.response('{"value": 1}', status='request_failed_or_uncertain')
        before = self.original_hashes()
        report = self.api().repair_saved_profile(self.work)
        self.assertEqual(report['status'], 'parsed')
        self.assertTrue((self.work / 'model/adopted-response.json').is_file())
        for name, digest in before.items(): self.assertEqual(sha_file(self.work / 'model' / name), digest)

    def test_changed_response_is_blocked(self):
        self.response('{"value": 1}')
        (self.work / 'model/response.txt').write_text('{"value": 2}', encoding='utf8')
        with self.assertRaises(self.api().ProfileRecoveryError): self.api().repair_saved_profile(self.work)

    def test_explicit_failure_retries_exact_original_request_once_and_preserves_history(self):
        self.failure()
        before = self.original_hashes()
        calls = []
        def invoke(request, connection, directory):
            from preextract_request import request_once
            self.assertEqual(request, self.request)
            calls.append(str(directory))
            class Reply(io.BytesIO): headers = {'Content-Type': 'application/json'}
            class Opener:
                def open(self, *a, **kw):
                    return Reply(b'{"choices":[{"message":{"content":"{\\"value\\":1}"},"finish_reason":"stop"}]}')
            return request_once(request, connection, directory, opener=Opener())
        result = self.api().retry_saved_request(self.work, self.source, self.template, self.connection, invoke)
        self.assertEqual(result['outcome'], 'complete')
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.original_hashes(), before)
        again = self.api().retry_saved_request(self.work, self.source, self.template, self.connection, invoke)
        self.assertEqual(again['outcome'], 'complete')
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.api().repair_saved_profile(self.work)['status'], 'parsed')

    def test_failed_or_crashed_recovery_cannot_reset_budget(self):
        self.failure()
        calls = []
        def crash(*args): calls.append(args); raise RuntimeError('transport boundary crash')
        with self.assertRaises(self.api().ProfileRecoveryError):
            self.api().retry_saved_request(self.work, self.source, self.template, self.connection, crash)
        with self.assertRaises(self.api().ProfileRecoveryError):
            self.api().retry_saved_request(self.work, self.source, self.template, self.connection, crash)
        self.assertEqual(len(calls), 1)
        child = self.work / 'revisions/profile-recovery-1'
        with self.assertRaises(self.api().ProfileRecoveryError):
            self.api().retry_saved_request(child, self.source, self.template, self.connection, crash)
        self.assertEqual(len(calls), 1)

    def test_not_sent_request_drift_is_blocked_before_invocation(self):
        self.failure()
        self.state.update(http_invoked=False, client_http_requests=0)
        write_json(self.work / 'model/request-state.json', self.state)
        changed = dict(self.request, max_tokens=999)
        write_json(self.work / 'model/request.json', changed)
        calls = []
        with self.assertRaises(self.api().ProfileRecoveryError):
            self.api().retry_saved_request(self.work, self.source, self.template,
                self.connection, lambda *a: calls.append(a))
        self.assertEqual(calls, [])
        self.assertFalse((self.work / self.api().INTENT).exists())

    def test_not_sent_request_file_hash_is_checked_before_invocation(self):
        self.failure()
        self.state.update(http_invoked=False, client_http_requests=0)
        write_json(self.work / 'model/request-state.json', self.state)
        with (self.work / 'model/request.json').open('ab') as stream: stream.write(b'\n')
        calls = []
        with self.assertRaises(self.api().ProfileRecoveryError):
            self.api().retry_saved_request(self.work, self.source, self.template,
                self.connection, lambda *a: calls.append(a))
        self.assertEqual(calls, [])

    def test_native_failure_before_http_can_retry_once(self):
        from preextract_request import request_once
        def fail_before_open(): raise RuntimeError('fixture local failure')
        with self.assertRaises(RuntimeError):
            request_once(self.request, self.connection, self.work / 'model', on_request_started=fail_before_open)
        self.assertEqual(read_json(self.work / 'model/request-state.json')['client_http_requests'], 0)
        calls = []
        def invoke(*args): calls.append(args); return self.fake_success(*args)
        self.api().retry_saved_request(self.work, self.source, self.template, self.connection, invoke)
        self.api().retry_saved_request(self.work, self.source, self.template, self.connection, invoke)
        self.assertEqual(len(calls), 1)

    def test_not_sent_flag_with_partial_stream_still_cannot_resend(self):
        self.response('partial', status='request_failed_or_uncertain', http_invoked=False, client_http_requests=0)
        (self.work / 'model/response-events.jsonl').write_bytes(b'data: {"choices":[]}\n')
        calls = []
        with self.assertRaises(self.api().ProfileRecoveryError):
            self.api().retry_saved_request(self.work, self.source, self.template,
                self.connection, lambda *a: calls.append(a))
        self.assertEqual(calls, [])

    def test_dangling_native_active_revision_blocks_new_attempt(self):
        self.failure()
        write_json(self.work / 'active-revision.json', {'work_dir': str(self.work / 'revisions/missing')})
        calls = []
        with self.assertRaises(self.api().ProfileRecoveryError):
            self.api().retry_saved_request(self.work, self.source, self.template,
                self.connection, lambda *a: calls.append(a))
        self.assertEqual(calls, [])

    def test_existing_native_recovery_consumes_budget_for_every_outcome(self):
        from preextract_pipeline import create_revision
        self.failure()
        for outcome in ('complete', 'failed', 'unknown', 'reserved'):
            with self.subTest(outcome=outcome):
                work = self.root / outcome
                shutil.copytree(self.work, work)
                child = create_revision(self.source, work, 'previous-recovery', mode='rerun',
                    reason='previous approved recovery', recover_failed=True, template_path=self.template)
                if outcome == 'complete': self.fake_success(self.request, self.connection, child / 'model')
                elif outcome == 'failed': self.native_http_failure(child / 'model')
                elif outcome == 'unknown':
                    write_json(child / 'model/request.json', self.request)
                    write_json(child / 'model/request-state.json', dict(self.state, error_type='TimeoutError', http_status=None))
                calls = []
                with self.assertRaises(self.api().ProfileRecoveryError):
                    self.api().retry_saved_request(work, self.source, self.template,
                        self.connection, lambda *a: calls.append(a))
                self.assertEqual(calls, [])
                self.assertFalse((work / self.api().INTENT).exists())

    def test_child_request_drift_stays_blocked_on_later_readback(self):
        self.failure()
        def changed_payload(request, connection, directory):
            return self.fake_success(dict(request, max_tokens=999), connection, directory)
        with self.assertRaises(self.api().ProfileRecoveryError):
            self.api().retry_saved_request(self.work, self.source, self.template, self.connection, changed_payload)
        with self.assertRaises(self.api().ProfileRecoveryError): self.api().inspect_request(self.work)
        with self.assertRaises(self.api().ProfileRecoveryError): self.api().repair_saved_profile(self.work)

    def test_child_response_without_revision_metadata_cannot_be_adopted(self):
        self.failure()
        self.api().retry_saved_request(self.work, self.source, self.template, self.connection, self.fake_success)
        (self.work / 'revisions/profile-recovery-1/revision.json').unlink()
        with self.assertRaises(self.api().ProfileRecoveryError): self.api().inspect_request(self.work)

    def test_native_http_failure_with_empty_event_log_allows_one_retry(self):
        for status in (429, 503):
            with self.subTest(status=status):
                work = self.root / str(status)
                work.mkdir()
                shutil.copyfile(self.work / 'material.json', work / 'material.json')
                self.native_http_failure(work / 'model', status)
                self.assertEqual((work / 'model/response-events.jsonl').stat().st_size, 0)
                calls = []
                def invoke(*args): calls.append(args); return self.fake_success(*args)
                self.assertTrue(self.api().inspect_request(work)['retry_allowed'])
                self.assertEqual(self.api().retry_saved_request(work, self.source, self.template,
                    self.connection, invoke)['outcome'], 'complete')
                self.api().retry_saved_request(work, self.source, self.template, self.connection, invoke)
                self.assertEqual(len(calls), 1)

    def test_http_failure_with_nonempty_event_log_cannot_retry(self):
        self.native_http_failure(self.work / 'model')
        (self.work / 'model/response-events.jsonl').write_bytes(b'data: {"partial":true}\n')
        calls = []
        with self.assertRaises(self.api().ProfileRecoveryError):
            self.api().retry_saved_request(self.work, self.source, self.template,
                self.connection, lambda *a: calls.append(a))
        self.assertEqual(calls, [])

    def test_parent_drift_during_revision_creation_blocks_dispatch(self):
        from preextract_pipeline import create_revision
        self.failure()
        def changed(*args, **kwargs):
            child = create_revision(*args, **kwargs)
            write_json(self.work / 'model/request.json', dict(self.request, max_tokens=999))
            return child
        calls = []
        with patch('preextract_pipeline.create_revision', side_effect=changed):
            with self.assertRaises(self.api().ProfileRecoveryError):
                self.api().retry_saved_request(self.work, self.source, self.template,
                    self.connection, lambda *a: calls.append(a))
        self.assertEqual(calls, [])

    def test_changed_source_template_or_model_cannot_start_retry(self):
        self.failure()
        self.template.write_text('different template')
        calls = []
        with self.assertRaises(self.api().ProfileRecoveryError):
            self.api().retry_saved_request(self.work, self.source, self.template,
                self.connection, lambda *args: calls.append(args))
        self.assertEqual(calls, [])

    def test_stage_history_preserves_failure_before_retry(self):
        from paperflow_prefect.records import Records
        records = Records(self.work)
        records.failed('profile', ValueError('PRIVATE SECRET'))
        original = records.path('profile').read_bytes()
        records.started('profile')
        records.failed('profile', ValueError('SECOND PRIVATE SECRET'))
        history = list((self.work / 'stages/history/profile').glob('*.json'))
        self.assertTrue(any(p.read_bytes() == original for p in history))
        self.assertNotIn('PRIVATE SECRET', records.path('profile').read_text())
        self.assertGreaterEqual(len(history), 3)

    def test_format_error_survives_body_entry_and_keeps_saved_response(self):
        from paperflow_prefect.body import extract_profile
        self.template.write_bytes(Settings.load().path('template_path').read_bytes())
        self.request['messages'][0]['content'] = self.template.read_text(encoding='utf8')
        write_json(self.work / 'model/request.json', self.request)
        from preextract_scheduler_adapter import _request_hash
        self.state['request_sha256'] = _request_hash(self.request)
        self.state['stored_request_sha256'] = sha_file(self.work / 'model/request.json')
        self.response('{"value":"before "broken words" after"}')
        calls = []
        with self.assertRaises(self.api().ProfileRecoveryError) as raised:
            extract_profile(self.work, self.template, {}, self.connection, lambda *a: calls.append(a))
        self.assertEqual(raised.exception.details['code'], 'profile_format_error')
        self.assertTrue(Path(raised.exception.details['diagnostic']).is_file())
        self.assertEqual(calls, [])

    def test_partition_marker_cannot_bypass_unknown_root_request(self):
        from paperflow_prefect.body import extract_profile
        self.template.write_bytes(Settings.load().path('template_path').read_bytes())
        self.response('partial', status='request_failed_or_uncertain', error_type='TimeoutError')
        write_json(self.work / 'model/partition-state.json', {'fixture': True})
        with patch('preextract_partition_run.run_partition') as partition:
            with self.assertRaises(self.api().ProfileRecoveryError):
                extract_profile(self.work, self.template, {}, self.connection, self.fake_success)
        partition.assert_not_called()

    def test_safe_stage_message_exposes_cause_without_provider_text(self):
        from paperflow_prefect.flows import stage_error_message
        error = self.api().ProfileRecoveryError('timeout_unknown')
        self.assertIn('超时', stage_error_message('profile', error))
        self.assertNotIn('PRIVATE', stage_error_message('profile', ValueError('PRIVATE')))

    def test_complete_profile_remains_cached_after_rag_and_nas_failure(self):
        from paperflow_prefect.backend import Backend
        from paperflow_prefect.records import Records
        backend = Backend.__new__(Backend)
        backend.root = self.root / 'data'
        backend.body_signature = 'fixed-original-signature'
        backend.settings = Settings({'execution_enabled': True, 'library_root': str(self.root / 'library'),
                                     'data_root': str(backend.root)})
        paper = {'paper_id': 'PF_fixture', 'revision': 'original',
                 'body': {'path': str(self.source), 'sha256': sha_file(self.source), 'relative': 'body/source.docx'}}
        work = backend.body_work(paper)
        work.mkdir(parents=True)
        artifacts = {}
        for name in ('candidate', 'validation', 'bundle'):
            p = work / (name + '.json'); write_json(p, {'fixture': name}); artifacts[name] = str(p)
        Records(work).complete('profile', artifacts, artifacts.values())
        for stage in ('rag-body', 'nas'): Records(backend.paper_root(paper)).failed(stage, RuntimeError('downstream'))
        with patch('paperflow_prefect.legacy.connection', side_effect=AssertionError('no credential needed')):
            self.assertEqual(backend.profile(paper), artifacts)
            self.assertEqual(backend.profile(paper), artifacts)

    def test_recovery_lock_rejects_concurrent_attempt(self):
        from filelock import Timeout
        self.failure()
        calls = []
        with self.api().recovery_lock(self.work):
            with self.assertRaises(Timeout):
                self.api().retry_saved_request(self.work, self.source, self.template,
                    self.connection, lambda *a: calls.append(a))
        self.assertEqual(calls, [])


if __name__ == '__main__': unittest.main()
