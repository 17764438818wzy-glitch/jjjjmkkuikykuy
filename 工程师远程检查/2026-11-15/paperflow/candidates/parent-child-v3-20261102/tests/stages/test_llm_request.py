"""Real request journal with a synthetic transport and isolated control doubles."""
import asyncio
from contextlib import asynccontextmanager
import io
import json
from pathlib import Path
import sys
from uuid import uuid4

import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKSPACE = ROOT.parents[1]
sys.path[:0] = [str(ROOT), str(WORKSPACE / 'prefect-paperflow'),
                str(WORKSPACE / 'prefect-paperflow/paperflow_prefect/clean_overlay'),
                str(WORKSPACE.parent / '程序/updates/unified-prefect-20261029-material-r02/desktop'),
                str(WORKSPACE.parent / '程序/updates/unified-prefect-20261029-material-r02/core'),
                str(WORKSPACE.parent / '程序/desktop')]


class Admission:
    def __init__(self):
        self.active = True

    def running_sync(self):
        return self.active

    async def running(self):
        return self.active

    async def wait(self):
        if not self.active:
            raise InterruptedError('synthetic task paused')


class Response(io.BytesIO):
    headers = {'Content-Type': 'application/json'}


def dependencies(monkeypatch):
    try:
        from paperflow_v3.stages import llm_request
    except ImportError:
        pytest.fail('The candidate single-request adapter is missing')
    receipts = []

    async def save_receipt(**data):
        receipts.append(data)

    async def read_receipts(_child_flow_run_id):
        return list(reversed(receipts))

    @asynccontextmanager
    async def isolated_slot(admission, limits):
        if hasattr(admission, 'admit'):
            assert await admission.admit()
        if not admission.active:
            raise InterruptedError('synthetic task paused')
        yield

    monkeypatch.setattr(llm_request, 'save_receipt', save_receipt)
    monkeypatch.setattr(llm_request, 'read_receipts', read_receipts, raising=False)
    monkeypatch.setattr(llm_request, 'admitted_slot', isolated_slot)
    return llm_request, receipts


def call(module, admission, work, opener, request_id, identity):
    return asyncio.run(module.run_raw_component(
        identity=identity,
        component_id='body-part-001', request_id=request_id, kind='body',
        request_body_factory=lambda: {'model': 'synthetic', 'messages': []},
        connection={'base_url': 'http://synthetic.invalid/v1', 'api_key': 'synthetic'},
        work=work, admission=admission, limit_names=['synthetic-total'],
        check_history=lambda: None, check_source=lambda: None, opener=opener))


def test_paused_after_send_still_saves_response_and_resume_reuses_it(tmp_path, monkeypatch):
    module, receipts = dependencies(monkeypatch)
    admission = Admission()
    calls, slots = [], []

    @asynccontextmanager
    async def counted_slot(admission, limits):
        assert admission.active
        slots.append(tuple(limits))
        yield

    monkeypatch.setattr(module, 'admitted_slot', counted_slot)

    class Opener:
        def open(self, request, timeout):
            assert [row['kind'] for row in receipts[-2:]] == ['llm_request_intent', 'llm_request_started']
            assert receipts[-1]['data']['component_id'] == 'body-part-001'
            assert receipts[-1]['data']['request_id'] == request_id
            assert receipts[-1]['data']['kind'] == 'body'
            calls.append(request)
            admission.active = False
            return Response(json.dumps({'choices': [{'message': {'content': 'saved reply'},
                'finish_reason': 'stop'}]}).encode())

    request_id = str(uuid4())
    identity = {'task_id': str(uuid4()), 'group_id': 'folder', 'task_run_id': str(uuid4())}
    result = call(module, admission, tmp_path / 'body', Opener(), request_id, identity)
    assert result['status'] == 'raw_response_saved'
    assert Path(result['response_path']).read_text(encoding='utf8') == 'saved reply'
    assert receipts[-1]['kind'] == 'llm_response_saved'
    assert receipts[-1]['data']['response_text'].encode('utf8') == Path(result['response_path']).read_bytes()
    original_receipts = list(receipts)
    assert not admission.active
    admission.active = True
    repeated = call(module, admission, tmp_path / 'body', Opener(), request_id, identity)
    assert repeated == result
    assert len(calls) == 1
    assert len(slots) == 1
    assert receipts == original_receipts


@pytest.mark.parametrize('change', ['request', 'response', 'server_response', 'newer_started', 'newer_failed'])
def test_saved_request_is_not_reused_when_evidence_changes(tmp_path, monkeypatch, change):
    module, receipts = dependencies(monkeypatch)
    admission = Admission()
    calls = []

    class Opener:
        def open(self, _request, timeout):
            calls.append(1)
            return Response(json.dumps({'choices': [{'message': {'content': 'saved reply'},
                'finish_reason': 'stop'}]}).encode())

    request_id = str(uuid4())
    identity = {'task_id': str(uuid4()), 'group_id': 'folder', 'task_run_id': str(uuid4())}
    work = tmp_path / 'body'
    result = call(module, admission, work, Opener(), request_id, identity)
    if change == 'response':
        Path(result['response_path']).write_text('changed', encoding='utf8')
    elif change == 'server_response':
        receipts[-1]['data']['response_text'] = 'changed'
    elif change in {'newer_started', 'newer_failed'}:
        receipts.append({'kind': 'llm_request_started' if change == 'newer_started' else 'llm_request_failed', 'data': {
            'component_id': 'body-part-001', 'request_id': request_id}})
    before = len(receipts)
    options = dict(identity=identity, component_id='body-part-001', request_id=request_id, kind='body',
        request_body_factory=lambda: {'model': 'changed' if change == 'request' else 'synthetic', 'messages': []},
        connection={'base_url': 'http://synthetic.invalid/v1', 'api_key': 'synthetic'}, work=work,
        admission=admission, limit_names=['synthetic'], check_history=lambda: None,
        check_source=lambda: None, opener=Opener())
    with pytest.raises((ValueError, RuntimeError, FileExistsError)):
        asyncio.run(module.run_raw_component(**options))
    assert calls == [1]
    assert len(receipts) == before


def test_started_receipt_without_other_evidence_never_authorizes_sending(tmp_path, monkeypatch):
    module, receipts = dependencies(monkeypatch)
    request_id = str(uuid4())
    receipts.append({'kind': 'llm_request_started', 'data': {
        'component_id': 'body-part-001', 'request_id': request_id}})
    identity = {'task_id': str(uuid4()), 'group_id': 'folder', 'task_run_id': str(uuid4())}

    class NoTransport:
        def open(self, *_args, **_kwargs):
            pytest.fail('started request must not be sent again')

    with pytest.raises(RuntimeError, match='待核对'):
        call(module, Admission(), tmp_path / 'body', NoTransport(), request_id, identity)
    assert len(receipts) == 1
    assert not (tmp_path / 'body').exists()


def test_later_not_sent_does_not_clear_an_unknown_request(tmp_path, monkeypatch):
    module, receipts = dependencies(monkeypatch)
    request_id = str(uuid4())
    data = {'component_id': 'body-part-001', 'request_id': request_id}
    receipts.extend([{'kind': 'llm_request_unknown', 'data': data},
                     {'kind': 'llm_request_not_sent', 'data': data}])
    identity = {'task_id': str(uuid4()), 'group_id': 'folder', 'task_run_id': str(uuid4())}
    with pytest.raises(RuntimeError, match='待核对'):
        call(module, Admission(), tmp_path / 'body', None, request_id, identity)
    assert len(receipts) == 2
    assert not (tmp_path / 'body').exists()


def test_pause_during_history_read_waits_before_reporting_unknown(tmp_path, monkeypatch):
    module, receipts = dependencies(monkeypatch)
    request_id = str(uuid4())
    identity = {'task_id': str(uuid4()), 'group_id': 'folder', 'task_run_id': str(uuid4())}
    receipts.append({'kind': 'llm_request_started', 'data': {
        'component_id': 'body-part-001', 'request_id': request_id}})

    class PausedWhileReading(Admission):
        resumed = 0
        async def wait(self):
            if not self.active:
                self.resumed += 1
                self.active = True

    admission = PausedWhileReading()

    async def read_receipts(_child):
        admission.active = False
        return list(reversed(receipts))

    monkeypatch.setattr(module, 'read_receipts', read_receipts)

    with pytest.raises(RuntimeError, match='待核对'):
        call(module, admission, tmp_path / 'body', None, request_id, identity)
    assert admission.resumed == 1
    assert len(receipts) == 1


def test_unknown_request_is_preserved_and_never_resent(tmp_path, monkeypatch):
    module, receipts = dependencies(monkeypatch)
    admission = Admission()
    calls = []

    class TimeoutOpener:
        def open(self, request, timeout):
            calls.append(request)
            raise TimeoutError('synthetic interrupted transport')

    request_id = str(uuid4())
    identity = {'task_id': str(uuid4()), 'group_id': 'folder', 'task_run_id': str(uuid4())}
    with pytest.raises(TimeoutError):
        call(module, admission, tmp_path / 'body', TimeoutOpener(), request_id, identity)
    assert receipts[-1]['kind'] == 'llm_request_unknown'
    journal = tmp_path / 'body/model/request-state.json'
    saved = journal.read_bytes()
    with pytest.raises(RuntimeError):
        call(module, admission, tmp_path / 'body', TimeoutOpener(), request_id, identity)
    assert journal.read_bytes() == saved
    assert len(calls) == 1


def test_complete_http_failure_is_distinct_from_unknown_and_never_retried_automatically(tmp_path, monkeypatch):
    from urllib.error import HTTPError
    module, receipts = dependencies(monkeypatch)
    admission = Admission()
    calls = []

    class ExplicitFailureOpener:
        def open(self, request, timeout):
            calls.append(request)
            raise HTTPError(request.full_url, 401, 'synthetic failure', {}, io.BytesIO(b'complete failure'))

    request_id = str(uuid4())
    identity = {'task_id': str(uuid4()), 'group_id': 'folder', 'task_run_id': str(uuid4())}
    with pytest.raises(HTTPError):
        call(module, admission, tmp_path / 'body', ExplicitFailureOpener(), request_id, identity)
    assert receipts[-1]['kind'] == 'llm_request_failed'
    assert receipts[-1]['data']['failure_reason'] == 'http_rejection'
    with pytest.raises(RuntimeError, match='待核对|人工'):
        call(module, admission, tmp_path / 'body', ExplicitFailureOpener(), request_id, identity)
    assert len(calls) == 1


@pytest.mark.parametrize('status', [400, 401, 403, 422])
def test_complete_auth_or_parameter_rejection_is_a_known_failure(tmp_path, monkeypatch, status):
    from urllib.error import HTTPError
    module, receipts = dependencies(monkeypatch)
    class Rejected:
        def open(self, request, timeout):
            raise HTTPError(request.full_url, status, 'synthetic rejection', {}, io.BytesIO(b'rejected'))
    identity = {'task_id': str(uuid4()), 'group_id': 'folder', 'task_run_id': str(uuid4())}
    with pytest.raises(HTTPError):
        call(module, Admission(), tmp_path / 'body', Rejected(), str(uuid4()), identity)
    assert receipts[-1]['kind'] == 'llm_request_failed'
    assert receipts[-1]['data']['failure_reason'] == 'http_rejection'
    assert receipts[-1]['data']['http_status'] == status
    assert receipts[-1]['data']['retryable'] is True


@pytest.mark.parametrize('status', [408, 503, 504])
def test_gateway_timeout_status_remains_unknown(tmp_path, monkeypatch, status):
    from urllib.error import HTTPError
    module, receipts = dependencies(monkeypatch)
    class Timeout:
        def open(self, request, timeout):
            raise HTTPError(request.full_url, status, 'synthetic timeout', {}, io.BytesIO(b'timeout'))
    identity = {'task_id': str(uuid4()), 'group_id': 'folder', 'task_run_id': str(uuid4())}
    with pytest.raises(HTTPError):
        call(module, Admission(), tmp_path / 'body', Timeout(), str(uuid4()), identity)
    assert receipts[-1]['kind'] == 'llm_request_unknown'


def test_payload_too_large_is_known_but_not_retried_unchanged(tmp_path, monkeypatch):
    from urllib.error import HTTPError
    module, receipts = dependencies(monkeypatch)
    class Rejected:
        def open(self, request, timeout):
            raise HTTPError(request.full_url, 413, 'synthetic', {}, io.BytesIO(b'payload too large'))
    identity = {'task_id': str(uuid4()), 'group_id': 'folder', 'task_run_id': str(uuid4())}
    with pytest.raises(HTTPError):
        call(module, Admission(), tmp_path / 'body', Rejected(), str(uuid4()), identity)
    assert receipts[-1]['kind'] == 'llm_request_failed'
    assert receipts[-1]['data']['failure_reason'] == 'http_payload_too_large'
    assert receipts[-1]['data']['retryable'] is False


def test_output_limit_is_known_failure_but_cannot_be_retried_unchanged(tmp_path, monkeypatch):
    from preextract_request import IncompleteModelResponse
    module, receipts = dependencies(monkeypatch)
    class Truncated:
        def open(self, _request, timeout):
            return Response(json.dumps({'choices': [{'message': {'content': 'partial result'},
                'finish_reason': 'length'}]}).encode())
    identity = {'task_id': str(uuid4()), 'group_id': 'folder', 'task_run_id': str(uuid4())}
    with pytest.raises(IncompleteModelResponse):
        call(module, Admission(), tmp_path / 'body', Truncated(), 'body-1', identity)
    assert receipts[-1]['kind'] == 'llm_request_failed'
    assert receipts[-1]['data']['failure_reason'] == 'output_length_requires_changed_plan'
    assert receipts[-1]['data']['retryable'] is False
    assert (tmp_path / 'body/model/response.txt').read_text(encoding='utf8') == 'partial result'
    failed = receipts[-1]['data']
    receipts.append({'kind': 'manual_retry', 'data': {'retry_id': 'manual-1', 'state': 'scheduled'}})
    with pytest.raises(RuntimeError, match='调整材料或处理计划'):
        asyncio.run(module.retry_raw_component(identity=identity, component_id='body-part-001',
            retry_id='manual-1', kind='body', original_work=tmp_path / 'body',
            original_model=tmp_path / 'body/model', expected_request_sha256=failed['request_sha256'],
            connection={'base_url': 'http://synthetic.invalid/v1', 'api_key': 'synthetic', 'model': 'synthetic'},
            admission=Admission(), limit_names=['synthetic'], check_source=lambda: None, opener=Truncated()))


def test_server_unknown_blocks_a_new_request_even_if_local_journal_is_missing(tmp_path, monkeypatch):
    module, receipts = dependencies(monkeypatch)
    admission = Admission()
    calls = []

    class TimeoutOpener:
        def open(self, request, timeout):
            calls.append(request)
            raise TimeoutError('synthetic interrupted transport')

    identity = {'task_id': str(uuid4()), 'group_id': 'folder', 'task_run_id': str(uuid4())}
    with pytest.raises(TimeoutError):
        call(module, admission, tmp_path / 'original', TimeoutOpener(), str(uuid4()), identity)
    assert receipts[-1]['kind'] == 'llm_request_unknown'
    with pytest.raises(RuntimeError, match='待核对'):
        call(module, admission, tmp_path / 'another-empty-directory', TimeoutOpener(), str(uuid4()), identity)
    assert len(calls) == 1


def test_real_stream_finishes_when_parent_pauses_between_chunks(tmp_path, monkeypatch):
    module, receipts = dependencies(monkeypatch)
    admission = Admission()

    class Stream:
        headers = {'Content-Type': 'text/event-stream'}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def __iter__(self):
            yield b'data: {"choices":[{"delta":{"content":"first"}}]}\n'
            admission.active = False
            yield b'data: {"choices":[{"delta":{"content":" second"},"finish_reason":"stop"}]}\n'
            yield b'data: [DONE]\n'

    class Opener:
        def open(self, request, timeout):
            return Stream()

    identity = {'task_id': str(uuid4()), 'group_id': 'folder', 'task_run_id': str(uuid4())}
    result = call(module, admission, tmp_path / 'body', Opener(), str(uuid4()), identity)
    assert Path(result['response_path']).read_text(encoding='utf8') == 'first second'
    state = json.loads((tmp_path / 'body/model/request-state.json').read_text(encoding='utf8'))
    assert state['status'] == 'response_complete'
    assert state['finish_reason'] == 'stop'
    assert receipts[-1]['kind'] == 'llm_response_saved'


def test_pause_after_intent_prevents_actual_transport(tmp_path, monkeypatch):
    module, receipts = dependencies(monkeypatch)
    admission = Admission()

    async def pause_after_saving(**data):
        receipts.append(data)
        if data['kind'] == 'llm_request_intent':
            admission.active = False

    monkeypatch.setattr(module, 'save_receipt', pause_after_saving)

    calls = []

    class Opener:
        def open(self, request, timeout):
            calls.append(request)
            return Response(json.dumps({'choices': [{'message': {'content': 'after resume'},
                'finish_reason': 'stop'}]}).encode())

    identity = {'task_id': str(uuid4()), 'group_id': 'folder', 'task_run_id': str(uuid4())}
    request_id = str(uuid4())
    with pytest.raises(InterruptedError):
        call(module, admission, tmp_path / 'body', Opener(), request_id, identity)
    assert receipts[-1]['kind'] == 'llm_request_not_sent'
    assert not any(row['kind'] == 'llm_request_started' for row in receipts)
    assert not calls
    admission.active = True

    async def save_without_pausing(**data):
        receipts.append(data)

    monkeypatch.setattr(module, 'save_receipt', save_without_pausing)
    assert call(module, admission, tmp_path / 'body', Opener(), request_id, identity)['status'] == 'raw_response_saved'
    assert len(calls) == 1


def test_local_request_without_server_binding_is_not_adopted_as_this_folder(tmp_path, monkeypatch):
    module, receipts = dependencies(monkeypatch)
    admission = Admission()

    class Opener:
        def open(self, request, timeout):
            return Response(json.dumps({'choices': [{'message': {'content': 'reply'},
                'finish_reason': 'stop'}]}).encode())

    identity = {'task_id': str(uuid4()), 'group_id': 'folder', 'task_run_id': str(uuid4())}
    request_id = str(uuid4())
    call(module, admission, tmp_path / 'body', Opener(), request_id, identity)
    receipts.clear()
    with pytest.raises(RuntimeError, match='待核对'):
        call(module, admission, tmp_path / 'body', Opener(), request_id, identity)


def test_unbound_partial_request_file_is_not_overwritten(tmp_path, monkeypatch):
    module, receipts = dependencies(monkeypatch)
    admission = Admission()
    journal = tmp_path / 'body/model/request.json'
    journal.parent.mkdir(parents=True)
    journal.write_text('{"earlier":"unknown"}', encoding='utf8')

    class Opener:
        def open(self, *_args, **_kwargs):
            pytest.fail('unbound prior request must not be sent again')

    identity = {'task_id': str(uuid4()), 'group_id': 'folder', 'task_run_id': str(uuid4())}
    with pytest.raises(RuntimeError, match='待核对'):
        call(module, admission, tmp_path / 'body', Opener(), str(uuid4()), identity)
    assert journal.read_text(encoding='utf8') == '{"earlier":"unknown"}'
    assert not receipts
