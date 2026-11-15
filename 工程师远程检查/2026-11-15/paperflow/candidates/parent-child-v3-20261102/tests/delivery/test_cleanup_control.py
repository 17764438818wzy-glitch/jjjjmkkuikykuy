"""Real cleanup files and Prefect HTTP client; no business services."""
import copy
from contextlib import nullcontext
from threading import Event
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from prefect.client.orchestration import SyncPrefectClient

from test_cleanup import delivery
from test_stage_slots import delivery_harness


@pytest.fixture
def control(monkeypatch):
    from paperflow_v3.control import stage_admission as stages
    from paperflow_v3.control.stage_queue import bind_phase

    ids = dict(task_id='t', group_id='g', task_run_id='child')
    task_key = 'paperflow_v3_task_t'
    values = {stages._SESSION: 'session-one', task_key: {'state': 'running'}}
    requests = []
    handshakes = []
    state = {'error': False}
    row_ids = {name: str(uuid4()) for name in values}

    def row(name):
        return {'id': row_ids[name], 'created': '2026-11-06T00:00:00Z',
                'updated': '2026-11-06T00:00:00Z', 'name': name,
                'value': copy.deepcopy(values[name]), 'tags': []}

    def transport(request):
        import json
        body = json.loads(request.content) if request.content else None
        if request.url.path == '/api/csrf-token':
            handshakes.append(request.url.path)
            return httpx.Response(404)
        requests.append((request.method, request.url.path, body))
        if state['error']:
            raise httpx.ReadTimeout('synthetic control read failed', request=request)
        reply = state.get('reply')
        if reply in (403, 500):
            return httpx.Response(reply, json={'detail': 'synthetic control rejection'})
        if reply == 'invalid-json':
            return httpx.Response(200, content=b'not-json', headers={'Content-Type': 'application/json'})
        if reply == 'invalid-schema':
            return httpx.Response(200, json=[{'id': 'not-a-uuid', 'name': 'control', 'value': True}])
        if request.method == 'GET':
            name = request.url.path.rsplit('/', 1)[-1]
            return httpx.Response(200, json=row(name)) if name in values else httpx.Response(404)
        if request.method == 'POST' and request.url.path == '/api/variables/filter':
            names = body.get('variables', {}).get('name', {}).get('any_', [])
            if set(names) != {stages._SESSION, task_key} or len(names) != 2 or body.get('limit') != 2:
                return httpx.Response(422, json={'detail': 'exact two-variable filter required'})
            return httpx.Response(200, json=[row(name) for name in names if name in values])
        return httpx.Response(404)

    http_transport = httpx.MockTransport(transport)
    stage = stages.FolderStage(SimpleNamespace(running_sync=lambda: False), ids, 'delivery')
    stage.admitted, stage.session = True, 'session-one'
    phase = SimpleNamespace(failed=Event())
    # The production delivery thread retains one SyncClientContext for the
    # whole paper. Keep the real client's reentrant context, including CSRF.
    with SyncPrefectClient('http://cleanup-control.test/api',
            httpx_settings={'transport': http_transport, 'timeout': 1}) as client, bind_phase(phase):
        monkeypatch.setattr(stages, 'get_client', lambda **kwargs: client)
        yield SimpleNamespace(stage=stage, values=values, task_key=task_key,
                              session_key=stages._SESSION, requests=requests, handshakes=handshakes,
                              state=state, phase=phase,
                              check=lambda: getattr(stage, 'cleanup_running_sync', stage.running_sync)())


def invoke(delivery, check, save=None, state=None):
    from paperflow_v3.delivery.cleanup import cleanup_paper
    paper, ids, handoff, packet, body, attachments, archive, extras, *_ = delivery
    state = {} if state is None else state
    return cleanup_paper(paper, ids, handoff, packet, body, attachments, archive, extras,
                         state, save or (lambda _: 'saved-local'), check)


def test_real_cleanup_uses_one_fresh_http_request_per_file(delivery, control):
    result = invoke(delivery, control.check)
    assert result['status'] == 'complete'
    assert len(result['files']) == 6
    # One initial permission read plus one fresh read immediately before each unlink.
    assert len(control.requests) == 7
    assert all(method == 'POST' and path == '/api/variables/filter'
               for method, path, _ in control.requests)
    assert (delivery[-1] / 'body.docx').is_file()
    assert (delivery[-4] / 'trash.txt').is_file()


@pytest.mark.parametrize(('change', 'allowed'), [
    ('none', True), ('paused', True), ('deleting', False), ('new_session', False),
    ('missing_session', False), ('missing_task_state', True),
])
def test_bulk_permission_keeps_current_session_and_pause_semantics(control, change, allowed):
    if change == 'paused':
        control.values[control.task_key] = {'state': 'paused'}
        control.stage.stop.set()  # Already-admitted stages still drain, as before.
    elif change == 'deleting':
        control.values[control.task_key] = {'state': 'deleting'}
    elif change == 'new_session':
        control.values[control.session_key] = 'session-two'
    elif change == 'missing_session':
        del control.values[control.session_key]
    elif change == 'missing_task_state':
        del control.values[control.task_key]
    assert control.check() is allowed
    assert len(control.requests) == 1


@pytest.mark.parametrize('block', ['lease', 'closed', 'not_admitted'])
def test_local_fences_cannot_be_bypassed_by_bulk_read(control, block):
    if block == 'lease':
        control.phase.failed.set()
    elif block == 'closed':
        control.stage.closed = True
    else:
        control.stage.admitted = False
    assert control.check() is False
    assert control.requests == []


def test_a_failed_fresh_query_never_reuses_an_earlier_permission(control):
    assert control.check() is True
    control.state['error'] = True
    with pytest.raises(httpx.ReadTimeout):
        control.check()


def test_revocation_while_intent_is_saved_blocks_that_file(delivery, control):
    state = {}

    def save(value):
        if any(row['status'] == 'in_flight' for row in value['files'].values()):
            control.values[control.task_key] = {'state': 'deleting'}
        return 'saved-local'

    with pytest.raises(InterruptedError):
        invoke(delivery, control.check, save, state)
    assert all(row['status'] == 'pending' for row in state['files'].values())
    assert (delivery[-4] / 'body.docx').is_file()
    assert (delivery[-1] / 'body.docx').is_file()


def test_query_failure_before_unlink_keeps_known_unsent_file_resumable(delivery, control):
    state = {}

    def save(value):
        if any(row['status'] == 'in_flight' for row in value['files'].values()):
            control.state['error'] = True
        return 'saved-local'

    with pytest.raises(httpx.ReadTimeout):
        invoke(delivery, control.check, save, state)
    assert all(row['status'] == 'pending' for row in state['files'].values())
    assert (delivery[-4] / 'body.docx').is_file()
    control.state['error'] = False
    result = invoke(delivery, control.check, state=state)
    assert result['status'] == 'complete'
    assert (delivery[-1] / 'body.docx').is_file()


def test_failed_pending_restore_preserves_unknown_and_does_not_retry(delivery, control):
    state, armed = {}, {'value': False}

    def save(value):
        if any(row['status'] == 'in_flight' for row in value['files'].values()):
            armed['value'] = True
            control.state['error'] = True
        elif armed['value']:
            return None
        return 'saved-local'

    with pytest.raises((ValueError, httpx.ReadTimeout)):
        invoke(delivery, control.check, save, state)
    assert any(row['status'] == 'in_flight' for row in state['files'].values())
    assert (delivery[-4] / 'body.docx').is_file()
    control.state['error'] = False
    with pytest.raises(ValueError, match='unknown'):
        invoke(delivery, control.check, state=state)
    assert (delivery[-4] / 'body.docx').is_file()


@pytest.mark.parametrize('reply', [403, 500, 'invalid-json', 'invalid-schema'])
def test_rejected_or_malformed_control_response_cannot_authorize_unlink(delivery, control, reply):
    state = {}

    def save(value):
        if any(row['status'] == 'in_flight' for row in value['files'].values()):
            control.state['reply'] = reply
        return 'saved-local'

    with pytest.raises((httpx.HTTPStatusError, ValueError)):
        invoke(delivery, control.check, save, state)
    assert all(row['status'] == 'pending' for row in state['files'].values())
    assert (delivery[-4] / 'body.docx').is_file()
    assert (delivery[-3] / 'body.docx').is_file()
    assert (delivery[-1] / 'body.docx').is_file()


@pytest.mark.parametrize('resuming', [False, True])
def test_both_runtime_cleanup_paths_use_the_cleanup_only_callback(delivery_harness, monkeypatch, resuming):
    runtime, _, arguments, body_target, route, records = delivery_harness
    args, handoff = arguments('A', rag_done=True, nas_done=resuming)
    args['limits']['cleanup'] = ['cleanup']
    ids = args['identity']
    records['child-A'].insert(0, {'task_id': 'task', 'folder_id': 'A',
                                'kind': 'postprocessing_complete', 'data': handoff})
    if resuming:
        archive = next(row['data'] for row in records['child-A'] if row['kind'] == 'nas')
        records['child-A'].insert(0, {'task_id': 'task', 'folder_id': 'A', 'kind': 'cleanup', 'data': {
            'identity': ids, 'status': 'incomplete', 'routes': [route],
            'body_targets': {route['code']: body_target(ids)}, 'attachment_receipts': {},
            'archive': archive, 'files': {args['registered']['paper']['archive_files'][0]['path']: {'status': 'pending'}}}})
    observed = []

    def cleanup_check():
        observed.append('cleanup')
        return True

    args['admission'] = SimpleNamespace(running_sync=lambda: True, cleanup_running_sync=cleanup_check)
    monkeypatch.setattr(runtime, 'admitted_sync_slot', lambda *args: nullcontext())
    if not resuming:
        def archive(_paper, identity, _handoff, _destination, state, _save, _check, **kwargs):
            state.update(identity=identity, status='complete', files={})
        monkeypatch.setattr(runtime, 'save_archive', archive)

    def clean(*values, **kwargs):
        assert values[10]() is True
        values[8].update(identity=ids, status='complete', files={})

    monkeypatch.setattr(runtime, 'cleanup_paper', clean)
    assert runtime.deliver_paper(**args)['status'] == 'complete'
    assert observed == ['cleanup']
