import copy
from importlib import import_module
from io import BytesIO
from urllib.error import HTTPError, URLError

import pytest


IDENTITY = {'task_id': 't', 'group_id': 'g', 'paper_id': 'p', 'receipt_id': 'r', 'task_run_id': 'child'}


class Remote:
    def __init__(self):
        self.uploads = []
        self.lose_reply = False

    def upload_bytes(self, dataset, name, content):
        self.uploads.append((dataset, name, content))
        if self.lose_reply:
            raise TimeoutError('synthetic lost response')
        return 'document-1'


def event_store():
    rows = []

    def save(event_id, kind, data):
        rows.insert(0, {'event_id': event_id, 'kind': kind, 'data': copy.deepcopy(data)})
        return 'server-artifact'

    return rows, save


def test_returned_write_is_reused_after_reloading_server_events():
    module = import_module('paperflow_v3.delivery.transport')
    rows, save = event_store()
    remote = Remote()
    first = module.TaskCheckedClient(remote, lambda: True, module.ServerEffects(IDENTITY, rows, save))
    assert first.upload_bytes('formal', 'paper.md', b'content') == 'document-1'
    second = module.TaskCheckedClient(remote, lambda: True, module.ServerEffects(IDENTITY, rows, save))
    assert second.upload_bytes('formal', 'paper.md', b'content') == 'document-1'
    assert len(remote.uploads) == 1


def test_write_with_unknown_response_is_not_resent_after_restart():
    module = import_module('paperflow_v3.delivery.transport')
    rows, save = event_store()
    remote = Remote()
    remote.lose_reply = True
    first = module.TaskCheckedClient(remote, lambda: True, module.ServerEffects(IDENTITY, rows, save))
    with pytest.raises(TimeoutError):
        first.upload_bytes('formal', 'paper.md', b'content')
    second = module.TaskCheckedClient(remote, lambda: True, module.ServerEffects(IDENTITY, rows, save))
    with pytest.raises(ValueError, match='unknown'):
        second.upload_bytes('formal', 'paper.md', b'content')
    assert len(remote.uploads) == 1


def test_pause_after_intent_but_before_final_admission_sends_nothing_and_can_resume():
    module = import_module('paperflow_v3.delivery.transport')
    rows, persist = event_store()
    remote, running = Remote(), {'value': True}

    def save(event_id, kind, data):
        result = persist(event_id, kind, data)
        if kind == 'rag_effect_intent':
            running['value'] = False
        return result

    first = module.TaskCheckedClient(remote, lambda: running['value'], module.ServerEffects(IDENTITY, rows, save))
    with pytest.raises(InterruptedError):
        first.upload_bytes('formal', 'paper.md', b'content')
    assert remote.uploads == []
    assert rows[0]['kind'] == 'rag_effect_not_sent'
    running['value'] = True
    second = module.TaskCheckedClient(remote, lambda: running['value'], module.ServerEffects(IDENTITY, rows, persist))
    assert second.upload_bytes('formal', 'paper.md', b'content') == 'document-1'
    assert len(remote.uploads) == 1


def test_the_last_approved_request_may_finish_but_the_next_is_blocked():
    module = import_module('paperflow_v3.delivery.transport')
    rows, save = event_store()
    remote, running = Remote(), {'value': True, 'checks': 0}

    def final_admission():
        if not running['value']:
            return False
        running['checks'] += 1
        if running['checks'] == 2:
            running['value'] = False
        return True

    client = module.TaskCheckedClient(remote, final_admission, module.ServerEffects(IDENTITY, rows, save))
    assert client.upload_bytes('formal', 'paper.md', b'content') == 'document-1'
    with pytest.raises(InterruptedError):
        client.upload_bytes('formal', 'next.md', b'content')
    assert len(remote.uploads) == 1


def test_native_client_downloads_use_existing_publisher_fallback_and_admission():
    module = import_module('paperflow_v3.delivery.transport')
    rows, save = event_store()
    running = {'value': True}

    class Response(BytesIO):
        headers = {'Content-Type': 'image/png'}

    class Opener:
        def __init__(self):
            self.urls = []

        def open(self, request, timeout):
            self.urls.append(request.full_url)
            return Response(b'synthetic download')

    class Native:
        base_url = 'http://127.0.0.1:9'
        api_key = 'fixture'
        opener = Opener()

    native = Native()
    client = module.TaskCheckedClient(native, lambda: running['value'], module.ServerEffects(IDENTITY, rows, save))
    assert client.download_bytes('formal', 'document') == b'synthetic download'
    assert client.download_image('image id') == b'synthetic download'
    assert native.opener.urls == [
        'http://127.0.0.1:9/api/v1/datasets/formal/documents/document',
        'http://127.0.0.1:9/api/v1/documents/images/image%20id',
    ]
    running['value'] = False
    with pytest.raises(InterruptedError):
        client.download_bytes('formal', 'document')
    assert len(native.opener.urls) == 2


@pytest.mark.parametrize('failure', ['connection', 'http-503'])
def test_real_native_get_retry_stops_after_parent_pauses(monkeypatch, failure):
    import ragflow_client
    from ragflow_client import RagflowClient
    from paperflow_v3.delivery.transport import ServerEffects, TaskCheckedClient

    rows, save = event_store()
    running = {'value': True}

    class Opener:
        def __init__(self):
            self.calls = []

        def open(self, request, timeout):
            self.calls.append((request.full_url, timeout))
            if len(self.calls) == 1:
                running['value'] = False
                if failure == 'http-503':
                    raise HTTPError(request.full_url, 503, 'synthetic', {}, BytesIO())
                raise URLError('synthetic connection failure')
            return BytesIO(b'{"code":0,"data":["resumed"]}')

    native = RagflowClient('http://127.0.0.1:9', 'fixture')
    native.opener = Opener()
    monkeypatch.setattr(ragflow_client, 'sleep', lambda _: None)
    client = TaskCheckedClient(native, lambda: running['value'], ServerEffects(IDENTITY, rows, save))
    with pytest.raises(InterruptedError):
        client.request('GET', '/datasets')
    assert len(native.opener.calls) == 1
    assert '_request_once' not in native.__dict__
    running['value'] = True
    assert client.request('GET', '/datasets') == ['resumed']
    assert len(native.opener.calls) == 2


def test_real_native_get_keeps_its_retry_behavior_while_running(monkeypatch):
    import ragflow_client
    from ragflow_client import RagflowClient
    from paperflow_v3.delivery.transport import ServerEffects, TaskCheckedClient

    rows, save = event_store()
    waits = []

    class Opener:
        def __init__(self):
            self.calls = 0

        def open(self, request, timeout):
            self.calls += 1
            if self.calls < 3:
                raise URLError('synthetic connection failure')
            return BytesIO(b'{"code":0,"data":["ok"]}')

    native = RagflowClient('http://127.0.0.1:9', 'fixture')
    native.opener = Opener()
    monkeypatch.setattr(ragflow_client, 'sleep', waits.append)
    client = TaskCheckedClient(native, lambda: True, ServerEffects(IDENTITY, rows, save))
    assert client.request('GET', '/datasets') == ['ok']
    assert native.opener.calls == 3 and waits == [0.5, 1.0]


def test_real_native_get_admitted_before_pause_can_finish():
    from ragflow_client import RagflowClient
    from paperflow_v3.delivery.transport import ServerEffects, TaskCheckedClient

    rows, save = event_store()
    running = {'value': True}

    class Opener:
        def __init__(self):
            self.calls = 0

        def open(self, request, timeout):
            self.calls += 1
            running['value'] = False
            return BytesIO(b'{"code":0,"data":["finished"]}')

    native = RagflowClient('http://127.0.0.1:9', 'fixture')
    native.opener = Opener()
    client = TaskCheckedClient(native, lambda: running['value'], ServerEffects(IDENTITY, rows, save))
    assert client.request('GET', '/datasets') == ['finished']
    with pytest.raises(InterruptedError):
        client.request('GET', '/datasets')
    assert native.opener.calls == 1


def test_definite_rejection_requires_explicit_retry_but_unknown_never_retries():
    from ragflow_client import RagflowHTTPError

    module = import_module('paperflow_v3.delivery.transport')
    rows, save = event_store()

    class Rejected(Remote):
        def upload_bytes(self, dataset, name, content):
            self.uploads.append((dataset, name, content))
            if len(self.uploads) == 1:
                raise RagflowHTTPError(403)
            return 'document-1'

    remote = Rejected()
    client = module.TaskCheckedClient(remote, lambda: True, module.ServerEffects(IDENTITY, rows, save))
    with pytest.raises(RagflowHTTPError):
        client.upload_bytes('formal', 'paper.md', b'content')
    assert rows[0]['kind'] == 'rag_effect_failed'
    client = module.TaskCheckedClient(remote, lambda: True, module.ServerEffects(IDENTITY, rows, save))
    with pytest.raises(ValueError, match='retry'):
        client.upload_bytes('formal', 'paper.md', b'content')
    assert len(remote.uploads) == 1
    client = module.TaskCheckedClient(remote, lambda: True,
                                     module.ServerEffects(IDENTITY, rows, save, retry_failed=True))
    assert client.upload_bytes('formal', 'paper.md', b'content') == 'document-1'
    assert len(remote.uploads) == 2
