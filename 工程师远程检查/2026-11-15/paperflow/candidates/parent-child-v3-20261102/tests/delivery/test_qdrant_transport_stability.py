"""Local mock transport: read recovery must never replay a write."""
from collections import Counter
import hashlib

import httpx
import pytest

from paperflow_qdrant import delivery


def setup_transport(monkeypatch, tmp_path, mode, check_running=None):
    raw = b'original attachment bytes'
    digest = hashlib.sha256(raw).hexdigest()
    path = tmp_path / 'source.bin'
    path.write_bytes(raw)
    doc = {'id': 'doc', 'dataset_ids': ['dataset'], 'meta_fields': {'source': digest},
           'records': [{'content': 'search text'}]}
    batch = {'folder_id': 'folder', 'documents': [doc]}
    monkeypatch.setattr(delivery, 'build_folder', lambda *args: (batch, {'route': {}}, {}, {digest: path}))
    calls = Counter()
    def handle(request):
        key = request.method + ' ' + request.url.path
        calls[key] += 1
        if request.method == 'PUT':
            assert request.read() == raw
            if mode.startswith('put_lost'):
                raise httpx.ReadError('upload ACK lost', request=request)
            return httpx.Response(200)
        if request.url.path.startswith('/paperflow/objects/'):
            if mode == 'object_flaky' and calls[key] == 1:
                raise httpx.ReadError('object read reset', request=request)
            if mode == 'put_lost_missing': return httpx.Response(404)
            return httpx.Response(200, content=b'changed bytes' if mode == 'put_lost_mismatch' else raw)
        if request.url.path == '/paperflow/folders':
            if mode == 'post_lost': raise httpx.ReadError('folder ACK lost', request=request)
            return httpx.Response(200, json={'status': 'complete', 'folder_id': 'folder'})
        if request.method == 'GET':
            if mode == 'metadata_exhausted' or mode in {'metadata_flaky', 'pause'} and calls[key] == 1:
                raise httpx.ReadTimeout('metadata read timeout', request=request)
            return httpx.Response(200, json={'data': {'docs': [{'id': 'doc', 'dataset_id': 'dataset',
                'meta_fields': doc['meta_fields'], 'chunk_count': 1}]}})
        return httpx.Response(200, json={'data': {'chunks': [{'dataset_id': 'dataset', 'document_id': 'doc'}]}})
    original = httpx.Client
    monkeypatch.setattr(delivery.httpx, 'Client', lambda **kwargs: original(transport=httpx.MockTransport(handle), **kwargs))
    def run():
        return delivery.deliver_folder(paper={}, identity={}, task={}, packet={}, handoff={}, budget=None,
            base_url='http://qdrant.invalid', api_key='synthetic', check_running=check_running)
    return run, calls, digest


@pytest.mark.parametrize('mode', ['metadata_flaky', 'object_flaky', 'put_lost_complete'])
def test_transient_read_or_lost_upload_ack_uses_readback_without_write_replay(monkeypatch, tmp_path, mode):
    run, calls, digest = setup_transport(monkeypatch, tmp_path, mode)
    targets, _ = run()
    assert targets['route']['verified'] is True
    assert calls['PUT /paperflow/objects/' + digest] == 1
    assert calls['POST /paperflow/folders'] == 1
    assert calls['POST /api/v1/retrieval'] == 1


@pytest.mark.parametrize('mode', ['put_lost_missing', 'put_lost_mismatch', 'post_lost'])
def test_unconfirmed_write_stays_unknown_and_is_never_replayed(monkeypatch, tmp_path, mode):
    run, calls, digest = setup_transport(monkeypatch, tmp_path, mode)
    with pytest.raises(httpx.ReadError, match='ACK lost'):
        run()
    assert calls['PUT /paperflow/objects/' + digest] == 1
    assert calls['POST /paperflow/folders'] == (1 if mode == 'post_lost' else 0)


def test_verification_read_has_bounded_attempts(monkeypatch, tmp_path):
    run, calls, _ = setup_transport(monkeypatch, tmp_path, 'metadata_exhausted')
    with pytest.raises(httpx.ReadTimeout): run()
    assert calls['GET /api/v1/datasets/dataset/documents'] == 3
    assert calls['POST /paperflow/folders'] == 1


def test_pause_blocks_next_verification_read(monkeypatch, tmp_path):
    calls = None
    def running(): return not calls['GET /api/v1/datasets/dataset/documents']
    run, calls, _ = setup_transport(monkeypatch, tmp_path, 'pause', running)
    with pytest.raises(InterruptedError): run()
    assert calls['GET /api/v1/datasets/dataset/documents'] == 1
    assert calls['POST /paperflow/folders'] == 1
