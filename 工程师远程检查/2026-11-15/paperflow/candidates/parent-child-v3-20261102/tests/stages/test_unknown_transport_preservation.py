"""Real one-shot journal: interrupted model responses never authorize replay."""
import hashlib
import io
import json
from urllib.error import HTTPError

import pytest
from preextract_request import request_once, ModelExplicitError
from paperflow_v3.stages.llm_request import request_outcome


@pytest.mark.parametrize('failure', ['429', '503', 'stream_error', 'truncated_frame'])
def test_model_transport_unknown_preserves_evidence_and_one_send(tmp_path, failure):
    body = {'model': 'synthetic', 'messages': []}
    connection = {'base_url': 'http://model.invalid/v1', 'api_key': 'synthetic'}
    calls = []
    class Response(io.BytesIO):
        headers = {'Content-Type': 'text/event-stream'}
    class Opener:
        def open(self, request, timeout):
            calls.append(request)
            if failure in ('429', '503'):
                raw = json.dumps({'error': {'code': 'workbuddy_route_changed_not_sent' if failure == '503'
                    else 'rate_limit_error', 'message': 'synthetic rejection'}}).encode()
                raise HTTPError(request.full_url, int(failure), 'synthetic', {}, io.BytesIO(raw))
            first = b'data: {"choices":[{"delta":{"content":"retained partial answer"}}]}\n\n'
            last = (b'data: {"error":{"code":"upstream_stream_error","message":"IDLE_TIMEOUT"}}\n\n'
                if failure == 'stream_error' else b'data: {"choices":[{"delta":{"content":"truncated')
            return Response(first + last)
    opener = Opener()
    expected = HTTPError if failure in ('429', '503') else ModelExplicitError if failure == 'stream_error' else json.JSONDecodeError
    with pytest.raises(expected): request_once(body, connection, tmp_path, opener=opener)
    outcome, _ = request_outcome({'http_invoked': True, 'work_dir': str(tmp_path)})
    assert outcome == 'llm_request_unknown'
    original = {path.name: path.read_bytes() for path in tmp_path.iterdir() if path.is_file()}
    state = json.loads(original['request-state.json'])
    if failure in ('stream_error', 'truncated_frame'):
        assert state['response_sha256'] == hashlib.sha256(original['response.txt']).hexdigest()
        assert b'retained partial answer' in original['response.txt']
        assert original['response-events.jsonl']
    with pytest.raises(RuntimeError): request_once(body, connection, tmp_path, opener=opener)
    assert calls and len(calls) == 1
    assert original == {path.name: path.read_bytes() for path in tmp_path.iterdir() if path.is_file()}
