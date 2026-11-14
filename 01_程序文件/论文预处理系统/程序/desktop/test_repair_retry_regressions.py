"""Exercise real retry request construction with a fake HTTP transport."""
import copy
import json

from test_vision_failover import _client, _http, _ok, _Opener, _png


def result():
    return {'decision': 'pass', 'reason': 'synthetic fixture', 'insertions': [],
            'sensitive_boxes': [], 'page_masks': [{'page': 15, 'boxes': []},
                                                 {'page': 16, 'boxes': []}]}


def assert_original_inputs_survive(requests):
    bodies = [json.loads(r.data) for r in requests]
    original = bodies[0]['messages'][:2]
    original_user = original[1]['content']
    assert sum(p['type'] == 'image_url' for p in original_user) == 2
    assert json.loads(original_user[0]['text'])['document_data']['pages'] == [15, 16]
    for body in bodies[1:]:
        assert body['messages'][:2] == original
        assert len(body['messages']) <= 3, 'Feedback must not grow on each retry'


def test_all_schema_retries_keep_original_pages_and_document_data(tmp_path):
    bad = result()
    bad['page_masks'] = []
    opener = _Opener(_ok(result=bad), _ok(result=bad), _ok(result=result()))
    client = _client(tmp_path, opener, vision_model={
        'retry': {'max_tries': 3, 'backoff_base_seconds': 0}})
    assert client.call('repair', {'pages': [15, 16], 'known_blocks': [{'id': 'b1'}]},
                       [_png(), _png()]) == result()
    assert len(opener.requests) == 3
    assert_original_inputs_survive(opener.requests)
    assert 'page_masks' in json.loads(opener.requests[1].data)['messages'][-1]['content']


def test_fallback_after_schema_and_service_errors_keeps_original_input(tmp_path):
    bad = copy.deepcopy(result())
    bad['sensitive_boxes'] = None
    opener = _Opener(_ok(result=bad), _http(503), _http(503), _ok(result=result()))
    client = _client(tmp_path, opener, vision_model={
        'retry': {'max_tries': 3, 'backoff_base_seconds': 0}})
    assert client.call('repair', {'pages': [15, 16]}, [_png(), _png()]) == result()
    assert len(opener.requests) == 4
    assert opener.requests[-1].full_url.startswith('https://fallback.test/')
    assert_original_inputs_survive(opener.requests)
