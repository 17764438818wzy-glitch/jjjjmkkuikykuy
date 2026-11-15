import hashlib
import json
from pathlib import Path

import pytest

from title_llm_supplement import (
    CapacityError, JournalError, build_request, generated_title,
    prepare_target, request_title,
)


def digest(data):
    return hashlib.sha256(data).hexdigest()


@pytest.fixture
def target(tmp_path):
    original = tmp_path / 'original.docx'
    original.write_bytes(b'original paper')
    text = tmp_path / 'body.txt'
    text.write_text('论文全文，研究如何实现图书管理。', encoding='utf8')
    source_hash, text_hash = digest(original.read_bytes()), digest(text.read_bytes())
    material = tmp_path / 'material.json'
    material.write_text(json.dumps({'source': {'source_sha256': source_hash, 'text_sha256': text_hash},
                                   'file_hashes': {str(text): text_hash}}), encoding='utf8')
    return {'paper_id': 'PF_123', 'body_file_id': 'BODY_123', 'source_sha256': source_hash,
            'original_path': str(original), 'text_path': str(text), 'material_path': str(material),
            'group': 'test', 'group_id': 'GROUP_123'}


def connection():
    return {'model': 'test-model', 'context_window': 1000000, 'max_output_tokens': 128000,
            'max_request_bytes': 50000000, 'supports_json_object': False}


def test_prepare_rejects_changed_source_and_changed_text(target):
    assert prepare_target(target)['text'] == '论文全文，研究如何实现图书管理。'
    Path(target['text_path']).write_text('changed', encoding='utf8')
    with pytest.raises(ValueError, match='正文'):
        prepare_target(target)


def test_request_is_title_only_and_respects_model_capacity(target):
    material = prepare_target(target)
    payload = build_request(connection(), material['text'])
    assert payload['model'] == 'test-model'
    assert payload['stream'] is True
    assert 'response_format' not in payload
    assert material['text'] in payload['messages'][1]['content'][0]['text']
    with pytest.raises(CapacityError):
        build_request({**connection(), 'context_window': 1000}, material['text'])


@pytest.mark.parametrize('response,expected', [
    ('{"title":"基于Java的图书管理系统设计与实现"}', '基于Java的图书管理系统设计与实现'),
    ('```json\n{"title":"图书管理系统的设计与实现"}\n```', '图书管理系统的设计与实现'),
    ('图书管理系统的设计与实现', '图书管理系统的设计与实现'),
])
def test_parse_generated_title_without_requiring_evidence(response, expected):
    assert generated_title(response) == expected


def test_ambiguous_response_is_not_a_title():
    with pytest.raises(ValueError):
        generated_title('无法判断\n建议继续查看')


@pytest.mark.parametrize('line_ending', ['\n', '\r\n'])
def test_complete_request_is_reused_even_if_invoked_twice(tmp_path, target, line_ending):
    calls = []
    def invoke(kind, body, conn, work):
        calls.append(1)
        work = Path(work); work.mkdir(parents=True, exist_ok=True)
        raw_request = json.dumps(body, ensure_ascii=False, indent=2).encode()
        (work / 'request.json').write_bytes(raw_request.replace(b'\n', line_ending.encode()))
        response = '{"title":"图书管理系统的设计与实现"}'
        (work / 'response.txt').write_text(response, encoding='utf8')
        compact = json.dumps(body, ensure_ascii=False, separators=(',', ':')).encode()
        state = {'status':'response_complete', 'finish_reason':'stop', 'http_invoked':True,
                 'request_sha256':digest(compact), 'stored_request_sha256':digest(raw_request),
                 'response_sha256':digest(response.encode())}
        (work / 'request-state.json').write_text(json.dumps(state), encoding='utf8')
        return response
    first = request_title(target, connection(), tmp_path / 'requests', invoke)
    second = request_title(target, connection(), tmp_path / 'requests', invoke)
    assert first['title'] == second['title'] == '图书管理系统的设计与实现'
    assert first['origin'] == 'model_generated'
    assert len(calls) == 1


def test_uncertain_request_is_never_sent_again(tmp_path, target):
    calls = []
    def uncertain(kind, body, conn, work):
        calls.append(1)
        work = Path(work); work.mkdir(parents=True, exist_ok=True)
        (work / 'request-state.json').write_text('{"status":"request_failed_or_uncertain"}', encoding='utf8')
        raise TimeoutError('unknown')
    with pytest.raises(TimeoutError):
        request_title(target, connection(), tmp_path / 'requests', uncertain)
    with pytest.raises(JournalError):
        request_title(target, connection(), tmp_path / 'requests', uncertain)
    assert len(calls) == 1


def test_source_identity_cannot_be_changed_in_existing_journal(tmp_path, target):
    def no_send(*args):
        raise TimeoutError('before or during send')
    with pytest.raises(TimeoutError):
        request_title(target, connection(), tmp_path / 'requests', no_send)
    changed = {**target, 'group_id': 'different'}
    with pytest.raises(JournalError):
        request_title(changed, connection(), tmp_path / 'requests', no_send)


def test_generated_title_can_name_body_and_attachment_without_claiming_original(tmp_path, target):
    from paperflow_prefect.named_delivery import resolved_delivery_row
    def invoke(kind, body, conn, work):
        work = Path(work); work.mkdir(parents=True, exist_ok=True)
        request = json.dumps(body, ensure_ascii=False, indent=2).encode()
        (work / 'request.json').write_bytes(request)
        response = '{"title":"图书管理系统的设计与实现"}'
        (work / 'response.txt').write_text(response, encoding='utf8')
        compact = json.dumps(body, ensure_ascii=False, separators=(',', ':')).encode()
        state = {'status':'response_complete', 'finish_reason':'stop', 'http_invoked':True,
                 'request_sha256':digest(compact), 'stored_request_sha256':digest(request),
                 'response_sha256':digest(response.encode())}
        (work / 'request-state.json').write_text(json.dumps(state), encoding='utf8')
        return response
    request_title(target, connection(), tmp_path / 'title-requests', invoke)
    source = Path(target['original_path'])
    body = {'file_id':target['body_file_id'], 'sha256':target['source_sha256'], 'path':str(source),
            'kind':'body', 'display_name':None, 'bytes':source.stat().st_size}
    paper = {'paper_id':target['paper_id'], 'group_id':target['group_id'], 'body':body}
    named = resolved_delivery_row(paper, body, tmp_path)
    assert named['display_name'] == '图书管理系统的设计与实现.docx'
    assert named['title_origin'] == 'model_generated'
    attachment = {'kind':'attachment', 'sequence':1, 'path':str(tmp_path/'代码.zip'), 'display_name':None}
    assert resolved_delivery_row(paper, attachment, tmp_path)['display_name'] == '图书管理系统的设计与实现_附件01.zip'
    body['display_name'] = '已有原稿题目.docx'
    assert resolved_delivery_row(paper, body, tmp_path)['display_name'] == '已有原稿题目.docx'


def test_changed_generated_response_is_not_used_for_naming(tmp_path, target):
    from paperflow_prefect.named_delivery import resolved_delivery_row
    def invoke(kind, body, conn, work):
        work = Path(work); work.mkdir(parents=True, exist_ok=True)
        request = json.dumps(body, ensure_ascii=False, indent=2).encode()
        (work/'request.json').write_bytes(request)
        response = '{"title":"图书管理系统的设计与实现"}'
        (work/'response.txt').write_text(response, encoding='utf8')
        compact = json.dumps(body, ensure_ascii=False, separators=(',', ':')).encode()
        (work/'request-state.json').write_text(json.dumps({'status':'response_complete','finish_reason':'stop',
            'request_sha256':digest(compact),'stored_request_sha256':digest(request),
            'response_sha256':digest(response.encode())}), encoding='utf8')
        return response
    request_title(target, connection(), tmp_path / 'title-requests', invoke)
    response = next((tmp_path / 'title-requests').rglob('response.txt'))
    response.write_text('{"title":"替换的论文标题"}', encoding='utf8')
    source = Path(target['original_path'])
    body = {'file_id':target['body_file_id'], 'sha256':target['source_sha256'], 'path':str(source),
            'kind':'body', 'display_name':None, 'bytes':source.stat().st_size}
    paper = {'paper_id':target['paper_id'], 'group_id':target['group_id'], 'body':body}
    assert resolved_delivery_row(paper, body, tmp_path)['display_name'] is None


def test_native_stream_reconciliation_must_accept_saved_response(tmp_path, monkeypatch):
    import sys
    from types import SimpleNamespace
    from paperflow_prefect.generated_title_journal import verify_response, JournalError
    payload = {'model':'test', 'messages':[]}
    formatted = json.dumps(payload, ensure_ascii=False, indent=2).encode()
    compact = json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode()
    response = '{"title":"图书管理系统的设计与实现"}'
    (tmp_path/'request.json').write_bytes(formatted)
    (tmp_path/'response.txt').write_text(response, encoding='utf8')
    (tmp_path/'response-events.jsonl').write_text('corrupt', encoding='utf8')
    (tmp_path/'request-state.json').write_text(json.dumps({'status':'response_complete','finish_reason':'stop',
        'request_sha256':digest(compact),'stored_request_sha256':digest(formatted),
        'response_sha256':digest(response.encode())}), encoding='utf8')
    monkeypatch.setitem(sys.modules, 'preextract_scheduler_adapter', SimpleNamespace(
        request_observation=lambda path: ('outcome_unknown', 'reconcile', {'reason':'response_events_not_complete'})))
    with pytest.raises(JournalError):
        verify_response(tmp_path, {'request_sha256':digest(compact)})
