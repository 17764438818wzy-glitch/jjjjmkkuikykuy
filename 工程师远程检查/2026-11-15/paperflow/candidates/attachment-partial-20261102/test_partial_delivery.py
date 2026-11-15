"""Synthetic regression checks for partial attachment delivery; no live requests."""
import copy
import importlib.util
import json
from pathlib import Path
import sys

import pytest

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
POST = ROOT / 'candidates/postprocess-prefect-20261031'
sys.path[:0] = [str(HERE), str(POST / 'tests'), str(POST)]
from test_attachment_adapter import fixture, refresh
from paperflow_prefect.records import read_json, sha_file, write_json


def module(name):
    spec = importlib.util.spec_from_file_location('partial_' + name, HERE / (name + '.py'))
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


@pytest.mark.parametrize('invalid', ['syntax', 'fields'])
def test_bad_saved_attachment_response_is_failed_without_reasking_model(tmp_path, invalid):
    ticket, handoff, row, response = fixture(tmp_path)
    response_path = Path(handoff['model_refs']['response']['path'])
    if invalid == 'syntax':
        response_path.write_text('{"kind":"bad "quote""}', encoding='utf8')
    else:
        response['evidence_refs'] = ['unsupported-evidence']
        response_path.write_text(json.dumps(response, ensure_ascii=False), encoding='utf8')
    state_path = Path(handoff['model_refs']['request_state']['path'])
    state = read_json(state_path)
    state['response_sha256'] = sha_file(response_path)
    write_json(state_path, state)
    refresh(handoff)
    before = response_path.read_bytes()
    adapter = module('generation_attachment_adapter')
    result = adapter.process_attachment(ticket, handoff, tmp_path / 'output')
    assert result['status'] == 'failed'
    assert result['code'] == 'attachment_format_error'
    assert result['binding']['attachment_id'] == row['file_id']
    assert result['raw_response'] == handoff['model_refs']['response']
    assert result['model_requests_added'] == 0
    assert response_path.read_bytes() == before
    assert adapter.process_attachment(ticket, handoff, tmp_path / 'output') == result


def test_folder_passes_body_and_records_failed_attachment(tmp_path, monkeypatch):
    import test_folder_processor as existing
    monkeypatch.setattr(existing, 'folder_processor', module('folder_processor'))
    run, store, _, inputs = existing.scenario(tmp_path)
    original = tmp_path / 'original' / 'attachment.zip'
    def failed(ticket, item, directory):
        path = Path(directory) / 'receipt.json'
        result = {'schema': 'attachment-postprocess-result/1', 'status': 'failed',
                  'ticket': ticket, 'binding': {'attachment_id': item['attachment_id'],
                      'source_path': str(original.resolve()), 'source_sha256': sha_file(original)},
                  'raw_response': {'path': 'retained-response', 'sha256': '1' * 64},
                  'code': 'attachment_format_error', 'message': '附件返回格式错误',
                  'diagnostic': None, 'model_requests_added': 0}
        existing.write(path, result)
        return copy.deepcopy(result)
    assert run(attachment_processor=failed)['state'] == 'waiting_rag'
    assert store.receipt['body']['package_path']
    assert store.receipt['attachments'] == []
    assert store.receipt['failed_attachments'][0]['attachment_id'] == inputs['attachments'][0]['attachment_id']
    assert store.receipt['failed_attachments'][0]['code'] == 'attachment_format_error'
    assert run(_check_existing=True, attachment_processor=failed) is True
