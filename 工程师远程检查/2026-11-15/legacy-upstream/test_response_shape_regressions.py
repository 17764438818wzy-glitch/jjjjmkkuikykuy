import copy

import pytest

from agent_client import AgentError
from test_common_contracts import client, picture, repair


@pytest.mark.parametrize('bad_entry', [
    {'kind': 'unsupported'}, {'kind': None}, {'kind': []},
    {'kind': 'image', 'bbox': [.1, .1, .8, .8], 'text': 'visible'},
    {'kind': 'image', 'bbox': [.1, .1, .8, .8], 'text': None, 'description': 'diagram'},
    {'kind': 'image', 'bbox': [.1, .1, .8, .8], 'text': '', 'description': ' '},
    {'kind': 'text', 'text': ''}, {'kind': 'text', 'text': ['wrong shape']},
    {'kind': 'text', 'text': 'visible', 'heading_level': {}},
    {'kind': 'table', 'rows': []}, {'kind': 'table', 'rows': [['A'], ['B', 'C']]},
    {'kind': 'table', 'rows': [[123]]},
    {'kind': 'table', 'rows': [['A']], 'header_rows': [5]},
])
def test_bad_transcription_fields_are_corrected_before_business_processing(monkeypatch, bad_entry):
    good = repair([.1, .1, .8, .8])
    bad = copy.deepcopy(good)
    bad['insertions'] = [{'after_id': 'b1', 'page': 1, **bad_entry}]
    adapter = client(monkeypatch, bad, good)
    assert adapter.call('repair', {'page': 1}, [picture()]) == good
    assert len(adapter.opener.requests) == 2
    assert 'insertions[0]' in adapter.opener.requests[-1]['messages'][-1]['content']


def test_unfixable_transcription_is_schema_error_with_bounded_retries(monkeypatch,tmp_path):
    from billing import Ledger
    bad = repair([.1, .1, .8, .8])
    del bad['insertions'][0]['description']
    adapter = client(monkeypatch, bad, bad, bad)
    adapter.ledger = Ledger(tmp_path)
    with pytest.raises(AgentError, match='description'):
        adapter.call('repair', {'page': 1}, [picture()])
    assert len(adapter.opener.requests) == 3
    assert [r['error_type'] for r in adapter.ledger.records()] == ['schema'] * 3


def test_blank_visible_text_is_valid_when_description_is_present(monkeypatch):
    good = repair([.1, .1, .8, .8])
    good['insertions'][0]['text'] = ''
    adapter = client(monkeypatch, good)
    assert adapter.call('repair', {'page': 1}, [picture()]) == good
    assert len(adapter.opener.requests) == 1


def test_extract_page_uses_same_field_validation(monkeypatch):
    good = {'decision': 'pass', 'reason': 'read', 'sensitive_boxes': [],
            'blocks': [{'kind': 'text', 'text': 'original'}]}
    bad = {**good, 'blocks': [{'kind': 'paragraph', 'text': 'original'}]}
    adapter = client(monkeypatch, bad, good)
    assert adapter.call('extract_page', {'page': 1}, [picture()]) == good
    assert len(adapter.opener.requests) == 2
