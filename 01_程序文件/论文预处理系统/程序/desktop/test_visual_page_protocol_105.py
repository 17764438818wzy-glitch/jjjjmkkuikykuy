"""Observed multi-page mask and object-page protocol failures stay fail-closed."""
import copy
import json

import pytest

from agent_client import AgentError
from test_common_contracts import client, picture


PAYLOAD = {'pages': [8, 9], 'known_blocks': [
    {'id': 'left', 'type': 'paragraph', 'candidate_pages': [8]},
    {'id': 'right', 'type': 'paragraph', 'candidate_pages': [9]},
]}
GOOD = {'decision': 'pass', 'reason': 'read original pages', 'sensitive_boxes': [],
        'page_masks': [{'page': 8, 'boxes': []}, {'page': 9, 'boxes': [[.1,.1,.2,.2]]}],
        'insertions': [{'after_id': 'left', 'page': 8, 'kind': 'text', 'text': 'original left'},
                       {'after_id': 'right', 'page': 9, 'kind': 'text', 'text': 'original right'}]}


def test_actual_source_pages_label_each_image_and_geometry(monkeypatch):
    adapter = client(monkeypatch, GOOD)
    assert adapter.call('repair', PAYLOAD, [picture(), picture()]) == GOOD
    content = adapter.opener.requests[0]['messages'][1]['content']
    geometry = json.loads(content[0]['text'])['image_geometry']
    assert [row['page'] for row in geometry] == [8, 9]
    for index, entry in enumerate(content):
        if entry['type'] == 'image_url':
            label = json.loads(content[index-1]['text'])
            assert label == {'image_index': 1 if index == 2 else 2,
                             'page': 8 if index == 2 else 9}


@pytest.mark.parametrize('failure', ['mask', 'target_page'])
def test_schema_retry_supplies_exact_page_contract_without_rewriting_response(monkeypatch, failure):
    bad = copy.deepcopy(GOOD)
    if failure == 'mask': bad['sensitive_boxes'] = [[.1,.1,.2,.2]]
    else: bad['insertions'][1]['page'] = 8
    untouched = copy.deepcopy(bad)
    adapter = client(monkeypatch, bad, GOOD)
    assert adapter.call('repair', PAYLOAD, [picture(), picture()]) == GOOD
    assert bad == untouched
    assert len(adapter.opener.requests) == 2
    feedback = adapter.opener.requests[1]['messages'][-1]['content']
    constraints = json.loads(feedback.split('\nPAGE_CONSTRAINTS=', 1)[1])
    assert constraints['image_pages'] == [8, 9]
    assert constraints['page_masks_required_pages'] == [8, 9]
    assert constraints['sensitive_boxes_must_be_empty'] is True
    assert constraints['target_candidate_pages'] == {'left': [8], 'right': [9]}
    # Images and their actual-page labels are repeated unchanged on retry.
    assert adapter.opener.requests[0]['messages'][1] == adapter.opener.requests[1]['messages'][1]


@pytest.mark.parametrize('failure', ['mask', 'target_page'])
def test_uncorrected_response_never_passes_after_three_attempts(monkeypatch, failure):
    bad = copy.deepcopy(GOOD)
    if failure == 'mask': bad['sensitive_boxes'] = [[.1,.1,.2,.2]]
    else: bad['insertions'][1]['page'] = 8
    adapter = client(monkeypatch, bad, bad, bad)
    with pytest.raises(AgentError):
        adapter.call('repair', PAYLOAD, [picture(), picture()])
    assert len(adapter.opener.requests) == 3


def test_repair_example_includes_mandatory_image_description_string():
    from agent_client import contract_for
    prompt = contract_for('repair', PAYLOAD)
    example, _ = json.JSONDecoder().raw_decode(prompt[prompt.index('{"decision"'):])
    assert isinstance(example['insertions'][0].get('description'), str)


def test_null_image_description_gets_typed_feedback_and_is_not_filled_locally(monkeypatch):
    good = copy.deepcopy(GOOD)
    good['insertions'][0].update(kind='image', bbox=[.1,.1,.8,.8], description='original chart meaning')
    bad = copy.deepcopy(good)
    bad['insertions'][0]['description'] = None
    adapter = client(monkeypatch, bad, good)
    assert adapter.call('repair', PAYLOAD, [picture(), picture()]) == good
    assert bad['insertions'][0]['description'] is None
    feedback = adapter.opener.requests[1]['messages'][-1]['content']
    constraints = json.loads(feedback.split('\nPAGE_CONSTRAINTS=', 1)[1])
    assert constraints['image_required_field_types'] == {'text': 'string', 'description': 'string'}


def test_optional_description_null_on_text_is_not_an_image_protocol_error(monkeypatch):
    good = copy.deepcopy(GOOD)
    good['insertions'][0]['description'] = None
    adapter = client(monkeypatch, good)
    assert adapter.call('repair', PAYLOAD, [picture(), picture()]) == good
    assert len(adapter.opener.requests) == 1
