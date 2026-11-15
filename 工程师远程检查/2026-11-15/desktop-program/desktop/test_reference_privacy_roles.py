"""Reference roles must reach the model without weakening identity validation."""
import json

import pytest

from auto_evidence import Rejected, text_privacy
from test_privacy_span_retry import client


REFERENCE = '[12]Arens,A.A.,Loebbecke,J.K.,张杰明,文善恩等译.当代审计学[M].北京:中国商业出版社.1990:10'


def document(*texts):
    return {'title': '审计案例研究', 'resources': [], 'formulas': [],
            'blocks': [{'type': 'paragraph', 'text': text} for text in texts]}


def test_reference_role_context_reaches_real_request_and_citation_remains(monkeypatch):
    item = document('作者：张杰明', '参考文献', REFERENCE)
    adapter = client(monkeypatch, {'decision': 'pass', 'reason': '引文译者为文献元数据',
                                   'sensitive_strings': []})
    text_privacy(item, adapter, lambda *args: None)
    request = adapter.opener.requests[0]
    instructions = request['messages'][0]['content']
    payload = json.loads(request['messages'][1]['content'])['document_data']['text']
    # Assert the model-request boundary, not the source-code representation.
    assert all(role in instructions for role in ('译者', '编者', '引用元数据'))
    assert REFERENCE in payload
    assert item['blocks'][0]['text'] == '作者：[NAME]'
    assert item['blocks'][2]['text'] == REFERENCE
    assert item['text_privacy_complete'] is True


@pytest.mark.parametrize('role', ['译者', '编者'])
def test_actual_submitter_role_is_still_masked_without_removing_same_name_reference(monkeypatch, role):
    identity = '本文' + role + '：张杰明'
    item = document(identity, '参考文献', REFERENCE)
    adapter = client(monkeypatch, {'decision': 'pass', 'reason': '本文身份字段可定位',
                                   'sensitive_strings': [identity]})
    text_privacy(item, adapter, lambda *args: None)
    assert item['blocks'][0]['text'] == '[已脱敏]'
    assert item['blocks'][2]['text'] == REFERENCE


def test_reference_presence_does_not_override_a_real_unlocatable_identity_rejection(monkeypatch):
    item = document('参考文献', REFERENCE)
    adapter = client(monkeypatch, {'decision': 'reject', 'reason': '另有实际身份字段无法可靠定位'})
    with pytest.raises(Rejected):
        text_privacy(item, adapter, lambda *args: None)
    assert item['text_privacy_complete'] is False
    assert len(adapter.opener.requests) == 1
