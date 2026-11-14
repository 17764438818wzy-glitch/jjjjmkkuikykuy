"""Empty cover fields and contradictory decisions get a bounded fresh review."""
import json

import pytest

from auto_evidence import Rejected, text_privacy
from test_privacy_span_retry import client
from test_reference_privacy_roles import document


@pytest.mark.parametrize('text,reason', [
    ('Author:  Instructor:', '英文署名段仅有角色标签，无可定位的具体身份值，无法可靠定位遮盖'),
    ('Author: AuthorTutor: Tutor&position', '英文作者与导师身份槽位未填写具体姓名，无法可靠定位'),
    ('引用调查者的研究结论。', '没有提交者身份上下文，不应据此 reject，但没有可返回的遮盖子串'),
    ('参考文献中的责任者元数据。', '文献署名不应作为本文身份信息处理，不应拒绝，但无法拆分故 reject'),
])
def test_empty_or_contradictory_reject_gets_one_review_without_deleting_text(monkeypatch, text, reason):
    item = document(text)
    adapter = client(monkeypatch,
                     {'decision': 'reject', 'reason': reason, 'sensitive_strings': []},
                     {'decision': 'pass', 'reason': '无实际提交者身份值', 'sensitive_strings': []})
    text_privacy(item, adapter, lambda *args: None)
    assert item['text_privacy_complete']
    assert item['blocks'][0]['text'] == text
    assert item['privacy_check']['decision_reviews'] == 1
    assert len(adapter.opener.requests) == 2
    request = adapter.opener.requests[1]
    payload = json.loads(request['messages'][1]['content'])['document_data']
    assert payload['phase'] == 'review_privacy_decision'
    assert payload['text'].endswith(text)
    instructions = request['messages'][0]['content']
    assert all(term in instructions for term in ('空角色标签', '模板文字', '实际身份值', 'decision'))


def test_empty_fields_do_not_override_real_unlocatable_identity_after_review(monkeypatch):
    item = document('Author:  Instructor:', '另有模糊签名无法定位。')
    bad = {'decision': 'reject', 'reason': '空角色标签旁仍有真实签名无法可靠定位', 'sensitive_strings': []}
    adapter = client(monkeypatch, bad, bad)
    with pytest.raises(Rejected):
        text_privacy(item, adapter, lambda *args: None)
    assert not item['text_privacy_complete']
    assert len(adapter.opener.requests) == 2


def test_clear_real_signature_reject_keeps_single_call(monkeypatch):
    item = document('实际签名无法可靠辨认。')
    adapter = client(monkeypatch, {'decision': 'reject', 'reason': '实际签名无法可靠定位',
                                   'sensitive_strings': []})
    with pytest.raises(Rejected):
        text_privacy(item, adapter, lambda *args: None)
    assert len(adapter.opener.requests) == 1


def test_initial_request_already_explains_empty_identity_values(monkeypatch):
    item = document('Author:  Instructor:')
    adapter = client(monkeypatch, {'decision': 'pass', 'reason': '仅空标签', 'sensitive_strings': []})
    text_privacy(item, adapter, lambda *args: None)
    request = adapter.opener.requests[0]
    instructions = request['messages'][0]['content']
    assert all(term in instructions for term in ('空角色标签', '模板文字', '实际身份值', 'decision'))
    assert item['blocks'][0]['text'] == 'Author:  Instructor:'
    assert len(adapter.opener.requests) == 1
