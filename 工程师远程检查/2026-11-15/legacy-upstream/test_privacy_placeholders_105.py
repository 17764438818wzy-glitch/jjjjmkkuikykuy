"""Known local placeholders do not require reconstructing private identities."""
import json

import pytest

from agent_client import contract_for
from auto_evidence import Rejected, text_privacy
from evidence import redactor
from test_privacy_span_retry import client


def document():
    return {'title': '财务风险研究', 'resources': [], 'formulas': [], 'blocks': [
        {'type': 'paragraph', 'text': '作者：张小测\n指导教师：李小测'},
        {'type': 'paragraph', 'text': 'Author: Xu An Jing Instructor: Dong Feng'},
        {'type': 'paragraph', 'text': 'Xu An Jing 与 Dong Feng 提出的研究方法被引用。资产为100.00元。'},
    ]}


def test_prompt_distinguishes_generated_placeholders_from_visible_identity():
    prompt = contract_for('privacy_text')
    for marker in ('[NAME]', '[TEACHER]', '[已脱敏]', '程序', '不得还原',
                   'Author: Xu An Jing', 'Instructor: Dong Feng'):
        assert marker in prompt
    assert '带明确身份上下文的原文精确子串' in prompt
    assert '学者' in prompt and '公司名称、财务金额、年份和研究数值保留' in prompt


def test_actual_precleaning_masks_complete_roles_and_preserves_same_name_research(monkeypatch):
    original = document()
    assert '[NAME]' in redactor()(original['blocks'][0]['text'])
    assert '[TEACHER]' in redactor()(original['blocks'][0]['text'])
    good = {'decision': 'pass', 'reason': '已遮盖标记无需还原；同名研究内容保留',
            'sensitive_strings': []}
    adapter = client(monkeypatch, good)
    text_privacy(original, adapter, lambda *args: None)
    request = adapter.opener.requests[0]
    payload = json.loads(request['messages'][1]['content'])['document_data']['text']
    assert '[NAME]' in payload and '[TEACHER]' in payload
    assert 'Author: Xu An Jing Instructor: Dong Feng' not in payload
    assert original['blocks'][1]['text'] == '[已脱敏]'
    assert original['blocks'][2]['text'] == 'Xu An Jing 与 Dong Feng 提出的研究方法被引用。资产为100.00元。'
    assert original['text_privacy_complete'] is True


def test_placeholder_presence_does_not_bypass_real_model_rejection(monkeypatch):
    item = document()
    adapter = client(monkeypatch, {'decision': 'reject', 'reason': '存在另一个无法定位的具体身份字段'})
    with pytest.raises(Rejected):
        text_privacy(item, adapter, lambda *args: None)
    assert item['text_privacy_complete'] is False
    assert len(adapter.opener.requests) == 1
