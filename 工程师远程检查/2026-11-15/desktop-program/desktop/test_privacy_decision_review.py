import json
import pytest
from auto_evidence import text_privacy,Rejected
from test_reference_privacy_roles import document
from test_privacy_span_retry import client


def test_anonymous_codes_and_placeholder_get_one_explicit_fresh_review(monkeypatch):
    item=document('教师A访谈记录：参与培训2次。','[已脱敏]')
    c=client(monkeypatch,{'decision':'reject','reason':'教师A与培训次数绑定，[已脱敏]无法确认覆盖'},
             {'decision':'pass','reason':'匿名研究编号与占位符不含提交者真实身份','sensitive_strings':[]})
    text_privacy(item,c,lambda *args:None)
    assert item['text_privacy_complete']
    assert item['blocks'][0]['text']=='教师A访谈记录：参与培训2次。'
    assert len(c.opener.requests)==2
    req=c.opener.requests[1]
    payload=json.loads(req['messages'][1]['content'])['document_data']
    assert payload['phase']=='review_privacy_decision' and payload['previous_decision']['decision']=='reject'
    assert '重新独立检查' in req['messages'][0]['content']


def test_locatable_identity_is_masked_only_after_validated_new_decision(monkeypatch):
    identity='我是一名学生，名叫李晓，负责本次问卷调查。'
    item=document(identity)
    c=client(monkeypatch,{'decision':'reject','reason':'提交者身份可精确定位','sensitive_strings':[identity]},
             {'decision':'pass','reason':'应用身份字段遮盖','sensitive_strings':[identity]})
    text_privacy(item,c,lambda *args:None)
    assert item['blocks'][0]['text']=='[已脱敏]'


def test_review_rejection_is_final_and_never_forced_to_pass(monkeypatch):
    item=document('教师A及另一个无法定位的真实身份字段')
    no={'decision':'reject','reason':'教师A之外仍有身份字段无法定位'}
    c=client(monkeypatch,no,no)
    with pytest.raises(Rejected):text_privacy(item,c,lambda *args:None)
    assert not item['text_privacy_complete'] and len(c.opener.requests)==2


def test_no_second_call_for_unambiguous_privacy_rejection(monkeypatch):
    item=document('图中签名文字不清晰')
    c=client(monkeypatch,{'decision':'reject','reason':'实际签名无法可靠定位'})
    with pytest.raises(Rejected):text_privacy(item,c,lambda *args:None)
    assert len(c.opener.requests)==1
