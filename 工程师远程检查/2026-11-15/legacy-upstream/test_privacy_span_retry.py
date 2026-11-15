import pytest
from test_common_contracts import client as visual_client
from agent_client import AgentError

def client(monkeypatch,*responses):
    adapter=visual_client(monkeypatch,*responses)
    adapter.config['text_model']='test-model'
    return adapter

@pytest.mark.parametrize('bad',[None,{},'',123,'Author:Wu Mian','译文姓名','100.00'])
def test_invalid_privacy_span_is_corrected_with_original_context(monkeypatch,bad):
    good={'decision':'pass','reason':'字段定位','sensitive_strings':['Author: Wu Mian']}
    c=client(monkeypatch,{**good,'sensitive_strings':[bad]},good)
    payload={'text':'Author: Wu Mian\n资产100.00元'}
    assert c.call('privacy_text',payload)==good
    assert len(c.opener.requests)==2
    assert c.opener.requests[0]['messages'][:2]==c.opener.requests[1]['messages'][:2]

def test_invalid_privacy_span_retries_are_bounded_and_recorded(monkeypatch,tmp_path):
    from billing import Ledger
    bad={'decision':'pass','reason':'定位','sensitive_strings':['wrong spelling']}
    c=client(monkeypatch,bad,bad,bad);c.ledger=Ledger(tmp_path)
    with pytest.raises(AgentError,match='脱敏位置'):c.call('privacy_text',{'text':'原文'})
    assert len(c.opener.requests)==3
    assert [r['error_type'] for r in c.ledger.records()]==['schema']*3

def test_business_rejection_is_not_silently_overridden(monkeypatch):
    bad={'decision':'reject','reason':'身份位置无法识别'}
    c=client(monkeypatch,bad)
    assert c.call('privacy_text',{'text':'原文'})==bad
    assert len(c.opener.requests)==1
