import json
import pytest
from agent_client import AgentClient,AgentError
from billing import Ledger
from test_vision_failover import _Opener,_Response,_ok,_png,_cfg

def truncated():
    return _Response(json.dumps({'choices':[{'finish_reason':'length','message':{'content':'{"decision":'}}],
                                'usage':{'prompt_tokens':10,'completion_tokens':8192,'total_tokens':8202}}).encode())

def client(tmp_path,opener,vision,base='https://api.deepseek.com/v1',**overrides):
    cfg=_cfg(max_tokens=8192,attempts=3,base_url=base,text_model='deepseek-flash',**overrides)
    cfg['vision_model']['primary']['base_url']=base
    cfg['vision_model']['retry']['max_tries']=3
    c=AgentClient(cfg,key='test',vision_key='test',fallback_key='test',state_root=tmp_path)
    c.opener=opener;c.ledger=Ledger(tmp_path)
    return c

@pytest.mark.parametrize('vision',[False,True])
def test_truncation_expands_budget_preserves_input_and_records_failure(tmp_path,vision):
    op=_Opener(truncated(),truncated(),_ok())
    c=client(tmp_path,op,vision)
    assert c.call('screen',{'text':'source evidence'},[_png()] if vision else ())['decision']=='pass'
    requests=[json.loads(r.data) for r in op.requests]
    assert [r['max_tokens'] for r in requests]==[8192,16384,32768]
    assert requests[0]['messages']==requests[1]['messages']==requests[2]['messages']
    rows=c.ledger.records()
    assert [r['error_type'] for r in rows]==['output_truncated','output_truncated',None]
    assert rows[0]['total_tokens']==8202 and rows[-1]['ok'] is True
    assert all(r.full_url.startswith('https://api.deepseek.com/') for r in op.requests)

@pytest.mark.parametrize('vision',[False,True])
def test_explicit_cap_stops_without_identical_retry_or_fallback(tmp_path,vision):
    op=_Opener(truncated(),truncated(),_ok())
    c=client(tmp_path,op,vision,max_tokens_retry_limit=16384)
    with pytest.raises(AgentError,match='截断'):c.call('screen',{},[_png()] if vision else ())
    assert [json.loads(r.data)['max_tokens'] for r in op.requests]==[8192,16384]

@pytest.mark.parametrize('vision',[False,True])
def test_unknown_provider_does_not_assume_large_budget(tmp_path,vision):
    op=_Opener(truncated(),_ok());c=client(tmp_path,op,vision,base='https://relay.test/v1')
    with pytest.raises(AgentError,match='截断'):c.call('screen',{},[_png()] if vision else ())
    assert len(op.requests)==1

@pytest.mark.parametrize('vision',[False,True])
def test_explicit_cap_enables_compatible_third_party(tmp_path,vision):
    op=_Opener(truncated(),_ok());c=client(tmp_path,op,vision,base='https://relay.test/v1',max_tokens_retry_limit=16000)
    assert c.call('screen',{},[_png()] if vision else ())['decision']=='pass'
    assert json.loads(op.requests[-1].data)['max_tokens']==16000

@pytest.mark.parametrize('vision',[False,True])
def test_attempt_budget_is_not_extended(tmp_path,vision):
    op=_Opener(truncated(),truncated(),truncated(),_ok());c=client(tmp_path,op,vision)
    with pytest.raises(AgentError,match='截断'):c.call('screen',{},[_png()] if vision else ())
    assert len(op.requests)==3
