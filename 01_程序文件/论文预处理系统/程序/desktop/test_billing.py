import io
import json
from decimal import Decimal
import pytest


def usage(inp=1000, out=100, cached=200):
    return {'prompt_tokens':inp,'completion_tokens':out,'total_tokens':inp+out,
            'prompt_tokens_details':{'cached_tokens':cached},'completion_tokens_details':{'reasoning_tokens':50}}


def test_official_price_cache_fx_and_large_context():
    from billing import estimate, normalize_usage, price_for
    p=price_for('gpt-5.6-sol',{'usd_cny':'7'})
    assert Decimal(estimate(normalize_usage(usage()),p)) == Decimal('0.03696')
    assert Decimal(estimate(normalize_usage(usage(300000,100,0)),p)) == Decimal('16.821')
    assert normalize_usage(usage())['total_tokens']==1100  # reasoning already included
    assert price_for('unknown-model',{'usd_cny':'7'}) is None


def test_unpriced_third_party_model_keeps_tokens_and_custom_price_is_user_provided(tmp_path):
    from billing import Ledger, price_for, summarize
    assert price_for('deepseek-v4-flash-ascend', {'usd_cny':'7'}) is None
    custom = {'usd_cny':'7', 'model_pricing': {'deepseek-v4-flash-ascend': {
        'input':'1', 'cached_input':'0.1', 'output':'2', 'basis':'user_provided',
        'source':'user configuration'}}}
    pricing = price_for('deepseek-v4-flash-ascend', custom)
    assert pricing['basis'] == 'user_provided' and pricing['source'] == 'user configuration'
    ledger = Ledger(tmp_path)
    row = ledger.begin('deepseek-v4-flash-ascend','text','screen',1,{'usd_cny':'7'}, {})
    ledger.finish(row, usage(), 'response')
    summary = summarize(ledger.records())
    assert summary['total_tokens'] == 1100 and summary['unpriced'] == 1


def test_ledger_finish_records_elapsed_time(tmp_path):
    from billing import Ledger
    ledger = Ledger(tmp_path)
    row = ledger.begin('gpt-5.6-sol','text','screen',1,{'usd_cny':'7'}, {})
    ledger.finish(row, usage(), 'response')
    finished = ledger.records()[0]
    assert finished['finished_at'] and isinstance(finished['seconds'], (int, float)) and finished['seconds'] >= 0


@pytest.mark.parametrize('data',[None,{}, {'prompt_tokens':-1,'completion_tokens':1},
    {'prompt_tokens':True,'completion_tokens':1},usage(10,1,11),
    {'prompt_tokens':10,'completion_tokens':1,'total_tokens':99}])
def test_missing_or_invalid_usage_never_means_free(data):
    from billing import normalize_usage
    assert normalize_usage(data) is None


def test_ledger_durable_unknown_and_price_snapshot(tmp_path):
    from billing import Ledger, summarize, summary_text
    ledger=Ledger(tmp_path)
    pending=ledger.begin('gpt-5.6-sol','text','screen',1,{'usd_cny':'7'},{'run_id':'r','item_id':'i'})
    assert summarize(Ledger(tmp_path).records())['unknown_usage']==1
    ledger.finish(pending,usage(),'response')
    ledger.begin('unknown','text','ping',1,{}, {'scope':'model_test'})
    rows=Ledger(tmp_path).records()
    assert len(rows)==2 and rows[0]['pricing']['usd_cny']=='7'
    summary=summarize(rows)
    assert summary['requests']==2 and summary['total_tokens']==1100 and summary['unknown_usage']==1
    assert '未知' in summary_text(summary)
    assert '0.036960' in summary_text(summary)
    assert '未记录' in summary_text(None)


def test_invalid_answer_retry_and_reject_still_count(tmp_path,monkeypatch):
    from agent_client import AgentClient
    from billing import Ledger,summarize
    monkeypatch.setattr('agent_client.time.sleep',lambda _:None)
    client=AgentClient({'base_url':'https://example.test/v1','text_model':'gpt-5.6-sol','attempts':2},'PRIVATE_KEY')
    client.ledger=Ledger(tmp_path)
    responses=['not json',json.dumps({'decision':'reject','reason':'short'})]
    class Opener:
        def open(self,*args,**kwargs):
            return io.BytesIO(json.dumps({'usage':usage(),'choices':[{'finish_reason':'stop',
                'message':{'content':responses.pop(0)}}]}).encode())
    client.opener=Opener()
    assert client.call('screen',{'text':'PRIVATE_DOCUMENT'})['decision']=='reject'
    assert summarize(client.ledger.records())['total_tokens']==2200
    raw=(tmp_path/'ai_usage.jsonl').read_text(encoding='utf-8')
    assert 'PRIVATE_KEY' not in raw and 'PRIVATE_DOCUMENT' not in raw


def test_timeout_is_unknown_not_zero(tmp_path):
    from agent_client import AgentClient,AgentError
    from billing import Ledger,summarize
    client=AgentClient({'base_url':'https://example.test/v1','text_model':'gpt-5.6-sol','attempts':1},'key')
    client.ledger=Ledger(tmp_path)
    class Opener:
        def open(self,*args,**kwargs): raise TimeoutError()
    client.opener=Opener()
    with pytest.raises(AgentError): client.call('ping',{})
    assert summarize(client.ledger.records())['unknown_usage']==1


def test_pipeline_billing_saved_and_rules_cost_nothing(tmp_path,monkeypatch):
    from auto_pipeline import AutoProcessor
    from engine import Store,scan_sources
    from test_workbench import paper
    source=tmp_path/'in';source.mkdir()
    (source/'paper.txt').write_text(paper(),encoding='utf-8')
    p=AutoProcessor(Store(tmp_path/'out'),{'base_url':'https://example.test/v1','text_model':'gpt-5.6-sol','attempts':1},'key')
    class Opener:
        def open(self,*args,**kwargs):
            return io.BytesIO(json.dumps({'usage':usage(),'choices':[{'finish_reason':'stop',
                'message':{'content':'invalid'}}]}).encode())
    p.agent.opener=Opener();events=[]
    result=p.run(scan_sources(source),events.append)
    assert result['billing']['total_tokens']==1100
    assert result['items'][0]['billing']['total_tokens']==1100
    assert result['items'][0]['status']=='reject'
    assert result['items'][0]['reason_kind']=='technical'
    assert p.store.load(result['run_id'])['billing']==result['billing']
    assert any(e.get('billing_item',{}).get('total_tokens')==1100 for e in events)
    (source/'short.txt').write_text('too short',encoding='utf-8')
    result=p.run([{'path':str(source/'short.txt')}])
    assert result['items'][0]['status']=='reject' and result['billing']['requests']==0
