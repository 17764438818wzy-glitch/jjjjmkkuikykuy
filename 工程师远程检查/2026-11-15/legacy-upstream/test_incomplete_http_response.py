"""Incomplete HTTP bodies are transient service errors, never model decisions."""
from http.client import IncompleteRead

import pytest

from agent_client import AgentClient, AgentError
from billing import Ledger
from stage_cache import StageCache
from test_vision_failover import _cfg, _ok, _png, _Response


class InterruptedBody(_Response):
    def __init__(self):
        # Even syntactically complete JSON in the partial buffer is untrusted:
        # the HTTP body has not been received in full.
        super().__init__(_ok({'prompt_tokens':40,'completion_tokens':20,'total_tokens':60},
            {'decision':'pass','reason':'untrusted-partial-buffer'}).body)

    def read(self,_limit=-1):
        raise IncompleteRead(self.body,37)


class Transport:
    def __init__(self,*responses):
        self.responses=list(responses);self.requests=[];self.timeouts=[]

    def open(self,request,timeout):
        self.requests.append(request);self.timeouts.append(timeout)
        return self.responses.pop(0)


def configured_client(tmp_path,transport,budget):
    config=_cfg(base_url='https://fixture.test/v1',text_model='fixture',attempts=budget,
        timeout=37,concurrency={'enabled':True,'max_retries':budget,'backoff_base_seconds':0},
        vision_model={'retry':{'max_tries':3,'backoff_base_seconds':0}})
    client=AgentClient(config,key='test',vision_key='test',fallback_key='test',state_root=tmp_path)
    client.opener=transport;client.ledger=Ledger(tmp_path)
    client.stage_cache=StageCache(tmp_path);client.cache_scope=('d'*64,'incomplete-response-test')
    return client


@pytest.mark.parametrize('vision',[False,True])
def test_incomplete_body_retries_identical_request_and_only_complete_result_can_be_cached(tmp_path,monkeypatch,vision):
    monkeypatch.setattr('agent_client.time.sleep',lambda _:None)
    complete={'decision':'pass','reason':'fully received and validated'}
    transport=Transport(InterruptedBody(),_ok({'prompt_tokens':3,'completion_tokens':2,'total_tokens':5},complete))
    client=configured_client(tmp_path,transport,2)
    images=[_png()] if vision else []
    result=client.call('screen',{'text':'original research'},images,channel='primary')
    assert result==complete
    assert len(transport.requests)==2
    assert transport.requests[0].data==transport.requests[1].data
    assert transport.timeouts==[37,37]
    records=client.ledger.records()
    assert [(r['error_type'],r['ok']) for r in records]==[('service_unavailable',False),(None,True)]
    assert records[0]['usage'] is None and records[0]['total_tokens'] is None
    assert records[1]['total_tokens']==5
    client.accept_cached_result(result)
    assert client.call('screen',{'text':'original research'},images,channel='primary')==complete
    assert client.last_call_cache_hit is True
    assert len(transport.requests)==2


@pytest.mark.parametrize('vision',[False,True])
@pytest.mark.parametrize('budget',[1,2,3])
def test_repeated_incomplete_bodies_exhaust_only_existing_budget_without_cache_or_usage_guess(tmp_path,monkeypatch,vision,budget):
    monkeypatch.setattr('agent_client.time.sleep',lambda _:None)
    transport=Transport(*[InterruptedBody() for _ in range(budget)],_ok())
    client=configured_client(tmp_path,transport,budget)
    with pytest.raises(AgentError,match='响应.*未完整接收.*重试已用尽') as caught:
        client.call('screen',{'text':'original research'},[_png()] if vision else [],channel='primary')
    assert 'untrusted-partial-buffer' not in str(caught.value)
    assert len(transport.requests)==budget
    assert transport.timeouts==[37]*budget
    assert not list(client.stage_cache.root.glob('*.json'))
    records=client.ledger.records()
    assert len(records)==budget
    assert all(r['error_type']=='service_unavailable' and r['ok'] is False for r in records)
    assert all(r['usage'] is None and r['total_tokens'] is None for r in records)
