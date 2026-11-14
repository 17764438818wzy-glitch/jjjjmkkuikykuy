import io,json
from email.message import Message
from urllib.error import HTTPError
import pytest
from agent_client import AgentClient,AgentError


def test_rate_limit_obeys_server_retry_after_without_extra_attempts(monkeypatch):
    waits=[];monkeypatch.setattr('agent_client.time.sleep',waits.append)
    c=AgentClient({'base_url':'https://fixture.test/v1','text_model':'fixture','attempts':2},'test-key')
    class Transport:
        count=0
        def open(self,request,timeout):
            self.count+=1;headers=Message();headers['Retry-After']='17'
            raise HTTPError(request.full_url,429,'limited',headers,None)
    c.opener=Transport()
    with pytest.raises(AgentError,match='429'):c.call('privacy_text',{'text':'测试文本'})
    assert c.opener.count==2 and waits==[17]


def test_non_json_decision_gets_explicit_format_feedback(monkeypatch):
    monkeypatch.setattr('agent_client.time.sleep',lambda _:None)
    c=AgentClient({'base_url':'https://fixture.test/v1','text_model':'fixture','attempts':2},'test-key')
    class Transport:
        requests=[]
        def open(self,request,timeout):
            self.requests.append(json.loads(request.data))
            content='不是 JSON' if len(self.requests)==1 else json.dumps({'decision':'pass','reason':'无身份','sensitive_strings':[]})
            return io.BytesIO(json.dumps({'choices':[{'finish_reason':'stop','message':{'content':content}}]}).encode())
    c.opener=Transport();assert c.call('privacy_text',{'text':'测试文本'})['decision']=='pass'
    assert 'JSON' in c.opener.requests[1]['messages'][-1]['content']
    assert len(c.opener.requests[1]['messages'])==3
