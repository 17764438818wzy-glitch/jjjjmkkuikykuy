"""Offline failure injection against the deployed RAGFlow Base and OpenAI SDK."""
import asyncio
import json
import logging
from types import SimpleNamespace
from unittest.mock import patch
import httpx
from openai import AsyncOpenAI
from rag.llm.chat_model import Base, LiteLLMBase
import litellm
from runtime import education_generate

logging.disable(logging.CRITICAL)


async def main():
    outputs=[]
    for repaired in (False, True):
        calls=[];delays=[]
        async def respond(request):
            calls.append(1)
            return httpx.Response(503,json={'error':{'message':'Service temporarily unavailable'}})
        async def sleep(delay): delays.append(delay)
        model=Base('test-only','deepseek-chat','http://offline/v1',max_retries=2,retry_interval=2)
        await model.async_client.close()
        client=AsyncOpenAI(api_key='test-only',base_url='http://offline/v1',
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(respond)))
        model.async_client=client
        model._get_delay=lambda:150
        obj=SimpleNamespace(_id='Extractor:EducationAnnotation',
            _canvas=SimpleNamespace(_kb_id='ecf88dc4b82b11f1a8846d29005ec004',_flow_id='72ff5454b8f711f1a8846d29005ec004'),
            _param=SimpleNamespace(llm_id='afb95246b4d311f1b74c89b9f52f3fbe'),
            chat_mdl=SimpleNamespace(mdl=model))
        async def delegate(msg,**kwargs):
            return await obj.chat_mdl.mdl.async_chat('test',[{'role':'user','content':'test'}],{})
        failure=None
        try:
            with patch('asyncio.sleep',sleep):
                if repaired: await education_generate(obj,delegate,[],{})
                else: await delegate([])
        except RuntimeError as exc:
            if not str(exc).startswith('EDUCATION_LLM_FAILURE|'):raise
            failure=json.loads(str(exc).split('|',1)[1])
        finally:
            await client.close();model.client.close()
        if repaired:
            assert len(calls)==1 and not delays and failure['status_code']==503
        else:
            assert len(calls)==9 and delays.count(150)==2
        outputs.append({'repaired':repaired,'http_requests':len(calls),
                        'long_retry_delays':sum(x>=100 for x in delays),'failure':failure})
    model=LiteLLMBase('test-only','deepseek-v4-flash',provider='DeepSeek',max_retries=2)
    obj=SimpleNamespace(_id='Extractor:EducationAnnotation',
        _canvas=SimpleNamespace(_kb_id='ecf88dc4b82b11f1a8846d29005ec004',_flow_id='2df63160b98911f199ca358d29021dae'),
        _param=SimpleNamespace(llm_id='e7e4a8e0b01511f199856dc9cfded896'),
        chat_mdl=SimpleNamespace(mdl=model))
    requests=[]
    async def unavailable(**kwargs):
        requests.append({'num_retries':kwargs['num_retries'],'timeout':kwargs['timeout']})
        raise litellm.ServiceUnavailableError(message='private',llm_provider='deepseek',model='deepseek-v4-flash')
    async def delegate(msg,**kwargs):
        return await obj.chat_mdl.mdl.async_chat('test',[{'role':'user','content':'test'}],{})
    with patch('litellm.acompletion',unavailable):
        try: await education_generate(obj,delegate,[],{})
        except RuntimeError as exc:
            failure=json.loads(str(exc).split('|',1)[1])
        else: raise AssertionError('LiteLLM failure was not raised')
    assert requests==[{'num_retries':0,'timeout':300}] and failure['status_code']==503
    assert obj.chat_mdl.mdl is model
    outputs.append({'adapter':'LiteLLMBase','requests':requests,'failure':failure})
    print(json.dumps(outputs))


asyncio.run(main())
