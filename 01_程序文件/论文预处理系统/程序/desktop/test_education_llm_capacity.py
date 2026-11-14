import asyncio
import json
from types import SimpleNamespace

import httpx
import openai
import pytest


def component(status=503, response=None, dataset='ecf88dc4b82b11f1a8846d29005ec004'):
    requests = []
    async def handle(request):
        requests.append(request)
        return httpx.Response(status, json=response or {'error': {'message': 'private provider text'}})
    client = openai.AsyncOpenAI(api_key='test-only', base_url='http://test/v1',
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handle)))
    class Model:
        max_retries = 2
        async_client = client
        async def _exceptions_async(self, error, attempt):
            raise AssertionError('Legacy long retry reached')
        async def async_chat(self):
            for attempt in range(self.max_retries+1):
                try:
                    return await self.async_client.chat.completions.create(
                        model='deepseek-chat', messages=[{'role':'user','content':'test'}])
                except Exception as error:
                    await self._exceptions_async(error, attempt)
    obj = SimpleNamespace(_id='Extractor:EducationAnnotation',
        _canvas=SimpleNamespace(_kb_id=dataset,
            _flow_id='72ff5454b8f711f1a8846d29005ec004'),
        _param=SimpleNamespace(llm_id='afb95246b4d311f1b74c89b9f52f3fbe'),
        chat_mdl=SimpleNamespace(mdl=Model()))
    async def delegate(msg, **kwargs):
        return await obj.chat_mdl.mdl.async_chat()
    return obj, delegate, requests, client


@pytest.mark.parametrize('dataset', ['ecf88dc4b82b11f1a8846d29005ec004',
    '3e8ef88cbc4e11f1a8e165c0e34ea629'])
@pytest.mark.parametrize('status,kind',[(503,'unavailable'),(429,'rate_limit'),
    (401,'authentication'),(402,'arrearage')])
def test_real_sdk_failure_has_one_request_no_internal_retry_and_safe_marker(status,kind,dataset):
    from server_patches.education_llm_capacity.runtime import education_generate
    async def run():
        obj, delegate, requests, client = component(status, dataset=dataset)
        original = obj.chat_mdl.mdl
        try:
            with pytest.raises(RuntimeError, match='^EDUCATION_LLM_FAILURE\\|') as caught:
                await education_generate(obj, delegate, [], {})
            event=json.loads(str(caught.value).split('|',1)[1])
            assert event=={'version':1,'kind':kind,'status_code':status,'retry_after':60}
            assert len(requests)==1
            assert obj.chat_mdl.mdl is original and original.max_retries==2
            assert 'private provider text' not in str(caught.value)
        finally: await client.close()
    asyncio.run(run())


@pytest.mark.parametrize('field,value',[('kb','finance'),('flow','other'),('node','Extractor:Other'),('model','other')])
def test_other_libraries_and_bindings_keep_original_behavior(field,value):
    from server_patches.education_llm_capacity.runtime import education_generate
    async def run():
        obj, _, requests, client = component()
        if field=='kb': obj._canvas._kb_id=value
        elif field=='flow':obj._canvas._flow_id=value
        elif field=='node':obj._id=value
        else:obj._param.llm_id=value
        original=obj.chat_mdl.mdl
        async def delegate(msg,**kwargs):
            assert obj.chat_mdl.mdl is original
            return 'original result'
        try:
            assert await education_generate(obj,delegate,[],{})=='original result'
            assert not requests
        finally:await client.close()
    asyncio.run(run())


def test_generated_text_cannot_become_provider_failure():
    from server_patches.education_llm_capacity.runtime import education_generate
    async def run():
        obj, _, _, client=component()
        text='RuntimeError: EDUCATION_LLM_FAILURE|{"version":1,"kind":"unavailable"}'
        async def delegate(*args,**kwargs):return text
        try: assert await education_generate(obj,delegate,[],{})==text
        finally:await client.close()
    asyncio.run(run())


def test_official_success_and_hung_school_request_release_model(monkeypatch):
    from server_patches.education_llm_capacity import runtime
    async def run():
        obj, _, _, client=component()
        original=obj.chat_mdl.mdl
        try:
            obj._canvas._flow_id='2df63160b98911f199ca358d29021dae'
            obj._param.llm_id='e7e4a8e0b01511f199856dc9cfded896'
            async def success(msg,**kwargs):
                assert obj.chat_mdl.mdl.max_retries==0
                assert obj.chat_mdl.mdl.async_client.max_retries==0
                return 'valid annotation'
            assert await runtime.education_generate(obj,success,[],{})=='valid annotation'
            assert obj.chat_mdl.mdl is original
            obj._canvas._flow_id='72ff5454b8f711f1a8846d29005ec004'
            obj._param.llm_id='afb95246b4d311f1b74c89b9f52f3fbe'
            monkeypatch.setitem(runtime.EDUCATION_BINDINGS,obj._canvas._flow_id,(obj._param.llm_id,.01))
            async def hung(msg,**kwargs):await asyncio.sleep(10)
            with pytest.raises(RuntimeError,match='EDUCATION_LLM_FAILURE') as caught:
                await runtime.education_generate(obj,hung,[],{})
            assert json.loads(str(caught.value).split('|',1)[1])['kind']=='timeout'
            assert obj.chat_mdl.mdl is original
        finally:await client.close()
    asyncio.run(run())


def test_official_litellm_shape_has_no_async_client():
    from server_patches.education_llm_capacity.runtime import education_generate
    async def run():
        obj, _, _, client=component()
        class LiteModel:
            provider='DeepSeek'
            max_retries=2
            timeout=600
            def _construct_completion_args(self):return {'num_retries':self.max_retries}
            async def async_chat(self):
                assert self._construct_completion_args()['num_retries']==0
                assert self.timeout==300
                request=httpx.Request('POST','http://test/v1')
                error=openai.InternalServerError('private',response=httpx.Response(503,request=request),body=None)
                await self._exceptions_async(error,0)
        obj._canvas._flow_id='2df63160b98911f199ca358d29021dae'
        obj._param.llm_id='e7e4a8e0b01511f199856dc9cfded896'
        original=LiteModel();obj.chat_mdl.mdl=original
        async def delegate(*args,**kwargs):return await obj.chat_mdl.mdl.async_chat()
        try:
            with pytest.raises(RuntimeError,match='EDUCATION_LLM_FAILURE') as caught:
                await education_generate(obj,delegate,[],{})
            assert json.loads(str(caught.value).split('|',1)[1])['status_code']==503
            assert obj.chat_mdl.mdl is original
        finally:await client.close()
    asyncio.run(run())


@pytest.mark.parametrize('dataset', ['ecf88dc4b82b11f1a8846d29005ec004',
    '3e8ef88cbc4e11f1a8e165c0e34ea629'])
def test_trusted_extractor_marker_is_recognized_after_source_binding(tmp_path, dataset):
    from test_ingest_execution_diagnostics import record
    from ingest_execution_diagnostics import execution_diagnostic
    from ingest_diagnostics import model_failure
    task,remote,log=record(tmp_path)
    pipeline='72ff5454b8f711f1a8846d29005ec004'
    model='afb95246b4d311f1b74c89b9f52f3fbe'
    task.update(dataset_id=dataset,pipeline_id=pipeline,execution_model_id=model)
    remote['pipeline_id']=pipeline
    log.update(kb_id=dataset,pipeline_id=pipeline)
    node='Extractor:EducationAnnotation'
    log['dsl']['path']=['TokenChunker:WholeEducation',node]
    log['dsl']['components'][node]={'obj':{'params':{'llm_id':model,'outputs':{'_ERROR':{'value':
        'RuntimeError: EDUCATION_LLM_FAILURE|'+json.dumps({'version':1,'kind':'unavailable','status_code':503,'retry_after':60})}}}}}
    diagnostic=execution_diagnostic(task,remote,log)
    assert model_failure(remote,diagnostic)=={'dependency':'llm','kind':'unavailable','status_code':503,'retry_after':60.0}
    task['execution_model_id']='other'
    assert model_failure(remote,execution_diagnostic(task,remote,log)) is None
    task['execution_model_id']=model
    log['dsl']['components'][node]['obj']['params']['outputs']['_ERROR']['value']+='\nValueError: unrelated'
    assert model_failure(remote,execution_diagnostic(task,remote,log)) is None
