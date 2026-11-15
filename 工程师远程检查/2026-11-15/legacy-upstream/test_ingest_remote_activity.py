from ingest_queue import QueueStore
from test_ingest_dual_router import settings
from test_ingest_routed_queue import waiting
from urllib.parse import parse_qs, urlsplit

import pytest


def test_remote_counts_exclude_owned_attempts_but_include_old_worker(tmp_path):
    from ingest_remote_activity import RagflowInFlightCounts
    queue=QueueStore(tmp_path/'queue.sqlite3')
    task_id,_=waiting(queue,'owned')
    queue.claim_routed_parse('worker',100,120,settings()['school'],2,1,{})
    class Client:
        def request(self,method,path):
            assert method=='GET'
            return {'total':3,'docs':[{'id':'remote-owned','run':'RUNNING','pipeline_id':'pipe-1'},
                            {'id':'old-worker','run':'RUNNING','pipeline_id':'pipe-1'},
                            {'id':'paid','run':'RUNNING','pipeline_id':'official-pipe'}]}
    reader=RagflowInFlightCounts(Client,queue,settings())
    assert reader()=={'total':2,'school':1,'official':1}
    queue.transition(task_id,'parse_intent','technical_review',remote_run='FAIL')
    # Recompute ownership even when the server snapshot is cached.
    assert reader()=={'total':3,'school':2,'official':1}


def test_unavailable_remote_activity_fails_closed(tmp_path):
    import pytest
    from ingest_remote_activity import RagflowInFlightCounts
    queue=QueueStore(tmp_path/'queue.sqlite3')
    class Client:
        def request(self,*args): raise TimeoutError('offline')
    with pytest.raises(TimeoutError):
        RagflowInFlightCounts(Client,queue,settings())()


def test_finished_library_size_does_not_expand_activity_inventory(tmp_path):
    from ingest_remote_activity import RagflowInFlightCounts
    queue=QueueStore(tmp_path/'queue.sqlite3')
    calls=[]
    class Client:
        def request(self,method,path):
            assert method=='GET'
            calls.append(path)
            query=parse_qs(urlsplit(path).query)
            if query.get('run')==['RUNNING']:
                return {'total':0,'docs':[]}
            offset=(int(query['page'][0])-1)*100
            return {'total':150001,'docs':[
                {'id':str(n),'run':'DONE','pipeline_id':'pipe-1'}
                for n in range(offset,min(offset+100,150001))]}
    assert RagflowInFlightCounts(Client,queue,settings())()=={'total':0,'school':0,'official':0}
    assert len(calls)==1


def test_ignored_running_filter_fails_closed(tmp_path):
    from ingest_remote_activity import RagflowInFlightCounts
    queue=QueueStore(tmp_path/'queue.sqlite3')
    class Client:
        def request(self,method,path):
            return {'total':1,'docs':[{'id':'finished','run':'DONE','pipeline_id':'pipe-1'}]}
    with pytest.raises(ValueError,match='RUNNING'):
        RagflowInFlightCounts(Client,queue,settings())()


@pytest.mark.parametrize('payload',[
    {'docs':[]},
    {'total':-1,'docs':[]},
    {'total':True,'docs':[]},
    {'total':2,'docs':[{'id':'only','run':'RUNNING','pipeline_id':'pipe-1'}]},
    {'total':0,'docs':[{'id':'unexpected','run':'RUNNING','pipeline_id':'pipe-1'}]},
    {'total':1,'docs':[{'run':'RUNNING','pipeline_id':'pipe-1'}]},
    {'total':2,'docs':[{'id':'same','run':'RUNNING','pipeline_id':'pipe-1'}]*2},
])
def test_incomplete_active_inventory_fails_closed(tmp_path,payload):
    from ingest_remote_activity import RagflowInFlightCounts
    queue=QueueStore(tmp_path/'queue.sqlite3')
    class Client:
        def request(self,method,path): return payload
    with pytest.raises(ValueError,match='incomplete'):
        RagflowInFlightCounts(Client,queue,settings())()


class ActivityClient:
    """Fake only the remote API; queue ownership and routing stay real."""
    def __init__(self,docs,pipelines=None,models=None):
        self.docs=docs
        self.pipelines=pipelines or {}
        self.models=models or []
        self.calls=[]

    def request(self,method,path):
        assert method=='GET'
        self.calls.append(path)
        if path=='/models': return self.models
        if path.startswith('/agents/'):
            pipeline=path.removeprefix('/agents/')
            return {'id':pipeline,'dsl':{'components':{
                'Extractor:Legacy':{'obj':{'component_name':'Extractor','params':{
                    'llm_id':self.pipelines[pipeline]}}}}}}
        query=parse_qs(urlsplit(path).query)
        assert query['run']==['RUNNING']
        offset=(int(query['page'][0])-1)*int(query['page_size'][0])
        return {'total':len(self.docs),'docs':self.docs[offset:offset+100]}


def test_legacy_school_pipeline_reserves_school_capacity(tmp_path):
    from ingest_remote_activity import RagflowInFlightCounts
    from ingest_dual_router import DeepSeekRouter
    queue=QueueStore(tmp_path/'queue.sqlite3')
    waiting(queue,'next')
    routes=settings(6,2)
    client=ActivityClient([
        {'id':f'legacy-{n}','run':'RUNNING','pipeline_id':'legacy-school'} for n in range(6)
    ],{'legacy-school':'school-model'})
    external=RagflowInFlightCounts(lambda:client,queue,routes)()
    assert external=={'total':6,'school':6,'official':0}
    assert DeepSeekRouter(queue,routes).claim('worker',100,external)['channel_key']=='official'


@pytest.mark.parametrize('provider,instance,expected',[
    ('DeepSeek','school',{'total':1,'school':1,'official':0}),
    ('DeepSeek','official',{'total':1,'school':0,'official':1}),
    ('DifferentProvider','school',{'total':1,'school':0,'official':0}),
    ('DeepSeek','other',{'total':1,'school':0,'official':0}),
])
def test_legacy_model_alias_uses_verified_provider_instance(tmp_path,provider,instance,expected):
    from ingest_remote_activity import RagflowInFlightCounts
    queue=QueueStore(tmp_path/'queue.sqlite3')
    client=ActivityClient([
        {'id':'legacy-doc','run':'RUNNING','pipeline_id':'legacy-pipeline'}
    ],{'legacy-pipeline':'alias-model'},[
        {'model_id':'alias-model','provider_name':provider,'instance_name':instance,
         'name':'deepseek-alias','model_type':['chat']}])
    assert RagflowInFlightCounts(lambda:client,queue,settings())()==expected


@pytest.mark.parametrize('pipeline,model,registry',[
    ('unknown-pipeline','missing-model',[]),
    ('unknown-pipeline','partial-model',[{'model_id':'partial-model','provider_name':'DeepSeek'}]),
    ('unknown-pipeline',None,[]),
    (None,None,[]),
])
def test_unknown_model_binding_reserves_each_possible_channel(tmp_path,pipeline,model,registry):
    from ingest_remote_activity import RagflowInFlightCounts
    queue=QueueStore(tmp_path/'queue.sqlite3')
    client=ActivityClient([{'id':'unknown','run':'RUNNING','pipeline_id':pipeline}],
                          {pipeline:model},registry)
    assert RagflowInFlightCounts(lambda:client,queue,settings())()=={'total':1,'school':1,'official':1}


def test_pipeline_and_model_identity_cache_is_bounded_in_time(tmp_path,monkeypatch):
    import ingest_remote_activity
    clock=[0.0]
    monkeypatch.setattr(ingest_remote_activity.time,'monotonic',lambda:clock[0])
    queue=QueueStore(tmp_path/'queue.sqlite3')
    client=ActivityClient([
        {'id':f'legacy-{n}','run':'RUNNING','pipeline_id':f'legacy-{n}'} for n in range(2)
    ],{'legacy-0':'alias','legacy-1':'alias'},[
        {'model_id':'alias','provider_name':'DeepSeek','instance_name':'school',
         'name':'deepseek-alias','model_type':['chat']}])
    reader=ingest_remote_activity.RagflowInFlightCounts(lambda:client,queue,settings())
    assert reader()=={'total':2,'school':2,'official':0}
    clock[0]=20.0
    assert reader()=={'total':2,'school':2,'official':0}
    assert client.calls.count('/models')==1
    assert client.calls.count('/agents/legacy-0')==1
    assert client.calls.count('/agents/legacy-1')==1
    client.models[0]['instance_name']='official'
    clock[0]=301.0
    assert reader()=={'total':2,'school':0,'official':2}


def test_mismatched_pipeline_identity_fails_closed(tmp_path):
    from ingest_remote_activity import RagflowInFlightCounts
    queue=QueueStore(tmp_path/'queue.sqlite3')
    class WrongPipeline(ActivityClient):
        def request(self,method,path):
            result=super().request(method,path)
            if path.startswith('/agents/'):
                result['id']='different-pipeline'
            return result
    client=WrongPipeline([{'id':'legacy','run':'RUNNING','pipeline_id':'old'}],{'old':'school-model'})
    with pytest.raises(ValueError,match='identity'):
        RagflowInFlightCounts(lambda:client,queue,settings())()


def test_same_remote_id_in_another_dataset_is_not_owned(tmp_path):
    from ingest_remote_activity import RagflowInFlightCounts
    from test_ingest_contracts import spec
    queue=QueueStore(tmp_path/'queue.sqlite3')
    routes=settings()
    queue.enqueue(spec(dataset_id='another-dataset'),{
        'status':'running','remote_id':'same-remote-id'})
    client=ActivityClient([{'id':'same-remote-id','run':'RUNNING','pipeline_id':'pipe-1'}])
    assert RagflowInFlightCounts(lambda:client,queue,routes)()=={'total':1,'school':1,'official':0}


def test_conflicting_model_registry_identity_fails_closed(tmp_path):
    from ingest_remote_activity import RagflowInFlightCounts
    queue=QueueStore(tmp_path/'queue.sqlite3')
    client=ActivityClient([{'id':'legacy','run':'RUNNING','pipeline_id':'old'}],{'old':'alias'},[
        {'model_id':'alias','provider_name':'DeepSeek','instance_name':'school'},
        {'model_id':'alias','provider_name':'DeepSeek','instance_name':'official'},
    ])
    with pytest.raises(ValueError,match='identity'):
        RagflowInFlightCounts(lambda:client,queue,settings())()


@pytest.mark.parametrize('active_count,requests',[(100,1),(101,2)])
def test_only_active_pages_are_read_to_the_reported_total(tmp_path,active_count,requests):
    from ingest_remote_activity import RagflowInFlightCounts
    queue=QueueStore(tmp_path/'queue.sqlite3')
    client=ActivityClient([
        {'id':str(n),'run':'RUNNING','pipeline_id':'pipe-1'} for n in range(active_count)])
    assert RagflowInFlightCounts(lambda:client,queue,settings())()=={
        'total':active_count,'school':active_count,'official':0}
    assert len(client.calls)==requests


def test_changing_active_total_fails_closed(tmp_path):
    from ingest_remote_activity import RagflowInFlightCounts
    queue=QueueStore(tmp_path/'queue.sqlite3')
    class ChangingInventory(ActivityClient):
        def request(self,method,path):
            result=super().request(method,path)
            if len(self.calls)>1: result['total']=100
            return result
    client=ChangingInventory([
        {'id':str(n),'run':'RUNNING','pipeline_id':'pipe-1'} for n in range(101)])
    with pytest.raises(ValueError,match='incomplete'):
        RagflowInFlightCounts(lambda:client,queue,settings())()


def test_cached_inventory_rechecks_new_queue_ownership(tmp_path):
    from ingest_remote_activity import RagflowInFlightCounts
    queue=QueueStore(tmp_path/'queue.sqlite3')
    waiting(queue,'owned')
    client=ActivityClient([{'id':'remote-owned','run':'RUNNING','pipeline_id':'pipe-1'}])
    reader=RagflowInFlightCounts(lambda:client,queue,settings())
    assert reader()=={'total':1,'school':1,'official':0}
    queue.claim_routed_parse('worker',100,120,settings()['school'],2,1,{})
    assert reader()=={'total':0,'school':0,'official':0}
    assert len(client.calls)==1


def test_expired_snapshot_is_not_used_when_refresh_fails(tmp_path,monkeypatch):
    import ingest_remote_activity
    clock=[0.0]
    monkeypatch.setattr(ingest_remote_activity.time,'monotonic',lambda:clock[0])
    queue=QueueStore(tmp_path/'queue.sqlite3')
    class OfflineAfterFirstRead(ActivityClient):
        def request(self,method,path):
            if self.calls: raise TimeoutError('offline')
            return super().request(method,path)
    client=OfflineAfterFirstRead([])
    reader=ingest_remote_activity.RagflowInFlightCounts(lambda:client,queue,settings())
    assert reader()=={'total':0,'school':0,'official':0}
    clock[0]=16.0
    with pytest.raises(TimeoutError): reader()
