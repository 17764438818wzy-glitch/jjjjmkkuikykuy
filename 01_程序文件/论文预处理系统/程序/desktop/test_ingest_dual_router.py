import time
from pathlib import Path

from ingest_queue import QueueStore
from ingest_scheduler import WorkPool
from test_ingest_routed_queue import route, waiting
from test_ingest_scheduler import FakeGateway


def settings(school_limit=1,official_limit=1):
    return {'enabled':True,'total_limit':school_limit+official_limit,
            'service_url':'http://localhost:9380','dataset_id':'md-1',
            'source_pipeline_id':'pipe-1','contract_version':'contract-1',
            'school':dict(route(),initial_limit=school_limit,max_limit=12),
            'official':dict(route('official'),initial_limit=official_limit,max_limit=official_limit),
            'adaptive':False}


def signal(dependency='llm',kind='rate_limit',code=429):
    return {'source':'execution_node','component_id':'Extractor:EducationAnnotation' if dependency=='llm' else 'Indexer:Embedding',
            'dependency':dependency,'kind':kind,'status_code':code,'retry_after':30}


def finish(queue,task,outcome='technical_review'):
    queue.transition(task['id'],'parse_intent',outcome,remote_run='FAIL' if outcome!='accepted' else 'DONE')


def test_school_first_and_official_overflow_pin_attempts(tmp_path):
    from ingest_dual_router import DeepSeekRouter
    queue=QueueStore(tmp_path/'queue.sqlite3'); waiting(queue,'p1'); waiting(queue,'p2')
    router=DeepSeekRouter(queue,settings())
    first=router.claim('worker',100,{})
    second=router.claim('worker',100,{})
    assert first['channel_key']=='school'
    assert second['channel_key']=='official'
    assert router.claim('worker',100,{}) is None


def test_llm_rate_limit_reduces_capacity_and_survives_restart(tmp_path):
    from ingest_dual_router import DeepSeekRouter
    queue=QueueStore(tmp_path/'queue.sqlite3'); task_id,_=waiting(queue)
    config=settings(6,2); config['adaptive']=True; router=DeepSeekRouter(queue,config)
    task=router.claim('worker',100,{})
    finish(queue,task)
    assert router.observe(task,{'run':'FAIL'},'technical_review',signal(),110)
    restored=DeepSeekRouter(QueueStore(queue.path),config)
    assert restored.snapshot()['school']['limit']==3
    selected=restored.claim('worker',111,{})
    assert selected['channel_key']=='official'
    assert selected['id']==task_id
    assert selected['execution_attempt']==2


def test_embedding_fault_blocks_both_channels_without_model_retry(tmp_path):
    from ingest_dual_router import DeepSeekRouter
    queue=QueueStore(tmp_path/'queue.sqlite3'); waiting(queue,'p1'); waiting(queue,'p2')
    router=DeepSeekRouter(queue,settings())
    task=router.claim('worker',100,{})
    finish(queue,task)
    assert not router.observe(task,{'run':'FAIL'},'technical_review',signal('embedding','arrearage',402),110)
    assert router.claim('worker',111,{}) is None
    assert queue.get(task['id'])['status']=='technical_review'


def test_quality_decision_and_unobservable_failure_do_not_switch(tmp_path):
    from ingest_dual_router import DeepSeekRouter
    queue=QueueStore(tmp_path/'queue.sqlite3'); waiting(queue,'p1'); waiting(queue,'p2')
    router=DeepSeekRouter(queue,settings())
    task=router.claim('worker',100,{})
    finish(queue,task,'quality_hold')
    assert not router.observe(task,{'run':'FAIL'},'quality_hold',signal(),110)
    assert router.snapshot()['school']['valid_decisions']==1
    second=router.claim('worker',111,{})
    assert second['channel_key']=='school'
    finish(queue,second)
    assert not router.observe(second,{'run':'FAIL','progress_msg':'429 rate limit'},'technical_review',None,120)
    assert router.snapshot()['school']['unobservable_failures']==1


def test_official_auth_failure_does_not_stop_school(tmp_path):
    from ingest_dual_router import DeepSeekRouter
    queue=QueueStore(tmp_path/'queue.sqlite3')
    for n in range(3): waiting(queue,'p'+str(n))
    router=DeepSeekRouter(queue,settings())
    school=router.claim('worker',100,{})
    official=router.claim('worker',100,{})
    finish(queue,official)
    assert not router.observe(official,{'run':'FAIL'},'technical_review',signal(kind='authentication',code=401),110)
    finish(queue,school,'accepted')
    router.observe(school,{'run':'DONE'},'accepted',None,111)
    assert router.claim('worker',112,{})['channel_key']=='school'


def test_budget_reservation_blocks_official_but_not_school(tmp_path):
    from ingest_dual_router import DeepSeekRouter
    queue=QueueStore(tmp_path/'queue.sqlite3'); waiting(queue,'p1'); waiting(queue,'p2')
    config=settings(); config['official'].update(daily_cny_limit=1.0,reservation_cny=1.0)
    router=DeepSeekRouter(queue,config)
    queue.save_route_state('school',{'limit':1,'paused_until':200})
    first=router.claim('worker',100,{})
    assert first['channel_key']=='official'
    finish(queue,first,'accepted')
    router.observe(first,{'run':'DONE'},'accepted',None,110)
    assert router.claim('worker',111,{}) is None
    assert queue.parse_attempts(first['id'])[0]['cost_basis']=='reserved_upper_bound'
    assert router.claim('worker',201,{})['channel_key']=='school'


def test_contract_failures_do_not_increase_school_capacity(tmp_path):
    from ingest_dual_router import DeepSeekRouter
    queue=QueueStore(tmp_path/'queue.sqlite3')
    config=settings(); config.update(adaptive=True,window_size=2)
    router=DeepSeekRouter(queue,config)
    for n in range(2):
        waiting(queue,'p'+str(n)); task=router.claim('worker',100+n,{})
        finish(queue,task)
        router.observe(task,{'run':'FAIL','meta_fields':{'education_processing_issue':'annotation_invalid'}},
                       'technical_review',None,110+n)
    assert router.snapshot()['school']['limit']==1
    assert router.snapshot()['school']['contract_failures']==2


def test_capacity_growth_requires_saturated_valid_backlog(tmp_path):
    from ingest_dual_router import DeepSeekRouter
    queue=QueueStore(tmp_path/'queue.sqlite3')
    for n in range(5): waiting(queue,'p'+str(n))
    config=settings(); config.update(adaptive=True,window_size=2,total_limit=8,growth_step=3)
    router=DeepSeekRouter(queue,config)
    for n in range(2):
        task=router.claim('worker',100+n,{})
        # A blocked school claim with backlog establishes actual saturation.
        router.claim('worker',100+n,{})
        finish(queue,task,'accepted')
        router.observe(task,{'run':'DONE'},'accepted',None,110+n)
    assert router.snapshot()['school']['limit']==4


def test_pool_uses_routed_pipeline_for_overflow_and_releases_on_hold(tmp_path):
    from ingest_dual_router import DeepSeekRouter
    queue=QueueStore(tmp_path/'queue.sqlite3'); gateway=FakeGateway()
    for name in ('p1','p2','p3'):
        task_id,_=waiting(queue,name)
        gateway.docs['remote-'+name]={'id':'remote-'+name,'name':name+'.md','pipeline_id':'pipe-1','run':'UNSTART'}
    routed=[]
    def parse(task):
        routed.append((task['source_id'],task['pipeline_id']))
        gateway.docs[task['remote_id']].update(run='RUNNING',pipeline_id=task['pipeline_id'])
    gateway.parse=parse
    router=DeepSeekRouter(queue,settings())
    pool=WorkPool(queue,lambda:gateway,lambda *_:'quality_hold',parse_limit=2,router=router)
    try:
        for _ in range(60):
            pool.tick(); time.sleep(.01)
            if len(routed)==2: break
        assert routed==[('p1','pipe-1'),('p2','official-pipe')]
        gateway.docs['remote-p1']['run']='FAIL'
        for _ in range(60):
            pool.tick(); time.sleep(.01)
            if len(routed)==3: break
        assert routed[-1]==('p3','pipe-1')
        assert router.snapshot()['school']['valid_decisions']==1
    finally:
        pool.close()

def test_generic_dispatch_respects_external_activity_under_dual_total(tmp_path):
    from ingest_dual_router import DeepSeekRouter
    from test_ingest_contracts import spec
    q=QueueStore(tmp_path/'q.sqlite3')
    q.enqueue(spec(source_id='other',policy_key='generic',pipeline_id='other-pipe',channel_key='other'),{'status':'parse_waiting','remote_id':'remote-other'})
    gateway=FakeGateway()
    router=DeepSeekRouter(q,settings())
    pool=WorkPool(q,lambda:gateway,lambda *_:'accepted',parse_limit=2,router=router,external_counts_reader=lambda:{'total':2,'school':1,'official':1})
    try:
        pool.tick()
        assert q.get(1)['status']=='parse_waiting'
    finally: pool.close()


def test_fixed_quotas_replace_stale_capacity_without_borrowing_slots(tmp_path):
    from ingest_dual_router import DeepSeekRouter
    queue=QueueStore(tmp_path/'queue.sqlite3')
    for n in range(8): waiting(queue,'p'+str(n))
    queue.save_route_state('school',{'limit':6})
    queue.save_route_state('official',{'limit':2})
    router=DeepSeekRouter(QueueStore(queue.path),settings(2,4))
    claimed=[router.claim('worker',100,{}) for _ in range(8)]
    assert sum(task is not None and task['channel_key']=='school' for task in claimed)==2
    assert sum(task is not None and task['channel_key']=='official' for task in claimed)==4
    assert sum(task is not None for task in claimed)==6
    restored=DeepSeekRouter(QueueStore(queue.path),settings(2,4))
    assert restored.snapshot()['school']['limit']==2
    assert restored.snapshot()['official']['limit']==4


def test_school_pause_leaves_two_slots_empty_and_resumes_with_official_four(tmp_path):
    from ingest_dual_router import DeepSeekRouter
    queue=QueueStore(tmp_path/'queue.sqlite3')
    for n in range(10): waiting(queue,'p'+str(n))
    queue.save_route_state('school',{'paused_until':200,'limit':2})
    router=DeepSeekRouter(queue,settings(2,4))
    official=[router.claim('worker',100,{}) for _ in range(4)]
    assert all(task['channel_key']=='official' for task in official)
    assert router.claim('worker',100,{}) is None
    assert len(queue.list_statuses(('parse_intent',)))==4
    restored=DeepSeekRouter(QueueStore(queue.path),settings(2,4))
    school=[restored.claim('worker',201,{}) for _ in range(2)]
    assert all(task['channel_key']=='school' for task in school)
    assert restored.claim('worker',201,{}) is None
    assert len(queue.list_statuses(('parse_intent',)))==6


def test_fixed_quota_rate_limit_only_pauses_channel_then_restores_two_plus_four(tmp_path):
    from ingest_dual_router import DeepSeekRouter
    queue=QueueStore(tmp_path/'queue.sqlite3')
    for n in range(10): waiting(queue,'p'+str(n))
    router=DeepSeekRouter(queue,settings(2,4))
    task=router.claim('worker',100,{})
    finish(queue,task)
    assert router.observe(task,{'run':'FAIL'},'technical_review',signal(),110)
    assert router.snapshot()['school']['limit']==2
    assert router.snapshot()['school']['paused_until']==140
    official=[router.claim('worker',111,{}) for _ in range(4)]
    assert all(item['channel_key']=='official' for item in official)
    assert router.claim('worker',111,{}) is None
    restored=DeepSeekRouter(QueueStore(queue.path),settings(2,4))
    school=[restored.claim('worker',141,{}) for _ in range(2)]
    assert all(item['channel_key']=='school' for item in school)
    assert restored.claim('worker',141,{}) is None


def test_quality_holds_free_each_channel_without_circuit_or_model_fallback(tmp_path):
    from ingest_dual_router import DeepSeekRouter
    queue=QueueStore(tmp_path/'queue.sqlite3')
    for n in range(8): waiting(queue,'p'+str(n))
    router=DeepSeekRouter(queue,settings(2,4))
    claimed=[router.claim('worker',100,{}) for _ in range(6)]
    held=[next(task for task in claimed if task['channel_key']==channel)
          for channel in ('school','official')]
    for task in held:
        finish(queue,task,'quality_hold')
        assert not router.observe(task,{'run':'FAIL'},'quality_hold',signal(kind='unavailable',code=503),110)
        assert len(queue.parse_attempts(task['id']))==1
        assert queue.get(task['id'])['status']=='quality_hold'
    replacements=[router.claim('worker',111,{}) for _ in range(2)]
    assert [task['channel_key'] for task in replacements]==['school','official']
    snapshot=router.snapshot()
    assert not snapshot['shared'].get('halted')
    for channel in ('school','official'):
        assert not snapshot[channel]['halted']
        assert snapshot[channel]['paused_until']==0
        assert snapshot[channel]['technical_failures']==0


def test_unknown_school_attempts_keep_own_slots_but_official_still_gets_four(tmp_path):
    from ingest_dual_router import DeepSeekRouter
    queue=QueueStore(tmp_path/'queue.sqlite3')
    for n in range(8): waiting(queue,'p'+str(n))
    router=DeepSeekRouter(queue,settings(2,4))
    school=[router.claim('worker',100,{}) for _ in range(2)]
    for task in school:
        queue.transition(task['id'],'parse_intent','parse_unknown')
    restored=DeepSeekRouter(QueueStore(queue.path),settings(2,4))
    official=[restored.claim('worker',100000,{}) for _ in range(4)]
    assert all(task['channel_key']=='official' for task in official)
    assert restored.claim('worker',100000,{}) is None
    assert all(queue.get(task['id'])['status']=='parse_unknown' for task in school)
