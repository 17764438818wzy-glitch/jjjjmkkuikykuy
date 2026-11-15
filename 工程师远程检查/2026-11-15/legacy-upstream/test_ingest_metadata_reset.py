import copy

import pytest

from ingest_gateway import RagflowGateway


def client_and_task():
    task={'id':1,'dataset_id':'edu','remote_id':'doc','remote_name':'paper.md',
        'pipeline_id':'pipeline','policy_key':'education','role':'paper_primary','source_id':'paper_one'}
    class Client:
        def __init__(self):
            self.doc={'id':'doc','name':'paper.md','pipeline_id':'pipeline','run':'DONE',
                'process_begin_at':'2026-09-26T12:00:00','meta_fields':{
                    'education_paper_id':'paper_one','education_method_details':'old methods',
                    'artifact_profile':'old profile','user_note':'keep'}}
            self.parses=0
        def documents(self,*_,**kw):return [copy.deepcopy(self.doc)]
        def request(self,method,path,payload):
            self.doc['meta_fields']=copy.deepcopy(payload['meta_fields'])
        def parse(self,*_):self.parses+=1
    return Client(),task


def test_education_reparse_removes_stale_generated_fields_and_preserves_user_metadata():
    client,task=client_and_task();baselines=[]
    gateway=RagflowGateway(lambda:client,baseline_recorder=lambda t,r:baselines.append(r))
    gateway.parse(task)
    assert client.doc['meta_fields']=={'user_note':'keep'}
    assert baselines[0]['meta_fields']=={'user_note':'keep'}
    assert client.parses==1


def test_finance_parse_does_not_touch_metadata():
    client,task=client_and_task();task['policy_key']='finance'
    gateway=RagflowGateway(lambda:client);gateway.parse(task)
    assert client.doc['meta_fields']['education_method_details']=='old methods'


def test_failed_metadata_reset_never_submits_parse_and_is_not_an_unknown_parse_write():
    from ingest_gateway import ParsePreparationFailed
    client,task=client_and_task()
    def fail(*_):raise TimeoutError('reset response lost')
    client.request=fail
    with pytest.raises(ParsePreparationFailed):RagflowGateway(lambda:client).parse(task)
    assert client.parses==0


def test_local_artifact_invalidation_is_inside_preflight_boundary():
    from ingest_gateway import ParsePreparationFailed
    client,task=client_and_task()
    def invalidation(_):raise OSError('Local artifact pointer could not be withdrawn')
    with pytest.raises(ParsePreparationFailed):
        RagflowGateway(lambda:client,before_parse=invalidation).parse(task)
    assert client.parses==0


def test_preflight_failure_releases_capacity_without_uncertain_submission(tmp_path):
    import time
    from ingest_queue import QueueStore
    from ingest_scheduler import WorkPool
    from test_ingest_contracts import spec
    client,task=client_and_task();client.base_url='http://localhost:9380'
    def fail(*_):raise TimeoutError()
    client.request=fail
    q=QueueStore(tmp_path/'queue.db')
    key=q.enqueue(spec(source_id='paper_one',remote_name='paper.md',pipeline_id='pipeline',
        policy_key='education'),{'status':'parse_waiting','remote_id':'doc'})
    pool=WorkPool(q,lambda:RagflowGateway(lambda:client,baseline_recorder=q.record_parse_baseline),lambda *_:'accepted')
    try:
        for _ in range(30):
            pool.tick()
            if q.get(key)['status']=='technical_review':break
            time.sleep(.01)
        assert q.get(key)['failure_kind']=='parse_preparation_failed'
        assert client.parses==0
        assert not q.list_statuses(('parse_unknown','parse_intent','running','submitted'))
    finally:pool.close()
