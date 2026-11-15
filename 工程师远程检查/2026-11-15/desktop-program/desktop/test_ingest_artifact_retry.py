import time

from ingest_execution import ArtifactEvidencePending
from ingest_queue import QueueStore
from ingest_scheduler import WorkPool
from test_ingest_contracts import spec
from test_ingest_scheduler import FakeGateway


def make_pool(tmp_path, publish):
    queue=QueueStore(tmp_path/'queue.db')
    key=queue.enqueue(spec(policy_key='education'),
        {'status':'accepted','remote_id':'doc','post_status':'pending'})
    gateway=FakeGateway()
    task=queue.get(key)
    gateway.docs['doc']={'id':'doc','name':task['remote_name'],
        'pipeline_id':task['pipeline_id'],'run':'DONE'}
    return queue,key,WorkPool(queue,lambda:gateway,lambda *_:'accepted',on_accepted_for=publish)


def collect_until(pool, predicate):
    for _ in range(100):
        pool.tick()
        if predicate():return
        time.sleep(.005)
    assert predicate()


def test_missing_final_evidence_retries_reads_after_restart_without_reparse(tmp_path, monkeypatch):
    now=[1000.0]
    monkeypatch.setattr(time,'time',lambda:now[0])
    calls=[]
    def publish(*_):
        calls.append(1)
        if len(calls)==1:raise ArtifactEvidencePending('source proof final log not visible')
    queue,key,pool=make_pool(tmp_path,publish)
    try:
        collect_until(pool,lambda:queue.get(key).get('post_retry_count')==1)
        row=queue.get(key)
        assert row['post_status']=='pending' and row['status']=='accepted'
        assert not queue.post_ready(1)
        assert not queue.list_statuses(('parse_waiting','parse_intent','submitted','running','parse_unknown'))
    finally:pool.close()
    now[0]+=16
    reopened=QueueStore(queue.path)
    gateway=FakeGateway();gateway.docs['doc']={'id':'doc','run':'DONE'}
    resumed=WorkPool(reopened,lambda:gateway,lambda *_:'accepted',on_accepted_for=publish)
    try:
        collect_until(resumed,lambda:reopened.get(key)['post_status']=='done')
        assert len(calls)==2
        assert reopened.artifact_summary()['complete']==1
    finally:resumed.close()


def test_missing_evidence_retry_is_finite_and_does_not_starve_other_artifacts(tmp_path,monkeypatch):
    now=[1000.0]
    monkeypatch.setattr(time,'time',lambda:now[0])
    calls=[]
    def publish(task,*_):
        calls.append(task['id'])
        if task['id']==1:raise ArtifactEvidencePending('source proof final log not visible')
    queue,key,pool=make_pool(tmp_path,publish)
    other=queue.enqueue(spec(policy_key='education',source_id='other',remote_name='other.md'),
        {'status':'accepted','remote_id':'other','post_status':'pending'})
    gateway=pool.gateway_factory()
    gateway.docs['other']={'id':'other','name':'other.md','pipeline_id':queue.get(other)['pipeline_id'],'run':'DONE'}
    try:
        collect_until(pool,lambda:queue.get(key).get('post_retry_count')==1 and queue.get(other)['post_status']=='done')
        for count,delay in ((2,16),(3,46),(4,91)):
            now[0]+=delay
            collect_until(pool,lambda:queue.get(key).get('post_retry_count')==count)
        assert queue.get(key)['post_status']=='needs_review'
        assert queue.get(key)['failure_kind']=='artifact_evidence_unavailable'
        assert calls.count(key)==4 and calls.count(other)==1
        now[0]+=1000
        for _ in range(5):pool.tick()
        assert calls.count(key)==4
    finally:pool.close()
