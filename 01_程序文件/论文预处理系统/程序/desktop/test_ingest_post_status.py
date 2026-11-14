import time

from ingest_queue import QueueStore
from ingest_scheduler import WorkPool
from test_ingest_contracts import spec
from test_ingest_scheduler import FakeGateway


def test_remote_failure_withdraws_local_acceptance_and_is_diagnosed(tmp_path):
    q = QueueStore(tmp_path / 'queue.db')
    key = q.enqueue(spec(policy_key='education'),
        {'status':'accepted','remote_id':'doc','post_status':'pending'})
    task=q.get(key)
    g=FakeGateway()
    g.docs['doc']={'id':'doc','name':task['remote_name'],
        'pipeline_id':task['pipeline_id'],'run':'FAIL'}
    def diagnostic(task, remote):
        return {'source':'execution_node','attempt':0,'remote_id':'doc',
            'pipeline_id':task['pipeline_id'],'kind':'annotation_invalid',
            'reason':'必填字段缺失','outcome':'technical_review'}
    with_pool=WorkPool(q,lambda:g,lambda *_:'technical_review',
        on_accepted_for=lambda *_:None,diagnostics_reader=diagnostic)
    try:
        for _ in range(40):
            with_pool.tick()
            if q.get(key)['status']=='technical_review':break
            time.sleep(.01)
        row=q.get(key)
        assert row['status']=='technical_review'
        assert row['failure_kind']=='annotation_invalid'
        assert row['error']=='必填字段缺失'
        assert row['post_status'] is None
    finally:with_pool.close()


def test_local_artifact_failure_preserves_admission_but_not_complete_count(tmp_path):
    q=QueueStore(tmp_path/'queue.db')
    key=q.enqueue(spec(),{'status':'accepted','remote_id':'doc','post_status':'pending'})
    task=q.get(key);g=FakeGateway()
    g.docs['doc']={'id':'doc','name':task['remote_name'],'pipeline_id':task['pipeline_id'],'run':'DONE'}
    def fail(*_):raise ValueError('Education artifact source hash mismatch')
    pool=WorkPool(q,lambda:g,lambda *_:'accepted',on_accepted_for=fail)
    try:
        for _ in range(40):
            snapshot=pool.tick()
            if q.get(key)['post_status']=='needs_review':break
            time.sleep(.01)
        row=q.get(key)
        assert row['status']=='accepted' and row['post_status']=='needs_review'
        assert 'hash mismatch' in row['error']
        assert snapshot['artifacts']['complete']==0
        assert snapshot['artifacts']['needs_review']==1
    finally:pool.close()


def test_workbench_does_not_label_unfinished_education_artifacts_complete():
    from upload_tracking import queued_upload_status
    task={'id':1,'role':'paper_primary','policy_key':'education','status':'accepted',
          'remote_id':'doc','dataset_id':'edu','post_status':'needs_review',
          'error':'来源校验未通过'}
    result=queued_upload_status([1],{1:task})
    assert result['process']=='入库，产物待复核'
    assert result['reason']=='来源校验未通过'
