import json
import time
from concurrent.futures import Future

from ingest_queue import QueueStore
from ingest_scheduler import WorkPool
from test_ingest_contracts import spec
from test_ingest_scheduler import FakeGateway


def setup(tmp_path, reader):
    q=QueueStore(tmp_path/'queue.sqlite3')
    first=q.enqueue(spec(source_id='p1',remote_name='p1.md',policy_key='education'),
                    {'status':'submitted','remote_id':'r1'})
    second=q.enqueue(spec(source_id='p2',remote_name='p2.md',policy_key='education'),
                     {'status':'parse_waiting','remote_id':'r2'})
    gateway=FakeGateway()
    gateway.docs={'r1':{'id':'r1','run':'FAIL','progress_msg':'0 tasks are ahead in the queue...'},
                  'r2':{'id':'r2','run':'UNSTART'}}
    pool=WorkPool(q,lambda:gateway,lambda *_:'outcome_unresolved',
                  parse_limit=1,diagnostics_reader=reader)
    return q,first,second,gateway,pool


def test_diagnostic_decision_is_persisted_and_refills_next_paper(tmp_path):
    def reader(task,remote):
        return {'source':'execution_node','component_id':'CodeExec:EducationGate',
                'kind':'quality_decision','outcome':'quality_hold','reason':'正文尚缺实施证据',
                'log_id':'log1','remote_id':task['remote_id'],'pipeline_id':task['pipeline_id'],
                'attempt':task.get('execution_attempt',0)}
    q,first,second,gateway,pool=setup(tmp_path,reader)
    try:
        for _ in range(80):
            pool.tick()
            if q.get(first)['status']=='quality_hold' and gateway.parse_calls:break
            time.sleep(.01)
        task=q.get(first)
        assert task['status']=='quality_hold'
        assert task['error']=='正文尚缺实施证据'
        assert gateway.parse_calls==['p2']
        history=[x for x in q.attempts(first) if x['operation']=='diagnostic']
        assert json.loads(history[0]['result'])['log_id']=='log1'
    finally:pool.close()


def test_missing_log_retries_read_only_and_still_releases_capacity(tmp_path):
    q,first,second,gateway,pool=setup(tmp_path,lambda *_:None)
    try:
        for _ in range(80):
            pool.tick()
            if gateway.parse_calls:break
            time.sleep(.01)
        assert q.get(first)['failure_kind']=='diagnostic_pending'
        assert gateway.parse_calls==['p2']
        # Exhaust only the diagnostic lookups; there is no second parse of p1.
        for _ in range(3):
            task=q.get(first);future=Future();future.set_result(pool._gateway_lookup(task))
            pool._futures[future]=('poll',first);pool._collect()
        assert q.get(first)['failure_kind']=='diagnostic_unavailable'
        assert gateway.parse_calls==['p2']
    finally:pool.close()


def test_ui_displays_diagnostic_reason_instead_of_internal_error_kind():
    from upload_tracking import queued_upload_status
    task={'id':1,'role':'paper_primary','status':'technical_review','remote_id':'r1',
          'dataset_id':'md','failure_kind':'annotation_invalid','error':'原文证据未覆盖：quality'}
    view=queued_upload_status([1],{1:task})
    assert view['reason']=='原文证据未覆盖：quality'
    assert view['process']=='技术失败，待复核'


def test_old_diagnostic_cannot_end_new_parse_attempt(tmp_path):
    q,first,second,gateway,pool=setup(tmp_path,lambda *_:None)
    try:
        q.transition(first,'submitted','outcome_unresolved',failure_kind='diagnostic_pending')
        old=q.get(first)
        future=Future()
        pool._futures[future]=('poll',first)
        q.request_review(first,'retry')
        from test_ingest_routed_queue import route
        new=q.claim_routed_parse('new-worker',time.time(),120,route(),3,3,{})
        assert new['id']==first and new['execution_attempt']==old['execution_attempt']+1
        diagnostic={'source':'execution_node','outcome':'quality_hold','kind':'quality_decision',
                    'reason':'old decision','attempt':old['execution_attempt'],
                    'remote_id':'r1','pipeline_id':old['pipeline_id'],'log_id':'old-log'}
        future.set_result(([gateway.docs['r1']],diagnostic,None))
        pool._collect()
        assert q.get(first)['status']=='parse_intent'
        assert not [a for a in q.attempts(first) if a['operation']=='diagnostic']
    finally:pool.close()


def test_poll_snapshot_is_discarded_if_review_changes_state(tmp_path):
    q,first,second,gateway,pool=setup(tmp_path,lambda *_:None)
    try:
        q.transition(first,'submitted','outcome_unresolved',failure_kind='diagnostic_pending')
        captured=q.get(first);future=Future()
        pool._futures[future]=('poll',first);pool._future_tasks[future]=captured
        q.request_review(first,'manual review while diagnostics pending')
        future.set_result(([gateway.docs['r1']],None,None))
        pool._collect()
        assert q.get(first)['status']=='parse_waiting'
        assert not [a for a in q.attempts(first) if a['operation']=='diagnostic_unavailable']
    finally:pool.close()


def test_diagnostic_transition_uses_atomic_version_guard(tmp_path):
    q,first,second,gateway,pool=setup(tmp_path,lambda *_:None)
    try:
        original=q.get(first)
        q.transition(first,'submitted','submitted',error='newer state')
        assert not q.transition(first,'submitted','quality_hold',expected_updated_at=original['updated_at'])
        assert q.get(first)['status']=='submitted'
    finally:pool.close()
