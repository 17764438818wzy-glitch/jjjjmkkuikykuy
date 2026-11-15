import copy
import json
import time

import pytest

from ingest_execution_diagnostics import IngestionLogReader
from test_ingest_execution_diagnostics import record


ERROR="02:17:34 [ERROR][Exception]: (1213, 'Deadlock found when trying to get lock; try restarting transaction')"


def test_current_database_failure_is_visible_without_a_final_pipeline_log(tmp_path):
    task,remote,_=record(tmp_path)
    remote['progress_msg']=ERROR
    class Client:
        def request(self,*_):raise AssertionError('Confirmed database error needs no log scan')
    diagnostic=IngestionLogReader(Client)(task,remote)
    assert diagnostic['kind']=='database_deadlock'
    assert diagnostic['outcome']=='technical_review'
    assert diagnostic['source']=='remote_execution_status'
    assert diagnostic['dependency']=='database'
    assert diagnostic['attempt']==2 and not diagnostic.get('log_id')


@pytest.mark.parametrize('message,kind',[
    ('Education execution-log writer is busy','database_log_busy'),
    ('Education execution-log connection changed before transaction','database_log_connection_changed'),
])
def test_known_log_persistence_error_is_visible_without_resubmitting_model(tmp_path,message,kind):
    task,remote,_=record(tmp_path)
    remote['progress_msg']='03:30:00 [ERROR][Exception]: '+message
    class Client:
        def request(self,*_):raise AssertionError('Exact bound database error needs no log scan')
    result=IngestionLogReader(Client)(task,remote)
    assert result['kind']==kind
    assert result['dependency']=='database' and result['outcome']=='technical_review'


@pytest.mark.parametrize('case',['old','wrong_name','unbound','body_text','earlier_line','done'])
def test_remote_text_cannot_override_execution_binding_or_create_quality_decisions(tmp_path,case):
    task,remote,_=record(tmp_path);remote['progress_msg']=ERROR
    if case=='old':remote['process_begin_at']='2026-09-25T18:07:00'
    if case=='wrong_name':remote['name']='another.md'
    if case=='unbound':task.pop('parse_baseline')
    if case=='body_text':remote['progress_msg']='正文提到 '+ERROR
    if case=='earlier_line':remote['progress_msg']=ERROR+'\n0 tasks are ahead in the queue...'
    if case=='done':remote['run']='DONE'
    class Client:
        def request(self,*_):return {'logs':[],'total':0}
    assert IngestionLogReader(Client)(task,remote) is None


def test_database_failure_releases_slot_and_persists_source_without_model_fallback(tmp_path):
    from test_ingest_diagnostic_integration import setup
    def reader(task,remote):
        return {'source':'remote_execution_status','dependency':'database','kind':'database_deadlock',
            'outcome':'technical_review','reason':'MySQL 1213 数据库事务死锁',
            'remote_id':task['remote_id'],'pipeline_id':task['pipeline_id'],
            'attempt':task.get('execution_attempt',0)}
    queue,first,second,gateway,pool=setup(tmp_path,reader)
    try:
        for _ in range(80):
            pool.tick()
            if gateway.parse_calls:break
            time.sleep(.01)
        assert queue.get(first)['failure_kind']=='database_deadlock'
        assert queue.get(first)['status']=='technical_review'
        assert gateway.parse_calls==['p2']
        history=[json.loads(a['result']) for a in queue.attempts(first) if a['operation']=='diagnostic']
        assert history[0]['source']=='remote_execution_status'
        bad=copy.deepcopy(history[0]);bad.update(outcome='quality_hold',kind='quality_decision')
        with pytest.raises(ValueError):queue.record_diagnostic(first,bad,'quality_hold')
    finally:pool.close()
