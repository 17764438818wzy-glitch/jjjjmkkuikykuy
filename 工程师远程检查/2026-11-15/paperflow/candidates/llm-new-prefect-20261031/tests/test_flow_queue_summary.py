"""A processed folder must not remain in the LLM waiting count."""
from contextlib import closing
import importlib.util
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace
from uuid import UUID, uuid4

from fastapi.testclient import TestClient
from prefect.states import Running
import pytest

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
from flow_queue_summary import summarize_llm

spec = importlib.util.spec_from_file_location('flow_waiting_api', HERE / 'flow_waiting_api.py')
waiting_api = importlib.util.module_from_spec(spec)
spec.loader.exec_module(waiting_api)


@pytest.fixture
def observed(tmp_path):
    task_id, parent_id = str(uuid4()), str(uuid4())
    items = [{'task_id':task_id, 'group_id':uuid4().hex, 'paper_id':f'paper-{n}', 'receipt_id':str(n)*64}
             for n in (1,2,3)]
    folder_ids = [str(uuid4()) for _ in items]
    path = tmp_path / 'control.db'
    with closing(sqlite3.connect(path)) as db, db:
        db.executescript('''
            CREATE TABLE tasks(task_id TEXT,prefect_run_id TEXT,prefect_order_model TEXT);
            CREATE TABLE task_deletions(task_id TEXT);
            CREATE TABLE llm_folder_queue(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,
                attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,state TEXT);
            CREATE TABLE llm_request_admissions(task_id TEXT,task_run_id TEXT,kind TEXT,state TEXT,receipt_id TEXT);
        ''')
        db.execute('INSERT INTO tasks VALUES(?,?,?)', (task_id,parent_id,'v2'))
        for item, folder_id, state in zip(items[1:],folder_ids[1:],('llm_processing','waiting_postprocessing')):
            db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?)',
                       tuple(item[k] for k in ('task_id','group_id','paper_id','receipt_id')) +
                       ('attempt',parent_id,folder_id,state))
        db.execute('INSERT INTO llm_request_admissions VALUES(?,?,?,?,?)',
                   (task_id,folder_ids[1],'body','admitted',items[1]['receipt_id']))
        db.execute('INSERT INTO llm_request_admissions VALUES(?,?,?,?,?)',
                   (task_id,folder_ids[1],'attachment','unknown',items[1]['receipt_id']))
    snapshot = {'receiver_status':'running','waiting_available':True,'as_of':'2026-10-31T00:00:00Z',
                'in_progress_count':0}
    class Queue:
        def list_waiting_llm(self): return {'items':items,'unavailable_count':0,'waiting_count':3}
        def snapshot(self, **_): return dict(snapshot)
    deployment = SimpleNamespace(id=uuid4())
    run = SimpleNamespace(id=UUID(folder_ids[1]), deployment_id=deployment.id, parent_task_run_id=None,
        state=Running(), idempotency_key='folder-waiting-v2-' + task_id + '-' + items[1]['group_id'],
        work_queue_name='task-' + task_id,
        parameters={'task_id':task_id,'group_id':items[1]['group_id'],
                    'receipt_id':items[1]['receipt_id'],'attempt_id':'attempt'})
    class Observer:
        index = path
        def _client(self): return self
        def __enter__(self): return self
        def __exit__(self,*_): return False
        def _safe_deployment(self, _): return deployment
        def read_flow_runs(self, **_): return [run]
        def summary(self, _):
            return {'source':'prefect','scope':'all_tasks','available':True,'counts':{}}
        def preprocessing_results(self,*_):
            return {'available':True,'counts':{'passed':10},'as_of':snapshot['as_of'],'partial':False}
    return Queue(), path, Observer(), snapshot, items, run


def test_uses_folder_stage_not_cumulative_preprocessing_passes(observed):
    queue, path, observer, snapshot, items, _ = observed
    result = summarize_llm(queue, path, observer, snapshot)
    assert result['waiting_llm'] == 1 and result['waiting_items'] == items[:1]
    assert result['llm_body_in_progress'] == 1
    assert result['llm_attachment_in_progress'] is None
    assert result['waiting_postprocessing'] == 1


def test_unknown_or_nonrunning_worker_does_not_look_like_zero(observed):
    queue, path, observer, snapshot, _, run = observed
    run.state = None
    assert summarize_llm(queue, path, observer, snapshot)['llm_body_in_progress'] is None


def test_8795_summary_and_waiting_endpoint_use_same_folder_count(observed):
    queue, _, observer, _, items, _ = observed
    with TestClient(waiting_api.create_app(queue,ticket_observer=observer)) as client:
        summary = client.get('/api/v1/local-intake/preprocessing/summary').json()
        waiting = client.get('/api/v1/local-intake/llm/waiting').json()
    assert summary['prefect_ticket_summary']['counts']['waiting_llm'] == 1
    assert summary['preprocessing_results']['counts']['passed'] == 10
    assert waiting['waiting_count'] == 1 and waiting['items'] == items[:1]


def test_changed_retention_never_masquerades_as_empty_queue(observed):
    queue, path, observer, snapshot, _, _ = observed
    queue.list_waiting_llm = lambda: {'items':[], 'unavailable_count':3, 'waiting_count':0}
    with pytest.raises(ValueError, match='交接'):
        summarize_llm(queue, path, observer, snapshot)
