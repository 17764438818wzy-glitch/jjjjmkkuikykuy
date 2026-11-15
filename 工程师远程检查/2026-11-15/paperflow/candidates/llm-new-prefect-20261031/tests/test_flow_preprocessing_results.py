"""A later LLM phase must not erase a completed preprocessing result."""
from contextlib import closing
import hashlib
import json
from pathlib import Path
from uuid import uuid4

from prefect.states import Failed, Paused, Running
import pytest

from test_real_folder_no_placeholder import FakePrefect, FakeQueue, HERE, module

orders = module('flow_preprocessing_results_orders', HERE / 'flow_prefect_orders.py')


@pytest.mark.parametrize('state', [Running(), Paused(name='Suspended',reschedule=True), Failed()])
def test_completed_handoff_remains_preprocessing_passed(tmp_path, monkeypatch, state):
    task_id, group_id, receipt_id = str(uuid4()), uuid4().hex, 'a' * 64
    client, queue = FakePrefect(task_id), FakeQueue(tmp_path / 'local.db')
    params = {'task_id':task_id,'group_id':group_id,'receipt_id':receipt_id,'attempt_id':'attempt-1'}
    run = client.create_flow_run_from_deployment(client.folder_deployment.id, parameters=params,
        idempotency_key='folder-waiting-v2-' + task_id + '-' + group_id,
        work_queue_name='task-' + task_id, name='资料文件夹 甲 · LLM 处理中', state=state)
    manifest = {'schema':'paperflow-local-preprocessing-handoff/1', **params,
                'paper_id':'paper-1','consumer_id':'prefect-local-preprocessing','gaps':[]}
    manifest_path = tmp_path / 'handoff.json'
    raw = json.dumps(manifest).encode()
    manifest_path.write_bytes(raw)
    ref = json.dumps({'manifest_path':str(manifest_path),'manifest_sha256':hashlib.sha256(raw).hexdigest()})
    with closing(queue._connect()) as db, db:
        db.executescript('''
            CREATE TABLE metadata(key TEXT,value TEXT);
            CREATE TABLE groups(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,state TEXT,
                review_reason TEXT,checked_session TEXT);
            CREATE TABLE retention_requests(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,
                consumer_id TEXT,state TEXT,retention_id TEXT,last_error TEXT,checked_session TEXT);
            CREATE TABLE claims(task_id TEXT,group_id TEXT,receipt_id TEXT,consumer_id TEXT,attempt_id TEXT,state TEXT);
            CREATE TABLE preprocessing_failures(consumer_id TEXT,attempt_id TEXT,error_type TEXT);
            CREATE TABLE preprocessing_handoffs(receipt_id TEXT,task_id TEXT,group_id TEXT,paper_id TEXT,
                consumer_id TEXT,attempt_id TEXT,state TEXT,handoff TEXT);
        ''')
        db.executemany('INSERT INTO metadata VALUES(?,?)', [('receiver_session_id','session'),
                       ('receiver_status','running'),('scan_source_instance','source')])
        db.execute('INSERT INTO groups VALUES(?,?,?,?,?,?,?)',
                   (task_id,group_id,'paper-1',receipt_id,'ready_for_llm',None,'session'))
        db.execute('INSERT INTO retention_requests VALUES(?,?,?,?,?,?,?,?,?)',
                   (task_id,group_id,'paper-1',receipt_id,'test','accepted','kept',None,'session'))
        db.execute('INSERT INTO claims VALUES(?,?,?,?,?,?)',
                   (task_id,group_id,receipt_id,'prefect-local-preprocessing','attempt-1','completed'))
        db.execute('INSERT INTO preprocessing_handoffs VALUES(?,?,?,?,?,?,?,?)',
                   (receipt_id,task_id,group_id,'paper-1','prefect-local-preprocessing','attempt-1','waiting',ref))
        db.execute('INSERT INTO preprocessing_folder_orders VALUES(?,?,?,?,?,?,?)',
                   (task_id,group_id,receipt_id,'attempt-1','甲','now',str(run.id)))
    service = orders.FolderOrderService(queue, prefect_api_url='http://new-prefect/api',
        first_step_index=tmp_path / 'control', first_step_data_root=tmp_path, first_step_receipt_root=tmp_path)
    monkeypatch.setattr(service, '_client', lambda: client)
    snapshot = {'receiver_session_id':'session','scan_source_instance':'source',
                'upstream_scan_verified':True,'waiting_llm_total':1}
    tasks = {task_id:{'prefect_run_id':str(client.root.id),'name':'合成任务'}}
    result = service._build_results(snapshot,tasks)
    assert len(result['buckets']['passed']) == 1
    assert result['buckets']['failed'] == []
