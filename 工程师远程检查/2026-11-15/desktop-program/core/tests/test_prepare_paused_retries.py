import json
from pathlib import Path

import pytest

from paperflow.control import Manager
from paperflow.package import load,sha
from paperflow.repair_store import RepairLedger
from paperflow.scheduling_store import SchedulingLedger
from scripts.prepare_paused_retries import prepare
from test_processing_graph import graph_case,run_worker,model_calls


def selection(case,job_state='held'):
    config,store,task,manager,_=case
    ledger=SchedulingLedger(store)
    service=ledger.ensure_service('llm',dict(test='no external calls'))
    job=ledger.enqueue('graph',task['source_id'],task['task_id'],dict(task_id=task['task_id']),service['service_id'])
    with store.db() as db:
        db.execute('CREATE TABLE web_batches(id TEXT PRIMARY KEY,request_id TEXT,selection TEXT,task_ids TEXT,created REAL)')
        db.execute('INSERT INTO web_batches VALUES(?,?,?,?,?)',('selected','request','[]',json.dumps([task['task_id']]),1))
        db.execute('UPDATE scheduler_jobs SET state=? WHERE job_id=?',(job_state,job))
    return job


def test_offline_classification_retry_retains_round1_budget_and_reusable_new_round2(graph_case,tmp_path):
    config,store,task,manager,path=graph_case
    run_worker(graph_case,'no-specialty')
    job=selection(graph_case)
    repairs=RepairLedger(store)
    with store.db() as db:
        repairs.reserve_sql(db,task,'existing-correction','syntax',{'stage':'round1'})
    round1=next((config.root/'work'/task['task_id']/'phase-cache'/'round1').glob('*.json'))
    old_round2=next((config.root/'work'/task['task_id']/'phase-cache'/'round2').glob('*.json'))
    output=Path(load(old_round2)['result']['draft_dir'])
    old_files={str(p):sha(p) for p in output.rglob('*') if p.is_file()}
    first_hash=sha(round1)
    manager.shutdown()
    result=prepare(config,'selected',[],tmp_path/'recovery')
    assert result['retry_jobs']==1 and result['classification_retry']==1
    assert result['paused'] and not result['business_work_started']
    assert model_calls(config)==['round1','round2']
    assert store.get(task['task_id'])['stage']=='round2'
    assert store.get(task['task_id'])['hold_reason'] is None
    assert sha(round1)==first_hash
    assert all(sha(name)==digest for name,digest in old_files.items())
    assert load(old_round2)['complete'] is False
    with store.db() as db:
        assert db.execute('SELECT state FROM scheduler_jobs WHERE job_id=?',(job,)).fetchone()[0]=='retry'
        assert db.execute('SELECT paused FROM controller_state').fetchone()[0]==1
        assert db.execute('SELECT COUNT(*) FROM defective_cache').fetchone()[0]==0
    assert repairs.used(task)==1

    # Synthetic model only: repair classification, crash after the next phase,
    # resume again. The new round2 marker must be reusable at the SAME cache key.
    from paperflow.processing_control import ProcessingCommits
    new=Manager(store,tmp_path/'new-manager',heartbeat_interval=.2,lease_seconds=10).start()
    try:
        ProcessingCommits(new,config);new.resume()
        case=(config,store,task,new,path)
        run_worker(case,'after-cache-round3_assets');run_worker(case)
        assert model_calls(config)==['round1','round2','round2','round3_assets','round3_redact','round3_chunk']
        assert load(old_round2)['complete'] is True
        assert repairs.used(task)==1
    finally:new.shutdown()


def test_offline_retry_keeps_completed_task_and_job_byte_for_byte(graph_case,tmp_path):
    config,store,task,manager,_=graph_case
    run_worker(graph_case)
    job=selection(graph_case,'complete')
    with store.db() as db:
        before_task=tuple(db.execute('SELECT * FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone())
        before_job=tuple(db.execute('SELECT * FROM scheduler_jobs WHERE job_id=?',(job,)).fetchone())
    manager.shutdown()
    result=prepare(config,'selected',[task['task_id']],tmp_path/'recovery')
    assert result['retry_jobs']==0 and result['protected_completed']==1
    with store.db() as db:
        assert tuple(db.execute('SELECT * FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone())==before_task
        assert tuple(db.execute('SELECT * FROM scheduler_jobs WHERE job_id=?',(job,)).fetchone())==before_job
    assert len(model_calls(config))==5


def test_offline_recovery_refuses_live_manager_even_if_operator_asks_to_retry(graph_case,tmp_path):
    config,store,task,manager,_=graph_case
    job=selection(graph_case)
    with pytest.raises(RuntimeError,match='running'):
        prepare(config,'selected',[],tmp_path/'recovery')
    with store.db() as db:
        assert db.execute('SELECT state FROM scheduler_jobs WHERE job_id=?',(job,)).fetchone()[0]=='held'
