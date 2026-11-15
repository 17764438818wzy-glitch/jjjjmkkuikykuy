import json
import time

from paperflow.config import Config
from paperflow.package import save,load
from paperflow.remote_harness import RemoteHarness
from paperflow.store import Store
import threading
from paperflow.workbench_view import remote_processes


def test_remote_process_view_is_bounded_to_selected_articles_and_hides_secrets(tmp_path):
    config=Config(home=str(tmp_path),remote_harness={'host':'server'})
    now=time.time();path=tmp_path/'private/remote-current/one.json'
    save(path,dict(task_id='one',job_id='nonce',host='server',remote_pid=123,container='owned',state='running',
        last_remote_heartbeat=now,started_at=now-2,exit_confirmed=False,key_file='must-not-expose',env={'KEY':'secret'}))
    status=tmp_path/'private/remote-executions/nonce/status.json';save(status,load(path))
    rows=remote_processes(config,['one','two'])
    assert len(rows)==1 and rows[0]['pid']==123 and rows[0]['location']=='server'
    assert 'secret' not in json.dumps(rows) and 'must-not-expose' not in json.dumps(rows)
    assert remote_processes(config,['other'])==[]
    value=load(status);value['last_remote_heartbeat']=now-5;save(status,value)
    assert remote_processes(config,['one'])[0]['state']=='unknown'


def test_old_stage_cannot_overwrite_new_stage_process_pointer(tmp_path):
    config=Config(home=str(tmp_path),remote_harness={'host':'server'})
    current=tmp_path/'private/remote-current/one.json';save(current,dict(task_id='one',job_id='new'))
    old=object.__new__(RemoteHarness);old.config=config;old.record_lock=threading.Lock()
    old.record=dict(task_id='one',job_id='old');old.path=tmp_path/'private/remote-executions/old/status.json'
    old._record(state='exited')
    assert load(current)['job_id']=='new'


def test_exit_event_is_set_only_after_exit_record_saved():
    h=object.__new__(RemoteHarness);h.settings={'image':'image'};h.job_id='job';h.exited=threading.Event()
    observed=[]
    h._record=lambda **_values:observed.append(h.exited.is_set())
    h._accept_proof(dict(all_exited=True,job_closed=True,job_id='job',image='image'))
    assert observed==[False] and h.exited.is_set()


def test_manager_reap_proof_replaces_stale_killed_worker_status(tmp_path):
    config=Config(home=str(tmp_path),remote_harness={'host':'server'});now=time.time()
    value=dict(task_id='one',job_id='job',execution_id='execution',epoch='old',host='server',state='running',
        last_remote_heartbeat=now-30,started_at=now-40,exit_confirmed=False)
    save(tmp_path/'private/remote-current/one.json',value)
    save(tmp_path/'private/remote-executions/job/status.json',value)
    store=Store(tmp_path/'ledger.sqlite3')
    proof=dict(execution_closed=True,all_exited=True,at=now-10)
    with store.db() as db:
        db.execute('CREATE TABLE remote_harness_executions (execution_id TEXT,epoch TEXT,state TEXT,proof TEXT)')
        db.execute('INSERT INTO remote_harness_executions VALUES(?,?,?,?)',('execution','old','exited',json.dumps(proof)))
    row=remote_processes(config,['one'],store)[0]
    assert row['state']=='exited' and row['exit_confirmed'] and 29<row['elapsed_seconds']<31
