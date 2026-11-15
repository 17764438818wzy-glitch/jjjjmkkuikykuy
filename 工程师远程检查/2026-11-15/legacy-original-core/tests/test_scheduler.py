import json
from pathlib import Path
import sys
import time
from types import SimpleNamespace

import pytest

from paperflow.config import Config
from paperflow.control import Manager
from paperflow.package import sha
from paperflow.store import Store
from test_winjob import wait_until


@pytest.fixture
def scheduler_case(tmp_path):
    from paperflow.scheduler import Scheduler
    config=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),source_roots=[],
        rag_url='http://127.0.0.1:1',min_free_gib=0,max_memory_percent=100)
    config.prepare();Path(config.handoff_root).mkdir()
    m=Manager(Store(tmp_path/'ledger.sqlite3'),tmp_path/'manager').start()
    scheduler=Scheduler(m,config,isolation_root=tmp_path)
    yield m,config,scheduler
    m.shutdown();scheduler.service.flush_clock()


def source(case,tmp_path,name):
    m,c,s=case;p=tmp_path/(name+'.txt');p.write_text(name)
    return m.store.register_source(p,sha(p))


def test_paused_discovery_is_fact_only_and_no_production_scope(tmp_path,scheduler_case):
    from paperflow.scheduler import Scheduler
    m,c,s=scheduler_case;t=source(scheduler_case,tmp_path,'a')
    s.tick();assert len(s.ledger.rows('SELECT * FROM scheduler_jobs'))==1 and not m.executions
    assert not s.ledger.rows('SELECT * FROM scheduler_attempts')
    with pytest.raises(ValueError,match='isolation'):
        Scheduler(m,Config(),isolation_root=tmp_path)


def test_scheduler_claim_launch_binding_and_exit_before_retry(scheduler_case,tmp_path):
    m,c,s=scheduler_case;task=source(scheduler_case,tmp_path,'a')
    def launch(manager,config,task_id,*,schedule_attempt):
        script="from paperflow.control_ipc import WorkerClient;c=WorkerClient.from_environment();c.report_execution('failed',{'fault':{'category':'transient','retry_after':0}});c.close()"
        return manager.launch('graph',task_id,[sys.executable,'-c',script],cwd=Path.cwd(),schedule_attempt=schedule_attempt)
    s.launchers['graph']=launch;m.resume();s.tick()
    job=s.ledger.rows('SELECT * FROM scheduler_jobs')[0];attempt=s.ledger.rows('SELECT * FROM scheduler_attempts')[0]
    ex=m.executions[attempt['execution_id']]
    assert ex.scheduler_job['job_id']==job['job_id'] and s.ledger.job(job['job_id'])['state']=='running'
    wait_until(lambda:ex.exit_saved);s.tick()
    result=s.ledger.job(job['job_id']);assert result['state']=='retry' and result['due']>time.time()
    assert s.ledger.budget(job['job_id'])['rounds']==1
    s.tick();assert len(s.ledger.rows('SELECT * FROM scheduler_attempts'))==1


def test_no_result_crash_is_held_and_next_article_proceeds(scheduler_case,tmp_path):
    m,c,s=scheduler_case;source(scheduler_case,tmp_path,'a');source(scheduler_case,tmp_path,'b')
    m.max_live_units=1
    s.launchers['graph']=lambda m,c,key,**kw:m.launch('graph',key,[sys.executable,'-c','pass'],cwd=Path.cwd(),**kw)
    m.resume();s.tick();first=next(iter(m.executions.values()));wait_until(lambda:first.exit_saved)
    s.tick();wait_until(lambda:all(e.exit_saved for e in m.executions.values()));s.tick()
    assert [r['state'] for r in s.ledger.rows('SELECT * FROM scheduler_jobs')]==['held','held']
    assert len(s.ledger.rows('SELECT * FROM scheduler_attempts'))==2


def test_memory_and_disk_backpressure_do_not_start_units(scheduler_case,tmp_path,monkeypatch):
    import paperflow.scheduler as module
    m,c,s=scheduler_case;source(scheduler_case,tmp_path,'a');m.resume()
    monkeypatch.setattr(module.psutil,'virtual_memory',lambda:SimpleNamespace(percent=101))
    assert 'memory' in s.tick()['pressure']['reasons'] and not m.executions
    monkeypatch.setattr(module.psutil,'virtual_memory',lambda:SimpleNamespace(percent=1))
    c.min_free_gib=1
    monkeypatch.setattr(module.shutil,'disk_usage',lambda path:SimpleNamespace(free=0))
    assert 'disk' in s.tick()['pressure']['reasons'] and not m.executions


def test_restart_retains_rounds_and_reaps_unlaunched_claim(scheduler_case,tmp_path):
    from paperflow.scheduler import Scheduler
    m,c,s=scheduler_case;t=source(scheduler_case,tmp_path,'a');s.discover()
    claim=s.ledger.claim();s.ledger.finish(claim['attempt_id'],'failed',{'fault':{'category':'transient'}},now=time.time()-100)
    claim=s.ledger.claim();assert claim and claim['execution_id'] if 'execution_id' in claim else claim
    m.shutdown()
    new=Manager(m.store,m.root).start()
    try:
        next_=Scheduler(new,c,isolation_root=tmp_path);next_.tick()
        assert new.stopped.is_set() and not new.executions
        job=next_.ledger.job(claim['job_id']);assert job['state']=='retry'
        assert next_.ledger.budget(job['job_id'])['rounds']==1
    finally:new.shutdown()
