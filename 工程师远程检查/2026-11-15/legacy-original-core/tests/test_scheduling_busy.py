"""Temporary ownership defers a queued unit without weakening exclusivity."""
from pathlib import Path
import json
import sys

import pytest

from paperflow.package import save
from paperflow.scheduler import Scheduler
from paperflow.delivery_store import target_key
from test_scheduler import scheduler_case, source
from test_control import manager, WORKER
from test_delivery_state import delivery_case, owner
from test_winjob import wait_until


RESUMED = '''
import sys,json
from pathlib import Path
from paperflow.control_ipc import WorkerClient
client=WorkerClient.from_environment()
try:
    resource=json.loads(sys.argv[2])
    client.perform('local',resource,lambda cancel:Path(sys.argv[1]).write_text('resumed'),
        total_timeout=10,expected_generation=resource.get('identity',{}).get('generation',0))
    client.report_execution('held',{'reason':'synthetic work ended without network'})
finally:client.close()
'''


def resume_with_local_job(manager,kind,key,marker,attempt,resource=None):
    return manager.launch(kind,key,[sys.executable,'-B','-c',RESUMED,str(marker),json.dumps(resource or {})],
                          cwd=Path.cwd(),schedule_attempt=attempt)


def test_busy_explicit_graph_resumes_after_confirmed_owner_exit(scheduler_case,tmp_path):
    m,c,s=scheduler_case;task=source(scheduler_case,tmp_path,'owned-source');m.resume()
    work=tmp_path/'explicit-graph'
    ex=m.launch('graph',task['task_id'],[sys.executable,'-B','-c',WORKER,str(work),'local'],cwd=Path.cwd())
    wait_until(lambda:(work/'ready').exists())
    s.tick();job=s.ledger.rows('SELECT * FROM scheduler_jobs')[0]
    assert job['state']=='retry'
    assert len(m.executions)==1 and s.ledger.budget(job['job_id'])['rounds']==0
    m.cancel_unit(ex.execution_id,'release explicit owner');wait_until(lambda:ex.exit_saved)
    marker=tmp_path/'resumed'
    s.launchers['graph']=lambda manager,config,key,*,schedule_attempt:resume_with_local_job(manager,'graph',key,marker,schedule_attempt)
    wait_until(lambda:(s.tick(),marker.exists())[1],seconds=6)
    wait_until(lambda:all(e.exit_saved for e in m.executions.values()))
    assert len(s.ledger.rows('SELECT * FROM scheduler_attempts WHERE execution_id IS NOT NULL'))==1
    assert s.ledger.budget(job['job_id'])['rounds']==0


def test_busy_explicit_specialty_resumes_after_confirmed_owner_exit(delivery_case,tmp_path):
    m,c,task,digest,commits,*_=delivery_case
    c.min_free_gib=0;c.max_memory_percent=100
    c.delivery_acceptance_report=str(tmp_path/'acceptance.json')
    save(c.delivery_acceptance_report,dict(passed=True,endpoint=c.rag_url))
    s=Scheduler(m,c,isolation_root=tmp_path);s.discover()
    wanted=next(row for row in s.ledger.rows('SELECT * FROM scheduler_jobs') if '120201' in row['data'])
    for row in s.ledger.rows('SELECT * FROM scheduler_jobs'):
        if row['job_id']!=wanted['job_id']:s.hold(row['job_id'],'exclude unrelated fixture')
    ex,identity=owner(delivery_case,tmp_path,'120201')
    s.tick();job=s.ledger.job(wanted['job_id'])
    assert job['state']=='retry'
    assert len(m.executions)==1 and s.ledger.budget(job['job_id'])['rounds']==0
    m.cancel_unit(ex.execution_id,'release explicit owner');wait_until(lambda:ex.exit_saved)
    marker=tmp_path/'resumed'
    s.launchers['delivery']=lambda manager,config,digest,code,*,schedule_attempt:resume_with_local_job(
        manager,'delivery',target_key(identity),marker,schedule_attempt,dict(identity=identity,operation='load'))
    wait_until(lambda:(s.tick(),marker.exists())[1],seconds=6)
    wait_until(lambda:all(e.exit_saved for e in m.executions.values()))
    assert len(s.ledger.rows('SELECT * FROM scheduler_attempts WHERE execution_id IS NOT NULL'))==1
    assert s.ledger.budget(job['job_id'])['rounds']==0


def test_nonownership_validation_error_still_holds(scheduler_case,tmp_path):
    m,c,s=scheduler_case;source(scheduler_case,tmp_path,'a');m.resume()
    def invalid(*a,**kw):raise ValueError('invalid source identity')
    s.launchers['graph']=invalid;s.tick()
    job=s.ledger.rows('SELECT * FROM scheduler_jobs')[0]
    assert job['state']=='held' and s.ledger.budget(job['job_id'])['rounds']==0


@pytest.mark.parametrize('owner_state',['sql_only','publication'])
def test_nonlive_owner_evidence_still_defers_until_released(scheduler_case,tmp_path,owner_state):
    m,c,s=scheduler_case;task=source(scheduler_case,tmp_path,'pending-owner');m.resume()
    if owner_state=='sql_only':
        # Simulate a durable prior owner not present in the in-memory execution map.
        with m.store.db() as db:
            db.execute("INSERT INTO control_executions(execution_id,epoch,unit_kind,unit_key,job_name,state,lease_until,last_heartbeat,deadline) VALUES('old','prior','graph',?,'fixture','active',0,0,0)",
                (task['task_id'],))
    else:
        work=tmp_path/'publishing'
        ex=m.launch('graph',task['task_id'],[sys.executable,'-B','-c',WORKER,str(work),'local'],cwd=Path.cwd())
        wait_until(lambda:(work/'ready').exists())
        with m.gate_lock:m.publications.add(ex.execution_id)
        m.cancel_unit(ex.execution_id,'finished worker with outstanding commit');wait_until(lambda:ex.exit_saved)
    try:
        s.tick();job=s.ledger.rows('SELECT * FROM scheduler_jobs')[0]
        assert job['state']=='retry' and s.ledger.budget(job['job_id'])['rounds']==0
        assert not s.ledger.rows('SELECT * FROM scheduler_attempts WHERE execution_id IS NOT NULL')
    finally:
        if owner_state=='sql_only':
            with m.store.db() as db:db.execute("UPDATE control_executions SET state='exited',exit_evidence='{}' WHERE execution_id='old'")
        else:
            with m.gate_lock:m.publications.discard(ex.execution_id)
    marker=tmp_path/'resumed'
    s.launchers['graph']=lambda manager,config,key,*,schedule_attempt:resume_with_local_job(manager,'graph',key,marker,schedule_attempt)
    wait_until(lambda:(s.tick(),marker.exists())[1],seconds=6)
    wait_until(lambda:all(e.exit_saved for e in m.executions.values()))
    assert s.ledger.budget(job['job_id'])['rounds']==0
