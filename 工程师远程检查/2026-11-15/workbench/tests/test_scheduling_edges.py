import json
from pathlib import Path
import sys
import time

import pytest

from paperflow.package import save
from paperflow.scheduler import Scheduler
from test_scheduler import scheduler_case,source
from test_delivery_state import delivery_case
from test_control import manager,start_worker
from test_service_control import service_control,permit
from test_scheduling_store import TRANSIENT
from test_winjob import wait_until


def test_package_pressure_blocks_graph_but_holds_do_not_permanently_block(delivery_case,tmp_path):
    m,c,task,digest,dc,*_=delivery_case;c.min_free_gib=0;c.max_memory_percent=100
    c.max_pending_package_bytes=1
    s=Scheduler(m,c,isolation_root=tmp_path);s.discover()
    pressure=s.pressure();assert pressure['pending_targets']==2 and 'package_bytes' in pressure['reasons']
    assert s.tick()['launched']==0  # No deployment acceptance: both targets are held independently.
    pressure=s.pressure();assert pressure['pending_targets']==0 and pressure['held_targets']==2
    assert pressure['pending_package_bytes']==0 and 'package_bytes' not in pressure['reasons']
    assert pressure['retained_package_bytes']>0  # Disk/retained accounting still includes held material.


def test_paused_worker_without_report_requeues_without_network_round(scheduler_case,tmp_path):
    m,c,s=scheduler_case;task=source(scheduler_case,tmp_path,'a')
    script='from paperflow.control_ipc import WorkerClient;c=WorkerClient.from_environment();c.stopped.wait(30);c.close()'
    s.launchers['graph']=lambda m,c,key,**kw:m.launch('graph',key,[sys.executable,'-c',script],cwd=Path.cwd(),**kw)
    m.resume();s.tick();ex=next(iter(m.executions.values()));wait_until(lambda:ex.peer is not None)
    m.pause();wait_until(lambda:ex.exit_saved);s.tick()
    job=s.ledger.rows('SELECT * FROM scheduler_jobs')[0]
    assert job['state']=='retry' and s.ledger.budget(job['job_id'])['rounds']==0


def test_preflight_configuration_error_is_service_event_not_article_failure(scheduler_case,tmp_path):
    m,c,s=scheduler_case;source(scheduler_case,tmp_path,'a')
    script="from paperflow.control_ipc import WorkerClient;c=WorkerClient.from_environment();c.report_execution('failed',{'fault':{'category':'configuration','status':401,'family':'unavailable'}});c.close()"
    s.launchers['graph']=lambda m,c,key,**kw:m.launch('graph',key,[sys.executable,'-c',script],cwd=Path.cwd(),**kw)
    m.resume();s.tick();ex=next(iter(m.executions.values()));wait_until(lambda:ex.exit_saved);s.tick()
    assert s.service.services['llm']['state']=='open'
    job=s.ledger.rows('SELECT * FROM scheduler_jobs')[0]
    assert job['state']=='blocked_service' and s.ledger.budget(job['job_id'])['rounds']==0


def test_one_probe_and_recovery_do_not_clear_unresolved_write(service_control,tmp_path):
    m,s=service_control
    ex,_=start_worker(m,tmp_path,unit='delivery',name='old-write',action='rag_write')
    ticket=m.request_action(ex.execution_id,'unknown','rag_write',{},0,30)['ticket']
    m.grant_begin(ex.execution_id,ticket,m.check_begin(ex.execution_id,ticket))
    m.record_result(ex.execution_id,ticket,m.epoch,'unknown',{'fault':dict(TRANSIENT,category='configuration')})
    m.cancel_unit(ex.execution_id,'fixture');wait_until(lambda:ex.exit_saved)
    s.open_due(now=time.time()+1000)
    job=s.ledger.enqueue('delivery','new-article','new-target',{},s.services['rag']['service_id'])
    claim=s.ledger.claim(now=time.time()+1001);s.refresh()
    other=s.ledger.enqueue('delivery','another','another-target',{},s.services['rag']['service_id'])
    assert s.ledger.claim(now=time.time()+1001) is None
    probe,_=start_worker(m,tmp_path,unit='delivery',name='probe',action='rag_read')
    probe.scheduler_job=dict(claim,job_id=job)
    probe_ticket=m.request_action(probe.execution_id,'probe','rag_read',{},0,30)['ticket']
    m.grant_begin(probe.execution_id,probe_ticket,m.check_begin(probe.execution_id,probe_ticket))
    m.record_result(probe.execution_id,probe_ticket,m.epoch,'complete',{'response_received':True})
    assert s.services['rag']['state']=='closed' and m.resource_limits['rag']==1
    assert m.ledger.rows('SELECT state FROM control_actions WHERE action_id=?',(ticket,))[0]['state']=='unresolved'


def test_unused_probe_reopens_existing_event(service_control):
    m,s=service_control;sid=s.services['llm']['service_id']
    s.ledger.observe(sid,'config','article',dict(TRANSIENT,category='configuration'))
    s.refresh();event=s.services['llm']['event_id'];s.open_due(now=time.time()+100)
    job=s.ledger.enqueue('graph','a','a',{},sid);s.ledger.claim(now=time.time()+101);s.refresh()
    s.abandon_probe(job)
    assert s.services['llm']['state']=='open' and s.services['llm']['event_id']==event
    assert s.services['llm']['next_probe']>time.time()
