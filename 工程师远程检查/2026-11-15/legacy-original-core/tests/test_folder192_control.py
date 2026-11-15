"""Actual Windows Job/pipe acceptance, with synthetic work and no model or RAG."""
import json
from pathlib import Path
import sqlite3
import sys
import threading
import time

import psutil
import pytest

from paperflow.control import Manager
from paperflow.package import save,sha
from paperflow.scheduler import Scheduler
from paperflow.scheduling_policy import ActionDeferred
from paperflow.store import Store
from test_managed_entry import entry_case
from test_scheduling_resources import worker,WORKER
from test_winjob import wait_until


def test_resource_wait_keeps_one_process_one_prepared_ticket_and_pause_wins(tmp_path):
    m=Manager(Store(tmp_path/'state.sqlite3'),tmp_path/'manager',resource_limits={'llm':1,'local':1,'rag':1}).start()
    m.wait_resources=True;m.resume()
    try:
        first,a=worker(m,tmp_path,'owner','local');second,b=worker(m,tmp_path,'waiting','local')
        (a/'go').touch();wait_until(lambda:(a/'performed').exists())
        (b/'go').touch();time.sleep(2.6)
        assert not second.exit_saved and not (b/'performed').exists() and not (b/'denied').exists()
        prepared=m.ledger.rows("SELECT action_id FROM control_actions WHERE execution_id=? AND state='prepared'",(second.execution_id,))
        assert len(prepared)==1
        lock=sqlite3.connect(m.store.path);lock.execute('BEGIN IMMEDIATE')
        try:
            start=time.monotonic();m.pause('blocked database pause')
            assert time.monotonic()-start<.5 and not m.status()['admission_open']
            assert not (b/'performed').exists()
        finally:lock.rollback();lock.close()
        wait_until(lambda:first.exit_saved and second.exit_saved,seconds=12)
        assert not (b/'performed').exists()
        assert not m.ledger.rows("SELECT 1 FROM control_actions WHERE execution_id=? AND state IN ('complete','possible')",(second.execution_id,))
    finally:m.shutdown()


def test_resource_wait_reuses_ticket_after_capacity_returns(tmp_path):
    m=Manager(Store(tmp_path/'state.sqlite3'),tmp_path/'manager',resource_limits={'llm':1,'local':1,'rag':1}).start()
    m.wait_resources=True;m.resume()
    try:
        first,a=worker(m,tmp_path,'a','local');second,b=worker(m,tmp_path,'b','local')
        (a/'go').touch();wait_until(lambda:(a/'performed').exists());(b/'go').touch()
        time.sleep(2.4);assert not second.exit_saved and not (b/'performed').exists()
        (a/'release').touch();wait_until(lambda:(b/'performed').exists())
        (b/'release').touch();wait_until(lambda:first.exit_saved and second.exit_saved)
        rows=m.ledger.rows('SELECT * FROM control_actions WHERE execution_id=?',(second.execution_id,))
        assert len(rows)==1 and rows[0]['state']=='complete'
    finally:m.shutdown()


def test_wait_never_extends_original_action_deadline(tmp_path):
    m=Manager(Store(tmp_path/'state.sqlite3'),tmp_path/'manager',resource_limits={'llm':1,'local':1,'rag':1}).start()
    m.wait_resources=True;m.resume()
    try:
        first,a=worker(m,tmp_path,'owner','local');(a/'go').touch();wait_until(lambda:(a/'performed').exists())
        b=tmp_path/'short';b.mkdir()
        second=m.launch('graph','short',[sys.executable,'-B','-c',WORKER.replace('total_timeout=30','total_timeout=.6'),str(b),'local'],cwd=Path.cwd())
        wait_until(lambda:(b/'ready').exists());started=time.monotonic();(b/'go').touch()
        wait_until(lambda:(b/'denied').exists(),seconds=3)
        assert time.monotonic()-started<2 and (b/'denied').read_text()=='ActionDeferred'
        assert not (b/'performed').exists() and not first.exit_saved
        (a/'release').touch();wait_until(lambda:first.exit_saved and second.exit_saved)
    finally:m.shutdown()


POOL_WORKER='''
import json,sys,time,os
from pathlib import Path
from paperflow.control_ipc import WorkerClient
root=Path(sys.argv[1]);root.mkdir(parents=True,exist_ok=True)
c=WorkerClient.from_environment()
try:
 def local(cancel):
  (root/'local-start').write_text(str(time.time()))
  cancel.wait(.015)
  (root/'local-end').write_text(str(time.time()))
  return {'prepared':True}
 c.perform('local',{'operation':'isolated-prepare'},local,total_timeout=180)
 def llm(cancel):
  (root/'active.json').write_text(json.dumps({'pid':os.getpid(),'at':time.time()}))
  while not (root/'release').exists() and not cancel.wait(.03):pass
  return {'synthetic':True}
 c.perform('llm',{'operation':'isolated-llm'},llm,total_timeout=180)
 c.report_execution('held',{'reason':'synthetic acceptance; never creates a package'})
except Exception as exc:
 (root/'error.txt').write_text(type(exc).__name__+': '+str(exc))
 try:c.report_execution('stopped' if c.stopped.is_set() else 'held',{'reason':'synthetic stopped'})
 except Exception:pass
finally:c.close()
'''

IDLE_WORKER='''
import sys
from pathlib import Path
from paperflow.control_ipc import WorkerClient
c=WorkerClient.from_environment();Path(sys.argv[1]).touch()
try:c.stopped.wait(120)
finally:c.close()
'''


def test_192_real_graph_workers_and_immediate_refill_with_reserved_serial_slots(entry_case):
    root,old,_,c=entry_case
    c.article_workers=192;c.llm_workers=192;c.local_workers=1;c.rag_workers=1;c.max_live_units=194
    c.prepare();Path(c.handoff_root).mkdir(parents=True);c.save(root/'config-192.json')
    m=Manager(Store(c.root/'state.sqlite3'),root/'manager').start()
    s=Scheduler(m,c,isolation_root=root);units=root/'synthetic-workers';units.mkdir()
    tasks=[];started=time.monotonic();peak_rss=0
    try:
        for n in range(193):
            p=root/'uploads'/f'{n}.txt';p.parent.mkdir(exist_ok=True);p.write_text(f'isolated paper {n}')
            tasks.append(m.store.register_source(p,sha(p)))
        def launch(manager,config,key,*,schedule_attempt):
            return manager.launch('graph',key,[sys.executable,'-B','-c',POOL_WORKER,str(units/key)],
                cwd=Path.cwd(),schedule_attempt=schedule_attempt,total_timeout=240)
        s.launchers['graph']=launch;m.resume()
        result=s.tick();assert result['launched']==192
        def active():return list(units.glob('*/active.json'))
        wait_until(lambda:len(active())==192,seconds=80)
        assert not list(units.glob('*/error.txt'))
        snapshot=m.status()
        assert snapshot['live_units']==192 and snapshot['resources']['llm']['used']==192
        assert len(m.server.peers)==192
        queue=s.ledger.rows('SELECT state FROM scheduler_jobs')
        assert [r['state'] for r in queue].count('running')==192 and [r['state'] for r in queue].count('queued')==1
        # Local packaging/preparation still uses exactly one lane.
        intervals=sorted((float(p.read_text()),float((p.parent/'local-end').read_text())) for p in units.glob('*/local-start'))
        assert all(a[1]<=b[0] for a,b in zip(intervals,intervals[1:]))
        pids=[json.loads(p.read_text())['pid'] for p in active()]
        peak_rss=sum(psutil.Process(pid).memory_info().rss for pid in pids)
        extra=[]
        for kind in ('delivery','cleanup'):
            marker=units/(kind+'-ready')
            ex=m.launch(kind,'isolated-'+kind,[sys.executable,'-B','-c',IDLE_WORKER,str(marker)],cwd=Path.cwd())
            extra.append(ex);wait_until(marker.exists)
            with pytest.raises(ActionDeferred,match='unit_capacity'):
                m.launch(kind,'second-'+kind,[sys.executable,'-c','pass'],cwd=Path.cwd())
        assert m.status()['live_units']==194
        first=active()[0].parent;first_id=first.name
        first_execution=next(e for e in m.executions.values() if e.key==first_id)
        (first/'release').touch();wait_until(lambda:first_execution.exit_saved)
        pending=next(t for t in tasks if not (units/t['task_id']/'active.json').exists())
        refill=time.monotonic();assert s.tick()['launched']==1
        wait_until(lambda:(units/pending['task_id']/'active.json').exists(),seconds=12)
        refill_seconds=time.monotonic()-refill
        assert sum(e.kind=='graph' and not e.exit_saved for e in m.executions.values())==192
        assert sum(e.kind=='graph' and e.exit_saved for e in m.executions.values())==1
        m.pause('isolated 192 acceptance finished')
        wait_until(lambda:not m.status()['live_units'],seconds=20)
        assert all(e.exit_saved for e in m.executions.values()) and not m.status()['admission_open']
        assert not s.ledger.rows('SELECT * FROM delivery_packages')
        save(Path('evidence')/'pool192.json',dict(passed=True,kind='real-windows-jobs-and-pipes-synthetic-work',
            graph_peak=192,ipc_peers_at_peak=192,total_reserved_peak=194,local_peak=1,rag_limit=1,
            graph_workers_rss_bytes_at_peak=peak_rss,refill_seconds=refill_seconds,
            total_seconds=time.monotonic()-started,formal_network_calls=0,final=m.status()))
    finally:m.shutdown()
