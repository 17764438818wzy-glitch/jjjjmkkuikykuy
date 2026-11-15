from pathlib import Path
import json
import sqlite3
import threading
import time

import pytest

from paperflow.config import Config
from paperflow.package import save
from paperflow.instance_lock import InstanceLock


@pytest.fixture
def entry_case(tmp_path):
    root=tmp_path/'owned'; root.mkdir()
    old=tmp_path/'legacy'
    for relative in ('langgraph_workflow/runtime/batch10-20261016-01/STOP_TRIAL', 'working/codex-dispatcher/STOP'):
        path=old/relative;path.parent.mkdir(parents=True,exist_ok=True);path.write_text('paused')
    source=tmp_path/'paper.txt';source.write_text('one explicitly selected paper')
    config=Config(home=str(root/'runtime'),handoff_root=str(root/'handoff'),source_roots=[],
        catalog_path=str(root/'catalog.json'),rag_url='http://127.0.0.1:18088',cleanup=False,
        delivery_acceptance_report=str(root/'visibility.json'),min_free_gib=0,max_memory_percent=100)
    save(config.catalog_path,{'majors':[{'code':'x','name':'Example','paper_dataset_id':'owned-body'}]})
    save(config.delivery_acceptance_report,{'passed':True,'endpoint':config.rag_url,'identity':{'dataset_ids':['owned-body']}})
    config.save(root/'config.json')
    return root,old,source,config


def test_default_entry_is_paused_and_only_registers_explicit_source(entry_case):
    from paperflow.managed import run_managed
    root,old,source,config=entry_case
    before=source.read_bytes()
    result=run_managed(config,root,[source],legacy_root=old)
    assert result['state']=='paused' and result['manager']['live_units']==0
    assert result['manager']['pause_persisted']
    assert len(result['tasks'])==1 and result['tasks'][0]['stage']=='round1'
    assert source.read_bytes()==before
    with sqlite3.connect(config.root/'state.sqlite3') as db:
        assert db.execute('SELECT COUNT(*) FROM control_actions').fetchone()[0]==0


def test_entry_rejects_formal_target_before_creating_runtime(entry_case):
    from paperflow.managed import run_managed
    root,old,source,config=entry_case
    config.rag_url='http://192.168.1.120:8088'
    with pytest.raises(ValueError,match='accepted|endpoint|isolated'):
        run_managed(config,root,[source],legacy_root=old,execute=True)
    assert not config.root.exists()


def test_entry_holds_actual_legacy_locks(entry_case):
    from paperflow.managed import run_managed, legacy_guard
    root,old,source,config=entry_case
    lock=legacy_guard(old).paths[0]
    with InstanceLock(lock):
        with pytest.raises(RuntimeError):
            run_managed(config,root,[source],legacy_root=old)


def test_preexisting_stop_never_resumes_or_launches(entry_case):
    from paperflow.managed import run_managed
    root,old,source,config=entry_case
    (root/'STOP').write_text('pause requested')
    result=run_managed(config,root,[source],legacy_root=old,execute=True,seconds=1)
    assert result['state']=='paused' and result['manager']['live_units']==0
    with sqlite3.connect(config.root/'state.sqlite3') as db:
        assert db.execute('SELECT COUNT(*) FROM control_executions').fetchone()[0]==0


def test_entry_rejects_unowned_catalog_targets(entry_case):
    from paperflow.managed import run_managed
    root,old,source,config=entry_case
    save(config.catalog_path,{'majors':[{'code':'x','name':'Example','paper_dataset_id':'formal-dataset'}]})
    with pytest.raises(ValueError,match='accepted'):
        run_managed(config,root,[source],legacy_root=old)


def test_entry_cannot_settle_between_reap_and_next_discovery(entry_case,monkeypatch):
    from paperflow.managed import run_managed
    from paperflow.scheduler import Scheduler
    root,old,source,config=entry_case
    def interrupted_tick(scheduler):
        scheduler.discover()
        scheduler.ledger._write(lambda db:db.execute("UPDATE scheduler_jobs SET state='complete'"))
        # Model the interval after the completed graph is reaped but before its
        # newly handed-off targets have been discovered by this same tick.
        time.sleep(.4)
        assert not scheduler.manager.stopped.is_set(), 'monitor closed admission inside an incomplete tick'
        return {'launched':0}
    monkeypatch.setattr(Scheduler,'tick',interrupted_tick)
    result=run_managed(config,root,[source],legacy_root=old,execute=True,seconds=3)
    assert result['reason']=='settled' and result['manager']['pause_persisted']


def test_stopped_half_open_probe_is_recovered_on_next_paused_boot(entry_case,monkeypatch):
    from paperflow.managed import run_managed
    from paperflow.scheduler import Scheduler
    import sys
    root,old,source,config=entry_case
    original=Scheduler.__init__
    def setup(scheduler,*args,**kwargs):
        original(scheduler,*args,**kwargs)
        service=scheduler.service.services['llm']['service_id']
        scheduler.ledger._write(lambda db:db.execute(
            "UPDATE scheduler_services SET state='half_open',probe_job=NULL WHERE service_id=?",(service,)))
        def launch(manager,config,task_id,*,schedule_attempt):
            code="from paperflow.control_ipc import WorkerClient;c=WorkerClient.from_environment();c.stopped.wait(20);c.report_execution('stopped',{});c.close()"
            result=manager.launch('graph',task_id,[sys.executable,'-B','-c',code],cwd=Path.cwd(),schedule_attempt=schedule_attempt)
            (root/'STOP').write_text('stop this owned probe')
            return result
        scheduler.launchers['graph']=launch
    monkeypatch.setattr(Scheduler,'__init__',setup)
    stopped=run_managed(config,root,[source],legacy_root=old,execute=True,seconds=20)
    assert stopped['manager']['live_units']==0 and stopped['manager']['pause_persisted']
    with sqlite3.connect(config.root/'state.sqlite3') as db:
        assert db.execute("SELECT state FROM scheduler_services WHERE kind='llm'").fetchone()[0]=='half_open'
        assert db.execute("SELECT COUNT(*) FROM scheduler_attempts WHERE state!='finished'").fetchone()[0]==1
    monkeypatch.setattr(Scheduler,'__init__',original)
    reboot=run_managed(config,root,[source],legacy_root=old)
    assert reboot['manager']['live_units']==0
    with sqlite3.connect(config.root/'state.sqlite3') as db:
        assert db.execute("SELECT state,probe_job FROM scheduler_services WHERE kind='llm'").fetchone()==('open',None)
        assert db.execute("SELECT COUNT(*) FROM scheduler_attempts WHERE state!='finished'").fetchone()[0]==0
