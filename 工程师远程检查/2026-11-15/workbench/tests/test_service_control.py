from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
import time

import pytest

from paperflow.config import Config
from paperflow.control import Manager
from paperflow.store import Store
from test_control import start_worker
from test_winjob import wait_until
from test_scheduling_store import TRANSIENT


@pytest.fixture
def service_control(tmp_path):
    from paperflow.service_control import ServiceControl
    config=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),rag_url='http://127.0.0.1:1')
    config.prepare()
    m=Manager(Store(tmp_path/'ledger.sqlite3'),tmp_path/'manager').start()
    service=ServiceControl(m,config)
    m.resume();service.flush_clock()
    yield m,service
    m.shutdown();service.flush_clock()


def permit(m,ex,label):
    ticket=m.request_action(ex.execution_id,label,'llm',{},0,30)['ticket']
    snapshot=m.check_begin(ex.execution_id,ticket)
    m.grant_begin(ex.execution_id,ticket,snapshot)
    return ticket


def test_failure_state_write_closes_service_gate_before_sql_and_preserves_other_service(service_control,tmp_path):
    m,service=service_control
    a,_=start_worker(m,tmp_path,name='a');b,_=start_worker(m,tmp_path,name='b');c,_=start_worker(m,tmp_path,name='c')
    ticket_a=permit(m,a,'first');ticket_b=permit(m,b,'second')
    stale=m.request_action(c.execution_id,'stale','llm',{},0,30)['ticket']
    m.record_result(a.execution_id,ticket_a,m.epoch,'diagnostic',{'fault':TRANSIENT})
    with m.store.db() as db,ThreadPoolExecutor(1) as pool:
        db.execute('BEGIN IMMEDIATE')
        future=pool.submit(m.record_result,b.execution_id,ticket_b,m.epoch,'diagnostic',{'fault':TRANSIENT})
        wait_until(lambda:not m.scopes['service:llm']['ready'])
        with pytest.raises(ValueError):m.snapshot(c.execution_id,'llm')
        assert m.scopes['service:rag']['ready'] and m.scopes['service:rag']['state']['enabled']
        db.commit();future.result(timeout=3)
    assert service.services['llm']['state']=='open'
    with pytest.raises(ValueError):m.check_begin(c.execution_id,stale)
    assert len(service.ledger.rows('SELECT * FROM scheduler_events'))==1


def test_pause_prevents_probe_and_reboot_preserves_incident(service_control,tmp_path):
    m,service=service_control
    a,_=start_worker(m,tmp_path,name='a');ticket=permit(m,a,'auth')
    m.record_result(a.execution_id,ticket,m.epoch,'diagnostic',{'fault':dict(TRANSIENT,category='configuration',status=401)})
    identity=service.services['llm']['service_id'];incident=service.services['llm']['event_id']
    m.pause();service.flush_clock()
    assert service.open_due(now=time.time()+100000)==[]
    assert service.ledger.service(identity)['event_id']==incident
    assert service.ledger.service(identity)['state']=='open'


def test_service_result_and_admission_state_commit_together(service_control,tmp_path):
    m,service=service_control
    ex,_=start_worker(m,tmp_path);ticket=permit(m,ex,'bad')
    m.record_result(ex.execution_id,ticket,m.epoch,'diagnostic',{'fault':dict(TRANSIENT,category='configuration')})
    row=m.ledger.rows("SELECT state FROM admission_scopes WHERE scope='service:llm'")[0]
    assert json.loads(row['state'])['enabled'] is False
    assert m.scopes['service:llm']['state']['enabled'] is False
    assert m.ledger.rows('SELECT state FROM control_actions WHERE action_id=?',(ticket,))[0]['state']=='diagnostic'
