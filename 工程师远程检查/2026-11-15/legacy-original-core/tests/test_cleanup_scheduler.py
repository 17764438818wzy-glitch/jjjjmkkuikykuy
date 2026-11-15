import json
import time

from paperflow.scheduler import Scheduler
from paperflow.cleanup_store import CleanupLedger
from test_control import manager
from test_delivery_state import delivery_case
from test_cleanup_local import material
from test_winjob import wait_until


def test_queue_runs_local_cleanup_while_rag_is_offline(delivery_case,tmp_path):
    m,c,t,d,*_=delivery_case
    c.min_free_gib=0;c.max_memory_percent=100
    m.store.update(t['task_id'],stage='handed_off')
    ledger,path=material(delivery_case)
    ledger.close_run(t['task_id'],t['task_id'],'complete');item=ledger.plan_artifact(path)
    scheduler=Scheduler(m,c,isolation_root=tmp_path)
    with m.store.db() as db:db.execute("UPDATE scheduler_services SET state='open' WHERE kind='rag'")
    scheduler.service.refresh()
    deadline=time.monotonic()+20
    while time.monotonic()<deadline:
        scheduler.tick()
        if not path.exists() and ledger.item(item['item_id'])['state']=='deleted':break
        time.sleep(.05)
    assert not path.exists()
    wait_until(lambda:m.status()['live_units']==0)
    scheduler.reap()
    rows=scheduler.ledger.rows("SELECT * FROM scheduler_jobs WHERE kind='cleanup'")
    assert len(rows)==1 and rows[0]['state']=='complete'
    assert m.store.get(t['task_id'])['stage']!='complete'  # cleanup never claims article completion


def test_cleanup_budget_cannot_consume_delivery_budget(delivery_case,tmp_path):
    m,c,t,*_=delivery_case;s=Scheduler(m,c,isolation_root=tmp_path)
    service=s.service.services['rag']['service_id'];unit=json.dumps([t['source_id'],'120201'],separators=(',',':'))
    delivery=s.ledger.enqueue('delivery',t['source_id'],unit,{'test':True},service)
    clean=s.ledger.enqueue('cleanup',t['source_id'],'cleanup:'+unit,{'test':True},service)
    attempt=s.ledger.claim(kinds=('cleanup',))
    s.ledger.finish(attempt['attempt_id'],'failed',dict(fault={'category':'transient','family':'connection'}),max_rounds=1)
    assert s.ledger.budget(clean)['rounds']==1
    assert s.ledger.budget(delivery)['rounds']==0


def test_cleanup_discovery_rotates_past_protected_history(delivery_case):
    from paperflow.cleanup_paths import track_artifact
    m,c,t,*_=delivery_case;c.processing_ledger_path=str(m.store.path)
    ledger=CleanupLedger(m.store,c)
    for index in range(66):
        path=c.root/'work'/t['task_id']/'private'/str(index);path.mkdir(parents=True);(path/'file').write_text('fixture')
        track_artifact(c,t,path,'reading' if index<65 else 'attempt')
    ledger.close_run(t['task_id'],t['task_id'],'complete')
    ledger.discover_local();ledger.discover_local()
    assert any(json.loads(r['resource']).get('path')==str(path.resolve()) for r in ledger.items())


def test_remote_cleanup_rebinds_service_configuration_without_resetting_budget(delivery_case,tmp_path):
    m,c,t,*_=delivery_case;s=Scheduler(m,c,isolation_root=tmp_path)
    prior=s.service.services['rag']['service_id']
    job=s.ledger.enqueue('cleanup',t['source_id'],'cleanup:unit',dict(item_id='fixture',resource_kind='remote'),prior)
    attempt=s.ledger.claim(kinds=('cleanup',))
    s.ledger.finish(attempt['attempt_id'],'failed',dict(fault={'category':'transient','family':'connection'}))
    fresh=s.ledger.ensure_service('rag',dict(endpoint='http://127.0.0.1:9999'))
    s.ledger.rebind_services(dict(s.service.services,rag=fresh))
    assert s.ledger.job(job)['service_id']==fresh['service_id']
    assert s.ledger.budget(job)['rounds']==1
