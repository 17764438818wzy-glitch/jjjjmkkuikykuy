import json
import time

import pytest

from paperflow.handoff import PackageHandoff
from paperflow.handoff_store import HandoffLedger, canonical
from test_control import manager
from test_delivery_state import delivery_case
from test_delivery_flow import IndexedRag, flow, advance, release


def retired(case, tmp_path):
    client=IndexedRag()
    old=flow(case,tmp_path,client);old.run();release(old)
    newcase=advance(case)
    new=flow(newcase,tmp_path,client);new.run();release(new)
    return newcase,client


def cleanup(case):
    from paperflow.cleanup_store import CleanupLedger
    m,c,*_=case
    return CleanupLedger(m.store,c)


def test_retirement_survives_switch_replay_and_new_switch(delivery_case,tmp_path):
    case,client=retired(delivery_case,tmp_path)
    ledger=cleanup(case);ledger.discover_remote()
    rows=ledger.items();assert len(rows)==1
    item=rows[0];assert item['category']=='delayed'
    assert item['eligible_at']==item['created']
    second=flow(case,tmp_path,client);second.run();release(second)
    ledger.discover_remote()
    assert ledger.items()[0]['created']==item['created']
    with pytest.raises(ValueError,match='retention'):
        ledger._write(lambda c:ledger.begin_sql(c,item['item_id'],now=item['eligible_at']-.01))
    assert ledger._write(lambda c:ledger.begin_sql(c,item['item_id'],now=item['eligible_at']))['state']=='deleting'


def test_new_references_cannot_point_at_deleting_package(delivery_case):
    m,c,t,d,*_=delivery_case
    with m.store.db() as db:db.execute("UPDATE delivery_packages SET lifecycle='deleting' WHERE manifest_sha256=?",(d,))
    with pytest.raises(ValueError,match='deleting'):
        HandoffLedger(m.store)._write(lambda db:HandoffLedger._reference(db,d,'repair','new','package',d))


def test_orphan_requires_explicit_close_and_separate_retention(delivery_case):
    m,c,t,d,commits,material,catalog=delivery_case
    c.preserve_sealed_packages=False  # Explicit legacy GC policy, never the default.
    handoff=PackageHandoff(m.store,c.handoff_root)
    intent=handoff.prepare('abandoned-run',dict(t,revision=3),material,catalog,mapping_version='orphan')
    package=handoff.publish('abandoned-run',intent['input_fingerprint'])
    ledger=cleanup(delivery_case)
    with pytest.raises(ValueError,match='closed|recoverable'):
        ledger.plan_publication('abandoned-run',intent['input_fingerprint'])
    ledger.close_run('abandoned-run',t['task_id'],'abandoned',reason='explicit fixture abandonment')
    with pytest.raises(ValueError,match='retention'):
        ledger.plan_publication('abandoned-run',intent['input_fingerprint'])
    c.orphan_package_retention_seconds=60
    item=ledger.plan_publication('abandoned-run',intent['input_fingerprint'])
    with pytest.raises(ValueError,match='retention'):
        ledger._write(lambda db:ledger.begin_sql(db,item['item_id'],now=item['eligible_at']-1))
    ledger._write(lambda db:ledger.begin_sql(db,item['item_id'],now=item['eligible_at']))
    with pytest.raises(ValueError,match='deleting|closed'):
        handoff.resume('abandoned-run',intent['input_fingerprint'])
    assert package.exists()  # intent is not deletion


def test_shared_package_waits_for_b_and_current_package_is_protected(delivery_case,tmp_path):
    case,_=retired(delivery_case,tmp_path)
    ledger=cleanup(case);m,c,t,old,*_=delivery_case
    ledger.close_run(t['task_id'],t['task_id'],'complete')
    with pytest.raises(ValueError,match='reference|delivery|current|retained'):
        ledger.plan_package(old)
    with pytest.raises(ValueError,match='reference|delivery|current|retained'):
        ledger.plan_package(case[3])


def test_cleanup_status_does_not_invalidate_active_delivery(delivery_case,tmp_path):
    case,_=retired(delivery_case,tmp_path);ledger=cleanup(case)
    ledger.discover_remote();row=ledger.items()[0]
    before=case[4].ledger.active(case[2]['source_id'],'120201')
    ledger._write(lambda c:ledger.result_sql(c,row['item_id'],'error',{'error':'permission denied'}))
    assert case[4].ledger.active(case[2]['source_id'],'120201')==before
    status=ledger.status(case[2]['task_id'])
    assert status['delayed']==1 and status['closeout_pending']==0
