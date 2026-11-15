import json
from concurrent.futures import ThreadPoolExecutor
import threading
import time

import pytest

from paperflow.cleanup_paths import track_artifact
from paperflow.cleanup_store import CleanupLedger
from paperflow.handoff import PackageHandoff
from paperflow.handoff_store import HandoffLedger,canonical
from paperflow.repair_store import RepairLedger
from test_control import manager
from test_delivery_state import delivery_case
from test_cleanup_local import material


@pytest.mark.parametrize('outcome',['classification_pending','repair_pending','dependency_pending','frozen_pending','reports_pending'])
def test_completed_repair_with_remaining_responsibility_retains_source(delivery_case,outcome):
    m,c,t,d,commits,*_=delivery_case;ledger=CleanupLedger(m.store,c)
    identity=commits.ledger.context(d,'120201')['identity']
    result=ledger._write(lambda db:RepairLedger(m.store,initialize=False).request_sql(db,identity,'round3_chunk','chunks_missing','120201',{'kind':'fixture'}))
    rid=result['repair_request_id']
    with m.store.db() as db:
        db.execute("UPDATE repair_requests SET state='complete' WHERE repair_request_id=?",(rid,))
        db.execute('INSERT INTO repair_targets VALUES(?,?,?,?,?)',(rid,'120201',d,1,outcome))
    ledger._write(ledger.release_sql)
    assert ledger.rows("SELECT 1 FROM package_references WHERE owner_kind='repair' AND owner_id=?",(rid,))


def test_bidirectional_package_reference_retains_predecessor(delivery_case):
    m,c,t,d,commits,*_=delivery_case;ledger=CleanupLedger(m.store,c)
    # A reference's owner package and resource package may differ.
    with m.store.db() as db:
        db.execute("DELETE FROM package_references WHERE package_sha256=?",(d,))
        db.execute('DELETE FROM target_versions WHERE manifest_sha256=?',(d,))
        HandoffLedger._reference(db,d,'external_recovery','future','package',d)
        with pytest.raises(ValueError,match='reference'):ledger.package_guard(db,d)


def test_delete_intent_and_new_path_reference_serialize(delivery_case):
    ledger,path=material(delivery_case);m,c,t,d,*_=delivery_case
    ledger.close_run(t['task_id'],t['task_id'],'complete');item=ledger.plan_artifact(path)
    def delete():
        try:ledger._write(lambda db:ledger.begin_sql(db,item['item_id']));return 'deleted'
        except ValueError:return 'protected'
    def reference():
        try:ledger._write(lambda db:HandoffLedger._reference(db,d,'future','cache','phase_output',str(path)));return 'referenced'
        except ValueError:return 'denied'
    with ThreadPoolExecutor(2) as pool:
        a,b=pool.submit(delete),pool.submit(reference);outcome=a.result(),b.result()
    assert outcome in (('deleted','denied'),('protected','referenced'))


def test_pending_package_publication_cannot_acquire_deleting_material(delivery_case):
    ledger,path=material(delivery_case);m,c,t,d,commits,source,catalog=delivery_case
    ledger.close_run(t['task_id'],t['task_id'],'complete');item=ledger.plan_artifact(path)
    ledger._write(lambda db:ledger.begin_sql(db,item['item_id']))
    intent=next(json.loads(r['data']) for r in ledger.rows('SELECT data FROM publication_intents'))
    intent=dict(intent,thread_id='late',material_dir=str(path),input_fingerprint='late')
    with pytest.raises(ValueError,match='deleting'):ledger.prepare(intent)


def test_distinct_inventory_cannot_register_second_delete_for_same_path(delivery_case):
    ledger,path=material(delivery_case);t=delivery_case[2]
    ledger.close_run(t['task_id'],t['task_id'],'complete');ledger.plan_artifact(path)
    (path/'result.txt').write_text('changed after plan')
    with pytest.raises(ValueError,match='changed|conflict|existing'):ledger.plan_artifact(path)


def test_original_moved_inside_product_root_is_still_protected(delivery_case):
    ledger,path=material(delivery_case);m,c,t,*_=delivery_case
    with m.store.db() as db:db.execute('INSERT INTO source_versions VALUES(?,?)',(str(path/'result.txt'),t['task_id']))
    ledger.close_run(t['task_id'],t['task_id'],'complete')
    with pytest.raises(ValueError,match='original'):ledger.plan_artifact(path)
