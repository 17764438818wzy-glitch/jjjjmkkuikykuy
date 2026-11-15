import json
import threading
from pathlib import Path

import pytest

from paperflow.cleanup_paths import inventory,delete_inventory,track_artifact
from paperflow.cleanup_store import CleanupLedger
from paperflow.handoff_store import HandoffLedger,canonical
from test_control import manager,start_worker
from test_delivery_state import delivery_case
from test_winjob import wait_until


def material(case,*,thread=None):
    m,c,t,d,*_=case;c.processing_ledger_path=str(m.store.path)
    path=c.root/'work'/t['task_id']/'attempts'/'discarded'
    path.mkdir(parents=True);(path/'result.txt').write_text('synthetic temporary')
    track_artifact(c,dict(t,thread_id=thread or t['task_id']),path,'attempt')
    return CleanupLedger(m.store,c),path


def test_registered_temporary_can_delete_only_after_closed_run(delivery_case):
    ledger,path=material(delivery_case);t=delivery_case[2]
    with pytest.raises(ValueError,match='closed|recoverable'):ledger.plan_artifact(path)
    ledger.close_run(t['task_id'],t['task_id'],'complete')
    item=ledger.plan_artifact(path)
    ledger._write(lambda c:ledger.begin_sql(c,item['item_id']))
    result=delete_inventory(json.loads(item['resource']),ledger.roots(),ledger.originals(),threading.Event())
    ledger._write(lambda c:ledger.result_sql(c,item['item_id'],'deleted',result))
    assert not path.exists() and Path(t['source_path']).read_text()=='original'


def test_deleting_directory_refuses_new_dependency(delivery_case):
    ledger,path=material(delivery_case);m,c,t,d,*_=delivery_case
    ledger.close_run(t['task_id'],t['task_id'],'complete');item=ledger.plan_artifact(path)
    ledger._write(lambda db:ledger.begin_sql(db,item['item_id']))
    with pytest.raises(ValueError,match='deleting'):
        ledger._write(lambda db:HandoffLedger._reference(db,d,'phase_dependency','late','phase_output',str(path/'child')))


def test_live_graph_prevents_run_close(delivery_case,tmp_path):
    ledger,path=material(delivery_case);m,c,t,*_=delivery_case
    ex,_=start_worker(m,tmp_path,name=t['task_id'])
    with pytest.raises(ValueError,match='exit'):ledger.close_run(t['task_id'],t['task_id'],'complete')
    m.cancel_unit(ex.execution_id,'test');wait_until(lambda:ex.exit_saved)
    ledger.close_run(t['task_id'],t['task_id'],'complete')
    with pytest.raises(ValueError,match='closed'):
        start_worker(m,tmp_path,name=t['task_id'])


def test_original_and_unregistered_paths_never_become_candidates(delivery_case):
    ledger,path=material(delivery_case);original=Path(delivery_case[2]['source_path'])
    with pytest.raises(ValueError,match='unregistered'):ledger.plan_artifact(original.parent)
    with pytest.raises(ValueError,match='original'):inventory(original.parent,[original.parent.parent],[original])


def test_changed_or_extra_file_blocks_delete_then_partial_retry_is_idempotent(delivery_case):
    ledger,path=material(delivery_case);t=delivery_case[2]
    ledger.close_run(t['task_id'],t['task_id'],'complete');item=ledger.plan_artifact(path)
    ledger._write(lambda db:ledger.begin_sql(db,item['item_id']))
    resource=json.loads(item['resource']);extra=path/'other.txt';extra.write_text('not in inventory')
    with pytest.raises(ValueError,match='unexpected'):delete_inventory(resource,ledger.roots(),ledger.originals(),threading.Event())
    assert (path/'result.txt').exists()
    extra.unlink();(path/'result.txt').unlink()  # simulate confirmed partial own deletion before process death
    result=delete_inventory(resource,ledger.roots(),ledger.originals(),threading.Event())
    assert result['absent']


def test_link_cannot_escape_owned_directory(delivery_case,tmp_path):
    import subprocess
    ledger,path=material(delivery_case)
    # Native NTFS junction does not require symlink privileges.
    outside=tmp_path/'outside';outside.mkdir();(outside/'keep').write_text('original')
    junction=path/'link'
    result=subprocess.run(['cmd','/c','mklink','/J',str(junction),str(outside)],capture_output=True)
    assert result.returncode==0
    with pytest.raises(ValueError,match='links'):inventory(path,ledger.roots(),ledger.originals())
    assert (outside/'keep').read_text()=='original'
    junction.rmdir()


def test_phase_cache_publishing_rejects_deleted_output(delivery_case):
    from paperflow.phase_cache import publish_cache
    ledger,path=material(delivery_case);m,c,t,*_=delivery_case
    ledger.close_run(t['task_id'],t['task_id'],'complete');item=ledger.plan_artifact(path)
    ledger._write(lambda db:ledger.begin_sql(db,item['item_id']))
    with pytest.raises(ValueError,match='deleting'):
        publish_cache(c,t,'round1','f'*64,dict(result={'draft_dir':str(path)},stage_records=[]),{})
