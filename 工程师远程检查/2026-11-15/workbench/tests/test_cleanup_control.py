import json
import sys
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from test_control import manager,WORKER
from test_delivery_state import delivery_case,begin
from test_cleanup_store import retired
from test_winjob import wait_until


def controlled(case,tmp_path):
    from paperflow.cleanup_control import CleanupCommits
    m,c,t,d,*_=case
    commits=CleanupCommits(m,c,isolation_root=tmp_path)
    commits.ledger.discover_remote();item=commits.ledger.items()[0]
    # Fixture clock only: accelerate seven days without changing production defaults.
    with m.store.db() as db:db.execute('UPDATE cleanup_items SET eligible_at=0 WHERE item_id=?',(item['item_id'],))
    directory=tmp_path/uuid.uuid4().hex
    key=commits.prepare(item['item_id'])
    ex=m.launch('cleanup',key,[sys.executable,'-c',WORKER,str(directory),'local'],cwd=Path.cwd(),expected_generation=0)
    wait_until(lambda:(directory/'ready').exists())
    return commits,commits.ledger.item(item['item_id']),ex


def request(commits,item,ex,kind,operation,**fields):
    m=commits.manager
    return m.request_action(ex.execution_id,uuid.uuid4().hex,kind,
        dict(item_id=item['item_id'],operation=operation,**fields),0,120)['ticket']


def test_cleanup_checks_exact_document_and_delete_body(delivery_case,tmp_path):
    case,client=retired(delivery_case,tmp_path);commits,item,ex=controlled(case,tmp_path)
    commits.ledger._write(lambda c:commits.ledger.begin_sql(c,item['item_id']))
    obj=json.loads(item['resource'])['object']
    resource=dict(method='DELETE',endpoint=f"/datasets/{obj['dataset_id']}/documents",ids=[obj['document_id']])
    request(commits,item,ex,'cleanup_write','http',**resource)
    for change in (dict(ids=['foreign']),dict(endpoint='/datasets/foreign/documents'),dict(method='PATCH')):
        with pytest.raises(ValueError):request(commits,item,ex,'cleanup_write','http',**dict(resource,**change))


def test_waiting_cleanup_admission_is_denied_after_pause(delivery_case,tmp_path):
    case,_=retired(delivery_case,tmp_path);commits,item,ex=controlled(case,tmp_path);m=commits.manager
    commits.ledger._write(lambda c:commits.ledger.begin_sql(c,item['item_id']))
    obj=json.loads(item['resource'])['object']
    tid=request(commits,item,ex,'cleanup_write','http',method='DELETE',endpoint=f"/datasets/{obj['dataset_id']}/documents",ids=[obj['document_id']])
    def acquire():
        snapshot=m.check_begin(ex.execution_id,tid)
        # SQL may finish after pause; only the serialized final gate issues authority.
        return m.grant_begin(ex.execution_id,tid,snapshot)
    with m.store.db() as writer,ThreadPoolExecutor(1) as pool:
        writer.execute('BEGIN IMMEDIATE');future=pool.submit(acquire)
        wait_until(lambda:m.tickets[tid].get('checking'))
        m.pause('cleanup wait');writer.commit()
        with pytest.raises(ValueError):future.result(timeout=5)
    assert not m.tickets[tid]['consumed']


def test_cleanup_commit_rechecks_pause_after_database_wait(delivery_case,tmp_path):
    case,_=retired(delivery_case,tmp_path);commits,item,ex=controlled(case,tmp_path);m=commits.manager
    tid=request(commits,item,ex,'local','begin')
    snapshot=m.check_begin(ex.execution_id,tid);m.grant_begin(ex.execution_id,tid,snapshot)
    descriptor=commits.stage(ex.execution_id,dict(item_id=item['item_id']))
    with m.store.db() as writer,ThreadPoolExecutor(1) as pool:
        writer.execute('BEGIN IMMEDIATE');future=pool.submit(commits.handle,ex.execution_id,'begin',descriptor)
        wait_until(lambda:ex.execution_id in m.publications)
        m.pause('cleanup commit wait');writer.commit()
        with pytest.raises(ValueError):future.result(timeout=5)
    assert commits.ledger.item(item['item_id'])['state']=='waiting'


def test_cleanup_launch_cannot_invalidate_existing_delivery_owner(delivery_case,tmp_path):
    from test_delivery_state import owner
    from paperflow.cleanup_control import CleanupCommits
    case,_=retired(delivery_case,tmp_path);m,c,*_=case
    commits=CleanupCommits(m,c,isolation_root=tmp_path);commits.ledger.discover_remote();item=commits.ledger.items()[0]
    ex,identity=owner(case,tmp_path)
    snapshot=m.snapshot(ex.execution_id,'local')
    with pytest.raises(ValueError,match='owned'):commits.prepare(item['item_id'])
    assert m._valid(snapshot,ex.execution_id)
