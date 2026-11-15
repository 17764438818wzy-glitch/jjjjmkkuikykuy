"""Behavioral regressions for rejected candidates and frozen committed versions."""
import copy
import json

import pytest

from test_control import manager
from test_delivery_state import delivery_case
from test_delivery_flow import IndexedRag,advance,flow,release
from test_delivery_reconciliation import unknown


def freeze(workflow):
    return workflow.actions.commits.freeze_active(workflow.identity['source_id'],workflow.code,'invalid material')


def safety(workflow,digest=None):
    return workflow.ledger.safety(workflow.identity['source_id'],workflow.code,digest or workflow.digest)


@pytest.mark.parametrize('origin',['package','scope'])
def test_frozen_committed_active_is_disabled_and_removed(delivery_case,tmp_path,origin):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client);a.run()
    if origin=='package':
        a.actions.m.ledger._write(lambda c:c.execute('INSERT INTO package_freezes VALUES(?,?,?)',(a.digest,a.code,'invalid')))
    else:a.actions.m.change_admission_state(a.actions.ex.scope,'freeze-active',dict(frozen=True))
    assert a.run()['state']=='frozen'
    assert all(doc['status']=='0' for doc in client.docs.values())
    assert all(ch['available'] is False for chunks in client.chunks.values() for ch in chunks)
    assert a.ledger.active(a.identity['source_id'],a.code) is None
    assert safety(a)['state']=='disabled'
    client.calls=[]
    assert a.run()['state']=='frozen'
    assert all(method=='GET' or path=='/retrieval' for method,path,_ in client.calls)


def test_freeze_while_paused_persists_plan_without_remote_write(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client);a.run()
    a.actions.m.pause('pause before freeze');client.calls=[]
    freeze(a)
    assert safety(a)['state']=='pending'
    assert a.ledger.active(a.identity['source_id'],a.code)['state']=='frozen_pending'
    with pytest.raises(ValueError):a.run()
    assert not client.calls
    assert all(doc['status']=='1' for doc in client.docs.values())


def test_freeze_disable_failure_is_durable_and_retry_only_disables(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client);a.run();freeze(a)
    client.fail=lambda method,path,body:(_ for _ in ()).throw(ConnectionError('read unavailable')) if method=='GET' else None
    assert a.run()['state']=='frozen_pending'
    assert safety(a)['state']=='pending'
    client.fail=None;client.calls=[]
    assert a.run()['state']=='frozen'
    assert not any(method=='POST' and path!='/retrieval' for method,path,_ in client.calls)
    assert a.ledger.active(a.identity['source_id'],a.code) is None


def test_freeze_unknown_disable_keeps_barrier_and_pending_plan(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client);a.run();freeze(a)
    native=client.request
    def lost(method,path,body=None,content_type=None):
        value=native(method,path,body,content_type)
        if method=='PATCH' and body.get('enabled')==0:raise TimeoutError('response lost')
        return value
    client.request=lost
    assert a.run()['state']=='frozen_pending'
    assert safety(a)['state']=='pending'
    assert a.ledger.unresolved(a.identity['source_id'],a.code)
    client.request=native;client.calls=[]
    assert a.run()['state']=='frozen_pending'
    assert all(method=='GET' or path=='/retrieval' for method,path,_ in client.calls)
    assert a.ledger.active(a.identity['source_id'],a.code)['state']=='frozen_pending'


def test_frozen_disable_after_remote_success_crash_recovers_without_rewrite(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client);a.run();freeze(a)
    def crash(point):
        if point=='safety_disabled':raise RuntimeError('before local disabled commit')
    a.fault=crash
    with pytest.raises(RuntimeError,match='before local'):a.run()
    assert all(doc['status']=='0' for doc in client.docs.values())
    assert safety(a)['state']=='pending'
    release(a)
    b=flow(delivery_case,tmp_path,client);client.calls=[]
    assert b.run()['state']=='frozen'
    assert all(method=='GET' or path=='/retrieval' for method,path,_ in client.calls)
    assert safety(b)['state']=='disabled'
    assert b.ledger.active(b.identity['source_id'],b.code) is None


def test_frozen_old_active_is_disabled_after_expected_advances_and_b_is_untouched(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client);a.run()
    b=flow(delivery_case,tmp_path,client,'120202');b.run()
    before_b=copy.deepcopy({key:doc for key,doc in client.docs.items() if key[0]=='paper-two'})
    old=set(key for key in client.docs if key[0]=='paper-ds');release(a)
    new_case=advance(delivery_case);v2=flow(new_case,tmp_path,client)
    freeze(v2)  # Freeze the actual active v1, not the newly expected v2.
    assert v2.run()['state']=='active'
    assert all(client.docs[key]['status']=='0' for key in old)
    assert before_b=={key:doc for key,doc in client.docs.items() if key[0]=='paper-two'}
    assert v2.ledger.active(v2.identity['source_id'],v2.code)['identity']['manifest_sha256']==v2.digest
    assert safety(v2,a.digest)['state']=='disabled'
    with v2.ledger.store.db() as c:
        refs=c.execute("SELECT package_sha256 FROM package_references WHERE owner_kind='active_specialty'").fetchall()
    assert v2.digest in {row[0] for row in refs}


@pytest.mark.parametrize('action',['enable','upload','other_document','mixed_update'])
def test_frozen_recovery_permit_only_allows_recorded_disable(delivery_case,tmp_path,action):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client);a.run();freeze(a)
    ds,did=next(iter(client.docs));client.calls=[]
    with pytest.raises(ValueError):
        with a.client.version(a.digest,recovery=True):
            if action=='enable':a.client.request('PATCH',f'/datasets/{ds}/documents/{did}',dict(enabled=1))
            elif action=='upload':a.client.request('POST',f'/datasets/{ds}/documents',dict(content='wrong'))
            elif action=='mixed_update':a.client.request('PATCH',f'/datasets/{ds}/documents/{did}',dict(enabled=0,name='unauthorized rename'))
            else:a.client.request('PATCH',f'/datasets/{ds}/documents/not-ours',dict(enabled=0))
    assert not client.calls


def test_historical_terminal_success_cannot_release_unknown_write(delivery_case,tmp_path):
    client,a,action=unknown(delivery_case,tmp_path)
    a.actions.m.record_result(action['execution_id'],action['action_id'],action['epoch'],'complete',
                             dict(terminal=True,response_sha256='a'*64))
    assert a.run()['state']=='blocked'
    assert a.ledger.unresolved(a.identity['source_id'],a.code)


def test_failed_old_restoration_stays_retryable_without_reactivating_bad_candidate(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client);a.run();release(a)
    old=set(client.docs);old_chunks=copy.deepcopy(client.chunks)
    b=flow(advance(delivery_case),tmp_path,client)
    b.fault=lambda point:(_ for _ in ()).throw(RuntimeError('crash')) if point=='old_disabled' else None
    with pytest.raises(RuntimeError):b.run()
    new=set(client.docs)-old
    for _,did in old:client.chunks[did].clear()
    for _,did in new:client.chunks[did][0]['content']+=' broken'
    b.fault=lambda point:None
    with pytest.raises(ConnectionError):b.run()
    assert all(client.docs[key]['status']=='0' for key in new)
    assert b.ledger.active(b.identity['source_id'],b.code)['state']=='restore_pending'
    client.chunks.update(old_chunks);client.calls=[]
    assert b.run()['state']=='withdrawn'
    assert all(client.docs[key]['status']=='0' for key in new)
    assert all(client.docs[key]['status']=='1' for key in old)
    assert b.ledger.active(b.identity['source_id'],b.code)['state']=='active'
    assert not any(method=='POST' and path!='/retrieval' for method,path,_ in client.calls)


def test_pause_at_disabled_commit_retains_pending_plan(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client);a.run();freeze(a)
    a.fault=lambda point:a.actions.m.pause('pause after remote disable') if point=='safety_disabled' else None
    with pytest.raises(ValueError):a.run()
    assert safety(a)['state']=='pending'
    assert a.ledger.active(a.identity['source_id'],a.code)['state']=='frozen_pending'
    assert all(doc['status']=='0' for doc in client.docs.values())


def test_freeze_during_active_readback_cannot_report_healthy(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client);a.run()
    native=client.request;done=False
    def freeze_on_query(method,path,body=None,content_type=None):
        nonlocal done
        value=native(method,path,body,content_type)
        if path=='/retrieval' and not done:
            done=True;freeze(a)
        return value
    client.request=freeze_on_query
    assert a.run()['state']=='frozen'
    assert a.ledger.active(a.identity['source_id'],a.code) is None


def test_late_disabled_commit_does_not_delete_new_active_or_its_reference(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client);a.run();freeze(a)
    old=a.digest;release(a)
    v2=flow(advance(delivery_case),tmp_path,client);assert v2.run()['state']=='active'
    current=v2.ledger.active(v2.identity['source_id'],v2.code)
    # Replay a late completion for the old plan under the current qualified owner.
    v2.actions.commit('safety',dict(phase='disabled',object_manifest=old))
    assert v2.ledger.active(v2.identity['source_id'],v2.code)==current
    with v2.ledger.store.db() as c:
        assert c.execute("SELECT 1 FROM package_references WHERE owner_kind='active_specialty' AND package_sha256=?",(v2.digest,)).fetchone()


def test_repeating_completed_freeze_returns_existing_disposition(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client);a.run();freeze(a)
    assert a.run()['state']=='frozen'
    client.calls=[]
    assert freeze(a)['state']=='disabled'
    assert a.run()['state']=='frozen'
    assert not any(method=='PATCH' for method,_,_ in client.calls)


def test_withdrawal_skips_frozen_old_version_without_reopening_bad_candidate(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client);a.run();release(a)
    old=set(client.docs);b=flow(advance(delivery_case),tmp_path,client)
    b.fault=lambda point:(_ for _ in ()).throw(RuntimeError('crash')) if point=='old_disabled' else None
    with pytest.raises(RuntimeError):b.run()
    for key in set(client.docs)-old:client.chunks[key[1]][0]['content']+=' broken'
    freeze(b);b.fault=lambda point:None
    with pytest.raises(ConnectionError):b.run()
    assert b.ledger.switching(b.identity['source_id'],b.code)['phase']=='withdrawn'
    assert b.ledger.active(b.identity['source_id'],b.code) is None
    assert all(doc['status']=='0' for doc in client.docs.values())


def test_freeze_during_old_restore_cannot_be_overwritten_by_withdraw_commit(delivery_case,tmp_path):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client);a.run();release(a)
    old=set(client.docs);b=flow(advance(delivery_case),tmp_path,client)
    b.fault=lambda point:(_ for _ in ()).throw(RuntimeError('crash')) if point=='old_disabled' else None
    with pytest.raises(RuntimeError):b.run()
    for key in set(client.docs)-old:client.chunks[key[1]][0]['content']+=' broken'
    b.fault=lambda point:None;native=client.request;done=False
    def freeze_on_old_query(method,path,body=None,content_type=None):
        nonlocal done
        value=native(method,path,body,content_type)
        if path=='/retrieval' and not done and set(body.get('document_ids',[])) & {did for _,did in old}:
            done=True;freeze(b)
        return value
    client.request=freeze_on_old_query
    with pytest.raises(ConnectionError):b.run()
    assert done
    assert b.ledger.active(b.identity['source_id'],b.code)['state']=='frozen_pending'
    assert safety(b,a.digest)['state']=='pending'
    assert b.run()['state']=='withdrawn'
    assert b.ledger.active(b.identity['source_id'],b.code) is None
    assert safety(b,a.digest)['state']=='disabled'
