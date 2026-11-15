import json
import time

import pytest

from paperflow.handoff_store import canonical
from test_control import manager
from test_delivery_state import delivery_case
from test_delivery_flow import IndexedRag,flow,release


def unknown(case,tmp_path):
    client=IndexedRag();first=flow(case,tmp_path,client);client.lose_chunk_response=True
    with pytest.raises(TimeoutError):first.run()
    action=first.ledger.unresolved(first.identity['source_id'],first.code)[0]
    release(first)
    second=flow(case,tmp_path,client)
    return client,second,action


def test_late_http_success_plus_complete_inventory_keeps_unknown_barrier(delivery_case,tmp_path):
    client,a,action=unknown(delivery_case,tmp_path)
    a.actions.m.record_result(action['execution_id'],action['action_id'],action['epoch'],'complete',
        dict(terminal=True,response_sha256='a'*64))
    writes=client.writes
    assert a.run()['state']=='blocked'
    assert a.ledger.unresolved(a.identity['source_id'],a.code)
    assert client.writes==writes


@pytest.mark.parametrize('mode',['unknown','changed','too_early','wrong_operation','clock_changed','valid'])
def test_bound_requires_matching_deployment_exit_silence_and_two_reads(delivery_case,tmp_path,mode):
    client,a,action=unknown(delivery_case,tmp_path)
    m=a.actions.m
    if mode!='unknown':
        m.delivery.boundary=dict(endpoint=client.base_url,configuration='fixture-v1' if mode!='changed' else 'old-config',
            end_to_end_seconds=60 if mode=='too_early' else .01,safety_margin=.01,visibility_seconds=.01,
            verified_end_to_end=True,evidence='bounded synthetic test backend',
            operations=['upload'] if mode=='wrong_operation' else ['chunk_add'])
    a.deployment_fingerprint='fixture-v1'
    if mode in ('valid','changed','wrong_operation','clock_changed'):
        # Fault clock injection, never used in deployed control.
        m.ledger._write(lambda c:c.execute('UPDATE control_actions SET deadline=? WHERE action_id=?',(time.time()-10,action['action_id'])))
        time.sleep(.03)
    if mode=='clock_changed':m.delivery.clock_anchor=(time.time()-5,time.monotonic())
    assert a.run()['state']==('active' if mode=='valid' else 'blocked')
    rows=m.ledger.rows('SELECT * FROM delivery_reconciliations')
    assert rows and all(json.loads(r['evidence_json'])['complete_inventory'] for r in rows)
    assert bool(a.ledger.unresolved(a.identity['source_id'],a.code))==(mode!='valid')


def test_late_write_between_reads_keeps_barrier(delivery_case,tmp_path):
    client,a,action=unknown(delivery_case,tmp_path)
    a.actions.m.record_result(action['execution_id'],action['action_id'],action['epoch'],'complete',dict(terminal=True,response_sha256='a'*64))
    a.deployment_fingerprint='fixture-v1'
    a.actions.m.delivery.boundary=dict(endpoint=client.base_url,configuration='fixture-v1',
        end_to_end_seconds=.01,safety_margin=.01,visibility_seconds=.01,
        verified_end_to_end=True,evidence='bounded synthetic backend',operations=['chunk_add'])
    time.sleep(.03)
    original=a.inventory;calls=0
    def later():
        nonlocal calls
        calls+=1
        if calls==2:
            next(iter(client.chunks.values()))[0]['content']+='late side effect'
        return original()
    a.inventory=later
    assert a.run()['state']=='blocked'
    assert a.ledger.unresolved(a.identity['source_id'],a.code)


def test_restart_cannot_use_old_epoch_wall_time_to_shorten_bound(delivery_case,tmp_path):
    from paperflow.control import Manager
    from paperflow.delivery_control import DeliveryCommits
    client,a,action=unknown(delivery_case,tmp_path);release(a)
    old=a.actions.m;old.shutdown()
    # Simulate a wall-clock jump while the manager was absent; monotonic elapsed is still short.
    def old_clock(c):
        c.execute('UPDATE control_actions SET deadline=? WHERE action_id=?',(time.time()-1000,action['action_id']))
        row=c.execute('SELECT exit_evidence FROM control_executions WHERE execution_id=?',(action['execution_id'],)).fetchone()
        evidence=json.loads(row['exit_evidence']);evidence['confirmed_at']=time.time()-1000
        c.execute('UPDATE control_executions SET exit_evidence=? WHERE execution_id=?',(canonical(evidence),action['execution_id']))
    old.ledger._write(old_clock)
    m=Manager(old.store,tmp_path/'restarted-manager').start();m.resume()
    try:
        c=a.config;commits=DeliveryCommits(m,c)
        case=(m,c,delivery_case[2],delivery_case[3],commits,*delivery_case[5:])
        b=flow(case,tmp_path,client);b.deployment_fingerprint='fixture-v1'
        commits.boundary=dict(endpoint=client.base_url,configuration='fixture-v1',end_to_end_seconds=10,
            safety_margin=.01,visibility_seconds=.01,verified_end_to_end=True,evidence='bounded backend',operations=['chunk_add'])
        assert b.run()['state']=='blocked'
        assert b.ledger.unresolved(b.identity['source_id'],b.code)
    finally:m.shutdown()


@pytest.mark.parametrize('fault',['first_read_before_silence','second_read_before_interval'])
def test_sql_wait_cannot_replace_remote_read_timing(delivery_case,tmp_path,fault):
    client,a,action=unknown(delivery_case,tmp_path);m=a.actions.m
    a.deployment_fingerprint='fixture-v1'
    m.delivery.boundary=dict(endpoint=client.base_url,configuration='fixture-v1',
        end_to_end_seconds=.5 if fault=='first_read_before_silence' else .01,safety_margin=.01,
        visibility_seconds=.01 if fault=='first_read_before_silence' else .5,
        verified_end_to_end=True,evidence='bounded backend',operations=['chunk_add'])
    m.ledger._write(lambda c:c.execute('UPDATE control_actions SET deadline=? WHERE action_id=?',(time.time()-10,action['action_id'])))
    time.sleep(.03)
    original=a.ledger.reconcile_sql;decisions=[]
    def wait_then_commit(*args,**kwargs):
        index=len(decisions)
        if index==(0 if fault=='first_read_before_silence' else 1):time.sleep(.7)
        result=original(*args,**kwargs);decisions.append(result.copy());return result
    a.ledger.reconcile_sql=wait_then_commit
    a.run()
    assert len(decisions)>=2 and decisions[1]['released'] is False
