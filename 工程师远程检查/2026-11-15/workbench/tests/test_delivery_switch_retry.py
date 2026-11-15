"""Real admission contention must not turn a valid switch into a rejected version."""
import pytest

from paperflow.scheduling_policy import ActionDeferred
from test_control import manager
from test_delivery_state import delivery_case, begin
from test_delivery_flow import IndexedRag, flow, advance, release


@pytest.mark.parametrize('boundary', ['retrieval', 'resume_verify'])
def test_switch_yields_resource_then_resumes_without_withdrawal(delivery_case, tmp_path, boundary):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client)
    other=flow(delivery_case,tmp_path,client,'120202')
    m=a.actions.m;m.resource_limits['rag']=1
    held=[]
    def occupy(point):
        if point=='enabled':
            held.append(begin(m,other.actions.ex,other.identity,kind='rag_read',operation='http',
                method='GET',endpoint='/pf-stage4/identity',object_manifest=other.digest))
    if boundary=='resume_verify':
        a.fault=lambda point:(_ for _ in ()).throw(RuntimeError('exit before retrieval')) if point=='enabled' else None
        with pytest.raises(RuntimeError,match='exit before retrieval'):a.run()
        occupy('enabled');a.fault=lambda point:None
    else:a.fault=occupy
    try:
        with pytest.raises(ActionDeferred,match='resource_busy'):a.run()
    finally:
        for action in held:m.record_result(other.actions.execution_id,action,m.epoch,'complete',{})
    assert a.ledger.switching(a.identity['source_id'],a.code)['phase']=='enabled'
    assert a.ledger.active(a.identity['source_id'],a.code) is None
    assert not a.ledger.unresolved(a.identity['source_id'],a.code)
    writes=client.writes;a.fault=lambda point:None
    assert a.run()['state']=='active'
    assert client.writes==writes


def test_read_outage_after_old_disabled_preserves_unconfirmed_state(delivery_case, tmp_path):
    from paperflow.legacy_readback import article_status
    client=IndexedRag();old=flow(delivery_case,tmp_path,client);old.run();release(old)
    old_ids=set(client.docs);a=flow(advance(delivery_case),tmp_path,client)
    a.fault=lambda point:(_ for _ in ()).throw(RuntimeError('exit after old disabled')) if point=='old_disabled' else None
    with pytest.raises(RuntimeError,match='exit after old disabled'):a.run()
    client.fail=lambda method,path,body:(_ for _ in ()).throw(TimeoutError('read outage')) if method=='GET' else None
    a.fault=lambda point:None
    with pytest.raises(TimeoutError,match='read outage'):a.run()
    assert a.ledger.switching(a.identity['source_id'],a.code)['phase']=='old_disabled'
    status=article_status(a.ledger.store,a.identity['task_id'],dict(closeout_pending=0))
    assert not next(t for t in status['targets'] if t['code']==a.code)['available']
    assert all(client.docs[k]['status']=='0' for k in old_ids)
    writes=client.writes;client.fail=None
    assert a.run()['state']=='active'
    assert client.writes==writes


@pytest.mark.parametrize('boundary', ['retrieval', 'resume_verify'])
def test_switch_read_outage_keeps_retryable_phase(delivery_case, tmp_path, boundary):
    client=IndexedRag();a=flow(delivery_case,tmp_path,client)
    def outage(method,path,body):
        if (boundary=='retrieval' and path=='/retrieval') or (boundary=='resume_verify' and method=='GET'):
            raise TimeoutError('temporary read outage')
    if boundary=='resume_verify':
        a.fault=lambda point:(_ for _ in ()).throw(RuntimeError('exit before retrieval')) if point=='enabled' else None
        with pytest.raises(RuntimeError,match='exit before retrieval'):a.run()
        a.fault=lambda point:None
    client.fail=outage
    with pytest.raises(TimeoutError,match='temporary read outage'):a.run()
    assert a.ledger.switching(a.identity['source_id'],a.code)['phase']=='enabled'
    assert a.ledger.active(a.identity['source_id'],a.code) is None
    writes=client.writes;client.fail=None
    assert a.run()['state']=='active'
    assert client.writes==writes
