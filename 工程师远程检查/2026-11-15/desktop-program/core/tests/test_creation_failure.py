"""Only a bound, verified failed create may release an unknown-create barrier."""
from contextlib import nullcontext
import hashlib
import json
import os

import pytest

from paperflow.remote_gateway import Gateway
from test_remote_gateway import header


def fixture(tmp_path, monkeypatch):
    gateway=Gateway(tmp_path,{'image':header()['image']})
    if os.name=='nt':gateway.locked=lambda *args,**kwargs:nullcontext()
    h=header();target=gateway.validate(h)/h['job_id'];target.mkdir(parents=True)
    identity={k:h[k] for k in ('namespace','execution_id','epoch','job_id')}
    (target/'intent.json').write_text(json.dumps(identity))
    (target/'creation.json').write_text(json.dumps(dict(state='unknown',at=100)))
    (target/'started.json').write_text(json.dumps(dict(name=gateway.name(h),image=h['image'],at=92)))
    (target.parent/'closed.json').write_text(json.dumps(dict(epoch=h['epoch'])))
    proof=dict(schema='paperflow-create-failure/1',identity=dict(h,deployment_id=None),
        container=gateway.name(h),creation_sha256=hashlib.sha256((target/'creation.json').read_bytes()).hexdigest(),
        verified=dict(boot_id='e'*32,creator_absent=True,inventory_empty=True),
        terminal=dict(_SYSTEMD_UNIT='docker.service',_EXE='/usr/bin/dockerd',_UID='0',_BOOT_ID='e'*32,_PID='123',__CURSOR='journal-fixture',
            __REALTIME_TIMESTAMP='101000000',MESSAGE='method=POST request-url="/v1.56/containers/create?name='+gateway.name(h)+'" status=499 msg="request cancelled by client" error="failed to lookup digest: Canceled: context canceled"'))
    monkeypatch.setattr(gateway,'inspect',lambda _name:None)
    monkeypatch.setattr(gateway,'inventory',lambda _h:[])
    return gateway,h,target,proof


def test_failed_creation_uses_actual_exit_flow_and_preserves_unknown_marker(tmp_path,monkeypatch):
    gateway,h,target,proof=fixture(tmp_path,monkeypatch)
    before=(target/'creation.json').read_bytes()
    output=target/'data/output';output.mkdir(parents=True);(output/'round1.json').write_bytes(b'preserve me')
    (target/'creation-failure.json').write_text(json.dumps(proof))
    result=gateway.reap_execution(h)
    assert result['all_exited'] and result['jobs'][0]['creation_failure']
    assert (target/'creation.json').read_bytes()==before
    assert (output/'round1.json').read_bytes()==b'preserve me'
    with pytest.raises(ValueError,match='closed'):gateway.check_open(h)


@pytest.mark.parametrize('damage',[
    'missing','identity','image','digest','timestamp','status','url','nonterminal','creating','execution_open','container_exists','creator_unknown'])
def test_incomplete_or_unbound_failure_evidence_keeps_the_barrier(tmp_path,monkeypatch,damage):
    gateway,h,target,proof=fixture(tmp_path,monkeypatch)
    if damage=='identity':proof['identity']['epoch']='f'*32
    elif damage=='image':proof['identity']['image']='other'
    elif damage=='digest':proof['creation_sha256']='0'*64
    elif damage=='timestamp':proof['terminal']['__REALTIME_TIMESTAMP']='99000000'
    elif damage=='status':proof['terminal']['MESSAGE']=proof['terminal']['MESSAGE'].replace('status=499','status=200')
    elif damage=='url':proof['terminal']['MESSAGE']=proof['terminal']['MESSAGE'].replace(h['job_id'],'f'*32)
    elif damage=='nonterminal':proof['terminal']['MESSAGE']=proof['terminal']['MESSAGE'].replace('request cancelled by client','request started')
    elif damage=='creating':(target/'creation.json').write_text(json.dumps(dict(state='creating',at=100)))
    elif damage=='execution_open':(target.parent/'closed.json').unlink()
    elif damage=='container_exists':monkeypatch.setattr(gateway,'inspect',lambda _name:dict(alive=True))
    elif damage=='creator_unknown':proof['verified']['creator_absent']=False
    if damage!='missing':(target/'creation-failure.json').write_text(json.dumps(proof))
    with pytest.raises((ConnectionError,ValueError)):gateway.reap_job(h)
    assert not (target/'exit.json').exists()
