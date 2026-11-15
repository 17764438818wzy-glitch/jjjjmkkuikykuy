"""A timed-out create can be reconciled only against a bound actual container."""
from collections import defaultdict
from contextlib import contextmanager
import hashlib
import json
import os
import subprocess
import threading

import pytest

from paperflow.remote_gateway import Gateway


def setup(tmp_path,monkeypatch,*,serialized=True):
    h=dict(namespace='a'*64,execution_id='b'*32,epoch='c'*32,job_id='d'*32,
        image='sha256:'+'1'*64,deployment_id='fixture-deployment')
    g=Gateway(tmp_path,dict(image=h['image'],deployment_id=h['deployment_id']))
    if os.name=='nt':
        locks=defaultdict(threading.Lock)
        @contextmanager
        def locked(path=None,blocking=True):
            lock=locks[str(path or tmp_path/'.lock')]
            if not lock.acquire(blocking=blocking):raise BlockingIOError()
            try:yield
            finally:lock.release()
        g.locked=locked
    target=g.validate(h)/h['job_id'];target.mkdir(parents=True)
    intent={k:h[k] for k in ('namespace','execution_id','epoch','job_id')}
    started=dict(name='paperflow-harness-aaaaaaaaaaaa-'+'d'*32,image=h['image'],at=92)
    creation=dict(state='unknown',at=100)
    if serialized:started['protocol']=creation['protocol']='serialized-start/1'
    for name,value in (('intent',intent),('started',started),('creation',creation)):
        (target/(name+'.json')).write_text(json.dumps(value),encoding='utf-8')
    output=target/'data/output';output.mkdir(parents=True);(output/'round1.json').write_bytes(b'EXISTING_LLM_RESULT')
    state={'container':dict(Id='2'*64,Name='/'+started['name'],Image=h['image'],
        Config=dict(Labels={'paperflow.'+k:intent[k] for k in intent}),
        State=dict(Status='created',Running=False,Pid=0)),'removed':[]}
    def inspect(name):
        value=state['container']
        return value if value and name in (value['Id'],value['Name'].lstrip('/')) else None
    def run(args,**_kwargs):
        assert args[:3]==['docker','rm','--force']
        assert args[3]=='2'*64,'reconciliation must remove the observed immutable ID'
        state['removed'].append(args[3]);state['container']=None
        return subprocess.CompletedProcess(args,0)
    monkeypatch.setattr(g,'inspect',inspect);monkeypatch.setattr(g,'inventory',lambda _h:['2'*64] if state['container'] else [])
    monkeypatch.setattr(subprocess,'run',run)
    return g,h,target,state


def test_owned_created_container_reconciles_without_erasing_the_unknown_marker(tmp_path,monkeypatch):
    g,h,target,state=setup(tmp_path,monkeypatch);original=(target/'creation.json').read_bytes()
    proof=g.reap_execution(h)
    assert proof['all_exited'] and proof['jobs'][0]['creation_observed']
    assert state['removed']==['2'*64] and state['container'] is None
    assert (target/'creation.json').read_bytes()==original
    assert (target/'data/output/round1.json').read_bytes()==b'EXISTING_LLM_RESULT'
    observed=json.loads((target/'creation-observed.json').read_text(encoding='utf-8'))
    assert observed['container_id']=='2'*64 and observed['creation_sha256']==hashlib.sha256(original).hexdigest()
    assert g.reap_execution(h)['all_exited'],'a second owner must reuse the bound observation'


def test_absence_after_a_timeout_is_still_unconfirmed(tmp_path,monkeypatch):
    g,h,target,state=setup(tmp_path,monkeypatch);state['container']=None
    with pytest.raises(ConnectionError):g.reap_execution(h)
    assert not (target/'exit.json').exists() and not (target/'creation-observed.json').exists()


@pytest.mark.parametrize('damage',['image','labels','id','name','state','intent','started'])
def test_unknown_observation_rejects_wrong_or_incomplete_identity(tmp_path,monkeypatch,damage):
    g,h,target,state=setup(tmp_path,monkeypatch)
    if damage=='image':state['container']['Image']='sha256:'+'9'*64
    elif damage=='labels':state['container']['Config']['Labels']['paperflow.epoch']='9'*32
    elif damage=='id':state['container']['Id']='2'*12
    elif damage=='name':state['container']['Name']='/different-container'
    elif damage=='state':state['container']['State']['Status']='removing'
    elif damage=='intent':(target/'intent.json').write_text(json.dumps(dict(namespace='f'*64)))
    elif damage=='started':(target/'started.json').write_text(json.dumps(dict(name='other',image=h['image'],at=92)))
    with pytest.raises((ConnectionError,ValueError,KeyError)):g.reap_execution(h)
    assert not state['removed'] and not (target/'exit.json').exists()


def test_a_create_still_in_progress_cannot_be_proved_exited(tmp_path,monkeypatch):
    g,h,target,state=setup(tmp_path,monkeypatch)
    (target/'creation.json').write_text(json.dumps(dict(state='creating',at=100,protocol='serialized-start/1')))
    with pytest.raises(ConnectionError):g.reap_execution(h)
    assert not state['removed']


def test_legacy_creator_requires_a_separate_bound_absence_audit(tmp_path,monkeypatch):
    g,h,target,state=setup(tmp_path,monkeypatch,serialized=False)
    with pytest.raises(ConnectionError):g.reap_execution(h)
    assert not state['removed'] and not (target/'creation-observed.json').exists()


def audit(h,target):
    return dict(schema='paperflow-legacy-creator-audit/1',identity=h,container_id='2'*64,
        creation_sha256=hashlib.sha256((target/'creation.json').read_bytes()).hexdigest(),
        started_sha256=hashlib.sha256((target/'started.json').read_bytes()).hexdigest(),
        intent_sha256=hashlib.sha256((target/'intent.json').read_bytes()).hexdigest(),
        checked_at=101,boot_id='e'*32,daemon_git_commit='3ce5872',
        run_gateway_absent=True,creator_absent=True,local_tree_exited=True)


def test_legacy_audit_allows_actual_reaping_without_rewriting_any_results(tmp_path,monkeypatch):
    g,h,target,state=setup(tmp_path,monkeypatch,serialized=False);g.boot_id=lambda:'e'*32
    (target.parent/'closed.json').write_text(json.dumps(dict(epoch=h['epoch'])))
    (target/'creator-audit.json').write_text(json.dumps(audit(h,target)),encoding='utf-8')
    assert g.reap_execution(h)['all_exited']
    assert state['removed']==['2'*64]
    assert json.loads((target/'creation.json').read_text())['state']=='unknown'


@pytest.mark.parametrize('damage',['identity','digest','container','old_gateway','creator','local_tree','boot','time'])
def test_legacy_audit_cannot_unlock_a_different_or_unverified_creation(tmp_path,monkeypatch,damage):
    g,h,target,state=setup(tmp_path,monkeypatch,serialized=False);g.boot_id=lambda:'e'*32
    (target.parent/'closed.json').write_text(json.dumps(dict(epoch=h['epoch'])))
    value=audit(dict(h),target)
    if damage=='identity':value['identity']['epoch']='f'*32
    elif damage=='digest':value['creation_sha256']='0'*64
    elif damage=='container':value['container_id']='9'*64
    elif damage=='old_gateway':value['run_gateway_absent']=False
    elif damage=='creator':value['creator_absent']=False
    elif damage=='local_tree':value['local_tree_exited']=False
    elif damage=='boot':value['boot_id']='f'*32
    elif damage=='time':value['checked_at']=99
    (target/'creator-audit.json').write_text(json.dumps(value))
    with pytest.raises((ValueError,ConnectionError)):g.reap_execution(h)
    assert not state['removed'] and not (target/'exit.json').exists()


def test_changed_container_after_observation_is_not_removed(tmp_path,monkeypatch):
    g,h,target,state=setup(tmp_path,monkeypatch)
    original=subprocess.run
    def unavailable(args,**kwargs):raise ConnectionError('synthetic lost removal response')
    monkeypatch.setattr(subprocess,'run',unavailable)
    with pytest.raises(ConnectionError):g.reap_execution(h)
    assert (target/'creation-observed.json').is_file()
    state['container']['Id']='3'*64;monkeypatch.setattr(subprocess,'run',original)
    with pytest.raises((ValueError,ConnectionError)):g.reap_execution(h)
    assert not state['removed'] and not (target/'exit.json').exists()
