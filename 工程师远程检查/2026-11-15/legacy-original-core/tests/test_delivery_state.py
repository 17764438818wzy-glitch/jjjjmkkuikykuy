import json
import uuid
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import threading

import pytest

from paperflow.config import Config
from paperflow.handoff import PackageHandoff
from paperflow.handoff_store import canonical
from paperflow.package import load,save,sha
from test_control import manager, start_worker
from test_winjob import wait_until


@pytest.fixture
def delivery_case(manager,tmp_path,package_factory):
    source=tmp_path/'original.txt';source.write_text('original')
    task=manager.store.register_source(source,sha(source))
    material,_,catalog=package_factory(task_id=task['task_id'],source_sha=task['source_sha256'])
    for name in ('profile.json','tree.json','assets.json','chunks.json','review.json','corrections.json'):
        value=load(material/name);value['source_id']=task['source_id'];save(material/name,value)
    (material/'manifest.json').unlink();(material/'READY.json').unlink()
    profile=load(material/'profile.json');profile['routing_candidates'].append({'code':'120202','name':'B'});save(material/'profile.json',profile)
    catalog['majors'].append(dict(code='120202',name='B',paper_dataset_id='paper-two',image_dataset_id='images-two'))
    config=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),catalog_path=str(tmp_path/'catalog.json'),source_roots=[],rag_url='http://127.0.0.1:18088')
    config.prepare();save(config.catalog_path,catalog)
    handoff=PackageHandoff(manager.store,config.handoff_root)
    intent=handoff.prepare(task['task_id'],task,material,catalog,mapping_version=sha(config.catalog_path))
    result=handoff.resume(task['task_id'],intent['input_fingerprint'])
    from paperflow.delivery_control import DeliveryCommits
    commits=DeliveryCommits(manager,config)
    return manager,config,task,result['manifest_sha256'],commits,material,catalog


def owner(case,tmp_path,code='120201'):
    m,c,t,d,commits,*_=case
    identity=commits.ledger.context(d,code)['identity']
    key=canonical([t['source_id'],code]);scope='unit:'+canonical(['target',key])
    m.change_admission_state(scope,uuid.uuid4().hex,dict(expected_generation=identity['generation']))
    # Keep the real Job/IPC worker idle; invoke its manager-side admission directly.
    from test_control import WORKER
    import sys
    directory=tmp_path/('idle-'+uuid.uuid4().hex);directory.mkdir()
    ex=m.launch('delivery',key,[sys.executable,'-c',WORKER,str(directory),'local'],cwd=Path.cwd())
    wait_until(lambda:(directory/'ready').exists())
    return ex,identity


def begin(m,ex,identity,operation='save',kind='local',**extra):
    resource=dict(identity=identity,operation=operation,**extra)
    ticket=m.request_action(ex.execution_id,uuid.uuid4().hex,kind,resource,identity['generation'],60)['ticket']
    snapshot=m.check_begin(ex.execution_id,ticket);m.grant_begin(ex.execution_id,ticket,snapshot)
    return ticket


def test_identity_checks_manifest_not_only_generation(delivery_case,tmp_path):
    m,c,t,d,commits,*_=delivery_case;ex,identity=owner(delivery_case,tmp_path)
    with pytest.raises(ValueError,match='identity|expected|retained'):
        begin(m,ex,dict(identity,manifest_sha256='f'*64))
    source=Path(t['source_path']);source.write_text('new source')
    new=m.store.register_source(source,sha(source))
    assert new['task_id']!=t['task_id']
    with pytest.raises(ValueError,match='source|identity'):
        begin(m,ex,identity)


def test_one_specialty_cannot_borrow_another_identity(delivery_case,tmp_path):
    m,c,t,d,commits,*_=delivery_case;ex,identity=owner(delivery_case,tmp_path)
    other=commits.ledger.context(d,'120202')['identity']
    with pytest.raises(ValueError,match='ownership'):
        begin(m,ex,other)


def test_commit_is_qualified_again_after_sql_wait(delivery_case,tmp_path):
    m,c,t,d,commits,*_=delivery_case;ex,identity=owner(delivery_case,tmp_path)
    begin(m,ex,identity)
    descriptor=commits.stage(ex.execution_id,dict(identity=identity,state='preparing',receipts={}))
    started=threading.Event()
    original=commits.ledger.save_sql
    def save_sql(*args):started.set();return original(*args)
    commits.ledger.save_sql=save_sql
    with m.store.db() as writer,ThreadPoolExecutor(1) as pool:
        writer.execute('BEGIN IMMEDIATE')
        future=pool.submit(commits.handle,ex.execution_id,'save',descriptor)
        wait_until(lambda:ex.execution_id in m.publications)
        m.pause('delivery commit DB wait');writer.commit()
        with pytest.raises(ValueError):future.result(timeout=5)
    assert commits.ledger.progress(d,'120201') is None


def test_final_pause_prevents_save_and_stale_epoch_cannot_commit(delivery_case,tmp_path,monkeypatch):
    m,c,t,d,commits,*_=delivery_case;ex,identity=owner(delivery_case,tmp_path)
    begin(m,ex,identity)
    descriptor=commits.stage(ex.execution_id,dict(identity=identity,state='preparing',receipts={}))
    original=commits.ledger.save_sql
    def stopped(*args):
        result=original(*args);m.pause('after SQL before gate');return result
    monkeypatch.setattr(commits.ledger,'save_sql',stopped)
    with pytest.raises(ValueError):commits.handle(ex.execution_id,'save',descriptor)
    assert commits.ledger.progress(d,'120201') is None


def test_receipt_cannot_reference_another_dataset(delivery_case,tmp_path):
    m,c,t,d,commits,*_=delivery_case;ex,identity=owner(delivery_case,tmp_path)
    begin(m,ex,identity)
    descriptor=commits.stage(ex.execution_id,dict(identity=identity,state='preparing',receipts={'120201':{'body':dict(dataset_id='foreign',document_id='d')}}))
    with pytest.raises(ValueError,match='dataset'):
        commits.handle(ex.execution_id,'save',descriptor)
