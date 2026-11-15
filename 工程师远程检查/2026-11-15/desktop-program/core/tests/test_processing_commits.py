from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import threading
import time
import uuid

import pytest

from paperflow.config import Config
from paperflow.handoff import PackageHandoff
from paperflow.handoff_store import canonical
from paperflow.package import sha, load, save
from test_control import manager, start_worker
from test_winjob import wait_until


def admission(manager, execution, operation):
    ticket = manager.request_action(execution.execution_id, uuid.uuid4().hex, 'local',
                                    {'operation': operation}, 0, 60)['ticket']
    snapshot = manager.check_begin(execution.execution_id, ticket)
    manager.grant_begin(execution.execution_id, ticket, snapshot)
    return ticket


def make_handoff(manager, tmp_path, package_factory):
    source = tmp_path/'original.txt';source.write_text('original')
    task = manager.store.register_source(source, sha(source))
    material, _, catalog = package_factory(task_id=task['task_id'], source_sha=task['source_sha256'])
    # Fixture package uses a literal source id; re-seal as a legitimate unsealed input.
    for name in ('profile.json','tree.json','assets.json','chunks.json'):
        value=load(material/name);value['source_id']=task['source_id'];save(material/name,value)
    (material/'manifest.json').unlink();(material/'READY.json').unlink()
    profile=load(material/'profile.json')
    profile['routing_candidates'].append({'code':'120202','name':'市场营销'})
    save(material/'profile.json',profile)
    catalog['majors'].append({'code':'120202','name':'市场营销','paper_dataset_id':'paper-two'})
    c=Config(home=str(tmp_path/'runtime'),handoff_root=str(tmp_path/'handoff'),catalog_path=str(tmp_path/'catalog.json'))
    save(c.catalog_path,catalog)
    h=PackageHandoff(manager.store,c.handoff_root)
    intent=h.prepare(task['task_id'],task,material,catalog,mapping_version=sha(c.catalog_path))
    h.publish(task['task_id'],intent['input_fingerprint'])
    execution,_=start_worker(manager,tmp_path,name=task['task_id'])
    admission(manager,execution,'handoff_commit')
    return c,task,h,intent,execution


def test_handoff_scope_versions_commit_atomically_and_replay(manager,tmp_path,package_factory):
    from paperflow.processing_control import ProcessingCommits
    c,task,h,intent,execution=make_handoff(manager,tmp_path,package_factory)
    commits=ProcessingCommits(manager,c)
    receipt=commits.handoff(execution.execution_id,intent['input_fingerprint'])
    assert len(receipt['targets'])==2
    with manager.store.db() as db:
        rows=db.execute('SELECT generation FROM target_versions').fetchall()
        scopes=db.execute("SELECT state FROM admission_scopes WHERE scope LIKE 'unit:%'").fetchall()
    assert [r['generation'] for r in rows]==[1,1]
    assert sorted(load_state['expected_generation'] for load_state in map(__import__('json').loads,[r['state'] for r in scopes]))==[0,1,1]
    manager.record_result(execution.execution_id,execution.active_action['action_id'],manager.epoch,'complete',{})
    admission(manager,execution,'handoff_commit')
    assert commits.handoff(execution.execution_id,intent['input_fingerprint'])==receipt
    assert len(h.ledger.deliveries(receipt['manifest_sha256']))==2
    assert all(v['generation']==1 for v in h.ledger.expectations(task['task_id'],['120201','120202']).values())


def test_pause_while_commit_waits_for_database_rolls_back_all_targets(manager,tmp_path,package_factory):
    from paperflow.processing_control import ProcessingCommits
    c,task,h,intent,execution=make_handoff(manager,tmp_path,package_factory)
    commits=ProcessingCommits(manager,c)
    # Ensure scopes first so this fault reaches the handoff transaction rather than schema setup.
    commits.target_scopes(intent)
    with manager.store.db() as writer, ThreadPoolExecutor(1) as pool:
        writer.execute('BEGIN IMMEDIATE')
        future=pool.submit(commits.handoff,execution.execution_id,intent['input_fingerprint'])
        wait_until(lambda:any(e['event']=='scopes_invalidated' for e in manager.events))
        start=time.monotonic();manager.pause('blocked handoff')
        assert time.monotonic()-start<1
        writer.commit()
        with pytest.raises(ValueError):future.result(timeout=5)
    assert not manager.ledger.rows('SELECT * FROM delivery_packages')
    assert not manager.ledger.rows('SELECT * FROM target_versions')
    assert all(not manager.scopes[s]['ready'] for s in commits.target_scopes(intent).values())


def test_pause_after_sql_before_final_qualification_rolls_back(manager,tmp_path,package_factory,monkeypatch):
    from paperflow.processing_control import ProcessingCommits
    c,task,h,intent,execution=make_handoff(manager,tmp_path,package_factory)
    commits=ProcessingCommits(manager,c)
    original=commits.handoff_service.ledger.register_sql
    def paused_after_sql(*args,**kwargs):
        result=original(*args,**kwargs)
        manager.pause('last qualification must notice')
        return result
    monkeypatch.setattr(commits.handoff_service.ledger,'register_sql',paused_after_sql)
    with pytest.raises(ValueError):commits.handoff(execution.execution_id,intent['input_fingerprint'])
    assert not manager.ledger.rows('SELECT * FROM specialty_tasks')
    assert not manager.ledger.rows('SELECT * FROM target_versions')


def test_stale_epoch_and_disconnected_worker_cannot_commit(manager,tmp_path,package_factory):
    from paperflow.processing_control import ProcessingCommits
    c,task,h,intent,execution=make_handoff(manager,tmp_path,package_factory)
    commits=ProcessingCommits(manager,c)
    execution.peer.close()
    with pytest.raises(ValueError):commits.handoff(execution.execution_id,intent['input_fingerprint'])
    assert not manager.ledger.rows('SELECT * FROM delivery_packages')


def test_frozen_target_is_registered_without_advancing_its_expected_version(manager,tmp_path,package_factory):
    from paperflow.processing_control import ProcessingCommits
    c,task,h,intent,execution=make_handoff(manager,tmp_path,package_factory)
    commits=ProcessingCommits(manager,c)
    scopes=commits.target_scopes(intent)
    manager.change_admission_state(scopes['120201'],'freeze-before-handoff',{'frozen':True})
    result=commits.handoff(execution.execution_id,intent['input_fingerprint'])
    assert {row['code']:row['status'] for row in result['targets']}=={'120201':'frozen','120202':'pending'}
    assert h.ledger.expectations(task['task_id'],['120201'])['120201']['generation']==0
