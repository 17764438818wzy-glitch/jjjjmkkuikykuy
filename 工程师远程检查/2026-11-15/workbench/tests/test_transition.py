import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from paperflow.config import Config
from paperflow.package import load, save, sha
from paperflow.store import Store


def old_case(tmp_path, package_factory):
    old=tmp_path/'old';old.mkdir();stop=old/'STOP';stop.write_text('paused')
    source=old/'source.txt';source.write_text('original')
    store=Store(old/'state.sqlite3');task=store.register_source(source,sha(source))
    material,identity,catalog=package_factory(task_id=task['task_id'],source_sha=task['source_sha256'])
    # Preserve the real source identity when adopting a sealed historical package.
    for name in ('profile.json','tree.json','assets.json','chunks.json','review.json','corrections.json'):
        value=load(material/name);value['source_id']=task['source_id'];save(material/name,value)
    from paperflow.package import seal_package
    seal_package(material,task,1)
    import shutil
    shutil.copytree(material,old/'package');material=old/'package'
    task=store.update(task['task_id'],stage='ingest_target',package_dir=str(material),manifest_sha256=sha(material/'manifest.json'),repair_count=1)
    save(old/'private/delivery-corrections'/(task['task_id']+'.json'),dict(source_sha256=task['source_sha256'],count=2))
    checkpoint=old/'checkpoints/graph.sqlite3';checkpoint.parent.mkdir()
    with sqlite3.connect(checkpoint) as c:
        c.execute('CREATE TABLE test(value)');c.execute("INSERT INTO test VALUES('do not resume old graph')")
    return old,task,catalog


def capture(tmp_path, package_factory):
    from paperflow.transition import TransitionGuard,snapshot_legacy,LegacyImporter
    old,task,catalog=old_case(tmp_path,package_factory)
    with TransitionGuard([old/'controller.lock'],[old/'STOP']):
        snap=snapshot_legacy(old,tmp_path/'snapshot',[task['task_id']])
    cfg=Config(home=str(tmp_path/'new'),handoff_root=str(tmp_path/'handoff'),source_roots=[])
    cfg.prepare();store=Store(cfg.root/'state.sqlite3')
    importer=LegacyImporter(store,cfg)
    return old,task,catalog,snap,cfg,store,importer


def test_snapshot_and_import_replay_preserve_identity_budget_and_pending(tmp_path,package_factory):
    old,task,catalog,snap,cfg,store,imp=capture(tmp_path,package_factory)
    before=sha(old/'state.sqlite3')
    result=imp.import_task(snap,task['task_id'],catalog)
    assert result==imp.import_task(snap,task['task_id'],catalog)
    adopted=store.get(task['task_id'])
    assert adopted['source_id']==task['source_id'] and adopted['kind']=='historical'
    assert adopted['stage']=='handed_off' and not adopted.get('control_request')
    assert imp.repairs.used(task)==2
    assert imp.ledger.active(task['source_id'],'120201') is None
    with store.db() as c:
        assert c.execute('SELECT state FROM legacy_targets').fetchone()[0]=='pending_reconcile'
        with pytest.raises(ValueError,match='legacy'):
            imp.ledger.check_identity(c,imp.ledger.context_sql(c,result['manifest_sha256'],'120201')['identity'])
    assert sha(old/'state.sqlite3')==before
    with sqlite3.connect(Path(snap)/'checkpoints/graph.sqlite3') as c:
        assert c.execute('SELECT * FROM test').fetchone()[0]=='do not resume old graph'


def test_changed_snapshot_rejected(tmp_path,package_factory):
    _,task,catalog,snap,_,_,imp=capture(tmp_path,package_factory)
    with (Path(snap)/'state.sqlite3').open('ab') as f:f.write(b'changed')
    with pytest.raises(ValueError,match='snapshot'):imp.import_task(snap,task['task_id'],catalog)


def test_missing_package_keeps_history_and_does_not_enqueue_graph(tmp_path,package_factory):
    from paperflow.transition import TransitionGuard,snapshot_legacy,LegacyImporter
    old,task,catalog=old_case(tmp_path,package_factory)
    Store(old/'state.sqlite3').update(task['task_id'],package_dir=str(old/'missing'))
    with TransitionGuard([old/'controller.lock'],[old/'STOP']):snap=snapshot_legacy(old,tmp_path/'snap',[task['task_id']])
    config=Config(home=str(tmp_path/'new'),handoff_root=str(tmp_path/'handoff'));config.prepare()
    store=Store(config.root/'state.sqlite3');imp=LegacyImporter(store,config)
    assert imp.import_task(snap,task['task_id'],catalog)['state']=='held'
    assert store.get(task['task_id'])['kind']=='historical'
    assert not imp.ledger.deliveries(task['manifest_sha256'])


def test_snapshot_requires_held_old_lock_and_stop(tmp_path):
    from paperflow.transition import TransitionGuard,snapshot_legacy
    old=tmp_path/'old';old.mkdir();Store(old/'state.sqlite3')
    with pytest.raises(ValueError):snapshot_legacy(old,tmp_path/'snap',['x'])
    with pytest.raises(ValueError):
        with TransitionGuard([old/'controller.lock'],[old/'STOP']):pass


def test_old_and_new_locks_exclude_both_directions(tmp_path):
    from paperflow.transition import TransitionGuard
    from paperflow.instance_lock import InstanceLock
    old=tmp_path/'controller.lock';dispatch=tmp_path/'worker.lock';stop=tmp_path/'STOP';stop.write_text('paused')
    program='from paperflow.instance_lock import InstanceLock; import sys;\nwith InstanceLock(sys.argv[1]): print("acquired")'
    with TransitionGuard([old,dispatch],[stop]):
        for p in (old,dispatch):
            run=subprocess.run([sys.executable,'-B','-c',program,str(p)],capture_output=True,text=True)
            assert run.returncode!=0 and 'acquired' not in run.stdout
    with InstanceLock(old):
        with pytest.raises(RuntimeError):
            with TransitionGuard([old,dispatch],[stop]):pass
    with InstanceLock(dispatch):pass # Partial acquisition released on failure.


def test_live_old_process_identity_rejects_even_free_lock(tmp_path):
    from paperflow.transition import TransitionGuard
    from paperflow.winjob import process_identity
    import os
    stop=tmp_path/'STOP';stop.write_text('paused')
    with pytest.raises(ValueError,match='alive'):
        with TransitionGuard([tmp_path/'lock'],[stop],identities=[process_identity(os.getpid())]):pass


def test_incompatible_processing_creates_separate_new_thread_preserving_budget(tmp_path,package_factory):
    from paperflow.transition import TransitionGuard,snapshot_legacy,LegacyImporter
    old,task,catalog=old_case(tmp_path,package_factory)
    Store(old/'state.sqlite3').update(task['task_id'],stage='round3_chunk',package_dir=None,repair_count=2)
    with TransitionGuard([old/'controller.lock'],[old/'STOP']):snap=snapshot_legacy(old,tmp_path/'snap',[task['task_id']])
    config=Config(home=str(tmp_path/'new'),handoff_root=str(tmp_path/'handoff'));config.prepare()
    store=Store(config.root/'state.sqlite3');imp=LegacyImporter(store,config)
    result=imp.import_task(snap,task['task_id'],catalog)
    assert result['processing_restart_allowed'] and store.get(task['task_id'])['kind']=='historical'
    restarted=imp.restart_incompatible_processing(result['snapshot_id'],task['task_id'])
    assert restarted['stage']=='round1' and restarted['thread_id'].startswith('restart:')
    assert restarted['source_id']==task['source_id'] and imp.repairs.used(restarted)==2
    assert not (config.root/'checkpoints/graph.sqlite3').exists()
    assert imp.restart_incompatible_processing(result['snapshot_id'],task['task_id'])['thread_id']==restarted['thread_id']


def test_same_source_collision_cannot_override_existing_new_runtime(tmp_path,package_factory):
    _,task,catalog,snap,_,store,imp=capture(tmp_path,package_factory)
    current=store.register_source(task['source_path'],task['source_sha256'])
    with pytest.raises(ValueError,match='existing task'):imp.import_task(snap,task['task_id'],catalog)
    assert store.get(current['task_id'])['kind']=='new'


def test_dispatcher_receipt_with_missing_package_archived_without_tasks(tmp_path):
    from paperflow.transition import archive_dispatcher_receipt
    p=tmp_path/'receipt.json';store=Store(tmp_path/'state.sqlite3')
    value=dict(schema='rag-mid-ingestion-receipt/1.0',task_id='legacy',source_id='oldsource',source_sha256='a'*64,
        revision='r01',manifest_sha256='b'*64,status='parse_complete',targets={'120201':dict(document_id='doc')})
    save(p,value);r=archive_dispatcher_receipt(store,p,tmp_path/'archive')
    assert r==archive_dispatcher_receipt(store,p,tmp_path/'archive') and not store.all_tasks()
    value['status']='changed';save(p,value)
    with pytest.raises(ValueError,match='identity conflict'):archive_dispatcher_receipt(store,p,tmp_path/'archive')


def test_transition_shutdown_failure_retains_old_locks(tmp_path):
    from paperflow.transition import TransitionGuard,TransitionSession
    from paperflow.instance_lock import InstanceLock
    stop=tmp_path/'STOP';stop.write_text('pause');lock=tmp_path/'controller.lock'
    class Pending:
        def start(self):pass
        def shutdown(self,timeout):raise TimeoutError('tree unconfirmed')
    guard=TransitionGuard([lock],[stop]);session=TransitionSession(Pending(),guard);session.__enter__()
    try:
        with pytest.raises(TimeoutError):session.__exit__()
        with pytest.raises(RuntimeError):
            with InstanceLock(lock):pass
    finally:guard.__exit__()


def test_pause_marker_changed_before_snapshot_is_rejected(tmp_path):
    from paperflow.transition import TransitionGuard,snapshot_legacy
    stop=tmp_path/'STOP';stop.write_text('pause')
    with TransitionGuard([tmp_path/'controller.lock'],[stop]):
        stop.write_text('changed')
        with pytest.raises(ValueError,match='changed'):snapshot_legacy(tmp_path,tmp_path.parent/'snap',['task'])
