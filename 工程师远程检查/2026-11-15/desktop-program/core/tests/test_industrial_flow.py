import json
from pathlib import Path
import subprocess
import sys
import threading
import time
import zipfile

from paperflow.package import load, save, sha


def test_legacy_doc_uses_existing_converter_with_private_temp(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from paperflow.reading import prepare_reading
    source = tmp_path/'原稿.doc'
    source.write_bytes(bytes.fromhex('d0cf11e0a1b11ae1') + b'\0'*504)
    monkeypatch.setenv('PAPERFLOW_OFFICE_TEMP_ROOT',str(tmp_path/'short-temps'))
    monkeypatch.setitem(sys.modules, 'engine', SimpleNamespace(
        convert_office=lambda *_: (_ for _ in ()).throw(AssertionError('desktop Office must not launch'))))
    def convert(command, **kwargs):
        target = Path(command[-1])/'source.docx';target.parent.mkdir()
        with zipfile.ZipFile(target, 'w') as archive:
            archive.writestr('word/document.xml', '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body><w:p><w:r><w:t>完整正文及研究方法</w:t></w:r></w:p></w:body></w:document>')
        return subprocess.CompletedProcess(command, 0, b'{"backend":"native"}', b'')
    monkeypatch.setattr(subprocess, 'run', convert)
    index = prepare_reading(source, tmp_path/'view')
    assert '完整正文及研究方法' in (tmp_path/'view/source.txt').read_text(encoding='utf-8')
    assert index['source_sha256'] == sha(source)
    assert index['reading_seconds'] >= index['conversion']['seconds'] >= 0
    assert index['conversion']['engine'] == 'existing-libreoffice'


def test_managed_entry_admits_legacy_word(entry_case):
    from paperflow.managed import validate_scope
    root, old, source, config = entry_case
    legacy = source.with_suffix('.doc'); legacy.write_bytes(b'legacy Word')
    assert validate_scope(config, root, [legacy], old)[1] == [legacy.resolve()]


def test_continuous_entry_discovers_after_start_and_does_not_stop_when_idle(entry_case, monkeypatch):
    from paperflow.managed import run_managed
    from paperflow.scheduler import Scheduler
    root, old, _, config = entry_case
    incoming = root.parent/'incoming'; incoming.mkdir()
    config.source_roots = [str(incoming)]
    config.scan_seconds = .03; config.stable_seconds = .08
    original = Scheduler.__init__
    received = []
    def setup(scheduler, *args, **kwargs):
        original(scheduler, *args, **kwargs)
        def launch(manager, config, task_id, *, schedule_attempt):
            task = manager.store.get(task_id)
            received.append(task)
            code = "from paperflow.control_ipc import WorkerClient;c=WorkerClient.from_environment();c.report_execution('held',{'stage':'held'});c.close()"
            return manager.launch('graph', task_id, [sys.executable, '-B', '-c', code],
                                  cwd=Path.cwd(), schedule_attempt=schedule_attempt)
        scheduler.launchers['graph'] = launch
    monkeypatch.setattr(Scheduler, '__init__', setup)
    result = []; errors = []
    def run():
        try: result.append(run_managed(config, root, [], legacy_root=old, execute=True, seconds=10, watch=True))
        except BaseException as exc: errors.append(exc)
    worker = threading.Thread(target=run); worker.start()
    try:
        time.sleep(.25)
        source = incoming/'new.txt'; source.write_text('new arrival')
        deadline = time.monotonic()+5
        while not received and not errors and time.monotonic()<deadline: time.sleep(.03)
        assert not errors, errors
        assert len(received) == 1
        task = received[0]
        assert Path(task['source_path']).is_relative_to(root)
        assert task['original_source_path'] == str(source.resolve())
        assert sha(task['source_path']) == sha(source)
        time.sleep(.4)
        assert worker.is_alive() and len(received) == 1
        before=Path(task['source_path']).read_bytes()
        source.write_text('changed source, next immutable version')
        deadline=time.monotonic()+5
        while len(received)<2 and not errors and time.monotonic()<deadline:time.sleep(.03)
        assert len(received)==2 and received[1]['task_id']!=task['task_id']
        assert received[1]['source_id']==task['source_id']
        assert Path(task['source_path']).read_bytes()==before
        from paperflow.store import Store
        assert not Store(config.root/'state.sqlite3').is_current(task['task_id'])
    finally:
        (root/'STOP').write_text('test stop')
        worker.join(10)
    assert not worker.is_alive() and not errors
    assert result[0]['reason'] == 'operator stop'
    timing = load(root/'run-timing.json')
    assert timing['preparation_seconds'] >= 0 and timing['runtime_seconds'] > 0
    assert timing['extra_audit_seconds'] is None
    assert timing['articles'][0]['task_id'] == received[0]['task_id']


# Reuse the existing accepted-endpoint fixture, including real STOP/lock files.
from test_managed_entry import entry_case


def test_converter_rejects_invalid_output_and_preserves_original(tmp_path,monkeypatch):
    from paperflow.reading import prepare_reading
    import pytest
    source=tmp_path/'bad.doc';source.write_bytes(b'unchanged original')
    monkeypatch.setenv('PAPERFLOW_OFFICE_TEMP_ROOT',str(tmp_path/'short-temps'))
    def invalid(command,**kwargs):
        assert Path(kwargs['env']['TEMP']).is_relative_to(tmp_path)
        target=Path(command[-1])/'source.docx';target.parent.mkdir();target.write_bytes(b'not a Word document')
        return subprocess.CompletedProcess(command,0,b'',b'')
    monkeypatch.setattr(subprocess,'run',invalid)
    with pytest.raises(RuntimeError,match='conversion failed'):prepare_reading(source,tmp_path/'view')
    assert source.read_bytes()==b'unchanged original'
    assert not (tmp_path/'view/source-index.json').exists()


def test_watch_refuses_output_directory_and_changed_approved_roots(entry_case):
    import pytest
    from paperflow.managed import validate_scope
    root,old,_,config=entry_case
    config.root.mkdir();config.source_roots=[str(config.root)]
    with pytest.raises(ValueError,match='overlaps'):validate_scope(config,root,[],old,watch=True)
    incoming=root.parent/'incoming';incoming.mkdir();config.source_roots=[str(incoming)]
    config.production_pilot=str(root/'scope.json')
    save(config.production_pilot,dict(mode='production-watch',endpoint=config.rag_url,root=str(root),
        expires_at=time.time()+60,catalog_sha256=sha(config.catalog_path),source_roots=[str(root/'other')]))
    with pytest.raises(ValueError,match='roots changed'):validate_scope(config,root,[],old,watch=True)


def test_watch_rejects_unapproved_existing_tasks_before_start(entry_case):
    import pytest
    from paperflow.managed import run_managed
    from paperflow.store import Store
    root,old,_,config=entry_case
    incoming=root.parent/'incoming';incoming.mkdir();config.source_roots=[str(incoming)]
    config.prepare();copy=root/'unselected.txt';copy.write_text('old outside selection')
    store=Store(config.root/'state.sqlite3')
    store.register_source(copy,sha(copy),identity_path=root.parent/'outside.txt',
                          original_source_path=str(root.parent/'outside.txt'))
    with pytest.raises(ValueError,match='existing task outside approved'):
        run_managed(config,root,[],legacy_root=old,watch=True)


def test_snapshot_versions_preserve_active_history_in_one_transaction(tmp_path):
    from paperflow.store import Store
    store=Store(tmp_path/'state.sqlite3')
    a=tmp_path/'old.txt';a.write_text('first')
    b=tmp_path/'new.txt';b.write_text('second')
    identity=tmp_path/'original.txt'
    first=store.register_source(a,sha(a),identity_path=identity,supersede_identity=True)
    store.mark_active(first['task_id'])
    second=store.register_source(b,sha(b),identity_path=identity,supersede_identity=True)
    assert not store.is_current(first['task_id'])
    assert store.previous_active(second['task_id'])==[first['task_id']]
    assert store.active_versions_for(second['task_id'])==[dict(task_id=first['task_id'],revision=1)]


def test_timing_assigns_each_attempt_to_its_own_revision(scheduler_case,tmp_path):
    from paperflow.run_timing import run_timing
    manager,config,scheduler=scheduler_case
    first=tmp_path/'a.txt';first.write_text('first')
    a=manager.store.register_source(first,sha(first),identity_path=tmp_path/'original.txt',supersede_identity=True)
    scheduler.discover()
    second=tmp_path/'b.txt';second.write_text('second')
    b=manager.store.register_source(second,sha(second),identity_path=tmp_path/'original.txt',supersede_identity=True)
    scheduler.discover()
    for task,attempt,start,finish in ((a,'attempt-a',110,120),(b,'attempt-b',125,145)):
        job=next(r for r in scheduler.ledger.rows('SELECT * FROM scheduler_jobs') if r['unit_key']==task['task_id'])
        scheduler.ledger._write(lambda c,j=job,t=attempt,l=start,r=finish:c.execute(
            'INSERT INTO scheduler_attempts VALUES(?,?,?,?,?,?,?,?)',
            (t,j['job_id'],None,'finished',l,r,'complete','{}')))
    result=run_timing(scheduler,90,100,150)
    by_task={r['task_id']:r for r in result['articles']}
    assert by_task[a['task_id']]['processing_seconds']==10
    assert by_task[b['task_id']]['processing_seconds']==20
    scheduler.ledger._write(lambda c:c.execute('INSERT INTO delivery_packages VALUES(?,?,?,?,?,?,?,?,?,?)',
        ('repaired-manifest',a['task_id'],'{}',b'{}','r02',str(tmp_path/'r02'),'[]','v1','{}','retained')))
    job=scheduler.ledger.enqueue('delivery',a['source_id'],'repaired-target',
        dict(manifest_sha256='repaired-manifest',specialty_code='x'),scheduler.service.services['rag']['service_id'])
    scheduler.ledger._write(lambda c:c.execute('INSERT INTO scheduler_attempts VALUES(?,?,?,?,?,?,?,?)',
        ('attempt-repaired',job,None,'finished',142,149,'complete','{}')))
    by_task={r['task_id']:r for r in run_timing(scheduler,90,100,150)['articles']}
    assert by_task[a['task_id']]['delivery_seconds']==7
    assert by_task[b['task_id']]['delivery_seconds']==0


from test_scheduler import scheduler_case


def test_office_recovery_removes_only_dead_readers_owned_scratch(tmp_path,monkeypatch):
    import paperflow.winjob as winjob
    from paperflow.config import Config
    from paperflow.office import cleanup_office_temps
    short=tmp_path/'short';scratch=short/'pf-owned';scratch.mkdir(parents=True)
    unrelated=short/'pf-unrelated';unrelated.mkdir();(unrelated/'keep.txt').write_text('unrelated')
    config=Config(home=str(tmp_path/'runtime'),office_temp_root=str(short))
    marker=config.root/'work/task/private/source-view/converted/office-owned.json'
    owner=dict(path=str(scratch),marker=str(marker.resolve()),process=dict(pid=1234,created_at=1),token='<REDACTED_SECRET>')
    save(marker,owner);save(scratch/'paperflow-owner.json',owner)
    (scratch/'source.doc').write_bytes(b'private original copy')
    monkeypatch.setattr(winjob,'identity_alive',lambda _:True)
    assert cleanup_office_temps(config)['removed']==[]
    assert (scratch/'source.doc').exists()
    monkeypatch.setattr(winjob,'identity_alive',lambda _:False)
    assert cleanup_office_temps(config)['removed']==[str(scratch)]
    assert not marker.exists() and not scratch.exists()
    assert (unrelated/'keep.txt').read_text()=='unrelated'
