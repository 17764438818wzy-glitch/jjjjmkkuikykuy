"""Bounded legacy handover. Old checkpoints are evidence, never executable state."""
from contextlib import ExitStack
from contextvars import ContextVar
import json
import os
import re
from pathlib import Path
import shutil
import sqlite3
import time

from .instance_lock import InstanceLock
from .package import load, save, sha
from .handoff import PackageHandoff, _files
from .handoff_store import canonical
from .delivery_store import DeliveryLedger
from .repair_store import RepairLedger
from .winjob import identity_alive

_GUARD=ContextVar('transition_guard',default=None)


class TransitionGuard:
    """Hold the *actual* old byte-range locks for the entire new session."""
    def __init__(self,locks,stops,*,identities=()):
        self.paths=sorted({Path(p).resolve() for p in locks},key=lambda p:str(p).casefold())
        self.stops={Path(p).resolve():None for p in stops}
        self.identities=list(identities);self.stack=None;self.token=None
        if not self.paths or not self.stops:raise ValueError('old locks and pause markers required')

    def check(self):
        if self.stack is None:raise ValueError('transition locks not held')
        for p,digest in self.stops.items():
            if not p.is_file() or (digest is not None and sha(p)!=digest):raise ValueError('old pause marker missing or changed')
        for identity in self.identities:
            if identity_alive(identity):raise ValueError('old process tree still alive')

    def __enter__(self):
        if self.stack is not None:raise ValueError('guard already entered')
        self.stack=ExitStack()
        try:
            for p in self.paths:self.stack.enter_context(InstanceLock(p))
            self.check()
            self.stops={p:sha(p) for p in self.stops}
            self.token=_GUARD.set(self)
            return self
        except BaseException:
            self.stack.close();self.stack=None
            raise

    def __exit__(self,*args):
        if self.token is not None:_GUARD.reset(self.token);self.token=None
        if self.stack:self.stack.close();self.stack=None


class TransitionSession:
    """Failed start or shutdown retains old locks until tree exit is confirmed."""
    def __init__(self,manager,guard):self.manager,self.guard=manager,guard
    def __enter__(self):
        self.guard.__enter__()
        # start() may fail specifically because a prior candidate tree is still
        # alive. Keep the old executors excluded even when __exit__ won't run.
        self.manager.transition_guard=self.guard
        self.manager.start()
        return self.manager
    def __exit__(self,*args):
        self.manager.shutdown(timeout=35)
        self.guard.check()
        self.guard.__exit__(*args)


def _backup(source,destination):
    destination.parent.mkdir(parents=True,exist_ok=True)
    src=sqlite3.connect(source.as_uri()+'?mode=ro',uri=True)
    dst=sqlite3.connect(destination)
    try:
        src.backup(dst)
        if dst.execute('PRAGMA integrity_check').fetchall()!=[('ok',)]:raise ValueError('snapshot integrity failed')
    finally:dst.close();src.close()


def snapshot_legacy(old_root,destination,task_ids):
    old_root,destination=Path(old_root).resolve(),Path(destination).resolve()
    guard=_GUARD.get()
    if not guard or old_root/'controller.lock' not in guard.paths:raise ValueError('old controller lock required for snapshot')
    guard.check()
    if (not task_ids or len(task_ids)>10 or len(set(task_ids))!=len(task_ids)
            or destination.exists() or destination.is_relative_to(old_root)):raise ValueError('new isolated snapshot and 1..10 explicit tasks required')
    if any(not isinstance(t,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,200}',t) for t in task_ids):raise ValueError('unsafe legacy task id')
    destination.mkdir(parents=True)
    _backup(old_root/'state.sqlite3',destination/'state.sqlite3')
    for source in sorted((old_root/'checkpoints').glob('*.sqlite3')):
        _backup(source,destination/'checkpoints'/source.name)
    connection=sqlite3.connect(destination/'state.sqlite3');connection.row_factory=sqlite3.Row
    tasks={}
    try:
        for tid in task_ids:
            row=connection.execute('SELECT * FROM tasks WHERE task_id=?',(tid,)).fetchone()
            if not row:raise ValueError('selected task missing')
            from .store import Store
            task=Store.decode(row)
            receipts=[dict(r) for r in connection.execute('SELECT revision,target,data FROM receipts WHERE task_id=?',(tid,))]
            item=dict(task=task,receipts=receipts,package=None,package_error=None,budget=0)
            for key in ('correction_count','repair_count'):
                value=task.get(key,0)
                if type(value) is not int or value<0:raise ValueError('invalid legacy correction count')
                item['budget']=max(item['budget'],value)
            for key in {tid,task.get('delivery_task_id',tid)}:
                if not isinstance(key,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,200}',key):raise ValueError('unsafe legacy task alias')
                record=old_root/'private/delivery-corrections'/(key+'.json')
                if record.exists():
                    value=load(record)
                    if value.get('source_sha256')!=task['source_sha256']:raise ValueError('correction source identity conflict')
                    count=value.get('count')
                    if type(count) is not int or count<0:raise ValueError('invalid legacy correction count')
                    item['budget']=max(item['budget'],count)
                    save(destination/'correction-evidence'/(key+'.json'),value)
            if task.get('package_dir'):
                source=Path(task['package_dir']).resolve()
                try:
                    if not source.is_relative_to(old_root):raise ValueError('legacy package outside selected runtime')
                    inventory=_files(source)
                    # A package copy is byte-preserving; no resealing or identity rewriting.
                    target=destination/'packages'/tid
                    for file in inventory:
                        dst=target/file['path'];dst.parent.mkdir(parents=True,exist_ok=True)
                        shutil.copyfile(source/file['path'],dst)
                    if _files(target)!=inventory:raise ValueError('legacy package changed while copying')
                    item['package']=target.relative_to(destination).as_posix()
                except (OSError,ValueError) as exc:item['package_error']=type(exc).__name__
            tasks[tid]=item
    finally:connection.close()
    save(destination/'selected.json',dict(source_root=str(old_root),tasks=tasks))
    guard.check()
    files={p.relative_to(destination).as_posix():sha(p) for p in sorted(destination.rglob('*')) if p.is_file()}
    import hashlib
    save(destination/'snapshot.json',dict(snapshot_id=hashlib.sha256(canonical(files).encode()).hexdigest(),
        files=files,task_ids=task_ids,created_at=time.time(),locks=[str(p) for p in guard.paths],stops={str(p):h for p,h in guard.stops.items()}))
    return destination


def verify_snapshot(path):
    path=Path(path).resolve();record=load(path/'snapshot.json')
    import hashlib
    if hashlib.sha256(canonical(record['files']).encode()).hexdigest()!=record['snapshot_id']:raise ValueError('snapshot identity changed')
    for name,digest in record['files'].items():
        candidate=(path/name).resolve()
        if not candidate.is_relative_to(path) or not candidate.is_file() or sha(candidate)!=digest:raise ValueError('snapshot bytes changed')
    return record,load(path/'selected.json')


class LegacyImporter:
    def __init__(self,store,config):
        self.store,self.config=store,config
        self.handoff=PackageHandoff(store,config.handoff_root)
        self.ledger=DeliveryLedger(store);self.repairs=RepairLedger(store)

    def import_task(self,snapshot,task_id,catalog):
        record,selection=verify_snapshot(snapshot)
        if task_id not in record['task_ids']:raise ValueError('task outside snapshot selection')
        item=selection['tasks'][task_id];original=item['task'];sid=record['snapshot_id']
        with self.store.db() as c:
            row=c.execute('SELECT result FROM legacy_runs WHERE snapshot_id=? AND task_id=?',(sid,task_id)).fetchone()
            if row:return json.loads(row[0])
        # Reserve an unschedulable source before filesystem work. A crash cannot
        # expose the old stage/control_request to either graph or delivery queues.
        task={k:original[k] for k in ('task_id','source_id','source_sha256','source_path','revision','contract_version')}
        task.update(stage='held',kind='historical',due=0,owner=None,lease_until=0,
            repair_count=item['budget'],legacy_snapshot=sid,thread_id='transition:'+sid+':'+task_id)
        def reserve(c):
            old=c.execute('SELECT data FROM tasks WHERE task_id=?',(task_id,)).fetchone()
            if old and json.loads(old[0]).get('legacy_snapshot')!=sid:raise ValueError('existing task requires explicit reconciliation')
            source=c.execute('SELECT task_id FROM sources WHERE path=?',(task['source_path'],)).fetchone()
            if source and source[0]!=task_id:raise ValueError('source already has another current task')
            c.execute('INSERT OR IGNORE INTO tasks VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                (task_id,task['source_sha256'],'held','historical',0,None,0,time.time(),time.time(),canonical(task),0))
            c.execute('INSERT OR IGNORE INTO sources(path,source_id,task_id) VALUES(?,?,?)',(task['source_path'],task['source_id'],task_id))
            c.execute('INSERT OR IGNORE INTO source_versions VALUES(?,?)',(task['source_path'],task_id))
            self.repairs.seed_sql(c,task,item['budget'])
        self.ledger._write(reserve)
        held=None
        if original.get('delivery_task_id',task_id)!=task_id:held='cross_task_identity_requires_mapping'
        elif not item['package']:held='missing_or_incompatible_package'
        if held:
            result=dict(state='held',reason=held,task_id=task_id,snapshot_id=sid,
                processing_restart_allowed=original['stage'] in ('round1','round2','round3','round3_assets','round3_redact','round3_chunk'))
            self.ledger._write(lambda c:c.execute('INSERT INTO legacy_runs VALUES(?,?,?,?,?)',
                (sid,task_id,canonical(item),'held',canonical(result))))
            return result
        material=Path(snapshot)/item['package']
        if sha(material/'manifest.json')!=original.get('manifest_sha256'):raise ValueError('legacy manifest identity mismatch')
        intent=self.handoff.prepare(task['thread_id'],task,material,catalog,mapping_version='legacy:'+sid)
        self.handoff.publish(task['thread_id'],intent['input_fingerprint'])
        intent,packet,manifest,registered=self.handoff.verify_registration(task['thread_id'],intent['input_fingerprint'])
        if int(packet['manifest']['revision'].removeprefix('r'))!=task['revision']:raise ValueError('legacy revision mismatch')
        receipts={r['target']:json.loads(r['data']) for r in item['receipts'] if r['revision']==task['revision'] and r['target'] not in ('all','activation')}
        def finish(c):
            existing=c.execute('SELECT result FROM legacy_runs WHERE snapshot_id=? AND task_id=?',(sid,task_id)).fetchone()
            if existing:return json.loads(existing[0])
            result=self.ledger.register_sql(c,task['thread_id'],intent['input_fingerprint'],packet,manifest,verified_registration=registered)
            digest=result['manifest_sha256']
            for target in result['targets']:
                code=target['code']
                c.execute('INSERT INTO legacy_targets VALUES(?,?,?,?,?,?)',(digest,code,sid,canonical(receipts.get(code,{})),'pending_reconcile','{}'))
            task.update(stage='handed_off',manifest_sha256=digest,package_dir=result['package_dir'])
            c.execute("UPDATE tasks SET stage='handed_off',data=?,version=version+1 WHERE task_id=?",(canonical(task),task_id))
            result=dict(result,state='pending_reconcile',snapshot_id=sid)
            c.execute('INSERT INTO legacy_runs VALUES(?,?,?,?,?)',(sid,task_id,canonical(item),'historical',canonical(result)))
            return result
        return self.ledger._write(finish)

    def restart_incompatible_processing(self,snapshot_id,task_id):
        """Explicit new run, same source/budget. Never copy an old graph checkpoint."""
        task=self.store.get(task_id)
        if not task or sha(task['source_path'])!=task['source_sha256']:raise ValueError('original source changed')
        expected_version=task['version']
        def write(c):
            row=c.execute('SELECT * FROM legacy_runs WHERE snapshot_id=? AND task_id=?',(snapshot_id,task_id)).fetchone()
            if not row or not json.loads(row['result']).get('processing_restart_allowed'):raise ValueError('not an incompatible processing run')
            task=self.store.decode(c.execute('SELECT * FROM tasks WHERE task_id=?',(task_id,)).fetchone())
            if task.get('legacy_snapshot')!=snapshot_id:raise ValueError('legacy source changed')
            if task.get('legacy_restart'):return task
            if task['version']!=expected_version:raise ValueError('legacy source changed')
            task.update(stage='round1',kind='new',legacy_restart=True,thread_id='restart:'+snapshot_id+':'+task_id)
            c.execute("UPDATE tasks SET stage='round1',kind='new',data=?,version=version+1 WHERE task_id=?",(canonical(task),task_id))
            return task
        return self.ledger._write(write)


def archive_dispatcher_receipt(store,path,archive_root):
    """Retain cleaned legacy receipts without inventing a new sealed package."""
    path=Path(path).resolve();root=Path(archive_root).resolve();value=load(path)
    if value.get('schema')!='rag-mid-ingestion-receipt/1.0':raise ValueError('unsupported legacy receipt schema')
    identity={k:value.get(k) for k in ('task_id','source_id','source_sha256','revision','manifest_sha256')}
    if any(not isinstance(v,str) or not v for v in identity.values()):raise ValueError('incomplete legacy receipt identity')
    for k in ('source_sha256','manifest_sha256'):
        if not re.fullmatch('[0-9a-f]{64}',identity[k]):raise ValueError('invalid historical hash')
    digest=sha(path);target=root/(digest+'.json');root.mkdir(parents=True,exist_ok=True)
    if target.exists() and sha(target)!=digest:raise ValueError('archived receipt changed')
    if not target.exists():shutil.copyfile(path,target)
    if sha(path)!=digest or sha(target)!=digest:raise ValueError('legacy receipt changed during archive')
    ledger=DeliveryLedger(store)
    def write(c):
        c.execute('CREATE TABLE IF NOT EXISTS legacy_receipt_archive (identity TEXT PRIMARY KEY,sha256 TEXT NOT NULL,data TEXT NOT NULL)')
        key=canonical(identity);prior=c.execute('SELECT * FROM legacy_receipt_archive WHERE identity=?',(key,)).fetchone()
        if prior:
            if prior['sha256']!=digest:raise ValueError('historical receipt identity conflict')
            return json.loads(prior['data'])
        result=dict(identity=identity,source=str(path),archive=str(target),receipt_sha256=digest,
            targets=sorted(value['targets']),state='pending_reconcile',reason='historical_protocol_requires_exact_package_and_image_mapping')
        c.execute('INSERT INTO legacy_receipt_archive VALUES(?,?,?)',(key,digest,canonical(result)))
        return result
    return ledger._write(write)
