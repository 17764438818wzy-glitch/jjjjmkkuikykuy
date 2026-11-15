"""Identity-scoped projections in the native delivery database.

Source manifests and native receipts remain authoritative. This index neither
delivers nor deletes files; incomplete indexing leaves attachments waiting.
"""
from collections import defaultdict
from contextlib import contextmanager, closing, nullcontext
from contextvars import ContextVar
import copy
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
import time

_states={}
_guard=threading.Lock()
_scope=ContextVar('handoff_association_scope',default=None)


def _json(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))
def _sha(value):return hashlib.sha256(_json(value).encode()).hexdigest()


def _root():
    from project_paths import DATA_DIR
    return Path(DATA_DIR)


def _snapshot(manifest):
    keys=('id','source_path','source_hash','input_source_path','archive_source_hash','archive_member_path',
          'content_format','status','preextract_work_dir','intake_role')
    rows=[]
    for item in manifest.get('items',[]):
        if not item.get('source_path'):continue
        row={key:copy.deepcopy(item[key]) for key in keys if key in item}
        info=item.get('associated_material') or {}
        if info:row['associated_material']={key:info[key] for key in ('kind','purpose_pending') if key in info}
        rows.append(row)
    return dict(run_id=manifest['run_id'],items=rows)


class _Index:
    def __init__(self,root):
        from preextract_delivery_queue import queue_path
        self.root=Path(root);self.path=queue_path(root)
        self.condition=threading.Condition();self.pending={};self.signatures={}
        self.thread=threading.Thread(target=self._run,name='project-delivery-index',daemon=True)
        self.thread.start()

    def note(self,manifest):
        snapshot=_snapshot(manifest);signature=_sha(snapshot)
        with self.condition:
            if self.signatures.get(snapshot['run_id'])==signature:return
            self.signatures[snapshot['run_id']]=signature
            self.pending[snapshot['run_id']]=snapshot
            self.condition.notify_all()

    def _schema(self,db):
        db.executescript('''
          CREATE TABLE IF NOT EXISTS handoff_sources(
            run_id TEXT NOT NULL,item_id TEXT NOT NULL,project_key TEXT NOT NULL,
            source_hash TEXT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(run_id,item_id));
          CREATE INDEX IF NOT EXISTS handoff_project_source ON handoff_sources(project_key,source_hash);
          CREATE TABLE IF NOT EXISTS handoff_index_meta(name TEXT PRIMARY KEY,value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS handoff_index_inbox(run_id TEXT PRIMARY KEY,payload TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS handoff_associations(
            project_key TEXT NOT NULL,fingerprint TEXT NOT NULL,result TEXT NOT NULL,
            PRIMARY KEY(project_key,fingerprint));
          CREATE INDEX IF NOT EXISTS deliveries_manifest_identity ON deliveries(json_extract(payload,'$.manifest_sha256'));
          CREATE INDEX IF NOT EXISTS deliveries_work_identity ON deliveries(json_extract(payload,'$.item.preextract_work_dir'));
          CREATE INDEX IF NOT EXISTS deliveries_package_identity ON deliveries(json_extract(payload,'$.item.package_path'));
          CREATE INDEX IF NOT EXISTS references_source_identity ON references_to_deliveries(source_hash,delivery_id);
        ''')

    def _publish(self,db,snapshot):
        from purpose_review import project_key
        rid=snapshot['run_id']
        rows=[(rid,row['id'],project_key(dict(path=row['source_path'],**{
            k:row[k] for k in ('input_source_path','archive_source_hash','archive_member_path') if k in row})),
            row.get('source_hash') or '',_json(row)) for row in snapshot['items']]
        db.execute('DELETE FROM handoff_sources WHERE run_id=?',(rid,))
        db.executemany('INSERT OR REPLACE INTO handoff_sources VALUES(?,?,?,?,?)',rows)

    def _run(self):
        from filelock import FileLock,Timeout
        leader=False;lease=FileLock(str(self.path.with_name('project-index.worker.lock')),timeout=0)
        try:lease.acquire();leader=True
        except Timeout:pass
        try:
            with closing(sqlite3.connect(self.path,timeout=5)) as db:
                self._schema(db);db.commit()
                if leader:
                    db.execute("INSERT OR REPLACE INTO handoff_index_meta VALUES('ready','0')");db.commit()
                    # One streaming bootstrap; never retain all historical manifests.
                    for path in sorted((self.root/'runs').glob('*/manifest.json')):
                        try:
                            value=json.loads(path.read_bytes());self._publish(db,_snapshot(value));db.commit()
                        except (OSError,ValueError,KeyError,TypeError):
                            db.rollback()
                            raise RuntimeError('Historical project identity is unreadable; attachments remain waiting')
                    db.execute("INSERT OR REPLACE INTO handoff_index_meta VALUES('ready','1')");db.commit()
                while True:
                    with self.condition:
                        if not self.pending:self.condition.wait(.2 if leader else 1)
                        pending=dict(self.pending)
                    try:
                        for rid,snapshot in pending.items():
                            db.execute('INSERT OR REPLACE INTO handoff_index_inbox VALUES(?,?)',(rid,_json(snapshot)))
                        db.commit()
                        with self.condition:
                            for rid,snapshot in pending.items():
                                if self.pending.get(rid) is snapshot:self.pending.pop(rid,None)
                        if leader:
                            messages=db.execute('SELECT run_id,payload FROM handoff_index_inbox').fetchall()
                            for rid,raw in messages:
                                self._publish(db,json.loads(raw))
                                db.execute('DELETE FROM handoff_index_inbox WHERE run_id=? AND payload=?',(rid,raw))
                                db.commit()
                    except (sqlite3.Error,ValueError,OSError):
                        db.rollback()
                        with self.condition:
                            for rid,snapshot in pending.items():self.pending.setdefault(rid,snapshot)
                        time.sleep(.2)
        except Exception as exc:
            from engine import write_json
            write_json(self.path.with_name('project-index-status.json'),dict(
                state='index_unavailable_originals_retained',error_type=type(exc).__name__))
        finally:
            if leader:lease.release()


def _index(root):
    key=str(root)
    with _guard:
        if key not in _states or not _states[key].thread.is_alive():_states[key]=_Index(root)
        return _states[key]


def note_manifest(store,manifest):
    if Path(store.root)!=_root():return
    _index(store.root).note(manifest)


def project_history(root,project):
    state=_index(root)
    try:
        with closing(sqlite3.connect(state.path.as_uri()+'?mode=ro',uri=True,timeout=.1)) as db:
            db.execute('BEGIN')
            ready=db.execute("SELECT value FROM handoff_index_meta WHERE name='ready'").fetchone()
            if not ready or ready[0]!='1':return None
            groups=defaultdict(list)
            for rid,raw in db.execute('SELECT run_id,payload FROM handoff_sources WHERE project_key=?',(project,)):
                groups[rid].append(json.loads(raw))
            overlays={rid:json.loads(raw) for rid,raw in db.execute('SELECT run_id,payload FROM handoff_index_inbox')}
        with state.condition:overlays.update(state.pending)
        from purpose_review import project_key
        for rid,snapshot in overlays.items():
            groups[rid]=[row for row in snapshot['items'] if project_key(dict(path=row['source_path'],**{
                k:row[k] for k in ('input_source_path','archive_source_hash','archive_member_path') if k in row}))==project]
        return [dict(run_id=rid,items=rows) for rid,rows in groups.items() if rows]
    except (sqlite3.Error,OSError,ValueError):return None


@contextmanager
def attachment_work():
    """Keep native file leases while releasing the parser reservation."""
    from purpose_review import current
    context=current()
    processor=context[0] if context else None
    scope=getattr(processor,'_preextract_conversion',nullcontext) if processor else nullcontext
    token=_scope.set({})
    try:
        with scope():yield
    finally:_scope.reset(token)


def association(record,context,calculate):
    from purpose_review import project_key,cached_role,DOCUMENTS,IMAGES,_contract
    project=project_key(record);members=context._purpose_projects.get(project,[])
    scoped=_scope.get();scope_key=(id(context),project,id(members))
    if scoped is not None and scope_key in scoped:return copy.deepcopy(scoped[scope_key])
    config=context._purpose_config;proof=[]
    for row in members:
        if Path(row['path']).suffix.lower() not in DOCUMENTS|IMAGES:continue
        role=cached_role(row,config)
        if not role or not role.get('confirmed') or role.get('action')=='pending':
            return calculate(record,context)
        try:stat=Path(row['path']).stat();stamp=(stat.st_size,stat.st_mtime_ns)
        except FileNotFoundError:stamp=None
        proof.append(dict(path=row['path'],stamp=stamp,role=role))
    fingerprint=_sha(dict(contract=_contract(config),proof=proof,
        resolution=config.get('parent_resolutions_sha256'),project=project))
    state=_index(_root());result=None
    try:
        with closing(sqlite3.connect(state.path.as_uri()+'?mode=ro',uri=True,timeout=.1)) as db:
            saved=db.execute('SELECT result FROM handoff_associations WHERE project_key=? AND fingerprint=?',
                             (project,fingerprint)).fetchone()
            if saved:result=json.loads(saved[0])
    except (OSError,ValueError,sqlite3.Error):pass
    if result is None:
        result=calculate(record,context)
        if result.get('association_status')=='candidate':
            try:
                with closing(sqlite3.connect(state.path,timeout=.1)) as db,db:
                    db.execute('INSERT OR REPLACE INTO handoff_associations VALUES(?,?,?)',(project,fingerprint,_json(result)))
            except sqlite3.Error:pass  # A cache write cannot grant or revoke a native association.
    if scoped is not None:scoped[scope_key]=copy.deepcopy(result)
    return result
