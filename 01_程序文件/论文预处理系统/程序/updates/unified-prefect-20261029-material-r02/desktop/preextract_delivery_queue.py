"""A durable index of existing sealed packages, separate from producer manifests."""
from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import time
from contextlib import closing
from pathlib import Path

from upload_tracking import service_key

MODE = 'disk_queue_v1'
DELIVERY_FIELDS = ('uploads', 'preextract_delivery', 'handoff_status', 'handoff_reason',
                   'preextract_handoff_blocked', 'preextract_revision_activation', 'preextract_cleanup',
                   'preextract_associated_delivery', 'preextract_retirement', 'associated_delivery', 'preextract_delivery_wait',
                   'preextract_native_observation', 'preextract_native_followup')


def queue_path(root):
    return Path(root).resolve() / 'private' / 'preextract_delivery' / 'queue.sqlite3'


def publication_registered(work, prepared, *, roots=()):
    """Read existing intents without creating a queue or altering its state."""
    work = Path(work).resolve()
    package = Path(prepared['package_path']).resolve()
    paths = {queue_path(root) for root in (*roots, work, *work.parents)}
    for path in paths:
        if not path.is_file():
            continue
        try:
            with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
                # Indexed candidates only; keep all original path/task/hash
                # guards below, including detection of an old intent at work.
                source=prepared.get('task',{}).get('source_sha256','')
                manifest=prepared.get('validation',{}).get('manifest_sha256','')
                query="""SELECT payload FROM deliveries WHERE
                    id IN (SELECT delivery_id FROM references_to_deliveries WHERE source_hash=?)
                    OR json_extract(payload,'$.manifest_sha256')=?
                    OR json_extract(payload,'$.item.preextract_work_dir')=?
                    OR json_extract(payload,'$.item.package_path')=?"""
                for (payload,) in db.execute(query,(source,manifest,str(work),str(package))):
                    job = json.loads(payload)
                    item = job['item']
                    if ((item.get('preextract_work_dir') and Path(item['preextract_work_dir']).resolve() == work)
                            or (item.get('package_path') and Path(item['package_path']).resolve() == package)
                            or (job.get('task') == prepared['task']
                                and job.get('manifest_sha256') == prepared['validation']['manifest_sha256'])):
                        return True
        except (sqlite3.Error, ValueError, KeyError, TypeError) as exc:
            raise ValueError('交付队列登记无法核对，保留原封包') from exc
    return False


def queue_stamp(root):
    """A cheap cache key; observers never create or start a delivery queue."""
    path = queue_path(root)
    result = []
    for file in (path, Path(str(path) + '-wal')):
        try:
            stat = file.stat()
            # A read-only SQLite observer can create an empty WAL. That is not
            # a delivery update and must not invalidate the reader's own cache.
            result.append(None if file != path and not stat.st_size else (stat.st_mtime_ns, stat.st_size))
        except FileNotFoundError:
            result.append(None)
    return tuple(result)


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _row(row):
    if row is None:
        return None
    result = dict(row)
    for field in ('payload', 'projection'):
        result[field] = json.loads(result[field])
    return result


def _control_command(db, request_id, scope, action, expected_generation):
    row = db.execute('SELECT * FROM delivery_control_commands WHERE request_id=?', (request_id,)).fetchone()
    if row is None:
        return None
    if (row['scope'], row['action'], row['expected_generation']) != (scope, action, expected_generation):
        raise ValueError('delivery command identity belongs to different parameters')
    return json.loads(row['result'])


def _save_control_command(db, request_id, scope, action, expected_generation, result):
    db.execute('INSERT INTO delivery_control_commands VALUES(?,?,?,?,?)',
               (request_id, scope, action, expected_generation, _json(result)))


def package_reference(store, run_id, item_id, service, *, item=None):
    """Register only a passed package at its existing publication path."""
    if item is None:
        item = next(row for row in store.load(run_id)['items'] if row['id'] == item_id)
    from preextract_retirement import find_terminal
    retired=find_terminal(store.root,work=item.get('preextract_work_dir'),
        source_hash=item.get('source_hash'),service=service_key(service)) if item.get('preextract_work_dir') else None
    if retired:
        payload=copy.deepcopy(retired['payload'])
        if str(Path(item.get('package_path','')).resolve())!=str(Path(payload['item']['package_path']).resolve()):
            raise ValueError('已清理的入库记录与当前条目身份不一致')
        payload['run_id']=run_id;payload['item']['id']=item_id
        return payload
    if (item.get('content_format') != 'paper_preextract' or item.get('status') != 'pass'
            or not item.get('automatic_approval')):
        raise ValueError('只有已经通过原封包检查的论文可以排队')
    published = Path(item['package_path']).resolve()
    if not published.is_relative_to((store.runs / run_id / 'packages').resolve()):
        raise ValueError('成品须保留在原批次发布目录')
    work = Path(item['preextract_work_dir']).resolve()
    prepared = json.loads((work / 'private' / 'prepared-publication.json').read_text(encoding='utf8'))
    manifest_sha = hashlib.sha256((published / 'manifest.json').read_bytes()).hexdigest()
    if (manifest_sha != prepared['validation'].get('manifest_sha256')
            or not prepared['validation'].get('embedding', {}).get('admission_verified')):
        raise ValueError('封包身份或容量验收无效，未登记上传')
    identity = dict(service=service_key(service), task=prepared['task'], manifest_sha256=manifest_sha)
    # The full candidate and its learning content stay in their original files.
    saved_item = {key: copy.deepcopy(item[key]) for key in (
        'id', 'title', 'status', 'automatic_approval', 'content_format', 'source_hash',
        'package_path', 'preextract_work_dir','prepared_scope') if key in item}
    saved_item['uploads'] = {}
    return dict(id=hashlib.sha256(_json(identity).encode('utf8')).hexdigest(),
                service=identity['service'], task=prepared['task'], manifest_sha256=manifest_sha,
                store_root=str(store.root), run_id=run_id, item=saved_item)


class DeliveryQueue:
    def __init__(self, path):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as db, db:
            db.execute('PRAGMA journal_mode=WAL')
            db.executescript('''
                CREATE TABLE IF NOT EXISTS deliveries (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    id TEXT NOT NULL UNIQUE, service TEXT NOT NULL,
                    status TEXT NOT NULL, payload TEXT NOT NULL,
                    projection TEXT NOT NULL DEFAULT '{}',
                    created_at REAL NOT NULL, updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS deliveries_ready ON deliveries(service,status,sequence);
                CREATE INDEX IF NOT EXISTS deliveries_retired_source
                    ON deliveries(json_extract(payload,'$.item.source_hash'))
                    WHERE status='complete' AND json_extract(projection,'$.preextract_retirement.schema')='uploaded-products-retirement/1';
                CREATE TABLE IF NOT EXISTS references_to_deliveries (
                    run_id TEXT NOT NULL, item_id TEXT NOT NULL, delivery_id TEXT NOT NULL,
                    package_path TEXT NOT NULL, source_hash TEXT NOT NULL,
                    PRIMARY KEY(run_id,item_id)
                );
                CREATE TABLE IF NOT EXISTS delivery_controls (
                    scope TEXT PRIMARY KEY, paused INTEGER NOT NULL,
                    generation INTEGER NOT NULL, reason TEXT NOT NULL, updated_at REAL NOT NULL
                    , fence_id TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS delivery_stop_commands (
                    request_id TEXT PRIMARY KEY, scope TEXT NOT NULL,
                    fence_id TEXT NOT NULL, generation INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS delivery_control_commands (
                    request_id TEXT PRIMARY KEY, scope TEXT NOT NULL, action TEXT NOT NULL,
                    expected_generation INTEGER NOT NULL, result TEXT NOT NULL
                );
            ''')
            if 'fence_id' not in {row[1] for row in db.execute('PRAGMA table_info(delivery_controls)')}:
                db.execute("ALTER TABLE delivery_controls ADD COLUMN fence_id TEXT NOT NULL DEFAULT ''")

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        return db

    def enqueue(self, payload, *, retry=False, workspace_locked=False):
        from contextlib import ExitStack
        from filelock import FileLock
        work = payload['item'].get('preextract_work_dir')
        locked_workspaces=set()
        with ExitStack() as scope:
            # Registration must survive a concurrent cleaner; a transient busy
            # mutex must not drop the last enqueue for a completed producer.
            from processing_stop import acquire_lock
            scope.enter_context(acquire_lock(FileLock(str(self.path.with_name('cleanup.lock')), timeout=-1), float('inf')))
            if work and not workspace_locked:
                scope.enter_context(FileLock(str(Path(work)/'operation.lock'), timeout=0))
            if work:locked_workspaces.add(Path(work).resolve())
            current = self.get(payload['id'])
            original = current['payload']['item'].get('preextract_work_dir') if current else None
            if original and (not work or Path(original).resolve() != Path(work).resolve()):
                scope.enter_context(FileLock(str(Path(original)/'operation.lock'), timeout=0))
                locked_workspaces.add(Path(original).resolve())
            return self._enqueue_locked(payload, retry=retry, locked_workspaces=locked_workspaces)

    def _enqueue_locked(self, payload, *, retry=False, locked_workspaces=()):
        if queue_path(payload['store_root']) != self.path:
            raise ValueError('交付队列不属于当前原批次根目录')
        item = payload['item']
        now = time.time()
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            existing=db.execute('SELECT payload FROM deliveries WHERE id=?',(payload['id'],)).fetchone()
            original=json.loads(existing['payload'])['item'].get('preextract_work_dir') if existing else None
            if original and Path(original).resolve() not in locked_workspaces:
                # Another first registrant may have appeared after the outer
                # lookup. Defer without waiting while holding the SQLite writer.
                from filelock import Timeout
                raise Timeout(str(Path(original)/'operation.lock'))
            old = db.execute('SELECT delivery_id FROM references_to_deliveries WHERE run_id=? AND item_id=?',
                             (payload['run_id'], item['id'])).fetchone()
            if old and old['delivery_id'] != payload['id']:
                raise ValueError('同一条目已有不同版本的交付登记，须保留原记录核对')
            db.execute('INSERT OR IGNORE INTO deliveries(id,service,status,payload,created_at,updated_at) '
                       'VALUES(?,?,?,?,?,?)', (payload['id'], payload['service'],
                        'waiting_parent' if payload.get('kind')=='associated_original' else 'queued', _json(payload), now, now))
            db.execute('INSERT OR IGNORE INTO references_to_deliveries VALUES(?,?,?,?,?)',
                       (payload['run_id'], item['id'], payload['id'],
                        str(Path(item['package_path']).resolve()), item.get('source_hash', '')))
            current = db.execute('SELECT * FROM deliveries WHERE id=?', (payload['id'],)).fetchone()
            from preextract_retirement import terminal
            if terminal(_row(current)):
                if any(json.loads(current['payload'])[k]!=payload[k] for k in ('service','task','manifest_sha256')):
                    raise ValueError('已清理的入库身份发生变化')
                return payload['id']
            recheck = ((retry and current['status'] in ({'failed','complete','needs_review'} if payload.get('kind')=='associated_original' else {'failed','complete'})) or
                       (old is None and current['status'] == 'complete'))
            if recheck:
                # Reuse a newly verified public copy, while keeping the first
                # private intent and receipt. A different working directory
                # must never bypass an earlier remote-write record.
                renewed = copy.deepcopy(payload)
                renewed['item']['preextract_work_dir'] = json.loads(current['payload'])['item']['preextract_work_dir']
                db.execute("UPDATE deliveries SET status='queued',payload=?,updated_at=? WHERE id=?",
                           (_json(renewed), now, payload['id']))
        return payload['id']

    def get(self, identity):
        with closing(self._connect()) as db:
            return _row(db.execute('SELECT * FROM deliveries WHERE id=?', (identity,)).fetchone())

    def list(self, service=None, *, status=None):
        with closing(self._connect()) as db:
            query, args, conditions = 'SELECT * FROM deliveries', [], []
            if service is not None:
                conditions.append('service=?')
                args.append(service_key(service))
            if status is not None:
                conditions.append('status=?')
                args.append(status)
            if conditions:
                query += ' WHERE ' + ' AND '.join(conditions)
            rows = db.execute(query + ' ORDER BY sequence', args)
            return [_row(row) for row in rows]

    def scoped_list(self, service=None, *, status=None, dispatch_scope=None):
        from preextract_task_delivery import capture_delivery_authority, _dispatch_context
        if service is None or dispatch_scope is None:
            return []
        context = _dispatch_context(self.path, service, dispatch_scope)
        if not context['scope']['scope_ids']:
            return []
        with closing(self._connect()) as db:
            query, args = 'SELECT d.* FROM deliveries d WHERE service=? AND ' + self._dispatch_sql(), [
                service_key(service), json.dumps(context['scope']['scope_ids'])]
            if status is not None:
                query += ' AND status=?'
                args.append(status)
            result = []
            for row in db.execute(query + ' ORDER BY sequence', args):
                job = _row(row)
                authority = capture_delivery_authority(self.path, job['payload'], delivery_id=job['id'], db=db,
                    dispatch_scope=dispatch_scope, _context=context)
                if authority['references']:
                    job.update(delivery_authority=authority, dispatch_scope=authority['dispatch_scope'])
                    result.append(job)
            return result

    def has_work(self, service, *, include_interrupted=False, dispatch_scope=None):
        from preextract_task_delivery import capture_delivery_authority, _dispatch_context
        context = _dispatch_context(self.path, service, dispatch_scope)
        if not context['scope']['scope_ids']:
            return False
        states = ('queued', 'uploading') if include_interrupted else ('queued',)
        with closing(self._connect()) as db:
            rows=db.execute("SELECT * FROM deliveries d WHERE json_extract(payload,'$.item.prepared_scope') IS NULL AND service=? AND status IN (" +
                ','.join('?' for _ in states) + ') AND ' + self._admission_sql()+' AND '+self._dispatch_sql()+' ORDER BY sequence',
                (service_key(service), *states, json.dumps(context['scope']['scope_ids'])))
            return any(capture_delivery_authority(self.path,json.loads(row['payload']),delivery_id=row['id'],db=db,
                       dispatch_scope=dispatch_scope,_context=context)['references']
                       for row in rows)

    def has_followup_work(self, service, *, dispatch_scope=None, due_only=False):
        # A watchdog may wake only an already candidate-marked original job.
        # The existing immutable dispatch grant and every live generation remain
        # the authority; historical intents are never imported into this path.
        from preextract_task_delivery import capture_delivery_authority, _dispatch_context
        context = _dispatch_context(self.path, service, dispatch_scope)
        if not context['scope']['scope_ids']:
            return False
        with closing(self._connect()) as db:
            query = ("SELECT d.* FROM deliveries d WHERE service=? AND status IN ('queued','uploading') "
                     "AND json_extract(projection,'$.preextract_native_followup.schema')='native-queue-followup/1' "
                     "AND json_extract(projection,'$.preextract_native_followup.phase') IN ('waiting','submitted_or_unknown') AND "
                     + self._admission_sql() + ' AND ' + self._dispatch_sql() + ' ORDER BY sequence')
            if due_only:
                query = query.replace(' ORDER BY sequence',
                    " AND (status='uploading' OR COALESCE(json_extract(projection,'$.preextract_delivery_wait.next_attempt_at'),0)<=?) ORDER BY sequence")
            args = [service_key(service), json.dumps(context['scope']['scope_ids'])]
            if due_only:
                args.append(time.time())
            rows = db.execute(query, args)
            for row in rows:
                job = _row(row)
                authority = capture_delivery_authority(self.path, job['payload'], delivery_id=job['id'],
                    db=db, dispatch_scope=dispatch_scope, _context=context)
                if authority['references']:
                    return True
        return False

    @staticmethod
    def _dispatch_sql():
        # Restrict the candidate set before interpreting controls. A scope is
        # loaded only from the immutable native command ledger, never caller
        # supplied source pairs; the frozen authority remains the final guard.
        return """EXISTS (SELECT 1 FROM references_to_deliveries r WHERE r.delivery_id=d.id AND EXISTS (
            SELECT 1 FROM delivery_control_commands c,json_each(c.result,'$.references') a
            WHERE c.action='authorize_dispatch' AND c.request_id IN (SELECT value FROM json_each(?))
            AND r.run_id=json_extract(a.value,'$.run_id') AND r.source_hash=json_extract(a.value,'$.source_hash')
            AND (json_extract(c.result,'$.mode')='task' OR
                (r.delivery_id=json_extract(a.value,'$.delivery_id') AND r.item_id=json_extract(a.value,'$.item_id')))))"""

    @staticmethod
    def _admission_sql():
        # Per-run/task permission depends on every real shared reference, not
        # the first registrant kept in canonical payload. Apply it after this
        # global prefilter, identically in has_work and claim.
        return "NOT EXISTS (SELECT 1 FROM delivery_controls c WHERE c.paused=1 AND c.scope='*')"

    def control(self, *, run_id=None):
        from preextract_delivery_control import control_scope,read_fence
        scope=control_scope(run_id);fence=read_fence(self.path,scope)
        with closing(self._connect()) as db:
            row=db.execute('SELECT * FROM delivery_controls WHERE scope=?',(scope,)).fetchone()
        return dict(scope=scope,paused=bool(fence) or (bool(row['paused']) if row else False),
            generation=row['generation'] if row else 0,reason=row['reason'] if row else '',
            updated_at=row['updated_at'] if row else None,fence_id=fence['fence_id'] if fence else None,
            stop_pending=bool(fence and (not row or row['fence_id']!=fence['fence_id'] or not row['paused'])))

    def pause(self, *, run_id=None, reason='operator_stop', request_id=None, expected_generation=None):
        from preextract_delivery_control import pause_delivery
        return pause_delivery(self.path.parents[2],run_id=run_id,reason=reason,queue=self,
                              request_id=request_id,expected_generation=expected_generation)

    def _pause_record(self,scope,reason,fence_id,*,request_id=None,expected_generation=None):
        with closing(self._connect()) as db,db:
            db.execute('BEGIN IMMEDIATE')
            if expected_generation is not None:
                known = _control_command(db, request_id, scope, 'pause', expected_generation)
                if known is not None:return known
            if request_id:
                previous=db.execute('SELECT * FROM delivery_stop_commands WHERE request_id=?',(request_id,)).fetchone()
                if previous:
                    if previous['scope']!=scope:raise ValueError('delivery stop identity belongs to another scope')
                    return
            row=db.execute('SELECT * FROM delivery_controls WHERE scope=?',(scope,)).fetchone()
            generation=(row['generation'] if row else 0)
            if expected_generation is not None and generation != expected_generation:
                raise ValueError('delivery control generation changed; retain pending stop for reconciliation')
            if not row or not row['paused'] or row['fence_id']!=fence_id:
                generation+=1
                db.execute('INSERT OR REPLACE INTO delivery_controls VALUES(?,?,?,?,?,?)',
                    (scope,1,generation,reason,time.time(),fence_id))
            if request_id:
                db.execute('INSERT INTO delivery_stop_commands VALUES(?,?,?,?)',
                           (request_id,scope,fence_id,generation))
            if expected_generation is not None:
                current=db.execute('SELECT * FROM delivery_controls WHERE scope=?',(scope,)).fetchone()
                result=dict(current,paused=True,stop_pending=False)
                _save_control_command(db,request_id,scope,'pause',expected_generation,result)
                return result

    def resume(self, *, expected_generation, run_id=None, request_id=None):
        from contextlib import ExitStack
        from filelock import FileLock
        from preextract_delivery_control import control_scope,read_fence,fence_path,intent_lock_path,stop_identity_path
        scope=control_scope(run_id)
        if type(expected_generation) is not int or expected_generation<0:
            raise ValueError('invalid delivery control generation')
        if request_id is not None and (not isinstance(request_id,str) or not request_id or len(request_id)>128):
            raise ValueError('invalid delivery command identity')
        fence_path(self.path,scope).parent.mkdir(parents=True,exist_ok=True)
        with ExitStack() as locks:
            if request_id is not None:
                identity_path=stop_identity_path(self.path,request_id)
                identity_path.parent.mkdir(parents=True,exist_ok=True)
                locks.enter_context(FileLock(str(identity_path)+'.lock',timeout=5))
            locks.enter_context(FileLock(intent_lock_path(self.path,scope),timeout=5))
            locks.enter_context(FileLock(str(self.path)+'.admission.lock',timeout=5))
            fence=read_fence(self.path,scope)
            with closing(self._connect()) as db,db:
                db.execute('BEGIN IMMEDIATE')
                if request_id is not None:
                    known=_control_command(db,request_id,scope,'resume',expected_generation)
                    if known is not None:
                        current=db.execute('SELECT * FROM delivery_controls WHERE scope=?',(scope,)).fetchone()
                        # Finish only this command's interrupted fence unlink. A
                        # newer stop must survive an old command's retry.
                        if (fence and fence['fence_id']==known.get('cleared_fence_id') and current
                                and not current['paused'] and current['generation']==known['generation']):
                            fence_path(self.path,scope).unlink(missing_ok=True)
                        return known
                    if identity_path.exists() or db.execute('SELECT 1 FROM delivery_stop_commands WHERE request_id=?',(request_id,)).fetchone():
                        raise ValueError('delivery command identity already used by a stop')
                row=db.execute('SELECT * FROM delivery_controls WHERE scope=?',(scope,)).fetchone()
                if not row or type(expected_generation) is not int or row['generation']!=expected_generation:
                    raise ValueError('上传暂停代次已变化；需要明确恢复当前控制状态')
                if fence and row['fence_id']!=fence['fence_id']:
                    raise ValueError('停止尚未完整登记，保留停止栅栏待核对')
                if request_id is not None and not row['paused']:
                    if fence:
                        raise ValueError('earlier resume fence cleanup is pending; retry its original command')
                    # A technical body attention may be explicitly resumed
                    # while delivery is already open. Save an exact-version
                    # no-op in the same native command ledger: never fake a
                    # pause, refresh authority, launch or replay delivery.
                    result=dict(scope=scope,paused=False,generation=row['generation'],reason=row['reason'],
                        updated_at=row['updated_at'],fence_id=None,stop_pending=False,
                        cleared_fence_id=None,effect='already_open',changed=False)
                    _save_control_command(db,request_id,scope,'resume',expected_generation,result)
                    return result
                db.execute('UPDATE delivery_controls SET paused=0,generation=generation+1,reason=?,updated_at=? WHERE scope=?',
                    ('explicit_resume',time.time(),scope))
                if request_id is not None:
                    result=dict(scope=scope,paused=False,generation=row['generation']+1,reason='explicit_resume',
                        updated_at=time.time(),fence_id=None,stop_pending=False,
                        cleared_fence_id=fence['fence_id'] if fence else None,effect='resumed',changed=True)
                    _save_control_command(db,request_id,scope,'resume',expected_generation,result)
            fence_path(self.path,scope).unlink(missing_ok=True)
        return result if request_id is not None else self.control(run_id=run_id)

    def generations(self, run_id):
        return {scope:self.control(run_id=rid)['generation'] for scope,rid in (('*',None),('run:'+run_id,run_id))}

    def deferred(self, job, *, reason='operation_lock_busy', delay=.25):
        work=Path(job['payload']['item']['preextract_work_dir'])
        if any((work/name).exists() for name in ('delivery-binding.json','delivery-receipt.json','associated-delivery-receipt.json')):
            from preextract_native_followup import recovered_projection
            follow = recovered_projection(self, job)
            if follow is not None:
                return self.update(job['id'],status='queued',expected_status='uploading',projection=follow)
            return self.update(job['id'],status='needs_review',expected_status='uploading',projection=dict(
                handoff_status='pending',preextract_handoff_blocked=True,
                handoff_reason='已有远端交付意图或回执，保留原证据等待明确核对；未自动重排'))
        return self.update(job['id'],status='queued',expected_status='uploading',projection=dict(
            handoff_status='queued',preextract_handoff_blocked=True,
            handoff_reason='工作锁忙碌，保留成品等待原处理者释放' if reason=='operation_lock_busy' else
                '工作线程未启动，封包保留等待接续' if reason=='executor_submit_failed' else '上传准入已暂停，成品与已有结果保留',
            preextract_delivery_wait=dict(reason=reason,next_attempt_at=time.time()+delay,owner='not_observed')))

    def claim(self, service, *, dispatch_scope=None, followup_only=False):
        from filelock import FileLock
        from preextract_task_delivery import _dispatch_context
        context = _dispatch_context(self.path, service, dispatch_scope)
        if not context['scope']['scope_ids']:
            return None
        with FileLock(str(self.path)+'.admission.lock',timeout=5):
            return self._claim_admitted(service, dispatch_scope=dispatch_scope, context=context, followup_only=followup_only)

    def _claim_admitted(self,service,*,dispatch_scope=None,context=None,followup_only=False):
        from preextract_task_delivery import capture_delivery_authority
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            follow_filter = (" AND json_extract(projection,'$.preextract_native_followup.schema')='native-queue-followup/1' "
                             "AND json_extract(projection,'$.preextract_native_followup.phase') IN ('waiting','submitted_or_unknown')"
                             if followup_only else '')
            rows = db.execute("SELECT * FROM deliveries d WHERE service=? AND status='queued' AND json_extract(payload,'$.item.prepared_scope') IS NULL AND " +
                self._admission_sql()+" AND "+self._dispatch_sql()+follow_filter+
                " AND COALESCE(json_extract(projection,'$.preextract_delivery_wait.next_attempt_at'),0)<=? ORDER BY sequence",
                              (service_key(service),json.dumps(context['scope']['scope_ids']),time.time()))
            for row in rows:
                result=_row(row)
                authority=capture_delivery_authority(self.path,result['payload'],delivery_id=result['id'],db=db,
                    dispatch_scope=dispatch_scope,_context=context)
                if not authority['references']:continue
                db.execute("UPDATE deliveries SET status='uploading',updated_at=? WHERE id=?", (time.time(), row['id']))
                result['status']='uploading'
                result['delivery_authority']=authority
                result['dispatch_scope']=authority['dispatch_scope']
                # Retain this compatibility field; QueueStop uses the frozen
                # per-reference authority, and never retargets the payload.
                result['control_generations']=self.generations(result['payload']['run_id'])
                return result
            return None

    def update(self, identity, *, status=None, projection=None, expected_status=None):
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            current = db.execute('SELECT projection,status FROM deliveries WHERE id=?', (identity,)).fetchone()
            if expected_status is not None and current['status'] != expected_status:
                return False
            fields = json.loads(current['projection'])
            for key, value in (projection or {}).items():
                if key in DELIVERY_FIELDS:
                    fields[key] = value
            db.execute('UPDATE deliveries SET status=?,projection=?,updated_at=? WHERE id=?',
                       (status or current['status'], _json(fields), time.time(), identity))
            return True

    def update_scoped(self, identity, *, dispatch_scope, status=None, projection=None, expected_status=None):
        from preextract_task_delivery import capture_delivery_authority, _dispatch_context
        from global_task_control import GlobalTaskControl, GlobalPaused
        # Service comes from the retained row; a caller cannot retarget a job.
        if dispatch_scope is None:
            return False
        current = self.get(identity)
        if current is None:
            return False
        gate = GlobalTaskControl(self.path.resolve().parents[2])
        if not gate.path.is_file():
            return False
        # This short writer owns G -> Q. Call outside an existing G admission;
        # claim instead runs under the worker's original outer G admission.
        try:
            with gate.admission():
                context = _dispatch_context(self.path, current['service'], dispatch_scope)
                if not context['scope']['scope_ids']:
                    return False
                with closing(self._connect()) as db, db:
                    db.execute('BEGIN IMMEDIATE')
                    row = db.execute('SELECT * FROM deliveries WHERE id=?', (identity,)).fetchone()
                    if row is None or expected_status is not None and row['status'] != expected_status:
                        return False
                    authority = capture_delivery_authority(self.path, json.loads(row['payload']), delivery_id=identity, db=db,
                        dispatch_scope=dispatch_scope, _context=context)
                    if not authority['references']:
                        return False
                    fields = json.loads(row['projection'])
                    fields.update({key: value for key, value in (projection or {}).items() if key in DELIVERY_FIELDS})
                    db.execute('UPDATE deliveries SET status=?,projection=?,updated_at=? WHERE id=?',
                               (status or row['status'], _json(fields), time.time(), identity))
                    return True
        except GlobalPaused:
            return False

    def recover(self, service, *, dispatch_scope=None, followup_only=False):
        """Caller must own the worker lock; never replay an uncertain remote write."""
        def update(job, **changes):
            return self.update_scoped(job['id'], dispatch_scope=job['dispatch_scope'],
                                      expected_status='uploading', **changes)
        for job in self.scoped_list(service, status='uploading', dispatch_scope=dispatch_scope):
            if followup_only:
                follow = job['projection'].get('preextract_native_followup') or {}
                if follow.get('schema') != 'native-queue-followup/1' or not follow.get('requests'):
                    continue
            from preextract_retirement import terminal
            if terminal(dict(job,status='complete')):
                update(job,status='complete')
                continue
            if job['payload']['item'].get('prepared_scope'):continue
            work = Path(job['payload']['item']['preextract_work_dir'])
            if job['payload'].get('kind')=='associated_original':
                path=work/'associated-delivery-receipt.json'
                try:complete=path.is_file() and json.loads(path.read_text(encoding='utf8')).get('status')=='complete'
                except (OSError,ValueError):complete=False
                state='queued' if complete or not path.exists() else 'needs_review'
                update(job,status=state,projection={'associated_delivery':dict(status=state,
                    reason='附件交付中断，保留原意图及回执待核对',service=job['service'],
                    parse_status=job['payload']['material'].get('parse_status','pending'))})
                continue
            receipt = work / 'delivery-receipt.json'
            binding = work / 'delivery-binding.json'
            try:
                completed = receipt.exists() and json.loads(receipt.read_text(encoding='utf8')).get('status') == 'complete'
            except (ValueError, OSError):
                completed = False
            if completed or (not receipt.exists() and not binding.exists()):
                update(job, status='queued')
            else:
                from preextract_native_followup import recovered_projection
                follow = recovered_projection(self, job)
                if follow is not None:
                    update(job, status='queued', projection=follow)
                else:
                    update(job, status='needs_review', projection=dict(
                        handoff_status='pending', preextract_handoff_blocked=True,
                        handoff_reason='上次交付中断且存在远端操作记录，保留原包与回执等待核对'))

    def bind_prepared(self,identity,marker):
        with closing(self._connect()) as db,db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT payload,status FROM deliveries WHERE id=?',(identity,)).fetchone()
            if not row:raise ValueError('missing native delivery')
            payload=json.loads(row['payload']);prior=payload['item'].get('prepared_scope')
            if row['status']=='uploading' and not prior:raise ValueError('legacy delivery still owns this package')
            if prior and any(prior.get(k)!=marker.get(k) for k in ('database','scope_id','task_id')):
                raise ValueError('delivery already bound to another prepared scope')
            payload['item']['prepared_scope']=marker
            db.execute('UPDATE deliveries SET payload=? WHERE id=?',(_json(payload),identity))


def project_manifest(root, manifest):
    """Read-only overlay: a producer's stale save cannot erase delivery facts."""
    path = queue_path(root)
    if not path.is_file():
        return manifest
    items = {item['id']: item for item in manifest.get('items', [])}
    if not items:
        return manifest
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
        db.row_factory = sqlite3.Row
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not {'deliveries', 'references_to_deliveries'} <= tables:
            # SQLite exposes the new file before first-time schema setup ends.
            return manifest
        rows = []
        identities = list(items)
        for start in range(0,len(identities),500):
            batch = identities[start:start+500]
            marks = ','.join('?' for _ in batch)
            rows.extend(db.execute('''SELECT d.*,r.item_id,r.package_path,r.source_hash
                FROM references_to_deliveries r JOIN deliveries d ON d.id=r.delivery_id
                WHERE r.run_id=? AND r.item_id IN ('''+marks+')',
                [manifest.get('run_id'),*batch]).fetchall())
        if any(i.get('content_format')=='paper_preextract' for i in items.values()):
            from associated_pending import project_dependencies
            attachments=db.execute("SELECT service,status,payload FROM deliveries WHERE json_extract(payload,'$.kind')='associated_original'").fetchall()
            project_dependencies(items.values(),attachments)
    for row in rows:
        item = items.get(row['item_id'])
        payload=json.loads(row['payload'])
        if item is not None and item.get('content_format')=='associated_material' and payload.get('kind')=='associated_original':
            from associated_pending import project_item
            project_item(item,payload,json.loads(row['projection']),row['status'])
            continue
        if item is None or item.get('content_format') != 'paper_preextract':
            continue
        item['preextract_delivery_queue'] = dict(mode=MODE, id=row['id'], status=row['status'],
            path=str(path), service=row['service'], queued_at=row['created_at'])
        if (str(Path(item.get('package_path', '')).resolve()) != row['package_path']
                or item.get('source_hash', '') != row['source_hash']):
            item['preextract_delivery_queue']['status'] = 'needs_review'
            item.update(handoff_status='pending',preextract_handoff_blocked=True,
                        handoff_reason='当前条目与已排队成品身份不一致，保留记录等待核对')
            continue
        payload = json.loads(row['payload'])
        try:
            current_hash = hashlib.sha256((Path(row['package_path']) / 'manifest.json').read_bytes()).hexdigest()
        except OSError:
            current_hash = None
        from preextract_retirement import terminal
        if current_hash != payload['manifest_sha256'] and not (current_hash is None and terminal(_row(row))):
            item['preextract_delivery_queue']['status'] = 'needs_review'
            item.update(handoff_status='pending',preextract_handoff_blocked=True,
                        handoff_reason='排队后封包清单发生变化或不可读取，不能沿用旧交付状态')
            continue
        item.update(json.loads(row['projection']))
        state = row['status']
        if state in {'queued', 'uploading'}:
            item['handoff_status'] = state
            item['preextract_handoff_blocked'] = True
            item['handoff_reason'] = '成品已落盘排队，等待上传名额' if state == 'queued' else '正在按原流程交付及读回'
            wait=item.get('preextract_delivery_wait') or {}
            if state=='queued' and wait.get('reason')=='operation_lock_busy':item['handoff_reason']='等待本篇工作锁释放后继续交付'
    return manifest


def load_for_item_ids(store, run_id, item_ids):
    """Fresh selected delivery overlay, retaining the COMPLETE producer batch.

    Other items remain exactly as saved. Their authoritative delivery facts
    remain in the queue and are still projected by every ordinary Store.load.
    Selected package identity/hash/state validation is unchanged.
    """
    from engine import Store,read_json
    if item_ids is None or type(store) is not Store:
        return store.load(run_id)
    selected = set(item_ids)
    with store.lock:
        manifest = read_json(store.manifest_path(run_id))
        shadow = dict(manifest,items=[item for item in manifest.get('items',[]) if item['id'] in selected])
        project_manifest(store.root,shadow)
        return manifest


def enqueue_items(store, run_id, items, service, progress=lambda *a: None, *, retry=False):
    from filelock import Timeout
    from preextract_handoff import _save
    queue = DeliveryQueue(queue_path(store.root))
    # One durable snapshot for the group, not a full batch reload per package.
    items = list(items)
    saved = {row['id']: row for row in load_for_item_ids(store,run_id,(item['id'] for item in items))['items']}
    for item in items:
        try:
            from filelock import FileLock
            work = Path(saved[item['id']]['preextract_work_dir'])
            # Serialize pointer reads and durable registration with packaging.
            with FileLock(str(work / 'operation.lock'), timeout=0):
                payload = package_reference(store, run_id, item['id'], service, item=saved[item['id']])
                queue.enqueue(payload, retry=retry, workspace_locked=True)
            progress('RAGFlow 成品已落盘排队', item.get('title', ''))
        except Timeout:
            progress('RAGFlow 成品登记稍后继续', '本篇正在使用或清理；已确认的入库结果保持不变')
        except (ValueError, OSError, KeyError, TypeError, sqlite3.Error) as exc:
            reason = f'成品登记未完成（{type(exc).__name__}）；原包保留，其他成品继续排队'
            _save(store, run_id, item['id'], dict(handoff_status='pending',
                  handoff_reason=reason, preextract_handoff_blocked=False))
            progress('RAGFlow 成品等待重新登记', reason)
    return queue
