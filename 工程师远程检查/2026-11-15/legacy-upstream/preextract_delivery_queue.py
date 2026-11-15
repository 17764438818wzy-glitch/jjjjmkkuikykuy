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
                   'preextract_associated_delivery')


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
                for (payload,) in db.execute('SELECT payload FROM deliveries'):
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


def package_reference(store, run_id, item_id, service, *, item=None):
    """Register only a passed package at its existing publication path."""
    if item is None:
        item = next(row for row in store.load(run_id)['items'] if row['id'] == item_id)
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
        'package_path', 'preextract_work_dir') if key in item}
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
                CREATE TABLE IF NOT EXISTS references_to_deliveries (
                    run_id TEXT NOT NULL, item_id TEXT NOT NULL, delivery_id TEXT NOT NULL,
                    package_path TEXT NOT NULL, source_hash TEXT NOT NULL,
                    PRIMARY KEY(run_id,item_id)
                );
            ''')

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
                       'VALUES(?,?,?,?,?,?)', (payload['id'], payload['service'], 'queued', _json(payload), now, now))
            db.execute('INSERT OR IGNORE INTO references_to_deliveries VALUES(?,?,?,?,?)',
                       (payload['run_id'], item['id'], payload['id'],
                        str(Path(item['package_path']).resolve()), item.get('source_hash', '')))
            current = db.execute('SELECT status,payload FROM deliveries WHERE id=?', (payload['id'],)).fetchone()
            recheck = ((retry and current['status'] in {'failed', 'complete'}) or
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

    def has_work(self, service, *, include_interrupted=False):
        states = ('queued', 'uploading') if include_interrupted else ('queued',)
        with closing(self._connect()) as db:
            return db.execute('SELECT 1 FROM deliveries WHERE service=? AND status IN (' +
                ','.join('?' for _ in states) + ') LIMIT 1', (service_key(service), *states)).fetchone() is not None

    def claim(self, service):
        with closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute("SELECT * FROM deliveries WHERE service=? AND status='queued' ORDER BY sequence LIMIT 1",
                             (service_key(service),)).fetchone()
            if row is None:
                return None
            db.execute("UPDATE deliveries SET status='uploading',updated_at=? WHERE id=?", (time.time(), row['id']))
            result = _row(row)
            result['status'] = 'uploading'
            return result

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

    def recover(self, service):
        """Caller must own the worker lock; never replay an uncertain remote write."""
        for job in self.list(service, status='uploading'):
            work = Path(job['payload']['item']['preextract_work_dir'])
            receipt = work / 'delivery-receipt.json'
            binding = work / 'delivery-binding.json'
            try:
                completed = receipt.exists() and json.loads(receipt.read_text(encoding='utf8')).get('status') == 'complete'
            except (ValueError, OSError):
                completed = False
            if completed or (not receipt.exists() and not binding.exists()):
                self.update(job['id'], status='queued')
            else:
                self.update(job['id'], status='needs_review', projection=dict(
                    handoff_status='pending', preextract_handoff_blocked=True,
                    handoff_reason='上次交付中断且存在远端操作记录，保留原包与回执等待核对'))


def project_manifest(root, manifest):
    """Read-only overlay: a producer's stale save cannot erase delivery facts."""
    path = queue_path(root)
    if not path.is_file():
        return manifest
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
        db.row_factory = sqlite3.Row
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not {'deliveries', 'references_to_deliveries'} <= tables:
            # SQLite exposes the new file before first-time schema setup ends.
            return manifest
        rows = db.execute('''SELECT d.*,r.item_id,r.package_path,r.source_hash
            FROM references_to_deliveries r JOIN deliveries d ON d.id=r.delivery_id
            WHERE r.run_id=?''', (manifest.get('run_id'),)).fetchall()
    items = {item['id']: item for item in manifest.get('items', [])}
    for row in rows:
        item = items.get(row['item_id'])
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
        if current_hash != payload['manifest_sha256']:
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
    return manifest


def enqueue_items(store, run_id, items, service, progress=lambda *a: None, *, retry=False):
    from filelock import Timeout
    from preextract_handoff import _save
    queue = DeliveryQueue(queue_path(store.root))
    # One durable snapshot for the group, not a full batch reload per package.
    saved = {row['id']: row for row in store.load(run_id)['items']}
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
