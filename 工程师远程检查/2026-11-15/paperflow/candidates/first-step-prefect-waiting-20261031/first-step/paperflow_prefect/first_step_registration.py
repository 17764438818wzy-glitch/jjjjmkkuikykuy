"""Register a completed first-step delivery without starting downstream work."""
from pathlib import Path
import json
import os
import re
import sqlite3

from filelock import FileLock, Timeout

from .first_step_control import ControlError, PauseRequested
from .intake_handoff import receive_handoff
from .receipts import ReceiptStore
from .records import identity


def registration_history_roots(data_root, configured=()):
    """Search historical work, excluding this workspace's upload staging."""
    data = Path(data_root).resolve()
    excluded = {'uploads', 'tusd', 'control-locks'}
    roots = ([path.resolve() for path in data.iterdir()
              if path.name.casefold() not in excluded and (path.is_dir() or path.name == 'inventory.json')]
             if data.is_dir() else [data])
    roots.extend(Path(path).resolve() for path in configured)
    return tuple(dict.fromkeys(roots))


def register_delivery(delivery_dir, *, receipt_root, history_roots, lock_path, stop_requested=None):
    """Queue registration; only an occupied writer, never the wait, uses SQLite."""
    lock_path = Path(lock_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock = FileLock(str(lock_path))
    while True:
        if stop_requested is not None and stop_requested():
            raise PauseRequested()
        try:
            lock.acquire(timeout=0.2)
            break
        except Timeout:
            continue
    try:
        if stop_requested is not None and stop_requested():
            raise PauseRequested()
        for value in history_roots:
            root = Path(value)
            if root.is_dir():
                with os.scandir(root) as entries:
                    next(entries, None)
            elif root.is_file():
                with root.open('rb') as stream:
                    stream.read(1)
            else:
                raise FileNotFoundError('历史数据位置不可用：' + str(root))
        store = ReceiptStore(receipt_root, history_roots=history_roots)
        return receive_handoff(store=store, delivery_dir=delivery_dir, retain_unused_delivery_history=False,
                               strict_history=True)
    finally:
        lock.release()


def task_receipt_plan(receipt_root, task_output):
    """Find receipts owned by one task; retain receipts needed by other tasks."""
    root, task_output = Path(receipt_root).resolve(), Path(task_output).resolve()
    database = root / 'receipts.sqlite3'
    if not database.is_file():
        return []
    with sqlite3.connect(database) as db:
        rows = db.execute('SELECT receipt_id, canonical_id, payload FROM receipt_events').fetchall()
    events, owned = [], []
    for receipt_id, canonical, raw in rows:
        try:
            payload = json.loads(raw)
            ref = (payload.get('delivery_ref') or {}).get('path')
            is_owned = bool(ref) and Path(ref).resolve().is_relative_to(task_output)
            events.append((receipt_id, payload, is_owned))
            if not is_owned:
                continue
            expected = root / 'receipts' / (receipt_id + '.json')
            original = {key: value for key, value in payload.items() if key not in {'receipt_id', 'record_path'}}
            if (not re.fullmatch('[0-9a-f]{64}', receipt_id) or payload.get('receipt_id') != receipt_id
                    or identity(original) != receipt_id or Path(payload['record_path']).resolve() != expected):
                raise ValueError('登记回执归属无法确认')
            owned.append(dict(receipt_id=receipt_id, canonical_id=canonical, delivery_path=str(Path(ref).resolve())))
        except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
            raise ControlError('登记记录无法核对，任务暂不能删除') from exc
    owned_ids = {entry['receipt_id'] for entry in owned}
    for _, payload, is_owned in events:
        if is_owned:
            continue
        for item in payload.get('items', []):
            if any(ref.get('receipt_id') in owned_ids for ref in item.get('delivery_history', [])):
                raise ControlError('其他任务的登记记录仍引用本任务，请先处理其他任务')
    return owned


def remove_task_receipt_events(receipt_root, entries):
    """Remove only planned events; shared registered identities remain in use."""
    if not entries:
        return
    root = Path(receipt_root).resolve()
    database = root / 'receipts.sqlite3'
    if not database.is_file():
        raise ControlError('登记数据库不可用，无法完成任务清理')
    ids = {entry['receipt_id'] for entry in entries}
    with sqlite3.connect(database, timeout=30, isolation_level=None) as db:
        db.execute('PRAGMA foreign_keys=ON')
        db.execute('BEGIN IMMEDIATE')
        events = db.execute('SELECT receipt_id, canonical_id, payload FROM receipt_events').fetchall()
        payloads = {}
        for receipt_id, canonical, raw in events:
            payload = json.loads(raw)
            payloads[receipt_id] = (canonical, payload)
            if receipt_id not in ids:
                for item in payload.get('items', []):
                    if any(ref.get('receipt_id') in ids for ref in item.get('delivery_history', [])):
                        raise ControlError('其他任务的登记记录仍引用本任务，请先处理其他任务')
        def evidence_versions(payload):
            return {(identity([ref['group_id'], ref['source']]), ref['evidence_version'])
                    for item in payload.get('items', []) for ref in item.get('evidence_records', [])}
        remaining_evidence = set().union(*(evidence_versions(payload) for rid, (_, payload) in payloads.items()
                                           if rid not in ids))
        removed_evidence = set()
        for entry in entries:
            current = payloads.get(entry['receipt_id'])
            if current is None:
                continue
            canonical, payload = current
            original = {key: value for key, value in payload.items() if key not in {'receipt_id', 'record_path'}}
            if (canonical != entry['canonical_id'] or
                    payload.get('receipt_id') != entry['receipt_id'] or identity(original) != entry['receipt_id'] or
                    str(Path(payload['delivery_ref']['path']).resolve()) != entry['delivery_path']):
                raise ControlError('登记记录归属已变化，任务暂不能删除')
            removed_evidence.update(evidence_versions(payload))
            db.execute('DELETE FROM receipt_events WHERE receipt_id=?', (entry['receipt_id'],))
        for key, version in removed_evidence - remaining_evidence:
            db.execute('DELETE FROM evidence_versions WHERE source_key=? AND evidence_version=?', (key, version))
        for canonical in {entry['canonical_id'] for entry in entries}:
            if db.execute('SELECT 1 FROM receipt_events WHERE canonical_id=? LIMIT 1', (canonical,)).fetchone():
                continue
            for table in ('history_links', 'file_versions', 'file_aliases', 'files', 'papers', 'aliases', 'groups'):
                db.execute('DELETE FROM ' + table + ' WHERE canonical_id=?', (canonical,))
        db.commit()
