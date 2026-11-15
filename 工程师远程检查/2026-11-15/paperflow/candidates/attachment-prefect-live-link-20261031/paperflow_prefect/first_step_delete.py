"""Delete one first-step task's own material without touching its original source."""
from contextlib import closing, nullcontext
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import stat
import time

from filelock import FileLock, Timeout

from .first_step_control import ControlError, _uuid
from .records import identity


def _check_conversion_processes(control, task_id):
    directory = control.data / 'control-locks' / 'first-step-office'
    for marker in directory.glob('*.uncertain') if directory.is_dir() else ():
        try:
            state = json.loads(marker.read_text(encoding='utf8'))
        except FileNotFoundError:
            continue
        except (OSError, ValueError, TypeError) as exc:
            raise ControlError('无法确认转换进程归属，请先核对后再删除') from exc
        if not isinstance(state, dict):
            raise ControlError('无法确认转换进程归属，请先核对后再删除')
        if state.get('scope') == 'offline' and state.get('task_id') is None:
            continue
        try:
            owner = _uuid(state.get('task_id'))
        except ControlError as exc:
            raise ControlError('无法确认转换进程归属，请先核对后再删除') from exc
        if owner == task_id:
            raise ControlError('任务的转换进程尚未确认退出，请先核对后再删除')


def _build_plan(control, db, task):
    task_id = task['task_id']
    plan = []
    shared = []
    shared_caches = 0
    unassigned_caches = 0
    seen = set()

    def source_hashes(handoff):
        rows = ([handoff['body']] if handoff.get('body') else [])
        for key in ('attachments', 'trash', 'unresolved'):
            rows += handoff.get(key, [])
        return {row['sha256'].lower() for row in rows if row.get('sha256')}

    def add(kind, root, path):
        root, path = Path(root).absolute(), Path(path).absolute()
        try:
            relative = path.relative_to(root)
        except ValueError:
            raise ControlError('无法确认待清理文件的归属') from None
        if not relative.parts or '..' in relative.parts:
            raise ControlError('无法确认待清理文件的归属')
        key = (kind, relative.as_posix())
        if key not in seen:
            seen.add(key)
            plan.append({'root': kind, 'relative': relative.as_posix()})

    output, data = control.output, control.data
    receipt_records = []
    if task['artifact_scoped']:
        add('output', output, output / 'tasks' / task_id)
        from .first_step_registration import task_receipt_plan
        receipt_records = task_receipt_plan(control.receipt_root, output / 'tasks' / task_id)
        for entry in receipt_records:
            add('receipt', control.receipt_root, control.receipt_root / 'receipts' / (entry['receipt_id'] + '.json'))
    else:
        own_hashes = set()
        group_ids = {row['group_id'] for row in db.execute(
            'SELECT group_id FROM control_groups WHERE task_id=?', (task_id,)) if row['group_id']}
        for group_id in sorted(group_ids):
            if not re.fullmatch(r'[0-9a-f]{32}', group_id):
                raise ControlError('旧任务的资料组归属无法确认')
            in_use = db.execute('SELECT 1 FROM control_groups g JOIN tasks t ON t.task_id=g.task_id '
                                'WHERE g.group_id=? AND g.task_id!=? LIMIT 1', (group_id, task_id)).fetchone()
            if in_use:
                shared.append(group_id)
                continue
            handoffs = [*output.glob(group_id + '-*.json'),
                        *(output / 'unreceived').glob(group_id + '-*.json')]
            for handoff_path in handoffs:
                try:
                    with handoff_path.open(encoding='utf-8') as stream:
                        handoff = json.load(stream)
                    if handoff.get('group_id') != group_id:
                        raise ValueError('group mismatch')
                    own_hashes.update(source_hashes(handoff))
                    for row in handoff.get('attachments', []):
                        key = identity([group_id, handoff['handoff_id'], handoff['result_version'],
                                        row['file_id'], row['sha256'].lower()])[:32]
                        add('output', output, output / 'attachment-evidence' / key)
                except (OSError, ValueError, TypeError, KeyError, AttributeError):
                    raise ControlError('旧任务的附件证据归属无法确认，清理未开始') from None
                add('output', output, handoff_path)
            for path in (output / 'classification-intents').glob(group_id + '-*.json'):
                add('output', output, path)
            add('output', output, output / 'deliveries' / group_id)
        if control.legacy_conversion_root.is_dir():
            other_hashes = set()
            other_groups = db.execute('SELECT DISTINCT g.group_id FROM control_groups g JOIN tasks t '
                'ON t.task_id=g.task_id WHERE g.task_id!=? AND t.artifact_scoped=0 AND g.group_id IS NOT NULL',
                (task_id,)).fetchall()
            for other in other_groups:
                group_id = other['group_id']
                if not re.fullmatch(r'[0-9a-f]{32}', group_id):
                    raise ControlError('其他旧任务的资料归属无法确认')
                for path in [*output.glob(group_id + '-*.json'), *(output / 'unreceived').glob(group_id + '-*.json')]:
                    try:
                        with path.open(encoding='utf-8') as stream:
                            other_hashes.update(source_hashes(json.load(stream)))
                    except (OSError, ValueError, TypeError, KeyError, AttributeError):
                        raise ControlError('其他旧任务的转换缓存归属无法确认，清理未开始') from None
            for record in control.legacy_conversion_root.glob('*/*/identity.json'):
                try:
                    with record.open(encoding='utf-8') as stream:
                        digest = json.load(stream)['source_sha256'].lower()
                except (OSError, ValueError, TypeError, KeyError, AttributeError):
                    unassigned_caches += 1
                    continue
                if digest in own_hashes:
                    if digest in other_hashes:
                        shared_caches += 1
                    else:
                        add('conversion', control.legacy_conversion_root, record.parent)
                elif digest not in other_hashes:
                    unassigned_caches += 1
    add('output', output, output / 'group-task-summaries' / (task_id + '.json'))

    upload_id = task['upload_id']
    if upload_id:
        upload_id = _uuid(upload_id)
        upload_root = data / 'uploads' / upload_id
        for row in db.execute('SELECT source_root FROM tasks WHERE task_id!=? AND source_root IS NOT NULL', (task_id,)):
            other_source = Path(row['source_root']).resolve()
            if other_source == upload_root.resolve() or other_source.is_relative_to(upload_root.resolve()):
                raise ControlError('其他任务正在使用此任务的上传副本，无法清理')
        add('data', data, upload_root)
        for row in db.execute('SELECT tus_id FROM tus_manifest WHERE upload_id=? AND tus_id IS NOT NULL', (upload_id,)):
            tus_id = row['tus_id']
            if not re.fullmatch(r'[0-9a-f]{32}', tus_id):
                raise ControlError('上传暂存文件的归属无法确认')
            add('data', data, control.tus_dir / tus_id)
            add('data', data, control.tus_dir / (tus_id + '.info'))
    return {'paths': plan, 'shared_groups': shared, 'shared_caches': shared_caches,
            'unassigned_caches': unassigned_caches, 'receipt_records': receipt_records}


def _remove_path(control, entry):
    root = (control.output if entry['root'] == 'output' else control.data if entry['root'] == 'data' else
            control.receipt_root if entry['root'] == 'receipt' else
            control.legacy_conversion_root if entry['root'] == 'conversion' else None)
    if root is None:
        raise ValueError('unknown cleanup root')
    relative = Path(entry['relative'])
    if relative.is_absolute() or not relative.parts or '..' in relative.parts:
        raise ValueError('invalid cleanup path')
    path = root / relative
    for parent in path.parents:
        if parent == root:
            break
        if parent.is_symlink() or parent.is_junction():
            raise ValueError('cleanup path has a linked parent')
    def remove_readonly(function, name, error):
        item = Path(name)
        if not isinstance(error, PermissionError) or item.is_symlink() or item.is_junction():
            raise error
        os.chmod(item, stat.S_IREAD | stat.S_IWRITE)
        function(name)

    if path.is_symlink():
        path.unlink()
    elif path.is_junction():
        path.rmdir()
    elif path.is_dir():
        if entry['root'] == 'conversion':
            lock_path = path.with_name(path.name + '.lock')
            try:
                with FileLock(str(lock_path), timeout=0):
                    shutil.rmtree(path, onexc=remove_readonly)
                lock_path.unlink(missing_ok=True)
            except Timeout as exc:
                raise OSError('转换缓存正在使用') from exc
        else:
            shutil.rmtree(path, onexc=remove_readonly)
    elif path.exists():
        try:
            path.unlink()
        except PermissionError as exc:
            remove_readonly(os.unlink, str(path), exc)


def delete_task(control, task_id):
    task_id = _uuid(task_id)
    registration_lock = control.data / 'control-locks' / 'first-step-registration.lock'
    if (control.receipt_root / 'receipts.sqlite3').is_file():
        registration_lock.parent.mkdir(parents=True, exist_ok=True)
        gate = FileLock(str(registration_lock))
    else:
        gate = nullcontext()
    with gate, control._lock('task', task_id), closing(control.summary._connect()) as db:
        task = control._task(db, task_id)
        upload_guard = control._lock('upload', task['upload_id']) if task['upload_id'] else nullcontext()
        with upload_guard:
            return _delete_locked(control, db, task_id)


def _delete_locked(control, db, task_id):
        with db:
            db.execute('BEGIN IMMEDIATE')
            task = control._task(db, task_id)
            unknown_rows = db.execute("SELECT d.*,g.source_digest,g.group_root FROM model_decisions d "
                "JOIN control_groups g ON g.task_id=d.task_id AND g.seq=d.seq "
                "WHERE d.task_id=? AND d.state='unknown'", (task_id,)).fetchall()
            for row in unknown_rows:
                try:
                    scope = control.group_scope(task, row['group_root'])
                except (TypeError, OSError, ValueError):
                    scope = None
                db.execute('INSERT OR REPLACE INTO deleted_unknown_requests '
                    '(task_id,seq,step,source_digest,group_scope,request_scope,request_kind,request_members) '
                    'VALUES (?,?,?,?,?,?,?,?)',
                    (task_id, row['seq'], row['step'], row['source_digest'], scope,
                     row['request_scope'], row['request_kind'], row['request_members']))
            busy = (task['task_status'] in {'starting', 'running', 'pausing'} or task['start_pending']
                    or control.has_upload_transfers(task_id)
                    or db.execute("SELECT 1 FROM control_groups WHERE task_id=? AND state='running' LIMIT 1", (task_id,)).fetchone()
                    or db.execute("SELECT 1 FROM model_decisions WHERE task_id=? AND state='inflight' LIMIT 1", (task_id,)).fetchone())
            if busy:
                raise ControlError('任务正在上传或处理，请等待完成或暂停后再删除')
            _check_conversion_processes(control, task_id)
            plan = json.loads(task['delete_plan']) if task['delete_plan'] else _build_plan(control, db, task)
            db.execute("UPDATE tasks SET task_status='deleting',desired_state='paused',delete_plan=?,"
                       "updated_at_ns=?,control_revision=control_revision+1 WHERE task_id=?",
                       (json.dumps(plan, ensure_ascii=False), time.time_ns(), task_id))
            if task['upload_id']:
                db.execute("UPDATE uploads SET state='deleting' WHERE upload_id=?", (task['upload_id'],))
        try:
            from .first_step_registration import remove_task_receipt_events
            remove_task_receipt_events(control.receipt_root, plan.get('receipt_records', []))
            for entry in plan['paths']:
                _remove_path(control, entry)
        except (OSError, ValueError, KeyError, sqlite3.Error, ControlError) as exc:
            with db:
                db.execute("UPDATE tasks SET task_status='delete_failed',stop_reason=?,updated_at_ns=? WHERE task_id=?",
                           ('清理未完成：' + type(exc).__name__, time.time_ns(), task_id))
            if isinstance(exc, ControlError):
                raise
            raise ControlError('部分文件清理失败，请检查文件占用或权限后重试', 500) from exc
        with db:
            db.execute('BEGIN IMMEDIATE')
            task = control._task(db, task_id)
            if task['upload_id']:
                db.execute('DELETE FROM tus_manifest WHERE upload_id=?', (task['upload_id'],))
                db.execute('DELETE FROM uploads WHERE upload_id=?', (task['upload_id'],))
            for table in ('group_results', 'model_decisions', 'control_groups', 'task_create_requests'):
                db.execute('DELETE FROM ' + table + ' WHERE task_id=?', (task_id,))
            db.execute('DELETE FROM tasks WHERE task_id=?', (task_id,))
        return {'task_id': task_id, 'deleted': True, 'shared_groups_retained': len(plan['shared_groups']),
                'shared_conversion_caches_retained': plan.get('shared_caches', 0),
                'unassigned_conversion_caches_retained': plan.get('unassigned_caches', 0),
                'unknown_history_retained': len(unknown_rows)}
