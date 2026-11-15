"""Isolated deletion copy with folder postprocessing stop barriers.

Three-way merged onto llm-new-prefect-20261031/flow_control/first_step_delete.py
SHA256 bcf114765add31c9785dac22c453603fd4c4ae2d863bb6c3bce9333fd5061fa8.
Never installed or invoked for live data by this candidate.
"""
from contextlib import closing, contextmanager, ExitStack, nullcontext
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import stat
import time

from filelock import FileLock, Timeout

from .first_step_control import ControlError, _process_dead, _start_is_abandoned, _uuid
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
    with closing(control.summary._connect()) as db:
        if control._task(db, task_id)['prefect_order_model'] != 'v2':
            raise ControlError('旧任务仍属于原工单服务，请使用原服务处理', 409)
    _request_delete(control, task_id)
    _tombstone_local_task(control, task_id)
    _wait_first_step(control, task_id)
    with _local_work_locks(control, task_id):
        with closing(control.summary._connect()) as db:
            run_id = control._task(db, task_id)['prefect_run_id']
        try:
            if control.scheduler is None:
                raise ValueError('工单服务不可用')
            control.scheduler.wait_until_stopped(task_id, run_id)
        except Exception as exc:
            raise ControlError('当前任务的工单尚未停止或无法核验，请稍后重试删除', 503) from exc
        _cancel_prefect_orders(control, task_id)
        return _delete_after_stop(control, task_id)


def _cancel_prefect_orders(control, task_id):
    if control.scheduler is None:
        raise ControlError('工单服务不可用，任务已停止，请稍后重试删除', 503)
    with closing(control.summary._connect()) as db:
        task = control._task(db, task_id)
        run_id = task['prefect_run_id']
    try:
        with closing(sqlite3.connect(control.local_intake_queue_db.as_uri() + '?mode=ro',
                                     uri=True, timeout=5)) as db:
            db.row_factory = sqlite3.Row
            orders = [dict(row) for row in db.execute(
                'SELECT task_id,group_id,receipt_id,attempt_id,prefect_run_id,folder_label '
                'FROM preprocessing_folder_orders WHERE task_id=?', (task_id,)).fetchall()]
        control.scheduler.cancel_task(task_id, run_id, orders)
    except Exception as exc:
        raise ControlError('当前任务的工单尚未全部停止或无法核验，请稍后重试删除', 503) from exc


def _request_delete(control, task_id):
    # This short gate is also held during local claim admission.
    with control._lock('preprocessing', task_id), closing(control.summary._connect()) as db, db:
        db.execute('BEGIN IMMEDIATE')
        control._task(db, task_id)
        db.execute('INSERT OR IGNORE INTO task_deletions(task_id,requested_at_ns) VALUES(?,?)',
                   (task_id, time.time_ns()))
        db.execute("UPDATE tasks SET desired_state='paused',task_status='deleting',"
                   'updated_at_ns=?,control_revision=control_revision+1 '
                   'WHERE task_id=?', (time.time_ns(), task_id))


def _wait_first_step(control, task_id):
    from .llm_admission_state import counts
    deadline = time.monotonic() + 600
    while True:
        with closing(control.summary._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            control._recover_stale_groups(db, task_id)
            task = control._task(db, task_id)
            if _start_is_abandoned(task):
                db.execute('UPDATE tasks SET start_pending=0 WHERE task_id=?', (task_id,))
                task = control._task(db, task_id)
            llm = counts(db, task_id)
            if llm['unknown']:
                raise ControlError('LLM 或后处理结果未确认，原记录保留，请先核对', 409)
            busy = (task['start_pending'] or control.has_upload_transfers(task_id)
                    or db.execute("SELECT 1 FROM control_groups WHERE task_id=? AND state='running' LIMIT 1", (task_id,)).fetchone()
                    or db.execute("SELECT 1 FROM model_decisions WHERE task_id=? AND state='inflight' LIMIT 1", (task_id,)).fetchone()
                    or llm['inflight'])
        if not busy:
            return
        if time.monotonic() >= deadline:
            raise ControlError('删除已请求，正在等待该任务当前工作停止，请稍后重试', 409)
        time.sleep(.25)


def _tombstone_local_task(control, task_id):
    if control.local_intake_queue_db is None or not control.local_intake_queue_db.is_file():
        raise ControlError('本地前处理状态不可用，任务已停止，暂无法完成删除', 503)
    with closing(sqlite3.connect(control.local_intake_queue_db, timeout=30)) as local, local:
        local.execute('BEGIN IMMEDIATE')
        schema = local.execute("SELECT value FROM metadata WHERE key='queue_schema'").fetchone()
        if not schema or schema[0] != 'paperflow-local-waiting/1':
            raise ControlError('本地前处理状态无法核对，任务已停止', 503)
        local.execute('CREATE TABLE IF NOT EXISTS cancelled_tasks (task_id TEXT PRIMARY KEY,cancelled_at TEXT NOT NULL)')
        local.execute('INSERT OR IGNORE INTO cancelled_tasks(task_id,cancelled_at) VALUES(?,?)',
                      (task_id, str(time.time_ns())))


def _local_lock_root(control):
    if control.local_intake_queue_db is None or not control.local_intake_queue_db.is_file():
        raise ControlError('本地前处理状态不可用，任务已停止，暂无法完成删除', 503)
    project = control.local_intake_queue_db.parent.parent / 'prefect-paperflow'
    config = json.loads((project / 'config.json').read_text(encoding='utf8'))
    lock_root = Path(config['data_root']).resolve() / 'locks'
    lock_root.mkdir(parents=True, exist_ok=True)
    return lock_root


def _local_lock_names(control, task_id, lock_root):
    with closing(sqlite3.connect(control.local_intake_queue_db.as_uri() + '?mode=ro', uri=True, timeout=30)) as local:
        local.row_factory = sqlite3.Row
        attempts = local.execute("SELECT attempt_id FROM claims WHERE task_id=? AND "
                                 "state IN ('checking','processing')", (task_id,)).fetchall()
        folder_orders = local.execute('SELECT attempt_id FROM preprocessing_folder_orders WHERE task_id=?',
                                      (task_id,)).fetchall()
        groups = local.execute('SELECT receipt_id,record,state FROM groups WHERE task_id=?', (task_id,)).fetchall()
    attempts_paths, person_paths, paper_paths = set(), set(), set()
    for row in attempts:
        attempt = row['attempt_id']
        if not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', attempt):
            raise ControlError('前处理领取记录无法核对，任务已停止', 503)
        attempts_paths.add(lock_root / ('local-preprocess-' + attempt + '.lock'))
    # Preserve the upstream folder-flow ownership barrier even after its
    # preprocessing claim has completed and LLM work is draining.
    for row in folder_orders:
        attempt = row['attempt_id']
        if not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', str(attempt or '')):
            raise ControlError('文件夹工单记录无法核对，任务已停止', 503)
        attempts_paths.add(lock_root / ('folder-order-' + attempt + '.lock'))
    for row in groups:
        if row['state'] == 'cancelled':
            continue
        record = json.loads(row['record'])
        receipt_id = row['receipt_id']
        if not re.fullmatch(r'[0-9a-f]{64}', receipt_id):
            raise ControlError('前处理回执身份无法核对，任务已停止', 503)
        expected = (control.receipt_root / 'receipts' / (receipt_id + '.json')).resolve(strict=True)
        path = Path(record['receipt_path']).resolve(strict=True)
        if path != expected:
            raise ControlError('前处理回执归属无法核对，任务已停止', 503)
        with path.open('rb') as stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        if digest != record.get('receipt_sha256'):
            raise ControlError('前处理回执发生变化，任务已停止', 503)
        receipt = json.loads(path.read_text(encoding='utf8'))
        paper = receipt.get('inventory') or {}
        if receipt.get('receipt_id') != receipt_id or paper.get('paper_id') != record.get('paper_id'):
            raise ControlError('前处理资料身份无法核对，任务已停止', 503)
        person = Path(paper['person_dir']).resolve()
        person_paths.add(lock_root / (identity(str(person).casefold()) + '.lock'))
        paper_paths.add(lock_root / ('paper-' + identity(paper['paper_id']) + '.lock'))
    return sorted(attempts_paths), sorted(person_paths), sorted(paper_paths)


@contextmanager
def _local_work_locks(control, task_id):
    lock_root = _local_lock_root(control)
    # The reception tombstone makes the set of this task's groups stable.
    lock_groups = _local_lock_names(control, task_id, lock_root)
    deadline = time.monotonic() + 600
    while True:
        stack = ExitStack()
        try:
            for group in lock_groups:
                for path in group:
                    stack.enter_context(FileLock(path, timeout=0, thread_local=False))
        except Timeout:
            stack.close()
            if time.monotonic() >= deadline:
                raise ControlError('删除已请求，正在等待该任务前处理停止，请稍后重试', 409)
            time.sleep(1)
            continue
        except BaseException:
            stack.close()
            raise
        try:
            yield
        finally:
            stack.close()
        return


def _cancel_local_task(control, upstream_db, task_id):
    """Remove this task from both waiting pools while its processing locks are held."""
    with closing(sqlite3.connect(control.local_intake_queue_db, timeout=30)) as local, local:
        local.row_factory = sqlite3.Row
        local.execute('BEGIN IMMEDIATE')
        schema = local.execute("SELECT value FROM metadata WHERE key='queue_schema'").fetchone()
        if not schema or schema[0] != 'paperflow-local-waiting/1':
            raise ControlError('本地前处理状态无法核对，任务已停止', 503)
        refs = upstream_db.execute('SELECT * FROM local_retention_references WHERE task_id=?', (task_id,)).fetchall()
        keys = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'consumer_id', 'request_id')
        for ref in refs:
            if ref['consumer_id'] != 'local-intake':
                raise ControlError('任务仍有其他使用中的资料引用，任务已停止', 409)
            row = local.execute('SELECT * FROM retention_requests WHERE request_id=?', (ref['request_id'],)).fetchone()
            if row is None or any(row[key] != ref[key] for key in keys):
                raise ControlError('本地接收记录无法核对，任务已停止', 503)
            unrecorded_reservation = (row['state'] in {'intent', 'cancelled'} and row['retention_id'] is None
                                      and ref['state'] == 'reserved')
            if row['retention_id'] != ref['retention_id'] and not unrecorded_reservation:
                raise ControlError('本地接收记录无法核对，任务已停止', 503)
        local.execute('CREATE TABLE IF NOT EXISTS cancelled_tasks (task_id TEXT PRIMARY KEY,cancelled_at TEXT NOT NULL)')
        local.execute('INSERT OR IGNORE INTO cancelled_tasks(task_id,cancelled_at) VALUES(?,?)',
                      (task_id, str(time.time_ns())))
        local.execute("UPDATE groups SET state='cancelled' WHERE task_id=?", (task_id,))
        local.execute("UPDATE claims SET state='cancelled' WHERE task_id=?", (task_id,))
        local.execute("UPDATE retention_requests SET state='cancelled' WHERE task_id=?", (task_id,))
        local.execute("UPDATE preprocessing_handoffs SET state='cancelled' WHERE task_id=?", (task_id,))
    upstream_db.execute('DELETE FROM local_retention_references WHERE task_id=?', (task_id,))


def _delete_after_stop(control, task_id):
    registration_lock = control.data / 'control-locks' / 'first-step-registration.lock'
    # Match registration's lock even if its database has not yet been created.
    registration_lock.parent.mkdir(parents=True, exist_ok=True)
    gate = FileLock(str(registration_lock))
    with gate, control._lock('task', task_id), closing(control.summary._connect()) as db:
        task = control._task(db, task_id)
        upload_guard = control._lock('upload', task['upload_id']) if task['upload_id'] else nullcontext()
        with upload_guard:
            return _delete_locked(control, db, task_id)


def _assert_originals_outside_plan(control, db, plan):
    roots = {'output': control.output, 'data': control.data,
             'receipt': control.receipt_root, 'conversion': control.legacy_conversion_root}
    sources = [Path(row['source_root']).resolve() for row in db.execute(
        'SELECT source_root FROM tasks WHERE source_root IS NOT NULL') if row['source_root']]
    for entry in plan['paths']:
        root = roots.get(entry['root'])
        relative = Path(entry['relative'])
        if root is None or relative.is_absolute() or not relative.parts or '..' in relative.parts:
            raise ControlError('无法确认待清理文件的归属')
        path = (root / relative).resolve()
        if any(path == source or path.is_relative_to(source) or source.is_relative_to(path)
               for source in sources):
            raise ControlError('待清理位置与原始资料重叠，任务已停止', 409)


def _assert_pipeline_stopped(db, task_id):
    """Must be called inside the caller's short control writer transaction."""
    from .llm_admission_state import counts
    pipeline = counts(db, task_id)
    if pipeline['unknown']:
        raise ControlError('LLM 或后处理结果未确认，原记录保留，请先核对', 409)
    if pipeline['inflight']:
        raise ControlError('LLM 或后处理仍在收尾，禁止清理或移除任务身份', 409)


def _delete_locked(control, db, task_id):
        from .llm_admission_state import counts
        with control._lock('preprocessing', task_id), db:
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
            llm = counts(db, task_id)
            if llm['unknown']:
                raise ControlError('LLM 或后处理结果未确认，原记录保留，请先核对', 409)
            busy = (task['start_pending'] or control.has_upload_transfers(task_id)
                    or db.execute("SELECT 1 FROM control_groups WHERE task_id=? AND state='running' LIMIT 1", (task_id,)).fetchone()
                    or db.execute("SELECT 1 FROM model_decisions WHERE task_id=? AND state='inflight' LIMIT 1", (task_id,)).fetchone()
                    or llm['inflight'])
            if busy:
                raise ControlError('任务正在上传或处理，请等待完成或暂停后再删除')
        # Filesystem/receipt inspection and the other SQLite database must not
        # run while holding the shared upstream writer lock. The task and
        # registration gates surrounding this function remain held.
        _check_conversion_processes(control, task_id)
        with db:
            _cancel_local_task(control, db, task_id)
        task = control._task(db, task_id)
        try:
            if task['delete_plan']:
                # A retry can follow a partially completed removal of receipt events.
                plan = json.loads(task['delete_plan'])
            else:
                from .first_step_retention import assert_deletion_safe
                assert_deletion_safe(control, db, task, allow_owned_registered=True)
                plan = _build_plan(control, db, task)
        except (OSError, ValueError, KeyError, sqlite3.Error, ControlError) as exc:
            with db:
                db.execute("UPDATE tasks SET task_status='delete_failed',stop_reason=?,updated_at_ns=? WHERE task_id=?",
                           (str(exc)[:500] or type(exc).__name__, time.time_ns(), task_id))
            if isinstance(exc, ControlError):
                raise
            raise ControlError('删除前核对未完成，请检查状态后重试', 503) from exc
        with control._lock('preprocessing', task_id), db:
            db.execute('BEGIN IMMEDIATE')
            task = control._task(db, task_id)
            if not db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone():
                raise ControlError('删除状态已变化，未清理文件')
            _assert_pipeline_stopped(db, task_id)
            _assert_originals_outside_plan(control, db, plan)
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
        with control._lock('preprocessing', task_id), db:
            db.execute('BEGIN IMMEDIATE')
            task = control._task(db, task_id)
            _assert_pipeline_stopped(db, task_id)
            if task['upload_id']:
                db.execute('DELETE FROM tus_manifest WHERE upload_id=?', (task['upload_id'],))
                db.execute('DELETE FROM uploads WHERE upload_id=?', (task['upload_id'],))
            for table in ('local_retention_references', 'group_results', 'model_decisions', 'control_groups', 'task_create_requests'):
                db.execute('DELETE FROM ' + table + ' WHERE task_id=?', (task_id,))
            db.execute('DELETE FROM tasks WHERE task_id=?', (task_id,))
        return {'task_id': task_id, 'deleted': True, 'shared_groups_retained': len(plan['shared_groups']),
                'shared_conversion_caches_retained': plan.get('shared_caches', 0),
                'unassigned_conversion_caches_retained': plan.get('unassigned_caches', 0),
                'unknown_history_retained': len(unknown_rows)}
