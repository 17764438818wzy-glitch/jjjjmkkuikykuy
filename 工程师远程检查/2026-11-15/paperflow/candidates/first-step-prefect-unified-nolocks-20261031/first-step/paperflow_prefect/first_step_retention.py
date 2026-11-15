"""Durable local-intake references and safe first-step deletion checks."""
from pathlib import Path
from contextlib import closing
import hashlib
import json
import sqlite3
import time
import uuid

from .first_step_control import ControlError, _uuid
from .first_step_registration import task_receipt_plan
from .records import identity, read_json


def _owned_result(control, db, task, group_id, paper_id, receipt_id):
    if (task['task_status'] in {'deleting', 'delete_failed'} or
            db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task['task_id'],)).fetchone()):
        raise ControlError('任务正在删除或删除未完成，不能接收')
    if not task['artifact_scoped']:
        raise ControlError('旧共享交付目录归属未确认，不能接收')
    rows = db.execute('SELECT r.status,r.payload FROM control_groups g JOIN group_results r '
                      'ON r.task_id=g.task_id AND r.seq=g.seq WHERE g.task_id=? AND g.group_id=?',
                      (task['task_id'], group_id)).fetchall()
    if len(rows) != 1:
        raise ControlError('资料组不存在或归属不唯一')
    try:
        row = json.loads(rows[0]['payload'])
        if (rows[0]['status'] != 'passed' or row.get('status') != 'passed'
                or row.get('group_id') != group_id or row.get('paper_id') != paper_id
                or row.get('receipt_id') != receipt_id or row.get('registration_status') != 'registered'
                or row.get('delivery_complete') is not True):
            raise ValueError('身份或状态不匹配')
        delivery_dir = Path(row['delivery_dir']).resolve(strict=True)
        task_root = (control.output / 'tasks' / task['task_id']).resolve()
        group_root = (task_root / 'deliveries' / group_id).resolve()
        if (not group_root.is_relative_to(task_root) or delivery_dir.parent != group_root
                or not delivery_dir.is_dir()):
            raise ValueError('交付目录不属于此任务')
        receipt_path = Path(row['receipt_path']).resolve(strict=True)
        expected = (control.receipt_root / 'receipts' / (receipt_id + '.json')).resolve()
        if (not expected.is_relative_to(control.receipt_root.resolve())
                or receipt_path != expected or not receipt_path.is_file()):
            raise ValueError('登记回执位置不符')
        receipt = read_json(receipt_path)
        source = {key: value for key, value in receipt.items() if key not in {'receipt_id', 'record_path'}}
        if (receipt.get('receipt_id') != receipt_id or identity(source) != receipt_id
                or receipt.get('group_id') != group_id or receipt.get('paper_id') != paper_id
                or receipt.get('state') != '已接收'
                or Path(receipt.get('record_path', '')).resolve() != receipt_path
                or Path((receipt.get('delivery_ref') or {}).get('path', '')).resolve()
                   != delivery_dir / 'delivery.json'
                or not (delivery_dir / 'delivery.json').is_file()
                or not (delivery_dir / 'handoff.json').is_file()):
            raise ValueError('登记回执与交付目录不符')
    except (OSError, KeyError, TypeError, ValueError) as exc:
        raise ControlError('资料组未完整登记或交付归属无法核对') from exc
    ledger = control.receipt_root / 'receipts.sqlite3'
    if not ledger.is_file():
        raise ControlError('登记数据库不可用，不能接收', 503)
    try:
        with closing(sqlite3.connect(ledger.as_uri() + '?mode=ro', uri=True, timeout=3)) as db_ledger:
            event = db_ledger.execute('SELECT payload FROM receipt_events WHERE receipt_id=?',
                                      (receipt_id,)).fetchone()
        if event is None or json.loads(event[0]) != receipt:
            raise ValueError('登记数据库与回执不一致')
    except (sqlite3.Error, OSError, ValueError, TypeError) as exc:
        raise ControlError('登记数据库无法核对，不能接收', 503) from exc
    return row


def _read_local_snapshot(control, reference, *, timeout=3):
    path = control.local_intake_queue_db
    if path is None or not path.is_file():
        raise ControlError('本地接收状态库不可用', 503)
    try:
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=timeout)) as local:
            local.row_factory = sqlite3.Row
            local.execute('PRAGMA query_only=ON')
            local.execute('BEGIN')
            schema = local.execute("SELECT value FROM metadata WHERE key='queue_schema'").fetchone()
            if schema is None or schema['value'] != 'paperflow-local-waiting/1':
                raise ControlError('本地接收状态库身份无法确认', 503)
            hold = local.execute('SELECT * FROM retention_requests WHERE request_id=?',
                                 (reference['request_id'],)).fetchone()
            group = local.execute('SELECT * FROM groups WHERE task_id=? AND group_id=?',
                                  (reference['task_id'], reference['group_id'])).fetchone()
            keys = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'consumer_id',
                    'request_id', 'retention_id')
            if (hold is None or any(hold[key] != reference[key] for key in keys)
                    or hold['state'] not in {'reserved', 'accepted'} or hold['last_error'] is not None
                    or group is None or any(group[key] != reference[key] for key in
                                            ('task_id', 'group_id', 'paper_id', 'receipt_id'))
                    or group['state'] not in {'waiting', 'processing'}):
                raise ControlError('本地接收记录尚未完成或身份不一致')
            if group['state'] == 'processing' and local.execute(
                    "SELECT 1 FROM claims WHERE task_id=? AND group_id=? AND receipt_id=? "
                    "AND state='processing' LIMIT 1", (reference['task_id'], reference['group_id'],
                                                        reference['receipt_id'])).fetchone() is None:
                raise ControlError('本地处理领取记录无法确认')
            return (group['state'], group['record'], hold['state'])
    except ControlError:
        raise
    except (sqlite3.Error, OSError, ValueError, TypeError, KeyError, IndexError, AttributeError) as exc:
        raise ControlError('本地接收状态库无法核对', 503) from exc


def _assert_local_acceptance(control, reference, upstream):
    """Check committed local evidence outside the upstream task/write lock."""
    snapshot = _read_local_snapshot(control, reference)
    try:
        record = json.loads(snapshot[1])
        if (not isinstance(record, dict) or any(record.get(key) != reference[key] for key in
                ('task_id', 'group_id', 'paper_id', 'receipt_id'))):
            raise ControlError('本地资料身份不一致')
        receipt_path = Path(upstream['receipt_path']).resolve(strict=True)
        delivery_dir = Path(upstream['delivery_dir']).resolve(strict=True)
        if (Path(record['receipt_path']).resolve(strict=True) != receipt_path
                or Path(record['delivery_dir']).resolve(strict=True) != delivery_dir
                or hashlib.sha256(receipt_path.read_bytes()).hexdigest() != record.get('receipt_sha256')
                or not isinstance(record.get('body'), dict)
                or not isinstance(record.get('attachments'), list)):
            raise ControlError('本地资料与登记回执不一致')
        receipt = read_json(receipt_path)
        if any(record.get(key) != receipt.get(key) for key in ('origin_group_id', 'result_version')):
            raise ControlError('本地交接版本与回执不一致')
        received = [item for item in receipt.get('items', [])
                    if isinstance(item, dict) and item.get('kind') in {'body', 'attachment'}]
        bodies = [item for item in received if item['kind'] == 'body']
        attachments = [item for item in received if item['kind'] == 'attachment']
        if len(bodies) != 1 or len(attachments) != len(record['attachments']):
            raise ControlError('本地正文或附件数量与回执不一致')
        metadata_paths = {receipt_path, delivery_dir / 'delivery.json', delivery_dir / 'handoff.json'}
        manifest = read_json(delivery_dir / 'delivery.json')
        if not isinstance(manifest.get('evidence_records'), list):
            raise ControlError('交付证据清单无效')
        for evidence in manifest['evidence_records']:
            evidence_path = (delivery_dir / evidence['path']).resolve(strict=True)
            if not evidence_path.is_relative_to(delivery_dir / 'attachment-evidence'):
                raise ControlError('交付证据路径无效')
            metadata_paths.add(evidence_path)
        required_paths = set(metadata_paths)
        fields = ('file_id', 'upstream_file_id', 'sha256', 'bytes', 'attachment_id', 'relative',
                  'delivered_path', 'source_path', 'format', 'name', 'evidence_records')
        for item, source in zip([record['body'], *record['attachments']], [*bodies, *attachments]):
            if (not isinstance(item, dict) or any(item.get(key) is None for key in
                    ('file_id', 'upstream_file_id', 'path', 'sha256', 'bytes'))
                    or any(item.get(key) != source.get(key) for key in fields if key in source)):
                raise ControlError('本地正文或附件记录不完整')
            item_path = Path(item['path']).resolve(strict=True)
            if (item_path != Path(source['delivered_path']).resolve(strict=True)
                    or not item_path.is_relative_to(delivery_dir / 'files') or not item_path.is_file()):
                raise ControlError('本地正文或附件位置不属于交付目录')
            required_paths.add(item_path)
        fingerprint = record.get('_fingerprint')
        if not isinstance(fingerprint, list):
            raise ControlError('本地资料核验指纹缺失')
        seen = set()
        for entry in fingerprint:
            if not isinstance(entry, list) or len(entry) != 7:
                raise ControlError('本地资料核验指纹无效')
            path = Path(entry[0]).resolve(strict=True)
            if path in seen or (path != receipt_path and not path.is_relative_to(delivery_dir)):
                raise ControlError('本地资料核验指纹路径无效')
            seen.add(path)
            stat = path.stat()
            if list(entry[1:6]) != [stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns,
                                    stat.st_dev, stat.st_ino]:
                raise ControlError('交付资料在本地核验后发生变化')
            digest = entry[6]
            if path in metadata_paths:
                if not isinstance(digest, str) or len(digest) != 64:
                    raise ControlError('交付清单或证据的摘要缺失')
                if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                    raise ControlError('交付清单或证据在本地核验后发生变化')
            elif digest is not None:
                raise ControlError('本地元数据指纹路径无效')
        if not required_paths.issubset(seen):
            raise ControlError('本地资料核验指纹不完整')
        return snapshot
    except ControlError:
        raise
    except (OSError, ValueError, TypeError, KeyError, IndexError, AttributeError) as exc:
        raise ControlError('本地接收记录或交付文件无法核对', 503) from exc


def _recheck_file_stats(snapshot):
    """Final cheap stat check under the task lock; never hash document contents here."""
    try:
        for entry in json.loads(snapshot[1])['_fingerprint']:
            stat = Path(entry[0]).stat()
            if list(entry[1:6]) != [stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns,
                                    stat.st_dev, stat.st_ino]:
                raise ControlError('交付资料在确认前发生变化')
    except ControlError:
        raise
    except (OSError, ValueError, TypeError, KeyError, IndexError) as exc:
        raise ControlError('交付资料在确认前无法核对', 409) from exc


def _reference(row, *, created=False):
    return {key: row[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id',
                                      'consumer_id', 'request_id', 'retention_id', 'state')} | {'created': created}


def reserve(control, task_id, group_id, paper_id, receipt_id, consumer_id, request_id):
    task_id, request_id = _uuid(task_id), _uuid(request_id)
    with control._lock('task', task_id), closing(control.summary._connect()) as db, db:
        db.execute('BEGIN IMMEDIATE')
        task = control._task(db, task_id)
        _owned_result(control, db, task, group_id, paper_id, receipt_id)
        old = db.execute('SELECT * FROM local_retention_references WHERE request_id=?', (request_id,)).fetchone()
        if old:
            if any(old[key] != value for key, value in (
                    ('task_id', task_id), ('group_id', group_id), ('paper_id', paper_id),
                    ('receipt_id', receipt_id), ('consumer_id', consumer_id))):
                raise ControlError('接收请求编号已用于其他资料')
            if old['state'] == 'released':
                raise ControlError('该接收请求已释放，请使用新的请求编号')
            return _reference(old)
        retention_id = str(uuid.uuid4())
        now = time.time_ns()
        db.execute('INSERT INTO local_retention_references '
                   '(retention_id,request_id,task_id,group_id,paper_id,receipt_id,consumer_id,state,'
                   'created_at_ns,updated_at_ns) VALUES (?,?,?,?,?,?,?,\'reserved\',?,?)',
                   (retention_id, request_id, task_id, group_id, paper_id, receipt_id, consumer_id, now, now))
        return _reference(db.execute('SELECT * FROM local_retention_references WHERE retention_id=?',
                                     (retention_id,)).fetchone(), created=True)


def change_state(control, task_id, retention_id, consumer_id, receipt_id, *, action, reason=None):
    task_id, retention_id = _uuid(task_id), _uuid(retention_id)
    if action not in {'confirm', 'release'}:
        raise ValueError('unknown local-retention action')
    with control._lock('task', task_id), closing(control.summary._connect()) as db:
        task = control._task(db, task_id)
        if (task['task_status'] in {'deleting', 'delete_failed'} or
                db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone()):
            raise ControlError('任务正在删除或删除未完成，不能修改引用')
        row = db.execute('SELECT * FROM local_retention_references WHERE task_id=? AND retention_id=?',
                         (task_id, retention_id)).fetchone()
        if row is None:
            raise ControlError('接收引用不存在', 404)
        if row['consumer_id'] != consumer_id or row['receipt_id'] != receipt_id:
            raise ControlError('接收引用身份不匹配')
        if action == 'release':
            raise ControlError('当前没有可核验的处理终态，不能释放接收保护')
        if row['state'] == 'released':
            raise ControlError('接收引用已释放')
        reference, task_snapshot = dict(row), dict(task)
    with closing(control.summary._connect()) as db:
        upstream = _owned_result(control, db, task_snapshot, reference['group_id'],
                                 reference['paper_id'], reference['receipt_id'])
    snapshot = _assert_local_acceptance(control, reference, upstream)
    with control._lock('task', task_id), closing(control.summary._connect()) as db, db:
        db.execute('BEGIN IMMEDIATE')
        task = control._task(db, task_id)
        row = db.execute('SELECT * FROM local_retention_references WHERE task_id=? AND retention_id=?',
                         (task_id, retention_id)).fetchone()
        keys = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'consumer_id', 'request_id', 'retention_id')
        if (task['task_status'] in {'deleting', 'delete_failed'}
                or db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (task_id,)).fetchone()
                or row is None
                or any(row[key] != reference[key] for key in keys) or row['state'] not in {'reserved', 'accepted'}):
            raise ControlError('接收引用在核验期间发生变化')
        group = db.execute('SELECT r.status,r.payload FROM control_groups g JOIN group_results r '
                           'ON r.task_id=g.task_id AND r.seq=g.seq WHERE g.task_id=? AND g.group_id=?',
                           (task_id, reference['group_id'])).fetchall()
        if len(group) != 1 or group[0]['status'] != 'passed' or json.loads(group[0]['payload']) != upstream:
            raise ControlError('上游资料组在核验期间发生变化')
        if _read_local_snapshot(control, reference, timeout=.2) != snapshot:
            raise ControlError('本地接收记录在核验期间发生变化')
        _recheck_file_stats(snapshot)
        if row['state'] == 'reserved':
            now = time.time_ns()
            db.execute("UPDATE local_retention_references SET state='accepted',accepted_at_ns=?,updated_at_ns=? "
                       'WHERE retention_id=?', (now, now, retention_id))
        return _reference(db.execute('SELECT * FROM local_retention_references WHERE retention_id=?',
                                     (retention_id,)).fetchone())


def _registered_results(db, task_id):
    groups = db.execute('SELECT payload FROM group_results WHERE task_id=?', (task_id,)).fetchall()
    try:
        result = {}
        for row in groups:
            payload = json.loads(row['payload'])
            if payload.get('registration_status') == 'registered':
                result[payload['receipt_id']] = (payload['group_id'], payload['paper_id'])
        return result
    except (KeyError, TypeError, ValueError) as exc:
        raise ControlError('资料组登记状态无法核对，任务暂不能删除', 503) from exc


def _owned_receipts(control, task, registered, group_ids, published_groups):
    task_id = task['task_id']
    if not task['artifact_scoped']:
        if registered or any((control.output / 'deliveries' / group_id).exists() for group_id in group_ids):
            raise ControlError('旧共享交付目录归属未确认，任务暂不能删除')
        return []

    task_output = control.output / 'tasks' / task_id
    receipt_db = control.receipt_root / 'receipts.sqlite3'
    if not receipt_db.is_file():
        deliveries = task_output / 'deliveries'
        receipt_files = control.receipt_root / 'receipts'
        if (registered or any(value.get('registration_status') == 'registered' for value in published_groups.values())
                or (receipt_files.is_dir() and any(receipt_files.glob('*.json')))):
            raise ControlError('登记数据库不可用，任务暂不能删除', 503)
        if deliveries.is_dir():
            for group_dir in deliveries.iterdir():
                if (group_dir.name not in group_ids or not group_dir.is_dir()
                        or group_dir.is_symlink() or group_dir.is_junction()):
                    raise ControlError('尚未发布的交付目录归属无法核对', 503)
        return []
    try:
        owned = task_receipt_plan(control.receipt_root, task_output)
        accepted = []
        with sqlite3.connect(receipt_db.as_uri() + '?mode=ro', uri=True, timeout=10) as ledger:
            for entry in owned:
                found = ledger.execute('SELECT payload FROM receipt_events WHERE receipt_id=?',
                                       (entry['receipt_id'],)).fetchone()
                if found is None:
                    raise ValueError('登记事件已消失')
                payload = json.loads(found[0])
                if payload.get('state') == '已接收':
                    items = payload.get('items')
                    if (not isinstance(items, list) or not payload.get('paper_id')
                            or len([item for item in items if isinstance(item, dict)
                                    and item.get('kind') == 'body'
                                    and item.get('state') == '已接收']) != 1):
                        raise ValueError('已接收回执缺少唯一正文')
                    group_id, paper_id = payload['group_id'], payload['paper_id']
                    if entry['receipt_id'] in registered and registered[entry['receipt_id']] != (group_id, paper_id):
                        raise ValueError('登记结果与回执身份不符')
                    accepted.append({'receipt_id': entry['receipt_id'], 'group_id': group_id,
                                     'paper_id': paper_id, 'delivery_dir': str(Path(entry['delivery_path']).parent)})
                elif payload.get('state') != '未接收':
                    raise ValueError('登记回执状态不明')
        if set(registered) - {entry['receipt_id'] for entry in accepted}:
            raise ValueError('登记结果不属于当前任务或回执尚未确认')
        deliveries = task_output / 'deliveries'
        if deliveries.is_dir():
            owned_versions = {Path(entry['delivery_path']).parent.resolve() for entry in owned}
            for group_dir in deliveries.iterdir():
                published = published_groups.get(group_dir.name)
                # Delivery can succeed before registration fails or the
                # process stops. The receipt ledger above has already been
                # checked for this task and for references from other tasks.
                if (group_dir.name not in group_ids or not group_dir.is_dir()
                        or group_dir.is_symlink() or group_dir.is_junction()):
                    raise ValueError('交付目录归属无法核对')
                if published and published.get('registration_status') == 'registered':
                    delivery_dir = published.get('delivery_dir')
                    if not delivery_dir or Path(delivery_dir).resolve() not in owned_versions:
                        raise ValueError('已登记交付版本缺少可核对的登记事件')
        return accepted
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError) as exc:
        raise ControlError('登记记录无法核对，任务暂不能删除', 503) from exc


def assert_deletion_safe(control, db, task, *, allow_owned_registered=False):
    """Called under registration and task locks, before every deletion attempt."""
    task_id = task['task_id']
    if db.execute('SELECT 1 FROM local_retention_references WHERE task_id=? LIMIT 1',
                  (task_id,)).fetchone():
        raise ControlError('资料仍有本地接收保护，任务暂不能删除')
    registered = _registered_results(db, task_id)
    groups = [row['group_id'] for row in db.execute(
        'SELECT group_id FROM control_groups WHERE task_id=?', (task_id,)) if row['group_id']]
    try:
        published = {}
        for row in db.execute('SELECT status,payload FROM group_results WHERE task_id=?', (task_id,)):
            payload = json.loads(row['payload'])
            group_id = payload['group_id']
            if (group_id in published or row['status'] not in {'passed', 'failed', 'unrecognized'}
                    or payload.get('status') != row['status']
                    or type(payload.get('delivery_complete')) is not bool
                    or (row['status'] == 'passed' and payload['delivery_complete'] is not True)):
                raise ValueError('资料组结果重复')
            published[group_id] = payload
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        raise ControlError('资料组结果无法核对，任务暂不能删除', 503) from exc
    owned = _owned_receipts(control, task, registered, groups, published)
    if owned and not allow_owned_registered:
        raise ControlError('资料已登记，正在等待本地接收或仍被使用，任务暂不能删除')


def list_retentions(control, task_id, page=1, page_size=200):
    """Report a read-only snapshot for reconciliation; writes still recheck under lock."""
    task_id = _uuid(task_id)
    with closing(control.summary._connect()) as db:
        db.execute('BEGIN')
        control._task(db, task_id)
        revision = db.execute('SELECT revision FROM local_retention_revisions WHERE task_id=?',
                              (task_id,)).fetchone()
        total = db.execute('SELECT COUNT(*) FROM local_retention_references WHERE task_id=?',
                           (task_id,)).fetchone()[0]
        refs = db.execute('SELECT * FROM local_retention_references WHERE task_id=? '
                          'ORDER BY created_at_ns, retention_id LIMIT ? OFFSET ?',
                          (task_id, page_size, (page - 1) * page_size)).fetchall()
    return {'task_id': task_id, 'items': [_reference(row) for row in refs], 'page': page,
            'page_size': page_size, 'list_total': total, 'has_next': page * page_size < total,
            'revision': revision['revision'] if revision else 0}
