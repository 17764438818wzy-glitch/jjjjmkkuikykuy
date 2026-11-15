"""Dispatch attachments from the retained local queue without claiming the paper."""
from contextlib import closing
from datetime import datetime, timezone
import asyncio
import json
from pathlib import Path
import re
import sqlite3

from filelock import FileLock, Timeout as FileLockTimeout

from .records import identity, read_json, sha_file, write_json
from .settings import Settings
from .stage3_handoff import read_receipt


class AttachmentQueue:
    def __init__(self, database, receipt_root, *, retention_consumer='local-intake'):
        self.database = Path(database).resolve(strict=True)
        self.receipt_root = Path(receipt_root).resolve(strict=True)
        self.retention_consumer = retention_consumer

    def _rows(self, receipt_id=None, *, include_without_attachments=False):
        sql = ("SELECT g.task_id,g.group_id,g.paper_id,g.receipt_id,g.state,g.record "
               "FROM groups g WHERE g.state!='needs_review' AND g.checked_session=? "
               "AND EXISTS(SELECT 1 FROM retention_requests r WHERE r.task_id=g.task_id "
               "AND r.group_id=g.group_id AND r.paper_id=g.paper_id AND r.receipt_id=g.receipt_id "
               "AND r.consumer_id=? AND r.state='accepted' AND r.retention_id IS NOT NULL "
               "AND r.last_error IS NULL AND r.checked_session=?)")
        with closing(sqlite3.connect(self.database.as_uri()+'?mode=ro', uri=True, timeout=30)) as db:
            db.row_factory = sqlite3.Row
            db.execute('BEGIN')
            meta = dict(db.execute("SELECT key,value FROM metadata WHERE key IN "
                                   "('receiver_status','receiver_session_id','receiver_heartbeat_at')"))
            try:
                age = (datetime.now(timezone.utc) - datetime.fromisoformat(meta['receiver_heartbeat_at'])).total_seconds()
            except (KeyError, ValueError, TypeError):
                raise ValueError('上游接收进程心跳不可核验') from None
            session = meta.get('receiver_session_id')
            if meta.get('receiver_status') != 'running' or not session or not 0 <= age <= 60:
                raise ValueError('上游接收进程当前不可核验')
            params = [session, self.retention_consumer, session]
            if receipt_id is not None:
                sql += ' AND g.receipt_id=?'
                params.append(receipt_id)
            sql += ' ORDER BY g.rowid'
            rows = db.execute(sql, params).fetchall()
        result = []
        for row in rows:
            try:
                value = json.loads(row['record'])
                if not isinstance(value, dict) or any(value.get(k) != row[k] for k in
                        ('task_id', 'group_id', 'paper_id', 'receipt_id')):
                    raise ValueError('等待池记录与登记身份不一致')
                if not isinstance(value.get('attachments'), list):
                    raise ValueError('等待池缺少附件清单')
            except (ValueError, TypeError, KeyError):
                result.append({'receipt_id': row['receipt_id'], '_invalid': True})
                continue
            if include_without_attachments or value['attachments']:
                result.append(value)
        return result

    def eligible(self):
        return self._rows()

    def verified(self, receipt_id, *, require_attachments=True):
        if not isinstance(receipt_id, str) or not re.fullmatch(r'[0-9a-f]{64}', receipt_id):
            raise ValueError('登记回执编号无效')
        rows = self._rows(receipt_id, include_without_attachments=not require_attachments)
        if len(rows) != 1:
            raise ValueError('附件资料不在已保留的等待池中')
        record = rows[0]
        if record.get('_invalid'):
            raise ValueError('等待池记录无效')
        try:
            path = Path(record['receipt_path']).resolve(strict=True)
            expected = (self.receipt_root / 'receipts' / (receipt_id + '.json')).resolve(strict=True)
            directory = Path(record['delivery_dir']).resolve(strict=True)
        except (OSError, KeyError, TypeError) as exc:
            raise ValueError('登记回执或交付目录不可用') from exc
        if path != expected or not path.is_relative_to(self.receipt_root):
            raise ValueError('登记回执不属于受信任的登记目录')
        before = sha_file(path)
        if before != record.get('receipt_sha256'):
            raise ValueError('等待池绑定的登记回执发生变化')
        receipt, paper = read_receipt(path)
        if (sha_file(path) != before or receipt.get('receipt_id') != receipt_id
                or receipt.get('paper_id') != record['paper_id']
                or receipt.get('group_id') != record['group_id']
                or (receipt.get('observation') or {}).get('stored_state', receipt.get('state')) != '已接收'):
            raise ValueError('登记回执与论文身份不一致')
        reference = receipt.get('delivery_ref') or {}
        if not reference.get('path') or Path(reference['path']).resolve().parent != directory:
            raise ValueError('登记回执与交付副本不一致')
        saved = {r['file_id']: (r['sha256'], r['bytes'], str(Path(r['path']).resolve()))
                 for r in record['attachments']}
        received = {r['file_id']: (r['sha256'], r['bytes'], str(Path(r['path']).resolve()))
                    for r in receipt['items'] if r.get('kind') == 'attachment'}
        if len(saved) != len(record['attachments']) or saved != received:
            raise ValueError('登记附件与等待池不一致')
        if not require_attachments:
            bodies = [r for r in receipt['items'] if r.get('kind') == 'body' and r.get('state') == '已接收']
            body = record.get('body')
            if body is None:
                if bodies:
                    raise ValueError('登记正文与等待池不一致')
            elif not isinstance(body, dict) or len(bodies) != 1 or any(
                    body.get(k) != bodies[0].get(k) for k in ('file_id', 'sha256', 'bytes', 'path', 'relative')):
                raise ValueError('登记正文与等待池不一致')
        current = self._rows(receipt_id, include_without_attachments=not require_attachments)
        if len(current) != 1 or current[0] != record:
            raise ValueError('复核期间等待池或保留状态发生变化')
        return dict(record, library_root=receipt['library_root'], paper=paper)


def settings_for_verified_receipt(settings, record):
    library = Path(record['library_root'])
    if not library.is_absolute():
        raise ValueError('登记来源资料库路径无效')
    result = Settings({**settings.values, 'library_root': str(library)})
    result.require_execution()
    return result


def paper_is_available(record, data_root):
    paper = record.get('paper')
    if not paper:
        return True
    root = Path(data_root)/'locks'
    root.mkdir(parents=True, exist_ok=True)
    person = root/(identity(str(Path(paper['person_dir']).resolve()).casefold())+'.lock')
    paper_lock = root/('paper-'+identity(paper['paper_id'])+'.lock')
    try:
        with FileLock(person, timeout=0, thread_local=False), FileLock(paper_lock, timeout=0, thread_local=False):
            return True
    except FileLockTimeout:
        return False


async def schedule_ready(queue, journal_root, deployment_id, submit, observe, *, available=None):
    """Use Prefect's idempotency key; never mutate the body waiting queue."""
    root = Path(journal_root)
    root.mkdir(parents=True, exist_ok=True)
    result = {'submitted': 0, 'already_submitted': 0, 'busy': 0, 'failed': 0, 'errors': []}
    with FileLock(str(root/'dispatch.lock'), timeout=0):
        for row in queue.eligible():
            receipt_id = row.get('receipt_id')
            try:
                if not isinstance(receipt_id, str) or not re.fullmatch(r'[0-9a-f]{64}', receipt_id):
                    raise ValueError('登记回执编号无效')
                if row.get('_invalid'):
                    raise ValueError('等待池记录无效')
                binding = {key: row[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'receipt_sha256')}
                binding['deployment_id'] = str(deployment_id)
                target = root/(receipt_id+'.json')
                attempt = 1
                if target.exists():
                    prior = read_json(target)
                    if (prior.get('schema') != 'attachment-dispatch/1' or prior.get('binding') != binding
                            or not prior.get('flow_run_id') or type(prior.get('attempt')) is not int or prior['attempt'] < 1):
                        raise ValueError('附件调度记录身份不一致')
                    observation = await observe(prior['flow_run_id'])
                    state = observation.get('state') if isinstance(observation, dict) else observation
                    if state in {'SCHEDULED', 'PENDING', 'RUNNING', 'PAUSED', 'COMPLETED'}:
                        result['already_submitted'] += 1
                        continue
                    if state not in {'FAILED', 'CRASHED', 'CANCELLED'}:
                        raise ValueError('Prefect 运行状态未知')
                    if (not isinstance(observation, dict) or observation.get('retryable') is not True
                            or observation.get('run_id') != prior['flow_run_id']
                            or observation.get('receipt_id') != receipt_id):
                        raise ValueError('附件运行未完成，不能自动重复请求')
                    attempt = prior['attempt'] + 1
                verified = await asyncio.to_thread(queue.verified, receipt_id)
                if any(verified.get(key) != value for key, value in binding.items() if key != 'deployment_id'):
                    raise ValueError('附件调度与登记身份不一致')
                if available is not None and not await asyncio.to_thread(available, verified):
                    result['busy'] += 1
                    continue
                run_id = await submit(deployment_id, parameters={'receipt_id': receipt_id, 'run_mode': 'process'},
                                      idempotency_key='paperflow-attachment-'+receipt_id+'-'+str(attempt))
                if not run_id:
                    raise ValueError('Prefect 未返回附件运行编号')
                write_json(target, {'schema': 'attachment-dispatch/1', 'binding': binding,
                                    'attempt': attempt, 'flow_run_id': str(run_id)})
                result['submitted'] += 1
            except Exception as exc:
                result['failed'] += 1
                result['errors'].append({'receipt_id': receipt_id, 'error_type': type(exc).__name__})
    return result
