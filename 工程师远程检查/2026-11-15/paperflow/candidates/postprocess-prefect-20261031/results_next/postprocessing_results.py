"""Candidate-only result ledger for existing folder work orders.

No dispatch, retries, model access, RAG access or server setup occurs here.
``reserve`` is a one-time durable intent and MUST precede input parsing and
stage execution; the dispatcher MUST reject any already-reserved folder.
Its short admission gate must be the same gate used by task pause/deletion.
An unresolved or terminal row is never a grant to retry.

A passed result binds an accepted folder-stage receipt, not a READY file. The
caller must supply the real receipt validator, and configure inspect_receipt as
FolderStageStore._inspect_receipt. Fixed public reason codes avoid exposing model
responses or private diagnostic paths in the results API. A human-readable
unrecognized count remains unavailable: existing contracts have no proven
whole-folder coverage, even if a separately proven unrecognized item is saved.
"""
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time
from uuid import UUID, uuid4


IDENTITY = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id',
            'parent_run_id', 'task_run_id')
FAILURES = {
    'invalid_handoff': '前处理交接材料或身份核验未通过',
    'missing_source': '处理所需的来源材料不可用',
    'incomplete_response': '正文或附件的模型回答不完整',
    'body_format': '正文模型回答格式无法解析',
    'body_validation': '正文画像校验未通过',
    'attachment_format': '附件模型回答格式无法解析',
    'attachment_validation': '附件说明校验未通过',
    'packaging': '封包或文件核验未通过',
    'resource_unavailable': '后处理所需的资源或配置不可用',
}
UNRESOLVED = {
    'processing': '后处理正在进行或等待确认',
    'paused': '任务已暂停，处理结果待核对',
    'outcome_unknown': '后处理结果未确认',
    'worker_lost': '执行进程状态待核对',
    'evidence_unavailable': '处理证据暂不可核验',
}
STATES = {'unresolved', 'passed', 'failed', 'unrecognized'}


class ResultConflict(RuntimeError):
    """Identity, prior intent or evidence does not authorize this transition."""


def _identity(value):
    if not isinstance(value, dict) or set(value) != set(IDENTITY):
        raise ResultConflict('incomplete folder identity')
    try:
        if any(str(UUID(value[k])) != value[k] for k in ('task_id', 'parent_run_id', 'task_run_id')):
            raise ValueError('noncanonical UUID')
    except (TypeError, ValueError, AttributeError) as exc:
        raise ResultConflict('invalid folder identity') from exc
    if (not isinstance(value['group_id'], str) or not re.fullmatch('[a-f0-9]{32}', value['group_id'])
            or not isinstance(value['receipt_id'], str) or not re.fullmatch('[a-f0-9]{64}', value['receipt_id'])
            or not isinstance(value['paper_id'], str) or not value['paper_id']
            or not isinstance(value['attempt_id'], str)
            or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', value['attempt_id'])):
        raise ResultConflict('invalid folder identity')
    return dict(value)


def _digest(value):
    if not isinstance(value, str) or not re.fullmatch('[a-f0-9]{64}', value):
        raise ResultConflict('invalid digest')
    return value


def _file_digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def _as_of():
    return datetime.now(timezone.utc).isoformat()


class ResultsStore:
    def __init__(self, database, *, admission_gate, inspect_receipt):
        self.database = Path(database).resolve()
        self.admission_gate = admission_gate
        self.inspect_receipt = inspect_receipt

    def _connect(self, readonly=False):
        db = sqlite3.connect(self.database.as_uri() + ('?mode=ro' if readonly else '?mode=rw'),
                             uri=True, timeout=30)
        db.row_factory = sqlite3.Row
        return db

    def _gate(self, task_id):
        if not callable(self.admission_gate):
            raise ResultConflict('shared task admission gate is required')
        return self.admission_gate(task_id)

    def initialize(self):
        """Explicitly create the ledger in an already existing selected DB."""
        with closing(self._connect()) as db, db:
            db.execute('''CREATE TABLE IF NOT EXISTS postprocessing_results (
                task_id TEXT NOT NULL,group_id TEXT NOT NULL,paper_id TEXT NOT NULL,
                receipt_id TEXT NOT NULL,attempt_id TEXT NOT NULL,parent_run_id TEXT NOT NULL,
                task_run_id TEXT NOT NULL UNIQUE,manifest_sha256 TEXT NOT NULL,
                generation INTEGER NOT NULL,result_id TEXT NOT NULL UNIQUE,state TEXT NOT NULL,
                code TEXT NOT NULL,claim_id TEXT,stage_receipt_sha256 TEXT,
                proof_path TEXT,proof_sha256 TEXT,created_at_ns INTEGER NOT NULL,updated_at_ns INTEGER NOT NULL,
                PRIMARY KEY(task_id,group_id))''')

    @staticmethod
    def _upstream(db, identity, manifest_sha256):
        item = db.execute('SELECT * FROM llm_folder_queue WHERE task_id=? AND group_id=?',
                          (identity['task_id'], identity['group_id'])).fetchone()
        if (not item or item['state'] != 'waiting_postprocessing'
                or item['manifest_sha256'] != manifest_sha256
                or any(item[k] != identity[k] for k in IDENTITY)):
            raise ResultConflict('upstream identity or completion changed')

    @staticmethod
    def _retry_source(db, identity, manifest_sha256):
        """Reuse the claim's same-transaction source check for final outcomes."""
        try:
            from folder_state import FolderStageStore
            FolderStageStore._upstream(db, identity, manifest_sha256)
        except Exception as exc:
            raise ResultConflict('the reserved retry source changed or is unavailable') from exc

    @staticmethod
    def _task(db, identity):
        task = db.execute('SELECT * FROM tasks WHERE task_id=?', (identity['task_id'],)).fetchone()
        deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (identity['task_id'],)).fetchone()
        if (not task or deleted or task['prefect_order_model'] != 'v2'
                or task['prefect_run_id'] != identity['parent_run_id']):
            raise ResultConflict('task identity unavailable or deleted')
        return task

    def reserve(self, identity, generation, manifest_sha256):
        """Record one processing intent before any input parsing; never retry it."""
        identity, manifest_sha256 = _identity(identity), _digest(manifest_sha256)
        if type(generation) is not int or generation < 0:
            raise ResultConflict('invalid task generation')
        with self._gate(identity['task_id']), closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            task = self._task(db, identity)
            from folder_state import FolderStageStore
            if not FolderStageStore._can_finish(db, identity, generation):
                raise ResultConflict('task is paused, deleting or has changed generation')
            self._upstream(db, identity, manifest_sha256)
            if db.execute('SELECT 1 FROM postprocessing_results WHERE task_id=? AND group_id=?',
                          (identity['task_id'], identity['group_id'])).fetchone():
                raise ResultConflict('folder already reserved; reconciliation is required')
            if db.execute('SELECT 1 FROM postprocessing_claims WHERE task_id=? AND group_id=?',
                          (identity['task_id'], identity['group_id'])).fetchone():
                raise ResultConflict('folder stage already exists; reconciliation is required')
            result_id, now = str(uuid4()), time.time_ns()
            try:
                db.execute('INSERT INTO postprocessing_results VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    tuple(identity[k] for k in IDENTITY) + (manifest_sha256, generation, result_id,
                    'unresolved', 'processing', None, None, None, None, now, now))
            except sqlite3.IntegrityError as exc:
                raise ResultConflict('folder result identity already reserved') from exc
            return result_id

    @staticmethod
    def _owned(db, identity, result_id):
        row = db.execute('SELECT * FROM postprocessing_results WHERE task_id=? AND group_id=?',
                         (identity['task_id'], identity['group_id'])).fetchone()
        if (not row or row['result_id'] != result_id or any(row[k] != identity[k] for k in IDENTITY)
                or row['state'] not in STATES):
            raise ResultConflict('result ownership or identity mismatch')
        return row

    def _pass_evidence(self, db, identity, claim_id):
        stage = db.execute('SELECT * FROM postprocessing_claims WHERE task_id=? AND group_id=?',
                           (identity['task_id'], identity['group_id'])).fetchone()
        saved = db.execute('SELECT * FROM postprocessing_receipts WHERE claim_id=?', (claim_id,)).fetchone()
        if (not stage or not saved or stage['claim_id'] != claim_id
                or stage['state'] not in {'waiting_rag', 'receipt_held'}
                or any(stage[k] != identity[k] for k in IDENTITY)):
            raise ResultConflict('accepted folder-stage receipt is missing')
        encoded, digest = saved['receipt_json'], _digest(saved['receipt_sha256'])
        if not isinstance(encoded, str) or hashlib.sha256(encoded.encode('utf8')).hexdigest() != digest:
            raise ResultConflict('folder-stage receipt changed')
        value = json.loads(encoded)
        body, attachments = value.get('body'), value.get('attachments')
        if (value.get('manifest_sha256') != stage['manifest_sha256']
                or not isinstance(body, dict) or not isinstance(body.get('package_path'), str)
                or not Path(body['package_path']).is_absolute()
                or not isinstance(attachments, list)):
            raise ResultConflict('folder-stage receipt is incomplete')
        _digest(body.get('manifest_sha256'))
        if not callable(self.inspect_receipt):
            raise ResultConflict('receipt inspection is unavailable')
        self.inspect_receipt(value, identity, claim_id)
        return dict(stage), encoded, digest

    def _fence_unaccepted(self, identity, result_id, claim_id):
        """Keep a not-yet-accepted receipt out of the RAG waiting state."""
        with self._gate(identity['task_id']), closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            owner = self._owned(db, identity, result_id)
            if owner['state'] != 'unresolved':
                return
            stage = db.execute('SELECT * FROM postprocessing_claims WHERE claim_id=?', (claim_id,)).fetchone()
            if (stage and stage['state'] == 'waiting_rag'
                    and all(stage[key] == identity[key] for key in IDENTITY)
                    and stage['manifest_sha256'] == owner['manifest_sha256']):
                db.execute("UPDATE postprocessing_claims SET state='unknown',detail=?,updated_at_ns=? "
                           "WHERE claim_id=? AND state='waiting_rag'",
                           ('result_acceptance_not_confirmed', time.time_ns(), claim_id))

    def record_pass(self, identity, result_id, claim_id, *, validate_receipt):
        identity = _identity(identity)
        if not callable(validate_receipt):
            raise ResultConflict('real receipt validator is required')
        try:
            with closing(self._connect(readonly=True)) as db:
                db.execute('BEGIN')
                owner = self._owned(db, identity, result_id)
                self._upstream(db, identity, owner['manifest_sha256'])
                stage, encoded, digest = self._pass_evidence(db, identity, claim_id)
                if stage['manifest_sha256'] != owner['manifest_sha256']:
                    raise ResultConflict('stage refers to a different input')
            value = json.loads(encoded)
            if validate_receipt(value) is not True or json.dumps(value, ensure_ascii=False, sort_keys=True,
                    separators=(',', ':'), allow_nan=False) != encoded:
                raise ResultConflict('folder-stage receipt is not accepted')
            source_changed = False
            with self._gate(identity['task_id']), closing(self._connect()) as db, db:
                db.execute('BEGIN IMMEDIATE')
                owner = self._owned(db, identity, result_id)
                self._upstream(db, identity, owner['manifest_sha256'])
                current = db.execute('SELECT * FROM postprocessing_claims WHERE claim_id=?', (claim_id,)).fetchone()
                saved = db.execute('SELECT receipt_json,receipt_sha256 FROM postprocessing_receipts WHERE claim_id=?',
                                   (claim_id,)).fetchone()
                if (owner['state'] != 'unresolved' or not current or dict(current) != stage
                        or not saved or saved['receipt_json'] != encoded or saved['receipt_sha256'] != digest):
                    raise ResultConflict('folder stage changed during acceptance')
                try:
                    self._retry_source(db, identity, owner['manifest_sha256'])
                except ResultConflict:
                    # The receipt must not remain eligible for RAG if its
                    # frozen retry source changed before final acceptance.
                    source_changed = True
                    if current['state'] == 'waiting_rag':
                        changed = db.execute("UPDATE postprocessing_claims SET state='unknown',detail=?,updated_at_ns=? "
                                             "WHERE claim_id=? AND state='waiting_rag' AND updated_at_ns=?",
                                             ('source_not_confirmed', time.time_ns(), claim_id, current['updated_at_ns']))
                        if changed.rowcount != 1:
                            raise ResultConflict('the changed receipt could not be fenced')
                else:
                    db.execute("UPDATE postprocessing_results SET state='passed',code='passed',claim_id=?,"
                               'stage_receipt_sha256=?,updated_at_ns=? WHERE result_id=?',
                               (claim_id, digest, time.time_ns(), result_id))
            if source_changed:
                raise ResultConflict('the reserved retry source changed before result acceptance')
        except Exception as exc:
            try:
                self._fence_unaccepted(identity, result_id, claim_id)
            except Exception as fence_error:
                exc.add_note('receipt fencing could not be confirmed: ' + type(fence_error).__name__)
            if isinstance(exc, ResultConflict):
                raise
            raise ResultConflict('folder-stage receipt cannot be verified') from exc

    def _outcome(self, identity, result_id, state, code, *, proof_path=None, proof_sha256=None):
        identity = _identity(identity)
        with self._gate(identity['task_id']), closing(self._connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            owner = self._owned(db, identity, result_id)
            self._upstream(db, identity, owner['manifest_sha256'])
            if owner['state'] != 'unresolved':
                raise ResultConflict('a completed outcome cannot be overwritten')
            stage = db.execute('SELECT * FROM postprocessing_claims WHERE task_id=? AND group_id=?',
                               (identity['task_id'], identity['group_id'])).fetchone()
            if stage and (any(stage[k] != identity[k] for k in IDENTITY)
                          or stage['manifest_sha256'] != owner['manifest_sha256']):
                raise ResultConflict('stage identity changed')
            if state in {'failed', 'unrecognized'} and stage and stage['state'] not in {'failed', 'unknown'}:
                raise ResultConflict('running or completed stage cannot be declared failed')
            if state in {'failed', 'unrecognized'}:
                self._retry_source(db, identity, owner['manifest_sha256'])
            db.execute('UPDATE postprocessing_results SET state=?,code=?,proof_path=?,proof_sha256=?,'
                       'updated_at_ns=? WHERE result_id=?',
                       (state, code, proof_path, proof_sha256, time.time_ns(), result_id))

    def record_failed(self, identity, result_id, *, code, confirm, evidence_path=None):
        identity = _identity(identity)
        try:
            path = Path(evidence_path).resolve(strict=True) if evidence_path is not None else None
            before = _file_digest(path) if path is not None else None
            if ((path is not None and not path.is_file()) or (code == 'body_validation' and path is None)
                    or code not in FAILURES or not callable(confirm) or confirm(dict(identity), code) is not True):
                raise ResultConflict('failure diagnosis is not confirmed')
            digest = _file_digest(path) if path is not None else None
            if before != digest:
                raise ResultConflict('failure diagnosis changed')
        except (OSError, TypeError, ValueError) as exc:
            raise ResultConflict('failure diagnosis is unavailable') from exc
        self._outcome(identity, result_id, 'failed', code,
                      proof_path=str(path) if path is not None else None, proof_sha256=digest)

    def record_unresolved(self, identity, result_id, *, code):
        if code not in UNRESOLVED:
            raise ResultConflict('unknown unresolved reason')
        self._outcome(identity, result_id, 'unresolved', code)

    def record_unrecognized(self, identity, result_id, *, evidence_path, confirm):
        """Only explicit whole-folder evidence may establish this outcome."""
        identity = _identity(identity)
        try:
            path = Path(evidence_path).resolve(strict=True)
            before = _file_digest(path)
            if not path.is_file() or not callable(confirm) or confirm(dict(identity), path) is not True:
                raise ResultConflict('whole-folder unrecognized evidence is not confirmed')
            digest = _file_digest(path)
            if before != digest:
                raise ResultConflict('whole-folder unrecognized evidence changed')
        except (OSError, TypeError, ValueError) as exc:
            raise ResultConflict('whole-folder unrecognized evidence is unavailable') from exc
        self._outcome(identity, result_id, 'unrecognized', 'unrecognized',
                      proof_path=str(path), proof_sha256=digest)

    @staticmethod
    def _unavailable(result_filter, page, page_size):
        result = {'available': False, 'counts': dict.fromkeys(('passed', 'failed', 'unrecognized')),
                  'partial': True, 'unresolved': None, 'as_of': _as_of(), 'message': '后处理结果暂不可核验'}
        if result_filter:
            result['list'] = dict(filter=result_filter, page=page, page_size=page_size,
                                  total=None, has_next=False, items=[], message=result['message'])
        return result

    def summary(self, result_filter=None, page=1, page_size=20):
        """Read a consistent result snapshot; never return false zero on errors."""
        if (result_filter not in {None, 'passed', 'failed', 'unrecognized'}
                or type(page) is not int or not 1 <= page <= 1_000_000
                or type(page_size) is not int or not 1 <= page_size <= 100):
            raise ValueError('invalid result filter or page')
        try:
            with closing(self._connect(readonly=True)) as db:
                db.execute('BEGIN')
                for table in ('tasks', 'task_deletions', 'llm_folder_queue',
                              'postprocessing_claims', 'postprocessing_receipts'):
                    db.execute('SELECT 1 FROM ' + table + ' LIMIT 1').fetchone()
                orphan = db.execute('SELECT 1 FROM postprocessing_claims c '
                    'LEFT JOIN postprocessing_results r ON c.task_id=r.task_id AND c.group_id=r.group_id '
                    'LEFT JOIN task_deletions d ON d.task_id=c.task_id '
                    'WHERE r.task_id IS NULL AND d.task_id IS NULL LIMIT 1').fetchone()
                if orphan:
                    raise ResultConflict('stage exists without a result intent')
                records = db.execute('SELECT * FROM postprocessing_results '
                                     'ORDER BY created_at_ns,task_id,group_id').fetchall()
                buckets = {'passed': [], 'failed': [], 'unrecognized': []}
                unresolved = 0
                for row in records:
                    identity = _identity({key: row[key] for key in IDENTITY})
                    if db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (identity['task_id'],)).fetchone():
                        continue
                    task = self._task(db, identity)
                    if not isinstance(task['name'], str):
                        raise ResultConflict('task name is unavailable')
                    self._upstream(db, identity, _digest(row['manifest_sha256']))
                    if row['state'] not in STATES:
                        raise ResultConflict('unknown result state')
                    if row['state'] == 'unresolved':
                        if row['code'] not in UNRESOLVED:
                            raise ResultConflict('unknown unresolved diagnosis')
                        unresolved += 1
                        continue
                    if row['state'] == 'passed':
                        _, _, digest = self._pass_evidence(db, identity, row['claim_id'])
                        if digest != row['stage_receipt_sha256'] or row['code'] != 'passed':
                            raise ResultConflict('accepted receipt changed')
                        reason = '正文封包和附件交接材料已核验'
                    elif row['state'] == 'failed':
                        if row['code'] not in FAILURES:
                            raise ResultConflict('unknown failure diagnosis')
                        if row['code'] == 'body_validation' and not row['proof_path']:
                            raise ResultConflict('failure diagnosis is missing')
                        if bool(row['proof_path']) != bool(row['proof_sha256']):
                            raise ResultConflict('failure diagnosis reference is incomplete')
                        if row['proof_path'] and _file_digest(row['proof_path']) != _digest(row['proof_sha256']):
                            raise ResultConflict('failure diagnosis changed')
                        reason = FAILURES[row['code']]
                    else:
                        if (row['code'] != 'unrecognized' or not row['proof_path']
                                or _file_digest(row['proof_path']) != _digest(row['proof_sha256'])):
                            raise ResultConflict('unrecognized evidence changed')
                        reason = '整份资料未识别，已有对应证据'
                    buckets[row['state']].append(dict(task_id=identity['task_id'], group_id=identity['group_id'],
                        paper_id=identity['paper_id'], receipt_id=identity['receipt_id'], task_name=task['name'],
                        folder_label='资料 ' + identity['group_id'], reason=reason, code=row['code']))
                result = {'available': True, 'counts': {'passed': len(buckets['passed']),
                    'failed': len(buckets['failed']), 'unrecognized': None}, 'partial': unresolved > 0,
                    'unresolved': unresolved, 'as_of': _as_of()}
                if result_filter:
                    items = buckets[result_filter]
                    start = (page - 1) * page_size
                    total = None if result_filter == 'unrecognized' else len(items)
                    message = ('未识别总数尚不可统计；已确认的资料可在此查看' if result_filter == 'unrecognized'
                               else '暂无资料')
                    result['list'] = dict(filter=result_filter, page=page, page_size=page_size, total=total,
                                          has_next=start + page_size < len(items),
                                          items=items[start:start + page_size], message=message)
                return result
        except Exception:
            return self._unavailable(result_filter, page, page_size)
