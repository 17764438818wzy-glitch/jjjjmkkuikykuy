"""Candidate: one Prefect stage for one existing folder FlowRun.

Importing this module starts nothing. The application must explicitly initialize
the schema, provide gate/identity/receipt adapters, and reserve a selected folder.
No scanner, worker, LLM caller or RAG uploader is started here.
"""
from contextlib import closing
import asyncio
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time
from uuid import UUID, uuid5

import psutil

TASK_KEY = 'paperflow-folder-postprocess-stage-v1'
TAG = 'paperflow-postprocess-stage-v1'
NAMESPACE = UUID('5e9b7732-458e-50cb-8916-6b53007c204f')
FIELDS = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id')


class StageConflict(RuntimeError):
    """A gate, durable record, or identity could not be established."""


class ConfirmedFailure(RuntimeError):
    """The caller has positively recorded a final, non-ambiguous failure."""
    def __init__(self, message, *, code=None, confirm=None, evidence_path=None):
        super().__init__(message)
        self.code = code
        self.confirm = confirm
        self.evidence_path = evidence_path


def _identity(identity):
    if not isinstance(identity, dict) or set(identity) != set(FIELDS):
        raise StageConflict('完整的文件夹身份不可核验')
    try:
        if any(str(UUID(identity[k])) != identity[k] for k in ('task_id', 'parent_run_id', 'task_run_id')):
            raise ValueError('UUID')
    except (TypeError, ValueError, AttributeError) as exc:
        raise StageConflict('文件夹 UUID 不可核验') from exc
    if (not isinstance(identity['paper_id'], str) or not identity['paper_id']
            or not re.fullmatch('[0-9a-f]{32}', str(identity['group_id']))
            or not re.fullmatch('[0-9a-f]{64}', str(identity['receipt_id']))
            or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', str(identity['attempt_id']))):
        raise StageConflict('文件夹业务身份不可核验')
    return {key: identity[key] for key in FIELDS}


def _job_id(folder_run_id):
    # No generation suffix: even a new generation cannot silently redo a folder.
    return str(uuid5(NAMESPACE, str(UUID(folder_run_id))))


def task_key_for_job(job_id):
    try:
        parsed = UUID(job_id)
        if str(parsed) != job_id:
            raise ValueError('noncanonical UUID')
    except (TypeError, ValueError, AttributeError) as exc:
        raise StageConflict('后处理阶段编号不可核验') from exc
    return TASK_KEY + '-' + parsed.hex


def _truth(fn):
    try:
        return callable(fn) and fn() is True
    except Exception:
        return False


def _retry_source_snapshot_db(db, identity, source_generation):
    """Bind one completed manual retry to its original folder and raw answers.

    This only reads the caller's transaction. The actual Prefect retry TaskRun
    and frozen files are separately verified by the original upstream binding.
    """
    identity = _identity(identity)
    if type(source_generation) is not int or source_generation < 1:
        raise StageConflict('原 LLM 代次不可核验')
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    jobs_table, items_table = 'paperflow_llm_stage_jobs', 'paperflow_llm_retry_items'
    if jobs_table not in tables and items_table not in tables:
        return None
    jobs = ([dict(row) for row in db.execute('SELECT * FROM paperflow_llm_stage_jobs WHERE task_id=? AND group_id=?',
             (identity['task_id'], identity['group_id']))] if jobs_table in tables else [])
    items = ([dict(row) for row in db.execute('SELECT * FROM paperflow_llm_retry_items WHERE task_id=? AND group_id=?',
              (identity['task_id'], identity['group_id']))] if items_table in tables else [])
    def checked_command(key, *, require_finished=False):
        if 'paperflow_llm_retry_commands' not in tables:
            raise StageConflict('人工重试命令表缺失')
        commands = [dict(row) for row in db.execute(
            'SELECT * FROM paperflow_llm_retry_commands WHERE request_id=?', (key,))]
        if len(commands) != 1 or (require_finished and commands[0].get('state') != 'finished'):
            raise StageConflict('人工重试命令不可核验')
        try:
            value = json.loads(commands[0]['request_json'])
        except (TypeError, ValueError, KeyError) as exc:
            raise StageConflict('人工重试命令内容不可核验') from exc
        if (not isinstance(value, dict) or value.get('request_id') != key
                or (require_finished and set(value) != {'request_id', 'scope', 'task_id', 'group_id',
                                                      'paper_id', 'receipt_id', 'attempt_id'})
                or value.get('scope') not in {'one', 'all'}
                or (value['scope'] == 'one' and any(value.get(field) != identity[field]
                    for field in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')))
                or (value['scope'] == 'all' and any(value.get(field) is not None
                    for field in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')))):
            raise StageConflict('人工重试命令与原文件夹不一致')
        return value

    def paused_unsent(extra, *, completed_retry=False):
        if jobs_table not in tables:
            raise StageConflict('人工重试阶段表缺失')
        if 'retry_key' not in {row[1] for row in db.execute('PRAGMA table_info(paperflow_llm_stage_jobs)')}:
            raise StageConflict('人工重试阶段身份列缺失')
        allowed_messages = {'大任务当前已暂停，本次未提交'}
        if completed_retry:
            allowed_messages.add('原请求结果未确认，已保留记录，本次未重发')
        if (extra.get('state') not in {'paused', 'needs_review'}
                or extra.get('message') not in allowed_messages
                or any(key not in extra or extra[key] is not None for key in ('job_id', 'folder_run_id', 'generation',
                    'stage_mode', 'old_flow_state_id', 'old_stage_state_id', 'old_job_json'))
                or any(extra.get(key) != identity[key]
                       for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id'))):
            return False
        try:
            key = str(UUID(extra['request_id']))
        except (TypeError, ValueError, AttributeError, KeyError) as exc:
            raise StageConflict('暂停时未提交的重试身份不可核验') from exc
        if key != extra['request_id'] or any(row.get('retry_key') == key for row in jobs):
            raise StageConflict('暂停时未提交的重试已关联阶段')
        checked_command(key, require_finished=True)
        return True

    if len({row.get('request_id') for row in items}) != len(items):
        raise StageConflict('人工重试条目身份重复')
    if any(row.get('state') not in {'submitted', 'not_sent', 'already_running', 'paused', 'needs_review'} for row in items):
        raise StageConflict('人工重试仍在进行或状态未知')
    retries = [row for row in jobs if row.get('retry_key')]
    if not retries:
        if items and not all(paused_unsent(row) for row in items):
            raise StageConflict('人工重试阶段记录缺失')
        if items and any(row.get('state') not in {'finished', 'failed', 'stopped', 'stopped_before_submit'}
                         for row in jobs):
            raise StageConflict('原 LLM 阶段尚未确认结束')
        return None
    if not {'paperflow_llm_retry_items', 'paperflow_llm_retry_commands', 'llm_request_admissions'} <= tables:
        raise StageConflict('人工重试证据表缺失')
    finished = [row for row in retries if row.get('state') == 'finished']
    if len(finished) != 1 or any(row is not finished[0] and row.get('state') not in {
            'failed', 'stopped', 'stopped_before_submit'} for row in jobs):
        raise StageConflict('人工重试完成记录不唯一或存在未确认阶段')
    job = finished[0]
    matches = [row for row in items if row.get('request_id') == job.get('retry_key')
               and row.get('job_id') == job.get('job_id')]
    if len(matches) != 1:
        raise StageConflict('人工重试条目不可核验')
    item = matches[0]
    for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id'):
        if job.get(key) != identity[key] or item.get(key) != identity[key]:
            raise StageConflict('人工重试与原文件夹身份不一致')
    if (job.get('parent_run_id') != identity['parent_run_id']
            or job.get('folder_run_id') != identity['task_run_id']
            or item.get('folder_run_id') != identity['task_run_id']
            or type(job.get('generation')) is not int or job['generation'] != source_generation
            or item.get('generation') != source_generation or item.get('state') != 'submitted'
            or job.get('task_key') != 'paperflow-folder-llm-stage-v1'):
        raise StageConflict('人工重试原工单或代次不一致')
    try:
        retry_key, job_id, old_state = (str(UUID(job['retry_key'])), str(UUID(job['job_id'])),
                                        str(UUID(item['old_flow_state_id'])))
        expected = str(uuid5(UUID('6aaf059a-c89a-52a8-acd5-4a928260a3b7'),
                             identity['task_run_id'] + ':' + str(source_generation) + ':manual:' + retry_key))
        if retry_key != job['retry_key'] or job_id != job['job_id'] or old_state != item['old_flow_state_id'] or job_id != expected:
            raise ValueError('noncanonical retry identity')
    except (TypeError, ValueError, AttributeError, KeyError) as exc:
        raise StageConflict('人工重试身份格式不可核验') from exc
    command = checked_command(retry_key)

    # Each previously failed stage must be the actual predecessor recorded by
    # the retry service, not merely an older-looking row in the same folder.
    prior_failed, prior_stopped, history, seen_jobs, seen_items = [], [], [], {job_id}, {retry_key}
    current_item = item
    while current_item.get('old_job_json') is not None:
        try:
            previous = json.loads(current_item['old_job_json'])
            previous_state = str(UUID(current_item['old_stage_state_id']))
            if (not isinstance(previous, dict) or previous_state != current_item['old_stage_state_id']
                    or not isinstance(previous.get('job_id'), str)):
                raise ValueError('invalid predecessor')
        except (TypeError, ValueError, KeyError, AttributeError) as exc:
            raise StageConflict('人工重试前一阶段记录不可核验') from exc
        previous_rows = [row for row in jobs if row.get('job_id') == previous['job_id']]
        if (len(previous_rows) != 1 or previous['job_id'] in seen_jobs
                or any(previous_rows[0].get(key) != value for key, value in previous.items())
                or previous_rows[0].get('state') != 'failed'):
            raise StageConflict('人工重试前一阶段已变化或不是已确认失败')
        previous_job = previous_rows[0]
        previous_generation = previous_job.get('generation')
        previous_key = previous_job.get('retry_key') or ''
        try:
            suffix = ':manual:' + str(UUID(previous_key)) if previous_key else ''
            expected_previous = str(uuid5(UUID('6aaf059a-c89a-52a8-acd5-4a928260a3b7'),
                identity['task_run_id'] + ':' + str(previous_generation) + suffix))
        except (TypeError, ValueError, AttributeError) as exc:
            raise StageConflict('人工重试前一阶段身份不可核验') from exc
        if (type(previous_generation) is not int or not 1 <= previous_generation <= source_generation
                or (previous_key and suffix != ':manual:' + previous_key)
                or previous_job['job_id'] != expected_previous
                or previous_job.get('parent_run_id') != identity['parent_run_id']
                or previous_job.get('folder_run_id') != identity['task_run_id']
                or previous_job.get('task_key') != 'paperflow-folder-llm-stage-v1'
                or any(previous_job.get(key) != identity[key]
                       for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id'))):
            raise StageConflict('人工重试前一阶段与原文件夹身份不一致')
        seen_jobs.add(previous_job['job_id'])
        prior_failed.append(dict(job_id=previous_job['job_id'], state_id=previous_state))
        if not previous_key:
            history.append(dict(job=previous, state_id=previous_state))
            current_item = None
            break
        previous_items = [row for row in items if row.get('request_id') == previous_key
                          and row.get('job_id') == previous_job['job_id']]
        if (len(previous_items) != 1 or previous_key in seen_items
                or previous_items[0].get('state') != 'submitted'
                or previous_items[0].get('folder_run_id') != identity['task_run_id']
                or previous_items[0].get('generation') != previous_generation
                or any(previous_items[0].get(key) != identity[key]
                       for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id'))):
            raise StageConflict('人工重试前一条目不可核验')
        seen_items.add(previous_key)
        current_item = previous_items[0]
        history.append(dict(job=previous, state_id=previous_state, item=current_item,
                            command=checked_command(previous_key)))
    if current_item is not None and current_item.get('old_stage_state_id') is not None:
        raise StageConflict('人工重试前一阶段记录不完整')
    for extra in items:
        if extra.get('request_id') in seen_items:
            continue
        if any(extra.get(key) != identity[key]
               for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')):
            raise StageConflict('其他人工重试与原文件夹身份不一致')
        if extra.get('state') == 'already_running':
            if extra.get('job_id') or extra.get('folder_run_id') or extra.get('generation'):
                raise StageConflict('重复重试记录身份不可核验')
            history.append(dict(item=extra))
            continue
        if paused_unsent(extra, completed_retry=True):
            extra_key = extra['request_id']
            if extra_key in seen_items:
                raise StageConflict('暂停时未提交的重试身份重复')
            seen_items.add(extra_key)
            # Keep the unresolved-request notice in the retry source history.
            if extra.get('message') == '原请求结果未确认，已保留记录，本次未重发':
                history.append(dict(item=extra, command=checked_command(extra_key)))
            continue
        extra_jobs = [row for row in jobs if row.get('job_id') == extra.get('job_id')
                      and row.get('retry_key') == extra.get('request_id')]
        if (extra.get('state') not in {'not_sent', 'submitted'} or len(extra_jobs) != 1
                or extra_jobs[0].get('state') not in {'stopped_before_submit', 'stopped'}
                or (extra.get('state') == 'not_sent' and extra_jobs[0].get('state') != 'stopped_before_submit')):
            raise StageConflict('存在其他已提交但尚未核验的人工重试')
        extra_job = extra_jobs[0]
        try:
            extra_key = str(UUID(extra['request_id']))
            extra_generation = extra_job['generation']
            expected_extra = str(uuid5(UUID('6aaf059a-c89a-52a8-acd5-4a928260a3b7'),
                identity['task_run_id'] + ':' + str(extra_generation) + ':manual:' + extra_key))
        except (TypeError, ValueError, KeyError, AttributeError) as exc:
            raise StageConflict('未提交重试的身份不可核验') from exc
        if (extra_key != extra['request_id'] or type(extra_generation) is not int
                or not 1 <= extra_generation <= source_generation
                or extra.get('generation') != extra_generation or extra_job.get('job_id') != expected_extra
                or extra.get('folder_run_id') != identity['task_run_id']
                or extra_job.get('folder_run_id') != identity['task_run_id']
                or extra_job.get('parent_run_id') != identity['parent_run_id']
                or extra_job.get('task_key') != 'paperflow-folder-llm-stage-v1'
                or any(extra_job.get(key) != identity[key]
                       for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id'))):
            raise StageConflict('未提交重试与原文件夹身份不一致')
        seen_jobs.add(extra_job['job_id'])
        if extra_job['state'] == 'stopped':
            prior_stopped.append(extra_job['job_id'])
        history.append(dict(job=extra_job, item=extra, command=checked_command(extra_key)))
    for extra_job in jobs:
        if extra_job.get('job_id') in seen_jobs:
            continue
        generation = extra_job.get('generation')
        expected_normal = (str(uuid5(UUID('6aaf059a-c89a-52a8-acd5-4a928260a3b7'),
                           identity['task_run_id'] + ':' + str(generation)))
                           if type(generation) is int else None)
        if (extra_job.get('state') not in {'stopped', 'stopped_before_submit'} or extra_job.get('retry_key')
                or type(generation) is not int or not 1 <= generation <= source_generation
                or extra_job.get('job_id') != expected_normal
                or extra_job.get('folder_run_id') != identity['task_run_id']
                or extra_job.get('parent_run_id') != identity['parent_run_id']
                or extra_job.get('task_key') != 'paperflow-folder-llm-stage-v1'
                or any(extra_job.get(key) != identity[key]
                       for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id'))):
            raise StageConflict('存在未关联的 LLM 阶段历史')
        seen_jobs.add(extra_job['job_id'])
        if extra_job['state'] == 'stopped':
            prior_stopped.append(extra_job['job_id'])
        history.append(dict(job=extra_job))
    if any(row.get('job_id') not in seen_jobs for row in jobs):
        raise StageConflict('存在未关联的 LLM 阶段历史')
    rows = [dict(row) for row in db.execute('SELECT * FROM llm_request_admissions WHERE task_id=? AND task_run_id=?',
                                            (identity['task_id'], identity['task_run_id']))]
    fields = ('task_id', 'task_run_id', 'receipt_id', 'generation', 'component_id', 'kind', 'source_sha256',
              'request_sha256', 'response_sha256', 'state', 'work_dir', 'model_dir')
    if (not rows or not any(row.get('kind') == 'body' for row in rows)
            or any(row.get('state') != 'raw_complete' or not isinstance(row.get('component_id'), str)
                   or not row['component_id'] or row.get('kind') not in {'body', 'attachment'}
                   or row.get('receipt_id') != identity['receipt_id']
                   or type(row.get('generation')) is not int or not 1 <= row['generation'] <= source_generation
                   or any(not isinstance(row.get(key), str) or not re.fullmatch('[0-9a-f]{64}', row[key])
                          for key in ('source_sha256', 'request_sha256', 'response_sha256'))
                   or any(not isinstance(row.get(key), str) or not row[key] for key in ('work_dir', 'model_dir'))
                   for row in rows)
            or len({row['component_id'] for row in rows}) != len(rows)):
        raise StageConflict('人工重试原始回答不完整或身份不可核验')
    proof = dict(identity=identity, generation=source_generation, retry_key=retry_key, job_id=job_id,
                 old_flow_state_id=old_state, command=command, prior_failed=prior_failed,
                 prior_stopped=sorted(prior_stopped),
                 history=sorted(history, key=lambda row: json.dumps(row, ensure_ascii=False, sort_keys=True)),
                 admissions=sorted(({key: row[key] for key in fields} for row in rows), key=lambda row: row['component_id']))
    raw = json.dumps(proof, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf8')
    return dict(retry_key=retry_key, job_id=job_id, old_flow_state_id=old_state,
                prior_failed=prior_failed, prior_stopped=sorted(prior_stopped),
                sha256=hashlib.sha256(raw).hexdigest())


def retry_source_snapshot(database, identity, source_generation):
    """Read one retry source from a consistent SQLite snapshot without writing."""
    path = Path(database).resolve(strict=True)
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        return _retry_source_snapshot_db(db, identity, source_generation)


def reserved_source_matches(database, identity, verified):
    """Recheck the currently bound source before a receipt may advance."""
    try:
        identity = _identity(identity)
        if not isinstance(verified, dict) or verified.get('identity') != identity:
            return False
        source = verified.get('source_llm_generation')
        if type(source) is not int or source < 1:
            return False
        path = Path(database).resolve(strict=True)
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
            db.row_factory = sqlite3.Row
            db.execute('BEGIN')
            selected = _retry_source_snapshot_db(db, identity, source)
            retry = selected['sha256'] if selected else None
            prefect_proof = verified.get('source_retry_prefect_sha256')
            if (retry != verified.get('source_retry_sha256')
                    or (retry is None) != (prefect_proof is None)
                    or (prefect_proof is not None and not re.fullmatch('[0-9a-f]{64}', str(prefect_proof)))):
                return False
            if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                              "AND name='paperflow_postprocess_stage_jobs'").fetchone():
                return retry is None
            row = db.execute('SELECT * FROM paperflow_postprocess_stage_jobs WHERE task_id=? AND group_id=?',
                             (identity['task_id'], identity['group_id'])).fetchone()
            if not row:
                return True
            return (all(row[key] == identity[key] for key in FIELDS)
                    and row['source_llm_generation'] == source
                    and 'source_retry_sha256' in row.keys()
                    and row['source_retry_sha256'] == retry
                    and 'source_retry_prefect_sha256' in row.keys()
                    and row['source_retry_prefect_sha256'] == prefect_proof
                    and row['manifest_sha256'] == verified.get('manifest_sha256'))
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError, StageConflict):
        return False


class RetryAwareUpstream:
    """Use the existing upstream binder for a normal or a completed retry source."""
    def __init__(self, upstream):
        required = ('queue_database', 'control_database', 'client', 'api_url', 'retention_consumer',
                    'verify_receipt', 'receiver_session')
        if any(not hasattr(upstream, key) for key in required) or not callable(getattr(upstream, 'load', None)):
            raise StageConflict('原上游来源绑定不可核验')
        self.upstream = upstream
        self.api_url = upstream.api_url

    def _source(self, identity):
        database = Path(self.upstream.control_database).resolve(strict=True)
        with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
            db.row_factory = sqlite3.Row
            db.execute('BEGIN')
            row = db.execute('SELECT * FROM llm_folder_queue WHERE task_id=? AND group_id=?',
                             (identity['task_id'], identity['group_id'])).fetchone()
            if (not row or any(row[key] != identity[key] for key in FIELDS)
                    or row['state'] != 'waiting_postprocessing'):
                raise StageConflict('原文件夹不在等待后处理或身份已变化')
            generation = row['generation']
            selected = _retry_source_snapshot_db(db, identity, generation)
            return generation, selected

    def _verify_prior_failed(self, selected):
        for prior in selected['prior_failed']:
            try:
                stage = self.upstream.client.read_task_run(UUID(prior['job_id']))
                state = getattr(stage, 'state', None)
                if (str(getattr(stage, 'id', '')) != prior['job_id']
                        or getattr(stage, 'flow_run_id', object()) is not None
                        or getattr(stage, 'task_key', None) != 'paperflow-folder-llm-stage-v1'
                        or getattr(stage, 'dynamic_key', None) != 'paperflow-llm-' + prior['job_id']
                        or 'paperflow-llm-stage-v1' not in (getattr(stage, 'tags', None) or [])
                        or str(getattr(state, 'id', '')) != prior['state_id']
                        or not (_truth(getattr(state, 'is_failed', None))
                                or _truth(getattr(state, 'is_crashed', None)))):
                    raise StageConflict('此前人工重试的 Prefect 失败记录不可核验')
            except StageConflict:
                raise
            except Exception as exc:
                raise StageConflict('此前人工重试的 Prefect 失败记录不可读取') from exc

    def _verify_prior_stopped(self, selected):
        signature = []
        for job_id in selected['prior_stopped']:
            try:
                stage = self.upstream.client.read_task_run(UUID(job_id))
                state = getattr(stage, 'state', None)
                state_id = str(UUID(str(getattr(state, 'id', ''))))
                if (str(getattr(stage, 'id', '')) != job_id
                        or getattr(stage, 'flow_run_id', object()) is not None
                        or getattr(stage, 'task_key', None) != 'paperflow-folder-llm-stage-v1'
                        or getattr(stage, 'dynamic_key', None) != 'paperflow-llm-' + job_id
                        or 'paperflow-llm-stage-v1' not in (getattr(stage, 'tags', None) or [])
                        or not _truth(getattr(state, 'is_completed', None))):
                    raise StageConflict('此前暂停重试的 Prefect 阶段尚未结束')
                signature.append((job_id, state_id))
            except StageConflict:
                raise
            except Exception as exc:
                raise StageConflict('此前暂停重试的 Prefect 阶段不可读取') from exc
        return tuple(signature)

    def _verify_completed_retry(self, selected):
        # The original retry binder also checks this run. Retain its concrete
        # completion state so a later observation cannot silently replace it.
        chosen = dict(prior_stopped=[selected['job_id']])
        return self._verify_prior_stopped(chosen)[0]

    def load(self, identity, *, require_running=False):
        if require_running:
            raise StageConflict('后处理执行身份必须由独立阶段核验')
        identity = _identity(identity)
        before = self._source(identity)
        selected = before[1]
        binding = self.upstream
        if selected is not None:
            self._verify_prior_failed(selected)
            stopped_before = self._verify_prior_stopped(selected)
            completed_before = self._verify_completed_retry(selected)
            binding = type(self.upstream)(self.upstream.queue_database, self.upstream.control_database,
                self.upstream.client, self.upstream.api_url, self.upstream.retention_consumer,
                self.upstream.verify_receipt, receiver_session=self.upstream.receiver_session,
                execution_task_run_id=selected['job_id'], execution_task_key='paperflow-folder-llm-stage-v1',
                allow_terminal_retry=True, allow_completed_retry=True)
        verified = binding.load(identity, require_running=False)
        if selected is not None:
            self._verify_prior_failed(selected)
            if self._verify_prior_stopped(selected) != stopped_before:
                raise StageConflict('此前暂停重试的 Prefect 状态在核验期间变化')
            if self._verify_completed_retry(selected) != completed_before:
                raise StageConflict('成功重试的 Prefect 状态在核验期间变化')
        if not isinstance(verified, dict) or self._source(identity) != before:
            raise StageConflict('上游人工重试来源在核验期间发生变化')
        proof = (hashlib.sha256(json.dumps(dict(completed=completed_before, stopped=stopped_before),
                 sort_keys=True, separators=(',', ':')).encode('utf8')).hexdigest() if selected else None)
        return {**verified, 'source_retry_sha256': selected['sha256'] if selected else None,
                'source_retry_prefect_sha256': proof}


def _task_identity(stage, item):
    return (stage is not None and str(getattr(stage, 'id', '')) == item['job_id']
            and getattr(stage, 'flow_run_id', object()) is None
            and getattr(stage, 'task_key', None) == item['task_key'] == task_key_for_job(item['job_id'])
            and getattr(stage, 'dynamic_key', None) == 'paperflow-postprocess-' + item['job_id']
            and TAG in (getattr(stage, 'tags', None) or []))


def validate_prefect_stage(item, context_task, client, *, expected_api_url, retry_upstream=None):
    """Check the actual postprocess TaskRun and its original folder source."""
    if (not isinstance(expected_api_url, str) or not expected_api_url.strip()
            or str(getattr(client, 'api_url', '')).rstrip('/') != expected_api_url.rstrip('/')):
        raise StageConflict('后处理阶段 Prefect 服务身份不一致')
    if not _task_identity(context_task, item):
        raise StageConflict('Prefect 执行上下文身份不一致')
    current = client.read_task_run(UUID(item['job_id']))
    if (not _task_identity(current, item) or not getattr(current, 'state', None)
            or not current.state.is_running()):
        raise StageConflict('Prefect 后处理阶段未确认运行')
    folder = client.read_flow_run(UUID(item['folder_run_id']))
    if str(getattr(folder, 'id', '')) != item['folder_run_id'] or not getattr(folder, 'state', None):
        raise StageConflict('原文件夹工单身份不可核验')
    proof = item.get('source_retry_sha256')
    if proof is not None:
        if (not isinstance(proof, str) or not re.fullmatch('[0-9a-f]{64}', proof)
                or not isinstance(item.get('source_retry_prefect_sha256'), str)
                or not re.fullmatch('[0-9a-f]{64}', item['source_retry_prefect_sha256'])
                or not callable(getattr(retry_upstream, 'load', None))):
            raise StageConflict('人工重试来源不可核验')
        identity = _identity({key: item[key] for key in FIELDS})
        verified = retry_upstream.load(identity, require_running=False)
        if (not isinstance(verified, dict) or verified.get('source_retry_sha256') != proof
                or verified.get('source_retry_prefect_sha256') != item.get('source_retry_prefect_sha256')):
            raise StageConflict('人工重试来源发生变化')
        terminal = (callable(getattr(folder.state, 'is_failed', None)) and folder.state.is_failed()) or (
            callable(getattr(folder.state, 'is_crashed', None)) and folder.state.is_crashed())
        if terminal:
            return True
    if (not folder.state.is_paused()
            or getattr(folder.state.state_details, 'pause_reschedule', None) is not True
            or not (getattr(folder, 'name', '') or '').endswith('等待后处理')):
        raise StageConflict('原文件夹工单尚未处于等待后处理')
    return True


class StageBoundHandoff:
    """Preserve upstream binding checks while verifying our separate TaskRun."""
    def __init__(self, upstream, client, item, context_task):
        self.upstream, self.client, self.item, self.context_task = upstream, client, item, context_task

    def load(self, identity, *, require_running=False):
        expected = {key: self.item[key] for key in FIELDS}
        if _identity(identity) != expected:
            raise StageConflict('后处理与上游文件夹身份不同')
        if require_running:
            validate_prefect_stage(self.item, self.context_task, self.client,
                                   expected_api_url=getattr(self.upstream, 'api_url', None),
                                   retry_upstream=self.upstream)
        verified = self.upstream.load(identity, require_running=False)
        if (not isinstance(verified, dict)
                or verified.get('source_retry_sha256') != self.item.get('source_retry_sha256')
                or verified.get('source_retry_prefect_sha256') != self.item.get('source_retry_prefect_sha256')):
            raise StageConflict('后处理预约的上游人工重试来源发生变化')
        if require_running:
            validate_prefect_stage(self.item, self.context_task, self.client,
                                   expected_api_url=getattr(self.upstream, 'api_url', None),
                                   retry_upstream=self.upstream)
        return verified


class Coordinator:
    """One-shot reservation, submission and execution; no automatic scanner."""
    def __init__(self, database, *, task_gate, folder_gate, postprocess_is_open, rag_is_held,
                 verify_upstream, verify_execution, slot, process_folder, results):
        self.database = Path(database).resolve(strict=True)
        self.task_gate, self.folder_gate = task_gate, folder_gate
        self.postprocess_is_open, self.rag_is_held = postprocess_is_open, rag_is_held
        self.verify_upstream, self.verify_execution = verify_upstream, verify_execution
        self.slot, self.process_folder, self.results = slot, process_folder, results
        if (not all(callable(x) for x in (task_gate, folder_gate, postprocess_is_open, rag_is_held,
                                           verify_upstream, verify_execution, slot, process_folder))
                or not all(callable(getattr(results, x, None)) for x in ('reserve', 'success', 'failure', 'unknown'))):
            raise ValueError('后处理阶段依赖不完整')

    def _db(self, *, readonly=False):
        db = sqlite3.connect(self.database.as_uri() + ('?mode=ro' if readonly else '?mode=rw'),
                             uri=True, timeout=30)
        db.row_factory = sqlite3.Row
        return db

    def initialize(self):
        """Explicit schema setup only; never opens a stage gate or starts work."""
        with closing(self._db()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('''CREATE TABLE IF NOT EXISTS paperflow_postprocess_stage_jobs (
                job_id TEXT PRIMARY KEY,task_id TEXT NOT NULL,group_id TEXT NOT NULL,paper_id TEXT NOT NULL,
                receipt_id TEXT NOT NULL,attempt_id TEXT NOT NULL,parent_run_id TEXT NOT NULL,
                task_run_id TEXT NOT NULL UNIQUE,folder_run_id TEXT NOT NULL,generation INTEGER NOT NULL,
                manifest_sha256 TEXT NOT NULL,task_key TEXT NOT NULL,state TEXT NOT NULL,error_type TEXT,
                owner_pid INTEGER,owner_started REAL,created_at_ns INTEGER NOT NULL,updated_at_ns INTEGER NOT NULL,
                source_llm_generation INTEGER NOT NULL,source_retry_sha256 TEXT,source_retry_prefect_sha256 TEXT,
                UNIQUE(task_id,group_id))''')
            columns = {row['name']: row for row in db.execute('PRAGMA table_info(paperflow_postprocess_stage_jobs)')}
            if 'source_llm_generation' not in columns:
                if db.execute('SELECT 1 FROM paperflow_postprocess_stage_jobs LIMIT 1').fetchone():
                    raise StageConflict('旧后处理阶段表非空，不允许自动推测原 LLM 代次')
                # SQLite permits adding the nullable column to the old, empty
                # table; every new row is checked and explicitly populated.
                db.execute('ALTER TABLE paperflow_postprocess_stage_jobs ADD COLUMN source_llm_generation INTEGER')
            elif str(columns['source_llm_generation']['type']).upper() != 'INTEGER':
                raise StageConflict('后处理源代次字段类型不可核验')
            if 'source_retry_sha256' not in columns:
                # Existing jobs remain untouched. A missing proof cannot later
                # be treated as an already-bound manual retry.
                db.execute('ALTER TABLE paperflow_postprocess_stage_jobs ADD COLUMN source_retry_sha256 TEXT')
            elif str(columns['source_retry_sha256']['type']).upper() != 'TEXT':
                raise StageConflict('后处理重试来源字段类型不可核验')
            if 'source_retry_prefect_sha256' not in columns:
                db.execute('ALTER TABLE paperflow_postprocess_stage_jobs ADD COLUMN source_retry_prefect_sha256 TEXT')
            elif str(columns['source_retry_prefect_sha256']['type']).upper() != 'TEXT':
                raise StageConflict('后处理重试 Prefect 来源字段类型不可核验')
            invalid = db.execute('SELECT 1 FROM paperflow_postprocess_stage_jobs '
                'WHERE source_llm_generation IS NULL OR source_llm_generation < 1 '
                'OR source_llm_generation > generation LIMIT 1').fetchone()
            if invalid:
                raise StageConflict('后处理阶段存在无法核验的原 LLM 代次')

    def _gates(self):
        if not _truth(self.postprocess_is_open):
            raise StageConflict('后处理未开放')

    def _verified(self, identity, generation):
        evidence = self.verify_upstream(dict(identity), generation)
        if (not isinstance(evidence, dict) or evidence.get('identity') != identity
                or type(evidence.get('generation')) is not int or evidence['generation'] != generation
                or evidence.get('raw_complete') is not True or evidence.get('session_current') is not True
                or evidence.get('not_deleted') is not True
                or not re.fullmatch('[0-9a-f]{64}', str(evidence.get('manifest_sha256', '')))):
            raise StageConflict('上游完整回答或当前会话不可核验')
        source = evidence.get('source_llm_generation')
        if type(source) is not int or not 1 <= source <= generation:
            raise StageConflict('上游 LLM 原代次不可核验')
        retry = evidence.get('source_retry_sha256')
        if retry is not None and (not isinstance(retry, str) or not re.fullmatch('[0-9a-f]{64}', retry)):
            raise StageConflict('上游人工重试来源不可核验')
        prefect_proof = evidence.get('source_retry_prefect_sha256')
        if ((retry is None) != (prefect_proof is None)
                or (prefect_proof is not None and (not isinstance(prefect_proof, str)
                    or not re.fullmatch('[0-9a-f]{64}', prefect_proof)))):
            raise StageConflict('上游人工重试 Prefect 来源不可核验')
        return evidence['manifest_sha256'], source, retry, prefect_proof

    @staticmethod
    def _check_db(db, identity, generation, manifest, source_generation, *, retry=None, allow_existing=False):
        task = db.execute('SELECT * FROM tasks WHERE task_id=?', (identity['task_id'],)).fetchone()
        if (not task or task['desired_state'] != 'running' or task['start_generation'] != generation
                or task['start_pending'] or task['task_status'] in {'deleting', 'delete_failed', 'unknown'}
                or task['prefect_order_model'] != 'v2' or task['prefect_run_id'] != identity['parent_run_id']
                or db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (identity['task_id'],)).fetchone()):
            raise StageConflict('大工单已暂停、删除或代次不符')
        row = db.execute('SELECT * FROM llm_folder_queue WHERE task_id=? AND group_id=?',
                         (identity['task_id'], identity['group_id'])).fetchone()
        if (not row or any(row[key] != identity[key] for key in FIELDS)
                or row['state'] != 'waiting_postprocessing' or row['generation'] != source_generation
                or type(source_generation) is not int or not 1 <= source_generation <= generation
                or row['manifest_sha256'] != manifest):
            raise StageConflict('上游文件夹完成状态或身份有变化')
        rows = db.execute('SELECT state,kind FROM llm_request_admissions WHERE task_id=? AND task_run_id=?',
                          (identity['task_id'], identity['task_run_id'])).fetchall()
        if (not rows or not any(row['kind'] == 'body' for row in rows)
                or any(row['state'] != 'raw_complete' for row in rows)):
            raise StageConflict('模型回答未全部确认完成')
        selected = _retry_source_snapshot_db(db, identity, source_generation)
        if (selected['sha256'] if selected else None) != retry:
            raise StageConflict('预约期间人工重试来源或原始回答发生变化')
        # Missing evidence tables are never proof of an empty history.
        for name in ('postprocessing_claims', 'postprocessing_results'):
            if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone():
                raise StageConflict('后处理领取或结果台账不可核验')
        # A prior material-processing claim is never silently reused or rerun.
        if not allow_existing:
            if db.execute('SELECT 1 FROM postprocessing_claims WHERE task_id=? AND group_id=?',
                          (identity['task_id'], identity['group_id'])).fetchone():
                raise StageConflict('已有后处理领取记录，需保留并核对')
        if not allow_existing:
            if db.execute('SELECT 1 FROM postprocessing_results WHERE task_id=? AND group_id=?',
                          (identity['task_id'], identity['group_id'])).fetchone():
                raise StageConflict('已有处理结果预约，需保留并核对')

    def reserve(self, identity, generation):
        """Reserve exactly one existing folder; no task is submitted here."""
        identity = _identity(identity)
        if type(generation) is not int or generation < 1:
            raise StageConflict('任务代次不可核验')
        self._gates()
        manifest, source_generation, retry, prefect_proof = self._verified(identity, generation)
        job_id, now = _job_id(identity['task_run_id']), time.time_ns()
        with self.task_gate(identity['task_id']), closing(self._db()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            self._gates()
            self._check_db(db, identity, generation, manifest, source_generation, retry=retry)
            try:
                db.execute('''INSERT INTO paperflow_postprocess_stage_jobs
                    (job_id,task_id,group_id,paper_id,receipt_id,attempt_id,parent_run_id,task_run_id,
                     folder_run_id,generation,manifest_sha256,task_key,state,error_type,owner_pid,owner_started,
                     created_at_ns,updated_at_ns,source_llm_generation,source_retry_sha256,source_retry_prefect_sha256)
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (job_id, *(identity[k] for k in FIELDS), identity['task_run_id'], generation,
                     manifest, task_key_for_job(job_id), 'reserved', None, None, None, now, now, source_generation, retry,
                     prefect_proof))
            except sqlite3.IntegrityError as exc:
                raise StageConflict('该文件夹已有后处理阶段记录，禁止重领') from exc
            self._gates()
        return self.job(job_id)

    def rearm_stopped(self, job_id, generation, *, client, expected_api_url):
        """Reopen only a provably never-submitted reservation under the same ID."""
        from prefect.exceptions import ObjectNotFound
        item = self.job(job_id)
        identity = _identity({key: item[key] for key in FIELDS})
        if (item['state'] != 'stopped_before_submit' or job_id != _job_id(identity['task_run_id'])
                or item['folder_run_id'] != identity['task_run_id'] or item['task_key'] != task_key_for_job(job_id)
                or type(generation) is not int or generation <= item['generation']
                or item['owner_pid'] is not None or item['owner_started'] is not None or item['error_type'] is not None):
            raise StageConflict('阶段并非可证明未提交的旧代次预约')
        self._gates()
        manifest, source, retry, prefect_proof = self._verified(identity, generation)
        if (manifest != item['manifest_sha256'] or source != item['source_llm_generation']
                or retry != item.get('source_retry_sha256')
                or prefect_proof != item.get('source_retry_prefect_sha256')):
            raise StageConflict('阶段冻结的上游材料或原代次发生变化')
        if (not isinstance(expected_api_url, str) or not expected_api_url.strip()
                or str(getattr(client, 'api_url', '')).rstrip('/') != expected_api_url.rstrip('/')):
            raise StageConflict('Prefect 服务身份不可核验')
        try:
            client.read_task_run(UUID(job_id))
        except ObjectNotFound:
            pass
        else:
            raise StageConflict('Prefect 已存在阶段，禁止重复提交')
        with self.task_gate(identity['task_id']), closing(self._db()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            self._gates()
            self._check_db(db, identity, generation, manifest, source, retry=retry)
            current = db.execute('SELECT * FROM paperflow_postprocess_stage_jobs WHERE job_id=?', (job_id,)).fetchone()
            if not current or dict(current) != item:
                raise StageConflict('远端核验期间阶段状态发生变化')
            changed = db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='reserved',generation=?,"
                "updated_at_ns=? WHERE job_id=? AND state='stopped_before_submit' AND updated_at_ns=?",
                (generation, time.time_ns(), job_id, item['updated_at_ns'])).rowcount
            if changed != 1:
                raise StageConflict('阶段恢复预约发生并发变化')
            self._gates()
        return self.job(job_id)

    def reconcile_held(self, job_id, generation, *, store, validator_factory):
        """Revalidate one already-completed receipt; only advance its held claim.

        This synchronous administrative primitive does not submit work. Callers
        must schedule it with the same folder ownership and resource policy.
        """
        item = self.job(job_id)
        identity = _identity({key: item[key] for key in FIELDS})
        if (item['state'] != 'finished' or job_id != _job_id(identity['task_run_id'])
                or item['folder_run_id'] != identity['task_run_id'] or item['task_key'] != task_key_for_job(job_id)
                or type(generation) is not int or generation <= item['generation']
                or Path(getattr(store, 'database', '')).resolve() != self.database
                or not callable(validator_factory) or not callable(getattr(store, '_observed', None))):
            raise StageConflict('阶段不是可核对的已完成旧代次回执')
        with self.folder_gate(item):
            self._gates()
            manifest, source, retry, prefect_proof = self._verified(identity, generation)
            if (manifest != item['manifest_sha256'] or source != item['source_llm_generation']
                    or retry != item.get('source_retry_sha256')
                    or prefect_proof != item.get('source_retry_prefect_sha256')):
                raise StageConflict('上游材料或原 LLM 代次改变')
            with closing(self._db(readonly=True)) as db:
                db.execute('BEGIN')
                current = db.execute('SELECT * FROM paperflow_postprocess_stage_jobs WHERE job_id=?', (job_id,)).fetchone()
                claim = db.execute('SELECT * FROM postprocessing_claims WHERE task_id=? AND group_id=?',
                                   (identity['task_id'], identity['group_id'])).fetchone()
                result = db.execute('SELECT * FROM postprocessing_results WHERE task_id=? AND group_id=?',
                                    (identity['task_id'], identity['group_id'])).fetchone()
                if (not current or dict(current) != item or not claim or not result
                        or claim['state'] != 'receipt_held' or result['state'] != 'passed'
                        or any(claim[key] != identity[key] or result[key] != identity[key] for key in FIELDS)
                        or claim['generation'] != item['generation'] or result['generation'] != item['generation']
                        or claim['manifest_sha256'] != manifest or result['manifest_sha256'] != manifest
                        or result['claim_id'] != claim['claim_id']):
                    raise StageConflict('原阶段、领取和结果身份不可核验')
                saved = db.execute('SELECT * FROM postprocessing_receipts WHERE claim_id=?',
                                   (claim['claim_id'],)).fetchone()
                if (not saved or result['stage_receipt_sha256'] != saved['receipt_sha256']
                        or hashlib.sha256(saved['receipt_json'].encode('utf8')).hexdigest() != saved['receipt_sha256']):
                    raise StageConflict('原阶段回执不完整或哈希不符')
                claim, result, saved = dict(claim), dict(result), dict(saved)
            validator = validator_factory(dict(item), claim['claim_id'])
            encoded, digest = store._accepted_receipt(identity, claim['claim_id'], json.loads(saved['receipt_json']), validator)
            if encoded != saved['receipt_json'] or digest != saved['receipt_sha256']:
                raise StageConflict('原阶段回执在复核期间发生变化')
            if (not store._observed(identity)
                    or self._verified(identity, generation) != (manifest, source, retry, prefect_proof)):
                raise StageConflict('文件夹或冻结上游在复核期间发生变化')
            with self.task_gate(identity['task_id']), closing(self._db()) as db, db:
                db.execute('BEGIN IMMEDIATE')
                self._gates()
                self._check_db(db, identity, generation, manifest, source, retry=retry, allow_existing=True)
                checks = (('paperflow_postprocess_stage_jobs', 'job_id', job_id, item),
                          ('postprocessing_claims', 'claim_id', claim['claim_id'], claim),
                          ('postprocessing_results', 'result_id', result['result_id'], result),
                          ('postprocessing_receipts', 'claim_id', claim['claim_id'], saved))
                for table, column, value, expected in checks:
                    row = db.execute('SELECT * FROM ' + table + ' WHERE ' + column + '=?', (value,)).fetchone()
                    if not row or dict(row) != expected:
                        raise StageConflict('复核期间持久状态发生变化')
                updated = db.execute("UPDATE postprocessing_claims SET state='waiting_rag',generation=?,updated_at_ns=? "
                    "WHERE claim_id=? AND state='receipt_held' AND updated_at_ns=?",
                    (generation, time.time_ns(), claim['claim_id'], claim['updated_at_ns'])).rowcount
                if updated != 1:
                    raise StageConflict('回执状态发生并发变化')
                self._gates()
                return store._result(store._owned(db, identity, claim['claim_id']))

    def job(self, job_id):
        with closing(self._db(readonly=True)) as db:
            row = db.execute('SELECT * FROM paperflow_postprocess_stage_jobs WHERE job_id=?', (job_id,)).fetchone()
        if not row:
            raise StageConflict('后处理阶段预约不存在')
        return dict(row)

    def jobs_for_task(self, task_id):
        """One read-only snapshot for UI reconciliation; it does not claim liveness."""
        try:
            if str(UUID(task_id)) != task_id:
                raise ValueError('noncanonical UUID')
        except (TypeError, ValueError, AttributeError) as exc:
            raise StageConflict('大工单身份不可核验') from exc
        with closing(self._db(readonly=True)) as db:
            db.execute('BEGIN')
            return [dict(row) for row in db.execute('SELECT * FROM paperflow_postprocess_stage_jobs '
                'WHERE task_id=? ORDER BY created_at_ns,job_id', (task_id,))]

    def _move(self, job_id, before, after, error=None):
        with closing(self._db()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            return db.execute('UPDATE paperflow_postprocess_stage_jobs SET state=?,error_type=?,updated_at_ns=? '
                              'WHERE job_id=? AND state=?', (after, error, time.time_ns(), job_id, before)).rowcount == 1

    def submit(self, job_id, client, task):
        """Submit once; an ambiguous create response is never resent."""
        item = self.job(job_id)
        self._gates()
        identity = {key: item[key] for key in FIELDS}
        if item.get('source_retry_sha256') is not None:
            current = self._verified(identity, item['generation'])
            if current != (item['manifest_sha256'], item['source_llm_generation'],
                           item['source_retry_sha256'], item.get('source_retry_prefect_sha256')):
                raise StageConflict('提交前人工重试的冻结来源发生变化')
        with self.task_gate(identity['task_id']), closing(self._db()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            self._gates()
            self._check_db(db, identity, item['generation'], item['manifest_sha256'], item['source_llm_generation'],
                           retry=item.get('source_retry_sha256'))
            changed = db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitting',updated_at_ns=? "
                                 "WHERE job_id=? AND state='reserved'", (time.time_ns(), job_id)).rowcount
            if changed != 1:
                raise StageConflict('预约已提交或结果未知，禁止重发')
            self._gates()
        try:
            from prefect.states import Scheduled
            state = Scheduled()
            state.state_details.deferred = True
            actual = client.create_task_run(task=task, flow_run_id=None, id=UUID(job_id),
                dynamic_key='paperflow-postprocess-' + job_id, name='资料文件夹后处理阶段', state=state)
            if not _task_identity(actual, item):
                raise StageConflict('Prefect 返回的后处理阶段身份不一致')
            # The worker may have already claimed it before create returns.
            self._move(job_id, 'submitting', 'submitted')
            return self.job(job_id)
        except BaseException as exc:
            self._move(job_id, 'submitting', 'submission_unknown', type(exc).__name__)
            raise

    async def execute(self, job_id, context_task):
        """Use the injected native Prefect capacity lease over one whole folder."""
        from paperflow_prefect.resources import drain_thread
        self._gates()
        async with self.slot():
            return await drain_thread(self._execute_sync, job_id, context_task)

    def _execute_sync(self, job_id, context_task):
        item = self.job(job_id)
        identity = _identity({key: item[key] for key in FIELDS})
        if (job_id != _job_id(identity['task_run_id']) or item['folder_run_id'] != identity['task_run_id']
                or item['task_key'] != task_key_for_job(job_id)):
            raise StageConflict('后处理阶段预约身份不一致')
        owned, result_id = False, None
        try:
            with self.folder_gate(item):
                self._gates()
                if self.verify_execution(item, context_task) is not True:
                    raise StageConflict('真实后处理 TaskRun 未确认')
                manifest, source_generation, retry, prefect_proof = self._verified(identity, item['generation'])
                if (manifest != item['manifest_sha256'] or source_generation != item['source_llm_generation']
                        or retry != item.get('source_retry_sha256')
                        or prefect_proof != item.get('source_retry_prefect_sha256')):
                    raise StageConflict('冻结材料身份已改变')
                with self.task_gate(identity['task_id']), closing(self._db()) as db, db:
                    db.execute('BEGIN IMMEDIATE')
                    self._gates()
                    self._check_db(db, identity, item['generation'], manifest, source_generation, retry=retry)
                    changed = db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='executing',owner_pid=?,"
                        "owner_started=?,updated_at_ns=? WHERE job_id=? AND state IN ('submitting','submitted')",
                        (os.getpid(), psutil.Process(os.getpid()).create_time(), time.time_ns(), job_id)).rowcount
                    if changed != 1:
                        raise StageConflict('阶段已领取或执行结果未知，禁止重做')
                owned = True
                self._gates()
                if self.verify_execution(item, context_task) is not True:
                    raise StageConflict('真实后处理 TaskRun 不再可核验')
                # This durable result identity exists before parsing any material.
                result_id = self.results.reserve(identity, item['generation'], manifest)
                if not isinstance(result_id, str) or not result_id:
                    raise StageConflict('处理结果预约身份不可核验')
                self._gates()
                if self._verified(identity, item['generation']) != (manifest, source_generation, retry, prefect_proof):
                    raise StageConflict('后处理前上游材料发生变化')
                accepted = []
                def accept(stage_result, validate_receipt):
                    if (accepted or not isinstance(stage_result, dict)
                            or stage_result.get('state') not in {'waiting_rag', 'receipt_held'}
                            or stage_result.get('identity') != identity
                            or not isinstance(stage_result.get('claim_id'), str)
                            or not stage_result['claim_id'] or not callable(validate_receipt)):
                        raise StageConflict('后处理阶段交接回执不可核验')
                    frozen_result = copy.deepcopy(stage_result)
                    self.results.success(identity, result_id, stage_result, validate_receipt)
                    if stage_result != frozen_result:
                        raise StageConflict('交接校验期间结果发生变化')
                    accepted.append(frozen_result)
                result = self.process_folder(identity, item['generation'], item, accept, context_task)
                if len(accepted) != 1 or result != accepted[0]:
                    raise StageConflict('后处理返回结果未经过同一回执校验')
                if not self._move(job_id, 'executing', 'finished'):
                    raise StageConflict('后处理阶段完成状态冲突')
                return result
        except BaseException as exc:
            if owned:
                known = isinstance(exc, ConfirmedFailure)
                if result_id:
                    try:
                        (self.results.failure if known else self.results.unknown)(identity, result_id, exc)
                    except BaseException as record_error:
                        known = False
                        exc.add_note('result outcome could not be recorded: ' + type(record_error).__name__)
                self._move(job_id, 'executing', 'failed' if known else 'execution_unknown', type(exc).__name__)
            raise


def build_task(runtime_factory, job_id):
    """Create a real Prefect Task definition. Constructing it dispatches nothing."""
    if not callable(runtime_factory):
        raise ValueError('缺少后处理运行时工厂')
    from prefect import task
    from prefect.cache_policies import CacheKeyFnPolicy
    from prefect.context import TaskRunContext

    @task(name='资料文件夹后处理阶段', tags=[TAG], retries=0, persist_result=True,
          cache_policy=CacheKeyFnPolicy(cache_key_fn=lambda _context, _parameters: None))
    async def execute_postprocessing_stage():
        context = TaskRunContext.get()
        if context is None or context.task_run is None:
            raise StageConflict('缺少真实的 Prefect TaskRun')
        coordinator = runtime_factory()
        if not isinstance(coordinator, Coordinator):
            raise StageConflict('后处理运行时不可核验')
        return await coordinator.execute(str(context.task_run.id), context.task_run)

    execute_postprocessing_stage.task_key = task_key_for_job(job_id)
    return execute_postprocessing_stage


def make_one_worker(stage, job_id):
    """Construct, but do not start, a worker subscribed to one stage only."""
    from prefect.task_worker import TaskWorker
    expected = task_key_for_job(job_id)
    if getattr(stage, 'task_key', None) != expected:
        raise StageConflict('单工单 worker 的任务键不一致')
    worker = TaskWorker(stage, limit=1)
    if worker.task_keys != {expected} or len(worker.tasks) != 1:
        raise StageConflict('单工单 worker 订阅范围不一致')
    # TaskWorker clones a Task and may recompute its code-derived key.
    worker.tasks[0].task_key = expected
    return worker
