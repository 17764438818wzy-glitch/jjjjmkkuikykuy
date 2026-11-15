"""Read-only display projection for the existing Prefect folder tickets.

This module neither dispatches work nor changes a task, queue, or receipt.
"""
from collections import defaultdict
from contextlib import closing
from datetime import datetime, timezone
import json
import sqlite3
from uuid import UUID, uuid5

from fastapi import Response

FIELDS = ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id')
RETRY_NAMESPACE = UUID('6aaf059a-c89a-52a8-acd5-4a928260a3b7')
BASIS = 'prefect-folder-stage-positions/1'
STAGES = ('waiting_postprocessing', 'postprocessing_in_progress', 'waiting_rag_upload',
          'rag_in_progress', 'waiting_nas_archive_delete', 'nas_in_progress', 'completed')
OUTCOMES = ('passed', 'failed', 'unrecognized')
DOWNSTREAM_NAMESPACE = UUID('ccf886ef-7347-4204-9a83-4fa33586d046')


def _db(path):
    db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=3)
    db.row_factory = sqlite3.Row
    return db


def _rows(db, sql):
    return [dict(row) for row in db.execute(sql)]


def _unique(rows, fields):
    result = {}
    for row in rows:
        key = tuple(row[field] for field in fields)
        if key in result:
            raise ValueError('duplicate order identity')
        result[key] = row
    return result


def _snapshot(projector):
    key_fields = ('task_id', 'group_id')
    with closing(_db(projector.control)) as db:
        db.execute('BEGIN')
        tasks = _unique(_rows(db, 'SELECT task_id,name,prefect_run_id,prefect_order_model FROM tasks'), ('task_id',))
        deleted = {row['task_id'] for row in _rows(db, 'SELECT task_id FROM task_deletions')}
        folders = _unique(_rows(db, 'SELECT ' + ','.join(FIELDS) + ',state,generation,manifest_sha256 '
                                  'FROM llm_folder_queue'), key_fields)
        jobs = _unique(_rows(db, 'SELECT ' + ','.join(FIELDS) + ',job_id,folder_run_id,task_key,state,'
                               'generation,source_llm_generation,manifest_sha256 FROM paperflow_postprocess_stage_jobs'), key_fields)
        claims = _unique(_rows(db, 'SELECT ' + ','.join(FIELDS) + ',claim_id,state,manifest_sha256 '
                                 'FROM postprocessing_claims'), key_fields)
        results = _unique(_rows(db, 'SELECT ' + ','.join(FIELDS) + ',result_id,claim_id,state,code,'
                                  'manifest_sha256,stage_receipt_sha256,created_at_ns FROM postprocessing_results'), key_fields)
        admissions = defaultdict(list)
        for row in _rows(db, 'SELECT task_id,task_run_id,component_id,kind,state,receipt_id,generation '
                             'FROM llm_request_admissions ORDER BY task_id,task_run_id,component_id'):
            admissions[(row['task_id'], row['task_run_id'])].append(row)
        retries = defaultdict(list)
        for row in _rows(db, 'SELECT j.job_id,j.task_key,j.state,j.generation,j.retry_key,j.task_id,j.group_id,'
                             'j.paper_id,j.receipt_id,j.attempt_id,j.parent_run_id,j.folder_run_id,'
                             'i.old_flow_state_id,i.state AS item_state,i.paper_id AS item_paper_id,'
                             'i.receipt_id AS item_receipt_id,i.attempt_id AS item_attempt_id,'
                             'i.folder_run_id AS item_folder_run_id,i.generation AS item_generation,'
                             'c.request_json AS command_json '
                             'FROM paperflow_llm_stage_jobs j JOIN paperflow_llm_retry_items i '
                             'ON i.request_id=j.retry_key AND i.job_id=j.job_id '
                             'AND i.task_id=j.task_id AND i.group_id=j.group_id '
                             'JOIN paperflow_llm_retry_commands c ON c.request_id=i.request_id '
                             'WHERE j.retry_key IS NOT NULL ORDER BY j.job_id'):
            retries[(row['task_id'], row['group_id'])].append(row)
        related = defaultdict(list)
        for row in _rows(db, 'SELECT task_id,group_id,paper_id,receipt_id,attempt_id,parent_run_id,folder_run_id,'
                             'job_id,task_key,state,generation,retry_key,created_at_ns '
                             'FROM paperflow_llm_stage_jobs ORDER BY job_id'):
            related[(row['task_id'], row['group_id'])].append(row)
        downstream_available = bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                                                "AND name='paperflow_rag_nas_jobs'").fetchone())
        downstream = _unique(_rows(db, 'SELECT * FROM paperflow_rag_nas_jobs'), key_fields) if downstream_available else {}
    with closing(_db(projector.local)) as db:
        db.execute('BEGIN')
        schema = db.execute("SELECT value FROM metadata WHERE key='queue_schema'").fetchone()
        if not schema or schema[0] != 'paperflow-local-waiting/1':
            raise ValueError('local order database is unavailable')
        cancelled = {row['task_id'] for row in _rows(db, 'SELECT task_id FROM cancelled_tasks')}
        orders = _unique(_rows(db, 'SELECT task_id,group_id,receipt_id,attempt_id,prefect_run_id '
                                  'FROM preprocessing_folder_orders'), key_fields)
    keys = set(jobs) | set(claims) | set(results) | set(downstream) | {
        key for key, folder in folders.items() if folder['state'] == 'waiting_postprocessing'}
    snapshot = {}
    for key in keys:
        if key[0] in deleted or key[0] in cancelled:
            continue
        folder = folders.get(key)
        snapshot[key] = dict(task=tasks.get((key[0],)), folder=folder, job=jobs.get(key),
            claim=claims.get(key), result=results.get(key), order=orders.get(key),
            downstream=downstream.get(key),
            admissions=admissions.get((key[0], folder['task_run_id']), []) if folder else [],
            retries=retries.get(key, []), related=related.get(key, []))
    return snapshot, downstream_available


def _downstream_position(row, identity, job, claim, result, task_runs):
    if row is None:
        return 'waiting_rag_upload'
    if (any(row[key] != identity[key] for key in FIELDS) or row['source_generation'] != job['generation']
            or row['claim_id'] != claim['claim_id'] or row['postprocess_job_id'] != job['job_id']
            or row['upstream_manifest_sha256'] != job['manifest_sha256']
            or row['postprocess_receipt_sha256'] != result['stage_receipt_sha256']):
        return None
    runs = {}
    for kind in ('rag', 'nas'):
        run_id = str(uuid5(DOWNSTREAM_NAMESPACE, identity['task_run_id'] + ':' + kind))
        if row[kind + '_task_run_id'] != run_id:
            return None
        run = task_runs.get(run_id)
        if run is not None and (str(run.id) != run_id
                or (run.flow_run_id is not None and str(run.flow_run_id) != identity['task_run_id'])
                or run.task_key != f'paperflow-folder-{kind}-stage-v1-{UUID(run_id).hex}'
                or run.dynamic_key != f'paperflow-{kind}-{run_id}'
                or f'paperflow-{kind}-stage-v1' not in (run.tags or []) or not run.state):
            return None
        runs[kind] = run
    rag, nas = runs['rag'], runs['nas']
    phase, state = row['stage'], row['state']
    if rag and nas and rag.state.is_running() and nas.state.is_running():
        return None
    if (nas and nas.state.is_running() and rag and rag.state.is_completed() and row['rag_receipt_path']
            and phase == 'nas' and state == 'running'):
        return 'nas_in_progress'
    if phase == 'rag' and state in {'waiting', 'paused', 'running'}:
        if nas and not (nas.state.is_scheduled() or nas.state.is_pending() or nas.state.is_paused()):
            return None
        if state in {'waiting', 'paused'} and rag and rag.state.is_running():
            return 'waiting_rag_upload'
        if state == 'running' and rag and rag.state.is_running():
            return 'rag_in_progress'
        if state in {'waiting', 'paused'} and (not rag or rag.state.is_scheduled()
                or rag.state.is_pending() or rag.state.is_paused()):
            return 'waiting_rag_upload'
    if not rag or not rag.state.is_completed() or not row['rag_receipt_path']:
        return None
    if phase == 'rag' and state == 'completed':
        if not nas or nas.state.is_scheduled() or nas.state.is_pending() or nas.state.is_paused():
            return 'waiting_nas_archive_delete'
        return None
    if phase == 'nas' and state in {'waiting', 'paused', 'running'}:
        if state in {'waiting', 'paused'} and nas and nas.state.is_running():
            return 'waiting_nas_archive_delete'
        if state == 'running' and nas and nas.state.is_running():
            return 'nas_in_progress'
        if state in {'waiting', 'paused'} and (not nas or nas.state.is_scheduled()
                or nas.state.is_pending() or nas.state.is_paused()):
            return 'waiting_nas_archive_delete'
    if (phase == 'completed' and state == 'completed' and nas and nas.state.is_completed()
            and row['nas_receipt_path'] and row['cleanup_receipt_path'] and row['completed_at_ns']):
        return 'completed'
    return None


def _uuid(value):
    try:
        return isinstance(value, str) and str(UUID(value)) == value
    except (ValueError, TypeError, AttributeError):
        return False


def _retry_handoff(record, identity, folder_run, task_runs):
    candidates = [r for r in record['retries'] if r['state'] in {'submitted', 'executing', 'finished'}
                  and r['generation'] == record['folder']['generation']]
    if len(candidates) != 1:
        return None
    retry = candidates[0]
    if any(row['job_id'] != retry['job_id'] and row['state'] in {
            'recovering_folder', 'recovery_unknown', 'reserved', 'submitting', 'submitted', 'executing',
            'submission_unknown', 'delivery_unknown', 'execution_unknown', 'identity_unknown'}
            for row in record['related']):
        return None
    try:
        command = json.loads(retry['command_json'])
        expected = str(uuid5(RETRY_NAMESPACE, identity['task_run_id'] + ':' + str(retry['generation']) +
                             ':manual:' + str(UUID(retry['retry_key']))))
        stage = task_runs.get(retry['job_id'])
        valid = (isinstance(command, dict) and command.get('request_id') == retry['retry_key']
            and command.get('scope') in {'one', 'all'}
            and (command['scope'] != 'one' or all(command.get(k) == identity[k]
                for k in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')))
            and (command['scope'] != 'all' or all(command.get(k) is None
                for k in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')))
            and _uuid(retry['retry_key']) and retry['job_id'] == expected
            and retry['item_state'] == 'submitted'
            and retry['old_flow_state_id'] == str(folder_run.state.id)
            and retry['task_key'] == 'paperflow-folder-llm-stage-v1'
            and retry['folder_run_id'] == retry['item_folder_run_id'] == identity['task_run_id']
            and retry['item_generation'] == retry['generation']
            and all(retry.get(k) == identity[k] for k in FIELDS if k != 'task_run_id')
            and all(retry.get('item_' + k) == identity[k] for k in ('paper_id', 'receipt_id', 'attempt_id'))
            and stage is not None and str(stage.id) == retry['job_id']
            and stage.flow_run_id in (None, UUID(identity['task_run_id']))
            and stage.task_key == retry['task_key'] and stage.dynamic_key == 'paperflow-llm-' + retry['job_id']
            and 'paperflow-llm-stage-v1' in (stage.tags or []) and stage.state is not None)
        if not valid:
            return None
        if stage.state.is_running():
            return 'running'
        if stage.state.is_completed() and retry['state'] == 'finished':
            return 'completed'
        return None
    except (ValueError, KeyError, TypeError, AttributeError):
        return None


def _llm_handoff(record, identity, original, task_runs):
    related = record['related']
    if not related:
        return ('completed' if original.state and original.state.is_paused()
                and (original.name or '').endswith('等待后处理') else None)
    try:
        if any(row['state'] in {'recovery_unknown', 'submission_unknown', 'delivery_unknown',
                                'execution_unknown', 'identity_unknown'} for row in related):
            return None
        effective = [row for row in related if row['state'] not in {'stopped_before_submit', 'stopped'}]
        if not effective:
            return None
        latest = max(effective, key=lambda row: (row['generation'], row['created_at_ns']))
        if (type(latest['generation']) is not int or latest['generation'] != record['folder']['generation']
                or latest['folder_run_id'] != identity['task_run_id']
                or any(latest.get(k) != identity[k] for k in FIELDS if k != 'task_run_id')
                or latest['task_key'] != 'paperflow-folder-llm-stage-v1'):
            return None
        suffix = ':manual:' + str(UUID(latest['retry_key'])) if latest['retry_key'] else ''
        if latest['retry_key'] and suffix != ':manual:' + latest['retry_key']:
            return None
        expected = str(uuid5(RETRY_NAMESPACE, identity['task_run_id'] + ':' + str(latest['generation']) + suffix))
        stage = task_runs.get(latest['job_id'])
        if (latest['job_id'] != expected or not stage or str(stage.id) != latest['job_id']
                or stage.flow_run_id not in (None, UUID(identity['task_run_id'])) or stage.task_key != latest['task_key']
                or stage.dynamic_key != 'paperflow-llm-' + latest['job_id']
                or 'paperflow-llm-stage-v1' not in (stage.tags or []) or not stage.state):
            return None
        if latest['retry_key']:
            return _retry_handoff(record, identity, original, task_runs)
        if stage.state.is_running() and latest['state'] in {'submitted', 'executing', 'finished'}:
            return 'running'
        if stage.state.is_completed() and latest['state'] == 'finished':
            return 'completed'
    except (ValueError, KeyError, TypeError, AttributeError):
        return None
    return None


def _position(projector, record, runs, task_runs, root_deployment, folder_deployment):
    folder, task, order = record['folder'], record['task'], record['order']
    if not folder or folder['state'] != 'waiting_postprocessing' or not task or not order:
        return None, None
    try:
        identity = projector.runtime._identity({name: folder[name] for name in FIELDS})
        if (task['prefect_order_model'] != 'v2' or task['prefect_run_id'] != identity['parent_run_id']
                or order['prefect_run_id'] != identity['task_run_id']
                or any(order[k] != identity[k] for k in ('receipt_id', 'attempt_id'))):
            return None, None
        parent, original = runs.get(identity['parent_run_id']), runs.get(identity['task_run_id'])
        if (not parent or parent.deployment_id != root_deployment.id or parent.parent_task_run_id is not None
                or not parent.state or parent.state.is_final() or parent.state.is_cancelling()
                or parent.idempotency_key != 'first-step-order-v2-' + identity['task_id']
                or (parent.parameters or {}).get('task_id') != identity['task_id']
                or not original or original.deployment_id != folder_deployment.id
                or original.parent_task_run_id is not None or not original.state
                or original.state.is_cancelling()
                or original.idempotency_key != 'folder-waiting-v2-' + identity['task_id'] + '-' + identity['group_id']
                or original.work_queue_name != 'task-' + identity['task_id']
                or any((original.parameters or {}).get(k) != identity[k]
                       for k in ('task_id', 'group_id', 'receipt_id', 'attempt_id'))):
            return None, None
        job, claim, result = record['job'], record['claim'], record['result']
        retry_handoff = None
        if original.state.is_failed() or original.state.is_crashed():
            if not job:
                retry_handoff = _retry_handoff(record, identity, original, task_runs)
                if not retry_handoff:
                    return None, None
        elif (not original.state.is_paused()
              or original.state.state_details.pause_reschedule is not True):
            return None, None
        admitted = record['admissions']
        if (not admitted or not any(row['kind'] == 'body' for row in admitted)
                or any(row['kind'] not in {'body', 'attachment'} or row['state'] != 'raw_complete'
                       or row['receipt_id'] != identity['receipt_id'] or type(row['generation']) is not int
                       or not 1 <= row['generation'] <= folder['generation'] for row in admitted)):
            return None, None
        if not job:
            if claim or result:
                return None, None
            llm_handoff = _llm_handoff(record, identity, original, task_runs)
            if llm_handoff == 'running' or retry_handoff == 'running':
                return 'upstream', None
            return ('waiting_postprocessing', None) if llm_handoff == 'completed' else (None, None)
        if (any(job.get(k) != identity[k] for k in FIELDS)
                or job['folder_run_id'] != identity['task_run_id']
                or job['task_key'] != projector.runtime.task_key_for_job(job['job_id'])
                or job['manifest_sha256'] != folder['manifest_sha256']
                or job['source_llm_generation'] != folder['generation']):
            return None, None
        for item in (claim, result):
            if item and (any(item.get(k) != identity[k] for k in FIELDS)
                         or item['manifest_sha256'] != folder['manifest_sha256']):
                return None, None
        stage = task_runs.get(job['job_id'])
        if stage is not None and not projector.runtime._task_identity(stage, job):
            return None, None
        if job['state'] in {'reserved', 'stopped_before_submit'} and not stage and not claim and not result:
            return 'waiting_postprocessing', None
        if job['state'] in {'submitting', 'submitted'} and stage and stage.state and not claim and not result:
            if stage.state.is_scheduled() or stage.state.is_pending() or stage.state.is_running():
                return 'waiting_postprocessing', None
        if job['state'] in {'executing', 'finished'} and stage and stage.state and stage.state.is_running():
            return 'postprocessing_in_progress', None
        if job['state'] == 'finished' and stage and stage.state and stage.state.is_completed() and claim and result:
            if (claim['state'] in {'waiting_rag', 'receipt_held'} and result['state'] == result['code'] == 'passed'
                    and result['claim_id'] == claim['claim_id']):
                if claim['state'] == 'receipt_held':
                    return 'held', 'passed'
                return _downstream_position(record['downstream'], identity, job, claim, result, task_runs), 'passed'
        if job['state'] == 'failed' and stage and stage.state and (stage.state.is_failed() or stage.state.is_crashed()):
            if result and result['state'] in {'failed', 'unrecognized'} and (not claim or claim['state'] in {'failed', 'unknown'}):
                return 'failed', result['state']
        return None, None
    except (ValueError, KeyError, TypeError, AttributeError, projector.runtime.StageConflict):
        return None, None


def observe(projector):
    observed_at = datetime.now(timezone.utc).isoformat()
    before, downstream_available = _snapshot(projector)
    valid_folders, valid_jobs, llm_stages = {}, {}, {}
    for key, record in before.items():
        folder, job = record['folder'], record['job']
        if not folder or not all(_uuid(folder.get(k)) for k in ('parent_run_id', 'task_run_id')):
            continue
        valid_folders[key] = folder
        if job and _uuid(job.get('job_id')):
            valid_jobs[key] = job
        for stage in record['related']:
            if _uuid(stage['job_id']):
                llm_stages[stage['job_id']] = stage
        if record['downstream']:
            for kind in ('rag', 'nas'):
                task_id = record['downstream'][kind + '_task_run_id']
                if _uuid(task_id):
                    llm_stages[task_id] = {'job_id': task_id}
    runs, task_runs, root, folder_deployment, _ = projector._prefect(valid_folders, valid_jobs, llm_stages)
    counts = dict.fromkeys(STAGES, 0)
    buckets = dict.fromkeys(OUTCOMES, 0)
    folder_positions = []
    unresolved = held = upstream = 0
    for key, record in before.items():
        if key not in valid_folders:
            unresolved += 1
            folder_positions.append({'task_id': key[0], 'group_id': key[1], 'stage': 'unresolved'})
            continue
        position, outcome = _position(projector, record, runs, task_runs, root, folder_deployment)
        if position in counts:
            counts[position] += 1
        elif position == 'held':
            held += 1
        elif position == 'upstream':
            upstream += 1
        elif position is None:
            unresolved += 1
        folder_positions.append({'task_id': key[0], 'group_id': key[1], 'stage': position or 'unresolved'})
        if outcome in buckets:
            buckets[outcome] += 1
    return {'available': True, 'basis': BASIS, 'as_of': observed_at,
        'counts': counts, 'partial': bool(unresolved), 'unresolved_count': unresolved, 'held_count': held,
        'upstream_count': upstream, 'result_counts': buckets,
        'downstream_available': downstream_available,
        'folder_positions': sorted(folder_positions,
            key=lambda row: (row['task_id'], row['group_id']))}


def install(app, projector, cache_factory):
    cache = cache_factory(max_inflight=1, max_age_seconds=15)
    epoch = cache.bind_source((BASIS,), 1)

    @app.get('/api/v1/local-intake/postprocessing/positions')
    def positions(response: Response):
        response.headers['Cache-Control'] = 'no-store'
        value = cache.get(('positions',), lambda: observe(projector), expected_epoch=epoch)
        if not isinstance(value, dict) or value.get('available') is not True:
            return {'available': False, 'basis': BASIS}
        return value
