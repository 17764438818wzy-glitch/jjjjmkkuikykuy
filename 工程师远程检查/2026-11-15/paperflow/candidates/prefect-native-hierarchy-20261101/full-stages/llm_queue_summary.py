"""Read folder-grain LLM stages from retained handoffs and admission records."""
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from uuid import UUID, uuid5


def _key(value):
    return tuple(value[k] for k in ('task_id', 'group_id', 'paper_id', 'receipt_id'))


def _stage_id(folder_id, generation, retry_key):
    suffix = '' if not retry_key else ':manual:' + str(UUID(retry_key))
    if retry_key and suffix != ':manual:' + retry_key:
        raise ValueError('人工重试编号无效')
    return str(uuid5(UUID('6aaf059a-c89a-52a8-acd5-4a928260a3b7'),
                     str(UUID(folder_id)) + ':' + str(generation) + suffix))


def _stage_rows(db):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                      "AND name='paperflow_llm_stage_jobs'").fetchone():
        return []
    columns = {row[1] for row in db.execute('PRAGMA table_info(paperflow_llm_stage_jobs)')}
    has_retry_items = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                                 "AND name='paperflow_llm_retry_items'").fetchone()
    retry_key = 'retry_key' if 'retry_key' in columns else "'' AS retry_key"
    if has_retry_items and 'retry_key' in columns:
        fields = ("(SELECT i.old_flow_state_id FROM paperflow_llm_retry_items i "
                  "WHERE i.job_id=paperflow_llm_stage_jobs.job_id AND i.request_id=paperflow_llm_stage_jobs.retry_key) "
                  "AS retry_old_flow_state_id,"
                  "(SELECT i.state FROM paperflow_llm_retry_items i "
                  "WHERE i.job_id=paperflow_llm_stage_jobs.job_id AND i.request_id=paperflow_llm_stage_jobs.retry_key) "
                  "AS retry_item_state")
    else:
        fields = 'NULL AS retry_old_flow_state_id,NULL AS retry_item_state'
    return [dict(row) for row in db.execute(
        'SELECT job_id,task_id,group_id,paper_id,receipt_id,attempt_id,parent_run_id,folder_run_id,'
        'generation,task_key,state,created_at_ns,' + retry_key + ',' + fields + ' FROM paperflow_llm_stage_jobs')]


def summarize_llm(queue, control_database, observer, snapshot):
    """Return only verified counts; an unknown result is None, never zero."""
    observed_at = datetime.now(timezone.utc).isoformat()
    if not isinstance(snapshot, dict) or snapshot.get('receiver_status') != 'running':
        raise ValueError('接收服务当前不可核验')
    waiting = queue.list_waiting_llm()
    if (not isinstance(waiting, dict) or type(waiting.get('unavailable_count')) is not int
            or waiting['unavailable_count'] < 0 or not isinstance(waiting.get('items'), list)):
        raise ValueError('上游 LLM 交接数量不可核验')
    items = waiting['items']
    by_key = {_key(item): item for item in items}
    if len(by_key) != len(items):
        raise ValueError('上游文件夹交接存在重复身份')
    path = Path(control_database).resolve(strict=True)
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=3)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        task_rows = [dict(row) for row in db.execute(
            "SELECT t.task_id,t.prefect_run_id,t.start_generation FROM tasks t WHERE t.prefect_order_model='v2' "
            "AND t.prefect_run_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM task_deletions d "
            "WHERE d.task_id=t.task_id)")]
        tasks = {row['task_id']: row['prefect_run_id'] for row in task_rows}
        generations = {row['task_id']: row['start_generation'] for row in task_rows}
        rows = [dict(row) for row in db.execute(
            "SELECT q.* FROM llm_folder_queue q JOIN tasks t ON t.task_id=q.task_id "
            "WHERE t.prefect_order_model='v2' AND t.prefect_run_id=q.parent_run_id "
            "AND NOT EXISTS(SELECT 1 FROM task_deletions d WHERE d.task_id=q.task_id)")]
        admissions = [dict(row) for row in db.execute(
            "SELECT a.* FROM llm_request_admissions a JOIN llm_folder_queue q "
            "ON q.task_id=a.task_id AND q.task_run_id=a.task_run_id "
            "JOIN tasks t ON t.task_id=q.task_id AND t.prefect_run_id=q.parent_run_id "
            "WHERE t.prefect_order_model='v2' AND NOT EXISTS(SELECT 1 FROM task_deletions d "
            "WHERE d.task_id=a.task_id) AND a.state IN ('admitted','returned_unverified','unknown')")]
        stages = _stage_rows(db)
        stages = [stage for stage in stages if tasks.get(stage['task_id']) == stage['parent_run_id']]
    phases = {}
    partial = (snapshot.get('waiting_available') is not True or waiting['unavailable_count'] > 0)
    if any(item.get('task_id') not in tasks for item in items):
        partial = True
        items = [item for item in items if item.get('task_id') in tasks]
        by_key = {_key(item): item for item in items}
    for row in rows:
        key = _key(row)
        if key in phases or row['state'] not in {'waiting_llm','llm_processing','waiting_postprocessing'}:
            raise ValueError('文件夹 LLM 队列状态不一致')
        if key not in by_key:
            partial = True
            continue
        phases[key] = row
    queued_groups = {(stage['task_id'], stage['group_id']) for stage in stages
                     if stage['state'] in {'reserved', 'submitting', 'submitted', 'recovering_folder',
                                           'stopped', 'stopped_before_submit'}}
    pending = [item for item in items if _key(item) not in phases or
               phases[_key(item)]['state'] == 'waiting_llm' or
               (phases[_key(item)]['state'] == 'llm_processing' and
                (item['task_id'], item['group_id']) in queued_groups)]
    with closing(queue._connect()) as db:
        orders = {(row['task_id'], row['group_id']): dict(row) for row in db.execute(
            'SELECT task_id,group_id,receipt_id,attempt_id,prefect_run_id FROM preprocessing_folder_orders')}
    pending_by_run = {}
    for item in pending:
        key = (item['task_id'], item['group_id'])
        order = orders.get(key)
        if (not order or order['receipt_id'] != item['receipt_id']
                or not order['prefect_run_id'] or not order['attempt_id']):
            partial = True
            continue
        pending_by_run[order['prefect_run_id']] = (item, order)
    verified_pending = []
    post = sum(row['state'] == 'waiting_postprocessing' for row in rows)
    running, unknown = {'body':set(), 'attachment':set()}, {'body':False, 'attachment':False}
    unconfirmed = {'body': set(), 'attachment': set()}
    processing_folders = set()
    position_unknown = False
    row_by_run = {(row['task_id'],row['task_run_id']): row for row in phases.values()}
    for admission in admissions:
        kind = admission.get('kind')
        row = row_by_run.get((admission.get('task_id'),admission.get('task_run_id')))
        if kind not in running:
            raise ValueError('文件夹模型请求状态不一致')
        if not row:
            partial = True
            unknown[kind] = True
            continue
        if row['state'] != 'llm_processing' or admission.get('receipt_id') != row['receipt_id']:
            raise ValueError('文件夹模型请求状态不一致')
        # A request's outcome may be unknown even when its Prefect stage has
        # already failed. Resolve the stage before deciding whether it is active.
        if admission['state'] == 'unknown':
            unconfirmed[kind].add(row['task_run_id'])
        else:
            running[kind].add(row['task_run_id'])

    stage_by_folder = {}
    for stage in stages:
        key = (stage['task_id'], stage['folder_run_id'])
        generation = stage.get('generation')
        if type(generation) is not int or generation > generations.get(stage['task_id'], -1):
            position_unknown = True
            continue
        prior = stage_by_folder.get(key)
        if prior is None or (generation, stage['created_at_ns']) > (prior['generation'], prior['created_at_ns']):
            stage_by_folder[key] = stage
    # A running stage must be visible even before its first model admission.
    active_ids = {row['task_run_id'] for row in phases.values() if row['state'] == 'llm_processing'}
    active_ids.update(stage['folder_run_id'] for stage in stages
                      if (row := row_by_run.get((stage['task_id'], stage['folder_run_id'])))
                      and (row['state'] != 'waiting_postprocessing' or stage['state'] in {
                          'reserved', 'submitting', 'submitted', 'executing', 'recovery_unknown',
                          'submission_unknown', 'delivery_unknown', 'execution_unknown', 'identity_unknown'}))
    active_ids.update(set().union(*running.values()))
    if active_ids or pending_by_run:
        try:
            from prefect.client.schemas.filters import (FlowRunFilter, FlowRunFilterId,
                                                        TaskRunFilter, TaskRunFilterId)
            with observer._client() as client:
                deployment = observer._safe_deployment(client)
                root_deployment = client.read_deployment_by_name('论文第一步-任务工单/手动运行')
                if 'paperflow-order-v2-live-20261031' not in (root_deployment.tags or []):
                    raise ValueError('大工单部署不可核验')
                actual = {}
                related_tasks = {row['task_id'] for row in phases.values() if row['task_run_id'] in active_ids}
                related_tasks.update(item['task_id'] for item, _ in pending_by_run.values())
                ids = [UUID(value) for value in (active_ids | set(pending_by_run) |
                       {tasks[task_id] for task_id in related_tasks})]
                for pos in range(0, len(ids), 100):
                    chunk = ids[pos:pos + 100]
                    actual.update({str(run.id): run for run in client.read_flow_runs(
                        flow_run_filter=FlowRunFilter(id=FlowRunFilterId(any_=chunk)), limit=len(chunk))})
                relevant = [stage for stage in stages if stage['folder_run_id'] in active_ids | set(pending_by_run)]
                observed_stages = {}
                stage_ids = [UUID(stage['job_id']) for stage in relevant]
                for pos in range(0, len(stage_ids), 100):
                    chunk = stage_ids[pos:pos + 100]
                    observed_stages.update({str(run.id): run for run in client.read_task_runs(
                        task_run_filter=TaskRunFilter(id=TaskRunFilterId(any_=chunk)), limit=len(chunk))})
                def parent_ok(task_id):
                    parent = actual.get(tasks[task_id])
                    return bool(parent and parent.deployment_id == root_deployment.id
                        and parent.parent_task_run_id is None and parent.state
                        and not parent.state.is_final() and not parent.state.is_cancelling()
                        and parent.idempotency_key == 'first-step-order-v2-' + task_id
                        and (parent.parameters or {}).get('task_id') == task_id)
                for value, (item, order) in pending_by_run.items():
                    run = actual.get(value)
                    row = row_by_run.get((item['task_id'], value))
                    stage = stage_by_folder.get((item['task_id'], value)) if row else None
                    actual_stage = observed_stages.get(stage['job_id']) if stage else None
                    retry_waiting = bool(stage and stage['retry_key'] and stage['retry_item_state'] == 'submitted'
                        and (run and run.state and (run.state.is_failed() or run.state.is_crashed()))
                        and run.state.id and stage['retry_old_flow_state_id'] == str(run.state.id))
                    if (not parent_ok(item['task_id']) or not run or run.deployment_id != deployment.id or run.parent_task_run_id is not None
                            or not run.state or (not retry_waiting and (not run.state.is_paused()
                                or run.state.state_details.pause_reschedule is not True
                                or not any((run.name or '').endswith(phase) for phase in ('等待 LLM','等待任务开始'))))
                            or run.idempotency_key != 'folder-waiting-v2-' + item['task_id'] + '-' + item['group_id']
                            or run.work_queue_name != 'task-' + item['task_id']
                            or any((run.parameters or {}).get(k) != order[k]
                                   for k in ('task_id','group_id','receipt_id','attempt_id'))):
                        partial = True
                        continue
                    if stage:
                        if (not row or stage['task_key'] != 'paperflow-folder-llm-stage-v1'
                                or stage['job_id'] != _stage_id(value, stage['generation'], stage['retry_key'])
                                or any(stage[k] != row[k] for k in
                                       ('task_id','group_id','paper_id','receipt_id','attempt_id','parent_run_id'))):
                            partial = True
                            continue
                        if stage['retry_key'] and stage['state'] == 'stopped_before_submit' and not actual_stage:
                            continue
                        if (stage['retry_key'] and stage['state'] == 'stopped'
                                and (not actual_stage or (actual_stage.state and actual_stage.state.is_completed()))):
                            continue
                        if actual_stage:
                            if (actual_stage.flow_run_id not in (None, UUID(value))
                                    or actual_stage.task_key != stage['task_key']
                                    or actual_stage.dynamic_key != 'paperflow-llm-' + stage['job_id']
                                    or 'paperflow-llm-stage-v1' not in (actual_stage.tags or [])
                                    or not actual_stage.state):
                                partial = True
                                continue
                            if actual_stage.state.is_running():
                                continue
                            if not (actual_stage.state.is_pending() or actual_stage.state.is_scheduled()
                                    or stage['state'] in {'stopped', 'stopped_before_submit'}):
                                continue
                        elif stage['state'] not in {'reserved', 'stopped', 'stopped_before_submit'}:
                            partial = True
                            continue
                    verified_pending.append(item)
                for value in active_ids:
                    run = actual.get(value)
                    row = next((item for item in rows if item['task_run_id'] == value), None)
                    if (not row or not parent_ok(row['task_id']) or not run or run.deployment_id != deployment.id
                            or run.parent_task_run_id is not None or not run.state
                            or run.idempotency_key != 'folder-waiting-v2-' + row['task_id'] + '-' + row['group_id']
                            or run.work_queue_name != 'task-' + row['task_id']
                            or any((run.parameters or {}).get(k) != row[k]
                                   for k in ('task_id','group_id','receipt_id','attempt_id'))):
                        position_unknown = True
                        for kind, values in running.items():
                            if value in values: unknown[kind] = True; values.discard(value)
                        continue
                    if run.state.is_running():
                        processing_folders.add(value)
                        for kind in unknown:
                            if value in unconfirmed[kind]: unknown[kind] = True
                        continue
                    stage = stage_by_folder.get((row['task_id'], value))
                    actual_stage = observed_stages.get(stage['job_id']) if stage else None
                    terminal_retry = bool(stage and stage['retry_key'] and stage['retry_item_state'] == 'submitted'
                        and (run.state.is_failed() or run.state.is_crashed()) and run.state.id
                        and stage['retry_old_flow_state_id'] == str(run.state.id))
                    if (stage and stage['retry_key'] and stage['state'] == 'stopped_before_submit' and not actual_stage
                            and stage['task_key'] == 'paperflow-folder-llm-stage-v1'
                            and stage['job_id'] == _stage_id(value, stage['generation'], stage['retry_key'])
                            and all(stage[k] == row[k] for k in
                                    ('task_id','group_id','paper_id','receipt_id','attempt_id','parent_run_id'))):
                        for kind, values in running.items():
                            if value in values:
                                unknown[kind] = True
                                values.discard(value)
                        continue
                    if (stage and stage['state'] in {'reserved', 'stopped', 'stopped_before_submit'} and not actual_stage
                            and any((item['task_id'], item['group_id']) == (row['task_id'], row['group_id'])
                                    for item in verified_pending)):
                        continue
                    valid_stage = (stage and (terminal_retry or (run.state.is_paused()
                            and run.state.state_details.pause_reschedule is True))
                        and stage['task_key'] == 'paperflow-folder-llm-stage-v1'
                        and stage['job_id'] == _stage_id(value, stage['generation'], stage['retry_key'])
                        and all(stage[k] == row[k] for k in
                                ('task_id','group_id','paper_id','receipt_id','attempt_id','parent_run_id'))
                        and actual_stage and actual_stage.flow_run_id in (None, UUID(value))
                        and actual_stage.task_key == stage['task_key']
                        and actual_stage.dynamic_key == 'paperflow-llm-' + stage['job_id']
                        and 'paperflow-llm-stage-v1' in (actual_stage.tags or []) and actual_stage.state)
                    if valid_stage and actual_stage.state.is_running():
                        processing_folders.add(value)
                        for kind in unknown:
                            if value in unconfirmed[kind]: unknown[kind] = True
                        verified_pending = [item for item in verified_pending
                                            if (item['task_id'], item['group_id']) != (row['task_id'], row['group_id'])]
                        continue
                    terminal_failure = bool(valid_stage and stage['state'] == 'failed'
                        and (actual_stage.state.is_failed() or actual_stage.state.is_crashed()))
                    terminal_stopped_retry = bool(valid_stage and stage['retry_key'] and stage['state'] == 'stopped'
                        and actual_stage.state.is_completed())
                    waiting_stage = bool(valid_stage and stage['state'] in {'submitting','submitted'}
                        and (actual_stage.state.is_pending() or actual_stage.state.is_scheduled()))
                    for kind, values in running.items():
                        if value in values:
                            if not terminal_failure and not terminal_stopped_retry and not waiting_stage: unknown[kind] = True
                            values.discard(value)
                    if not terminal_failure and not terminal_stopped_retry and not waiting_stage:
                        position_unknown = True
        except Exception:
            if pending_by_run:
                partial = True
                verified_pending.clear()
            position_unknown = True
            for kind, values in running.items():
                if values or unconfirmed[kind]:
                    unknown[kind] = True
                    values.clear()
    # Re-read the local ledger after observing Prefect. A folder that changed
    # during the cross-service snapshot cannot contribute to a claimed lower
    # bound from the earlier snapshot.
    try:
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=3)) as db:
            db.row_factory = sqlite3.Row
            db.execute('BEGIN')
            latest_task_rows = [dict(row) for row in db.execute(
                "SELECT t.task_id,t.prefect_run_id,t.start_generation FROM tasks t WHERE t.prefect_order_model='v2' "
                "AND t.prefect_run_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM task_deletions d "
                "WHERE d.task_id=t.task_id)")]
            latest_tasks = {row['task_id']: row['prefect_run_id'] for row in latest_task_rows}
            latest_generations = {row['task_id']: row['start_generation'] for row in latest_task_rows}
            latest_rows = [dict(row) for row in db.execute(
                "SELECT q.* FROM llm_folder_queue q JOIN tasks t ON t.task_id=q.task_id "
                "WHERE t.prefect_order_model='v2' AND t.prefect_run_id=q.parent_run_id "
                "AND NOT EXISTS(SELECT 1 FROM task_deletions d WHERE d.task_id=q.task_id)")]
            latest_admissions = [dict(row) for row in db.execute(
                "SELECT a.* FROM llm_request_admissions a JOIN llm_folder_queue q "
                "ON q.task_id=a.task_id AND q.task_run_id=a.task_run_id "
                "JOIN tasks t ON t.task_id=q.task_id AND t.prefect_run_id=q.parent_run_id "
                "WHERE t.prefect_order_model='v2' AND NOT EXISTS(SELECT 1 FROM task_deletions d "
                "WHERE d.task_id=a.task_id) AND a.state IN ('admitted','returned_unverified','unknown')")]
            latest_stages = _stage_rows(db)
            latest_stages = [s for s in latest_stages if latest_tasks.get(s['task_id']) == s['parent_run_id']]
        old_rows = {(r['task_id'],r['group_id']): r for r in rows}
        new_rows = {(r['task_id'],r['group_id']): r for r in latest_rows}
        changed_groups = {key for key in set(old_rows) | set(new_rows) if old_rows.get(key) != new_rows.get(key)}
        changed_tasks = {key for key in set(tasks) | set(latest_tasks)
                         if tasks.get(key) != latest_tasks.get(key)
                         or generations.get(key) != latest_generations.get(key)}
        changed_groups.update((item['task_id'],item['group_id']) for item in items
                              if item['task_id'] in changed_tasks)
        old_jobs = {s['job_id']: s for s in stages}
        new_jobs = {s['job_id']: s for s in latest_stages}
        for job_id in set(old_jobs) | set(new_jobs):
            if old_jobs.get(job_id) != new_jobs.get(job_id):
                for stage in (old_jobs.get(job_id),new_jobs.get(job_id)):
                    if stage:
                        changed_groups.add((stage['task_id'],stage['group_id']))
        old_admissions = {(a['task_id'],a['component_id']): a for a in admissions}
        new_admissions = {(a['task_id'],a['component_id']): a for a in latest_admissions}
        run_groups = {(r['task_id'],r['task_run_id']):(r['task_id'],r['group_id'])
                      for r in rows + latest_rows}
        for component in set(old_admissions) | set(new_admissions):
            if old_admissions.get(component) != new_admissions.get(component):
                for admission in (old_admissions.get(component),new_admissions.get(component)):
                    if admission:
                        group = run_groups.get((admission['task_id'],admission['task_run_id']))
                        if group:
                            changed_groups.add(group)
        latest_waiting = queue.list_waiting_llm()
        if (not isinstance(latest_waiting, dict) or not isinstance(latest_waiting.get('items'), list)
                or type(latest_waiting.get('unavailable_count')) is not int):
            raise ValueError('上游 LLM 交接复核不可用')
        latest_items = {_key(item): item for item in latest_waiting['items']}
        if set(latest_items) != set(by_key):
            partial = True
        with closing(queue._connect()) as local:
            latest_orders = {(row['task_id'], row['group_id']): dict(row) for row in local.execute(
                'SELECT task_id,group_id,receipt_id,attempt_id,prefect_run_id FROM preprocessing_folder_orders')}
        for item in items:
            group = (item['task_id'],item['group_id'])
            if latest_items.get(_key(item)) != item or latest_orders.get(group) != orders.get(group):
                changed_groups.add(group)
        if latest_waiting['unavailable_count'] != waiting['unavailable_count']:
            partial = True
        if changed_groups:
            partial = True
            verified_pending = [item for item in verified_pending
                                if (item['task_id'],item['group_id']) not in changed_groups]
            for kind, values in running.items():
                for run_id in list(values):
                    row = next(item for item in rows if item['task_run_id'] == run_id)
                    if (row['task_id'],row['group_id']) in changed_groups:
                        values.discard(run_id)
                        unknown[kind] = True
            for run_id in list(processing_folders):
                row = next(item for item in rows if item['task_run_id'] == run_id)
                if (row['task_id'], row['group_id']) in changed_groups:
                    processing_folders.discard(run_id)
                    position_unknown = True
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError):
        partial = True
        verified_pending.clear()
        unknown = {'body': True, 'attachment': True}
        position_unknown = True
        processing_folders.clear()
        for values in running.values():
            values.clear()
    body_partial = partial or position_unknown or unknown['body']
    attachment_partial = partial or position_unknown or unknown['attachment']
    position_partial = partial or position_unknown
    folder_positions = [{'task_id': item['task_id'], 'group_id': item['group_id'], 'stage': 'waiting_llm'}
                        for item in verified_pending]
    for run_id in sorted(processing_folders):
        row = next(item for item in rows if item['task_run_id'] == run_id)
        folder_positions.append({'task_id': row['task_id'], 'group_id': row['group_id'],
            'stage': 'llm_processing',
            'body': True if run_id in running['body'] else (None if run_id in unconfirmed['body'] else False),
            'attachment': True if run_id in running['attachment'] else (None if run_id in unconfirmed['attachment'] else False)})
    return {'waiting_llm': len(verified_pending) if not position_partial or verified_pending else None,
            'llm_processing_folders': len(processing_folders) if not position_partial or processing_folders else None,
            'llm_processing_folders_partial': position_partial,
            'llm_folder_positions': folder_positions,
            'llm_body_in_progress': len(running['body']) if not body_partial or running['body'] else None,
            'llm_attachment_in_progress': len(running['attachment']) if not attachment_partial or running['attachment'] else None,
            'llm_body_in_progress_partial': body_partial,
            'llm_attachment_in_progress_partial': attachment_partial,
            'waiting_postprocessing': None if partial else post,
            'waiting_llm_partial': position_partial, 'waiting_items': verified_pending,
            'unavailable_count': waiting['unavailable_count'],
            'as_of': observed_at}
