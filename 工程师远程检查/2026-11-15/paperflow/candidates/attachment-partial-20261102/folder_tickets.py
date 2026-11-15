"""One read-only view of each existing Prefect folder order."""
from collections import defaultdict
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import sqlite3

from fastapi import Response


def observe(projector, positions=None):
    observed_at = datetime.now(timezone.utc).isoformat()
    with closing(sqlite3.connect(projector.local.as_uri() + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        local = [dict(row) for row in db.execute('''SELECT g.task_id,g.group_id,g.paper_id,g.receipt_id,
            g.state AS group_state,g.review_reason,o.prefect_run_id,o.folder_label,o.attempt_id,
            c.state AS claim_state,f.error_type,h.state AS handoff_state
            FROM groups g LEFT JOIN preprocessing_folder_orders o
              ON o.task_id=g.task_id AND o.group_id=g.group_id AND o.receipt_id=g.receipt_id
            LEFT JOIN claims c ON c.task_id=g.task_id AND c.group_id=g.group_id
              AND c.receipt_id=g.receipt_id AND c.attempt_id=o.attempt_id
              AND c.consumer_id='prefect-local-preprocessing'
            LEFT JOIN preprocessing_failures f ON f.attempt_id=o.attempt_id
              AND f.consumer_id='prefect-local-preprocessing'
            LEFT JOIN preprocessing_handoffs h ON h.task_id=g.task_id AND h.group_id=g.group_id
              AND h.receipt_id=g.receipt_id AND h.attempt_id=o.attempt_id
            WHERE g.state!='cancelled' AND NOT EXISTS
              (SELECT 1 FROM cancelled_tasks c WHERE c.task_id=g.task_id)
            ORDER BY g.task_id,g.group_id''')]
    with closing(sqlite3.connect(projector.control.as_uri() + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        tasks = {row['task_id']: dict(row) for row in db.execute('''SELECT task_id,name,prefect_run_id,prefect_order_model
            FROM tasks t WHERE NOT EXISTS(SELECT 1 FROM task_deletions d WHERE d.task_id=t.task_id)''')}
        llm = {(row['task_id'], row['group_id']): dict(row) for row in db.execute('SELECT * FROM llm_folder_queue')}
        post = {(row['task_id'], row['group_id']): dict(row) for row in db.execute('SELECT * FROM paperflow_postprocess_stage_jobs')}
        outcomes = {(row['task_id'], row['group_id']): dict(row) for row in db.execute('SELECT * FROM postprocessing_results')}
        failures = {(row['task_id'], row['group_id']): json.loads(row['receipt_json']).get('failed_attachments', [])
                    for row in db.execute('''SELECT r.task_id,r.group_id,p.receipt_json FROM postprocessing_results r
                        JOIN postprocessing_receipts p ON p.claim_id=r.claim_id
                        AND p.receipt_sha256=r.stage_receipt_sha256 WHERE r.state='passed' ''')}
        downstream = {(row['task_id'], row['group_id']): dict(row) for row in db.execute('SELECT * FROM paperflow_rag_nas_jobs')}
        llm_jobs = {}
        for row in db.execute('SELECT * FROM paperflow_llm_stage_jobs ORDER BY generation,created_at_ns'):
            llm_jobs[(row['task_id'], row['group_id'])] = dict(row)
        admissions = defaultdict(list)
        for row in db.execute('SELECT task_id,task_run_id,kind,state,response_sha256 FROM llm_request_admissions'):
            admissions[(row['task_id'], row['task_run_id'])].append(dict(row))

    records = positions._snapshot(projector)[0] if positions else {}
    folders, jobs, stages = {}, {}, {}
    complete = True
    for row in local:
        task = tasks.get(row['task_id'])
        if not task or task['prefect_order_model'] != 'v2' or not task['prefect_run_id'] or not row['prefect_run_id']:
            complete = False
            continue
        key = row['task_id'], row['group_id']
        folders[key] = {'parent_run_id': task['prefect_run_id'], 'task_run_id': row['prefect_run_id']}
        if key in post: jobs[key] = post[key]
        if key in llm_jobs: stages[llm_jobs[key]['job_id']] = llm_jobs[key]
        if key in downstream:
            for name in ('rag_task_run_id', 'nas_task_run_id'):
                ident = downstream[key][name]
                stages[ident] = {'job_id': ident}
        for stage in records.get(key, {}).get('related', []):
            stages[stage['job_id']] = stage
    runs, task_runs, root_deployment, folder_deployment, _ = projector._prefect(folders, jobs, stages)
    task_runs = {key: run for key, run in task_runs.items() if run.state}
    projector.observer._add_result_titles(local)
    result = []
    for row in local:
        key = row['task_id'], row['group_id']
        if key not in folders:
            continue
        task, order_id = tasks[row['task_id']], row['prefect_run_id']
        folder = runs.get(order_id)
        position, failed_stage = 'unresolved', None
        processed = (row['group_state'] == 'ready_for_llm' and row['claim_state'] == 'completed'
                     and row['handoff_state'] == 'waiting')
        preprocessing_result = 'passed' if processed else ('failed' if row['claim_state'] == 'failed' else None)
        admission = admissions[(row['task_id'], order_id)]
        raw_complete = bool(admission) and any(a['kind'] == 'body' for a in admission) and all(
            a['state'] == 'raw_complete' for a in admission)
        raw_complete = llm.get(key, {}).get('state') == 'waiting_postprocessing' and raw_complete
        empty_body = raw_complete and any(a['kind'] == 'body' and a['response_sha256'] == hashlib.sha256(b'').hexdigest()
                                          for a in admission)
        outcome = outcomes.get(key, {})
        post_result = outcome.get('state') if outcome.get('state') in {'passed', 'failed', 'unrecognized'} else None
        llm_result = 'unrecognized' if empty_body else 'passed' if raw_complete and post_result == 'passed' else None
        body_running = attachment_running = False
        if runs.get(task['prefect_run_id']) and folder and folder.state and (folder.parameters or {}).get('task_id') == row['task_id'] and (
                folder.parameters or {}).get('group_id') == row['group_id']:
            job, next_job = post.get(key), downstream.get(key)
            if next_job:
                phase, state = next_job['stage'], next_job['state']
                current = task_runs.get(next_job['nas_task_run_id'] if phase in {'nas', 'completed'}
                                        else next_job['rag_task_run_id'])
                if phase == 'completed' and state == 'completed' and current and current.state.is_completed():
                    position = 'completed'
                elif phase in {'rag', 'nas'} and state in {'waiting', 'paused'}:
                    position = 'waiting_rag_upload' if phase == 'rag' else 'waiting_nas_archive_delete'
                elif phase in {'rag', 'nas'} and state == 'running' and current and current.state.is_running():
                    position = 'rag_in_progress' if phase == 'rag' else 'nas_in_progress'
            elif job:
                current = task_runs.get(job['job_id'])
                if post_result in {'failed', 'unrecognized'}:
                    position, failed_stage = 'failed', 'postprocessing'
                elif job['state'] == 'finished' and post_result == 'passed' and current and current.state.is_completed():
                    position = 'waiting_rag_upload'
                elif job['state'] == 'finished' and post_result == 'passed' and current and current.state.is_running():
                    position = 'postprocessing_in_progress'
                elif job['state'] == 'executing' and current and current.state.is_running():
                    position = 'postprocessing_in_progress'
                elif job['state'] in {'reserved', 'stopped_before_submit'} or (
                        job['state'] in {'submitting', 'submitted'} and current
                        and (current.state.is_scheduled() or current.state.is_pending() or current.state.is_running())):
                    position = 'waiting_postprocessing'
            elif raw_complete:
                stage = llm_jobs.get(key)
                current = task_runs.get(stage['job_id']) if stage else None
                if stage and current and current.state.is_running() and stage['state'] in {'submitted', 'executing', 'finished'}:
                    position = 'llm_in_progress'
                elif stage and current and current.state.is_completed() and stage['state'] == 'finished':
                    position = 'waiting_postprocessing'
                elif not stage and folder.state.is_paused() and (folder.name or '').endswith('等待后处理'):
                    position = 'waiting_postprocessing'
            elif processed:
                stage = llm_jobs.get(key)
                observed = task_runs.get(stage['job_id']) if stage else None
                if stage and stage['state'] == 'failed' and observed and observed.state.is_failed():
                    position, failed_stage = 'failed', 'llm'
                elif stage and stage['state'] == 'executing' and observed and observed.state.is_running():
                    position = 'llm_in_progress'
                elif folder.state.is_running() and llm.get(key, {}).get('state') == 'llm_processing' and not stage:
                    position = 'llm_in_progress'
                elif llm.get(key, {}).get('state') != 'llm_processing' or stage and stage['state'] in {
                        'reserved', 'submitting', 'submitted', 'stopped', 'stopped_before_submit'}:
                    position = 'waiting_llm'
                if position == 'llm_in_progress':
                    body_running = any(a['kind'] == 'body' and a['state'] in {'admitted', 'returned_unverified'} for a in admission)
                    attachment_running = any(a['kind'] == 'attachment' and a['state'] in {'admitted', 'returned_unverified'} for a in admission)
            elif row['claim_state'] == 'failed':
                position, failed_stage = 'failed', 'preprocessing'
            elif row['group_state'] == 'processing' and folder.state.is_running():
                position = 'preprocessing_in_progress'
            elif row['group_state'] == 'waiting':
                position = 'waiting_preprocessing'
            if positions and (job or next_job or raw_complete):
                record = records.get(key)
                if (not record or any((record.get(name) or {}).get('state') != (value or {}).get('state')
                        for name, value in (('job', job), ('result', outcomes.get(key)), ('downstream', next_job)))):
                    position, post_result = 'unresolved', None
                else:
                    verified_position, verified_result = positions._position(
                        projector, record, runs, task_runs, root_deployment, folder_deployment)
                    position = ('llm_in_progress' if verified_position == 'upstream' else verified_position
                                if verified_position in {'waiting_postprocessing', 'postprocessing_in_progress',
                                    'waiting_rag_upload', 'rag_in_progress', 'waiting_nas_archive_delete',
                                    'nas_in_progress', 'completed', 'failed'} else 'unresolved')
                    post_result = verified_result
                    if position == 'failed': failed_stage = 'postprocessing'
                llm_result = 'unrecognized' if empty_body else 'passed' if raw_complete and post_result == 'passed' else None
        if position == 'unresolved':
            complete = False
        attachment_failures = failures.get(key, []) if post_result == 'passed' else []
        display_post_result = 'failed' if attachment_failures else post_result
        if attachment_failures:
            detail = '正文已通过；附件未通过，待后续处理：' + '；'.join(
                f"{item.get('source_name') or item['attachment_id']}（{item['attachment_id']}）：{item['message']}"
                for item in attachment_failures)
        else:
            detail = (outcome.get('code') if post_result != 'passed' else
                      '已登记通过；缺口与警告以交接封包为准，页面未展开详情。')
        result.append(dict(ticket_id=order_id, task_id=row['task_id'], folder_id=row['group_id'],
            folder_name=row['folder_label'], paper_title=row.get('paper_title'),
            paper_title_candidate=row.get('paper_title_candidate'), task_name=task['name'],
            stage=position, failed_stage=failed_stage, llm_body_running=body_running,
            llm_attachment_running=attachment_running, llm_response_saved=bool(raw_complete and not empty_body),
            preprocessing_result=preprocessing_result,
            llm_result=llm_result, postprocessing_result=display_post_result,
            attachment_failures=attachment_failures,
            preprocessing_detail=row.get('error_type') or row.get('review_reason'),
            llm_detail=('正文原始回答为空；附件记录保留，需核对。' if empty_body else
                        '原始回答已保存；画像校验在后处理进行。' if raw_complete else None),
            postprocessing_detail=detail))
    return {'as_of': observed_at, 'complete': complete, 'folders': result}


def install(app, projector, positions):
    @app.get('/api/v1/prefect/folder-tickets')
    def folder_tickets(response: Response):
        response.headers['Cache-Control'] = 'no-store'
        return observe(projector, positions)
