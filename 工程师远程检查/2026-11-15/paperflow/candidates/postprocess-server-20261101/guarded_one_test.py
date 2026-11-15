"""Run exactly one previously checked original ticket and re-close its test gate.

This is an explicit operator entry for the migration test. It does not scan,
retry, send LLM requests, or invoke RAG. The existing queue barriers and task
controls remain authoritative. A lost execution outcome must not be retried.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    if len(sys.argv) != 8:
        raise ValueError('expected pinned config, evidence and output paths')
    config_path, config_sha, evidence_path, evidence_sha, progress_path, log_path, workspace = sys.argv[1:]
    workspace, progress_path, log_path = map(Path, (workspace, progress_path, log_path))
    if sha(config_path) != config_sha or sha(evidence_path) != evidence_sha:
        raise ValueError('configured test evidence changed')
    config = json.loads(Path(config_path).read_text(encoding='utf-8'))
    evidence = json.loads(Path(evidence_path).read_text(encoding='utf-8'))
    identity = evidence['identity']
    if evidence['schema'] != 'paperflow-selected-ticket-evidence/1' or evidence['claim_created']:
        raise ValueError('one unclaimed selected ticket is required')
    if progress_path.exists() or log_path.exists():
        raise ValueError('test output already exists')
    python = Path(sys.executable)
    candidate = workspace / 'candidates/postprocess-server-20261101'
    sys.path.insert(0, str(workspace / 'candidates/postprocess-prefect-20261031'))
    from stage_gates import QueueBarriers
    check = subprocess.run([str(python), '-B', str(candidate / 'selected_ticket_probe.py'), 'verify',
                            '--evidence', evidence_path, '--evidence-sha256', evidence_sha],
                           capture_output=True, timeout=90, check=False)
    if check.returncode or not json.loads(check.stdout).get('same_original_identity'):
        raise ValueError('selected ticket is no longer eligible')
    import requests
    response = requests.get(config['expected_prefect_api_url'].rstrip('/') + '/flow_runs/'
                            + identity['task_run_id'], timeout=10)
    response.raise_for_status()
    folder = response.json()
    state = folder.get('state') or {}
    if (folder.get('id') != identity['task_run_id'] or state.get('type') != 'PAUSED'
            or not folder.get('name', '').endswith('等待后处理')
            or (state.get('state_details') or {}).get('pause_reschedule') is not True):
        raise ValueError('the original Prefect folder is not waiting for postprocessing')
    settings = json.loads((workspace / 'candidates/first-step-control-20261029/runtime/workspace-config.json')
                          .read_text(encoding='utf-8'))
    database = Path(settings['classification_root']) / 'group-task-summaries/summary-index.sqlite3'
    with sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=5) as db:
        parent = db.execute('SELECT desired_state,start_pending,task_status,start_generation FROM tasks '
                            'WHERE task_id=?', (identity['task_id'],)).fetchone()
        deleted = db.execute('SELECT 1 FROM task_deletions WHERE task_id=?', (identity['task_id'],)).fetchone()
        occupied = any(db.execute('SELECT 1 FROM ' + table + ' WHERE task_id=? AND group_id=? LIMIT 1',
                                 (identity['task_id'], identity['group_id'])).fetchone()
                       for table in ('postprocessing_claims', 'postprocessing_results',
                                     'paperflow_postprocess_stage_jobs'))
    if (not parent or parent[0] != 'running' or parent[1] != 0
            or parent[2] in {'deleting', 'delete_failed', 'deleted', 'unknown'}
            or parent[3] != evidence['generation'] or deleted or occupied):
        raise ValueError('the original parent or selected ticket changed')
    barriers = QueueBarriers(database)
    if barriers._read() != {'rag_upload': 1, 'postprocessing': 1}:
        raise ValueError('the original test barriers are not held')

    def progress(value):
        temp = progress_path.with_suffix('.tmp')
        with temp.open('x', encoding='utf-8') as stream:
            json.dump(value, stream, ensure_ascii=False, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, progress_path)

    result = {'state': 'starting', 'exit_code': None, 'gate_reclosed': False}
    progress(result)
    opened = False
    try:
        with log_path.open('xb') as log:
            barriers.open_postprocessing()
            opened = True
            result['state'] = 'running'
            progress(result)
            child = subprocess.run([str(python), '-B', str(candidate / 'remote_live_launch.py'), 'run-one',
                                    '--config', config_path, '--config-sha256', config_sha,
                                    '--task-id', identity['task_id'], '--group-id', identity['group_id']],
                                   stdout=log, stderr=subprocess.STDOUT, timeout=5000, check=False)
            result.update(state='exited', exit_code=child.returncode)
    except BaseException as exc:
        result.update(state='unconfirmed', error_type=type(exc).__name__)
    finally:
        if opened:
            try:
                barriers.hold_postprocessing()
                result['gate_reclosed'] = barriers._read() == {'rag_upload': 1, 'postprocessing': 1}
            except Exception as exc:
                result['gate_error_type'] = type(exc).__name__
        result['ended_ns'] = time.time_ns()
        progress(result)
    return 0 if result['state'] == 'exited' and result['exit_code'] == 0 and result['gate_reclosed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
