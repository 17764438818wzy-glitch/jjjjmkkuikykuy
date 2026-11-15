"""Freeze only the observed, authorized old attempts for the paused task."""
import json
from pathlib import Path
import sqlite3

here = Path(__file__).resolve().parent
root = here.parents[1]
task_id = '6cb5a766-94f9-4613-aa5e-17a883be9016'
remote = json.loads((root / 'candidates/postprocess-rag-unblock-20261101/remote-config.json').read_text(encoding='utf8'))
server_rows = json.loads((root / 'candidates/postprocess-rag-unblock-20261101/body-normalization/unknown-observation.json').read_text(encoding='utf8'))
prefect_states = json.loads((here / 'prefect-old-stage-observation.json').read_text(encoding='utf8'))
with sqlite3.connect(Path(remote['journal_database']).resolve().as_uri() + '?mode=ro', uri=True) as db:
    control = dict(db.execute('SELECT name,value FROM journal_settings'))['control_database']
with sqlite3.connect(Path(control).resolve().as_uri() + '?mode=ro', uri=True) as db:
    task = db.execute('SELECT desired_state,start_pending FROM tasks WHERE task_id=?', (task_id,)).fetchone()
    rows = db.execute("SELECT j.job_id FROM paperflow_postprocess_stage_jobs j "
                      "LEFT JOIN paperflow_rag_nas_jobs r ON r.task_id=j.task_id AND r.group_id=j.group_id "
                      "WHERE j.task_id=? AND j.state='execution_unknown' AND r.group_id IS NULL", (task_id,)).fetchall()
server = {row['job_id']: row for row in server_rows}
ids = [row[0] for row in rows]
if (task != ('paused', 0) or len(ids) != 55 or set(ids) != set(server) or set(ids) != set(prefect_states)
        or any(prefect_states[job] != 'FAILED' for job in ids)
        or any(row['wire_result_exists'] or not (
            row['executor_state'] is None and row['service_phase'] is None
            or row['executor_state'] == row['service_phase'] == 'unknown'
               and row['cleanup_terminated'] is True and row['cleanup_scope_removed'] is True)
            for row in server_rows)):
    raise ValueError('the observed original attempts changed; do not prepare the recovery list')
(here / 'retry-targets.json').write_text(json.dumps({task_id: sorted(ids)}, ensure_ascii=False), encoding='utf8')
print('prepared', len(ids), 'original postprocessing attempts; task remains paused')
