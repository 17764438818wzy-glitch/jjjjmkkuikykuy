"""Read old stage states for the paused task without submitting any work."""
from collections import Counter
import json
import os
from pathlib import Path
import sqlite3
from uuid import UUID

root = Path(__file__).resolve().parents[2]
remote = json.loads((root / 'candidates/postprocess-rag-unblock-20261101/remote-config.json').read_text(encoding='utf8'))
settings = json.loads((root / 'prefect-paperflow/config.json').read_text(encoding='utf-8-sig'))
with sqlite3.connect(Path(remote['journal_database']).resolve().as_uri() + '?mode=ro', uri=True) as db:
    control = dict(db.execute('SELECT name,value FROM journal_settings'))['control_database']
with sqlite3.connect(Path(control).resolve().as_uri() + '?mode=ro', uri=True) as db:
    jobs = db.execute("SELECT job_id FROM paperflow_postprocess_stage_jobs WHERE task_id=? AND state='execution_unknown'",
        ('6cb5a766-94f9-4613-aa5e-17a883be9016',)).fetchall()
os.environ['PREFECT_API_URL'] = settings['prefect_api_url']
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import TaskRunFilter, TaskRunFilterId
with get_client(sync_client=True) as client:
    runs = client.read_task_runs(task_run_filter=TaskRunFilter(id=TaskRunFilterId(any_=[UUID(x[0]) for x in jobs])),
                                 limit=len(jobs))
states = {str(run.id): run.state.type.value if run.state else None for run in runs}
states.update({job[0]: 'MISSING' for job in jobs if job[0] not in states})
(Path(__file__).parent / 'prefect-old-stage-observation.json').write_text(json.dumps(states), encoding='utf8')
print(json.dumps(dict(total=len(jobs), states=Counter(states.values())), ensure_ascii=True))
