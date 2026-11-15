"""Read-only aggregate comparison of local folders and their Prefect runs."""
from collections import Counter
import json
from pathlib import Path
import sqlite3
import urllib.request


config = json.loads(Path('candidates/first-step-control-20261029/runtime/workspace-config.json').read_text(encoding='utf8'))
database = Path(config['classification_root']) / 'group-task-summaries' / 'summary-index.sqlite3'
with sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=3) as db:
    folders = db.execute('SELECT state,task_run_id FROM llm_folder_queue').fetchall()
    post = db.execute('SELECT state,job_id,task_run_id FROM paperflow_postprocess_stage_jobs').fetchall()


def read_runs(endpoint, key, ids):
    output = []
    for pos in range(0, len(ids), 100):
        query = {key: {'id': {'any_': ids[pos:pos + 100]}}, 'limit': 100}
        data = json.dumps(query).encode('utf8')
        request = urllib.request.Request(config['prefect_api_url'].rstrip('/') + endpoint,
            data=data, method='POST', headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=12) as response:
            output.extend(json.load(response))
    return {row['id']: row for row in output}


runs = read_runs('/flow_runs/filter', 'flow_runs', [r[1] for r in folders])
tasks = read_runs('/task_runs/filter', 'task_runs', [r[1] for r in post])
counts = Counter()
for phase, run_id in folders:
    run = runs.get(run_id)
    state = (run or {}).get('state') or {}
    label = ((run or {}).get('name') or '').rsplit('·', 1)[-1].strip()
    counts[(phase, state.get('type'), label or None)] += 1
print('folder_counts', json.dumps([{'local': k[0], 'prefect': k[1], 'name_suffix': k[2], 'count': v}
                                  for k, v in sorted(counts.items(), key=lambda x: str(x[0]))], ensure_ascii=False))
job_counts = Counter((state, ((tasks.get(job) or {}).get('state') or {}).get('type')) for state, job, _ in post)
print('postprocess_task_counts', json.dumps([{'local': k[0], 'prefect': k[1], 'count': v}
                                           for k, v in sorted(job_counts.items(), key=lambda x: str(x[0]))]))
