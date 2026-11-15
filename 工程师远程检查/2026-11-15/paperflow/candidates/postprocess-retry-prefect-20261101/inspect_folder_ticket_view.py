"""Read one candidate ticket snapshot; no business work or mutations."""
from collections import Counter
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sqlite3
import time

here = Path(__file__).parent
root = here.parents[1]
remote = json.loads((root / 'candidates/postprocess-rag-unblock-20261101/remote-config.json').read_text(encoding='utf8'))
settings = json.loads((root / 'prefect-paperflow/config.json').read_text(encoding='utf8'))
with sqlite3.connect(Path(remote['journal_database']).resolve().as_uri() + '?mode=ro', uri=True) as db:
    control = dict(db.execute('SELECT name,value FROM journal_settings'))['control_database']
os.environ['PREFECT_API_URL'] = settings['prefect_api_url']
from prefect.client.orchestration import get_client
from projections import Projections
from folder_tickets import observe
import stage_runtime
import ticket_positions

class Observer:
    api_url = settings['prefect_api_url']
    @contextmanager
    def _client(self):
        with get_client(sync_client=True) as client:
            yield client
    def _safe_deployment(self, client):
        return client.read_deployment_by_name('资料文件夹工单/前处理')
    def _add_result_titles(self, rows):
        pass

class Projector:
    local = (root / 'local-intake-runtime/waiting-pool.sqlite3').resolve()
    control = Path(control).resolve()
    observer = Observer()
    runtime = stage_runtime
    _prefect = Projections._prefect

started = time.monotonic()
snapshot = observe(Projector(), ticket_positions)
print(json.dumps({'count': len(snapshot['folders']), 'complete': snapshot['complete'],
                  'stages': Counter(row['stage'] for row in snapshot['folders']),
                  'seconds': round(time.monotonic() - started, 2)}))
