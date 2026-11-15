import json
import os
import sys
import time
from pathlib import Path
from urllib.request import urlopen

root = Path(__file__).resolve().parents[2]
os.environ['PREFECT_API_URL'] = 'http://192.168.1.120:4200/api'
sys.path[:0] = [str(root / 'candidates/postprocess-prefect-20261031/server_release_next'),
                str(root / 'candidates/postprocess-prefect-20261031/projection_next'),
                str(root / 'candidates/postprocess-prefect-20261031/runtime_next')]
import ticket_positions
import projections
import stage_runtime
from prefect.client.orchestration import get_client


class Observer:
    api_url = os.environ['PREFECT_API_URL']

    def _client(self):
        return get_client(sync_client=True)

    def _safe_deployment(self, client):
        deployment = client.read_deployment_by_name('资料文件夹工单/前处理')
        pool = client.read_work_pool('paperflow-unified-preprocessing')
        if (deployment.work_pool_name != 'paperflow-unified-preprocessing'
                or 'paperflow-order-v2-live-20261031' not in deployment.tags
                or pool.is_paused or pool.description != 'paperflow-order-v2-live-20261031'
                or pool.type != 'process'):
            raise ValueError('deployment unavailable')
        return deployment


projector = projections.Projections.__new__(projections.Projections)
projector.control = root / 'candidates/first-step-control-20261029/runtime/workspace/output/group-task-summaries/summary-index.sqlite3'
projector.local = root / 'local-intake-runtime/waiting-pool.sqlite3'
projector.runtime = stage_runtime
projector.observer = Observer()

results = []
until = time.monotonic() + 58
while time.monotonic() < until:
    started = time.monotonic()
    with urlopen('http://127.0.0.1:14206/api/v1/local-intake/postprocessing/positions', timeout=5) as response:
        value = json.load(response)
    if value.get('available') is not True:
        probe = time.monotonic()
        try:
            direct = ticket_positions.observe(projector)
            results.append({'available': False, 'direct_available': direct.get('available'),
                            'direct_seconds': round(time.monotonic() - probe, 3)})
        except Exception as exc:
            results.append({'available': False, 'direct_error_type': type(exc).__name__,
                            'direct_error': str(exc)[:160],
                            'direct_seconds': round(time.monotonic() - probe, 3)})
    else:
        results.append({'available': True})
    time.sleep(max(0, 1 - (time.monotonic() - started)))

print(json.dumps({'samples': len(results), 'unavailable': [row for row in results if not row['available']]},
                 ensure_ascii=False), flush=True)
