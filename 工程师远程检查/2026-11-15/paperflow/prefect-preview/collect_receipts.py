"""Read-only preview verification; run in the authenticated maintenance session."""
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import urllib.request

API = 'http://192.168.1.120:4200/api'


def read_api(path, query=None):
    payload = None if query is None else json.dumps(query).encode()
    request = urllib.request.Request(API + path, data=payload,
                                     headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.load(response)


def collect_receipts():
    runs = json.loads((LOCAL / 'runs-created.json').read_text('utf-8'))
    summaries = []
    sftp = client.open_sftp()
    try:
        for original in runs:
            run = read_api('/flow_runs/' + original['id'])
            if run['state_type'] != 'COMPLETED':
                raise RuntimeError(f"Preview run not complete: {run['name']} {run['state_type']}")
            tasks = read_api('/task_runs/filter', {
                'flow_runs': {'id': {'any_': [run['id']]}}, 'limit': 200,
            })
            kind = 'five-steps' if run['name'] == 'preview-five-steps' else 'concurrency'
            name = kind + '-' + run['id'] + '.json'
            destination = LOCAL / name
            sftp.get(REMOTE + '/demo-output/' + name, str(destination))
            result = json.loads(destination.read_text('utf-8'))
            if not result['simulation_only'] or result['business_external_calls'] != 0:
                raise RuntimeError('Preview scope mismatch')
            item = {
                'run_id': run['id'], 'name': run['name'], 'state': run['state_type'],
                'task_states': dict(Counter(task['state_type'] for task in tasks)),
                'task_count': len(tasks), 'result_file': name,
                'result_sha256': hashlib.sha256(destination.read_bytes()).hexdigest(),
                'simulation_only': True, 'business_external_calls': 0,
            }
            if kind == 'concurrency':
                active = {'body': 0, 'attachment': 0}
                peak = {'body': 0, 'attachment': 0, 'total': 0}
                events = []
                for row in result['requests']:
                    events.extend([(row['started'], 1, row['kind']), (row['finished'], -1, row['kind'])])
                for _, delta, lane in sorted(events):
                    active[lane] += delta
                    peak[lane] = max(peak[lane], active[lane])
                    peak['total'] = max(peak['total'], sum(active.values()))
                if peak != result['observed_simulated_peak']:
                    raise RuntimeError('Recorded peak differs from timestamp calculation')
                item['simulated_peak_recomputed_from_timestamps'] = peak
            else:
                item['synthetic_papers'] = result['paper_count']
                item['local_retention_simulation'] = all(row['local_folder_retained'] for row in result['papers'])
            summaries.append(item)
        env_mode = oct(sftp.stat(REMOTE + '/.env').st_mode & 0o777)
    finally:
        sftp.close()
    receipt = {
        'checked_at_utc': datetime.now(timezone.utc).isoformat(),
        'url': 'http://192.168.1.120:4200',
        'health': read_api('/health'), 'prefect_version': read_api('/admin/version'),
        'remote_directory': REMOTE, 'database_secret_file_mode': env_mode,
        'runs': summaries,
        'container_check': json.loads((LOCAL / 'containers-after.json').read_text('utf-8')),
        'limits_after_runs': [read_api('/v2/concurrency_limits/' + name) for name in
                              ('demo-body-llm', 'demo-attachment-llm', 'demo-total-llm')],
        'limits': 'Isolated synthetic demo only; no real LLM, paper, RAG or NAS acceptance.',
    }
    (LOCAL / 'deployment-receipt.json').write_text(json.dumps(receipt, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'health': receipt['health'], 'version': receipt['prefect_version'], 'runs': summaries}, ensure_ascii=False))


collect_receipts()
