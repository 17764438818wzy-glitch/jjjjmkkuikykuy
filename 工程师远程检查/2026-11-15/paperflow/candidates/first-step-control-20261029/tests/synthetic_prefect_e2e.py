"""Exercise 14204 -> 4292 using synthetic files and the synthetic local model."""
import base64
import json
from pathlib import Path
import sqlite3
import time
import uuid

import httpx

from paperflow_prefect.group_delivery import load_delivery
from paperflow_prefect.settings import Settings


API = 'http://127.0.0.1:14204'
PREFECT = 'http://127.0.0.1:4292/api'


def expect(response, status=200):
    if response.status_code != status:
        raise AssertionError(f'{response.request.method} {response.request.url.path}: '
                             f'{response.status_code}: {response.text[:500]}')
    return response.json() if response.content else None


def wait_state(client, task, predicate, seconds=90):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        state = expect(client.get(API + f'/api/v1/first-step/tasks/{task}'))
        if predicate(state):
            return state
        time.sleep(.15)
    raise AssertionError(f'task did not reach expected state: {state}')


def main():
    settings = Settings.load()
    if settings['prefect_api_url'] != PREFECT or ':11436/' not in settings['nimble_endpoint']:
        raise RuntimeError('synthetic test requires the integration profile and fake model')
    root = '合成资料'
    files = [(f'{root}/测试组{i}/论文.txt', f'题目：合成测试论文{i}\n摘要：这是一份用于验证的论文正文。'.encode())
             for i in range(1, 5)]
    files += [(f'{root}/测试组1/数据.csv', b'year,value\n2020,42\n'),
              (f'{root}/测试组5/空论文.txt', b''),
              (f'{root}/测试组6/未知文件.bin', b'synthetic bytes')]
    writer = str(uuid.uuid4())
    with httpx.Client(timeout=15) as client:
        task = expect(client.post(API + '/api/v1/first-step/tasks',
                      json={'name': '后端合成端到端', 'request_id': str(uuid.uuid4())}))['task_id']
        upload = expect(client.post(API + f'/api/v1/first-step/tasks/{task}/uploads',
                                    json={'root_name': root}))['upload_id']
        folder = API + f'/api/v1/first-step/uploads/{upload}'
        expect(client.post(folder + '/manifest', json={'files': [
            {'relative_path': path, 'size': len(data)} for path, data in files]}))
        expect(client.post(folder + '/seal', json={'expected_files': len(files)}))
        bound = {'X-Task-ID': task, 'X-Folder-Upload-ID': upload, 'X-Writer-ID': writer,
                 'Tus-Resumable': '1.0.0'}
        for path, data in files:
            meta = {'task_id': task, 'folder_upload_id': upload, 'relativePath': path}
            encoded = ','.join(k + ' ' + base64.b64encode(v.encode()).decode() for k, v in meta.items())
            created = client.post(API + '/tus/files/', content=b'', headers={**bound,
                'Upload-Length': str(len(data)), 'Upload-Metadata': encoded, 'X-Request-ID': str(uuid.uuid4())})
            expect(created, 201)
            if data:
                patched = client.patch(created.headers['Location'], content=data, headers={**bound,
                    'Upload-Offset': '0', 'Content-Type': 'application/offset+octet-stream'})
                expect(patched, 204)
        ready = expect(client.post(folder + '/finish'))
        assert ready['task_status'] == 'ready' and ready['total_groups'] == 6, ready
        started = expect(client.post(API + f'/api/v1/first-step/tasks/{task}/start',
                                     json={'expected_revision': ready['control_revision']}))
        print('started', started['task_status'], flush=True)
        running = wait_state(client, task, lambda s: s['active_groups'] == 4 and s['inflight_requests'] == 1)
        print('observed', running['active_groups'], 'groups and', running['inflight_requests'], 'model request', flush=True)
        paused = expect(client.post(API + f'/api/v1/first-step/tasks/{task}/pause', json={}))
        assert paused['task_status'] in ('pausing', 'paused'), paused
        stopped = wait_state(client, task, lambda s: s['task_status'] in ('paused', 'unknown'))
        assert stopped['task_status'] == 'paused', stopped
        print('paused with', stopped['processed_groups'], 'groups processed', flush=True)
        expect(client.post(API + f'/api/v1/first-step/tasks/{task}/start',
                           json={'expected_revision': stopped['control_revision']}))
        done = wait_state(client, task, lambda s: s['task_status'] in ('complete', 'needs_review', 'unknown'))
        assert done['task_status'] == 'complete' and done['processed_groups'] == 6, done
        assert done['total_groups'] == 6
        assert sum(done[k] for k in ('passed_groups', 'failed_groups', 'unrecognized_groups')) == 6
        for kind in ('passed', 'failed', 'unrecognized'):
            listing = expect(client.get(API + f'/api/v1/first-step/tasks/{task}', params={'filter': kind}))
            assert listing['list_total'] == done[kind + '_groups'] == len(listing['items'])
        assert done['passed_groups'] >= 1
        first = expect(client.get(API + f'/api/v1/first-step/tasks/{task}', params={'filter': 'passed'}))['items'][0]
        handoff, delivery = load_delivery(first['delivery_dir'])
        assert handoff.is_file() and delivery['manifest']['ready_for_receipt'] is True
        with sqlite3.connect(settings.path('classification_root') / 'group-task-summaries' / 'summary-index.sqlite3') as db:
            run_id = db.execute('SELECT prefect_run_id FROM tasks WHERE task_id=?', (task,)).fetchone()[0]
        assert run_id
        for _ in range(100):
            run = expect(client.get(PREFECT + '/flow_runs/' + run_id))
            if run['state']['type'] == 'COMPLETED':
                break
            time.sleep(.1)
        else:
            raise AssertionError(f"Prefect run state: {run['state']['type']}")
        print('done', json.dumps({k: done[k] for k in ('total_groups', 'passed_groups',
            'failed_groups', 'unrecognized_groups', 'processed_groups')}, ensure_ascii=False), flush=True)
        print('Prefect', run['state']['type'], 'delivery verified', flush=True)


if __name__ == '__main__':
    main()
