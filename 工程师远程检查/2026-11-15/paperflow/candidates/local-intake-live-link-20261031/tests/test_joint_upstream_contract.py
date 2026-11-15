"""Joint synthetic test of the two candidate implementations over loopback HTTP."""

from pathlib import Path
import subprocess
import sys


def test_real_upstream_candidate_reservation_and_local_reception(tmp_path):
    local = Path(__file__).resolve().parents[1]
    upstream = local.parent / 'first-step-intake-retention-20261031'
    script = r'''
import json, socket, sqlite3, subprocess, sys, threading, time, uuid
from pathlib import Path

base, upstream, local = map(Path, sys.argv[1:])
sys.path[:0] = [str(upstream), str(upstream / 'tests'), str(local)]
import test_local_retention as upstream_fixtures
from paperflow_prefect.group_task_summary import install_group_summary_routes
from local_queue import LocalQueue
from upstream_discovery import FirstStepHTTPSource, discover_once
from waiting_api import create_app
from fastapi.testclient import TestClient
import uvicorn

original_ready = upstream_fixtures.ready_control
def ready_with_attachment_and_trash(*args, **kwargs):
    control, scheduler, task_id = original_ready(*args, **kwargs)
    with control.summary._connect() as db:
        source = Path(db.execute('SELECT group_root FROM control_groups WHERE task_id=?', (task_id,)).fetchone()[0])
    (source / 'data.csv').write_text('date,value\n1,2', encoding='utf8')
    (source / '开题报告.rtf').write_text('开题报告\n合成过程材料', encoding='utf8')
    return control, scheduler, task_id
upstream_fixtures.ready_control = ready_with_attachment_and_trash
client, control, task_id, row, receipt, request, seq = upstream_fixtures.prepare_registered(base)
handoff = json.loads((Path(row['delivery_dir']) / 'handoff.json').read_text(encoding='utf8'))
assert len(handoff['trash']) == 1 and len(receipt['inventory']['attachments']) == 1
(Path(row['delivery_dir']) / 'files' / handoff['trash'][0]['relative']).unlink()
queue_path = base / 'local-queue.sqlite3'
queue = LocalQueue(queue_path, require_retention=True)
queue.start_receiver()
client.app.state.test_control.local_intake_queue_db = queue_path
install_group_summary_routes(client.app, control.output)

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind(('127.0.0.1', 0))
sock.listen(128)
port = sock.getsockname()[1]
server = uvicorn.Server(uvicorn.Config(client.app, host='127.0.0.1', port=port,
                                       log_level='error', lifespan='off', access_log=False))
thread = threading.Thread(target=server.run, kwargs={'sockets': [sock]}, daemon=True)
thread.start()
try:
    deadline = time.monotonic() + 20
    while not server.started:
        assert thread.is_alive() and time.monotonic() < deadline, 'synthetic server did not start'
        time.sleep(0.05)
    source = FirstStepHTTPSource(f'http://127.0.0.1:{port}', page_size=200)
    result = discover_once(source, queue, receipt_root=control.receipt_root, retention_client=source)
    assert result['complete'], result
    api_queue = LocalQueue(queue_path, require_retention=True, require_live_watcher=True)
    with TestClient(create_app(api_queue)) as api:
        summary = api.get('/api/v1/local-intake/preprocessing/summary').json()
        assert summary['waiting_available'] and summary['waiting_count'] == 1, summary
        assert api.post('/api/v1/local-intake/preprocessing/claims',
                        json={'consumer_id':'test','attempt_id':'a'}).status_code == 404
    waiting = queue.list_waiting()
    assert len(waiting) == 1 and waiting[0]['paper_id'] == row['paper_id']
    assert waiting[0]['body']['file_id'] == receipt['inventory']['body']['file_id']
    assert len(waiting[0]['attachments']) == 1 and 'trash' not in waiting[0]
    refs = source.list_retentions(task_id)['items']
    assert len(refs) == 1 and refs[0]['state'] == 'accepted'
    retained_id = refs[0]['retention_id']
    with control.summary._connect() as db, db:
        db.executemany('INSERT INTO local_retention_references(retention_id,request_id,task_id,group_id,'
                       'paper_id,receipt_id,consumer_id,state,created_at_ns,updated_at_ns) '
                       "VALUES(?,?,?,?,?,?,?,'accepted',?,?)",
                       [(str(uuid.uuid4()), str(uuid.uuid4()), task_id, uuid.uuid4().hex,
                         f'synthetic-{i}', f'{i:064x}', 'local-intake', i, i) for i in range(205)])
    refs = source.list_retentions(task_id)['items']
    assert len(refs) == 206 and retained_id in {ref['retention_id'] for ref in refs}
    assert discover_once(source, queue, receipt_root=control.receipt_root, retention_client=source)['complete']
    assert len(queue.list_waiting()) == 1
    release = client.post(f"/api/v1/first-step/tasks/{task_id}/local-retentions/{retained_id}/release",
                          json={'consumer_id':'local-intake', 'receipt_id':row['receipt_id'],
                                'reason':'processed','confirm_not_in_use':True})
    assert release.status_code == 409
    queue.record_receiver('stopped')

    def start_worker():
        return subprocess.Popen([sys.executable, str(local / 'run_waiting_service.py'), '--log',
            str(base / 'watch.log'), '--', 'watch', '--db', str(queue_path),
            '--receipt-root', str(control.receipt_root), '--upstream', f'http://127.0.0.1:{port}',
            '--interval-seconds', '0.2'], cwd=local, stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))

    def wait_received(worker, prior_session=None):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            assert worker.poll() is None, (base / 'watch.log').read_text(encoding='utf8')
            state = api_queue.snapshot()
            with sqlite3.connect(queue_path) as db:
                meta = dict(db.execute('SELECT key,value FROM metadata'))
            if (state['waiting_available'] and state['waiting_count'] == 1
                    and state['receiver_status'] == 'running'
                    and meta.get('receiver_session_id') != prior_session
                    and meta.get('scan_completed_at')):
                return meta
            time.sleep(0.1)
        raise AssertionError((base / 'watch.log').read_text(encoding='utf8'))

    workers = []
    try:
        worker = start_worker()
        workers.append(worker)
        first = wait_received(worker, queue.receiver_session_id)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            with sqlite3.connect(queue_path) as db:
                later = dict(db.execute('SELECT key,value FROM metadata'))
            if later.get('scan_completed_at') != first['scan_completed_at']:
                break
            time.sleep(0.1)
        else:
            raise AssertionError('watcher did not repeat discovery')
        assert api_queue.snapshot()['waiting_count'] == 1
        worker.terminate()
        worker.wait(timeout=10)
        with sqlite3.connect(queue_path) as db, db:
            db.execute("UPDATE metadata SET value='2000-01-01T00:00:00+00:00' "
                       "WHERE key='receiver_heartbeat_at'")
        stopped = api_queue.snapshot()
        assert stopped['receiver_status'] == 'interrupted' and stopped['waiting_available'] is False
        restarted = start_worker()
        workers.append(restarted)
        second = wait_received(restarted, first['receiver_session_id'])
        assert second['receiver_session_id'] != first['receiver_session_id']
        assert api_queue.snapshot()['waiting_count'] == 1
    finally:
        for worker in workers:
            if worker.poll() is None:
                worker.terminate()
                worker.wait(timeout=10)
    print(json.dumps({'status':'ok','waiting':summary['waiting_count'],'reference_pages':len(refs)}))
finally:
    server.should_exit = True
    thread.join(timeout=10)
    sock.close()
    assert not thread.is_alive(), 'synthetic server failed to stop'
'''
    result = subprocess.run([sys.executable, '-B', '-X', 'utf8', '-c', script,
                             str(tmp_path), str(upstream), str(local)], capture_output=True,
                            encoding='utf8', text=True, timeout=60,
                            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    assert result.returncode == 0, result.stdout + '\n' + result.stderr
    assert '"status": "ok"' in result.stdout
