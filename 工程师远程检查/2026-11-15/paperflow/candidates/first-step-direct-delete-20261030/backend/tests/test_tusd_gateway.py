"""Exercise the task-bound gateway against official tusd using synthetic files."""
import base64
from pathlib import Path
import socket
import subprocess
import threading
import time
import uuid
import importlib

from fastapi import FastAPI
import httpx
import uvicorn
from fastapi.testclient import TestClient

from paperflow_prefect.first_step_control_api import install_control_routes
from paperflow_prefect.group_task_summary import install_group_summary_routes


def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def test_candidate_cors_allows_frontend_tus_requests(tmp_path, monkeypatch):
    from paperflow_prefect.settings import Settings
    class FakeSettings:
        def path(self, name):
            return tmp_path / name
        def __getitem__(self, name):
            return 'http://127.0.0.1:1/api'
    monkeypatch.setattr(Settings, 'load', classmethod(lambda cls: FakeSettings()))
    import summary_api
    importlib.reload(summary_api)
    client = TestClient(summary_api.app)
    for method in ('POST', 'PATCH', 'HEAD'):
        response = client.options('/tus/files/', headers={'Origin': 'http://127.0.0.1:8795',
            'Access-Control-Request-Method': method,
            'Access-Control-Request-Headers': 'x-task-id,x-folder-upload-id,x-writer-id,x-request-id,tus-resumable'})
        assert response.status_code == 200
        assert response.headers['access-control-allow-origin'] == 'http://127.0.0.1:8795'
    response = client.post('/tus/files/', headers={'Origin': 'http://127.0.0.1:8795'})
    exposed = response.headers['access-control-expose-headers'].lower()
    assert all(name in exposed for name in ('location', 'upload-offset', 'upload-length', 'tus-resumable'))


def test_tusd_gateway_binds_task_and_upload_window(tmp_path, monkeypatch):
    api_port, tus_port = port(), port()
    api = f'http://127.0.0.1:{api_port}'
    app = FastAPI()
    install_group_summary_routes(app, tmp_path / 'output')
    control = install_control_routes(app, tmp_path / 'output', tmp_path / 'data',
        tus_base_url=api + '/tus/files/', tus_backend_url=f'http://127.0.0.1:{tus_port}/files/')
    server = uvicorn.Server(uvicorn.Config(app, host='127.0.0.1', port=api_port, log_level='error'))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    binary = Path(__file__).resolve().parents[2] / 'first-step-uppy-tusd-20261029' / 'tusd' / 'tusd.exe'
    with (tmp_path / 'tusd.log').open('w') as log:
        proc = subprocess.Popen([str(binary), '-host', '127.0.0.1', '-port', str(tus_port),
            '-upload-dir', str(control.tus_dir), '-hooks-http', api + '/api/v1/first-step/internal/tusd-hooks',
            '-hooks-enabled-events', 'pre-create,pre-finish', '-disable-download', '-disable-termination'],
            stdout=log, stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NO_WINDOW)
        try:
            with httpx.Client(timeout=5) as client:
                for _ in range(100):
                    try:
                        if server.started and client.options(api + '/tus/files/').status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    time.sleep(.05)
                else:
                    raise AssertionError('synthetic gateway did not start')
                task = client.post(api + '/api/v1/first-step/tasks', json={'name': '甲'}).json()['task_id']
                upload = client.post(api + f'/api/v1/first-step/tasks/{task}/uploads',
                                     json={'root_name': 'root'}).json()['upload_id']
                folder = api + f'/api/v1/first-step/uploads/{upload}'
                relative = 'root/person/paper.txt'
                assert client.post(folder + '/manifest', json={'files': [{'relative_path': relative, 'size': 6}]}).status_code == 200
                assert client.post(folder + '/seal', json={'expected_files': 1}).status_code == 200
                writer = str(uuid.uuid4())
                bound = {'X-Task-ID': task, 'X-Folder-Upload-ID': upload, 'X-Writer-ID': writer,
                         'Tus-Resumable': '1.0.0'}
                meta = {'task_id': task, 'folder_upload_id': upload, 'relativePath': relative}
                encoded = ','.join(k + ' ' + base64.b64encode(v.encode()).decode() for k, v in meta.items())
                created = client.post(api + '/tus/files/', content=b'', headers={**bound,
                    'Upload-Length': '6', 'Upload-Metadata': encoded, 'X-Request-ID': str(uuid.uuid4())})
                assert created.status_code == 201, created.text
                location = created.headers['Location']
                assert location.startswith(api + '/tus/files/')
                from contextlib import closing
                with closing(control.summary._connect()) as db:
                    row = db.execute('SELECT * FROM tus_manifest WHERE upload_id=?', (upload,)).fetchone()
                    hook_headers = {'X-Task-ID': [task], 'X-Folder-Upload-ID': [upload],
                        'X-Writer-ID': [writer], 'X-Request-ID': [row['create_request_id']],
                        'X-Gateway-Invocation': [row['create_invocation_id']]}
                hook = {'Type': 'pre-create', 'Event': {'Upload': {'Size': 6, 'MetaData': meta},
                        'HTTPRequest': {'Header': hook_headers}}}
                replay = client.post(api + '/api/v1/first-step/internal/tusd-hooks', json=hook)
                assert replay.json()['ChangeFileInfo']['ID'] == location.rsplit('/', 1)[-1]
                original_release = control.tus.release_failed_create
                def fail_release(_):
                    raise RuntimeError('synthetic cleanup failure')
                monkeypatch.setattr(control.tus, 'release_failed_create', fail_release)
                again = client.post(api + '/tus/files/', content=b'', headers={**bound,
                    'Upload-Length': '6', 'Upload-Metadata': encoded, 'X-Request-ID': hook_headers['X-Request-ID'][0]})
                assert again.status_code == 409
                monkeypatch.setattr(control.tus, 'release_failed_create', original_release)
                bad = client.patch(location, content=b'evil', headers={**bound, 'X-Writer-ID': str(uuid.uuid4()),
                    'Upload-Offset': '0', 'Content-Type': 'application/offset+octet-stream'})
                assert bad.status_code == 403
                bad = client.head(location, headers={**bound, 'X-Task-ID': str(uuid.uuid4())})
                assert bad.status_code in {403, 404}
                assert client.head(location, headers=bound).headers['Upload-Offset'] == '0'
                started, release, replies = threading.Event(), threading.Event(), []
                def chunks():
                    yield b'abc'
                    started.set()
                    assert release.wait(5)
                    yield b'def'
                def send_partial():
                    with httpx.Client(timeout=10) as writer_client:
                        replies.append(writer_client.patch(location, content=chunks(), headers={**bound,
                            'Upload-Offset': '0', 'Content-Type': 'application/offset+octet-stream',
                            'Content-Length': '6'}))
                writing = threading.Thread(target=send_partial)
                writing.start()
                try:
                    assert started.wait(5)
                    for _ in range(100):
                        if control.has_upload_transfers(task):
                            break
                        time.sleep(.01)
                    assert control.has_upload_transfers(task)
                    assert client.delete(api + f'/api/v1/first-step/tasks/{task}').status_code == 409
                finally:
                    release.set()
                    writing.join(timeout=10)
                assert not writing.is_alive() and replies[0].status_code == 204
                done = client.post(folder + '/finish')
                assert done.status_code == 200, done.text
                assert (tmp_path / 'data' / 'uploads' / upload / 'source' / relative).read_bytes() == b'abcdef'
        finally:
            proc.terminate()
            proc.wait(timeout=5)
            server.should_exit = True
            thread.join(timeout=5)
