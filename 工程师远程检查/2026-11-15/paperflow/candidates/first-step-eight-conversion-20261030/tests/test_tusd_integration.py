"""Synthetic HTTP integration with the official local tusd binary."""
import base64
from pathlib import Path
import socket
import subprocess
import threading
import time
import uuid

from fastapi import FastAPI
import httpx
import uvicorn

from paperflow_prefect.first_step_control_api import install_control_routes
from paperflow_prefect.group_task_summary import install_group_summary_routes


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def test_official_tusd_uploads_and_restores_synthetic_folder(tmp_path):
    api_port, tus_port = free_port(), free_port()
    app = FastAPI()
    install_group_summary_routes(app, tmp_path / 'output')
    control = install_control_routes(app, tmp_path / 'output', tmp_path / 'data',
        tus_base_url=f'http://127.0.0.1:{tus_port}/files/')
    server = uvicorn.Server(uvicorn.Config(app, host='127.0.0.1', port=api_port, log_level='error'))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    binary = Path(__file__).resolve().parents[2] / 'first-step-uppy-tusd-20261029' / 'tusd' / 'tusd.exe'
    log = (tmp_path / 'synthetic-tusd.log').open('w', encoding='utf-8')
    process = subprocess.Popen([str(binary), '-host', '127.0.0.1', '-port', str(tus_port),
        '-upload-dir', str(control.tus_dir), '-hooks-http',
        f'http://127.0.0.1:{api_port}/api/v1/first-step/internal/tusd-hooks',
        '-hooks-enabled-events', 'pre-create,pre-finish', '-disable-download', '-disable-termination'],
        stdout=log, stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NO_WINDOW)
    try:
        api = f'http://127.0.0.1:{api_port}'
        tus = f'http://127.0.0.1:{tus_port}/files/'
        with httpx.Client(timeout=5) as client:
            for attempt in range(100):
                try:
                    if server.started and client.options(tus).status_code == 200:
                        break
                except httpx.HTTPError:
                    time.sleep(.05)
            else:
                raise AssertionError('synthetic servers did not start')
            task = client.post(api + '/api/v1/first-step/tasks', json={'name': '合成任务'}).json()['task_id']
            upload = client.post(api + f'/api/v1/first-step/tasks/{task}/uploads',
                                 json={'root_name': '原始文件'}).json()['upload_id']
            relative, content = '原始文件/一班/甲/论文.txt', b'synthetic content'
            empty_path = '原始文件/一班/甲/空白.txt'
            folder = api + f'/api/v1/first-step/uploads/{upload}'
            assert client.post(folder + '/manifest', json={'files': [
                {'relative_path': relative, 'size': len(content)},
                {'relative_path': empty_path, 'size': 0}]}).status_code == 200
            assert client.post(folder + '/seal', json={'expected_files': 2}).status_code == 200
            metadata = {'task_id': task, 'folder_upload_id': upload, 'relativePath': relative}
            encoded = ','.join(key + ' ' + base64.b64encode(value.encode()).decode()
                               for key, value in metadata.items())
            headers = {'Tus-Resumable': '1.0.0', 'Upload-Length': str(len(content)),
                       'Upload-Metadata': encoded, 'X-Request-ID': str(uuid.uuid4())}
            create = client.post(tus, content=b'', headers=headers)
            assert create.status_code == 201, create.text
            location = create.headers['Location']
            patch = client.patch(location, content=content[:3], headers={'Tus-Resumable': '1.0.0',
                'Upload-Offset': '0', 'Content-Type': 'application/offset+octet-stream'})
            assert patch.status_code == 204, patch.text
            repeated_create = client.post(tus, content=b'', headers=headers)
            assert repeated_create.status_code == 409, repeated_create.text
            head = client.head(location, headers={'Tus-Resumable': '1.0.0'})
            assert head.headers['Upload-Offset'] == '3'
            patch = client.patch(location, content=content[3:], headers={'Tus-Resumable': '1.0.0',
                'Upload-Offset': '3', 'Content-Type': 'application/offset+octet-stream'})
            assert patch.status_code == 204, patch.text
            head = client.head(location, headers={'Tus-Resumable': '1.0.0'})
            assert head.headers['Upload-Offset'] == head.headers['Upload-Length'] == str(len(content))
            empty_metadata = {'task_id': task, 'folder_upload_id': upload, 'relativePath': empty_path}
            empty_encoded = ','.join(key + ' ' + base64.b64encode(value.encode()).decode()
                                     for key, value in empty_metadata.items())
            empty = client.post(tus, content=b'', headers={'Tus-Resumable': '1.0.0', 'Upload-Length': '0',
                'Upload-Metadata': empty_encoded, 'X-Request-ID': str(uuid.uuid4())})
            assert empty.status_code == 201, empty.text
            result = client.post(folder + '/finish')
            assert result.status_code == 200, result.text
            tus_id = location.rsplit('/', 1)[-1]
            replay = {'Type': 'pre-finish', 'Event': {'Upload': {'ID': tus_id, 'Size': len(content),
                'Offset': len(content), 'MetaData': metadata}}}
            assert client.post(api + '/api/v1/first-step/internal/tusd-hooks', json=replay).status_code == 200
            assert result.json()['task_status'] == 'ready' and result.json()['discovered_groups'] == 1
            restored = tmp_path / 'data' / 'uploads' / upload / 'source' / Path(relative)
            assert restored.read_bytes() == content
            assert (tmp_path / 'data' / 'uploads' / upload / 'source' / Path(empty_path)).read_bytes() == b''
            assert client.get(api + f'/api/v1/first-step/tasks/{task}').json()['processed_groups'] == 0
    finally:
        process.terminate()
        process.wait(timeout=5)
        log.close()
        server.should_exit = True
        thread.join(timeout=5)
