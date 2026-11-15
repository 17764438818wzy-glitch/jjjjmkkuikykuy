import json
import hashlib

from fastapi import FastAPI
from fastapi.testclient import TestClient

from paperflow_prefect.first_step_control_api import install_control_routes
from paperflow_prefect.group_task_summary import install_group_summary_routes


class Scheduler:
    def __init__(self): self.calls = []
    def start(self, task_id, generation):
        self.calls.append((task_id, generation))
        return {'flow_run_id': 'synthetic'}


def setup(tmp_path):
    app = FastAPI()
    scheduler = Scheduler()
    control = install_control_routes(app, tmp_path / 'output', tmp_path / 'data', scheduler=scheduler)
    install_group_summary_routes(app, tmp_path / 'output', refresh_task=control.refresh_task)
    return TestClient(app), control, scheduler


def test_task_creation_request_id_can_be_replayed_after_uncertain_response(tmp_path):
    client, _, _ = setup(tmp_path)
    request_id = '11111111-1111-4111-8111-111111111111'
    first = client.post('/api/v1/first-step/tasks', json={'name': '同一任务', 'request_id': request_id}).json()
    replay = client.post('/api/v1/first-step/tasks', json={'name': '同一任务', 'request_id': request_id}).json()
    assert first['task_id'] == replay['task_id']
    lookup = client.get('/api/v1/first-step/tasks/by-request/' + request_id)
    assert lookup.status_code == 200 and lookup.json()['task_id'] == first['task_id']
    conflict = client.post('/api/v1/first-step/tasks', json={'name': '另一个任务', 'request_id': request_id})
    assert conflict.status_code == 409


def test_status_poll_detects_dead_worker_and_reports_unknown_request(tmp_path):
    from contextlib import closing
    from test_control_admission import ready_control
    control, scheduler, task = ready_control(tmp_path, groups=1)
    assert control.begin_run(task, scheduler.calls[-1][1], 'synthetic-run')
    seq, _ = control.claim_next(task)
    with closing(control.summary._connect()) as db, db:
        db.execute("UPDATE control_groups SET worker_pid=2147483647,worker_started=1 WHERE task_id=? AND seq=?", (task, seq))
        db.execute("UPDATE tasks SET worker_owner_pid=2147483647,worker_owner_started=1 WHERE task_id=?", (task,))
        db.execute("INSERT INTO model_decisions (task_id,seq,step,snapshot_id,payload_hash,state) "
                   "VALUES (?,?,?,?,?,'inflight')", (task, seq, 'body', 'snapshot', 'payload'))
    app = FastAPI()
    install_group_summary_routes(app, tmp_path / 'output', refresh_task=control.refresh_task)
    status = TestClient(app).get(f'/api/v1/first-step/tasks/{task}').json()
    assert status['task_status'] == 'paused'
    assert status['processed_groups'] == 0
    assert status['unknown_requests'] == 1
    assert status['unknown_groups'] == 1
    assert status['unknown_details'][0]['reason']


def test_tus_manifest_restores_directory_only_after_verified_upload(tmp_path, monkeypatch):
    client, control, scheduler = setup(tmp_path)
    from paperflow_prefect import first_step_tusd
    original_copy = first_step_tusd.shutil.copyfileobj
    def inspect_copy(incoming, outgoing, **kwargs):
        assert 'source' not in __import__('pathlib').Path(outgoing.name).parts
        return original_copy(incoming, outgoing, **kwargs)
    monkeypatch.setattr(first_step_tusd.shutil, 'copyfileobj', inspect_copy)
    task = client.post('/api/v1/first-step/tasks', json={'name': '测试'}).json()['task_id']
    upload = client.post(f'/api/v1/first-step/tasks/{task}/uploads', json={'root_name': '原始文件'}).json()['upload_id']
    manifest_url = f'/api/v1/first-step/uploads/{upload}/manifest'
    files = [{'relative_path': '原始文件/一班/甲/论文.txt', 'size': 5},
             {'relative_path': '原始文件/一班/乙/论文.txt', 'size': 4}]
    assert client.post(manifest_url, json={'files': files}).status_code == 200
    assert client.post(f'/api/v1/first-step/uploads/{upload}/seal', json={'expected_files': 2}).json()['expected_files'] == 2
    assert client.post(f'/api/v1/first-step/uploads/{upload}/finish').status_code == 409
    uploads = {}
    for entry, content in zip(files, [b'alpha', b'beta']):
        meta = {'task_id': task, 'folder_upload_id': upload, 'relativePath': entry['relative_path']}
        request = {'Type': 'pre-create', 'Event': {'Upload': {
            'Size': entry['size'], 'SizeIsDeferred': False, 'MetaData': meta},
            'HTTPRequest': {'Header': {'X-Request-Id': [entry['relative_path']]}}}}
        response = client.post('/api/v1/first-step/internal/tusd-hooks', json=request)
        assert response.status_code == 200 and not response.json().get('RejectUpload')
        tus_id = response.json()['ChangeFileInfo']['ID']
        assert tus_id not in uploads
        uploads[tus_id] = {'offset': len(content), 'length': len(content), 'metadata': meta}
        control.tus_dir.mkdir(parents=True, exist_ok=True)
        (control.tus_dir / tus_id).write_bytes(content)
        (control.tus_dir / (tus_id + '.info')).write_text(json.dumps({'ID': tus_id}), encoding='utf-8')
    control._tus_head = lambda tus_id: uploads[tus_id]
    result = client.post(f'/api/v1/first-step/uploads/{upload}/finish')
    assert result.status_code == 200, result.text
    assert result.json()['task_id'] == task and result.json()['discovered_groups'] == 2
    assert result.json()['task_status'] == 'ready' and result.json()['total_groups'] == 2
    source = tmp_path / 'data' / 'uploads' / upload / 'source'
    assert (source / '原始文件/一班/甲/论文.txt').read_bytes() == b'alpha'
    assert (source / '原始文件/一班/乙/论文.txt').read_bytes() == b'beta'
    assert scheduler.calls == []


def test_tus_rejects_unknown_task_and_windows_path_collisions(tmp_path):
    client, control, _ = setup(tmp_path)
    task = client.post('/api/v1/first-step/tasks', json={'name': '测试'}).json()['task_id']
    upload = client.post(f'/api/v1/first-step/tasks/{task}/uploads', json={'root_name': 'root'}).json()['upload_id']
    url = f'/api/v1/first-step/uploads/{upload}/manifest'
    assert client.post(url, json={'files': [{'relative_path': 'root/../x', 'size': 1}]}).status_code == 400
    assert client.post(url, json={'files': [{'relative_path': 'root/A/doc.txt', 'size': 1},
                                             {'relative_path': 'root/a/DOC.txt', 'size': 1}]}).status_code == 409
    assert client.post(url, json={'files': [{'relative_path': 'root/A/doc.txt', 'size': 1}]}).status_code == 200
    client.post(f'/api/v1/first-step/uploads/{upload}/seal', json={'expected_files': 1})
    hook = {'Type': 'pre-create', 'Event': {'Upload': {'Size': 1, 'SizeIsDeferred': False,
             'MetaData': {'task_id': 'not-a-task', 'folder_upload_id': upload,
                          'relativePath': 'root/A/doc.txt'}}}}
    response = client.post('/api/v1/first-step/internal/tusd-hooks', json=hook)
    assert response.status_code == 200 and response.json()['RejectUpload'] is True
    assert client.post(f'/api/v1/first-step/uploads/{upload}/finish').status_code == 409


def test_upload_creation_and_manifest_replay_after_response_loss(tmp_path):
    client, _, _ = setup(tmp_path)
    task = client.post('/api/v1/first-step/tasks', json={'name': '重试'}).json()['task_id']
    url = f'/api/v1/first-step/tasks/{task}/uploads'
    first = client.post(url, json={'root_name': 'root'}).json()
    replay = client.post(url, json={'root_name': 'root'})
    assert replay.status_code == 200 and replay.json()['upload_id'] == first['upload_id']
    assert client.get(f'/api/v1/first-step/tasks/{task}').json()['upload_id'] == first['upload_id']
    assert client.post(url, json={'root_name': 'different'}).status_code == 409
    manifest = f"/api/v1/first-step/uploads/{first['upload_id']}/manifest"
    data = {'files': [{'relative_path': 'root/person/file.txt', 'size': 1}]}
    assert client.post(manifest, json=data).status_code == 200
    assert client.post(manifest, json=data).status_code == 200
    seal = f"/api/v1/first-step/uploads/{first['upload_id']}/seal"
    assert client.post(seal, json={'expected_files': 2}).status_code == 409
    assert client.post(seal, json={'expected_files': 1}).status_code == 200
    assert client.post(seal, json={'expected_files': 1}).status_code == 200
    assert client.post(seal, json={'expected_files': 2}).status_code == 409
    assert client.post(manifest, json=data).status_code == 200
    assert client.post(manifest, json={'files': [{'relative_path': 'root/person/new', 'size': 1}]}).status_code == 409


def test_resume_info_uses_the_received_prefix_instead_of_name_or_size(tmp_path):
    client, control, _ = setup(tmp_path)
    task = client.post('/api/v1/first-step/tasks', json={'name': '续传'}).json()['task_id']
    upload = client.post(f'/api/v1/first-step/tasks/{task}/uploads', json={'root_name': 'root'}).json()['upload_id']
    relative = 'root/person/paper.txt'
    client.post(f'/api/v1/first-step/uploads/{upload}/manifest',
                json={'files': [{'relative_path': relative, 'size': 5}]})
    client.post(f'/api/v1/first-step/uploads/{upload}/seal', json={'expected_files': 1})
    meta = {'task_id': task, 'folder_upload_id': upload, 'relativePath': relative}
    hook = {'Type': 'pre-create', 'Event': {'Upload': {'Size': 5, 'MetaData': meta},
            'HTTPRequest': {'Header': {'X-Request-Id': ['synthetic-create']}}}}
    tus_id = client.post('/api/v1/first-step/internal/tusd-hooks', json=hook).json()['ChangeFileInfo']['ID']
    control.tus_dir.mkdir(parents=True, exist_ok=True)
    (control.tus_dir / tus_id).write_bytes(b'abc')
    control._tus_head = lambda _: {'offset': 3, 'length': 5, 'metadata': meta}
    response = client.get(f'/api/v1/first-step/uploads/{upload}/resume-info', params={'relative_path': relative})
    assert response.status_code == 200, response.text
    assert response.json()['offset'] == 3
    assert response.json()['prefix_block_sha256'] == [hashlib.sha256(b'abc').hexdigest()]
    assert response.json()['tus_url'].endswith(tus_id)
    (control.tus_dir / tus_id).write_bytes(b'abcd')
    assert client.get(f'/api/v1/first-step/uploads/{upload}/resume-info',
                      params={'relative_path': relative}).status_code == 409


def test_confirmed_failed_create_releases_only_the_absent_upload(tmp_path, monkeypatch):
    from urllib.error import HTTPError
    from paperflow_prefect import first_step_tusd
    client, control, _ = setup(tmp_path)
    control.tus_gateway_enabled = True
    task = client.post('/api/v1/first-step/tasks', json={'name': '合成失败'}).json()['task_id']
    upload = client.post(f'/api/v1/first-step/tasks/{task}/uploads', json={'root_name': 'root'}).json()['upload_id']
    relative = 'root/person/file.txt'
    client.post(f'/api/v1/first-step/uploads/{upload}/manifest', json={'files': [{'relative_path': relative, 'size': 1}]})
    client.post(f'/api/v1/first-step/uploads/{upload}/seal', json={'expected_files': 1})
    import uuid
    writer, invocation = str(uuid.uuid4()), uuid.uuid4().hex
    meta = {'task_id': task, 'folder_upload_id': upload, 'relativePath': relative}
    headers = {'X-Task-ID': [task], 'X-Folder-Upload-ID': [upload], 'X-Writer-ID': [writer],
               'X-Gateway-Invocation': [invocation], 'X-Request-ID': ['synthetic']}
    hook = {'Type': 'pre-create', 'Event': {'Upload': {'Size': 1, 'MetaData': meta},
            'HTTPRequest': {'Header': headers}}}
    tus_id = control.tus.hook(hook)['ChangeFileInfo']['ID']
    def not_found(*args, **kwargs):
        raise HTTPError('http://tusd', 404, 'absent', None, None)
    monkeypatch.setattr(first_step_tusd, 'urlopen', not_found)
    control.tus_dir.mkdir(parents=True, exist_ok=True)
    (control.tus_dir / tus_id).write_bytes(b'partial')
    assert control.tus.release_failed_create(invocation) is False
    (control.tus_dir / tus_id).write_bytes(b'')
    assert control.tus.release_failed_create(invocation) is True
    saved = control.data / 'uploads' / upload / 'failed-create' / tus_id
    assert saved.is_file() and saved.stat().st_size == 0
    assert control.tus.hook(hook).get('RejectUpload') is True
    headers['X-Gateway-Invocation'] = [uuid.uuid4().hex]
    assert 'ChangeFileInfo' in control.tus.hook(hook)
