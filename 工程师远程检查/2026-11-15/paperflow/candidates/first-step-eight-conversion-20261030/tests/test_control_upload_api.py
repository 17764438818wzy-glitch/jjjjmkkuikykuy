from fastapi import FastAPI
from fastapi.testclient import TestClient
import threading
import uuid

from paperflow_prefect.group_task_summary import install_group_summary_routes
from paperflow_prefect.first_step_control_api import install_control_routes


class FakeScheduler:
    def __init__(self):
        self.calls = []

    def start(self, task_id, generation):
        self.calls.append((task_id, generation))
        return {'flow_run_id': 'synthetic-flow-run'}


def make_client(tmp_path):
    app = FastAPI()
    scheduler = FakeScheduler()
    install_group_summary_routes(app, tmp_path / 'output')
    control = install_control_routes(app, tmp_path / 'output', tmp_path / 'data', scheduler=scheduler)
    client = TestClient(app)
    client.control = control
    return client, scheduler


def upload_synthetic(client, task, upload, files):
    ctl = client.control
    entries = [{'relative_path': path, 'size': len(data)} for path, data in files]
    assert client.post(f'/api/v1/first-step/uploads/{upload}/manifest', json={'files': entries}).status_code == 200
    assert client.post(f'/api/v1/first-step/uploads/{upload}/seal', json={'expected_files': len(entries)}).status_code == 200
    ctl.tus_dir.mkdir(parents=True, exist_ok=True)
    statuses = {}
    for path, data in files:
        meta = {'task_id': task, 'folder_upload_id': upload, 'relativePath': path}
        hook = {'Type': 'pre-create', 'Event': {'Upload': {'Size': len(data), 'MetaData': meta},
                'HTTPRequest': {'Header': {'X-Request-Id': [str(uuid.uuid4())]}}}}
        response = client.post('/api/v1/first-step/internal/tusd-hooks', json=hook)
        tus_id = response.json()['ChangeFileInfo']['ID']
        (ctl.tus_dir / tus_id).write_bytes(data)
        statuses[tus_id] = {'offset': len(data), 'length': len(data), 'metadata': meta}
    ctl._tus_head = lambda tus_id: statuses[tus_id]


def test_upload_saves_relative_paths_without_starting_and_exposes_provisional_groups(tmp_path):
    client, scheduler = make_client(tmp_path)
    task_response = client.post('/api/v1/first-step/tasks', json={'name': '我的任务'})
    assert task_response.status_code == 200
    task_id = task_response.json()['task_id']
    assert task_response.json()['task_status'] == 'draft'
    response = client.post(f'/api/v1/first-step/tasks/{task_id}/uploads', json={'root_name': '原始文件'})
    assert response.status_code == 200
    upload_id = response.json()['upload_id']
    upload_synthetic(client, task_id, upload_id, [('原始文件/一班/甲/a.txt', b'alpha'),
        ('原始文件/一班/乙/b.txt', b'beta'), ('原始文件/loose.txt', b'loose')])
    response = client.post(f'/api/v1/first-step/uploads/{upload_id}/finish', json={})
    assert response.status_code == 200, response.text
    data = response.json()
    assert data['task_id'] == task_id and data['task_status'] == 'ready' and data['discovered_groups'] == 2
    assert data['total_groups'] is None and data['ungrouped_files'] == ['原始文件/loose.txt']
    assert not scheduler.calls
    assert (tmp_path / 'data' / 'uploads' / upload_id / 'source' / '原始文件' / '一班' / '甲' / 'a.txt').read_bytes() == b'alpha'
    summary = client.get('/api/v1/first-step/tasks/current').json()
    assert summary['task_id'] == data['task_id'] and summary['discovered_groups'] == 2
    assert summary['total_groups'] is None and summary['processed_groups'] == 0


def test_one_selected_group_folder_and_known_group_total(tmp_path):
    client, _ = make_client(tmp_path)
    task = client.post('/api/v1/first-step/tasks', json={'name': '单组'}).json()['task_id']
    upload = client.post(f'/api/v1/first-step/tasks/{task}/uploads', json={'root_name': '张三'}).json()['upload_id']
    upload_synthetic(client, task, upload, [('张三/论文.txt', b'body'), ('张三/附件/data.csv', b'1,2')])
    result = client.post(f'/api/v1/first-step/uploads/{upload}/finish').json()
    assert result['task_status'] == 'ready'
    assert result['discovered_groups'] == result['total_groups'] == 1
    assert result['ungrouped_files'] == []
    summary = client.get(f'/api/v1/first-step/tasks/{task}').json()
    assert summary['total_groups'] == 1 and summary['processed_groups'] == 0


def test_explicit_start_and_pause_control_task(tmp_path):
    client, scheduler = make_client(tmp_path)
    task = client.post('/api/v1/first-step/tasks', json={'name': '控制任务'}).json()['task_id']
    upload = client.post(f'/api/v1/first-step/tasks/{task}/uploads', json={'root_name': 'root'}).json()['upload_id']
    upload_synthetic(client, task, upload, [('root/person/paper.txt', b'text')])
    assert client.post(f'/api/v1/first-step/uploads/{upload}/finish', json={}).json()['task_id'] == task
    assert scheduler.calls == []
    revision = client.get(f'/api/v1/first-step/tasks/{task}').json()['control_revision']
    result = client.post(f'/api/v1/first-step/tasks/{task}/start', json={'expected_revision': revision})
    assert result.status_code == 200 and result.json()['task_status'] == 'starting'
    assert len(scheduler.calls) == 1
    result = client.post(f'/api/v1/first-step/tasks/{task}/pause', json={})
    assert result.status_code == 200 and result.json()['task_status'] == 'paused'
    assert client.get(f'/api/v1/first-step/tasks/{task}').json()['task_status'] == 'paused'
    assert len(scheduler.calls) == 1


def test_upload_rejects_traversal_and_duplicate_without_overwrite(tmp_path):
    client, _ = make_client(tmp_path)
    task = client.post('/api/v1/first-step/tasks', json={'name': '上传任务'}).json()['task_id']
    upload = client.post(f'/api/v1/first-step/tasks/{task}/uploads', json={'root_name': 'root'}).json()['upload_id']
    url = f'/api/v1/first-step/uploads/{upload}/manifest'
    bad = client.post(url, json={'files': [{'relative_path': 'root/../outside', 'size': 3}]})
    assert bad.status_code == 400
    good = client.post(url, json={'files': [{'relative_path': 'root/person/x', 'size': 5}]})
    assert good.status_code == 200
    duplicate = client.post(url, json={'files': [{'relative_path': 'root/person/x', 'size': 6}]})
    assert duplicate.status_code == 409


def test_history_and_save_survive_restart_without_starting_or_resuming(tmp_path):
    client, scheduler = make_client(tmp_path)
    first = client.post('/api/v1/first-step/tasks', json={'name': '甲任务'}).json()['task_id']
    second = client.post('/api/v1/first-step/tasks', json={'name': '乙任务'}).json()['task_id']
    saved = client.post(f'/api/v1/first-step/tasks/{first}/save', json={'name': '重命名甲'}).json()
    assert saved['task_status'] == 'draft' and saved['name'] == '重命名甲' and saved['saved_at']
    assert not scheduler.calls
    restarted, restarted_scheduler = make_client(tmp_path)
    result = restarted.get('/api/v1/first-step/tasks?page=1&page_size=1').json()
    assert result['total'] == 2 and len(result['items']) == 1
    all_tasks = result['items'] + restarted.get('/api/v1/first-step/tasks?page=2&page_size=1').json()['items']
    by_id = {row['task_id']: row for row in all_tasks}
    assert by_id[first]['name'] == '重命名甲' and by_id[first]['saved_at'] == saved['saved_at']
    assert by_id[second]['name'] == '乙任务' and by_id[second]['task_status'] == 'draft'
    assert restarted.get(f'/api/v1/first-step/tasks/{first}').json()['task_status'] == 'draft'
    assert restarted_scheduler.calls == []


def test_pause_during_start_wins_and_stale_start_cannot_resume(tmp_path):
    class SlowScheduler(FakeScheduler):
        def __init__(self):
            super().__init__()
            self.entered, self.release = threading.Event(), threading.Event()

        def start(self, task_id, generation):
            self.calls.append((task_id, generation))
            self.entered.set()
            assert self.release.wait(5)
            return {'flow_run_id': 'synthetic-flow-run'}

    scheduler = SlowScheduler()
    app = FastAPI()
    install_group_summary_routes(app, tmp_path / 'output')
    control = install_control_routes(app, tmp_path / 'output', tmp_path / 'data', scheduler=scheduler)
    client = TestClient(app)
    client.control = control
    task = client.post('/api/v1/first-step/tasks', json={'name': '竞态任务'}).json()['task_id']
    upload = client.post(f'/api/v1/first-step/tasks/{task}/uploads', json={'root_name': 'root'}).json()['upload_id']
    upload_synthetic(client, task, upload, [('root/person/paper.txt', b'text')])
    ready = client.post(f'/api/v1/first-step/uploads/{upload}/finish', json={}).json()
    revision = ready['control_revision']
    replies = []
    thread = threading.Thread(target=lambda: replies.append(client.post(
        f'/api/v1/first-step/tasks/{task}/start', json={'expected_revision': revision})))
    thread.start()
    assert scheduler.entered.wait(5)
    paused = client.post(f'/api/v1/first-step/tasks/{task}/pause', json={}).json()
    assert paused['task_status'] == 'pausing'
    scheduler.release.set()
    thread.join(timeout=5)
    assert not thread.is_alive() and replies[0].status_code == 200
    assert replies[0].json()['task_status'] == 'paused'
    state = client.get(f'/api/v1/first-step/tasks/{task}').json()
    assert state['task_status'] == 'paused' and state['control_revision'] >= paused['control_revision']
    stale = client.post(f'/api/v1/first-step/tasks/{task}/start', json={'expected_revision': revision})
    assert stale.status_code == 409 and len(scheduler.calls) == 1
