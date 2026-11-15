from pathlib import Path
import json
import os
import stat
import sys
from contextlib import contextmanager
from types import SimpleNamespace
import pytest

from test_control_upload_api import make_client, upload_synthetic


def test_local_folder_counts_groups_without_copy_or_start(tmp_path):
    client, scheduler = make_client(tmp_path)
    source = tmp_path / 'original'
    (source / '甲').mkdir(parents=True)
    (source / '乙' / '附件').mkdir(parents=True)
    (source / '甲' / '正文.txt').write_text('甲正文', encoding='utf-8')
    (source / '乙' / '正文.txt').write_text('乙正文', encoding='utf-8')
    (source / '乙' / '附件' / '资料.txt').write_text('附件', encoding='utf-8')
    task = client.post('/api/v1/first-step/tasks', json={'name': '本地目录'}).json()['task_id']

    response = client.post(f'/api/v1/first-step/tasks/{task}/local-source', json={'path': str(source)})
    assert response.status_code == 200, response.text
    assert response.json()['total_groups'] == 2
    assert response.json()['task_status'] == 'ready'
    assert not scheduler.calls
    assert not (client.control.data / 'uploads').exists()
    with client.control.summary._connect() as db:
        groups = [Path(row['group_root']) for row in db.execute(
            'SELECT group_root FROM control_groups WHERE task_id=? ORDER BY seq', (task,))]
    assert set(groups) == {source / '甲', source / '乙'}
    assert (source / '甲' / '正文.txt').read_text(encoding='utf-8') == '甲正文'

    revision = response.json()['control_revision']
    start = client.post(f'/api/v1/first-step/tasks/{task}/start', json={'expected_revision': revision})
    assert start.status_code == 200
    assert scheduler.calls == [(task, 1)]


def test_single_folder_on_same_disk_produces_complete_body_and_attachment(tmp_path, monkeypatch):
    from paperflow_prefect import controlled_nimble
    from paperflow_prefect.controlled_first_step import run_first_step
    from paperflow_prefect.group_delivery import load_delivery

    class Judge:
        def __init__(self, *args, **kwargs):
            pass
        def choose_body(self, records):
            return {'choice': records[0]['file_id']}
        def choose_other(self, record, body):
            return {'choice': 'attachment'}
        def choose_filename_title(self, filename):
            return None

    monkeypatch.setattr(controlled_nimble, 'ControlledNimbleJudge', Judge)
    client, scheduler = make_client(tmp_path)
    source = tmp_path / 'single-person'
    source.mkdir()
    (source / 'paper.txt').write_text('题目：合成测试论文\n这是论文正文', encoding='utf-8')
    (source / 'data.csv').write_text('name,value\ntest,1\n', encoding='utf-8')
    task = client.post('/api/v1/first-step/tasks', json={'name': '单组'}).json()['task_id']
    selected = client.post(f'/api/v1/first-step/tasks/{task}/local-source', json={'path': str(source)})
    assert selected.status_code == 200 and selected.json()['total_groups'] == 1
    assert client.post(f'/api/v1/first-step/tasks/{task}/start',
                       json={'expected_revision': selected.json()['control_revision']}).status_code == 200
    run_first_step(client.control, task, scheduler.calls[-1][1], 'test-run', endpoint='http://unused')
    summary = client.get(f'/api/v1/first-step/tasks/{task}?filter=passed').json()
    assert summary['passed_groups'] == 1, summary
    delivery = Path(summary['items'][0]['delivery_dir'])
    _, package = load_delivery(delivery)
    assert package['manifest']['ready_for_receipt'] is True
    assert len(package['manifest']['files']) == 2
    assert len(package['manifest']['evidence_records']) == 1
    assert (source / 'paper.txt').exists() and (source / 'data.csv').exists()
    artifacts = client.control.output / 'tasks' / task
    assert artifacts.exists()
    result = client.delete(f'/api/v1/first-step/tasks/{task}')
    assert result.status_code == 200, result.text
    assert not artifacts.exists() and not delivery.exists()
    assert (source / 'paper.txt').exists() and (source / 'data.csv').exists()


def test_delete_local_task_removes_its_artifacts_and_keeps_originals_and_other_task(tmp_path):
    client, scheduler = make_client(tmp_path)
    source = tmp_path / 'original'
    source.mkdir()
    original = source / '正文.txt'
    original.write_text('原始资料', encoding='utf-8')
    task = client.post('/api/v1/first-step/tasks', json={'name': '删除目标'}).json()['task_id']
    other = client.post('/api/v1/first-step/tasks', json={'name': '其他任务'}).json()['task_id']
    assert client.post(f'/api/v1/first-step/tasks/{task}/local-source', json={'path': str(source)}).status_code == 200
    task_dir = client.control.output / 'tasks' / task
    (task_dir / 'deliveries').mkdir(parents=True)
    (task_dir / 'deliveries' / 'copy.txt').write_text('交付副本')
    (task_dir / '中间.json').write_text('中间产物')
    other_dir = client.control.output / 'tasks' / other
    other_dir.mkdir(parents=True)
    (other_dir / 'keep.txt').write_text('其他任务')

    result = client.delete(f'/api/v1/first-step/tasks/{task}')
    assert result.status_code == 200, result.text
    assert result.json()['deleted'] is True
    assert not task_dir.exists()
    assert original.read_text(encoding='utf-8') == '原始资料'
    assert (other_dir / 'keep.txt').exists()
    assert [row['task_id'] for row in client.get('/api/v1/first-step/tasks').json()['items']] == [other]
    assert client.get(f'/api/v1/first-step/tasks/{task}').status_code == 404
    assert not scheduler.calls


def test_delete_upload_task_removes_upload_copy_and_staging_blobs(tmp_path):
    client, _ = make_client(tmp_path)
    task = client.post('/api/v1/first-step/tasks', json={'name': '上传任务'}).json()['task_id']
    upload = client.post(f'/api/v1/first-step/tasks/{task}/uploads', json={'root_name': 'root'}).json()['upload_id']
    upload_synthetic(client, task, upload, [('root/person/paper.txt', b'paper')])
    assert client.post(f'/api/v1/first-step/uploads/{upload}/finish').status_code == 200
    with client.control.summary._connect() as db:
        tus_id = db.execute('SELECT tus_id FROM tus_manifest WHERE upload_id=?', (upload,)).fetchone()[0]
    blob = client.control.tus_dir / tus_id
    (client.control.tus_dir / (tus_id + '.info')).write_text('temporary')
    assert blob.exists()

    result = client.delete(f'/api/v1/first-step/tasks/{task}')
    assert result.status_code == 200, result.text
    assert not (client.control.data / 'uploads' / upload).exists()
    assert not blob.exists()
    assert not (client.control.tus_dir / (tus_id + '.info')).exists()


def test_incomplete_upload_can_be_deleted_after_active_transfer_finishes(tmp_path):
    client, _ = make_client(tmp_path)
    task = client.post('/api/v1/first-step/tasks', json={'name': '中断上传'}).json()['task_id']
    upload = client.post(f'/api/v1/first-step/tasks/{task}/uploads', json={'root_name': 'root'}).json()['upload_id']
    upload_synthetic(client, task, upload, [('root/person/paper.txt', b'paper')])
    control = client.control
    control.begin_upload_transfer(task, upload)
    assert client.delete(f'/api/v1/first-step/tasks/{task}').status_code == 409
    control.end_upload_transfer(task)
    assert client.delete(f'/api/v1/first-step/tasks/{task}').status_code == 200
    assert client.get(f'/api/v1/first-step/tasks/{task}').status_code == 404


def test_failed_delete_is_visible_and_can_be_retried(tmp_path, monkeypatch):
    from paperflow_prefect import first_step_delete
    client, _ = make_client(tmp_path)
    task = client.post('/api/v1/first-step/tasks', json={'name': '重试'}).json()['task_id']
    target = client.control.output / 'tasks' / task
    target.mkdir(parents=True)
    (target / 'intermediate.txt').write_text('fixture')
    original_remove = first_step_delete._remove_path
    monkeypatch.setattr(first_step_delete, '_remove_path', lambda *_: (_ for _ in ()).throw(PermissionError()))
    response = client.delete(f'/api/v1/first-step/tasks/{task}')
    assert response.status_code == 500
    assert client.get(f'/api/v1/first-step/tasks/{task}').json()['task_status'] == 'delete_failed'
    assert target.exists()
    monkeypatch.setattr(first_step_delete, '_remove_path', original_remove)
    assert client.delete(f'/api/v1/first-step/tasks/{task}').status_code == 200
    assert not target.exists()


def test_delete_removes_readonly_task_artifact(tmp_path):
    client, _ = make_client(tmp_path)
    task = client.post('/api/v1/first-step/tasks', json={'name': '只读产物'}).json()['task_id']
    target = client.control.output / 'tasks' / task
    target.mkdir(parents=True)
    artifact = target / 'readonly.txt'
    artifact.write_text('fixture')
    os.chmod(artifact, stat.S_IREAD)
    response = client.delete(f'/api/v1/first-step/tasks/{task}')
    assert response.status_code == 200, response.text
    assert not target.exists()


def test_office_conversion_cache_is_inside_task_and_deleted(tmp_path, monkeypatch):
    from paperflow_prefect import first_step_conversion, group_sort_cli, legacy
    client, _ = make_client(tmp_path)
    task = client.post('/api/v1/first-step/tasks', json={'name': '转换产物'}).json()['task_id']
    task_root = client.control.output / 'tasks' / task
    original = tmp_path / 'original.doc'
    original.write_bytes(b'original')
    monkeypatch.setattr(group_sort_cli, 'detect_format', lambda _: ('doc', 'fixture'))
    monkeypatch.setattr(group_sort_cli.Settings, 'load',
                        lambda: SimpleNamespace(path=lambda _: tmp_path / 'shared-data'))
    monkeypatch.setattr(legacy, 'bind', lambda _: None)
    monkeypatch.setattr(group_sort_cli, 'read_evidence',
                        lambda path, detected, convert_office: {'converted': str(convert_office(path))})
    class FakeConverter:
        def __init__(self, root, channel_root, **kwargs):
            self.cache = Path(root) / 'private' / 'material-conversion' / 'cache'
            self.cache.mkdir(parents=True)
            self.channel_root = Path(channel_root)
            self.channel_root.mkdir(parents=True, exist_ok=True)
            (self.channel_root / 'lo-1.lock').write_text('shared slot')
        def convert(self, source, target):
            result = self.cache / 'converted.docx'
            result.write_bytes(b'fixture')
            return result
    monkeypatch.setattr(first_step_conversion, 'FirstStepConverter', FakeConverter)
    group_sort_cli._read_for_sort(original, work_root=task_root / 'conversion')
    assert list(task_root.rglob('converted.docx'))
    assert client.delete(f'/api/v1/first-step/tasks/{task}').status_code == 200
    assert not task_root.exists() and original.exists()
    assert (tmp_path / 'shared-data' / 'control-locks' / 'first-step-office' / 'lo-1.lock').exists()


def test_cannot_delete_a_running_task(tmp_path):
    client, _ = make_client(tmp_path)
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'paper.txt').write_text('original')
    task = client.post('/api/v1/first-step/tasks', json={'name': '运行中'}).json()['task_id']
    selected = client.post(f'/api/v1/first-step/tasks/{task}/local-source', json={'path': str(source)}).json()
    assert client.post(f'/api/v1/first-step/tasks/{task}/start',
                       json={'expected_revision': selected['control_revision']}).status_code == 200
    assert client.delete(f'/api/v1/first-step/tasks/{task}').status_code == 409
    assert (source / 'paper.txt').exists()


def test_delete_keeps_task_with_unconfirmed_owned_converter(tmp_path):
    import json
    client, _ = make_client(tmp_path)
    first = client.post('/api/v1/first-step/tasks', json={'name': '转换未知'}).json()['task_id']
    second = client.post('/api/v1/first-step/tasks', json={'name': '其他任务'}).json()['task_id']
    task_root = client.control.output / 'tasks' / first
    task_root.mkdir(parents=True)
    (task_root / 'intermediate.txt').write_text('fixture')
    marker = client.control.data / 'control-locks' / 'first-step-office' / 'wps.uncertain'
    marker.parent.mkdir(parents=True)
    marker.write_text(json.dumps({'task_id': first, 'engine': 'wps', 'state': 'uncertain'}), encoding='utf8')
    assert client.delete(f'/api/v1/first-step/tasks/{first}').status_code == 409
    assert task_root.exists() and marker.exists()
    assert client.delete(f'/api/v1/first-step/tasks/{second}').status_code == 200


@pytest.mark.parametrize('owner', [None, '', 'not-a-task-id'])
def test_delete_does_not_guess_unknown_conversion_owner(tmp_path, owner):
    import json
    client, _ = make_client(tmp_path)
    task = client.post('/api/v1/first-step/tasks', json={'name': '归属未知'}).json()['task_id']
    marker = client.control.data / 'control-locks' / 'first-step-office' / 'wps.uncertain'
    marker.parent.mkdir(parents=True)
    marker.write_text(json.dumps({'task_id': owner, 'engine': 'wps', 'state': 'uncertain'}), encoding='utf8')
    assert client.delete(f'/api/v1/first-step/tasks/{task}').status_code == 409
    assert marker.exists()


def test_unknown_request_survives_delete_and_different_selection_root(tmp_path):
    from paperflow_prefect.first_step_control import UnknownRequest
    client, scheduler = make_client(tmp_path)
    source = tmp_path / 'source'
    group = source / 'class' / 'student'
    group.mkdir(parents=True)
    (group / 'paper.txt').write_text('original')
    control = client.control

    def begin(name, folder):
        task = client.post('/api/v1/first-step/tasks', json={'name': name}).json()['task_id']
        ready = client.post(f'/api/v1/first-step/tasks/{task}/local-source', json={'path': str(folder)}).json()
        assert client.post(f'/api/v1/first-step/tasks/{task}/start',
                           json={'expected_revision': ready['control_revision']}).status_code == 200
        seq, actual = control.claim_next(task)
        with control.summary._connect() as db:
            scope = control.group_scope(control._task(db, task), actual)
        return task, seq, scope, scheduler.calls[-1][1]

    first, seq, first_scope, generation = begin('首次', source)
    key = {'scope': first_scope, 'kind': 'body', 'members': ['same-paper']}
    def lost(_):
        raise TimeoutError('fixture response unknown')
    with pytest.raises(UnknownRequest):
        control.invoke_model(first, seq, 'body', 'snapshot', 'payload', lambda: object(), lost,
                             timeout=1, generation=generation, request_identity=key)
    control.mark_group_unknown(first, seq, generation, 'fixture unknown')
    deleted = client.delete(f'/api/v1/first-step/tasks/{first}')
    assert deleted.status_code == 200, deleted.text
    assert deleted.json()['unknown_history_retained'] == 1

    second, seq2, second_scope, generation2 = begin('再次选择上级', source / 'class')
    assert first_scope == second_scope
    second_key = {'scope': second_scope, 'kind': 'body', 'members': ['same-paper']}
    sent = []
    with pytest.raises(UnknownRequest):
        control.invoke_model(second, seq2, 'body', 'new', 'payload', lambda: sent.append(True),
                             lambda _: {}, timeout=1, generation=generation2, request_identity=second_key)
    assert sent == [] and (group / 'paper.txt').exists()


def test_unknown_upload_request_blocks_same_group_direct_and_after_deletion(tmp_path):
    from paperflow_prefect.first_step_control import UnknownRequest
    client, scheduler = make_client(tmp_path)
    source = tmp_path / 'root'
    group = source / 'person'
    group.mkdir(parents=True)
    (group / 'paper.txt').write_bytes(b'paper')
    control = client.control
    first = client.post('/api/v1/first-step/tasks', json={'name': '上传'}).json()['task_id']
    upload = client.post(f'/api/v1/first-step/tasks/{first}/uploads', json={'root_name': 'root'}).json()['upload_id']
    upload_synthetic(client, first, upload, [('root/person/paper.txt', b'paper')])
    ready = client.post(f'/api/v1/first-step/uploads/{upload}/finish').json()
    assert client.post(f'/api/v1/first-step/tasks/{first}/start',
                       json={'expected_revision': ready['control_revision']}).status_code == 200
    seq, actual = control.claim_next(first)
    with control.summary._connect() as db:
        upload_scope = control.group_scope(control._task(db, first), actual)
    first_key = {'scope': upload_scope, 'kind': 'body', 'members': ['same-paper']}
    generation = scheduler.calls[-1][1]
    def lost(_):
        raise TimeoutError('unknown fixture')
    with pytest.raises(UnknownRequest):
        control.invoke_model(first, seq, 'body', 'snapshot', 'payload', lambda: object(), lost,
                             timeout=1, generation=generation, request_identity=first_key)
    control.mark_group_unknown(first, seq, generation, 'fixture')

    def attempt(label):
        task = client.post('/api/v1/first-step/tasks', json={'name': label}).json()['task_id']
        ready = client.post(f'/api/v1/first-step/tasks/{task}/local-source', json={'path': str(source)}).json()
        assert client.post(f'/api/v1/first-step/tasks/{task}/start',
                           json={'expected_revision': ready['control_revision']}).status_code == 200
        seq2, actual2 = control.claim_next(task)
        with control.summary._connect() as db:
            local_scope = control.group_scope(control._task(db, task), actual2)
        key = {'scope': local_scope, 'kind': 'body', 'members': ['same-paper']}
        sent = []
        with pytest.raises(UnknownRequest):
            control.invoke_model(task, seq2, 'body', 'next', 'payload', lambda: sent.append(True),
                                 lambda _: {}, timeout=1, generation=scheduler.calls[-1][1], request_identity=key)
        assert not sent
    attempt('本机直读')
    deleted = client.delete(f'/api/v1/first-step/tasks/{first}')
    assert deleted.status_code == 200, deleted.text
    attempt('删除后直读')


def test_scope_matching_requires_whole_path_components():
    from paperflow_prefect.first_step_control import _same_request_scope
    assert _same_request_scope('root/person', 'local:c:/資料/root/person')
    assert not _same_request_scope('root/person', 'local:c:/資料/otherroot/person')
    assert not _same_request_scope('local:c:/a/root/person', 'local:c:/b/root/person')


def test_shared_legacy_group_is_not_removed_for_other_task(tmp_path):
    client, _ = make_client(tmp_path)
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'paper.txt').write_text('original')
    first = client.post('/api/v1/first-step/tasks', json={'name': '旧任务'}).json()['task_id']
    second = client.post('/api/v1/first-step/tasks', json={'name': '其他任务'}).json()['task_id']
    for task in (first, second):
        assert client.post(f'/api/v1/first-step/tasks/{task}/local-source', json={'path': str(source)}).status_code == 200
    with client.control.summary._connect() as db:
        db.execute('UPDATE tasks SET artifact_scoped=0 WHERE task_id=?', (first,))
        group_id = db.execute('SELECT group_id FROM control_groups WHERE task_id=?', (first,)).fetchone()[0]
        db.commit()
    shared = client.control.output / 'deliveries' / group_id
    shared.mkdir(parents=True)
    (shared / 'keep.txt').write_text('shared')
    result = client.delete(f'/api/v1/first-step/tasks/{first}')
    assert result.status_code == 200 and result.json()['shared_groups_retained'] == 1
    assert (shared / 'keep.txt').exists()
    assert client.get(f'/api/v1/first-step/tasks/{second}').status_code == 200


def test_delete_legacy_task_cleans_handoff_delivery_and_attachment_evidence(tmp_path, monkeypatch):
    from paperflow_prefect import controlled_nimble
    from paperflow_prefect.controlled_first_step import run_first_step

    class Judge:
        def __init__(self, *args, **kwargs):
            pass
        def choose_body(self, records):
            return {'choice': records[0]['file_id']}
        def choose_other(self, record, body):
            return {'choice': 'attachment'}
        def choose_filename_title(self, filename):
            return None

    monkeypatch.setattr(controlled_nimble, 'ControlledNimbleJudge', Judge)
    client, scheduler = make_client(tmp_path)
    source = tmp_path / 'original'
    source.mkdir()
    (source / 'paper.txt').write_text('题目：合成论文\n正文', encoding='utf-8')
    (source / 'data.csv').write_text('x,y\n1,2', encoding='utf-8')
    task = client.post('/api/v1/first-step/tasks', json={'name': '旧任务'}).json()['task_id']
    with client.control.summary._connect() as db:
        db.execute('UPDATE tasks SET artifact_scoped=0 WHERE task_id=?', (task,))
        db.commit()
    selected = client.post(f'/api/v1/first-step/tasks/{task}/local-source', json={'path': str(source)}).json()
    assert client.post(f'/api/v1/first-step/tasks/{task}/start',
                       json={'expected_revision': selected['control_revision']}).status_code == 200
    run_first_step(client.control, task, scheduler.calls[-1][1], 'fixture-run', endpoint='http://unused')
    result = client.get(f'/api/v1/first-step/tasks/{task}?filter=passed').json()
    assert result['passed_groups'] == 1, result
    output = client.control.output
    cache = tmp_path / 'legacy-conversion' / 'cache'
    client.control.legacy_conversion_root = cache
    owned_cache = cache / 'libreoffice' / ('a' * 64)
    unrelated_cache = cache / 'libreoffice' / ('b' * 64)
    owned_cache.mkdir(parents=True)
    unrelated_cache.mkdir(parents=True)
    from paperflow_prefect.records import sha_file
    (owned_cache / 'identity.json').write_text(json.dumps({'source_sha256': sha_file(source / 'paper.txt')}))
    (owned_cache / 'content.docx').write_bytes(b'converted')
    (unrelated_cache / 'identity.json').write_text(json.dumps({'source_sha256': 'f' * 64}))
    assert list(output.glob('*.json'))
    assert list((output / 'attachment-evidence').rglob('*.json'))
    assert list((output / 'deliveries').rglob('delivery.json'))
    response = client.delete(f'/api/v1/first-step/tasks/{task}')
    assert response.status_code == 200, response.text
    assert not list(output.glob('*.json'))
    assert not list((output / 'attachment-evidence').rglob('*.json'))
    assert not list((output / 'deliveries').rglob('delivery.json'))
    assert response.json()['unassigned_conversion_caches_retained'] == 1
    assert not owned_cache.exists() and unrelated_cache.exists()
    assert (source / 'paper.txt').exists() and (source / 'data.csv').exists()
