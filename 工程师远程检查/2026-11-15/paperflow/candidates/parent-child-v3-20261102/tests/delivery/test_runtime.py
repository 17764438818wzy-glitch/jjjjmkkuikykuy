import copy
from contextlib import nullcontext
from pathlib import Path
import shutil
from datetime import datetime, timezone

import pytest

from paperflow_prefect.records import sha_file
from paperflow_prefect.settings import Settings
from test_attachment_ingest import handoff as attachment_handoff
from test_attachment_publish import Counter, FakeStore


@pytest.mark.parametrize('completion_failure', [None, 'rag_complete', 'nas', 'cleanup'])
def test_real_delivery_entry_orchestrates_one_synthetic_paper(tmp_path, monkeypatch, completion_failure):
    import paperflow_v3.delivery.runtime as runtime
    import paperflow.package as native_package
    import preextract_pipeline

    source, copies = tmp_path / 'source', tmp_path / 'data/first-step/task-1/group-1/copies'
    source.mkdir()
    copies.mkdir(parents=True)
    data = tmp_path / 'data'
    work = data / 'paperflow-v3' / 'task-1' / 'group-1'
    work.mkdir(parents=True)
    first_step = data / 'first-step' / 'task-1'
    group = first_step / 'group-1'
    receipt_file = group / 'registration' / 'receipts' / 'receipt-1.json'
    receipt_file.parent.mkdir(parents=True)
    receipt_file.write_text('{}', encoding='utf8')
    database = group / 'registration' / 'receipts.sqlite3'
    database.write_bytes(b'retain database')
    nas = tmp_path / 'nas'
    nas.mkdir()
    item = attachment_handoff(source)
    card = work / 'attachment.md'
    shutil.move(item['md_path'], card)
    item['md_path'] = str(card)
    (source / 'body.docx').write_bytes(b'synthetic body')
    (source / 'trash.txt').write_bytes(b'keep trash')
    shutil.copyfile(source / 'body.docx', copies / 'body.docx')
    shutil.copyfile(item['source_path'], copies / 'attachment.txt')
    paper = {'paper_id': item['paper_id'], 'group_id': 'group-1', 'person_dir': str(source),
             'excluded': ['trash.txt'], 'body': {'file_id': 'body', 'sha256': 'b' * 64},
             'attachments': [{'file_id': item['attachment_id'], 'sha256': item['source_sha256']}],
             'archive_files': [
                 {'file_id': 'body', 'kind': 'body', 'source_path': str(source / 'body.docx'),
                  'path': str(copies / 'body.docx'), 'relative': '正文/body.docx'},
                 {'file_id': item['attachment_id'], 'kind': 'attachment', 'source_path': item['source_path'],
                  'path': str(copies / 'attachment.txt'), 'relative': '附件/attachment.txt'}]}
    identity = {'task_id': 'task-1', 'group_id': 'group-1', 'paper_id': paper['paper_id'],
                'receipt_id': 'receipt-1', 'attempt_id': 'a', 'parent_run_id': 'parent', 'task_run_id': 'child'}
    registered = {**{key: identity[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id')},
                  'status': 'passed', 'delivery_complete': True, 'registration_status': 'registered',
                  'receipt_path': str(receipt_file), 'receipt_sha256': sha_file(receipt_file), 'paper': paper}
    handoff = {'schema': 'paperflow-postprocessing-receipt/1', 'identity': copy.deepcopy(identity),
               'validation_status': 'passed', 'body': {'package_path': str(work / 'publication'),
                                                        'manifest_sha256': 'd' * 64}, 'attachments': [item]}
    routes = [{'code': '081301', 'name': '化学工程与工艺', 'paper_dataset_id': 'body-1', 'image_dataset_id': 'ds-main'},
              {'code': '080206', 'name': '过程装备与控制工程', 'paper_dataset_id': 'body-2', 'image_dataset_id': 'ds-ref'}]
    packet = {'routes': routes, 'manifest_sha256': 'd' * 64,
              'manifest': {'files': [{'path': 'profile.json', 'sha256': 'c' * 64}]}, 'root': str(work),
              'profile': {'article': {'title': 'Synthetic paper'},
                          'routing_candidates': [{'code': row['code']} for row in routes]},
              'tree': {'nodes': []}, 'chunks': [{'id': 'c1', 'content': 'Synthetic text.', 'kind': 'body'}],
              'assets': []}
    task = {'task_id': 'task-1', 'source_sha256': 'b' * 64, 'revision': 1}
    validation = {'embedding': {'admission_verified': True}}
    catalog = tmp_path / 'catalog.json'
    catalog.write_text(__import__('json').dumps({'majors': routes}), encoding='utf8')
    settings = Settings({'data_root': str(data), 'catalog_path': str(catalog), 'nas_destination': str(nas),
                         'release_root': str(tmp_path / 'release')})
    parent = [{'task_id': 'task-1', 'folder_id': 'group-1', 'kind': 'registration', 'data': registered}]
    child = [{'task_id': 'task-1', 'folder_id': 'group-1', 'kind': 'postprocessing_complete', 'data': handoff}]
    control = {'running': True, 'pause_after_first_delete': True, 'completion_failed': False}

    def read(run_id):
        return copy.deepcopy(parent if run_id == 'task-1' else child)

    def save(*, task_id, folder_id, child_flow_run_id, event_id, kind, data):
        if kind == completion_failure and not control['completion_failed']:
            control['completion_failed'] = True
            raise OSError('synthetic final save failure')
        child.insert(0, {'task_id': task_id, 'folder_id': folder_id, 'kind': kind,
                         'event_id': event_id, 'data': copy.deepcopy(data), 'folder_completion': True,
                         'recorded_at': datetime.now(timezone.utc).isoformat()})
        if (kind == 'cleanup' and control['pause_after_first_delete']
                and any(row['status'] == 'complete' for row in data.get('files', {}).values())):
            control['running'] = False
        return 'artifact'

    class Client(FakeStore):
        def request(self, method, endpoint, body=None):
            result = super().request(method, endpoint, body)
            return {'id': result['id']} if method == 'PATCH' and isinstance(result, dict) else result

    class Backend:
        def __init__(self, scoped, invoke):
            self.root, self.runtime = scoped.path('data_root'), {}

    class Counting(Counter):
        def close(self):
            pass

    class Admission:
        def running_sync(self):
            return control['running']

    client = Client()
    monkeypatch.setattr(runtime, 'read_receipts_sync', read)
    async def registration(task_id, group_id):
        assert (task_id, group_id) == ('task-1', 'group-1')
        return copy.deepcopy(registered)
    monkeypatch.setattr(runtime, 'read_registration', registration)
    monkeypatch.setattr(runtime, 'save_folder_completion', save)
    original_progress_save = runtime.FolderProgress.save

    def local_save(progress, kind, data, event_id=None):
        result = original_progress_save(progress, kind, data, event_id)
        if (kind == 'cleanup' and control['pause_after_first_delete']
                and any(row['status'] == 'complete' for row in data.get('files', {}).values())):
            control['running'] = False
        return result

    monkeypatch.setattr(runtime.FolderProgress, 'save', local_save)
    monkeypatch.setattr(runtime, 'read_complete_handoff', lambda *args: {'handoff': handoff,
        'body_result': {'task': task, 'validation': validation, 'package_path': handoff['body']['package_path']},
        'stage_work_files': [str(card)]})
    monkeypatch.setattr(native_package, 'validate_package', lambda *args, **kwargs: packet)
    monkeypatch.setattr(runtime, 'UnifiedBackend', Backend)
    monkeypatch.setattr(runtime, 'receipt_root_for', lambda backend, paper, row: backend.root / 'attachment-receipts')
    monkeypatch.setattr(runtime, '_original_folder', lambda *args: 'folder-main')
    monkeypatch.setattr(preextract_pipeline, 'offline_counter', lambda *args: Counting())
    monkeypatch.setattr(runtime.legacy, 'bind', lambda settings: None)
    monkeypatch.setattr(runtime.legacy, 'rag_client', lambda scoped: client)
    monkeypatch.setattr(runtime, 'admitted_sync_slot', lambda *args: nullcontext())
    args = dict(identity=identity, registered=registered, settings=settings, admission=Admission(),
                limits={'rag': ['rag'], 'nas': ['nas'], 'cleanup': ['cleanup']}, first_step_work_root=first_step)
    scope = {key: identity[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'task_run_id')}
    child.insert(0, {'task_id': 'task-1', 'folder_id': 'group-1', 'kind': 'rag_effect_intent',
                     'data': {'scope': scope, 'effect_id': 'previous-unknown'}})
    with pytest.raises(ValueError, match='unknown'):
        runtime.deliver_paper(**args)
    assert client.sent == [] and (source / 'body.docx').is_file()
    child.pop(0)
    runtime.FolderProgress(settings, identity, child, save).save('rag_effect_returned',
        {'scope': scope, 'effect_id': 'previous-unknown', 'result': {}})
    child.insert(0, {'task_id': 'task-1', 'folder_id': 'group-1', 'kind': 'rag_effect_failed',
                     'data': {'scope': scope, 'effect_id': 'previous-failure'}})
    with pytest.raises(ValueError, match='retry'):
        runtime.deliver_paper(**args)
    assert client.sent == [] and (source / 'body.docx').is_file()
    child.pop(0)
    runtime.FolderProgress(settings, identity, child, save).save('rag_effect_returned',
        {'scope': scope, 'effect_id': 'previous-failure', 'result': {}})
    if completion_failure in {'rag_complete', 'nas'}:
        with pytest.raises(OSError, match='synthetic final save'):
            runtime.deliver_paper(**args)
        from paperflow_v3.control.retry import retry_decision
        recovery = runtime.FolderProgress(settings, identity, child, save)
        assert retry_decision(recovery.records, identity=identity)['stage'] == (
            'rag' if completion_failure == 'rag_complete' else 'nas')
        sent_before, completed_before = list(client.sent), copy.deepcopy(child)
        if completion_failure == 'nas':
            # Existing recovery policy requires the immutable remote NAS
            # completion. A local successful copy cannot authorize cleanup.
            before = {path: path.read_bytes() for path in (source / 'body.docx', copies / 'body.docx')}
            archive_file = nas / 'task-1/group-1/正文/body.docx'
            archived = archive_file.read_bytes()
            with pytest.raises(ValueError, match='RAG and NAS completion receipts'):
                runtime.deliver_paper(**args)
            assert client.sent == sent_before and child == completed_before
            assert all(path.read_bytes() == value for path, value in before.items())
            assert archive_file.read_bytes() == archived
            assert not any(row['kind'] == 'cleanup' for row in child)
            assert 'completion:nas' in runtime.FolderProgress(settings, identity, child, save).value['records']
            return
        admissions = []
        class PausedAdmission:
            def admit_sync(self):
                admissions.append(True)
                return False
            def running_sync(self):
                return False
        with pytest.raises(InterruptedError):
            runtime.deliver_paper(**{**args, 'admission': PausedAdmission()})
        assert admissions == [True]
        assert child == completed_before and client.sent == sent_before
    with pytest.raises(InterruptedError):
        runtime.deliver_paper(**args)
    assert not (source / 'body.docx').exists() and (copies / 'body.docx').is_file()
    sent = list(client.sent)
    control.update(running=True, pause_after_first_delete=False)
    if completion_failure == 'cleanup':
        with pytest.raises(OSError, match='synthetic final save'):
            runtime.deliver_paper(**args)
        assert client.sent == sent and not (copies / 'body.docx').exists()
    assert runtime.deliver_paper(**args) == {'status': 'complete', 'cleanup_status': 'complete'}
    assert client.sent == sent
    completed = [row for row in child if row['kind'] != 'postprocessing_complete']
    assert [row['kind'] for row in reversed(completed)] == ['rag_complete', 'nas', 'cleanup']
    assert (nas / 'task-1/group-1/正文/body.docx').read_bytes() == b'synthetic body'
    assert (nas / 'task-1/group-1/附件/attachment.txt').read_bytes() == b'print(1)'
    assert not (source / 'body.docx').exists() and not Path(item['source_path']).exists()
    assert not (copies / 'body.docx').exists() and not card.exists() and not receipt_file.exists()
    assert (source / 'trash.txt').is_file() and not database.exists()
    assert runtime.deliver_paper(**args)['status'] == 'complete'
    assert client.sent == sent
