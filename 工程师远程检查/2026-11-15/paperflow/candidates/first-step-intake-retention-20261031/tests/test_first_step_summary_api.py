from pathlib import Path
import threading
import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from paperflow_prefect.group_task_summary import GroupTaskSummary, install_group_summary_routes
from paperflow_prefect.records import identity, read_json, write_json


def client_for(output):
    app = FastAPI()
    install_group_summary_routes(app, output)
    return TestClient(app)


def add_handoff(task, index, *, body_sha=None, title=None, candidate=None, unresolved=None):
    group = task.library / f'group-{index}'
    group_id = identity(['source-group/1', str(task.library).casefold(), group.name.casefold()])[:32]
    body = {'file_id': 'body', 'relative': 'paper.docx', 'sha256': body_sha} if body_sha else None
    handoff = {'group_id': group_id, 'body': body, 'paper_title': title,
               'paper_title_candidate': candidate,
               'title_candidate_source': {'source_sha256': 'f' * 64} if candidate else None,
               'attachments': [], 'trash': [], 'unresolved': unresolved or []}
    path = task.path.parent / f'synthetic-handoff-{task.task_id}-{index}.json'
    write_json(path, handoff)
    task.record(index, {'group_root': str(group), 'handoff': str(path),
                        'delivery_complete': not bool(unresolved),
                        'delivery_dir': str(path.parent / 'delivery')})
    return group_id


def test_live_counts_filters_titles_and_reasons(tmp_path):
    client = client_for(tmp_path)
    assert client.get('/api/v1/first-step/tasks/current').status_code == 404
    task = GroupTaskSummary(tmp_path, tmp_path / 'library', 5)
    task.last_write = time.monotonic()  # Existing JSON snapshot is intentionally throttled.

    add_handoff(task, 0, body_sha='a' * 64, title='确定的题目')
    data = client.get('/api/v1/first-step/tasks/current?filter=passed').json()
    assert data['task_id'] == task.task_id and data['task_status'] == 'running'
    assert (data['total_groups'], data['processed_groups'], data['passed_groups']) == (5, 1, 1)
    assert data['list_total'] == 1 and data['items'][0]['paper_title'] == '确定的题目'

    add_handoff(task, 1, body_sha='b' * 64, candidate='备选题目')
    unresolved_id = add_handoff(task, 2, candidate='未知题目', unresolved=[
        {'file_id': 'unknown', 'relative': 'unknown.docx', 'sha256': 'c' * 64,
         'reason': '正文尚未确认，文件分类未完成'}])
    task.record(3, {'group_root': str(task.library / 'group-3'), 'error': '读取失败：KeyError'})
    task.record(4, {'group_root': str(task.library / 'group-4'), 'error': '历史分类请求结果未确认'})
    task.finish()
    legacy = read_json(task.path)
    assert (legacy['passed_groups'], legacy['failed_groups'], legacy['review_groups']) == (2, 2, 1)

    data = client.get(f'/api/v1/first-step/tasks/{task.task_id}?filter=passed&page=2&page_size=1').json()
    assert data['task_status'] == 'complete'
    assert (data['processed_groups'], data['passed_groups'], data['failed_groups'], data['unrecognized_groups']) == (5, 2, 2, 1)
    assert data['list_total'] == 2 and len(data['items']) == 1
    row = data['items'][0]
    assert row['status'] == 'passed' and row['title_status'] == '待核对'
    assert row['paper_title'] is None and row['paper_title_candidate'] == '备选题目'
    assert row['display_hash'] == 'b' * 64 and row['reason'] is None

    data = client.get(f'/api/v1/first-step/tasks/{task.task_id}?filter=unrecognized').json()
    assert data['list_total'] == data['unrecognized_groups'] == 1
    row = data['items'][0]
    assert row['group_id'] == unresolved_id == row['display_hash']
    assert row['paper_title_candidate'] == '未知题目' and '正文尚未确认' in row['reason']
    data = client.get(f'/api/v1/first-step/tasks/{task.task_id}?filter=failed').json()
    assert data['list_total'] == data['failed_groups'] == 2
    assert 'KeyError' in data['items'][0]['reason']
    assert '未确认' in data['items'][1]['reason']
    assert client.get(f'/api/v1/first-step/tasks/{task.task_id}?filter=other').status_code == 422


def test_current_means_latest_started_task_not_most_recent_update(tmp_path):
    client = client_for(tmp_path)
    earlier = GroupTaskSummary(tmp_path, tmp_path / 'library', 1)
    time.sleep(0.002)
    latest = GroupTaskSummary(tmp_path, tmp_path / 'library', 1)
    add_handoff(earlier, 0, body_sha='a' * 64, title='旧任务')
    earlier.finish()
    data = client.get('/api/v1/first-step/tasks/current?filter=passed').json()
    assert data['task_id'] == latest.task_id
    assert data['processed_groups'] == 0 and data['list_total'] == 0


def test_unrecognized_title_does_not_make_a_passed_group_unrecognized(tmp_path):
    task = GroupTaskSummary(tmp_path, tmp_path / 'library', 1)
    add_handoff(task, 0, body_sha='d' * 64)
    row = client_for(tmp_path).get('/api/v1/first-step/tasks/current?filter=passed').json()['items'][0]
    assert row['status'] == 'passed' and row['title_status'] == '未提取'
    assert row['display_hash'] == 'd' * 64 and row['display_subtitle'] == '题目未识别'


def test_duplicate_group_inputs_are_counted_once(tmp_path):
    from paperflow_prefect.group_classification import classify_batch

    library = tmp_path / 'library'
    group = library / 'person'
    group.mkdir(parents=True)
    output = tmp_path / 'output'
    results = classify_batch([str(group), str(group)], library, output,
                             lambda: object(), lambda path: {},
                             verified_new_groups=[str(group)])
    assert len(results) == 1
    data = client_for(output).get('/api/v1/first-step/tasks/current').json()
    assert data['total_groups'] == data['processed_groups'] == 1
    assert data['passed_groups'] + data['failed_groups'] + data['unrecognized_groups'] == 1


def test_failed_group_reports_why_its_folder_is_invalid(tmp_path):
    from paperflow_prefect.group_classification import classify_batch

    library = tmp_path / 'library'
    library.mkdir()
    output = tmp_path / 'output'
    classify_batch([str(library / 'missing')], library, output, lambda: object(), lambda path: {})
    data = client_for(output).get('/api/v1/first-step/tasks/current?filter=failed').json()
    assert data['list_total'] == 1 and '资料组文件夹不存在' in data['items'][0]['reason']
    assert data['items'][0]['group_id'] is None and data['items'][0]['display_hash'] is None


def test_package_failure_preserves_known_title_and_body_hash_for_display(tmp_path, monkeypatch):
    from paperflow_prefect import group_delivery
    from paperflow_prefect.group_classification import classify_batch
    from paperflow_prefect.records import sha_file

    library = tmp_path / 'library'
    group = library / 'person'
    group.mkdir(parents=True)
    body = group / 'paper.txt'
    body.write_text('题目：合成题目\n姓名：示例', encoding='utf-8')

    class Judge:
        def choose_body(self, records):
            return {'choice': records[0]['file_id']}

    def read_evidence(path):
        return {'format': 'text', 'text': path.read_text(encoding='utf-8'), 'truncated': False}

    def fail_package(*args):
        raise PermissionError('synthetic')

    monkeypatch.setattr(group_delivery, 'package_result', fail_package)
    output = tmp_path / 'output'
    classify_batch([str(group)], library, output, Judge, read_evidence,
                   title_reader=lambda record, evidence: ('合成题目', '正文文本开头', '合成题目', True),
                   verified_new_groups=[str(group)])
    row = client_for(output).get('/api/v1/first-step/tasks/current?filter=failed').json()['items'][0]
    assert row['paper_title'] == '合成题目' and row['display_hash'] == sha_file(body)
    assert 'PermissionError' in row['reason'] and row['delivery_complete'] is False


def test_timeout_and_delivery_errors_are_failures_with_details(tmp_path):
    task = GroupTaskSummary(tmp_path, tmp_path / 'library', 2)
    add_handoff(task, 0, unresolved=[{'file_id': 'x', 'relative': 'paper.docx',
                                      'reason': '正文判断失败：TimeoutError'}])
    group = task.library / 'group-1'
    handoff = task.path.parent / 'delivery-error-handoff.json'
    write_json(handoff, {'body': {'file_id': 'body', 'relative': 'paper.docx', 'sha256': 'a' * 64},
                         'paper_title': None, 'paper_title_candidate': None,
                         'attachments': [], 'trash': [], 'unresolved': []})
    task.record(1, {'group_root': str(group), 'handoff': str(handoff),
                    'delivery_errors': [{'relative': 'paper.docx', 'error': 'PermissionError'}],
                    'evidence_errors': [{'file_id': 'body', 'error_type': 'ValueError'}]})
    data = client_for(tmp_path).get(f'/api/v1/first-step/tasks/{task.task_id}?filter=failed').json()
    assert data['list_total'] == 2
    assert 'TimeoutError' in data['items'][0]['reason']
    assert 'PermissionError' in data['items'][1]['reason']
    assert 'ValueError' in data['items'][1]['reason']


def test_failed_index_write_cannot_mark_an_unrecorded_group_complete(tmp_path, monkeypatch):
    task = GroupTaskSummary(tmp_path, tmp_path / 'library', 1)
    def unavailable(*args):
        raise OSError('index unavailable')
    monkeypatch.setattr(task.store, 'record', unavailable)
    with pytest.raises(OSError):
        task.record(0, {'group_root': str(task.library / 'g'), 'error': 'test error'})
    task.finish()
    data = client_for(tmp_path).get('/api/v1/first-step/tasks/current').json()
    assert data['task_status'] == 'incomplete' and data['processed_groups'] == 0


def test_preflight_failure_is_the_current_task(tmp_path, monkeypatch):
    from first_step_flow import first_step
    from paperflow_prefect.settings import Settings

    class FakeSettings:
        def path(self, key):
            assert key == 'classification_root'
            return tmp_path

    monkeypatch.setattr(Settings, 'load', lambda: FakeSettings())
    with pytest.raises(ValueError):
        first_step.fn(person_folders=[])
    data = client_for(tmp_path).get('/api/v1/first-step/tasks/current').json()
    assert data['task_status'] == 'failed' and data['total_groups'] == 0


def test_task_is_current_before_group_processing_begins(tmp_path, monkeypatch):
    import first_step_flow as flow_module
    from paperflow_prefect.settings import Settings

    class FakeSettings:
        def path(self, key):
            assert key == 'classification_root'
            return tmp_path

    def fake_classification(groups, library, output, *args, task_id, **kwargs):
        data = client_for(output).get('/api/v1/first-step/tasks/current').json()
        assert data['task_id'] == task_id and data['task_status'] == 'running'
        assert data['total_groups'] == 1 and data['processed_groups'] == 0
        raise RuntimeError('synthetic stop before processing')

    monkeypatch.setattr(Settings, 'load', lambda: FakeSettings())
    monkeypatch.setattr(flow_module, 'classify_batch', fake_classification)
    with pytest.raises(RuntimeError):
        flow_module.first_step.fn(person_folders=['group'], library_root=str(tmp_path / 'library'))


def test_error_after_classification_does_not_report_task_complete(tmp_path, monkeypatch):
    import first_step_flow as flow_module
    from paperflow_prefect.group_summary_store import GroupSummaryStore
    from paperflow_prefect.settings import Settings

    class FakeSettings:
        def path(self, key):
            assert key == 'classification_root'
            return tmp_path

    def fake_classification(groups, library, output, *args, task_id, **kwargs):
        store = GroupSummaryStore(output)
        store.create_task(task_id, 1)
        store.finish(task_id, 'complete')

    monkeypatch.setattr(Settings, 'load', lambda: FakeSettings())
    monkeypatch.setattr(flow_module, 'classify_batch', fake_classification)
    with pytest.raises(FileNotFoundError):
        flow_module.first_step.fn(person_folders=['group'], library_root=str(tmp_path / 'library'))
    data = client_for(tmp_path).get('/api/v1/first-step/tasks/current').json()
    assert data['task_status'] == 'failed'


def test_reads_remain_consistent_while_groups_finish(tmp_path):
    task = GroupTaskSummary(tmp_path, tmp_path / 'library', 50)
    client = client_for(tmp_path)
    failures = []

    def write_results():
        try:
            for index in range(50):
                task.record(index, {'group_root': str(task.library / str(index)), 'error': '合成错误'})
            task.finish()
        except Exception as exc:
            failures.append(exc)

    writer = threading.Thread(target=write_results)
    writer.start()
    while writer.is_alive():
        data = client.get(f'/api/v1/first-step/tasks/{task.task_id}?filter=failed&page_size=200').json()
        assert data['list_total'] == data['failed_groups'] == len(data['items'])
        assert data['processed_groups'] == data['passed_groups'] + data['failed_groups'] + data['unrecognized_groups']
    writer.join()
    assert not failures
