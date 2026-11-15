from contextlib import closing
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from paperflow_prefect.first_step_control import FirstStepControl
from paperflow_prefect.group_classification import _snapshot_id, _version
from paperflow_prefect.group_task_summary import GroupTaskSummary, install_group_summary_routes
from paperflow_prefect.records import identity, sha_file, write_json


def client_for(output):
    app = FastAPI()
    install_group_summary_routes(app, output)
    return TestClient(app)


def saved_group(task, index, *, body=True, attachment=False, trash=False, unresolved=False, failed=False):
    group = task.library / f'person-{index}'
    group.mkdir(parents=True, exist_ok=True)
    group_id = identity(['source-group/1', str(task.library).casefold(), group.name.casefold()])[:32]

    def row(file_id, relative, **extra):
        return dict(file_id=file_id, relative=relative, bytes=20, sha256='a' * 64, **extra)

    body_row = row('body-1', '正文/paper.docx') if body else None
    attachments = [row('attachment-1', '附件/data.xlsx')] if attachment else []
    trash_rows = [row('trash-1', '垃圾/draft.docx')] if trash else []
    unresolved_rows = [row('pending-1', '附件/pending.pdf', reason='尚不能确认分类')] if unresolved else []
    rows = ([body_row] if body_row else []) + attachments + trash_rows + unresolved_rows
    rows.sort(key=lambda r: r['relative'].casefold())
    handoff = dict(schema='paperflow-group-handoff/2', group_id=group_id,
                   library_root=str(task.library), group_root=str(group), group_relative=group.name,
                   handoff_id=_snapshot_id(group_id, rows), paper_id=None,
                   body=body_row, attachments=attachments, trash=trash_rows, unresolved=unresolved_rows,
                   classification_complete=not unresolved, ready_for_receipt=not unresolved,
                   paper_title='已识别题目' if body else None, paper_title_candidate=None)
    handoff['result_version'] = _version(handoff)
    delivery = task.path.parent / f'delivery-{task.task_id}-{index}'
    delivery.mkdir()
    handoff_path = delivery / 'handoff.json'
    write_json(handoff_path, handoff)
    copied_files = [dict(file_id=r['file_id'], relative=r['relative'], bytes=r['bytes'],
                         sha256=r['sha256'], copied=True, error=None) for r in rows]
    if failed and copied_files:
        copied_files[0].update(copied=False, error='PermissionError')
    manifest = dict(schema='paperflow-group-delivery/1', group_id=group_id,
                    handoff_id=handoff['handoff_id'], result_version=handoff['result_version'],
                    handoff_sha256=sha_file(handoff_path), files=copied_files, evidence_records=[], evidence_errors=[],
                    files_complete=not failed, classification_complete=not unresolved,
                    ready_for_receipt=not unresolved and not failed)
    manifest['delivery_id'] = identity(manifest)
    write_json(delivery / 'delivery.json', manifest)
    item = dict(group_root=str(group), handoff=str(handoff_path), delivery_dir=str(delivery),
                delivery_complete=not unresolved and not failed)
    if failed:
        item['delivery_errors'] = [{'relative': copied_files[0]['relative'], 'error': 'PermissionError'}]
    task.record(index, item)
    return group_id, delivery


def test_counts_and_on_demand_detail_do_not_depend_on_the_filtered_page(tmp_path):
    task = GroupTaskSummary(tmp_path, tmp_path / 'library', 2)
    complete_id, _ = saved_group(task, 0, attachment=True, trash=True)
    partial_id, _ = saved_group(task, 1, unresolved=True)
    task.finish()
    client = client_for(tmp_path)

    response = client.get(f'/api/v1/first-step/tasks/{task.task_id}?filter=passed&page_size=1')
    row = response.json()['items'][0]
    assert row['group_id'] == complete_id and row['status'] == 'passed'
    assert row['confirmed_counts'] == {'body': 1, 'attachment': 1, 'trash': 1}
    assert row['unresolved_count'] == 0 and row['classification_state'] == 'complete'
    assert 'files' not in row and 'group_detail' not in response.json()

    partial = client.get(f'/api/v1/first-step/tasks/{task.task_id}?filter=unrecognized').json()['items'][0]
    assert partial['group_id'] == partial_id and partial['status'] == 'unrecognized'
    assert partial['confirmed_counts'] == {'body': 1, 'attachment': 0, 'trash': 0}
    assert partial['unresolved_count'] == 1 and partial['classification_state'] == 'partial'

    response = client.get(f'/api/v1/first-step/tasks/{task.task_id}', params=dict(
        filter='passed', page_size=1, group_id=partial_id, include_files='true'))
    assert response.status_code == 200
    data = response.json()
    assert data['items'][0]['group_id'] == complete_id
    detail = data['group_detail']
    assert detail['task_id'] == task.task_id and detail['group_id'] == partial_id
    assert detail['classification_state'] == 'partial'
    assert detail['files']['body'] == [{'file_id': 'body-1', 'relative_path': '正文/paper.docx'}]
    assert detail['files']['attachment'] == [] and detail['files']['trash'] == []
    assert detail['files']['unresolved'] == [dict(file_id='pending-1',
        relative_path='附件/pending.pdf', reason='尚不能确认分类')]


def test_historical_task_is_scoped_and_missing_record_is_not_zero(tmp_path):
    earlier = GroupTaskSummary(tmp_path, tmp_path / 'earlier', 1)
    old_id, old_delivery = saved_group(earlier, 0)
    earlier.finish()
    latest = GroupTaskSummary(tmp_path, tmp_path / 'later', 1)
    new_id, _ = saved_group(latest, 0, attachment=True)
    latest.finish()
    client = client_for(tmp_path)

    old = client.get(f'/api/v1/first-step/tasks/{earlier.task_id}', params=dict(
        group_id=old_id, include_files='true')).json()
    assert old['group_detail']['group_id'] == old_id
    assert old['items'][0]['confirmed_counts']['attachment'] == 0
    assert client.get(f'/api/v1/first-step/tasks/{earlier.task_id}', params=dict(
        group_id=new_id, include_files='true')).status_code == 404

    (old_delivery / 'handoff.json').unlink()
    unavailable = client.get(f'/api/v1/first-step/tasks/{earlier.task_id}', params=dict(
        group_id=old_id, include_files='true')).json()
    row = unavailable['items'][0]
    assert row['classification_state'] == 'unavailable'
    assert row['confirmed_counts'] is None and row['unresolved_count'] is None
    assert unavailable['group_detail']['files'] is None
    assert unavailable['group_detail']['classification_state'] == 'unavailable'


def test_unknown_group_without_saved_result_is_unavailable_not_404(tmp_path):
    task = GroupTaskSummary(tmp_path, tmp_path / 'library', 1)
    control = FirstStepControl(tmp_path, tmp_path / 'data')
    group_id = 'a' * 32
    with closing(control.summary._connect()) as db, db:
        db.execute("INSERT INTO control_groups(task_id,seq,group_root,state,group_id) VALUES (?,?,?,?,?)",
                   (task.task_id, 0, str(task.library / 'unknown'), 'unknown', group_id))
    client = client_for(tmp_path)
    response = client.get(f'/api/v1/first-step/tasks/{task.task_id}', params=dict(
        filter='unknown', group_id=group_id, include_files='true'))
    assert response.status_code == 200
    result = response.json()
    assert result['items'][0]['status'] == 'unknown'
    assert result['items'][0]['classification_state'] == 'unavailable'
    assert result['items'][0]['confirmed_counts'] is None
    assert result['group_detail']['files'] is None
    assert client.get(f'/api/v1/first-step/tasks/{task.task_id}?include_files=true').status_code == 422


def test_missing_body_does_not_turn_an_unrecognized_group_into_a_pass(tmp_path):
    task = GroupTaskSummary(tmp_path, tmp_path / 'library', 1)
    group_id, _ = saved_group(task, 0, body=False, trash=True)
    task.finish()
    result = client_for(tmp_path).get(f'/api/v1/first-step/tasks/{task.task_id}', params=dict(
        filter='unrecognized', group_id=group_id, include_files='true')).json()
    row = result['items'][0]
    assert row['status'] == 'unrecognized'
    assert row['classification_state'] == 'complete'
    assert row['confirmed_counts'] == {'body': 0, 'attachment': 0, 'trash': 1}
    assert result['group_detail']['files']['body'] == []


def test_delivery_from_another_group_is_unavailable(tmp_path):
    task = GroupTaskSummary(tmp_path, tmp_path / 'library', 2)
    _, first_delivery = saved_group(task, 0, attachment=True)
    second_id, _ = saved_group(task, 1)
    task.finish()
    with closing(task.store._connect()) as db, db:
        db.execute("UPDATE group_results SET payload=json_set(payload, '$.delivery_dir', ?) "
                   'WHERE task_id=? AND seq=1', (str(first_delivery), task.task_id))
    data = client_for(tmp_path).get(f'/api/v1/first-step/tasks/{task.task_id}', params=dict(
        group_id=second_id, include_files='true')).json()
    assert data['items'][1]['classification_state'] == 'unavailable'
    assert data['items'][1]['confirmed_counts'] is None
    assert data['group_detail']['files'] is None


def test_corrupt_delivery_does_not_hide_other_groups(tmp_path):
    task = GroupTaskSummary(tmp_path, tmp_path / 'library', 2)
    first_id, first_delivery = saved_group(task, 0)
    second_id, _ = saved_group(task, 1, attachment=True)
    task.finish()
    client = client_for(tmp_path)
    url = f'/api/v1/first-step/tasks/{task.task_id}'
    assert client.get(url).json()['items'][0]['classification_state'] == 'complete'
    (first_delivery / 'delivery.json').write_text('{broken', encoding='utf-8')
    data = client.get(url, params={'group_id': first_id, 'include_files': 'true'}).json()
    assert data['items'][0]['classification_state'] == 'unavailable'
    assert data['group_detail']['files'] is None
    assert data['items'][1]['group_id'] == second_id
    assert data['items'][1]['classification_state'] == 'complete'


def test_failed_delivery_preserves_confirmed_file_classifications(tmp_path):
    task = GroupTaskSummary(tmp_path, tmp_path / 'library', 1)
    group_id, _ = saved_group(task, 0, attachment=True, failed=True)
    task.finish()
    result = client_for(tmp_path).get(f'/api/v1/first-step/tasks/{task.task_id}', params=dict(
        filter='failed', group_id=group_id, include_files='true')).json()
    row = result['items'][0]
    assert row['status'] == 'failed' and row['delivery_complete'] is False
    assert row['classification_state'] == 'complete'
    assert row['confirmed_counts'] == {'body': 1, 'attachment': 1, 'trash': 0}
    assert result['group_detail']['files']['attachment'][0]['relative_path'] == '附件/data.xlsx'


def test_direct_local_task_exposes_its_saved_classification(tmp_path, monkeypatch):
    from paperflow_prefect import controlled_nimble
    from paperflow_prefect.controlled_first_step import run_first_step
    from test_control_upload_api import make_client

    class FixtureJudge:
        def __init__(self, *args, **kwargs):
            pass

        def choose_body(self, records):
            return {'choice': records[0]['file_id']}

        def choose_other(self, record, body):
            return {'choice': 'attachment'}

        def choose_filename_title(self, filename):
            return None

    monkeypatch.setattr(controlled_nimble, 'ControlledNimbleJudge', FixtureJudge)
    client, scheduler = make_client(tmp_path)
    source = tmp_path / 'original'
    source.mkdir()
    (source / 'paper.txt').write_text('题目：合成测试论文\n这是论文正文', encoding='utf-8')
    (source / 'data.csv').write_text('name,value\ntest,1\n', encoding='utf-8')
    task = client.post('/api/v1/first-step/tasks', json={'name': '合成直读'}).json()['task_id']
    selected = client.post(f'/api/v1/first-step/tasks/{task}/local-source', json={'path': str(source)}).json()
    assert client.post(f'/api/v1/first-step/tasks/{task}/start',
                       json={'expected_revision': selected['control_revision']}).status_code == 200
    run_first_step(client.control, task, scheduler.calls[-1][1], 'fixture-run', endpoint='http://unused')
    data = client.get(f'/api/v1/first-step/tasks/{task}?filter=passed').json()
    row = data['items'][0]
    assert row['confirmed_counts'] == {'body': 1, 'attachment': 1, 'trash': 0}
    assert row['classification_state'] == 'complete' and row['unresolved_count'] == 0
    detail = client.get(f'/api/v1/first-step/tasks/{task}', params=dict(
        group_id=row['group_id'], include_files='true')).json()['group_detail']
    assert detail['files']['body'][0]['relative_path'] == 'paper.txt'
    assert detail['files']['attachment'][0]['relative_path'] == 'data.csv'
    assert '/tasks/' in row['delivery_dir'].replace('\\', '/')

