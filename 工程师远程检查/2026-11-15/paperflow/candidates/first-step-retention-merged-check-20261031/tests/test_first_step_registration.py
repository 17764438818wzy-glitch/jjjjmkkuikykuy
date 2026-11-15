from pathlib import Path
import sqlite3
import threading
import time
import json

from filelock import FileLock
import pytest

from paperflow_prefect.group_classification import process_one_group
from paperflow_prefect.records import read_json
from test_control_admission import ready_control


class FixtureJudge:
    def choose_body(self, rows):
        return {'choice': next(row['file_id'] for row in rows if row['relative'] == 'paper.txt')}

    def choose_other(self, record, body_id):
        return {'choice': 'attachment', 'model': 'fixture', 'confidence': 1,
                'probabilities': {'attachment': 1, 'trash': 0, 'uncertain': 0}}


def evidence(path):
    return {'format': 'text', 'text': path.read_text(encoding='utf8'), 'truncated': False}


def make_delivery(tmp_path, output_name='task-a', *, attachment=False):
    library = tmp_path / 'library'
    group = library / 'person'
    group.mkdir(parents=True, exist_ok=True)
    (group / 'paper.txt').write_text('题目：合成论文\n摘要：测试正文', encoding='utf8')
    if attachment:
        (group / 'data.csv').write_text('date,value\n1,2', encoding='utf8')
    result = process_one_group(group, library, tmp_path / output_name, FixtureJudge, evidence,
                               verified_new=True)
    assert result.get('delivery_complete') is True, result
    return group, result


def test_registers_delivery_copies_and_reuses_same_receipt(tmp_path):
    from paperflow_prefect.first_step_registration import register_delivery

    group, item = make_delivery(tmp_path, attachment=True)
    original = group.with_name('source-moved-away')
    group.rename(original)
    options = dict(receipt_root=tmp_path / 'receipts', history_roots=(),
                   lock_path=tmp_path / 'locks' / 'receipt.lock')
    first = register_delivery(item['delivery_dir'], **options)
    second = register_delivery(item['delivery_dir'], **options)

    assert first['state'] == '已接收' and first['paper_id']
    assert first['receipt_id'] == second['receipt_id']
    body = next(row for row in first['items'] if row['kind'] == 'body')
    attachment = next(row for row in first['items'] if row['kind'] == 'attachment')
    for row in (body, attachment):
        assert Path(row['path']).is_relative_to(Path(item['delivery_dir']) / 'files')
        assert Path(row['path']).is_file()
        assert Path(row['source_path']).is_relative_to(group)
    assert read_json(first['record_path'])['receipt_id'] == first['receipt_id']
    from paperflow_prefect.stage3_handoff import read_receipt
    observed, paper = read_receipt(first['record_path'])
    assert observed['state'] == '已接收' and paper['paper_id'] == first['paper_id']


def test_controlled_flow_automatically_registers_passed_group(tmp_path, monkeypatch):
    from paperflow_prefect import group_classification
    from paperflow_prefect.controlled_first_step import run_first_step

    control, scheduler, task_id = ready_control(tmp_path, groups=1)
    with control.summary._connect() as db:
        task = control._task(db, task_id)
        group = Path(db.execute('SELECT group_root FROM control_groups WHERE task_id=?', (task_id,)).fetchone()[0])
        library = control.source_library(task)
        output = control.artifact_root(task)
    item = process_one_group(group, library, output, FixtureJudge, evidence, verified_new=True)
    assert item.get('delivery_complete') is True, item
    monkeypatch.setattr(group_classification, 'process_one_group', lambda *args, **kwargs: item)

    run_first_step(control, task_id, scheduler.calls[-1][1], 'synthetic-run',
                   endpoint='http://127.0.0.1:0', receipt_root=tmp_path / 'receipts', history_roots=())
    result = control.summary.read(task_id, 'passed', 1, 50)
    assert result['passed_groups'] == 1
    row = result['items'][0]
    assert row['registration_status'] == 'registered' and row['paper_id']
    receipt = read_json(row['receipt_path'])
    assert receipt['state'] == '已接收' and receipt['paper_id'] == row['paper_id']


def test_registration_failure_is_separate_from_classification(tmp_path, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from paperflow_prefect import group_classification, first_step_registration
    from paperflow_prefect.controlled_first_step import run_first_step
    from paperflow_prefect.group_task_summary import install_group_summary_routes

    control, scheduler, task_id = ready_control(tmp_path, groups=1)
    with control.summary._connect() as db:
        task = control._task(db, task_id)
        group = Path(db.execute('SELECT group_root FROM control_groups WHERE task_id=?', (task_id,)).fetchone()[0])
        library = control.source_library(task)
        output = control.artifact_root(task)
    item = process_one_group(group, library, output, FixtureJudge, evidence, verified_new=True)
    monkeypatch.setattr(group_classification, 'process_one_group', lambda *args, **kwargs: item)
    def fail(*args, **kwargs):
        raise ValueError('合成登记错误')
    monkeypatch.setattr(first_step_registration, 'register_delivery', fail)

    run_first_step(control, task_id, scheduler.calls[-1][1], 'synthetic-run',
                   endpoint='http://127.0.0.1:0', receipt_root=tmp_path / 'receipts', history_roots=())
    result = control.summary.read(task_id, 'passed', 1, 50)
    assert result['passed_groups'] == 1 and result['failed_groups'] == 0
    assert result['items'][0]['registration_status'] == 'failed'
    assert '合成登记错误' in result['items'][0]['registration_reason']
    app = FastAPI()
    install_group_summary_routes(app, control.output)
    response = TestClient(app).get(f'/api/v1/first-step/tasks/{task_id}?filter=registration_failed')
    assert response.status_code == 200
    listed = response.json()
    assert listed['registration_failed_groups'] == listed['list_total'] == 1
    assert listed['items'][0]['registration_status'] == 'failed'


def test_same_group_in_two_tasks_keeps_paper_identity(tmp_path):
    from paperflow_prefect.first_step_registration import register_delivery
    _, first = make_delivery(tmp_path, 'task-a')
    _, second = make_delivery(tmp_path, 'task-b')
    options = dict(receipt_root=tmp_path / 'receipts', history_roots=(),
                   lock_path=tmp_path / 'locks' / 'receipt.lock')
    first_receipt = register_delivery(first['delivery_dir'], **options)
    second_receipt = register_delivery(second['delivery_dir'], **options)
    assert first_receipt['paper_id'] == second_receipt['paper_id']
    assert first_receipt['receipt_id'] != second_receipt['receipt_id']


def test_deleting_distinct_versions_cleans_each_tasks_evidence(tmp_path):
    from paperflow_prefect.first_step_registration import register_delivery, task_receipt_plan, remove_task_receipt_events
    group, first = make_delivery(tmp_path, 'task-a', attachment=True)
    (group / 'data.csv').write_text('date,value\n2,9', encoding='utf8')
    second = process_one_group(group, tmp_path/'library', tmp_path/'task-b', FixtureJudge, evidence, verified_new=True)
    assert second.get('delivery_complete') is True
    root = tmp_path/'receipts'
    options = dict(receipt_root=root, history_roots=(), lock_path=tmp_path/'locks'/'receipt.lock')
    a = register_delivery(first['delivery_dir'], **options)
    b = register_delivery(second['delivery_dir'], **options)
    assert a['paper_id'] == b['paper_id']
    with sqlite3.connect(root/'receipts.sqlite3') as db:
        assert db.execute('SELECT COUNT(*) FROM evidence_versions').fetchone()[0] == 2
    remove_task_receipt_events(root, task_receipt_plan(root, tmp_path/'task-b'))
    remove_task_receipt_events(root, task_receipt_plan(root, tmp_path/'task-a'))
    with sqlite3.connect(root/'receipts.sqlite3') as db:
        assert db.execute('SELECT COUNT(*) FROM evidence_versions').fetchone()[0] == 0


def test_repeated_classification_does_not_prevent_deleting_old_task(tmp_path):
    import shutil
    from paperflow_prefect.first_step_registration import register_delivery, task_receipt_plan, remove_task_receipt_events
    from paperflow_prefect.stage3_handoff import read_receipt
    _, first = make_delivery(tmp_path, 'task-a')
    _, second = make_delivery(tmp_path, 'task-b')
    root = tmp_path/'receipts'
    options = dict(receipt_root=root, history_roots=(), lock_path=tmp_path/'locks'/'receipt.lock')
    a = register_delivery(first['delivery_dir'], **options)
    b = register_delivery(second['delivery_dir'], **options)
    assert a['paper_id'] == b['paper_id']
    plan = task_receipt_plan(root, tmp_path/'task-a')
    remove_task_receipt_events(root, plan)
    Path(a['record_path']).unlink()
    shutil.rmtree(tmp_path/'task-a')
    observed, paper = read_receipt(b['record_path'])
    assert observed['state'] == '已接收' and paper['paper_id'] == b['paper_id']


def test_known_previous_work_keeps_its_delivery_reference(tmp_path):
    from paperflow_prefect.first_step_control import ControlError
    from paperflow_prefect.first_step_registration import register_delivery, task_receipt_plan
    from paperflow_prefect.records import identity
    _, first = make_delivery(tmp_path, 'task-a')
    _, second = make_delivery(tmp_path, 'task-b')
    root = tmp_path/'receipts'
    options = dict(receipt_root=root, history_roots=(), lock_path=tmp_path/'locks'/'receipt.lock')
    a = register_delivery(first['delivery_dir'], **options)
    inventory = a['inventory']
    history = tmp_path/'history'
    inventory_path = history/'papers'/a['paper_id']/'revisions'/inventory['revision']/'signature'/'inventory.json'
    inventory_path.parent.mkdir(parents=True)
    inventory_path.write_text(json.dumps(inventory, ensure_ascii=False), encoding='utf8')
    body = inventory['body']
    key = identity([body['sha256'], body['relative'], 'signature'])[:24]
    (inventory_path.parents[3]/'body'/key).mkdir(parents=True)
    b = register_delivery(second['delivery_dir'], **dict(options, history_roots=(history,)))
    assert b['paper_id'] == a['paper_id']
    assert b['items'][0]['delivery_history'][0]['receipt_id'] == a['receipt_id']
    with pytest.raises(ControlError, match='其他任务'):
        task_receipt_plan(root, tmp_path/'task-a')


def test_registration_waits_for_writer_without_failing(tmp_path):
    from paperflow_prefect.first_step_registration import register_delivery
    _, item = make_delivery(tmp_path)
    lock_path = tmp_path / 'locks' / 'receipt.lock'
    lock_path.parent.mkdir()
    done, result = threading.Event(), []
    with FileLock(str(lock_path)):
        def run():
            result.append(register_delivery(item['delivery_dir'], receipt_root=tmp_path / 'receipts',
                           history_roots=(), lock_path=lock_path))
            done.set()
        thread = threading.Thread(target=run)
        thread.start()
        assert not done.wait(.35)
    thread.join(5)
    assert done.is_set() and result[0]['state'] == '已接收'


def test_delete_task_removes_its_registration_record(tmp_path, monkeypatch):
    from paperflow_prefect import group_classification
    from paperflow_prefect.controlled_first_step import run_first_step
    from paperflow_prefect.first_step_delete import delete_task
    control, scheduler, task_id = ready_control(tmp_path, groups=1)
    with control.summary._connect() as db:
        task = control._task(db, task_id)
        group = Path(db.execute('SELECT group_root FROM control_groups WHERE task_id=?', (task_id,)).fetchone()[0])
        library, output = control.source_library(task), control.artifact_root(task)
    item = process_one_group(group, library, output, FixtureJudge, evidence, verified_new=True)
    monkeypatch.setattr(group_classification, 'process_one_group', lambda *args, **kwargs: item)
    root = tmp_path / 'receipts'
    run_first_step(control, task_id, scheduler.calls[-1][1], 'run', endpoint='http://unused', receipt_root=root)
    row = control.summary.read(task_id, 'passed', 1, 50)['items'][0]
    assert row['registration_status'] == 'registered'
    receipt_file = Path(row['receipt_path'])
    assert receipt_file.is_file()
    assert delete_task(control, task_id)['deleted'] is True
    assert not receipt_file.exists()
    with sqlite3.connect(root / 'receipts.sqlite3') as db:
        assert db.execute('SELECT COUNT(*) FROM receipt_events').fetchone()[0] == 0
        assert db.execute('SELECT COUNT(*) FROM papers').fetchone()[0] == 0


def test_each_group_registers_without_waiting_for_the_whole_batch(tmp_path, monkeypatch):
    from paperflow_prefect import group_classification
    from paperflow_prefect.controlled_first_step import run_first_step
    control, scheduler, task_id = ready_control(tmp_path, groups=2)
    with control.summary._connect() as db:
        task = control._task(db, task_id)
        paths = [Path(row[0]) for row in db.execute('SELECT group_root FROM control_groups WHERE task_id=? ORDER BY seq', (task_id,))]
        library, output = control.source_library(task), control.artifact_root(task)
    results = {str(path): process_one_group(path, library, output, FixtureJudge, evidence, verified_new=True)
               for path in paths}
    waiting, release = threading.Event(), threading.Event()
    def classify(path, *args, **kwargs):
        if Path(path) == paths[1]:
            waiting.set()
            assert release.wait(5)
        return results[str(path)]
    monkeypatch.setattr(group_classification, 'process_one_group', classify)
    errors = []
    def run():
        try:
            run_first_step(control, task_id, scheduler.calls[-1][1], 'run', endpoint='http://unused',
                           receipt_root=tmp_path/'receipts')
        except Exception as exc:
            errors.append(exc)
    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert waiting.wait(5)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            summary = control.summary.read(task_id, 'passed', 1, 50)
            if summary['registered_groups'] == 1:
                break
            time.sleep(.05)
        assert summary['registered_groups'] == 1 and summary['processed_groups'] == 1
        assert summary['items'][0]['registration_status'] == 'registered'
    finally:
        release.set()
        thread.join(5)
    assert not thread.is_alive() and not errors
    assert control.summary.read(task_id, None, 1, 50)['registered_groups'] == 2


def test_deleting_one_task_preserves_other_tasks_receipt(tmp_path, monkeypatch):
    from paperflow_prefect import group_classification
    from paperflow_prefect.controlled_first_step import run_first_step
    from paperflow_prefect.first_step_delete import delete_task
    first, first_scheduler, first_id = ready_control(tmp_path, groups=1)
    second, second_scheduler, second_id = ready_control(tmp_path, groups=1)
    prepared = {}
    for control, task_id in ((first, first_id), (second, second_id)):
        with control.summary._connect() as db:
            task = control._task(db, task_id)
            group = Path(db.execute('SELECT group_root FROM control_groups WHERE task_id=?', (task_id,)).fetchone()[0])
            prepared[str(group)] = process_one_group(group, control.source_library(task), control.artifact_root(task),
                                                     FixtureJudge, evidence, verified_new=True)
    monkeypatch.setattr(group_classification, 'process_one_group', lambda path, *a, **k: prepared[str(path)])
    for control, scheduler, task_id in ((first, first_scheduler, first_id), (second, second_scheduler, second_id)):
        run_first_step(control, task_id, scheduler.calls[-1][1], 'run', endpoint='http://unused',
                       receipt_root=tmp_path/'receipts')
    first_row = first.summary.read(first_id, 'passed', 1, 50)['items'][0]
    second_row = second.summary.read(second_id, 'passed', 1, 50)['items'][0]
    assert first_row['registration_status'] == second_row['registration_status'] == 'registered'
    assert delete_task(first, first_id)['deleted']
    assert not Path(first_row['receipt_path']).exists()
    assert Path(second_row['receipt_path']).is_file()
    with sqlite3.connect(tmp_path/'receipts'/'receipts.sqlite3') as db:
        assert db.execute('SELECT COUNT(*) FROM receipt_events').fetchone()[0] == 1
        assert db.execute('SELECT COUNT(*) FROM papers').fetchone()[0] == 1


def test_history_roots_exclude_first_step_upload_staging(tmp_path):
    from paperflow_prefect.first_step_registration import registration_history_roots
    data = tmp_path/'data'
    (data/'uploads'/'many-folders').mkdir(parents=True)
    (data/'tusd').mkdir()
    (data/'control-locks').mkdir()
    (data/'papers').mkdir()
    configured = tmp_path/'old-history'
    configured.mkdir()
    roots = registration_history_roots(data, (configured,))
    assert data/'papers' in roots and configured in roots
    assert data/'uploads' not in roots and data/'tusd' not in roots and data/'control-locks' not in roots


@pytest.mark.parametrize('history_kind', ['missing', 'corrupt'])
def test_unavailable_history_does_not_create_a_new_paper_identity(tmp_path, history_kind):
    from paperflow_prefect.first_step_registration import register_delivery
    _, item = make_delivery(tmp_path)
    old = tmp_path/'history'
    if history_kind == 'corrupt':
        old.mkdir()
        (old/'inventory.json').write_text('{broken', encoding='utf8')
    root = tmp_path/'receipts'
    with pytest.raises((OSError, ValueError)):
        register_delivery(item['delivery_dir'], receipt_root=root, history_roots=(old,),
                          lock_path=tmp_path/'locks'/'receipt.lock')
    if (root/'receipts.sqlite3').exists():
        with sqlite3.connect(root/'receipts.sqlite3') as db:
            assert db.execute('SELECT COUNT(*) FROM papers').fetchone()[0] == 0
