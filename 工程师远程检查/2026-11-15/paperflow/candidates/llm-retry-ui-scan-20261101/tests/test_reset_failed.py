import importlib.util
import json
import sqlite3
from pathlib import Path
from uuid import uuid4

import pytest

SOURCE = Path(__file__).resolve().parents[1] / 'retry_service.py'
spec = importlib.util.spec_from_file_location('retry_reset_candidate', SOURCE)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


@pytest.fixture
def situation(tmp_path):
    db = tmp_path / 'control.sqlite'
    task, folder = str(uuid4()), str(uuid4())
    work = tmp_path / 'body'
    model = work / 'model'
    model.mkdir(parents=True)
    for name in module.JOURNAL_FILES:
        (model / name).write_text('failed result', encoding='utf8')
    (work / 'frozen-plan.json').write_text('input plan', encoding='utf8')
    attachment = tmp_path / 'attachment'
    (attachment / 'model').mkdir(parents=True)
    (attachment / 'model' / 'response.txt').write_text('completed', encoding='utf8')
    with sqlite3.connect(db) as conn:
        conn.executescript('''CREATE TABLE tasks(task_id TEXT, desired_state TEXT, start_generation INTEGER);
        CREATE TABLE llm_request_admissions(task_id TEXT, task_run_id TEXT, component_id TEXT,
            state TEXT, kind TEXT, work_dir TEXT, model_dir TEXT, updated_at_ns INTEGER);''')
        conn.execute('INSERT INTO tasks VALUES(?,?,?)', (task, 'running', 2))
        conn.execute('INSERT INTO llm_request_admissions VALUES(?,?,?,?,?,?,?,?)',
            (task, folder, 'failed', 'unknown', 'body', str(work), str(model), 1))
        conn.execute('INSERT INTO llm_request_admissions VALUES(?,?,?,?,?,?,?,?)',
            (task, folder, 'good', 'raw_complete', 'attachment', str(attachment), str(attachment/'model'), 2))
    service = module.RetryService.__new__(module.RetryService)
    service.database = db
    def check_task(conn, item):
        row = conn.execute('SELECT * FROM tasks WHERE task_id=?', (item['task_id'],)).fetchone()
        if row['desired_state'] != 'running':
            raise module.RetryStopped('大任务当前已暂停')
        return {'start_generation': row['start_generation'], 'prefect_run_id': 'parent'}
    service._task = check_task
    return service, db, task, folder, work, model, attachment


def test_reset_only_failed_component_keeps_inputs_and_successful_attachment(situation):
    service, db, task, folder, work, model, attachment = situation
    service._reset_failed({'task_id': task}, {'start_generation': 2, 'prefect_run_id': 'parent'}, folder)
    assert not any((model / name).exists() for name in module.JOURNAL_FILES)
    assert (work / 'frozen-plan.json').read_text(encoding='utf8') == 'input plan'
    assert (attachment / 'model' / 'response.txt').read_text(encoding='utf8') == 'completed'
    with sqlite3.connect(db) as conn:
        assert conn.execute('SELECT component_id,state FROM llm_request_admissions').fetchall() == [('good', 'raw_complete')]


def test_pause_prevents_reset(situation):
    service, db, task, folder, work, model, _ = situation
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE tasks SET desired_state='paused'")
    with pytest.raises(module.RetryStopped, match='暂停'):
        service._reset_failed({'task_id': task}, {'start_generation': 2, 'prefect_run_id': 'parent'}, folder)
    assert (model / 'request.json').exists()
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT count(*) FROM llm_request_admissions WHERE state='unknown'").fetchone()[0] == 1


def test_retry_cannot_reset_a_model_directory_outside_its_work(situation):
    service, db, task, folder, work, model, _ = situation
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE llm_request_admissions SET model_dir=? WHERE component_id='failed'", (str(work.parent),))
    with pytest.raises(module.RetryStopped):
        service._reset_failed({'task_id': task}, {'start_generation': 2, 'prefect_run_id': 'parent'}, folder)
    assert (model / 'request.json').exists()


def test_completed_response_is_not_discarded_if_verification_fails(situation):
    service, _, task, _, _, model, _ = situation
    (model / 'request-state.json').write_text(json.dumps({
        'status': 'response_complete', 'finish_reason': 'stop', 'response_sha256': 'b' * 64,
    }), encoding='utf8')
    class Guard:
        def complete(self, *_): raise ValueError('cannot verify')
    service.guard = Guard()
    with pytest.raises(module.RetryStopped, match='已有回复'):
        service._review_existing_response({'task_id': task},
            {'component_id': 'failed', 'state': 'unknown', 'response_sha256': None}, model)
    assert (model / 'response.txt').exists()


def test_failed_response_can_be_reset(situation):
    service, _, task, _, _, model, _ = situation
    (model / 'request-state.json').write_text(json.dumps({
        'status': 'request_failed_or_uncertain', 'finish_reason': None,
    }), encoding='utf8')
    service._review_existing_response({'task_id': task},
        {'component_id': 'failed', 'state': 'unknown', 'response_sha256': None}, model)
