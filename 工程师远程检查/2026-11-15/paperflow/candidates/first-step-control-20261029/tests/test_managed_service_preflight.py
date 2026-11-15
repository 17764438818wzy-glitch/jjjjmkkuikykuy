import sqlite3

import pytest

from managed_service_preflight import UnsafeToStart, check_idle


def databases(tmp_path):
    control = tmp_path / 'control.sqlite3'
    prefect = tmp_path / 'prefect.sqlite3'
    with sqlite3.connect(control) as db:
        db.executescript("""
            CREATE TABLE tasks (task_status TEXT, start_pending INTEGER);
            CREATE TABLE control_groups (state TEXT);
            CREATE TABLE uploads (state TEXT);
            CREATE TABLE model_decisions (state TEXT);
            INSERT INTO tasks VALUES ('draft', 0);
        """)
    with sqlite3.connect(prefect) as db:
        db.execute('CREATE TABLE flow_run (state_type TEXT)')
    return control, prefect


def test_draft_alone_allows_service_start(tmp_path):
    check_idle(*databases(tmp_path))


@pytest.mark.parametrize(('table', 'values'), [
    ('tasks', "'running', 0"),
    ('tasks', "'starting', 1"),
    ('control_groups', "'running'"),
    ('uploads', "'sealed'"),
    ('model_decisions', "'inflight'"),
])
def test_active_work_blocks_automatic_service_start(tmp_path, table, values):
    control, prefect = databases(tmp_path)
    with sqlite3.connect(control) as db:
        db.execute(f'INSERT INTO {table} VALUES ({values})')
    with pytest.raises(UnsafeToStart):
        check_idle(control, prefect)


def test_pending_prefect_run_blocks_automatic_service_start(tmp_path):
    control, prefect = databases(tmp_path)
    with sqlite3.connect(prefect) as db:
        db.execute("INSERT INTO flow_run VALUES ('SCHEDULED')")
    with pytest.raises(UnsafeToStart):
        check_idle(control, prefect)


def test_missing_database_blocks_start(tmp_path):
    with pytest.raises(UnsafeToStart):
        check_idle(tmp_path / 'missing-control', tmp_path / 'missing-prefect')
