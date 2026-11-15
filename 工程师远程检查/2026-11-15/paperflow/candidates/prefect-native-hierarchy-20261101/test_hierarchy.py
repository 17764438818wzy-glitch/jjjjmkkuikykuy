import importlib.util
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

root = Path(__file__).resolve().parents[2]
import sys
sys.path[:0] = [str(Path(__file__).resolve().parent),
    str(root / 'candidates/rag-nas-prefect-20261031/tests'),
    str(root / 'candidates/rag-nas-prefect-20261031'),
    str(root / 'candidates/postprocess-rag-unblock-20261101'),
    str(root / 'prefect-paperflow')]

from test_group_stage import control
from group_stage import prefect_task_identity, reserve_group
import group_prefect
import test_rag_nas_positions as cases

spec = importlib.util.spec_from_file_location('hierarchy_positions', Path(__file__).with_name('ticket_positions.py'))
positions = importlib.util.module_from_spec(spec)
spec.loader.exec_module(positions)


class Client:
    def create_task_run(self, *, task, flow_run_id, id, dynamic_key, name, state):
        self.created = SimpleNamespace(id=id, task_key=task.task_key, dynamic_key=dynamic_key,
            tags=task.tags, flow_run_id=flow_run_id)
        return self.created


def test_new_stage_is_attached_to_the_original_folder(control):
    database, ticket, verified = control
    job = reserve_group(database, verified, 5)
    identity = prefect_task_identity('rag', job['rag_task_run_id'])
    task = SimpleNamespace(task_key=identity['task_key'], tags={identity['tag']})
    client = Client()
    group_prefect.submit_stage(database, ticket['task_id'], ticket['group_id'], 'rag', client, task)
    assert client.created.flow_run_id == UUID(ticket['task_run_id'])
    group_prefect.claim_stage(database, ticket['task_id'], ticket['group_id'], 'rag', client.created)


def test_projection_accepts_correct_parent_and_preserves_detached_history():
    value = cases.row('rag', 'running')
    rag = cases.task('rag', 'running')
    runs = {str(rag.id): rag}
    args = (value, cases.identity, cases.job, cases.claim, cases.result, runs)
    assert positions._downstream_position(*args) == 'rag_in_progress'
    rag.flow_run_id = UUID(cases.identity['task_run_id'])
    assert positions._downstream_position(*args) == 'rag_in_progress'
    rag.flow_run_id = uuid4()
    assert positions._downstream_position(*args) is None


def test_real_prefect_stage_attaches_to_paused_folder(control, monkeypatch):
    import asyncio
    from contextlib import closing, nullcontext
    import sqlite3
    from prefect import flow
    from prefect.client.orchestration import get_client
    from prefect.states import Paused
    from prefect.testing.utilities import prefect_test_harness

    database, ticket, verified = control
    @flow(name='synthetic-original-folder')
    def parent():
        pass

    def finish(*_args, **_kwargs):
        with closing(sqlite3.connect(database)) as db, db:
            db.execute("UPDATE paperflow_rag_nas_jobs SET rag_submission='finished' WHERE task_id=? AND group_id=?",
                       (ticket['task_id'], ticket['group_id']))

    monkeypatch.setattr(group_prefect, 'finish_rag', finish)
    with prefect_test_harness():
        with get_client(sync_client=True) as client:
            folder = client.create_flow_run(parent, state=Paused())
        ticket['task_run_id'] = str(folder.id)
        with closing(sqlite3.connect(database)) as db, db:
            for table in ('postprocessing_claims', 'paperflow_postprocess_stage_jobs',
                          'postprocessing_results', 'llm_folder_queue'):
                db.execute('UPDATE ' + table + ' SET task_run_id=?', (ticket['task_run_id'],))
        job = reserve_group(database, verified, 5)
        def operation(_stage, _job):
            return {'rag_receipt_path': 'synthetic', 'copy_receipt_path': 'synthetic'}
        asyncio.run(group_prefect.execute_prefect_stage(database, job, 'rag', operation, lambda _: nullcontext()))
        with get_client(sync_client=True) as client:
            actual = client.read_task_run(UUID(job['rag_task_run_id']))
            parent_after = client.read_flow_run(folder.id)
        assert actual.flow_run_id == folder.id and actual.state.is_completed()
        assert parent_after.state.is_paused()
        with closing(sqlite3.connect(database)) as db:
            assert db.execute('SELECT stage,state FROM paperflow_rag_nas_jobs').fetchone() == ('nas', 'waiting')
