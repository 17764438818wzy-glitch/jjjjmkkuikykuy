from contextlib import nullcontext
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace
from uuid import UUID
import pytest


root = Path(__file__).resolve().parents[3]
rag = root / 'candidates/rag-nas-prefect-20261031'
sys.path[:0] = [str(Path(__file__).parent), str(rag / 'tests'), str(rag), str(root / 'prefect-paperflow')]

from paperflow_prefect.legacy import bind
from paperflow_prefect.settings import Settings
bind(Settings.load())
from test_group_stage import control
from group_stage import FIELDS, prefect_task_identity, reserve_group
import group_prefect
import group_rag_evidence
import isolated_group_operation
from folder_native_client import NativeNotReady
from rag_nas_serve import snapshot


@pytest.mark.parametrize('resource_wait', [False, True])
def test_running_rag_finishes_current_stage_after_parent_pauses(control, tmp_path, monkeypatch, resource_wait):
    database, ticket, handoff = control
    job = reserve_group(database, handoff, 5)
    identity = prefect_task_identity('rag', job['rag_task_run_id'])
    actual = SimpleNamespace(id=UUID(identity['id']), flow_run_id=None,
        task_key=identity['task_key'], dynamic_key=identity['dynamic_key'], tags=[identity['tag']],
        state=SimpleNamespace(is_completed=lambda: True))
    receipt = {'schema': 'paperflow-isolated-group-rag/1', 'status': 'verified', 'group_count': 1,
        'binding': {'ticket': {key: job[key] for key in FIELDS},
                    'postprocess': {key: job[key] for key in
                        ('claim_id', 'postprocess_job_id', 'postprocess_receipt_sha256',
                         'upstream_manifest_sha256', 'source_generation')}}}
    monkeypatch.setattr(group_rag_evidence, 'read_verified_group', lambda *_: (receipt, None))
    calls = []

    def publish(*args):
        admit = args[10]
        with sqlite3.connect(database) as db:
            db.execute("UPDATE tasks SET desired_state='paused'")
        if resource_wait and not calls:
            calls.append('resource wait')
            raise NativeNotReady('synthetic resource wait')
        with admit(None, None) as permit:
            permit.check_current()
            calls.append('stage continued after pause')
        return {'receipt_path': str(tmp_path / 'rag.json')}

    monkeypatch.setattr(isolated_group_operation, 'publish_group', publish)
    context = dict(database=database, copy_receipt_path=str(tmp_path / 'copy.json'), work_root=tmp_path,
        attachments=[], prepared={}, packet={}, plan={}, folder={}, profile_sha256='a' * 64,
        raw=None, journal=None, require_runtime=lambda: True, config=None)
    with sqlite3.connect(database) as db:
        db.execute("UPDATE paperflow_rag_nas_jobs SET rag_submission='submitted'")

    result = group_prefect._run_attempt(database, ticket['task_id'], ticket['group_id'], 'rag', actual,
        lambda stage, current: isolated_group_operation.execute_group_stage(stage, current, context),
        lambda _: nullcontext())
    if resource_wait:
        assert result is None
        assert group_prefect._stage_can_continue(database, ticket['task_id'], ticket['group_id'])
        result = group_prefect._run_attempt(database, ticket['task_id'], ticket['group_id'], 'rag', actual,
            lambda stage, current: isolated_group_operation.execute_group_stage(stage, current, context),
            lambda _: nullcontext())
    assert result is not None, 'an already running RAG stage stopped inside the stage'
    assert calls == (['resource wait'] if resource_wait else []) + ['stage continued after pause']
    group_prefect.advance_stage(database, ticket['task_id'], ticket['group_id'], 'rag', actual)
    with sqlite3.connect(database) as db:
        assert db.execute('SELECT stage,state FROM paperflow_rag_nas_jobs').fetchone() == ('nas', 'waiting')
        assert db.execute('SELECT count(*) FROM paperflow_rag_nas_intervals').fetchone()[0] == 1
    assert snapshot(database, ticket['task_id'])['nas'] == []
    with sqlite3.connect(database) as db:
        db.execute("UPDATE tasks SET desired_state='running', start_generation=6")
    assert [row['group_id'] for row in snapshot(database, ticket['task_id'])['nas']] == [ticket['group_id']]


def test_existing_paused_task_is_available_after_the_parent_restarts(control):
    database, ticket, handoff = control
    reserve_group(database, handoff, 5)
    with sqlite3.connect(database) as db:
        db.execute("UPDATE paperflow_rag_nas_jobs SET state='paused',rag_submission='submitted'")
        db.execute("UPDATE tasks SET desired_state='paused'")
    assert snapshot(database, ticket['task_id'])['rag'] == []
    with sqlite3.connect(database) as db:
        db.execute("UPDATE tasks SET desired_state='running',start_generation=6")
    assert snapshot(database, ticket['task_id'])['rag'] == [ticket['group_id']]
