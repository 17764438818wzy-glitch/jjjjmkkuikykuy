"""Pause/resume evidence boundaries, using synthetic data only."""
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace

import pytest
from prefect.exceptions import ObjectNotFound

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from stage_runtime import Coordinator, StageConflict
from live_runtime import select_folder
from test_stage_runtime import context
from test_batch_next import populate
from batch_next import snapshot_waiting


def with_source_generation(context):
    def verify(identity, generation):
        with sqlite3.connect(context.db) as db:
            source = db.execute('select generation from llm_folder_queue where task_id=? and group_id=?',
                                (identity['task_id'], identity['group_id'])).fetchone()[0]
        return dict(identity=identity, generation=generation, source_llm_generation=source,
                    manifest_sha256='c' * 64, raw_complete=True, session_current=True, not_deleted=True)
    context.coordinator.verify_upstream = verify


def test_resume_uses_new_task_generation_but_preserves_llm_source(context):
    populate(context, 1)
    context.switches['open'] = True
    with_source_generation(context)
    with sqlite3.connect(context.db) as db:
        db.execute('update tasks set start_generation=2')
    assert select_folder(context.db, context.identity['task_id'], context.identity['group_id']) == (context.identity, 2)
    assert snapshot_waiting(context.db, context.identity['task_id'], 8) == [(context.identity, 2)]
    job = context.coordinator.reserve(context.identity, 2)
    assert job['generation'] == 2
    assert job['source_llm_generation'] == 1
    with sqlite3.connect(context.db) as db:
        assert db.execute('select generation from llm_folder_queue').fetchone()[0] == 1


def test_changed_frozen_generation_cannot_be_submitted(context):
    context.switches['open'] = True
    with_source_generation(context)
    with sqlite3.connect(context.db) as db:
        db.execute('update tasks set start_generation=3')
    job = context.coordinator.reserve(context.identity, 3)
    with sqlite3.connect(context.db) as db:
        db.execute('update llm_folder_queue set generation=2')
    calls = []
    client = SimpleNamespace(create_task_run=lambda **kw: calls.append(kw))
    with pytest.raises(StageConflict):
        context.coordinator.submit(job['job_id'], client, object())
    assert calls == []


@pytest.mark.parametrize('source', [0, 4])
def test_invalid_source_generation_is_rejected(context, source):
    context.switches['open'] = True
    with_source_generation(context)
    with sqlite3.connect(context.db) as db:
        db.execute('update tasks set start_generation=3')
        db.execute('update llm_folder_queue set generation=?', (source,))
    with pytest.raises(StageConflict):
        context.coordinator.reserve(context.identity, 3)


def test_old_empty_stage_table_is_migrated_but_nonempty_is_refused(context):
    with sqlite3.connect(context.db) as db:
        columns = [r[1] for r in db.execute('pragma table_info(paperflow_postprocess_stage_jobs)')]
    assert 'source_llm_generation' in columns
    # Simulate a legacy table while preserving the rest of the synthetic DB.
    with sqlite3.connect(context.db) as db:
        db.execute('alter table paperflow_postprocess_stage_jobs drop column source_llm_generation')
        db.execute("insert into paperflow_postprocess_stage_jobs(job_id,task_id,group_id,paper_id,receipt_id,"
                   "attempt_id,parent_run_id,task_run_id,folder_run_id,generation,manifest_sha256,task_key,state,"
                   "created_at_ns,updated_at_ns) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                   ('old', *context.identity.values(), context.identity['task_run_id'], 1, 'c'*64,
                    'old-key', 'reserved', 1, 1))
    with pytest.raises(StageConflict): context.coordinator.initialize()
    with sqlite3.connect(context.db) as db:
        assert 'source_llm_generation' not in [r[1] for r in db.execute('pragma table_info(paperflow_postprocess_stage_jobs)')]
        assert db.execute('select count(*) from paperflow_postprocess_stage_jobs').fetchone()[0] == 1


def test_only_proven_never_submitted_job_rearms_with_same_identity(context):
    context.switches['open'] = True
    with_source_generation(context)
    original = context.coordinator.reserve(context.identity, 1)
    with sqlite3.connect(context.db) as db:
        db.execute("update paperflow_postprocess_stage_jobs set state='stopped_before_submit'")
        db.execute('update tasks set start_generation=2')
    class Client:
        api_url = 'http://prefect.test/api'
        def read_task_run(self, run_id): raise ObjectNotFound(Exception('not found'))
    resumed = context.coordinator.rearm_stopped(original['job_id'], 2, client=Client(),
                                               expected_api_url='http://prefect.test/api')
    assert resumed['job_id'] == original['job_id']
    assert resumed['state'] == 'reserved' and resumed['generation'] == 2
    assert resumed['source_llm_generation'] == 1


@pytest.mark.parametrize('missing', ['postprocessing_claims', 'postprocessing_results'])
def test_missing_evidence_table_never_permits_rearm(context, missing):
    context.switches['open'] = True
    with_source_generation(context)
    original = context.coordinator.reserve(context.identity, 1)
    with sqlite3.connect(context.db) as db:
        db.execute("update paperflow_postprocess_stage_jobs set state='stopped_before_submit'")
        db.execute('update tasks set start_generation=2')
        db.execute('drop table ' + missing)
    class Client:
        api_url = 'http://prefect.test/api'
        def read_task_run(self, run_id): raise ObjectNotFound(Exception('not found'))
    with pytest.raises(StageConflict):
        context.coordinator.rearm_stopped(original['job_id'], 2, client=Client(), expected_api_url='http://prefect.test/api')
    assert context.coordinator.job(original['job_id'])['state'] == 'stopped_before_submit'


@pytest.mark.parametrize('race', ['pause', 'source', 'claimed'])
def test_remote_absence_is_not_enough_if_local_evidence_changes(context, race):
    context.switches['open'] = True
    with_source_generation(context)
    original = context.coordinator.reserve(context.identity, 1)
    with sqlite3.connect(context.db) as db:
        db.execute("update paperflow_postprocess_stage_jobs set state='stopped_before_submit'")
        db.execute('update tasks set start_generation=2')
    class Client:
        api_url = 'http://prefect.test/api'
        def read_task_run(self, run_id):
            with sqlite3.connect(context.db) as db:
                if race == 'pause': db.execute("update tasks set desired_state='paused'")
                if race == 'source': db.execute('update llm_folder_queue set generation=2')
                if race == 'claimed': db.execute('insert into postprocessing_claims values (?,?)',
                                                   (context.identity['task_id'], context.identity['group_id']))
            raise ObjectNotFound(Exception('not found'))
    with pytest.raises(StageConflict):
        context.coordinator.rearm_stopped(original['job_id'], 2, client=Client(), expected_api_url='http://prefect.test/api')
    assert context.coordinator.job(original['job_id'])['state'] == 'stopped_before_submit'


@pytest.mark.parametrize('state,remote', [('submission_unknown', 'missing'), ('cancel_fenced_submitting', 'missing'),
                                           ('stopped_before_submit', 'unknown'), ('stopped_before_submit', 'exists')])
def test_uncertain_or_existing_remote_is_never_rearmed(context, state, remote):
    context.switches['open'] = True
    with_source_generation(context)
    original = context.coordinator.reserve(context.identity, 1)
    with sqlite3.connect(context.db) as db:
        db.execute('update paperflow_postprocess_stage_jobs set state=?', (state,))
        db.execute('update tasks set start_generation=2')
    class Client:
        api_url = 'http://prefect.test/api'
        def read_task_run(self, run_id):
            if remote == 'missing': raise ObjectNotFound(Exception('not found'))
            if remote == 'unknown': raise TimeoutError()
            return object()
    with pytest.raises((StageConflict, TimeoutError)):
        context.coordinator.rearm_stopped(original['job_id'], 2, client=Client(), expected_api_url='http://prefect.test/api')
    assert context.coordinator.job(original['job_id'])['state'] == state
