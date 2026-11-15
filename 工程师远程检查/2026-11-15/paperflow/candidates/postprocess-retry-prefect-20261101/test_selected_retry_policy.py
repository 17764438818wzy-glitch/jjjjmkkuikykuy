import asyncio
import importlib.util
from pathlib import Path
import sqlite3
import sys
from uuid import uuid4

import pytest

root = Path(__file__).resolve().parents[2]
candidate = Path(__file__).parent
server = root / 'candidates/postprocess-server-20261101'
sys.path[:0] = [str(candidate), str(server / 'tests'), str(server)]
import test_selected_ticket_probe as original
from test_remote_folder_config import fixture as config_fixture


def load(name):
    spec = importlib.util.spec_from_file_location('candidate_' + name, candidate / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def case(tmp_path, monkeypatch):
    monkeypatch.setattr(original, 'module', lambda: load('selected_ticket_probe'))
    return original.case.__wrapped__(tmp_path, monkeypatch)


def test_retry_policy_reads_original_folder_without_removing_previous_history(case):
    old_job = str(uuid4())
    with sqlite3.connect(case.database) as db:
        db.execute('ALTER TABLE paperflow_postprocess_stage_jobs ADD COLUMN job_id TEXT')
        db.execute('ALTER TABLE paperflow_postprocess_stage_jobs ADD COLUMN state TEXT')
        db.execute('ALTER TABLE paperflow_postprocess_stage_jobs ADD COLUMN generation INTEGER')
        db.execute('INSERT INTO paperflow_postprocess_stage_jobs VALUES(?,?,?,?,?)',
                   (case.task_id, case.inputs['identity']['group_id'], old_job, 'execution_unknown', 1))
        db.execute('UPDATE tasks SET start_generation=2')
    before = case.database.read_bytes()
    record = case.module.inspect_exact_folder(sources=case.sources, identity=case.inputs['identity'],
                                               loader=case.loader, retry_job_id=old_job)
    assert record['identity'] == case.inputs['identity'] and record['retry_job_id'] == old_job
    assert case.database.read_bytes() == before


@pytest.mark.parametrize(('state', 'stage_generation'), [
    ('reserved', 2), ('reserved', 1), ('stopped_before_submit', 1),
    ('submitting', 1), ('submitted', 1)])
def test_policy_accepts_the_same_unclaimed_stage(case, state, stage_generation):
    identity, job = case.inputs['identity'], str(uuid4())
    fields = ('paper_id', 'receipt_id', 'attempt_id', 'parent_run_id', 'task_run_id', 'folder_run_id',
              'manifest_sha256', 'owner_pid', 'owner_started', 'error_type', 'job_id', 'state')
    with sqlite3.connect(case.database) as db:
        for field in fields:
            db.execute('ALTER TABLE paperflow_postprocess_stage_jobs ADD COLUMN ' + field + ' TEXT')
        for field in ('generation', 'source_llm_generation'):
            db.execute('ALTER TABLE paperflow_postprocess_stage_jobs ADD COLUMN ' + field + ' INTEGER')
        db.execute('UPDATE tasks SET start_generation=2')
        row = {**identity, 'folder_run_id': identity['task_run_id'], 'manifest_sha256': case.inputs['manifest_sha256'],
               'job_id': job, 'state': state, 'generation': stage_generation, 'source_llm_generation': 1,
               'owner_pid': None, 'owner_started': None, 'error_type': None}
        columns = ('task_id', 'group_id', *fields, 'generation', 'source_llm_generation')
        db.execute('INSERT INTO paperflow_postprocess_stage_jobs (' + ','.join(columns) + ') VALUES (' +
                   ','.join('?' for _ in columns) + ')', [row[column] for column in columns])
    record = case.module.inspect_exact_folder(sources=case.sources, identity=identity,
                                               loader=case.loader, resume_job_id=job)
    assert record['identity'] == identity and record['resume_job_id'] == job and record['generation'] == 2


def test_launcher_passes_previous_attempt_to_the_same_folder_stage():
    launcher_module = load('scoped_task_launcher')
    template, record, identity = config_fixture()
    old_job = str(uuid4())
    record['retry_job_id'] = old_job
    class Runtime:
        execution_enabled = True
        def check_configuration(self): pass
    seen = []
    def read_policy(actual, generation, *, retry_job_id):
        seen.append(('policy', actual, retry_job_id))
        return record
    async def run_exact(runtime, actual, generation, *, retry_job_id):
        seen.append(('stage', actual, retry_job_id))
        return {'state': 'finished'}
    launcher = launcher_module.ScopedTaskLauncher(Runtime(), template, {'counter': 'approved'},
        read_policy=read_policy, make_remote=lambda config: config, run_exact=run_exact)
    prepared = launcher.prepare(identity, 2, retry_job_id=old_job)
    assert asyncio.run(launcher.launch(identity, 2, prepared)) == 'finished'
    assert seen == [('policy', identity, old_job), ('stage', identity, old_job)]
