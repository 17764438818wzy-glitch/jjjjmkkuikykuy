from contextlib import closing
import copy
import importlib.util
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace
from uuid import uuid4

import pytest

root = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(root / 'prefect-paperflow'))
import paperflow_prefect.llm_queue

for module in ('pause_admission', 'folder_ledger'):
    name = 'paperflow_prefect.llm_queue.' + module
    spec = importlib.util.spec_from_file_location(name, Path(__file__).parent / (module + '.py'))
    loaded = importlib.util.module_from_spec(spec)
    sys.modules[name] = loaded
    spec.loader.exec_module(loaded)

from paperflow_prefect.llm_queue.pause_admission import AdmissionGuard, ControlStop
from paperflow_prefect.llm_queue.folder_ledger import FolderLedger
from paperflow_prefect.llm_queue.folder_completion import expected_components
from paperflow_prefect.llm_queue.flow_executor import execute_folder


def setup_stage(tmp_path, state='executing', desired='paused', current_generation=7):
    task, parent, folder, job = (str(uuid4()) for _ in range(4))
    identity = dict(task_id=task, group_id='f' * 32, paper_id='paper-1', receipt_id='e' * 64,
                    attempt_id='attempt-1', parent_run_id=parent, task_run_id=folder)
    manifest = dict(schema='paperflow-local-preprocessing-handoff/1',
        **{key: identity[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id', 'attempt_id')},
        body={'file_id': 'body', 'sha256': 'a' * 64},
        attachments=[{'file_id': 'attachment', 'sha256': 'b' * 64}],
        reading_material={'sha256': 'd' * 64},
        capacity={'status': 'planned', 'mode': 'single', 'parts': 1, 'plan_identity': {
            'schema': 'paperflow-body-input-plan/1', 'source_sha256': 'a' * 64,
            'reading_sha256': 'd' * 64, 'plan_sha256': '5' * 64, 'coverage_sha256': '6' * 64,
            'mode': 'single', 'part_count': 1, 'parts': [{'id': 'part-001', 'request_sha256': '1' * 64}]}})
    database, locks = tmp_path / 'control.sqlite', tmp_path / 'locks'
    locks.mkdir()
    with closing(sqlite3.connect(database)) as db, db:
        db.executescript('CREATE TABLE tasks(task_id TEXT PRIMARY KEY,desired_state TEXT,start_generation INTEGER,'
                         'start_pending INTEGER,task_status TEXT);CREATE TABLE task_deletions(task_id TEXT);'
                         'CREATE TABLE paperflow_llm_stage_jobs(job_id TEXT,task_id TEXT,group_id TEXT,'
                         'folder_run_id TEXT,generation INTEGER,state TEXT);')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?)', (task, desired, current_generation, 0, 'complete'))
        db.execute('INSERT INTO paperflow_llm_stage_jobs VALUES(?,?,?,?,?,?)', (job, task, identity['group_id'], folder, 7, state))
    ledger = FolderLedger(database, execution_task_run_id=job)
    guard = AdmissionGuard(database, locks, execution_task_run_id=job)
    ledger.initialize()
    guard.initialize()
    stop = ControlStop(database, task, 7, execution_task_run_id=job)
    class Upstream:
        def __call__(self, _): return copy.deepcopy(manifest)
        def for_execution(self, _): return copy.deepcopy(manifest)
    return database, identity, manifest, ledger, guard, stop, Upstream()


@pytest.mark.parametrize('current_generation', [7, 8])
def test_claimed_llm_stage_finishes_after_pause_and_waits_for_postprocessing(tmp_path, current_generation):
    db_path, identity, manifest, ledger, guard, stop, upstream = setup_stage(tmp_path, current_generation=current_generation)
    assert not stop.is_set()
    components = expected_components(manifest, identity['task_run_id'])
    sent = []
    class Operations:
        def send(self, component):
            work = tmp_path / component['kind']
            work.mkdir()
            request_sha = component.get('request_sha256', '3' * 64)
            with guard.admit(identity['task_id'], 7, component['component_id'], request_sha,
                    kind=component['kind'], model_dir=work / 'model', work_dir=work,
                    receipt_id=identity['receipt_id'], task_run_id=identity['task_run_id'],
                    source_sha256=component['source_sha256']) as token:
                token.start_transport(lambda: sent.append(component['kind']))
            with sqlite3.connect(db_path) as db:
                db.execute("UPDATE llm_request_admissions SET state='raw_complete',response_sha256=? "
                           'WHERE task_id=? AND component_id=?', ('4' * 64, identity['task_id'], component['component_id']))
        def body(self): self.send(components[0])
        def attachment(self, _): self.send(components[1])
    verifier = SimpleNamespace(complete=lambda *_: None)
    result = execute_folder(identity, 7, ledger, upstream, Operations(), verifier, stop)
    assert result['state'] == 'waiting_postprocessing'
    assert sorted(sent) == ['attachment', 'body']


def test_unclaimed_or_deleted_stage_cannot_start_after_pause(tmp_path):
    db_path, identity, _, ledger, guard, stop, upstream = setup_stage(tmp_path, state='submitted')
    assert stop.is_set()
    ledger.register(identity, upstream)
    with pytest.raises(InterruptedError):
        ledger.begin(identity, 7, upstream)
    with sqlite3.connect(db_path) as db:
        db.execute("UPDATE paperflow_llm_stage_jobs SET state='executing'")
        db.execute('INSERT INTO task_deletions VALUES(?)', (identity['task_id'],))
    assert stop.is_set()
    with pytest.raises(InterruptedError):
        ledger.begin(identity, 7, upstream)
