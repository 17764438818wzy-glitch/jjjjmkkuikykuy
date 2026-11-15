"""Exercise folder-level queue transitions without a model or live Prefect."""
from contextlib import closing
import copy
import importlib.util
from pathlib import Path
import sqlite3
import sys
from uuid import uuid4

import pytest


HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
spec = importlib.util.spec_from_file_location('folder_ledger', HERE / 'folder_ledger.py')
ledger_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ledger_module)
completion_spec = importlib.util.spec_from_file_location('folder_completion', HERE / 'folder_completion.py')
completion = importlib.util.module_from_spec(completion_spec)
completion_spec.loader.exec_module(completion)


class FakeVerifier:
    def __init__(self):
        self.proofs = []

    def complete(self, task_id, component_id, response_sha256):
        self.proofs.append((task_id, component_id, response_sha256))


@pytest.fixture
def context(tmp_path):
    task = str(uuid4())
    ticket = str(uuid4())
    parent = str(uuid4())
    group = 'f' * 32
    identity = {'task_id': task, 'group_id': group, 'paper_id': 'paper-1',
                'receipt_id': 'e' * 64, 'attempt_id': 'attempt-1',
                'parent_run_id': parent, 'task_run_id': ticket}
    manifest = {'schema': 'paperflow-local-preprocessing-handoff/1',
                'task_id': task, 'group_id': group, 'paper_id': 'paper-1',
                'receipt_id': 'e' * 64, 'attempt_id': 'attempt-1',
                'body': {'file_id': 'body-1', 'sha256': 'a' * 64},
                'attachments': [{'file_id': 'attachment-1', 'sha256': 'b' * 64}],
                'reading_material': {'sha256': 'd' * 64},
                'capacity': {'status': 'planned', 'mode': 'partitioned', 'parts': 2,
                             'plan_identity': {'schema': 'paperflow-body-input-plan/1',
                                 'source_sha256': 'a' * 64, 'reading_sha256': 'd' * 64,
                                 'plan_sha256': '5' * 64, 'coverage_sha256': '6' * 64,
                                 'mode': 'partitioned', 'part_count': 2,
                                 'parts': [{'id': 'part-001', 'request_sha256': '1' * 64},
                                           {'id': 'part-002', 'request_sha256': '2' * 64}]}}}
    database = tmp_path / 'control.sqlite'
    locks = tmp_path / 'locks'
    locks.mkdir()
    with closing(sqlite3.connect(database)) as db, db:
        db.executescript('''
            CREATE TABLE tasks(task_id TEXT PRIMARY KEY, desired_state TEXT,
                               start_generation INTEGER, start_pending INTEGER, task_status TEXT);
            CREATE TABLE task_deletions(task_id TEXT PRIMARY KEY);
            CREATE TABLE llm_request_admissions (
                task_id TEXT NOT NULL, component_id TEXT NOT NULL, generation INTEGER NOT NULL,
                request_sha256 TEXT NOT NULL, state TEXT NOT NULL, response_sha256 TEXT,
                kind TEXT NOT NULL, work_dir TEXT NOT NULL, model_dir TEXT NOT NULL, receipt_id TEXT NOT NULL,
                task_run_id TEXT NOT NULL, source_sha256 TEXT NOT NULL,
                created_at_ns INTEGER NOT NULL, updated_at_ns INTEGER NOT NULL,
                PRIMARY KEY(task_id, component_id));
        ''')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?)', (task, 'running', 7, 0, 'complete'))
    ledger = ledger_module.FolderLedger(database, locks)
    ledger.initialize()
    verifier = FakeVerifier()
    checked = []
    class VerifyUpstream:
        def __call__(self, given):
            checked.append(given.copy())
            assert given == identity
            return copy.deepcopy(manifest)
        def for_execution(self, given):
            return self(given)
    return ledger, database, identity, manifest, verifier, VerifyUpstream()


def insert_response(database, identity, component, state='raw_complete'):
    with closing(sqlite3.connect(database)) as db, db:
        db.execute('INSERT INTO llm_request_admissions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (identity['task_id'], component['component_id'], 7,
                    component.get('request_sha256') or '3' * 64, state,
                    '4' * 64 if state == 'raw_complete' else None,
                    component['kind'], 'work', 'model', identity['receipt_id'],
                    identity['task_run_id'], component['source_sha256'], 1, 1))


def test_folder_moves_only_after_all_raw_responses_are_verified(context):
    ledger, database, identity, manifest, verifier, upstream = context
    assert ledger.register(identity, upstream)['state'] == 'waiting_llm'
    assert ledger.register(identity, upstream)['state'] == 'waiting_llm'
    assert ledger.begin(identity, 7, upstream)['state'] == 'llm_processing'
    components = completion.expected_components(manifest, identity['task_run_id'])
    for component in components[:-1]:
        insert_response(database, identity, component)
    assert ledger.finish(identity, upstream, verifier)['state'] == 'llm_processing'
    assert ledger.counts(identity['task_id'])['waiting_postprocessing'] == 0
    insert_response(database, identity, components[-1])
    ready = ledger.finish(identity, upstream, verifier)
    assert ready['state'] == 'waiting_postprocessing'
    assert ledger.finish(identity, upstream, verifier)['state'] == 'waiting_postprocessing'
    assert ledger.counts(identity['task_id'])['waiting_postprocessing'] == 1
    assert {p[1] for p in verifier.proofs} == {c['component_id'] for c in components}


def test_pause_prevents_begin_and_changed_handoff_cannot_advance(context):
    ledger, database, identity, manifest, verifier, upstream = context
    ledger.register(identity, upstream)
    with closing(sqlite3.connect(database)) as db, db:
        db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (identity['task_id'],))
    with pytest.raises(InterruptedError):
        ledger.begin(identity, 7, upstream)
    with closing(sqlite3.connect(database)) as db, db:
        db.execute("UPDATE tasks SET desired_state='running' WHERE task_id=?", (identity['task_id'],))
    with pytest.raises(InterruptedError):
        ledger.begin(identity, None, upstream)
    ledger.begin(identity, 7, upstream)
    manifest['capacity']['plan_identity']['parts'][0]['request_sha256'] = '9' * 64
    with pytest.raises(ValueError, match='交接'):
        ledger.finish(identity, upstream, verifier)
    assert ledger.counts(identity['task_id'])['waiting_postprocessing'] == 0


def test_unknown_never_advances_and_work_counts_are_per_folder(context):
    ledger, database, identity, manifest, verifier, upstream = context
    ledger.register(identity, upstream)
    ledger.begin(identity, 7, upstream)
    components = completion.expected_components(manifest, identity['task_run_id'])
    insert_response(database, identity, components[0], 'admitted')
    insert_response(database, identity, components[1], 'returned_unverified')
    insert_response(database, identity, components[2], 'unknown')
    counts = ledger.counts(identity['task_id'])
    assert counts['body_folders_in_progress'] == 1
    assert counts['attachment_folders_in_progress'] is None
    assert counts['unknown_requests'] == 1
    assert ledger.finish(identity, upstream, verifier)['state'] == 'llm_processing'


def test_delete_or_changed_ticket_never_completes(context):
    ledger, database, identity, manifest, verifier, upstream = context
    ledger.register(identity, upstream)
    ledger.begin(identity, 7, upstream)
    for component in completion.expected_components(manifest, identity['task_run_id']):
        insert_response(database, identity, component)
    changed = dict(identity, task_run_id=str(uuid4()))
    with pytest.raises((ValueError, AssertionError)):
        ledger.finish(changed, upstream, verifier)
    with closing(sqlite3.connect(database)) as db, db:
        db.execute('INSERT INTO task_deletions VALUES(?)', (identity['task_id'],))
    with pytest.raises(InterruptedError):
        ledger.finish(identity, upstream, verifier)


def test_corrupt_response_does_not_enter_waiting_postprocessing(context):
    ledger, database, identity, manifest, _, upstream = context
    ledger.register(identity, upstream)
    ledger.begin(identity, 7, upstream)
    for component in completion.expected_components(manifest, identity['task_run_id']):
        insert_response(database, identity, component)
    class CorruptVerifier:
        def complete(self, *args):
            raise ValueError('原始响应证据损坏')
    with pytest.raises(ValueError, match='证据损坏'):
        ledger.finish(identity, upstream, CorruptVerifier())
    assert ledger.counts(identity['task_id'])['waiting_postprocessing'] == 0


def test_pause_freezes_folder_until_the_user_resumes(context):
    ledger, database, identity, manifest, verifier, upstream = context
    ledger.register(identity, upstream)
    ledger.begin(identity, 7, upstream)
    for component in completion.expected_components(manifest, identity['task_run_id']):
        insert_response(database, identity, component)
    with closing(sqlite3.connect(database)) as db, db:
        db.execute("UPDATE tasks SET desired_state='paused' WHERE task_id=?", (identity['task_id'],))
    with pytest.raises(InterruptedError):
        ledger.finish(identity, upstream, verifier)
    assert ledger.counts(identity['task_id'])['waiting_postprocessing'] == 0
    with closing(sqlite3.connect(database)) as db, db:
        db.execute("UPDATE tasks SET desired_state='running',start_generation=8 WHERE task_id=?", (identity['task_id'],))
    with pytest.raises(InterruptedError):
        ledger.finish(identity, upstream, verifier)
    ledger.begin(identity, 8, upstream)
    assert ledger.finish(identity, upstream, verifier)['state'] == 'waiting_postprocessing'
