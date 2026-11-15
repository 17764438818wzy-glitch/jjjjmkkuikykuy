"""Stage ownership tests use only synthetic SQLite databases and local evidence."""
import hashlib
import importlib.util
import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import time
from uuid import uuid4

import pytest


MODULE_PATH = Path(__file__).resolve().parents[1] / 'folder_state.py'
if MODULE_PATH.exists():
    spec = importlib.util.spec_from_file_location('postprocess_folder_state', MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
else:
    module = None


def test_stage_store_api_is_available():
    assert module is not None, 'folder stage store is not implemented'


def running_activity(identity, generation):
    return dict(identity=dict(identity), generation=generation, flow_run_state='RUNNING',
                process_alive=True, observed_at_ns=time.time_ns())


@pytest.fixture
def sample(tmp_path):
    # Missing atomic ownership must fail before implementation exists.
    assert module is not None, 'folder stage store is not implemented'
    identity = dict(task_id=str(uuid4()), group_id='a' * 32, paper_id='paper-1',
                    receipt_id='b' * 64, attempt_id='attempt-1',
                    parent_run_id=str(uuid4()), task_run_id=str(uuid4()))
    database = tmp_path / 'control.sqlite3'
    with sqlite3.connect(database) as db:
        db.execute('CREATE TABLE tasks(task_id TEXT PRIMARY KEY,desired_state TEXT,start_generation INTEGER,'
                   'start_pending INTEGER,task_status TEXT,prefect_run_id TEXT,prefect_order_model TEXT)')
        db.execute('CREATE TABLE task_deletions(task_id TEXT PRIMARY KEY)')
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?)',
                   (identity['task_id'], 'running', 2, 0, 'running', identity['parent_run_id'], 'v2'))
        db.execute('CREATE TABLE llm_folder_queue(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,'
                   'attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,manifest_sha256 TEXT,state TEXT,'
                   'generation INTEGER,PRIMARY KEY(task_id,group_id))')
        db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?,?)',
                   tuple(identity.values()) + ('c' * 64, 'waiting_postprocessing', 2))
    store = module.FolderStageStore(database, observe_run=lambda value: True, observe_activity=running_activity)
    store.initialize()
    return database, identity, store


def control(database, sql, args=()):
    with sqlite3.connect(database, timeout=5) as db:
        db.execute(sql, args)


def receipt(tmp_path, identity, claim_id):
    artifact = tmp_path / 'result.json'
    artifact.write_text('{"validated":true}', encoding='utf8')
    return dict(schema='paperflow-postprocessing-receipt/1', identity=identity,
                manifest_sha256='c' * 64, claim_id=claim_id, validation_status='passed',
                artifacts=[dict(path=str(artifact.resolve()), sha256=hashlib.sha256(artifact.read_bytes()).hexdigest())])


def test_claim_freezes_full_identity_without_consuming_llm_fact(sample):
    database, identity, store = sample
    claim = store.claim(identity, 2, 'c' * 64)
    assert claim['state'] == 'processing'
    assert claim['identity'] == identity
    assert claim['claim_id']
    with sqlite3.connect(database) as db:
        assert db.execute('SELECT state FROM llm_folder_queue').fetchone()[0] == 'waiting_postprocessing'
    assert store.summary() == dict(waiting_postprocessing=0, postprocessing=1, waiting_rag=0, held=0, unresolved=0)


def test_duplicate_concurrent_workers_have_one_owner(sample):
    database, identity, store = sample
    def claim_once(_):
        try:
            return module.FolderStageStore(database, observe_run=lambda value: True).claim(identity, 2, 'c' * 64)
        except module.StageConflict:
            return None
    with ThreadPoolExecutor(max_workers=8) as workers:
        results = list(workers.map(claim_once, range(16)))
    assert sum(value is not None for value in results) == 1


def test_independent_process_workers_share_atomic_ownership(sample):
    database, identity, _ = sample
    script = '''import importlib.util,json,sys
spec=importlib.util.spec_from_file_location('stage',sys.argv[1])
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
store=module.FolderStageStore(sys.argv[2],observe_run=lambda identity:True)
try:
 store.claim(json.loads(sys.argv[3]),2,'c'*64);print('CLAIMED')
except module.StageConflict:
 print('CONFLICT')
'''
    processes = [subprocess.Popen([sys.executable, '-B', '-c', script, str(MODULE_PATH), str(database),
                                   json.dumps(identity)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                 for _ in range(4)]
    results = [process.communicate(timeout=10) for process in processes]
    assert all(process.returncode == 0 for process in processes), results
    assert sorted(stdout.strip() for stdout, _ in results) == ['CLAIMED', 'CONFLICT', 'CONFLICT', 'CONFLICT']


@pytest.mark.parametrize('field,value', [('task_run_id', str(uuid4())), ('receipt_id', 'd' * 64),
                                        ('paper_id', 'different'), ('attempt_id', 'attempt-2')])
def test_claim_rejects_wrong_full_identity(sample, field, value):
    _, identity, store = sample
    with pytest.raises(module.StageConflict):
        store.claim({**identity, field: value}, 2, 'c' * 64)


@pytest.mark.parametrize('mutation,args', [
    ("UPDATE tasks SET desired_state='paused'", ()),
    ("UPDATE tasks SET start_generation=3", ()),
    ("UPDATE tasks SET start_pending=1", ()),
    ("UPDATE tasks SET task_status='deleting'", ()),
    ('INSERT INTO task_deletions VALUES(?)', None),
    ("UPDATE llm_folder_queue SET state='llm_processing'", ()),
])
def test_claim_requires_active_control_and_llm_completion(sample, mutation, args):
    database, identity, store = sample
    control(database, mutation, (identity['task_id'],) if args is None else args)
    with pytest.raises(module.StageConflict):
        store.claim(identity, 2, 'c' * 64)


def test_claim_rejects_wrong_manifest_digest(sample):
    _, identity, store = sample
    with pytest.raises(module.StageConflict):
        store.claim(identity, 2, 'd' * 64)


def test_unobserved_flow_is_not_admitted_or_reported_as_zero(sample):
    database, identity, _ = sample
    store = module.FolderStageStore(database, observe_run=lambda value: None)
    with pytest.raises(module.StageConflict):
        store.claim(identity, 2, 'c' * 64)
    assert all(value is None for value in store.summary().values())


def test_valid_receipt_requires_actual_artifact_and_acceptance(sample, tmp_path):
    _, identity, store = sample
    claim = store.claim(identity, 2, 'c' * 64)
    proof = receipt(tmp_path, identity, claim['claim_id'])
    with pytest.raises(module.StageConflict):
        store.complete(identity, claim['claim_id'], proof, validate_receipt=lambda value: False)
    result = store.complete(identity, claim['claim_id'], proof, validate_receipt=lambda value: True)
    assert result['state'] == 'waiting_rag'
    assert store.summary() == dict(waiting_postprocessing=0, postprocessing=0, waiting_rag=1, held=0, unresolved=0)
    assert store.complete(identity, claim['claim_id'], proof, validate_receipt=lambda value: True) == result


@pytest.mark.parametrize('damage', ['artifact', 'hash', 'identity', 'owner', 'manifest', 'missing_validator'])
def test_completion_cannot_publish_wrong_or_unverified_receipt(sample, tmp_path, damage):
    _, identity, store = sample
    claim = store.claim(identity, 2, 'c' * 64)
    proof = receipt(tmp_path, identity, claim['claim_id'])
    validator = lambda value: True
    if damage == 'artifact':
        Path(proof['artifacts'][0]['path']).write_text('changed', encoding='utf8')
    elif damage == 'hash':
        proof['artifacts'][0]['sha256'] = 'd' * 64
    elif damage == 'identity':
        proof['identity'] = {**identity, 'paper_id': 'other'}
    elif damage == 'owner':
        proof['claim_id'] = str(uuid4())
    elif damage == 'manifest':
        proof['manifest_sha256'] = 'd' * 64
    else:
        validator = None
    with pytest.raises(module.StageConflict):
        store.complete(identity, claim['claim_id'], proof, validate_receipt=validator)
    assert store.summary()['waiting_rag'] == 0


@pytest.mark.parametrize('deleted', [False, True])
def test_pause_or_delete_during_validation_keeps_safe_receipt(sample, tmp_path, deleted):
    database, identity, store = sample
    claim = store.claim(identity, 2, 'c' * 64)
    proof = receipt(tmp_path, identity, claim['claim_id'])
    def accept(value):
        control(database, 'INSERT INTO task_deletions VALUES(?)', (identity['task_id'],)) if deleted else control(
            database, "UPDATE tasks SET desired_state='paused'")
        return True
    result = store.complete(identity, claim['claim_id'], proof, validate_receipt=accept)
    assert result['state'] == 'receipt_held'
    with sqlite3.connect(database) as db:
        assert db.execute('SELECT COUNT(*) FROM postprocessing_receipts').fetchone()[0] == 1
    assert store.summary()['waiting_rag'] == 0


def test_resume_reconciles_saved_receipt_without_reclaim_or_overwrite(sample, tmp_path):
    database, identity, store = sample
    claim = store.claim(identity, 2, 'c' * 64)
    proof = receipt(tmp_path, identity, claim['claim_id'])
    control(database, "UPDATE tasks SET desired_state='paused'")
    store.complete(identity, claim['claim_id'], proof, validate_receipt=lambda value: True)
    control(database, "UPDATE tasks SET desired_state='running',start_generation=3")
    with pytest.raises(module.StageConflict):
        store.claim(identity, 3, 'c' * 64)
    assert store.reconcile(identity, claim['claim_id'], 3, validate_receipt=lambda value: True)['state'] == 'waiting_rag'
    with pytest.raises(module.StageConflict):
        store.complete(identity, claim['claim_id'], {**proof, 'extra': 'overwrite'}, validate_receipt=lambda value: True)


def test_crash_unknown_is_never_reclaimed_or_forged_complete(sample, tmp_path):
    database, identity, store = sample
    claim = store.claim(identity, 2, 'c' * 64)
    restarted = module.FolderStageStore(database, observe_run=lambda value: True)
    with pytest.raises(module.StageConflict):
        restarted.claim(identity, 2, 'c' * 64)
    assert restarted.mark_unknown(identity, claim['claim_id'], 'process disappeared')['state'] == 'unknown'
    assert restarted.summary()['unresolved'] == 1
    with pytest.raises(module.StageConflict):
        restarted.claim(identity, 2, 'c' * 64)
    with pytest.raises(module.StageConflict):
        restarted.reconcile(identity, claim['claim_id'], 2, validate_receipt=lambda value: True)


def test_read_only_summary_missing_tables_or_corruption_returns_unknown(tmp_path, sample):
    missing = tmp_path / 'absent.sqlite3'
    store = module.FolderStageStore(missing, observe_run=lambda value: True)
    assert all(value is None for value in store.summary().values())
    assert not missing.exists()
    database, _, store = sample
    control(database, "UPDATE llm_folder_queue SET state='nonsense'")
    assert all(value is None for value in store.summary().values())


def test_summary_counts_unclaimed_folders_and_does_not_write(sample):
    database, _, store = sample
    before = database.read_bytes()
    assert store.summary()['waiting_postprocessing'] == 1
    assert database.read_bytes() == before


def test_empty_summary_without_runtime_observer_is_unknown(sample):
    database, _, _ = sample
    control(database, 'DELETE FROM llm_folder_queue')
    store = module.FolderStageStore(database)
    assert all(value is None for value in store.summary().values())


@pytest.mark.parametrize('damage', ['missing_artifact', 'receipt_identity', 'receipt_digest'])
def test_summary_does_not_count_unverifiable_published_evidence(sample, tmp_path, damage):
    database, identity, store = sample
    claim = store.claim(identity, 2, 'c' * 64)
    proof = receipt(tmp_path, identity, claim['claim_id'])
    store.complete(identity, claim['claim_id'], proof, validate_receipt=lambda value: True)
    if damage == 'missing_artifact':
        Path(proof['artifacts'][0]['path']).unlink()
    elif damage == 'receipt_digest':
        control(database, "UPDATE postprocessing_receipts SET receipt_sha256=?", ('d' * 64,))
    else:
        proof['identity'] = {**identity, 'paper_id': 'another'}
        encoded = json.dumps(proof, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        control(database, 'UPDATE postprocessing_receipts SET receipt_json=?,receipt_sha256=?',
                (encoded, hashlib.sha256(encoded.encode('utf8')).hexdigest()))
    assert all(value is None for value in store.summary().values())


def test_pause_transaction_wins_before_claim_and_short_gate_is_released(sample):
    database, identity, _ = sample
    gate_lock = threading.Lock()
    entered = threading.Event()
    @contextmanager
    def gate(task_id):
        with gate_lock:
            entered.set()
            yield
    store = module.FolderStageStore(database, observe_run=lambda value: True, admission_gate=gate)
    gate_lock.acquire()
    with ThreadPoolExecutor(max_workers=1) as worker:
        future = worker.submit(store.claim, identity, 2, 'c' * 64)
        control(database, "UPDATE tasks SET desired_state='paused'")
        gate_lock.release()
        with pytest.raises(module.StageConflict):
            future.result(timeout=5)
    assert entered.is_set()
    assert gate_lock.acquire(timeout=1)
    gate_lock.release()


def test_in_progress_pause_sqlite_transaction_blocks_then_rejects_claim(sample):
    database, identity, store = sample
    db = sqlite3.connect(database)
    db.execute('BEGIN IMMEDIATE')
    db.execute("UPDATE tasks SET desired_state='paused'")
    began = threading.Event()
    def try_claim():
        began.set()
        return store.claim(identity, 2, 'c' * 64)
    try:
        with ThreadPoolExecutor(max_workers=1) as worker:
            pending = worker.submit(try_claim)
            assert began.wait(timeout=2)
            db.commit()
            with pytest.raises(module.StageConflict):
                pending.result(timeout=5)
    finally:
        db.close()


def test_unknown_claim_cannot_be_completed_by_a_late_owner(sample, tmp_path):
    _, identity, store = sample
    claimed = store.claim(identity, 2, 'c' * 64)
    store.mark_unknown(identity, claimed['claim_id'], 'unconfirmed interruption')
    with pytest.raises(module.StageConflict):
        store.complete(identity, claimed['claim_id'], receipt(tmp_path, identity, claimed['claim_id']),
                       validate_receipt=lambda value: True)


def test_receipt_validator_cannot_mutate_accepted_proof(sample, tmp_path):
    _, identity, store = sample
    claimed = store.claim(identity, 2, 'c' * 64)
    def changes_proof(proof):
        proof['artifacts'] = []
        return True
    with pytest.raises(module.StageConflict):
        store.complete(identity, claimed['claim_id'], receipt(tmp_path, identity, claimed['claim_id']),
                       validate_receipt=changes_proof)


def test_metadata_resume_requires_current_generation_and_untampered_saved_proof(sample, tmp_path):
    database, identity, store = sample
    claimed = store.claim(identity, 2, 'c' * 64)
    control(database, "UPDATE tasks SET desired_state='paused'")
    proof = receipt(tmp_path, identity, claimed['claim_id'])
    store.complete(identity, claimed['claim_id'], proof, validate_receipt=lambda value: True)
    control(database, "UPDATE tasks SET desired_state='running',start_generation=3")
    with pytest.raises(module.StageConflict):
        store.reconcile(identity, claimed['claim_id'], 2, validate_receipt=lambda value: True)
    Path(proof['artifacts'][0]['path']).write_text('corrupted', encoding='utf8')
    with pytest.raises(module.StageConflict):
        store.reconcile(identity, claimed['claim_id'], 3, validate_receipt=lambda value: True)


def test_slow_acceptance_never_holds_admission_gate(sample, tmp_path):
    database, identity, _ = sample
    gate_lock = threading.Lock()
    @contextmanager
    def gate(task_id):
        with gate_lock:
            yield
    store = module.FolderStageStore(database, observe_run=lambda value: True, admission_gate=gate)
    claim = store.claim(identity, 2, 'c' * 64)
    def accept(value):
        assert gate_lock.acquire(timeout=1), 'semantic validator ran under the task gate'
        gate_lock.release()
        # A concurrent control transaction must also be able to acquire SQLite.
        control(database, "UPDATE tasks SET desired_state='paused'")
        return True
    assert store.complete(identity, claim['claim_id'], receipt(tmp_path, identity, claim['claim_id']),
                          validate_receipt=accept)['state'] == 'receipt_held'


def test_processing_identity_alone_is_not_liveness_and_never_reclaims(sample):
    database, identity, store = sample
    claim = store.claim(identity, 2, 'c' * 64)
    restarted = module.FolderStageStore(database, observe_run=lambda value: True)
    before = database.read_bytes()
    counts = restarted.summary()
    assert counts['postprocessing'] is None
    assert counts['waiting_postprocessing'] == counts['waiting_rag'] == 0
    assert database.read_bytes() == before
    with pytest.raises(module.StageConflict):
        restarted.claim(identity, 2, 'c' * 64)
    with sqlite3.connect(database) as db:
        assert db.execute('SELECT claim_id,state FROM postprocessing_claims').fetchone() == (claim['claim_id'], 'processing')


@pytest.mark.parametrize('change', [
    {'flow_run_state': 'PAUSED'}, {'flow_run_state': 'SUSPENDED'}, {'flow_run_state': 'COMPLETED'},
    {'process_alive': False}, {'process_alive': None}, {'generation': 3},
    {'observed_at_ns': 0}, {'observed_at_ns': time.time_ns() - 60_000_000_000},
    {'observed_at_ns': time.time_ns() + 600_000_000_000},
    {'identity': {}},
])
def test_processing_requires_fresh_bound_running_process_evidence(sample, change):
    _, identity, store = sample
    store.claim(identity, 2, 'c' * 64)
    store.observe_activity = lambda value, generation: {**running_activity(value, generation), **change}
    assert store.summary()['postprocessing'] is None


@pytest.mark.parametrize('observation', [None, True, {}, {'flow_run_state': 'RUNNING'}])
def test_unavailable_or_boolean_activity_is_not_reliable_running_evidence(sample, observation):
    _, identity, store = sample
    store.claim(identity, 2, 'c' * 64)
    store.observe_activity = lambda value, generation: observation
    assert store.summary()['postprocessing'] is None


def test_activity_observer_failure_is_unknown_not_zero(sample):
    _, identity, store = sample
    store.claim(identity, 2, 'c' * 64)
    def unavailable(value, generation):
        raise ConnectionError('synthetic observer unavailable')
    store.observe_activity = unavailable
    assert store.summary()['postprocessing'] is None


@pytest.mark.parametrize('mutation', [
    "UPDATE tasks SET desired_state='paused'", 'UPDATE tasks SET start_generation=3',
    'UPDATE tasks SET start_pending=1', "UPDATE tasks SET task_status='unknown'",
])
def test_processing_liveness_cannot_override_paused_or_changed_task_control(sample, mutation):
    database, identity, store = sample
    store.claim(identity, 2, 'c' * 64)
    control(database, mutation)
    assert store.summary()['postprocessing'] is None


@pytest.mark.parametrize('mutation', [
    "UPDATE tasks SET desired_state='paused'", 'UPDATE tasks SET start_generation=3',
])
def test_control_is_rechecked_after_external_activity_observation(sample, mutation):
    database, identity, store = sample
    store.claim(identity, 2, 'c' * 64)
    def pause_during_observation(value, generation):
        control(database, mutation)
        return running_activity(value, generation)
    store.observe_activity = pause_during_observation
    assert store.summary()['postprocessing'] is None


def test_known_zero_without_processing_does_not_need_activity_callback(sample):
    _, _, store = sample
    store.observe_activity = None
    counts = store.summary()
    assert counts == dict(waiting_postprocessing=1, postprocessing=0, waiting_rag=0, held=0, unresolved=0)


def test_verified_waiting_rag_survives_other_unknown_processing_liveness(sample, tmp_path):
    database, identity, store = sample
    claimed = store.claim(identity, 2, 'c' * 64)
    store.complete(identity, claimed['claim_id'], receipt(tmp_path, identity, claimed['claim_id']),
                   validate_receipt=lambda value: True)
    other = {**identity, 'group_id': 'd' * 32, 'paper_id': 'paper-2', 'task_run_id': str(uuid4())}
    control(database, 'INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?,?)',
            tuple(other[field] for field in module.IDENTITY) + ('c' * 64, 'waiting_postprocessing', 2))
    store.claim(other, 2, 'c' * 64)
    store.observe_activity = None
    counts = store.summary()
    assert counts['postprocessing'] is None
    assert counts['waiting_rag'] == 1


def test_fresh_real_running_observation_confirms_processing_count(sample):
    _, identity, store = sample
    store.claim(identity, 2, 'c' * 64)
    observations = []
    def observe(value, generation):
        observations.append((dict(value), generation))
        return running_activity(value, generation)
    store.observe_activity = observe
    assert store.summary()['postprocessing'] == 1
    assert observations == [(identity, 2)]
