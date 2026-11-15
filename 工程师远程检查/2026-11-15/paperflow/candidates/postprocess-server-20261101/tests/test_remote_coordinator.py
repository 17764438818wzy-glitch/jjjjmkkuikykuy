"""Only temporary control databases, synthetic receipts and injected transports."""
import copy
from contextlib import contextmanager
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
from uuid import uuid4

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT.parent / 'postprocess-prefect-20261031'))
from folder_state import FolderStageStore, StageConflict


def coordinator():
    spec = importlib.util.spec_from_file_location('remote_coordinator_under_test', ROOT / 'remote_coordinator.py')
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


@pytest.fixture
def sample(tmp_path):
    identity = dict(task_id=str(uuid4()), group_id='a' * 32, paper_id='synthetic',
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
                   'attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,manifest_sha256 TEXT,state TEXT)')
        db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?)',
                   tuple(identity.values()) + ('c' * 64, 'waiting_postprocessing'))
    gate_depth = [0]
    @contextmanager
    def gate(task_id):
        assert task_id == identity['task_id']
        gate_depth[0] += 1
        try:
            yield
        finally:
            gate_depth[0] -= 1
    store = FolderStageStore(database, observe_run=lambda _: True, admission_gate=gate)
    store.initialize()
    module = coordinator()
    server = {'server_id': 'synthetic-server', 'ledger_id': str(uuid4())}
    journal = module.RemoteSubmissionJournal(tmp_path / 'remote-intents.sqlite3',
        control_database=database, admission_gate=gate)
    journal.initialize()
    calls = []
    def prepare(claim):
        assert state() == 'processing'  # persist ownership before any snapshot work
        calls.append('prepare')
        return dict(snapshot_sha256='d' * 64, code_sha256='e' * 64, payload={'synthetic': True})
    def transfer(job, snapshot):
        assert gate_depth[0] == 0
        frozen = journal.get_transfer(job['job_id'])
        assert frozen['job'] == job and frozen['server_identity'] == server
        assert journal.get(job['job_id']) is None
        for path in (journal.database, database):
            with sqlite3.connect(path, timeout=0) as db:
                db.execute('BEGIN IMMEDIATE')
        calls.append('transfer')
        return dict(job=copy.deepcopy(job), server_identity=copy.deepcopy(server), state='staged',
                    snapshot_sha256=job['snapshot_sha256'], code_sha256=job['code_sha256'],
                    payload=copy.deepcopy(snapshot['payload']), staging_receipt={'synthetic_authenticated': True})
    def validate_staging(job, snapshot, staging):
        assert gate_depth[0] == 0
        calls.append('staging-validated')
        return staging['staging_receipt'] == {'synthetic_authenticated': True}
    def submit(job, snapshot, staging):
        assert gate_depth[0] == 1
        assert snapshot['server_identity'] == server
        intent = journal.get(job['job_id'])
        assert intent['job'] == job and intent['server_identity'] == server
        assert intent['staging'] == staging
        # Remote submission must not hold a journal/control write transaction.
        for path in (journal.database, database):
            with sqlite3.connect(path, timeout=0) as db:
                db.execute('BEGIN IMMEDIATE')
        calls.append('submit')
        return {'job': copy.deepcopy(job), 'server_identity': copy.deepcopy(server), 'accepted': True}
    def wait(job):
        assert gate_depth[0] == 0
        calls.append('wait')
        artifact = tmp_path / 'artifact.json'
        artifact.write_text('{"synthetic":true}', encoding='utf8')
        receipt = dict(schema='paperflow-postprocessing-receipt/1', identity=copy.deepcopy(identity),
                       manifest_sha256='c' * 64, claim_id=job['claim_id'], validation_status='passed',
                       body=dict(package_path=str(tmp_path / 'synthetic-publication'), manifest_sha256='f' * 64),
                       attachments=[],
                       artifacts=[dict(path=str(artifact.resolve()),
                           sha256=hashlib.sha256(artifact.read_bytes()).hexdigest())])
        raw = encoded(receipt)
        return dict(job=copy.deepcopy(job), state='finished', server_validation='passed', receipt=receipt,
                    receipt_json=raw, receipt_sha256=hashlib.sha256(raw.encode('utf8')).hexdigest(),
                    server_identity=copy.deepcopy(server),
                    server_receipt={'ledger_id': 'synthetic-authenticated-ledger'})
    def validate(job, result):
        assert gate_depth[0] == 0
        calls.append('validate')
        return result['server_receipt'] == {'ledger_id': 'synthetic-authenticated-ledger'}
    def observe(claim, job, clock):
        assert gate_depth[0] == 0
        return dict(identity=copy.deepcopy(identity), generation=2, claim_id=claim['claim_id'],
                    job_id=job['job_id'], state='RUNNING', observed_at_ns=time.time_ns(),
                    clock=copy.deepcopy(clock),
                    binding={'stage_version': 11, 'owner': 'synthetic-worker'})
    def bind(claim, job, evidence):
        assert gate_depth[0] == 1
        return evidence['binding'] == {'stage_version': 11, 'owner': 'synthetic-worker'}
    def state():
        with sqlite3.connect(database) as db:
            row = db.execute('SELECT state FROM postprocessing_claims').fetchone()
            return row[0] if row else None
    def run(**overrides):
        options = dict(identity=identity, generation=2, manifest_sha256='c' * 64, store=store,
                       prepare_snapshot=prepare, submit_once=submit, wait_for_result=wait,
                       validate_result=validate, journal=journal, server_identity=server,
                       observe_execution=observe, bind_execution=bind,
                       transfer_snapshot=transfer, validate_staging=validate_staging)
        options.update(overrides)
        return module.process_remote_folder(**options)
    return dict(database=database, identity=identity, store=store, calls=calls, run=run,
                state=state, wait=wait, prepare=prepare, tmp_path=tmp_path, journal=journal,
                module=module, gate=gate, gate_depth=gate_depth, server=server, observe=observe,
                transfer=transfer)


def test_success_uses_original_durable_claim_and_receipt(sample):
    result = sample['run']()
    assert result['identity'] == sample['identity']
    assert result['state'] == 'waiting_rag'
    assert sample['calls'].count('submit') == 1
    with sqlite3.connect(sample['database']) as db:
        saved = json.loads(db.execute('SELECT receipt_json FROM postprocessing_receipts').fetchone()[0])
        assert saved['claim_id'] == result['claim_id']
        assert db.execute('SELECT state FROM llm_folder_queue').fetchone()[0] == 'waiting_postprocessing'


@pytest.mark.parametrize('phase', ['submit_once', 'wait_for_result'])
def test_disconnect_is_unknown_and_never_resubmitted(sample, phase):
    def disconnect(*args):
        raise ConnectionError('synthetic lost connection')
    with pytest.raises(ConnectionError):
        sample['run'](**{phase: disconnect})
    assert sample['state']() == 'unknown'
    with pytest.raises(StageConflict):
        sample['run']()
    assert sample['calls'].count('prepare') == 1


def test_duplicate_completed_folder_preserves_saved_receipt(sample):
    sample['run']()
    with sqlite3.connect(sample['database']) as db:
        before = db.execute('SELECT * FROM postprocessing_receipts').fetchall()
    with pytest.raises(StageConflict):
        sample['run']()
    with sqlite3.connect(sample['database']) as db:
        assert db.execute('SELECT * FROM postprocessing_receipts').fetchall() == before
    assert sample['calls'].count('submit') == 1


def test_wrong_folder_identity_fails_before_snapshot(sample):
    with pytest.raises(StageConflict):
        sample['run'](identity={**sample['identity'], 'task_run_id': str(uuid4())})
    assert sample['state']() is None
    assert sample['calls'] == []


def test_pause_during_execution_saves_held_receipt(sample):
    def finish_paused(job):
        with sqlite3.connect(sample['database']) as db:
            db.execute("UPDATE tasks SET desired_state='paused'")
        return sample['wait'](job)
    result = sample['run'](wait_for_result=finish_paused)
    assert result['state'] == 'receipt_held'
    with sqlite3.connect(sample['database']) as db:
        assert db.execute('SELECT COUNT(*) FROM postprocessing_receipts').fetchone()[0] == 1


@pytest.mark.parametrize('change', ['identity', 'claim_id', 'manifest_sha256', 'generation', 'digest', 'artifact'])
def test_tampered_transport_result_never_publishes(sample, change):
    def tamper(job):
        value = sample['wait'](job)
        if change == 'generation':
            value['job']['generation'] += 1
        elif change == 'identity':
            value['receipt']['identity']['task_run_id'] = str(uuid4())
        elif change == 'digest':
            value['receipt_sha256'] = '0' * 64
        elif change == 'artifact':
            Path(value['receipt']['artifacts'][0]['path']).write_text('tampered', encoding='utf8')
        else:
            value['receipt'][change] = str(uuid4()) if change == 'claim_id' else '0' * 64
        return value
    with pytest.raises((ValueError, StageConflict)):
        sample['run'](wait_for_result=tamper)
    assert sample['state']() == 'unknown'
    with sqlite3.connect(sample['database']) as db:
        assert db.execute('SELECT COUNT(*) FROM postprocessing_receipts').fetchone()[0] == 0


@pytest.mark.parametrize('dependency', ['validate_result', 'transfer_snapshot', 'validate_staging',
                                       'observe_execution', 'bind_execution'])
def test_missing_verifier_fails_before_claim(sample, dependency):
    with pytest.raises(ValueError):
        sample['run'](**{dependency: None})
    assert sample['state']() is None


def test_untrusted_server_receipt_rejected_even_with_valid_digest(sample):
    with pytest.raises(StageConflict):
        sample['run'](validate_result=lambda *_: False)
    assert sample['state']() == 'unknown'


def test_pause_while_preparing_snapshot_prevents_submit(sample):
    def prepare(claim):
        value = sample['prepare'](claim)
        with sqlite3.connect(sample['database']) as db:
            db.execute("UPDATE tasks SET desired_state='paused'")
        return value
    with pytest.raises((ValueError, StageConflict)):
        sample['run'](prepare_snapshot=prepare)
    assert 'submit' not in sample['calls']


def test_validator_mutation_does_not_change_transport_object_or_accept(sample):
    collected = []
    def wait(job):
        value = sample['wait'](job)
        collected.append(value)
        return value
    def mutate(job, value):
        value['server_receipt']['ledger_id'] = 'mutated'
        return True
    with pytest.raises(StageConflict):
        sample['run'](wait_for_result=wait, validate_result=mutate)
    assert collected[0]['server_receipt']['ledger_id'] == 'synthetic-authenticated-ledger'
    assert sample['state']() == 'unknown'


@pytest.mark.parametrize('control', ['deleted', 'generation'])
def test_control_changes_during_snapshot_block_start(sample, control):
    def prepare(claim):
        result = sample['prepare'](claim)
        with sqlite3.connect(sample['database']) as db:
            if control == 'deleted':
                db.execute('INSERT INTO task_deletions VALUES(?)', (sample['identity']['task_id'],))
            else:
                db.execute('UPDATE tasks SET start_generation=3')
        return result
    with pytest.raises(StageConflict):
        sample['run'](prepare_snapshot=prepare)
    assert 'submit' not in sample['calls']


def test_ambiguous_acknowledgement_is_not_followed_by_wait_or_retry(sample):
    with pytest.raises(ValueError):
        sample['run'](submit_once=lambda *_: {'accepted': True})
    assert sample['state']() == 'unknown'
    assert 'wait' not in sample['calls']
    with pytest.raises(StageConflict):
        sample['run']()
    assert sample['calls'] == ['prepare', 'transfer', 'staging-validated']


def test_error_after_separate_completion_cannot_rewrite_sealed_records(sample):
    saved = []
    def wait(job):
        result = sample['wait'](job)
        sample['store'].complete(sample['identity'], job['claim_id'], result['receipt'],
                                 validate_receipt=lambda _: True)
        with sqlite3.connect(sample['database']) as db:
            saved.append((db.execute('SELECT * FROM postprocessing_claims').fetchall(),
                          db.execute('SELECT * FROM postprocessing_receipts').fetchall()))
        raise ConnectionError('synthetic error after another owner accepted same receipt')
    with pytest.raises(ConnectionError):
        sample['run'](wait_for_result=wait)
    with sqlite3.connect(sample['database']) as db:
        assert (db.execute('SELECT * FROM postprocessing_claims').fetchall(),
                db.execute('SELECT * FROM postprocessing_receipts').fetchall()) == saved[0]


def test_self_consistent_receipt_from_another_claim_still_rejected(sample):
    def wait(job):
        result = sample['wait'](job)
        result['receipt']['claim_id'] = str(uuid4())
        result['receipt_json'] = encoded(result['receipt'])
        result['receipt_sha256'] = hashlib.sha256(result['receipt_json'].encode('utf8')).hexdigest()
        return result
    with pytest.raises(ValueError):
        sample['run'](wait_for_result=wait)
    assert sample['state']() == 'unknown'


def test_journal_requires_explicit_initialization_and_refuses_control_database(sample):
    module = sample['module']
    pending = module.RemoteSubmissionJournal(sample['tmp_path'] / 'not-installed.sqlite3',
        control_database=sample['database'], admission_gate=sample['gate'])
    with pytest.raises(ValueError):
        sample['run'](journal=pending)
    assert sample['state']() is None
    assert not pending.database.exists()
    with pytest.raises(ValueError):
        module.RemoteSubmissionJournal(sample['database'], control_database=sample['database'],
                                       admission_gate=sample['gate'])
    with pytest.raises(ValueError):
        sample['journal'].initialize()


def test_intent_is_durable_after_ambiguous_start_and_can_only_be_queried(sample):
    jobs = []
    def lost(job, snapshot, staging):
        jobs.append(copy.deepcopy(job))
        raise TimeoutError('synthetic timeout after start admission')
    with pytest.raises(TimeoutError):
        sample['run'](submit_once=lost)
    old = sample['journal']
    reopened = sample['module'].RemoteSubmissionJournal(old.database,
        control_database=sample['database'], admission_gate=sample['gate'], journal_id=old.journal_id)
    value = reopened.get(jobs[0]['job_id'])
    assert value['job'] == jobs[0]
    assert value['server_identity'] == sample['server']
    assert value['submission_intent'] == 'start_once_outcome_unknown_until_verified'
    assert value['job']['snapshot_sha256'] == 'd' * 64
    assert value['job']['code_sha256'] == 'e' * 64
    before = reopened.database.read_bytes()
    value['server_identity']['server_id'] = 'caller-mutation'
    assert reopened.get(jobs[0]['job_id'])['server_identity'] == sample['server']
    assert reopened.database.read_bytes() == before
    script = '''import json,sys
from contextlib import nullcontext
sys.path[:0] = [sys.argv[1], sys.argv[2]]
from remote_coordinator import RemoteSubmissionJournal
journal = RemoteSubmissionJournal(sys.argv[3], control_database=sys.argv[4],
                                  admission_gate=lambda _: nullcontext(), journal_id=sys.argv[5])
print(json.dumps(journal.get(sys.argv[6]), ensure_ascii=True))
'''
    process = subprocess.run([sys.executable, '-B', '-c', script, str(ROOT),
        str(ROOT.parent / 'postprocess-prefect-20261031'), str(old.database),
        str(sample['database']), old.journal_id, jobs[0]['job_id']],
        capture_output=True, text=True, timeout=10, check=True)
    assert json.loads(process.stdout)['job'] == jobs[0]
    assert reopened.database.read_bytes() == before
    with pytest.raises(StageConflict):
        sample['run'](journal=reopened)
    assert sample['calls'].count('prepare') == 1


def test_paused_before_admission_has_no_start_intent(sample):
    jobs = []
    def observe(claim, job, clock):
        jobs.append(job)
        proof = sample['observe'](claim, job, clock)
        with sqlite3.connect(sample['database']) as db:
            db.execute("UPDATE tasks SET desired_state='paused'")
        return proof
    with pytest.raises(StageConflict):
        sample['run'](observe_execution=observe)
    assert sample['journal'].get(jobs[0]['job_id']) is None
    assert 'submit' not in sample['calls']


@pytest.mark.parametrize('change', ['stale', 'identity', 'claim', 'generation', 'job', 'state', 'future'])
def test_execution_proof_must_be_fresh_and_bind_original_claim(sample, change):
    def observe(claim, job, clock):
        proof = sample['observe'](claim, job, clock)
        if change == 'stale':
            proof['clock']['observed_monotonic_ns'] -= 31_000_000_000
        elif change == 'future':
            proof['clock']['observed_monotonic_ns'] += 31_000_000_000
        elif change == 'identity':
            proof['identity']['task_run_id'] = str(uuid4())
        elif change == 'claim':
            proof['claim_id'] = str(uuid4())
        elif change == 'generation':
            proof['generation'] = 3
        elif change == 'job':
            proof['job_id'] = str(uuid4())
        else:
            proof['state'] = 'CANCELLED'
        return proof
    with pytest.raises(ValueError):
        sample['run'](observe_execution=observe)
    assert 'submit' not in sample['calls']
    assert sample['state']() == 'unknown'


def test_gate_binding_rejects_changed_stage_context_before_record_or_start(sample):
    with pytest.raises(ValueError):
        sample['run'](bind_execution=lambda *_: False)
    assert 'submit' not in sample['calls']
    with sqlite3.connect(sample['journal'].database) as db:
        assert db.execute('SELECT COUNT(*) FROM remote_start_intents').fetchone()[0] == 0


def test_hook_runs_after_same_receipt_is_saved_and_supplies_strict_validator(sample):
    accepted = []
    def hook(result, validate):
        assert sample['state']() == 'waiting_rag'
        assert 'submit' in sample['calls'] and 'wait' in sample['calls']
        with sqlite3.connect(sample['database']) as db:
            receipt = json.loads(db.execute('SELECT receipt_json FROM postprocessing_receipts').fetchone()[0])
        assert validate(receipt) is True
        changed = copy.deepcopy(receipt)
        changed['claim_id'] = str(uuid4())
        assert validate(changed) is False
        accepted.append(copy.deepcopy(result))
    result = sample['run'](accepted_hook=hook)
    assert accepted == [result]


def test_result_acceptance_rechecks_source_after_remote_receipt_is_saved(sample):
    sys.path.insert(0, str(ROOT.parent / 'postprocess-prefect-20261031' / 'results_next'))
    from postprocessing_results import ResultConflict, ResultsStore
    results = ResultsStore(sample['database'], admission_gate=sample['gate'],
                           inspect_receipt=FolderStageStore._inspect_receipt)
    results.initialize()
    ticket = results.reserve(sample['identity'], 2, 'c' * 64)
    def accept(result, validate):
        sample['store'].observe_run = lambda _: False
        results.record_pass(sample['identity'], ticket, result['claim_id'], validate_receipt=validate)
    with pytest.raises(ResultConflict):
        sample['run'](accepted_hook=accept)
    assert sample['state']() == 'unknown'
    with sqlite3.connect(sample['database']) as db:
        assert db.execute('SELECT state FROM postprocessing_results').fetchone()[0] == 'unresolved'


def test_paused_result_hook_receives_held_receipt_and_never_publishes(sample):
    accepted = []
    def wait(job):
        with sqlite3.connect(sample['database']) as db:
            db.execute("UPDATE tasks SET desired_state='paused'")
        return sample['wait'](job)
    sample['run'](wait_for_result=wait, accepted_hook=lambda result, _: accepted.append(result))
    assert accepted[0]['state'] == 'receipt_held'
    assert sample['state']() == 'receipt_held'


def test_no_hook_on_failed_or_unknown_submission(sample):
    accepted = []
    def lost(*_):
        raise TimeoutError('synthetic timeout')
    with pytest.raises(TimeoutError):
        sample['run'](submit_once=lost, accepted_hook=lambda *args: accepted.append(args))
    assert accepted == []


def test_hook_failure_does_not_change_sealed_receipt_or_start_again(sample):
    saved = []
    def fail_hook(result, validate):
        with sqlite3.connect(sample['database']) as db:
            saved.append(db.execute('SELECT * FROM postprocessing_receipts').fetchall())
        raise RuntimeError('synthetic results sink unavailable')
    with pytest.raises(RuntimeError):
        sample['run'](accepted_hook=fail_hook)
    with sqlite3.connect(sample['database']) as db:
        assert db.execute('SELECT * FROM postprocessing_receipts').fetchall() == saved[0]
    assert sample['state']() == 'waiting_rag'
    with pytest.raises(StageConflict):
        sample['run']()


def test_hook_cannot_change_the_completed_result(sample):
    def mutate(result, _):
        result['claim_id'] = str(uuid4())
    with pytest.raises(ValueError, match='mutated'):
        sample['run'](accepted_hook=mutate)
    assert sample['state']() == 'waiting_rag'
    with sqlite3.connect(sample['database']) as db:
        original = db.execute('SELECT claim_id FROM postprocessing_claims').fetchone()[0]
        receipt = json.loads(db.execute('SELECT receipt_json FROM postprocessing_receipts').fetchone()[0])
    assert original == receipt['claim_id']


def test_journal_must_bind_original_control_database_and_gate(sample):
    other = sample['module'].RemoteSubmissionJournal(sample['tmp_path'] / 'wrong-gate.sqlite3',
        control_database=sample['database'], admission_gate=lambda _: None)
    other.initialize()
    with pytest.raises(ValueError):
        sample['run'](journal=other)
    assert sample['state']() is None


@pytest.mark.parametrize('target', ['ack', 'result'])
def test_changed_server_or_ledger_identity_is_never_accepted(sample, target):
    if target == 'ack':
        def bad(job, snapshot, staging):
            return dict(job=job, accepted=True, server_identity={**sample['server'], 'ledger_id': str(uuid4())})
        overrides = {'submit_once': bad}
    else:
        def bad(job):
            result = sample['wait'](job)
            result['server_identity']['server_id'] = 'another-server'
            return result
        overrides = {'wait_for_result': bad}
    with pytest.raises(ValueError):
        sample['run'](**overrides)
    assert sample['state']() == 'unknown'


def test_journal_get_detects_changed_frozen_metadata(sample):
    result = sample['run']()
    with sqlite3.connect(sample['journal'].database) as db:
        row = db.execute('SELECT job_id,intent_json FROM remote_start_intents').fetchone()
        value = json.loads(row[1])
        value['job']['code_sha256'] = '0' * 64
        db.execute('UPDATE remote_start_intents SET intent_json=?', (encoded(value),))
    with pytest.raises(ValueError, match='changed'):
        sample['journal'].get(row[0])
    assert sample['state']() == result['state']


def test_journal_missing_after_install_is_never_silently_recreated(sample):
    sample['journal'].database.unlink()
    with pytest.raises(ValueError):
        sample['run']()
    assert sample['state']() is None
    assert not sample['journal'].database.exists()


def test_journal_identity_change_is_rejected_before_claim(sample):
    with sqlite3.connect(sample['journal'].database) as db:
        db.execute("UPDATE journal_settings SET value=? WHERE name='journal_id'", (str(uuid4()),))
    with pytest.raises(ValueError):
        sample['run']()
    assert sample['state']() is None


def test_large_start_metadata_is_rejected_before_gate_admission(sample):
    def large(claim):
        value = sample['prepare'](claim)
        value['payload'] = {'incorrectly_embedded_file': 'x' * 100_000}
        return value
    with pytest.raises(ValueError, match='small'):
        sample['run'](prepare_snapshot=large)
    assert 'submit' not in sample['calls']
    with sqlite3.connect(sample['journal'].database) as db:
        assert db.execute('SELECT COUNT(*) FROM remote_start_intents').fetchone()[0] == 0


def test_proof_expiring_during_intent_commit_blocks_start_without_erasing_intent(sample, monkeypatch):
    original = sample['journal']._record_start
    def commit_then_age(*args):
        original(*args)
        original_clock = time.monotonic_ns
        monkeypatch.setattr(sample['module'].time, 'monotonic_ns', lambda: original_clock() + 31_000_000_000)
    monkeypatch.setattr(sample['journal'], '_record_start', commit_then_age)
    with pytest.raises(ValueError, match='stale'):
        sample['run']()
    assert 'submit' not in sample['calls']
    with sqlite3.connect(sample['journal'].database) as db:
        assert db.execute('SELECT COUNT(*) FROM remote_start_intents').fetchone()[0] == 1


@pytest.mark.parametrize('pause_after_start', [False, True])
def test_existing_stage_coordinator_and_results_store_accept_remote_hook(sample, pause_after_start):
    """Use the real local coordinator and result ledger, with only fake transport."""
    from contextlib import asynccontextmanager, nullcontext
    sys.path.insert(0, str(ROOT.parent / 'postprocess-prefect-20261031' / 'runtime_next'))
    sys.path.insert(0, str(ROOT.parent / 'postprocess-prefect-20261031' / 'results_next'))
    sys.path.insert(0, str(ROOT.parent.parent / 'prefect-paperflow'))
    from stage_runtime import Coordinator
    from live_runtime import ResultsHooks
    from postprocessing_results import ResultsStore
    import asyncio

    with sqlite3.connect(sample['database']) as db:
        db.execute('ALTER TABLE llm_folder_queue ADD COLUMN generation INTEGER DEFAULT 2')
        db.execute('ALTER TABLE tasks ADD COLUMN name TEXT DEFAULT "synthetic remote"')
        db.execute('CREATE TABLE llm_request_admissions(task_id TEXT,task_run_id TEXT,component_id TEXT,'
                   'state TEXT,kind TEXT)')
        db.execute('INSERT INTO llm_request_admissions VALUES(?,?,?,?,?)',
            (sample['identity']['task_id'], sample['identity']['task_run_id'], 'body', 'raw_complete', 'body'))
    ledger = ResultsStore(sample['database'], admission_gate=sample['gate'],
                          inspect_receipt=FolderStageStore._inspect_receipt)
    ledger.initialize()
    slots = []
    @asynccontextmanager
    async def slot():
        slots.append('enter')
        try:
            yield
        finally:
            slots.append('exit')
    def verify(identity, generation):
        return dict(identity=identity, generation=generation, source_llm_generation=2,
                    manifest_sha256='c' * 64, raw_complete=True, session_current=True, not_deleted=True)
    def wait(job):
        if pause_after_start:
            with sqlite3.connect(sample['database']) as db:
                db.execute("UPDATE tasks SET desired_state='paused'")
        return sample['wait'](job)
    def observe(claim, job, clock):
        evidence = sample['observe'](claim, job, clock)
        with sqlite3.connect(sample['database']) as db:
            row = db.execute('SELECT updated_at_ns,owner_pid,owner_started FROM paperflow_postprocess_stage_jobs '
                             'WHERE job_id=?', (job['job_id'],)).fetchone()
        evidence['binding'] = dict(updated_at_ns=row[0], owner_pid=row[1], owner_started=row[2])
        return evidence
    def bind(claim, job, evidence):
        assert sample['gate_depth'][0] == 1
        with sqlite3.connect(sample['database']) as db:
            row = db.execute('SELECT state,updated_at_ns,owner_pid,owner_started '
                'FROM paperflow_postprocess_stage_jobs WHERE job_id=?', (job['job_id'],)).fetchone()
        return (row[0] == 'executing' and evidence['binding']
                == dict(updated_at_ns=row[1], owner_pid=row[2], owner_started=row[3]))
    def process(identity, generation, item, accept, context):
        return sample['run'](accepted_hook=accept, wait_for_result=wait,
                             observe_execution=observe, bind_execution=bind)
    runtime = Coordinator(sample['database'], task_gate=sample['gate'], folder_gate=lambda _: nullcontext(),
        postprocess_is_open=lambda: True, rag_is_held=lambda: True,
        verify_upstream=verify, verify_execution=lambda item, ctx: ctx == item['job_id'],
        slot=slot, process_folder=process, results=ResultsHooks(ledger))
    runtime.initialize()
    item = runtime.reserve(sample['identity'], 2)
    with sqlite3.connect(sample['database']) as db:
        db.execute("UPDATE paperflow_postprocess_stage_jobs SET state='submitted'")
    result = asyncio.run(runtime.execute(item['job_id'], item['job_id']))
    assert slots == ['enter', 'exit']
    assert runtime.job(item['job_id'])['state'] == 'finished'
    assert result['state'] == ('receipt_held' if pause_after_start else 'waiting_rag')
    with sqlite3.connect(sample['database']) as db:
        assert db.execute('SELECT state FROM postprocessing_results').fetchone()[0] == 'passed'
    assert ledger.summary()['counts']['passed'] == 1


def test_transfer_ack_loss_keeps_frozen_identity_and_never_retransfers(sample):
    jobs = []
    def lost(job, snapshot):
        assert sample['gate_depth'][0] == 0
        assert sample['journal'].get_transfer(job['job_id'])['job'] == job
        jobs.append(copy.deepcopy(job))
        raise TimeoutError('synthetic upload accepted but ACK lost')
    with pytest.raises(TimeoutError):
        sample['run'](transfer_snapshot=lost)
    assert sample['state']() == 'unknown'
    old = sample['journal']
    reopened = sample['module'].RemoteSubmissionJournal(old.database, control_database=sample['database'],
        admission_gate=sample['gate'], journal_id=old.journal_id)
    transfer = reopened.get_transfer(jobs[0]['job_id'])
    assert transfer['job'] == jobs[0]
    assert transfer['server_identity'] == sample['server']
    assert transfer['transfer_intent'] == 'transfer_once_outcome_unknown_until_verified'
    assert reopened.get(jobs[0]['job_id']) is None
    before = reopened.database.read_bytes()
    with pytest.raises(StageConflict):
        sample['run'](journal=reopened)
    assert sample['calls'] == ['prepare']
    assert reopened.database.read_bytes() == before


def test_freeze_pause_prevents_both_transfer_intent_and_network(sample):
    def freeze(claim):
        value = sample['prepare'](claim)
        with sqlite3.connect(sample['database']) as db:
            db.execute("UPDATE tasks SET desired_state='paused'")
        return value
    with pytest.raises(StageConflict):
        sample['run'](prepare_snapshot=freeze)
    assert 'transfer' not in sample['calls'] and 'submit' not in sample['calls']
    with sqlite3.connect(sample['journal'].database) as db:
        assert db.execute('SELECT COUNT(*) FROM remote_transfer_intents').fetchone()[0] == 0
        assert db.execute('SELECT COUNT(*) FROM remote_start_intents').fetchone()[0] == 0


def test_pause_during_upload_preserves_transfer_but_cannot_admit_start(sample):
    jobs = []
    def transfer(job, snapshot):
        result = sample['transfer'](job, snapshot)
        jobs.append(copy.deepcopy(job))
        with sqlite3.connect(sample['database']) as db:
            db.execute("UPDATE tasks SET desired_state='paused'")
        return result
    with pytest.raises(StageConflict):
        sample['run'](transfer_snapshot=transfer)
    assert sample['journal'].get_transfer(jobs[0]['job_id'])['job'] == jobs[0]
    assert sample['journal'].get(jobs[0]['job_id']) is None
    assert 'submit' not in sample['calls'] and 'wait' not in sample['calls']


@pytest.mark.parametrize('change', ['server', 'ledger', 'job', 'snapshot', 'code', 'payload', 'state', 'auth'])
def test_start_requires_authenticated_staging_for_exact_frozen_job(sample, change):
    jobs = []
    def transfer(job, snapshot):
        jobs.append(copy.deepcopy(job))
        result = sample['transfer'](job, snapshot)
        if change == 'server':
            result['server_identity']['server_id'] = 'another'
        elif change == 'ledger':
            result['server_identity']['ledger_id'] = str(uuid4())
        elif change == 'job':
            result['job']['claim_id'] = str(uuid4())
        elif change == 'snapshot':
            result['snapshot_sha256'] = '0' * 64
        elif change == 'code':
            result['code_sha256'] = '0' * 64
        elif change == 'payload':
            result['payload'] = {'another_snapshot': True}
        elif change == 'state':
            result['state'] = 'uploading'
        else:
            result['staging_receipt'] = {'synthetic_authenticated': False}
        return result
    with pytest.raises(ValueError):
        sample['run'](transfer_snapshot=transfer)
    assert sample['journal'].get(jobs[0]['job_id']) is None
    assert sample['journal'].get_transfer(jobs[0]['job_id'])['job'] == jobs[0]
    assert 'submit' not in sample['calls']


def test_transfer_failure_does_not_call_stage_observer_or_hook(sample):
    def lost(*_):
        raise ConnectionError('synthetic connection lost while uploading')
    def forbidden(*_):
        pytest.fail('cannot observe or accept execution before upload is authenticated')
    with pytest.raises(ConnectionError):
        sample['run'](transfer_snapshot=lost, observe_execution=forbidden, accepted_hook=forbidden)
    assert sample['state']() == 'unknown'


def test_process_death_during_synthetic_transfer_leaves_durable_queryable_identity(sample):
    """No sockets: the child simulates delivery, then exits without cleanup."""
    script = '''import json,os,sys
from contextlib import nullcontext
sys.path[:0] = [sys.argv[1],sys.argv[2]]
from folder_state import FolderStageStore
from remote_coordinator import RemoteSubmissionJournal,process_remote_folder
database,journal_path,journal_id = sys.argv[3:6]
identity,server = json.loads(sys.argv[6]),json.loads(sys.argv[7])
gate=lambda _: nullcontext()
store=FolderStageStore(database,observe_run=lambda _:True,admission_gate=gate)
journal=RemoteSubmissionJournal(journal_path,control_database=database,admission_gate=gate,journal_id=journal_id)
def transfer(job,snapshot):
    assert journal.get_transfer(job['job_id'])['job'] == job
    print(json.dumps(job),flush=True)
    os._exit(23)
process_remote_folder(identity=identity,generation=2,manifest_sha256='c'*64,store=store,journal=journal,
    server_identity=server,prepare_snapshot=lambda claim:dict(snapshot_sha256='d'*64,code_sha256='e'*64,payload={'synthetic':True}),
    transfer_snapshot=transfer,validate_staging=lambda *args:True,submit_once=lambda *args:None,
    wait_for_result=lambda *args:None,validate_result=lambda *args:True,
    observe_execution=lambda *args:None,bind_execution=lambda *args:True)
'''
    process = subprocess.run([sys.executable, '-B', '-c', script, str(ROOT),
        str(ROOT.parent / 'postprocess-prefect-20261031'), str(sample['database']),
        str(sample['journal'].database), sample['journal'].journal_id, encoded(sample['identity']),
        encoded(sample['server'])], capture_output=True, text=True, timeout=15)
    assert process.returncode == 23, process.stderr
    job = json.loads(process.stdout)
    assert sample['journal'].get_transfer(job['job_id'])['job'] == job
    assert sample['journal'].get_transfer(job['job_id'])['server_identity'] == sample['server']
    assert sample['journal'].get(job['job_id']) is None
    assert sample['state']() == 'processing'  # abrupt death cannot run mark_unknown
    with pytest.raises(StageConflict):
        sample['run']()
    assert sample['calls'] == []


def test_existing_transfer_intent_blocks_new_claim_even_if_control_claim_is_missing(sample):
    def lost(*_):
        raise TimeoutError('synthetic ACK lost')
    with pytest.raises(TimeoutError):
        sample['run'](transfer_snapshot=lost)
    with sqlite3.connect(sample['database']) as db:
        db.execute('DELETE FROM postprocessing_claims')  # simulate inconsistent synthetic recovery
    before = list(sample['calls'])
    with pytest.raises(StageConflict):
        sample['run']()
    assert sample['state']() is None
    assert sample['calls'] == before


def test_staging_validator_cannot_mutate_returned_transport_evidence(sample):
    observed = []
    def transfer(job, snapshot):
        value = sample['transfer'](job, snapshot)
        observed.append(value)
        return value
    def mutate(job, snapshot, staging):
        staging['staging_receipt']['synthetic_authenticated'] = False
        return True
    with pytest.raises(ValueError, match='mutated'):
        sample['run'](transfer_snapshot=transfer, validate_staging=mutate)
    assert observed[0]['staging_receipt'] == {'synthetic_authenticated': True}
    assert 'submit' not in sample['calls']


def test_changed_transfer_metadata_cannot_be_read_as_valid_start_binding(sample):
    sample['run']()
    with sqlite3.connect(sample['journal'].database) as db:
        job_id, raw = db.execute('SELECT job_id,intent_json FROM remote_transfer_intents').fetchone()
        value = json.loads(raw)
        value['snapshot']['payload'] = {'tampered': True}
        db.execute('UPDATE remote_transfer_intents SET intent_json=?', (encoded(value),))
    with pytest.raises(ValueError, match='changed'):
        sample['journal'].get_transfer(job_id)
    with pytest.raises(ValueError, match='changed'):
        sample['journal'].get(job_id)
    assert sample['state']() == 'waiting_rag'


def test_wall_clock_rollback_cannot_hide_monotonic_execution_expiry(sample, monkeypatch):
    original = sample['journal']._record_start
    observed = {}
    def observe(claim, job, clock):
        proof = sample['observe'](claim, job, clock)
        observed.update(wall=proof['observed_at_ns'], monotonic=time.monotonic_ns())
        return proof
    def commit_then_rewind_wall(*args):
        original(*args)
        # Thirty-one seconds elapsed in the process, but wall time now appears
        # only one second newer than the original RUNNING observation.
        monkeypatch.setattr(sample['module'].time, 'monotonic_ns', lambda: observed['monotonic'] + 31_000_000_000)
        monkeypatch.setattr(sample['module'].time, 'time_ns', lambda: observed['wall'] + 1_000_000_000)
    monkeypatch.setattr(sample['journal'], '_record_start', commit_then_rewind_wall)
    with pytest.raises(ValueError):
        sample['run'](observe_execution=observe)
    assert 'submit' not in sample['calls']


@pytest.mark.parametrize('jump', [-3_600_000_000_000, 3_600_000_000_000])
def test_wall_clock_jumps_are_display_only_when_execution_is_monotonically_fresh(sample, monkeypatch, jump):
    original = sample['journal']._record_start
    def commit_then_jump_wall(*args):
        original(*args)
        wall = time.time_ns()
        monkeypatch.setattr(sample['module'].time, 'time_ns', lambda: wall + jump)
    monkeypatch.setattr(sample['journal'], '_record_start', commit_then_jump_wall)
    assert sample['run']()['state'] == 'waiting_rag'


@pytest.mark.parametrize('change', ['boot', 'pid', 'observation', 'deadline', 'missing', 'boolean_pid'])
def test_unverifiable_execution_clock_domain_never_authorizes_start(sample, change):
    jobs = []
    def observe(claim, job, clock):
        jobs.append(copy.deepcopy(job))
        proof = sample['observe'](claim, job, clock)
        if change == 'boot':
            proof['clock']['boot_id'] = str(uuid4())
        elif change == 'pid':
            proof['clock']['process_id'] += 1
        elif change == 'observation':
            proof['clock']['observation_id'] = str(uuid4())
        elif change == 'deadline':
            proof['clock']['deadline_monotonic_ns'] += 31_000_000_000
        elif change == 'boolean_pid':
            proof['clock']['process_id'] = True
        else:
            proof.pop('clock')
        return proof
    with pytest.raises(ValueError):
        sample['run'](observe_execution=observe)
    assert sample['state']() == 'unknown'
    assert sample['journal'].get(jobs[0]['job_id']) is None
    assert 'submit' not in sample['calls']


def test_observer_latency_consumes_preassigned_monotonic_budget(sample, monkeypatch):
    def slow(claim, job, clock):
        proof = sample['observe'](claim, job, clock)
        monkeypatch.setattr(sample['module'].time, 'monotonic_ns',
                            lambda: clock['deadline_monotonic_ns'] + 1)
        return proof
    with pytest.raises(ValueError, match='stale'):
        sample['run'](observe_execution=slow)
    assert sample['state']() == 'unknown'
    assert 'submit' not in sample['calls']


def test_current_process_domain_is_rechecked_after_intent_commit(sample, monkeypatch):
    original = sample['journal']._record_start
    def commit_then_new_boot(*args):
        original(*args)
        monkeypatch.setattr(sample['module'], '_PROCESS_BOOT_ID', str(uuid4()))
    monkeypatch.setattr(sample['journal'], '_record_start', commit_then_new_boot)
    with pytest.raises(ValueError, match='another process'):
        sample['run']()
    assert 'submit' not in sample['calls']


def test_missing_local_monotonic_observation_fails_closed(sample, monkeypatch):
    def unavailable():
        raise OSError('synthetic monotonic observation unavailable')
    monkeypatch.setattr(sample['module'].time, 'monotonic_ns', unavailable)
    with pytest.raises(OSError):
        sample['run']()
    assert sample['state']() == 'unknown'
    assert 'submit' not in sample['calls']
    with sqlite3.connect(sample['journal'].database) as db:
        assert db.execute('SELECT COUNT(*) FROM remote_transfer_intents').fetchone()[0] == 1
        assert db.execute('SELECT COUNT(*) FROM remote_start_intents').fetchone()[0] == 0
