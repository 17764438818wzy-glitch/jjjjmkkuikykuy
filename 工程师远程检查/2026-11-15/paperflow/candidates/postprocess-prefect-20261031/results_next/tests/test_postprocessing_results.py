"""Synthetic-only contract for durable, folder-grain postprocessing results."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / 'results_next' / 'postprocessing_results.py'


def load(path, name):
    if not path.is_file():
        return None
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


module = load(MODULE_PATH, 'postprocessing_results')
stage_module = load(ROOT / 'folder_state.py', 'result_test_folder_state')


def test_result_store_exists():
    assert module is not None, 'folder results ledger is not implemented'


def identity(n=1):
    return dict(task_id=str(uuid4()), group_id=f'{n:032x}', paper_id=f'paper-{n}',
                receipt_id='b' * 64, attempt_id=f'attempt-{n}', parent_run_id=str(uuid4()),
                task_run_id=str(uuid4()))


@pytest.fixture
def sample(tmp_path):
    assert module is not None, 'folder results ledger is not implemented'
    database = tmp_path / 'control.sqlite3'
    with sqlite3.connect(database) as db:
        db.execute('CREATE TABLE tasks(task_id TEXT PRIMARY KEY,desired_state TEXT,start_generation INTEGER,'
                   'start_pending INTEGER,task_status TEXT,prefect_run_id TEXT,prefect_order_model TEXT,name TEXT)')
        db.execute('CREATE TABLE task_deletions(task_id TEXT PRIMARY KEY)')
        db.execute('CREATE TABLE llm_folder_queue(task_id TEXT,group_id TEXT,paper_id TEXT,receipt_id TEXT,'
                   'attempt_id TEXT,parent_run_id TEXT,task_run_id TEXT,manifest_sha256 TEXT,state TEXT,'
                   'generation INTEGER,PRIMARY KEY(task_id,group_id))')
    stages = stage_module.FolderStageStore(database, observe_run=lambda _: True)
    stages.initialize()
    store = module.ResultsStore(database, admission_gate=lambda _: nullcontext(),
                                inspect_receipt=stage_module.FolderStageStore._inspect_receipt)
    store.initialize()
    return tmp_path, database, stages, store


def insert_folder(database, item, *, generation=2):
    with sqlite3.connect(database) as db:
        db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?,?,?)',
                   (item['task_id'], 'running', generation, 0, 'running', item['parent_run_id'], 'v2', '合成任务'))
        db.execute('INSERT INTO llm_folder_queue VALUES(?,?,?,?,?,?,?,?,?,?)',
                   (item['task_id'], item['group_id'], item['paper_id'], item['receipt_id'], item['attempt_id'],
                    item['parent_run_id'], item['task_run_id'], 'c' * 64, 'waiting_postprocessing', generation))


def finish_stage(sample, item, n=1):
    tmp_path, _, stages, _ = sample
    claim = stages.claim(item, 2, 'c' * 64)
    artifact = tmp_path / f'artifact-{n}.json'
    artifact.write_text('{"verified":true}', encoding='utf8')
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    receipt = dict(schema='paperflow-postprocessing-receipt/1', identity=item,
        manifest_sha256='c' * 64, claim_id=claim['claim_id'], validation_status='passed',
        body={'package_path': str(tmp_path / 'publication'), 'manifest_sha256': 'd' * 64},
        attachments=[], artifacts=[dict(path=str(artifact.resolve()), sha256=digest)])
    stages.complete(item, claim['claim_id'], receipt, validate_receipt=lambda _: True)
    return claim['claim_id'], receipt, artifact


def test_verified_receipt_is_required_to_report_passed(sample):
    _, database, _, store = sample
    item = identity()
    insert_folder(database, item)
    ticket = store.reserve(item, 2, 'c' * 64)
    assert store.summary()['counts'] == {'passed': 0, 'failed': 0, 'unrecognized': None}
    with pytest.raises(module.ResultConflict):
        store.record_pass(item, ticket, str(uuid4()), validate_receipt=lambda _: True)
    claim_id, receipt, _ = finish_stage(sample, item)
    store.record_pass(item, ticket, claim_id, validate_receipt=lambda value: value == receipt)
    result = store.summary('passed', page=1, page_size=20)
    assert result['counts'] == {'passed': 1, 'failed': 0, 'unrecognized': None}
    assert result['list']['total'] == 1
    assert result['list']['items'][0]['group_id'] == item['group_id']


def test_rejected_receipt_fences_claim_until_reconciled(sample):
    _, database, _, store = sample
    item = identity()
    insert_folder(database, item)
    ticket = store.reserve(item, 2, 'c' * 64)
    claim_id, receipt, _ = finish_stage(sample, item)
    with pytest.raises(module.ResultConflict):
        store.record_pass(item, ticket, claim_id, validate_receipt=lambda _: False)
    with pytest.raises(module.ResultConflict):
        store.record_pass(item, ticket, claim_id, validate_receipt=lambda value: value == receipt)
    assert store.summary()['counts']['passed'] == 0


def test_duplicate_reservation_is_atomic_and_never_restarts(sample):
    _, database, _, store = sample
    item = identity()
    insert_folder(database, item)
    def reserve(_):
        try:
            return store.reserve(item, 2, 'c' * 64)
        except module.ResultConflict:
            return None
    with ThreadPoolExecutor(max_workers=8) as executor:
        answers = list(executor.map(reserve, range(8)))
    assert sum(answer is not None for answer in answers) == 1
    assert store.summary()['unresolved'] == 1


def test_preexisting_stage_claim_also_blocks_a_new_result_reservation(sample):
    _, database, stages, store = sample
    item = identity()
    insert_folder(database, item)
    stages.claim(item, 2, 'c' * 64)
    with pytest.raises(module.ResultConflict):
        store.reserve(item, 2, 'c' * 64)


def test_input_failure_can_be_recorded_before_any_stage_claim(sample):
    _, database, _, store = sample
    item = identity()
    insert_folder(database, item)
    ticket = store.reserve(item, 2, 'c' * 64)
    with pytest.raises(module.ResultConflict):
        store.record_failed(item, ticket, code='invalid_handoff', confirm=lambda *_: False)
    store.record_failed(item, ticket, code='invalid_handoff', confirm=lambda actual, code: actual == item)
    result = store.summary('failed')
    assert result['counts']['failed'] == 1
    assert result['list']['items'][0]['reason']
    with pytest.raises(module.ResultConflict):
        store.reserve(item, 2, 'c' * 64)


def test_failed_result_binds_and_rechecks_saved_diagnostic(sample):
    tmp_path, database, _, store = sample
    item = identity()
    insert_folder(database, item)
    ticket = store.reserve(item, 2, 'c' * 64)
    with pytest.raises(module.ResultConflict):
        store.record_failed(item, ticket, code='body_validation', confirm=lambda *_: True)
    proof = tmp_path / 'validation.json'
    proof.write_text('{"valid":false}', encoding='utf8')
    store.record_failed(item, ticket, code='body_validation', evidence_path=proof,
                        confirm=lambda actual, code: actual == item and code == 'body_validation')
    assert store.summary('failed')['counts']['failed'] == 1
    proof.write_text('{"valid":true}', encoding='utf8')
    assert store.summary('failed')['available'] is False


def test_unresolved_never_becomes_unrecognized_and_does_not_expose_private_text(sample):
    tmp_path, database, _, store = sample
    item = identity()
    insert_folder(database, item)
    ticket = store.reserve(item, 2, 'c' * 64)
    store.record_unresolved(item, ticket, code='outcome_unknown')
    proof = tmp_path / 'proof.json'
    proof.write_text('{}', encoding='utf8')
    with pytest.raises(module.ResultConflict):
        store.record_unrecognized(item, ticket, evidence_path=proof, confirm=lambda *_: False)
    with pytest.raises(module.ResultConflict):
        store.record_failed(item, ticket, code='raw secret text', confirm=lambda *_: True)
    result = store.summary('unrecognized')
    assert result['counts']['unrecognized'] is None
    assert result['unresolved'] == 1 and result['partial'] is True
    assert result['list']['items'] == [] and result['list']['total'] is None
    assert 'raw secret text' not in json.dumps(result, ensure_ascii=False)


def test_explicit_proof_can_record_unrecognized_without_asserting_full_coverage(sample):
    tmp_path, database, _, store = sample
    item = identity()
    insert_folder(database, item)
    ticket = store.reserve(item, 2, 'c' * 64)
    proof = tmp_path / 'proof.json'
    proof.write_text('{}', encoding='utf8')
    store.record_unrecognized(item, ticket, evidence_path=proof, confirm=lambda actual, path: actual == item)
    result = store.summary('unrecognized')
    assert result['counts']['unrecognized'] is None
    assert result['list']['total'] is None and len(result['list']['items']) == 1


def test_confirmed_unrecognized_items_can_be_paged_without_claiming_total(sample):
    tmp_path, database, _, store = sample
    for n in range(1, 4):
        item = identity(n)
        insert_folder(database, item)
        ticket = store.reserve(item, 2, 'c' * 64)
        proof = tmp_path / f'proof-{n}.json'
        proof.write_text('{}', encoding='utf8')
        store.record_unrecognized(item, ticket, evidence_path=proof, confirm=lambda *_: True)
    first = store.summary('unrecognized', page=1, page_size=2)['list']
    last = store.summary('unrecognized', page=2, page_size=2)['list']
    assert first['total'] is None and first['has_next'] is True and len(first['items']) == 2
    assert last['total'] is None and last['has_next'] is False and len(last['items']) == 1


def test_pagination_uses_full_folder_identity_and_safe_diagnostics(sample):
    _, database, _, store = sample
    ids = [identity(n) for n in range(1, 4)]
    for item in ids:
        insert_folder(database, item)
        ticket = store.reserve(item, 2, 'c' * 64)
        store.record_failed(item, ticket, code='invalid_handoff', confirm=lambda *_: True)
    page = store.summary('failed', page=2, page_size=2)
    assert page['list']['total'] == 3 and page['list']['page_size'] == 2
    assert len(page['list']['items']) == 1
    row = page['list']['items'][0]
    assert set(row) == {'task_id', 'group_id', 'paper_id', 'receipt_id', 'task_name', 'folder_label', 'reason', 'code'}
    assert row['group_id'] in [x['group_id'] for x in ids]
    selected = next(x for x in ids if x['group_id'] == row['group_id'])
    assert row['paper_id'] == selected['paper_id'] and row['receipt_id'] == selected['receipt_id']
    assert row['task_name'] == '合成任务'


def test_changed_receipt_or_artifact_makes_summary_unavailable(sample):
    _, database, _, store = sample
    item = identity()
    insert_folder(database, item)
    ticket = store.reserve(item, 2, 'c' * 64)
    claim_id, _, artifact = finish_stage(sample, item)
    store.record_pass(item, ticket, claim_id, validate_receipt=lambda _: True)
    artifact.write_text('changed', encoding='utf8')
    assert store.summary('passed')['available'] is False


def test_missing_table_or_broken_identity_never_appears_as_zero(sample):
    _, database, _, store = sample
    item = identity()
    insert_folder(database, item)
    store.reserve(item, 2, 'c' * 64)
    with sqlite3.connect(database) as db:
        db.execute("UPDATE llm_folder_queue SET receipt_id=?", ('d' * 64,))
    assert store.summary()['available'] is False
    with sqlite3.connect(database) as db:
        db.execute('DROP TABLE postprocessing_results')
    result = store.summary('passed')
    assert result['available'] is False and result['counts']['passed'] is None


def test_missing_stage_table_is_unknown_even_with_no_results(sample):
    _, database, _, store = sample
    with sqlite3.connect(database) as db:
        db.execute('DROP TABLE postprocessing_claims')
    result = store.summary()
    assert result['available'] is False and result['counts']['passed'] is None


def test_untracked_stage_claim_cannot_be_reported_as_zero(sample):
    _, database, stages, store = sample
    item = identity()
    insert_folder(database, item)
    stages.claim(item, 2, 'c' * 64)
    assert store.summary()['available'] is False


def test_confirmed_result_is_terminal(sample):
    _, database, _, store = sample
    item = identity()
    insert_folder(database, item)
    ticket = store.reserve(item, 2, 'c' * 64)
    store.record_failed(item, ticket, code='invalid_handoff', confirm=lambda *_: True)
    with pytest.raises(module.ResultConflict):
        store.record_unresolved(item, ticket, code='paused')
    assert store.summary()['counts']['failed'] == 1


def test_processing_stage_cannot_be_declared_failed_while_still_running(sample):
    _, database, stages, store = sample
    item = identity()
    insert_folder(database, item)
    ticket = store.reserve(item, 2, 'c' * 64)
    stages.claim(item, 2, 'c' * 64)
    with pytest.raises(module.ResultConflict):
        store.record_failed(item, ticket, code='packaging', confirm=lambda *_: True)
    assert store.summary()['unresolved'] == 1


@pytest.mark.parametrize('change', ["UPDATE tasks SET desired_state='paused'",
                                   "UPDATE tasks SET start_generation=3",
                                   "UPDATE llm_folder_queue SET state='llm_processing'"])
def test_reservation_rejects_inactive_or_mismatched_work(sample, change):
    _, database, _, store = sample
    item = identity()
    insert_folder(database, item)
    with sqlite3.connect(database) as db:
        db.execute(change)
    with pytest.raises(module.ResultConflict):
        store.reserve(item, 2, 'c' * 64)
