"""Server jobs cannot multiply when a controller disconnects or submits twice."""
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import os
from uuid import UUID, uuid4, uuid5

import pytest


def test_server_ledger_lock_wait_can_be_bounded(tmp_path):
    import sqlite3
    import time
    database = tmp_path / 'bounded.sqlite3'
    original = RemoteJobs(database, capacity=1)
    ledger_id = original.initialize()
    bounded = RemoteJobs(database, capacity=1, ledger_id=ledger_id, busy_timeout=.15)
    with sqlite3.connect(database, timeout=1) as blocker:
        blocker.execute('BEGIN EXCLUSIVE')
        started = time.monotonic()
        with pytest.raises(sqlite3.OperationalError):
            bounded.reserve(job())
        assert time.monotonic() - started < 2

from remote_jobs import RemoteJobs, JobConflict, CapacityFull


def job(n=0):
    identity = {'task_id': str(uuid4()), 'group_id': f'{n:032x}', 'paper_id': 'paper-1',
                         'receipt_id': 'a' * 64, 'attempt_id': 'attempt-1',
                         'parent_run_id': str(uuid4()), 'task_run_id': str(uuid4())}
    return {'job_id': str(uuid5(UUID('5e9b7732-458e-50cb-8916-6b53007c204f'), identity['task_run_id'])),
            'claim_id': str(uuid4()), 'identity': identity,
            'generation': 3, 'manifest_sha256': 'b' * 64,
            'snapshot_sha256': 'c' * 64, 'code_sha256': 'd' * 64}


def process_attempt(database, ledger_id, item, event, results):
    event.wait(timeout=10)
    try:
        results.put(RemoteJobs(database, capacity=8, ledger_id=ledger_id).reserve(item)['may_start'])
    except CapacityFull:
        results.put(False)


def test_duplicate_submission_never_grants_second_execution(tmp_path):
    store = RemoteJobs(tmp_path / 'jobs.sqlite3', capacity=8); store.initialize()
    item = job()
    first = store.reserve(item)
    assert first['may_start'] is True
    duplicate = store.reserve(item)
    assert duplicate['may_start'] is False
    assert duplicate['state'] == 'reserved'
    with pytest.raises(JobConflict):
        store.reserve(dict(item, snapshot_sha256='e' * 64))
    assert store.status(item['job_id'])['state'] == 'reserved'


def test_eight_is_a_server_side_limit_even_for_concurrent_arrivals(tmp_path):
    store = RemoteJobs(tmp_path / 'jobs.sqlite3', capacity=8); store.initialize()
    items = [job(n) for n in range(16)]
    def attempt(item):
        try:
            return store.reserve(item)['may_start']
        except CapacityFull:
            return False
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(attempt, items))
    assert sum(results) == 8
    assert store.occupied() == 8


@pytest.mark.skipif(os.name != 'posix', reason='server-only process concurrency check')
def test_server_ledger_serializes_competing_processes(tmp_path):
    database = tmp_path / 'jobs.sqlite3'
    store = RemoteJobs(database, capacity=8); store.initialize()
    ctx = multiprocessing.get_context('spawn')
    event, results = ctx.Event(), ctx.Queue()
    processes = [ctx.Process(target=process_attempt, args=(database, store.ledger_id, job(i), event, results))
                 for i in range(12)]
    for process in processes: process.start()
    event.set()
    for process in processes:
        process.join(timeout=20)
        assert process.exitcode == 0
    observed = [results.get(timeout=2) for _ in processes]
    assert sum(observed) == store.occupied() == 8


def test_unknown_still_occupies_slot_and_is_never_implicitly_retried(tmp_path):
    store = RemoteJobs(tmp_path / 'jobs.sqlite3', capacity=1); store.initialize()
    first, second = job(1), job(2)
    store.reserve(first)
    store.start(first['job_id'], first['claim_id'])
    store.mark_unknown(first['job_id'], first['claim_id'])
    with pytest.raises(CapacityFull):
        store.reserve(second)
    assert store.reserve(first)['may_start'] is False
    with pytest.raises(JobConflict):
        store.start(first['job_id'], first['claim_id'])
    with pytest.raises(JobConflict):
        store.finish(first['job_id'], first['claim_id'], 'f' * 64)


def test_only_confirmed_finish_frees_slot_and_keeps_receipt_identity(tmp_path):
    store = RemoteJobs(tmp_path / 'jobs.sqlite3', capacity=1); store.initialize()
    first, second = job(1), job(2)
    store.reserve(first)
    with pytest.raises(JobConflict):
        store.finish(first['job_id'], first['claim_id'], 'f' * 64, confirm=lambda *args: True)
    store.start(first['job_id'], first['claim_id'])
    with pytest.raises(JobConflict):
        store.finish(first['job_id'], str(uuid4()), 'f' * 64, confirm=lambda *args: True)
    with pytest.raises(JobConflict):
        store.finish(first['job_id'], first['claim_id'], 'f' * 64)
    with pytest.raises(JobConflict):
        store.finish(first['job_id'], first['claim_id'], 'f' * 64, confirm=lambda *args: False)
    assert store.occupied() == 1
    store.finish(first['job_id'], first['claim_id'], 'f' * 64, confirm=lambda *args: True)
    assert store.status(first['job_id'])['receipt_sha256'] == 'f' * 64
    assert store.reserve(second)['may_start'] is True
    assert store.reserve(first)['may_start'] is False


def test_different_job_cannot_claim_same_existing_folder(tmp_path):
    store = RemoteJobs(tmp_path / 'jobs.sqlite3', capacity=8); store.initialize()
    first = job(1); store.reserve(first)
    second = dict(first, job_id=str(uuid4()), claim_id=str(uuid4()))
    with pytest.raises(JobConflict):
        store.reserve(second)


def test_job_identity_is_derived_from_original_folder_run(tmp_path):
    store = RemoteJobs(tmp_path / 'jobs.sqlite3', capacity=8); store.initialize()
    wrong = job(1); wrong['job_id'] = str(uuid4())
    with pytest.raises(JobConflict):
        store.reserve(wrong)


def test_missing_or_recreated_ledger_does_not_appear_empty(tmp_path):
    path = tmp_path / 'missing.sqlite3'
    store = RemoteJobs(path, capacity=8)
    with pytest.raises((JobConflict, OSError)):
        store.occupied()
    assert not path.exists()


def test_invalid_frozen_identity_is_rejected(tmp_path):
    store = RemoteJobs(tmp_path / 'jobs.sqlite3', capacity=8); store.initialize()
    bad = job(); bad['identity']['receipt_id'] = 'not-a-sha'
    with pytest.raises(JobConflict):
        store.reserve(bad)
