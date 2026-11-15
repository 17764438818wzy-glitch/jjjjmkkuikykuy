"""Isolated tests for a server-side task pause fence; no real papers or queue."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
import sqlite3
import subprocess
import sys
import threading
from uuid import UUID, uuid4, uuid5

import pytest

from remote_jobs import JobConflict, RemoteJobs
from task_generation_fence import FenceError, IMMUTABLE_TRIGGER, TaskGenerationFence


def item(task=None, generation=1, group=1):
    identity = dict(task_id=task or str(uuid4()), group_id=f'{group:032x}', paper_id='synthetic',
                    receipt_id='a' * 64, attempt_id='synthetic', parent_run_id=str(uuid4()),
                    task_run_id=str(uuid4()))
    return dict(job_id=str(uuid5(UUID('5e9b7732-458e-50cb-8916-6b53007c204f'),
                                identity['task_run_id'])), claim_id=str(uuid4()), identity=identity,
                generation=generation, manifest_sha256='b' * 64,
                snapshot_sha256='c' * 64, code_sha256='d' * 64)


def initialized(tmp_path, capacity=8):
    ledger = RemoteJobs(tmp_path / 'isolated.sqlite3', capacity=capacity)
    ledger.initialize()
    fence = TaskGenerationFence(ledger)
    fence.install()
    return ledger, fence


def test_closed_generation_rejects_old_admission_but_preserves_new_generation_and_other_task(tmp_path):
    ledger, fence = initialized(tmp_path)
    task = str(uuid4())
    assert fence.close(task, 3)['closed_through'] == 3
    assert fence.close(task, 2)['closed_through'] == 3  # late pause cannot reopen
    old = item(task, 3)
    with pytest.raises(JobConflict):
        ledger.reserve(old)  # the original unmodified server ledger is subject to the trigger
    with pytest.raises(JobConflict):
        ledger.status(old['job_id'])
    assert ledger.reserve(item(task, 4, 2))['may_start'] is True
    assert ledger.reserve(item(str(uuid4()), 1, 3))['may_start'] is True


def test_earlier_admission_is_observable_and_duplicate_cannot_start_again(tmp_path):
    ledger, fence = initialized(tmp_path)
    old = item(generation=2)
    assert ledger.reserve(old)['may_start'] is True
    response = fence.close(old['identity']['task_id'], 2)
    assert response['jobs'] == {'reserved': 1, 'running': 0, 'unknown': 0, 'finished': 0}
    assert ledger.reserve(old) == {'may_start': False, 'state': 'reserved'}
    ledger.start(old['job_id'], old['claim_id'])
    assert fence.status(old['identity']['task_id'])['jobs']['running'] == 1


def test_close_and_reserve_are_atomic_under_competition(tmp_path):
    ledger, fence = initialized(tmp_path)
    task = str(uuid4())
    jobs = [item(task, 1, n) for n in range(1, 33)]
    def reserve(job):
        try:
            return ledger.reserve(job)['may_start']
        except (JobConflict, RuntimeError):
            return False
    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = [pool.submit(reserve, job) for job in jobs]
        closed = pool.submit(fence.close, task, 1)
        results = [f.result() for f in futures]
        snapshot = closed.result()
    after = fence.status(task)
    assert snapshot['closed_through'] == after['closed_through'] == 1
    assert after['jobs']['reserved'] == sum(results)
    # Free an existing capacity slot if the old ledger filled it before the
    # close; otherwise its capacity guard can reject before reaching the fence.
    if sum(results) == 8:
        previous = next(job for job, accepted in zip(jobs, results) if accepted)
        ledger.start(previous['job_id'], previous['claim_id'])
        ledger.finish(previous['job_id'], previous['claim_id'], 'e' * 64, confirm=lambda *_: True)
    with pytest.raises(JobConflict):
        ledger.reserve(item(task, 1, 99))


def test_bad_or_missing_trigger_is_never_reported_as_a_valid_close(tmp_path):
    ledger, fence = initialized(tmp_path)
    with sqlite3.connect(ledger.database) as db:
        db.execute('DROP TRIGGER paperflow_postprocess_task_fence')
    with pytest.raises(FenceError):
        fence.close(str(uuid4()), 1)


@pytest.mark.parametrize('generation', [0, -1, True, '1', 2**63])
def test_bad_generation_cannot_be_closed(tmp_path, generation):
    _, fence = initialized(tmp_path)
    with pytest.raises(FenceError):
        fence.close(str(uuid4()), generation)


def test_malformed_legacy_row_is_not_reported_as_safe(tmp_path):
    ledger, fence = initialized(tmp_path)
    row = item()
    ledger.reserve(row)
    with sqlite3.connect(ledger.database) as db:
        db.execute('DROP TRIGGER paperflow_postprocess_frozen_identity')
        db.execute("UPDATE executor_jobs SET frozen_json='{}' WHERE job_id=?", (row['job_id'],))
        db.execute(IMMUTABLE_TRIGGER)
    with pytest.raises(FenceError):
        fence.close(row['identity']['task_id'], 1)
    # The durable close survives a failed status inspection.
    with sqlite3.connect(ledger.database) as db:
        assert db.execute('SELECT closed_through FROM task_generation_fences WHERE task_id=?',
                          (row['identity']['task_id'],)).fetchone() == (1,)


def test_legacy_row_with_wrong_relational_task_cannot_hide_original_job(tmp_path):
    ledger, fence = initialized(tmp_path)
    row = item()
    ledger.reserve(row)
    with sqlite3.connect(ledger.database) as db:
        db.execute('DROP TRIGGER paperflow_postprocess_frozen_identity')
        db.execute('UPDATE executor_jobs SET task_id=? WHERE job_id=?', (str(uuid4()), row['job_id']))
        db.execute(IMMUTABLE_TRIGGER)
    with pytest.raises(FenceError):
        fence.close(row['identity']['task_id'], row['generation'])


def test_slow_status_validation_does_not_hold_read_transaction_against_other_task(tmp_path, monkeypatch):
    import task_generation_fence as fence_module

    ledger, fence = initialized(tmp_path)
    first = item()
    ledger.reserve(first)
    fence.close(first['identity']['task_id'], first['generation'])
    with sqlite3.connect(ledger.database) as db:
        assert db.execute('PRAGMA journal_mode').fetchone()[0].lower() == 'delete'

    validating, release = threading.Event(), threading.Event()
    original = fence_module._validated
    def slow_validate(value):
        validating.set()
        assert release.wait(timeout=5)
        return original(value)
    monkeypatch.setattr(fence_module, '_validated', slow_validate)
    short_wait_ledger = RemoteJobs(ledger.database, capacity=ledger.capacity,
                                   ledger_id=ledger.ledger_id, busy_timeout=.05)
    with ThreadPoolExecutor(max_workers=1) as pool:
        status = pool.submit(fence.status, first['identity']['task_id'])
        assert validating.wait(timeout=5)
        try:
            result = short_wait_ledger.reserve(item())
        finally:
            release.set()
        assert result['may_start'] is True
        assert status.result(timeout=5)['jobs']['reserved'] == 1


def test_status_candidate_lookup_uses_task_and_malformed_identity_indexes(tmp_path):
    from task_generation_fence import STATUS_JOBS_QUERY

    ledger, _ = initialized(tmp_path)
    task = str(uuid4())
    with sqlite3.connect(ledger.database) as db:
        plan = [row[3] for row in db.execute('EXPLAIN QUERY PLAN ' + STATUS_JOBS_QUERY, (task, task))]
    assert any('fence_frozen_task_idx' in step for step in plan), plan
    assert any('fence_invalid_task_idx' in step for step in plan), plan
    assert any('task_id' in step and 'SEARCH' in step for step in plan), plan


def test_unrelated_malformed_frozen_identity_keeps_status_uncertain(tmp_path):
    ledger, fence = initialized(tmp_path)
    unrelated = item()
    ledger.reserve(unrelated)
    with sqlite3.connect(ledger.database) as db:
        db.execute('DROP TRIGGER paperflow_postprocess_frozen_identity')
        db.execute("UPDATE executor_jobs SET frozen_json='not-json' WHERE job_id=?", (unrelated['job_id'],))
        db.execute(IMMUTABLE_TRIGGER)
    with pytest.raises(FenceError):
        fence.close(str(uuid4()), 1)


def test_status_rejects_schema_change_during_outside_transaction_validation(tmp_path, monkeypatch):
    import task_generation_fence as fence_module

    ledger, fence = initialized(tmp_path)
    first = item()
    ledger.reserve(first)
    fence.close(first['identity']['task_id'], first['generation'])
    validating, release = threading.Event(), threading.Event()
    original = fence_module._validated
    def slow_validate(value):
        validating.set()
        assert release.wait(timeout=5)
        return original(value)
    monkeypatch.setattr(fence_module, '_validated', slow_validate)
    with ThreadPoolExecutor(max_workers=1) as pool:
        status = pool.submit(fence.status, first['identity']['task_id'])
        assert validating.wait(timeout=5)
        try:
            with sqlite3.connect(ledger.database, timeout=.1) as db:
                db.execute('DROP TRIGGER paperflow_postprocess_task_fence')
        finally:
            release.set()
        with pytest.raises(FenceError):
            status.result(timeout=5)


def test_install_rejects_legacy_duplicate_task_key_without_partial_schema(tmp_path):
    ledger = RemoteJobs(tmp_path / 'legacy.sqlite3', capacity=8)
    ledger.initialize()
    first = item()
    ledger.reserve(first)
    other_task = str(uuid4())
    with sqlite3.connect(ledger.database) as db:
        raw = db.execute('SELECT frozen_json FROM executor_jobs WHERE job_id=?', (first['job_id'],)).fetchone()[0]
        needle = '"task_id":"' + first['identity']['task_id'] + '"'
        duplicate = '"task_id":"' + other_task + '",' + needle
        assert raw.count(needle) == 1
        db.execute('UPDATE executor_jobs SET task_id=?,frozen_json=? WHERE job_id=?',
                   (other_task, raw.replace(needle, duplicate), first['job_id']))
    with pytest.raises(FenceError):
        TaskGenerationFence(ledger).install()
    with sqlite3.connect(ledger.database) as db:
        assert not db.execute("SELECT 1 FROM sqlite_master WHERE name='task_generation_fences'").fetchone()


def test_installed_fence_preserves_frozen_identity_and_rejects_duplicate_keys(tmp_path):
    ledger, _ = initialized(tmp_path)
    first = item()
    ledger.reserve(first)
    with sqlite3.connect(ledger.database) as db:
        with pytest.raises(sqlite3.IntegrityError):
            db.execute('UPDATE executor_jobs SET task_id=? WHERE job_id=?', (str(uuid4()), first['job_id']))
        with pytest.raises(sqlite3.IntegrityError):
            db.execute('DELETE FROM executor_jobs WHERE job_id=?', (first['job_id'],))
    ledger.start(first['job_id'], first['claim_id'])  # ordinary state transitions still work
    second = item()
    raw = json.dumps(second, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    needle = '"task_id":"' + second['identity']['task_id'] + '"'
    duplicate = needle + ',"task_id":"' + str(uuid4()) + '"'
    with sqlite3.connect(ledger.database) as db:
        with pytest.raises(sqlite3.IntegrityError):
            db.execute('INSERT INTO executor_jobs VALUES(?,?,?,?,?,?,?,?,?,?)',
                (second['job_id'], second['claim_id'], second['identity']['task_id'],
                 second['identity']['group_id'], second['identity']['task_run_id'],
                 raw.replace(needle, duplicate), 'reserved', None, 1, 1))


def test_old_connection_invalid_state_check_uses_partial_index(tmp_path):
    ledger, _ = initialized(tmp_path)
    with sqlite3.connect(ledger.database) as db:
        plan = [row[3] for row in db.execute("EXPLAIN QUERY PLAN SELECT 1 FROM executor_jobs "
            "WHERE state NOT IN ('reserved','running','unknown','finished') LIMIT 1")]
    assert any('fence_invalid_state_idx' in step for step in plan), plan


@pytest.mark.parametrize('unique_key', ['job_id', 'claim_id', 'task_run_id', 'task_group'])
def test_insert_or_replace_cannot_replace_existing_frozen_job(tmp_path, unique_key):
    ledger, _ = initialized(tmp_path)
    old, replacement = item(), item(group=2)
    ledger.reserve(old)
    if unique_key == 'job_id': replacement['job_id'] = old['job_id']
    if unique_key == 'claim_id': replacement['claim_id'] = old['claim_id']
    if unique_key == 'task_run_id': replacement['identity']['task_run_id'] = old['identity']['task_run_id']
    if unique_key == 'task_group':
        replacement['identity']['task_id'] = old['identity']['task_id']
        replacement['identity']['group_id'] = old['identity']['group_id']
    raw = json.dumps(replacement, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    ident = replacement['identity']
    with sqlite3.connect(ledger.database) as db:
        with pytest.raises(sqlite3.IntegrityError):
            db.execute('INSERT OR REPLACE INTO executor_jobs VALUES(?,?,?,?,?,?,?,?,?,?)',
                (replacement['job_id'], replacement['claim_id'], ident['task_id'], ident['group_id'],
                 ident['task_run_id'], raw, 'reserved', None, 1, 1))
        assert db.execute('SELECT COUNT(*) FROM executor_jobs WHERE job_id=?', (old['job_id'],)).fetchone() == (1,)


def test_insert_rejects_nul_suffixed_json_key_with_different_task_identity(tmp_path):
    ledger, _ = initialized(tmp_path)
    row, other_task = item(), str(uuid4())
    raw = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    needle = '"task_id":"' + row['identity']['task_id'] + '"'
    misleading = '"task_id\\u0000x":"' + other_task + '",' + needle
    assert raw.count(needle) == 1
    with sqlite3.connect(ledger.database) as db:
        with pytest.raises(sqlite3.IntegrityError):
            db.execute('INSERT INTO executor_jobs VALUES(?,?,?,?,?,?,?,?,?,?)',
                (row['job_id'], row['claim_id'], other_task, row['identity']['group_id'],
                 row['identity']['task_run_id'], raw.replace(needle, misleading), 'reserved', None, 1, 1))


def test_install_aborts_if_ledger_changes_during_legacy_validation(tmp_path, monkeypatch):
    import task_generation_fence as fence_module

    ledger = RemoteJobs(tmp_path / 'changing.sqlite3', capacity=8)
    ledger.initialize()
    ledger.reserve(item())
    fence = TaskGenerationFence(ledger)
    validating, release = threading.Event(), threading.Event()
    original = fence_module._validate_row
    def slow_validate(row):
        validating.set()
        assert release.wait(timeout=5)
        return original(row)
    monkeypatch.setattr(fence_module, '_validate_row', slow_validate)
    with ThreadPoolExecutor(max_workers=1) as pool:
        installed = pool.submit(fence.install)
        assert validating.wait(timeout=5)
        try:
            assert ledger.reserve(item())['may_start'] is True
        finally:
            release.set()
        with pytest.raises(FenceError):
            installed.result(timeout=5)
    with sqlite3.connect(ledger.database) as db:
        assert not db.execute("SELECT 1 FROM sqlite_master WHERE name='task_generation_fences'").fetchone()


def test_closed_generation_cannot_be_deleted_lowered_or_replaced(tmp_path):
    ledger, fence = initialized(tmp_path)
    task = str(uuid4())
    fence.close(task, 2)
    with sqlite3.connect(ledger.database) as db:
        for statement, params in (
            ('DELETE FROM task_generation_fences WHERE task_id=?', (task,)),
            ('UPDATE task_generation_fences SET task_id=? WHERE task_id=?', (str(uuid4()), task)),
            ('UPDATE task_generation_fences SET closed_through=1 WHERE task_id=?', (task,)),
            ('INSERT OR REPLACE INTO task_generation_fences VALUES(?,?,?)', (task, 1, 1)),
        ):
            with pytest.raises(sqlite3.IntegrityError):
                db.execute(statement, params)
    assert fence.close(task, 3)['closed_through'] == 3
    assert fence.close(task, 1)['closed_through'] == 3


@pytest.mark.skipif(not os.environ.get('PAPERFLOW_HISTORICAL_APP'),
                    reason='requires separately installed historical server app')
def test_original_app_can_reserve_transition_finish_and_observe_duplicate(tmp_path):
    ledger, _ = initialized(tmp_path)
    row = item()
    program = '''import json,sys
sys.path.insert(0,sys.argv[1])
from remote_jobs import RemoteJobs
req=json.loads(sys.stdin.read())
ledger=RemoteJobs(req["db"],capacity=8,ledger_id=req["ledger"])
job=req["job"]
assert ledger.reserve(job)["may_start"] is True
assert ledger.reserve(job)=={"may_start":False,"state":"reserved"}
assert ledger.start(job["job_id"],job["claim_id"]) == "running"
assert ledger.finish(job["job_id"],job["claim_id"],"f"*64,confirm=lambda *_: True) == "finished"
assert ledger.reserve(job)=={"may_start":False,"state":"finished"}
print("COMPATIBLE")
'''
    result = subprocess.run([sys.executable, '-c', program, os.environ['PAPERFLOW_HISTORICAL_APP']],
        cwd=tmp_path, input=json.dumps(dict(db=str(ledger.database), ledger=ledger.ledger_id, job=row)),
        capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == 'COMPATIBLE'


def test_trigger_rejects_direct_invalid_or_mismatched_frozen_identity(tmp_path):
    ledger, _ = initialized(tmp_path)
    row = item()
    altered = dict(row)
    altered['identity'] = dict(row['identity'], task_id=str(uuid4()))
    with sqlite3.connect(ledger.database) as db:
        with pytest.raises(sqlite3.IntegrityError):
            db.execute('INSERT INTO executor_jobs VALUES(?,?,?,?,?,?,?,?,?,?)',
                (row['job_id'], row['claim_id'], row['identity']['task_id'], row['identity']['group_id'],
                 row['identity']['task_run_id'], json.dumps(altered), 'reserved', None, 1, 1))


@pytest.mark.skipif(not os.environ.get('PAPERFLOW_HISTORICAL_APP'),
                    reason='requires separately installed historical server app')
def test_delayed_original_app_admission_cannot_cross_fence(tmp_path):
    ledger, fence = initialized(tmp_path)
    late = item(generation=3)
    program = '''import json,sys
sys.path.insert(0, sys.argv[1])
import remote_jobs
from remote_jobs import RemoteJobs, JobConflict
request=json.loads(sys.stdin.readline())
print("READY", flush=True)
sys.stdin.readline()
try:
    result=RemoteJobs(request["db"],capacity=8,ledger_id=request["ledger"]).reserve(request["job"])
    print("START" if result["may_start"] else "DUPLICATE", flush=True)
except JobConflict:
    print("REJECTED", flush=True)
'''
    proc = subprocess.Popen([sys.executable, '-c', program, os.environ['PAPERFLOW_HISTORICAL_APP']],
        cwd=tmp_path, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        proc.stdin.write(json.dumps(dict(db=str(ledger.database), ledger=ledger.ledger_id, job=late)) + '\n')
        proc.stdin.flush()
        assert proc.stdout.readline().strip() == 'READY'
        fence.close(late['identity']['task_id'], late['generation'])
        output, errors = proc.communicate(input='continue\n', timeout=10)
        assert proc.returncode == 0, errors
        assert output.strip() == 'REJECTED'
        with pytest.raises(JobConflict):
            ledger.status(late['job_id'])
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
