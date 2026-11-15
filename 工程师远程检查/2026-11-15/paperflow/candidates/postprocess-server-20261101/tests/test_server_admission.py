"""Admission tests use one disposable ledger and synthetic jobs."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import pytest

from remote_jobs import RemoteJobs, CapacityFull
from server_admission import AdmissionError, start_once
from test_server_supervisor import make_job


def setup(tmp_path):
    ledger = RemoteJobs(tmp_path / 'ledger.sqlite', capacity=8)
    ledger_id = ledger.initialize()
    return ledger, dict(server_id='synthetic-server', ledger_id=ledger_id)


def test_capacity_busy_is_an_authenticated_no_start_result(tmp_path):
    from remote_jobs import JobConflict
    from server_admission import start_with_capacity_result
    ledger = RemoteJobs(tmp_path / 'busy-ledger.sqlite', capacity=1)
    ledger.initialize()
    identity = dict(server_id='synthetic-server', ledger_id=ledger.ledger_id)
    first, second, calls = make_job(1), make_job(2), []
    ledger.reserve(first)
    result = start_with_capacity_result(second, ledger=ledger, server_identity=identity,
        confirm_prepared=lambda actual: True, launch_service=lambda actual: calls.append(actual))
    assert result == dict(job=second, server_identity=identity, accepted=False,
                          reason='capacity_full', this_attempt_started=False)
    assert calls == []
    with pytest.raises(JobConflict):
        ledger.status(second['job_id'])
    ledger.start(first['job_id'], first['claim_id'])
    ledger.finish(first['job_id'], first['claim_id'], 'a' * 64, confirm=lambda *_: True)
    admitted = start_with_capacity_result(second, ledger=ledger, server_identity=identity,
        confirm_prepared=lambda actual: True, launch_service=lambda actual: calls.append(actual) or True)
    assert admitted['accepted'] is True and calls == [second]


def test_unknown_service_ack_is_not_reported_as_safe_capacity_busy(tmp_path):
    from server_admission import start_with_capacity_result
    ledger, identity = setup(tmp_path)
    item = make_job()
    def unknown(_):
        raise TimeoutError('service acknowledgement missing')
    with pytest.raises(AdmissionError):
        start_with_capacity_result(item, ledger=ledger, server_identity=identity,
            confirm_prepared=lambda actual: True, launch_service=unknown)
    assert ledger.status(item['job_id'])['state'] == 'unknown'


def test_contended_reservation_is_confirmed_unstarted_and_can_wait(tmp_path):
    import sqlite3
    from remote_jobs import JobConflict
    from server_admission import start_with_capacity_result
    ledger = RemoteJobs(tmp_path / 'busy-write.sqlite', capacity=1, busy_timeout=.05)
    ledger.initialize()
    identity = dict(server_id='synthetic-server', ledger_id=ledger.ledger_id)
    item, starts = make_job(), []
    with sqlite3.connect(ledger.database) as blocker:
        blocker.execute('BEGIN IMMEDIATE')
        result = start_with_capacity_result(item, ledger=ledger, server_identity=identity,
            confirm_prepared=lambda _: True, launch_service=lambda actual: starts.append(actual) or True)
        assert result == dict(job=item, server_identity=identity, accepted=False,
                              reason='admission_busy', this_attempt_started=False)
        assert starts == []
    with pytest.raises(JobConflict):
        ledger.status(item['job_id'])
    result = start_with_capacity_result(item, ledger=ledger, server_identity=identity,
        confirm_prepared=lambda _: True, launch_service=lambda actual: starts.append(actual) or True)
    assert result['accepted'] is True and starts == [item]


def test_busy_label_only_catches_the_ledger_capacity_decision(tmp_path):
    from server_admission import start_with_capacity_result
    ledger, identity = setup(tmp_path)
    def invalid_preflight(_):
        raise CapacityFull('synthetic unrelated failure')
    with pytest.raises(CapacityFull):
        start_with_capacity_result(make_job(), ledger=ledger, server_identity=identity,
            confirm_prepared=invalid_preflight, launch_service=lambda _: True)


def test_one_admitted_job_gets_one_fixed_service_launch(tmp_path):
    ledger, identity = setup(tmp_path)
    job, calls = make_job(), []
    def launch(actual): calls.append(actual); return True
    result = start_once(job, ledger=ledger, server_identity=identity,
                        confirm_prepared=lambda actual: actual == job, launch_service=launch)
    assert result == dict(job=job, server_identity=identity, accepted=True)
    assert calls == [job] and ledger.occupied() == 1
    second = start_once(job, ledger=ledger, server_identity=identity,
                        confirm_prepared=lambda actual: True, launch_service=launch)
    assert second['accepted'] is False and calls == [job]


def test_launch_timeout_keeps_unknown_slot_and_cannot_relaunch(tmp_path):
    ledger, identity = setup(tmp_path)
    job, calls = make_job(), []
    def timeout(actual): calls.append(actual); raise TimeoutError('unknown service result')
    with pytest.raises(AdmissionError):
        start_once(job, ledger=ledger, server_identity=identity,
                   confirm_prepared=lambda actual: True, launch_service=timeout)
    assert ledger.status(job['job_id'])['state'] == 'unknown'
    assert ledger.occupied() == 1
    assert start_once(job, ledger=ledger, server_identity=identity,
                      confirm_prepared=lambda actual: True, launch_service=timeout)['accepted'] is False
    assert calls == [job]


def test_unverified_staging_never_reserves_or_launches(tmp_path):
    ledger, identity = setup(tmp_path)
    calls = []
    with pytest.raises(AdmissionError):
        start_once(make_job(), ledger=ledger, server_identity=identity,
                   confirm_prepared=lambda actual: False, launch_service=lambda actual: calls.append(actual))
    assert ledger.occupied() == 0 and not calls


def test_competing_server_admissions_reserve_at_most_eight(tmp_path):
    ledger, identity = setup(tmp_path)
    jobs = [make_job(i + 1) for i in range(12)]
    def attempt(job):
        try:
            return start_once(job, ledger=ledger, server_identity=identity,
                              confirm_prepared=lambda actual: True, launch_service=lambda actual: True)['accepted']
        except CapacityFull:
            return False
    with ThreadPoolExecutor(max_workers=12) as pool:
        accepted = list(pool.map(attempt, jobs))
    assert sum(accepted) == 8
    assert ledger.occupied() == 8
