import copy
import hashlib
from contextlib import contextmanager

import pytest

from test_remote_server_transport import setup, encoded  # shared synthetic fixture


def prepared(x, rpc):
    from capacity_aware_transport import CapacityAwareTransport
    adapter = CapacityAwareTransport(x['config'], config_sha256=hashlib.sha256(encoded(x['config']).encode()).hexdigest(),
        journal=x['journal'], stage_call=x['stage'], rpc_call=rpc)
    staged = adapter.transfer_snapshot(x['job'], x['snapshot'], x['manifest'], verify_locally=False)
    adapter.validate_staging(x['job'], x['snapshot'], staged)
    x['start'][x['job']['job_id']] = dict(x['records'][x['job']['job_id']], staging=copy.deepcopy(staged))
    return adapter, staged


@pytest.mark.parametrize('reason', ['capacity_full', 'admission_busy'])
def test_only_authenticated_capacity_busy_keeps_same_in_process_preflight(setup, reason):
    x, admits = setup, []
    def rpc(connection, command, payload, **kwargs):
        if command.endswith(' admit'):
            admits.append(True)
            if len(admits) == 1:
                return dict(job=x['job'], server_identity=x['server'], accepted=False,
                            reason=reason, this_attempt_started=False)
        return x['rpc'](connection, command, payload, **kwargs)
    adapter, staged = prepared(x, rpc)
    assert adapter.coordinator_callbacks()['submit_once'] == adapter.submit_attempt
    assert adapter.submit_attempt(x['job'], x['snapshot'], staged)['reason'] == reason
    assert adapter.submit_attempt(x['job'], x['snapshot'], staged)['accepted'] is True
    assert len(admits) == 2
    with pytest.raises(x['m'].TransportError):
        adapter.submit_attempt(x['job'], x['snapshot'], staged)


def test_lost_server_ack_never_allows_another_attempt(setup):
    x, admits = setup, []
    def rpc(connection, command, payload, **kwargs):
        if command.endswith(' admit'):
            admits.append(True)
            raise TimeoutError('synthetic response lost')
        return x['rpc'](connection, command, payload, **kwargs)
    adapter, staged = prepared(x, rpc)
    for _ in range(2):
        with pytest.raises(x['m'].TransportError):
            adapter.submit_attempt(x['job'], x['snapshot'], staged)
    assert len(admits) == 1


def test_other_server_rejection_never_becomes_retryable_busy(setup):
    x, admits = setup, []
    def rpc(connection, command, payload, **kwargs):
        if command.endswith(' admit'):
            admits.append(True)
            return dict(job=x['job'], server_identity=x['server'], accepted=False, state='reserved')
        return x['rpc'](connection, command, payload, **kwargs)
    adapter, staged = prepared(x, rpc)
    for _ in range(2):
        with pytest.raises(x['m'].TransportError):
            adapter.submit_attempt(x['job'], x['snapshot'], staged)
    assert len(admits) == 1


def test_server_boolean_cannot_impersonate_frozen_integer_generation(setup):
    x, admits = setup, []
    def rpc(connection, command, payload, **kwargs):
        if command.endswith(' admit'):
            admits.append(True)
            changed = copy.deepcopy(x['job']); changed['generation'] = True
            return dict(job=changed, server_identity=x['server'], accepted=True)
        return x['rpc'](connection, command, payload, **kwargs)
    adapter, staged = prepared(x, rpc)
    with pytest.raises(x['m'].TransportError):
        adapter.submit_attempt(x['job'], x['snapshot'], staged)
    assert len(admits) == 1


def test_busy_then_free_uses_original_preflight_and_releases_task_gate(setup, tmp_path):
    from admission_wait import wait_for_admission
    from remote_jobs import RemoteJobs
    from server_admission import start_with_capacity_result
    from test_server_supervisor import make_job
    x = setup
    ledger = RemoteJobs(tmp_path / 'synthetic-server.sqlite', capacity=1, ledger_id=x['server']['ledger_id'])
    ledger.initialize()
    blocker = make_job(99)
    ledger.reserve(blocker)
    starts, held = [], [False]
    def rpc(connection, command, payload, **kwargs):
        if command.endswith(' admit'):
            return start_with_capacity_result(payload['job'], ledger=ledger, server_identity=x['server'],
                confirm_prepared=lambda _: True, launch_service=lambda item: starts.append(item) or True)
        return x['rpc'](connection, command, payload, **kwargs)
    adapter, staged = prepared(x, rpc)
    @contextmanager
    def gate():
        assert not held[0]
        held[0] = True
        try: yield
        finally: held[0] = False
    def check(): assert held[0]
    def wait():
        assert not held[0]
        ledger.start(blocker['job_id'], blocker['claim_id'])
        ledger.finish(blocker['job_id'], blocker['claim_id'], 'a' * 64, confirm=lambda *_: True)
    response = wait_for_admission(x['job'], x['server'], gate=gate, check=check,
        send=lambda: adapter.submit_attempt(x['job'], x['snapshot'], staged), wait=wait, max_attempts=2)
    assert response['accepted'] is True and starts == [x['job']]


def test_write_contention_waits_then_admits_once_with_original_preflight(setup, tmp_path):
    import sqlite3
    from admission_wait import wait_for_admission
    from remote_jobs import RemoteJobs
    from server_admission import start_with_capacity_result
    x = setup
    ledger = RemoteJobs(tmp_path / 'synthetic-contention.sqlite', capacity=1,
                        ledger_id=x['server']['ledger_id'], busy_timeout=.05)
    ledger.initialize()
    starts, busy_responses, held = [], [], [False]
    blocker = sqlite3.connect(ledger.database)
    blocker.execute('BEGIN IMMEDIATE')
    def rpc(connection, command, payload, **kwargs):
        if command.endswith(' admit'):
            result = start_with_capacity_result(payload['job'], ledger=ledger, server_identity=x['server'],
                confirm_prepared=lambda _: True, launch_service=lambda item: starts.append(item) or True)
            if not result['accepted']:
                busy_responses.append(result['reason'])
            return result
        return x['rpc'](connection, command, payload, **kwargs)
    adapter, staged = prepared(x, rpc)
    @contextmanager
    def gate():
        held[0] = True
        try: yield
        finally: held[0] = False
    def wait():
        assert not held[0]
        blocker.rollback()
    try:
        response = wait_for_admission(x['job'], x['server'], gate=gate, check=lambda: None,
            send=lambda: adapter.submit_attempt(x['job'], x['snapshot'], staged), wait=wait, max_attempts=None)
    finally:
        blocker.close()
    assert response['accepted'] is True and busy_responses == ['admission_busy'] and starts == [x['job']]
