from contextlib import contextmanager

import pytest


@pytest.mark.parametrize('reason', ['capacity_full', 'admission_busy'])
def test_busy_wait_releases_the_existing_task_gate_before_retrying(reason):
    from admission_wait import wait_for_admission
    held, checks, requests, waits = [False], [], [], []
    job, server = {'job_id': 'job'}, {'server_id': 'server'}
    @contextmanager
    def gate():
        assert not held[0]
        held[0] = True
        try: yield
        finally: held[0] = False
    def check():
        assert held[0]
        checks.append(True)
    def send():
        assert held[0]
        requests.append(True)
        if len(requests) == 1:
            return dict(job=job, server_identity=server, accepted=False,
                        reason=reason, this_attempt_started=False)
        return dict(job=job, server_identity=server, accepted=True)
    def wait():
        assert not held[0]
        waits.append(True)
    result = wait_for_admission(job, server, gate=gate, check=check, send=send, wait=wait, max_attempts=2)
    assert result['accepted'] is True and len(checks) == len(requests) == 2 and len(waits) == 1


def test_fresh_execution_observation_occurs_outside_the_task_gate():
    from admission_wait import wait_for_admission
    held, observed = [False], []
    job, server = {'job_id': 'job'}, {'server_id': 'server'}
    @contextmanager
    def gate():
        held[0] = True
        try: yield
        finally: held[0] = False
    def observe():
        assert not held[0]
        observed.append(True)
    result = wait_for_admission(job, server, gate=gate, before_gate=observe, check=lambda: None,
        send=lambda: dict(job=job, server_identity=server, accepted=True), wait=lambda: None, max_attempts=1)
    assert result['accepted'] is True and observed == [True]


@pytest.mark.parametrize('max_attempts', [3, None])
def test_paused_parent_prevents_the_next_server_attempt(max_attempts):
    from admission_wait import wait_for_admission
    job, server, sends = {'job_id': 'job'}, {'server_id': 'server'}, []
    @contextmanager
    def gate(): yield
    def check():
        if sends: raise ValueError('parent paused')
    def send():
        sends.append(True)
        return dict(job=job, server_identity=server, accepted=False,
                    reason='capacity_full', this_attempt_started=False)
    with pytest.raises(ValueError, match='parent paused'):
        wait_for_admission(job, server, gate=gate, check=check, send=send, wait=lambda: None, max_attempts=max_attempts)
    assert len(sends) == 1


@pytest.mark.parametrize('max_attempts', [3, None])
def test_missing_acknowledgement_never_triggers_another_attempt(max_attempts):
    from admission_wait import wait_for_admission
    sends = []
    @contextmanager
    def gate(): yield
    def send():
        sends.append(True)
        raise TimeoutError('outcome unknown')
    with pytest.raises(TimeoutError):
        wait_for_admission({'job_id': 'job'}, {'server_id': 'server'}, gate=gate,
                           check=lambda: None, send=send, wait=lambda: None, max_attempts=max_attempts)
    assert len(sends) == 1


def test_long_capacity_wait_continues_without_losing_the_original_job():
    from admission_wait import wait_for_admission
    job, server = {'job_id': 'job'}, {'server_id': 'server'}
    held, requests, waits = [False], [], []
    @contextmanager
    def gate():
        assert not held[0]
        held[0] = True
        try: yield
        finally: held[0] = False
    def send():
        requests.append(True)
        if len(requests) <= 1001:
            return dict(job=job, server_identity=server, accepted=False,
                        reason='capacity_full', this_attempt_started=False)
        return dict(job=job, server_identity=server, accepted=True)
    def wait():
        assert not held[0]
        waits.append(True)
    result = wait_for_admission(job, server, gate=gate, check=lambda: None,
                                send=send, wait=wait, max_attempts=None)
    assert result['accepted'] is True and len(requests) == 1002 and len(waits) == 1001


def test_unrecognized_or_foreign_busy_response_never_authorizes_retry():
    from admission_wait import wait_for_admission
    sends = []
    @contextmanager
    def gate(): yield
    def send():
        sends.append(True)
        return dict(job={'job_id': 'other'}, server_identity={'server_id': 'server'},
                    accepted=False, reason='capacity_full', this_attempt_started=False)
    with pytest.raises(ValueError):
        wait_for_admission({'job_id': 'job'}, {'server_id': 'server'}, gate=gate,
                           check=lambda: None, send=send, wait=lambda: None, max_attempts=3)
    assert len(sends) == 1


@pytest.mark.parametrize('accepted,started', [(0, False), (False, 0), (1, False)])
def test_integer_lookalikes_do_not_count_as_authenticated_boolean_results(accepted, started):
    from admission_wait import wait_for_admission
    sends = []
    job, server = {'job_id': 'job'}, {'server_id': 'server'}
    @contextmanager
    def gate(): yield
    def send():
        sends.append(True)
        return dict(job=job, server_identity=server, accepted=accepted,
                    reason='capacity_full', this_attempt_started=started)
    with pytest.raises(ValueError):
        wait_for_admission(job, server, gate=gate, check=lambda: None, send=send,
                           wait=lambda: None, max_attempts=3)
    assert len(sends) == 1


def test_boolean_cannot_impersonate_integer_in_frozen_job_identity():
    from admission_wait import wait_for_admission
    job, server = {'job_id': 'job', 'generation': 1}, {'server_id': 'server'}
    @contextmanager
    def gate(): yield
    def send():
        return dict(job={'job_id': 'job', 'generation': True}, server_identity=server, accepted=True)
    with pytest.raises(ValueError):
        wait_for_admission(job, server, gate=gate, check=lambda: None, send=send,
                           wait=lambda: None, max_attempts=1)
