import copy
import sqlite3

import pytest

from test_remote_coordinator import sample  # shared synthetic original task/claim fixture


@pytest.mark.parametrize('max_attempts', [2, None])
def test_definite_busy_rechecks_the_same_task_and_records_one_start_intent(sample, max_attempts):
    x, requests, waits = sample, [], []
    def submit(job, snapshot, staging):
        assert x['gate_depth'][0] == 1
        assert x['journal'].get(job['job_id'])['job'] == job
        requests.append(copy.deepcopy(job))
        if len(requests) == 1:
            return dict(job=job, server_identity=x['server'], accepted=False,
                        reason='capacity_full', this_attempt_started=False)
        return dict(job=job, server_identity=x['server'], accepted=True)
    def wait():
        assert x['gate_depth'][0] == 0
        waits.append(True)
    result = x['run'](submit_once=submit, admission_wait={'wait': wait, 'max_attempts': max_attempts, 'check_gate': lambda: None})
    assert result['state'] == 'waiting_rag' and len(requests) == 2 and requests[0] == requests[1]
    assert len(waits) == 1
    with sqlite3.connect(x['journal'].database) as db:
        assert db.execute('select count(*) from remote_start_intents').fetchone()[0] == 1


def test_pause_during_capacity_wait_never_sends_a_second_server_attempt(sample):
    x, requests = sample, []
    def submit(job, snapshot, staging):
        requests.append(job)
        return dict(job=job, server_identity=x['server'], accepted=False,
                    reason='capacity_full', this_attempt_started=False)
    def pause():
        assert x['gate_depth'][0] == 0
        with sqlite3.connect(x['database']) as db:
            db.execute("update tasks set desired_state='paused' where task_id=?", (x['identity']['task_id'],))
    with pytest.raises(RuntimeError):
        x['run'](submit_once=submit, admission_wait={'wait': pause, 'max_attempts': 3, 'check_gate': lambda: None})
    assert len(requests) == 1 and x['state']() == 'unknown'


def test_lost_ack_is_never_treated_as_capacity_busy(sample):
    x, requests, waits = sample, [], []
    def submit(job, snapshot, staging):
        requests.append(job)
        raise TimeoutError('synthetic acknowledgement lost')
    with pytest.raises(TimeoutError):
        x['run'](submit_once=submit, admission_wait={'wait': lambda: waits.append(True), 'max_attempts': 3, 'check_gate': lambda: None})
    assert len(requests) == 1 and waits == [] and x['state']() == 'unknown'


def test_changed_execution_binding_after_busy_stops_before_another_attempt(sample):
    x, requests, observations = sample, [], []
    def submit(job, snapshot, staging):
        requests.append(job)
        return dict(job=job, server_identity=x['server'], accepted=False,
                    reason='capacity_full', this_attempt_started=False)
    def observe(claim, job, clock):
        evidence = x['observe'](claim, job, clock)
        observations.append(True)
        if len(observations) > 1:
            evidence['binding']['owner'] = 'another-worker'
        return evidence
    with pytest.raises(ValueError, match='ownership'):
        x['run'](submit_once=submit, observe_execution=observe,
                 admission_wait={'wait': lambda: None, 'max_attempts': 3, 'check_gate': lambda: None})
    assert len(requests) == 1 and x['state']() == 'unknown'


def test_changed_durable_start_intent_stops_before_another_attempt(sample):
    x, requests = sample, []
    def submit(job, snapshot, staging):
        requests.append(job)
        return dict(job=job, server_identity=x['server'], accepted=False,
                    reason='capacity_full', this_attempt_started=False)
    def corrupt_intent():
        with sqlite3.connect(x['journal'].database) as db:
            db.execute("update remote_start_intents set intent_json='{}'")
    with pytest.raises(ValueError):
        x['run'](submit_once=submit, admission_wait={'wait': corrupt_intent, 'max_attempts': 3, 'check_gate': lambda: None})
    assert len(requests) == 1 and x['state']() == 'unknown'


def test_closed_postprocessing_gate_during_busy_wait_prevents_another_attempt(sample):
    x, requests, open_gate = sample, [], [True]
    def submit(job, snapshot, staging):
        requests.append(job)
        return dict(job=job, server_identity=x['server'], accepted=False,
                    reason='capacity_full', this_attempt_started=False)
    def recheck():
        if not open_gate[0]:
            raise ValueError('postprocessing gate closed')
    with pytest.raises(ValueError, match='gate closed'):
        x['run'](submit_once=submit,
                 admission_wait={'wait': lambda: open_gate.__setitem__(0, False),
                                 'max_attempts': 3, 'check_gate': recheck})
    assert len(requests) == 1 and x['state']() == 'unknown'


def test_gate_closed_while_binding_is_rechecked_before_server_send(sample):
    x, requests, open_gate = sample, [], [True]
    def bind(claim, job, execution):
        open_gate[0] = False
        return True
    def recheck():
        if not open_gate[0]:
            raise ValueError('postprocessing gate closed')
    with pytest.raises(ValueError, match='gate closed'):
        x['run'](bind_execution=bind,
                 submit_once=lambda *args: requests.append(args),
                 admission_wait={'wait': lambda: None, 'max_attempts': None, 'check_gate': recheck})
    assert requests == [] and x['state']() == 'unknown'
