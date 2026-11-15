"""Wait for an authenticated, definitely-not-started admission reply.

No queue or retry is installed here. The caller must persist the original
start intent and recheck its existing task/claim and execution evidence in
``check``. An ambiguous transport or service response immediately propagates;
only a matching no-start busy response permits another bounded attempt. An
explicit None attempt limit keeps waiting while the original worker is alive.
"""
import copy
import json


class CapacityStillBusy(RuntimeError):
    pass


def _encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def wait_for_admission(job, server_identity, *, gate, check, send, wait, max_attempts, before_gate=None):
    if (not isinstance(job, dict) or not isinstance(server_identity, dict)
            or not all(callable(fn) for fn in (gate, check, send, wait))
            or (before_gate is not None and not callable(before_gate))
            or (max_attempts is not None and
                (type(max_attempts) is not int or not 1 <= max_attempts <= 1000))):
        raise ValueError('one original admission and its existing gate are required')
    original_job, original_server = copy.deepcopy(job), copy.deepcopy(server_identity)
    job_bytes, server_bytes = _encoded(original_job), _encoded(original_server)
    number = 0
    while max_attempts is None or number < max_attempts:
        if before_gate is not None:
            before_gate()
        with gate():
            check()
            if _encoded(job) != job_bytes or _encoded(server_identity) != server_bytes:
                raise ValueError('original admission identity changed')
            response = send()
            if (_encoded(job) != job_bytes or _encoded(server_identity) != server_bytes or not isinstance(response, dict)
                    or _encoded(response.get('job')) != job_bytes or _encoded(response.get('server_identity')) != server_bytes
                    or type(response.get('accepted')) is not bool):
                raise ValueError('server admission identity is unconfirmed')
            if response == dict(job=original_job, server_identity=original_server, accepted=True):
                return copy.deepcopy(response)
            reason = response.get('reason')
            busy = dict(job=original_job, server_identity=original_server, accepted=False,
                        reason=reason, this_attempt_started=False)
            if (reason not in {'capacity_full', 'admission_busy'}
                    or type(response.get('this_attempt_started')) is not bool or response != busy):
                raise ValueError('server admission outcome is unconfirmed')
        number += 1
        if max_attempts is None or number < max_attempts:
            wait()  # The existing task gate is never held while waiting.
    raise CapacityStillBusy('server capacity remains occupied; no start was confirmed')
