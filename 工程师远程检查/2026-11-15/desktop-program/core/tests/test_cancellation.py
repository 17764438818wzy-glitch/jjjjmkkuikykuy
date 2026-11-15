from concurrent.futures import ThreadPoolExecutor
import threading
import time


def test_concurrent_pause_and_timeout_close_only_once_without_blocking():
    from paperflow.cancellation import Cancellation
    entered, release = threading.Event(), threading.Event()
    calls = []
    def close():
        calls.append(time.monotonic()); entered.set(); release.wait(2)
    cancel = Cancellation(close)
    started = time.monotonic()
    with ThreadPoolExecutor(2) as pool:
        list(pool.map(cancel.request, ['pause', 'total timeout']))
    assert time.monotonic() - started < .5
    assert entered.wait(1) and len(calls) == 1
    release.set()
    assert cancel.finished.wait(1)
    assert cancel.requested_at <= cancel.finished_at


def test_close_failure_is_evidence_not_claim_of_exit():
    from paperflow.cancellation import Cancellation
    def close(): raise RuntimeError('synthetic failure')
    cancel = Cancellation(close)
    cancel.request('pause')
    assert cancel.finished.wait(1)
    assert cancel.error == 'RuntimeError'
