"""Synthetic remote effects; no Prefect server, RAGFlow, NAS, or real data."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import importlib.util
from pathlib import Path
import subprocess
import sys
import threading
import time

import pytest


path = Path(__file__).resolve().parents[1] / 'effect_journal.py'
if path.exists():
    spec = importlib.util.spec_from_file_location('rag_effect_journal', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
else:
    module = None


def test_remote_effect_journal_is_available():
    assert module is not None, 'new Prefect external-effect journal is missing'


class Permit:
    def __init__(self, *, allowed=True):
        self.allowed = allowed

    def start_transport(self, record_intent):
        if not self.allowed:
            raise InterruptedError('task is paused')
        record_intent()

    def check_current(self):
        if not self.allowed:
            raise InterruptedError('task is paused')


@contextmanager
def admission(*, allowed=True):
    yield Permit(allowed=allowed)


@pytest.fixture
def journal(tmp_path):
    assert module is not None, 'new Prefect external-effect journal is missing'
    ledger = module.EffectJournal(tmp_path / 'effects.sqlite3')
    ledger.initialize()
    return ledger


def test_confirmed_effect_returns_saved_response_without_sending_again(journal):
    sends = []
    def send():
        sends.append('sent')
        return {'document_id': 'd1'}
    assert journal.perform('e' * 64, 'a' * 64, send, admission()) == {'document_id': 'd1'}
    assert journal.perform('e' * 64, 'a' * 64, send, admission()) == {'document_id': 'd1'}
    assert sends == ['sent']


def test_changed_payload_cannot_reuse_a_confirmed_effect_identity(journal):
    journal.perform('e' * 64, 'a' * 64, lambda: 'done', admission())
    with pytest.raises(module.EffectConflict):
        journal.perform('e' * 64, 'b' * 64, lambda: pytest.fail('must not send'), admission())


def test_lost_response_leaves_unknown_and_restart_does_not_resend(journal):
    def unknown():
        raise TimeoutError('response was lost')
    with pytest.raises(TimeoutError):
        journal.perform('e' * 64, 'a' * 64, unknown, admission())
    reloaded = module.EffectJournal(journal.path)
    with pytest.raises(module.EffectUnknown):
        reloaded.perform('e' * 64, 'a' * 64, lambda: pytest.fail('must not resend'), admission())


def test_pause_before_send_does_not_create_an_unknown_intent(journal):
    with pytest.raises(InterruptedError):
        journal.perform('e' * 64, 'a' * 64, lambda: pytest.fail('must not send'), admission(allowed=False))
    assert journal.perform('e' * 64, 'a' * 64, lambda: 'sent after resume', admission()) == 'sent after resume'


def test_cached_effect_cannot_be_reused_by_a_paused_task(journal):
    journal.perform('e' * 64, 'a' * 64, lambda: 'sent', admission())
    with pytest.raises(InterruptedError):
        journal.perform('e' * 64, 'a' * 64, lambda: pytest.fail('must not send'), admission(allowed=False))


def test_broken_admission_that_omits_intent_never_sends(journal):
    class BrokenPermit(Permit):
        def start_transport(self, record_intent):
            pass
    @contextmanager
    def broken():
        yield BrokenPermit()
    with pytest.raises(RuntimeError, match='intent'):
        journal.perform('e' * 64, 'a' * 64, lambda: pytest.fail('must not send'), broken())


def test_response_is_durable_before_inflight_admission_is_released(journal):
    state_at_release = []
    @contextmanager
    def tracked_admission():
        try:
            yield Permit()
        finally:
            row = journal._read('e' * 64)
            state_at_release.append(None if row is None else row['state'])
    assert journal.perform('e' * 64, 'a' * 64, lambda: 'remote-id', tracked_admission()) == 'remote-id'
    assert state_at_release == ['returned']


def test_concurrent_attempts_have_at_most_one_remote_send(journal):
    sending = threading.Event()
    finish = threading.Event()
    calls = []
    def send():
        calls.append(1)
        sending.set()
        assert finish.wait(5)
        return 'remote-id'
    def run():
        try:
            return journal.perform('e' * 64, 'a' * 64, send, admission())
        except module.EffectUnknown:
            return 'unknown'
    with ThreadPoolExecutor(max_workers=4) as pool:
        first = pool.submit(run)
        assert sending.wait(5)
        others = [pool.submit(run) for _ in range(3)]
        assert [future.result(timeout=5) for future in others] == ['unknown'] * 3
        finish.set()
        assert first.result(timeout=5) == 'remote-id'
    assert calls == [1]


def test_shared_journal_coordinates_separate_python_processes(journal, tmp_path):
    worker = r'''
import sys, time
from contextlib import contextmanager
from pathlib import Path
from effect_journal import EffectJournal, EffectUnknown
db, root, number = sys.argv[1:]
root = Path(root)
class Permit:
    def check_current(self): pass
    def start_transport(self, record): record()
@contextmanager
def admission(): yield Permit()
def send():
    with (root / 'sends.txt').open('a', encoding='utf8') as stream:
        stream.write('sent\n')
    time.sleep(.3)
    return {'remote': 'fixture'}
(root / ('ready' + number)).touch()
while not (root / 'go').exists(): time.sleep(.01)
try:
    EffectJournal(db).perform('e' * 64, 'a' * 64, send, admission())
except EffectUnknown:
    pass
'''
    processes = [subprocess.Popen([sys.executable, '-c', worker, str(journal.path), str(tmp_path), str(index)],
                                  cwd=str(path.parent), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                  text=True) for index in range(2)]
    try:
        deadline = time.monotonic() + 10
        while not all((tmp_path / ('ready' + str(index))).exists() for index in range(2)):
            assert time.monotonic() < deadline, 'fixture processes did not start'
            time.sleep(.01)
        (tmp_path / 'go').touch()
        for process in processes:
            out, err = process.communicate(timeout=10)
            assert process.returncode == 0, (out, err)
        assert (tmp_path / 'sends.txt').read_text(encoding='utf8').splitlines() == ['sent']
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)


def test_slow_preflight_does_not_hold_the_shared_sqlite_writer(journal):
    preflight_entered, release = threading.Event(), threading.Event()
    def preflight():
        preflight_entered.set()
        assert release.wait(5)
        return True
    with ThreadPoolExecutor(max_workers=2) as pool:
        slow = pool.submit(journal.perform, '1' * 64, 'a' * 64, lambda: 'first', admission(),
                           before_intent=preflight)
        try:
            assert preflight_entered.wait(5)
            other = pool.submit(journal.perform, '2' * 64, 'b' * 64, lambda: 'second', admission())
            assert other.result(timeout=1) == 'second'
        finally:
            release.set()
        assert slow.result(timeout=5) == 'first'
