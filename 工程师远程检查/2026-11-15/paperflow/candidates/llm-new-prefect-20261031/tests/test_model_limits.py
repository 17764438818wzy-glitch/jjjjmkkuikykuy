"""Verify new 50/20 lease wiring with an isolated, in-memory lease provider."""
from contextlib import contextmanager, ExitStack
import importlib.util
from pathlib import Path
import threading
import time

from prefect.settings import PREFECT_API_URL, temporary_settings
import pytest


spec = importlib.util.spec_from_file_location('model_limits', Path(__file__).parent.parent / 'model_limits.py')
model_limits = importlib.util.module_from_spec(spec)
spec.loader.exec_module(model_limits)


class FakeLeaseProvider:
    def __init__(self):
        self.condition = threading.Condition()
        self.active = {}
        self.max_total = 0
        self.max_attachments = 0
    @contextmanager
    def reserve(self, names, stop, *, expected):
        with self.condition:
            while any(self.active.get(name, 0) >= expected[name] for name in names):
                assert not stop.is_set()
                self.condition.wait(0.05)
            for name in names:
                self.active[name] = self.active.get(name, 0) + 1
            self.max_total = max(self.max_total, self.active.get(model_limits.TOTAL, 0))
            self.max_attachments = max(self.max_attachments, self.active.get(model_limits.ATTACHMENT, 0))
            self.condition.notify_all()
        try:
            yield
        finally:
            with self.condition:
                for name in names:
                    self.active[name] -= 1
                self.condition.notify_all()


def test_body_and_attachment_share_50_and_attachments_stop_at_20():
    provider = FakeLeaseProvider()
    release = threading.Event()
    done = []
    failures = []
    endpoint = 'http://127.0.0.1:45001/api'
    def call(kind):
        try:
            with temporary_settings({PREFECT_API_URL: endpoint}):
                with model_limits.model_lease(kind, threading.Event(), endpoint, provider.reserve):
                    assert release.wait(10)
            done.append(kind)
        except BaseException as exc:
            failures.append(exc)
    threads = [threading.Thread(target=call, args=('attachment',)) for _ in range(21)]
    threads += [threading.Thread(target=call, args=('body',)) for _ in range(31)]
    for thread in threads:
        thread.start()
    deadline = time.monotonic() + 5
    with provider.condition:
        while provider.active.get(model_limits.TOTAL, 0) < 50 and time.monotonic() < deadline:
            provider.condition.wait(0.05)
        assert provider.active.get(model_limits.TOTAL, 0) == 50
        assert provider.active.get(model_limits.ATTACHMENT, 0) == 20
    release.set()
    for thread in threads:
        thread.join(10)
    assert not failures and len(done) == 52 and all(not t.is_alive() for t in threads)
    assert provider.max_total == 50 and provider.max_attachments == 20


def test_wrong_prefect_is_rejected_before_acquiring():
    provider = FakeLeaseProvider()
    with temporary_settings({PREFECT_API_URL: 'http://127.0.0.1:45002/api'}):
        with pytest.raises(ValueError, match='Prefect'):
            with model_limits.model_lease('body', threading.Event(), 'http://127.0.0.1:45001/api', provider.reserve):
                pass
    assert provider.active == {}


def test_existing_title_or_body_requests_share_the_total_with_new_attachments():
    provider = FakeLeaseProvider()
    endpoint = 'http://127.0.0.1:45001/api'
    entered, released = threading.Event(), threading.Event()
    errors = []
    stop = threading.Event()

    def attachment():
        try:
            with temporary_settings({PREFECT_API_URL: endpoint}):
                with model_limits.model_lease('attachment', stop, endpoint, provider.reserve):
                    entered.set()
                    assert released.wait(3)
        except BaseException as exc:
            errors.append(exc)

    with ExitStack() as stack:
        # Existing generated-title and body calls both use these same two
        # global leases on the currently selected Prefect server.
        old_names = ['paperflow-connected-total_llm', 'paperflow-connected-body_llm']
        for _ in range(49):
            stack.enter_context(provider.reserve(old_names, stop, expected={name: 50 for name in old_names}))
        last = provider.reserve(old_names, stop, expected={name: 50 for name in old_names})
        last.__enter__()
        thread = threading.Thread(target=attachment)
        thread.start()
        try:
            assert not entered.wait(0.3)
            last.__exit__(None, None, None)
            assert entered.wait(2)
        finally:
            released.set()
            thread.join(5)
    assert not errors and not thread.is_alive()
