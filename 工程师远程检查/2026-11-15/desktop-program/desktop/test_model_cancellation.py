import threading
import time
import tkinter as tk

import pytest

from app import Workbench
from agent_client import AgentClient


@pytest.fixture
def ui(tmp_path, monkeypatch):
    monkeypatch.delenv('RAGFLOW_API_KEY', raising=False)
    root = tk.Tk()
    root.withdraw()
    app = Workbench(root, tmp_path)
    app.ai_vars['base_url'].set('https://example.test/v1')
    app.ai_vars['key'].set('fixture')
    root.update()
    yield root, app
    for callback in root.tk.call('after', 'info'):
        root.after_cancel(callback)
    try:
        root.destroy()
    except tk.TclError:
        pass


def pump(root, predicate):
    deadline = time.monotonic() + 3
    while not predicate() and time.monotonic() < deadline:
        root.update()
        time.sleep(.01)
    assert predicate()


def test_change_model_releases_ui_and_late_reply_cannot_finish_new_task(ui, monkeypatch):
    root, app = ui
    entered, release, exited = threading.Event(), threading.Event(), threading.Event()
    clients = []
    def blocked(client, vision=False):
        clients.append(client)
        entered.set()
        release.wait(3)
        exited.set()
        return ['obsolete-model']
    monkeypatch.setattr(AgentClient, 'models', blocked)
    app.ai_read_button.invoke()
    try:
        pump(root, entered.is_set)
        assert app.busy
        app.ai_vars['text_model'].set('new-model')
        assert not app.busy, 'Changing model must release the old settings request immediately'
        assert clients[0].stop.is_set()
        assert app.task_started is None
        new_release = threading.Event()
        app.async_task(lambda: new_release.wait(3), lambda _: None, kind='processing')
        try:
            release.set()
            pump(root, exited.is_set)
            # Deliver the old completion while the new task is still blocked.
            time.sleep(.05)
            app.poll()
            assert app.busy and app.task_kind == 'processing'
            assert 'obsolete-model' not in app.ai_model_combos['text_model']['values']
        finally:
            new_release.set()
            pump(root, lambda: not app.busy)
    finally:
        release.set()


def test_manual_cancel_leaves_new_configuration_usable(ui, monkeypatch):
    root, app = ui
    release = threading.Event()
    entered = threading.Event()
    def blocked(client, vision=False):
        entered.set()
        release.wait(3)
        return ['late']
    monkeypatch.setattr(AgentClient, 'models', blocked)
    try:
        app.ai_read_button.invoke()
        pump(root, entered.is_set)
        app.ai_cancel_button.invoke()
        assert not app.busy
        assert '停止' in app.ai_status.get()
    finally:
        release.set()


def test_window_close_cancels_model_read_instead_of_showing_busy_warning(ui, monkeypatch):
    root, app = ui
    release = threading.Event()
    entered = threading.Event()
    def blocked(client, vision=False):
        entered.set()
        release.wait(3)
        return ['late']
    monkeypatch.setattr(AgentClient, 'models', blocked)
    try:
        app.ai_read_button.invoke()
        pump(root, entered.is_set)
        app.close()
        assert not app.busy
    finally:
        release.set()


def test_deadline_releases_ui_and_probe_has_no_document_retry_budget(ui, monkeypatch):
    import auto_ui
    root, app = ui
    monkeypatch.setattr(auto_ui, 'MODEL_REQUEST_DEADLINE_MS', 60)
    release = threading.Event()
    clients = []
    def blocked(client, vision=False):
        clients.append(client)
        release.wait(3)
        return 'late probe'
    monkeypatch.setattr(AgentClient, 'probe', blocked)
    app.ai_vars['timeout'].set('300')
    app.ai_vars['attempts'].set('5')
    try:
        app.ai_test_button.invoke()
        pump(root, lambda: not app.busy)
        assert '超时' in app.ai_status.get()
        assert clients[0].config['timeout'] <= 20
        assert clients[0].config['attempts'] == 1
        assert clients[0].stop.is_set()
    finally:
        release.set()


def test_changing_model_does_not_cancel_document_processing(ui):
    root, app = ui
    release = threading.Event()
    app.async_task(lambda: release.wait(3), lambda _: None, kind='processing')
    try:
        app.ai_vars['vision_model'].set('another-model')
        assert app.busy and app.task_kind == 'processing'
        assert not app.processor.stop.is_set()
    finally:
        release.set()
        pump(root, lambda: not app.busy)
