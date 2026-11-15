"""Control changes must finish before ownership can move to another manager."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import json
import threading

import pytest

from paperflow.control import Manager
from paperflow.store import Store
from test_control import start_worker
from test_winjob import wait_until


@contextmanager
def held_change(controller, scope, *, after_commit=False, mutate=None):
    """Delay one real SQL call at the pre-transaction or post-commit boundary."""
    entered, release = threading.Event(), threading.Event()
    original = controller.ledger._write
    caller = {}

    def write(operation, **kwargs):
        if threading.current_thread() is not caller.get('thread'):
            return original(operation, **kwargs)
        if after_commit:
            result = original(operation, **kwargs)
        entered.set()
        if not release.wait(5):
            raise TimeoutError('test scheduling barrier')
        return result if after_commit else original(operation, **kwargs)

    def change():
        caller['thread'] = threading.current_thread()
        return controller.change_admission_state(scope, 'late-freeze', {'frozen': True}, mutate=mutate)

    controller.ledger._write = write
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(change)
        try:
            assert entered.wait(3)
            yield future, release
        finally:
            release.set()
            controller.ledger._write = original


def scope_row(controller, scope):
    rows = controller.ledger.rows('SELECT * FROM admission_scopes WHERE scope=?', (scope,))
    return rows[0] if rows else None


@pytest.mark.parametrize('scope', ['service:llm', 'service:rag', 'unit:new-scope'])
def test_shutdown_retains_ownership_until_pending_scope_change_is_cancelled(tmp_path, scope):
    old = Manager(Store(tmp_path / 'ledger.sqlite3'), tmp_path / 'old').start()
    old.resume()
    new = Manager(old.store, tmp_path / 'new')
    before = scope_row(old, scope)
    try:
        with held_change(old, scope) as (future, release):
            with pytest.raises(TimeoutError):
                old.shutdown(timeout=.15)
            wait_until(lambda: old.status()['pause_persisted'])
            assert old.status()['state'] == 'pausing'
            with pytest.raises(RuntimeError, match='running'):
                new.start()
            release.set()
            with pytest.raises(ValueError):
                future.result(timeout=3)
        assert scope_row(old, scope) == before
        old.shutdown(timeout=3)
        new.start()
        new.resume()
        if scope in ('service:llm', 'service:rag'):
            unit, action = ('graph', 'llm') if scope == 'service:llm' else ('delivery', 'rag_write')
            execution, _ = start_worker(new, tmp_path, unit, 'current-worker', action)
            ticket = new.request_action(execution.execution_id, 'current-action', action, {}, 0, 30)['ticket']
            checked = new.check_begin(execution.execution_id, ticket)
            with pytest.raises(ValueError):
                old.change_admission_state(scope, 'stale-freeze', {'frozen': True})
            assert scope_row(new, scope) == before
            # The cancelled old freeze must not leave the new owner stuck or alter its snapshot.
            assert new.grant_begin(execution.execution_id, ticket, checked)['epoch'] == new.epoch
    finally:
        if new.started:
            new.shutdown()
        if old.started:
            old.shutdown()


def test_closed_manager_cannot_initialize_a_scope(tmp_path):
    controller = Manager(Store(tmp_path / 'ledger.sqlite3'), tmp_path / 'manager').start()
    controller.shutdown()
    with pytest.raises(ValueError):
        controller.change_admission_state('unit:late-new', 'late', {'frozen': True})
    assert scope_row(controller, 'unit:late-new') is None


def test_epoch_is_checked_inside_transaction_after_preflight(tmp_path, monkeypatch):
    controller = Manager(Store(tmp_path / 'ledger.sqlite3'), tmp_path / 'manager').start()
    scope = 'service:rag'
    before = scope_row(controller, scope)
    with controller.store.db() as c:
        c.execute('CREATE TABLE business_marker(value TEXT)')
    original_db = controller.store.db
    entered, release = threading.Event(), threading.Event()
    caller = {}

    @contextmanager
    def delayed_db(*args, **kwargs):
        with original_db(*args, **kwargs) as c:
            if threading.current_thread() is caller.get('thread'):
                entered.set()
                if not release.wait(5):
                    raise TimeoutError('test scheduling barrier')
            yield c

    def change():
        caller['thread'] = threading.current_thread()
        return controller.change_admission_state(scope, 'old-epoch', {'frozen': True},
            mutate=lambda c: c.execute("INSERT INTO business_marker VALUES('must not commit')"))

    monkeypatch.setattr(controller.store, 'db', delayed_db)
    try:
        with ThreadPoolExecutor(1) as pool:
            future = pool.submit(change)
            try:
                assert entered.wait(3)
                # Fault injection after the memory/preflight check, before BEGIN IMMEDIATE.
                with original_db() as c:
                    c.execute("UPDATE controller_state SET epoch='replacement' WHERE id=1")
                release.set()
                with pytest.raises(ValueError):
                    future.result(timeout=3)
                assert scope_row(controller, scope) == before
                assert controller.ledger.rows('SELECT * FROM business_marker') == []
            finally:
                release.set()
    finally:
        with original_db() as c:
            c.execute('UPDATE controller_state SET epoch=? WHERE id=1', (controller.epoch,))
        controller.shutdown()


@pytest.mark.parametrize('scope', ['service:rag', 'unit:created-in-transaction'])
def test_shutdown_during_mutate_rolls_back_scope_and_business_sql(tmp_path, scope):
    controller = Manager(Store(tmp_path / 'ledger.sqlite3'), tmp_path / 'manager').start()
    before = scope_row(controller, scope)
    with controller.store.db() as c:
        c.execute('CREATE TABLE business_marker(value TEXT)')
    entered, release = threading.Event(), threading.Event()

    def mutate(c):
        c.execute("INSERT INTO business_marker VALUES('must roll back')")
        entered.set()
        if not release.wait(5):
            raise TimeoutError('test scheduling barrier')

    try:
        with ThreadPoolExecutor(1) as pool:
            future = pool.submit(controller.change_admission_state, scope, 'freeze', {'frozen': True}, mutate=mutate)
            try:
                assert entered.wait(3)
                with pytest.raises(TimeoutError):
                    controller.shutdown(timeout=.15)
                release.set()
                with pytest.raises(ValueError):
                    future.result(timeout=3)
            finally:
                release.set()
        assert scope_row(controller, scope) == before
        assert controller.ledger.rows('SELECT * FROM business_marker') == []
        controller.shutdown(timeout=3)
    finally:
        if controller.started:
            controller.shutdown()


def test_shutdown_waits_for_post_commit_alignment_before_releasing_lock(tmp_path):
    old = Manager(Store(tmp_path / 'ledger.sqlite3'), tmp_path / 'old').start()
    new = Manager(old.store, tmp_path / 'new')
    try:
        with held_change(old, 'service:llm', after_commit=True) as (future, release):
            assert json.loads(scope_row(old, 'service:llm')['state'])['frozen'] is True
            with pytest.raises(TimeoutError):
                old.shutdown(timeout=.15)
            with pytest.raises(RuntimeError, match='running'):
                new.start()
            release.set()
            with pytest.raises(ValueError):
                future.result(timeout=3)
        old.shutdown(timeout=3)
        new.start()
        new.resume()
        execution, _ = start_worker(new, tmp_path, 'graph', 'current-worker', 'llm')
        with pytest.raises(ValueError):
            new.request_action(execution.execution_id, 'frozen', 'llm', {}, 0, 30)
    finally:
        if new.started:
            new.shutdown()
        if old.started:
            old.shutdown()


def test_failed_mutate_does_not_leak_pending_ownership(tmp_path):
    controller = Manager(Store(tmp_path / 'ledger.sqlite3'), tmp_path / 'manager').start()
    before = scope_row(controller, 'service:llm')

    def fail(c):
        raise RuntimeError('business SQL failure')

    try:
        with pytest.raises(RuntimeError, match='business SQL failure'):
            controller.change_admission_state('service:llm', 'failed', {'frozen': True}, mutate=fail)
        assert scope_row(controller, 'service:llm') == before
        controller.shutdown(timeout=3)
    finally:
        if controller.started:
            controller.shutdown()
