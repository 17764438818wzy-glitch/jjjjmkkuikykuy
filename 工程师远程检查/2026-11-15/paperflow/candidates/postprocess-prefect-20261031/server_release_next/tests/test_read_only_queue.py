"""A projection must not initialize a live queue or acquire work locks."""
import importlib
import importlib.util
from pathlib import Path
import sqlite3
import sys

import pytest

HERE = Path(__file__).resolve().parents[1]
INTAKE = HERE.parents[1] / 'local-intake-live-link-20261031'


def load():
    spec = importlib.util.spec_from_file_location('synthetic_observation_queue', HERE / 'read_only_queue.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_queue_does_not_call_live_constructor_or_open_writable_database(tmp_path, monkeypatch):
    path = tmp_path / 'existing.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT)')
        db.execute("INSERT INTO metadata VALUES ('queue_schema','paperflow-local-waiting/1')")
    monkeypatch.syspath_prepend(str(INTAKE))
    original = importlib.import_module('local_queue')
    monkeypatch.setattr(original.LocalQueue, '__init__', lambda *_args, **_kwargs:
                        (_ for _ in ()).throw(AssertionError('live constructor must not run')))
    def snapshot(_self, *, max_age_seconds, include_in_progress):
        assert include_in_progress is False
        return {'in_progress_count': None, 'max_age': max_age_seconds}
    monkeypatch.setattr(original.LocalQueue, 'snapshot', snapshot)
    queue = load().open_queue(path)
    assert queue.snapshot(max_age_seconds=42, include_in_progress=True)['in_progress_count'] is None
    assert queue.require_retention and queue.require_live_watcher
    with queue._connect() as db:
        assert db.execute("SELECT value FROM metadata WHERE key='queue_schema'").fetchone()[0] == 'paperflow-local-waiting/1'
        with pytest.raises(sqlite3.OperationalError):
            db.execute('CREATE TABLE must_not_be_created(x)')


def test_missing_database_is_not_created(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(INTAKE))
    path = tmp_path / 'missing.sqlite3'
    with pytest.raises((FileNotFoundError, ValueError)):
        load().open_queue(path)
    assert not path.exists()
