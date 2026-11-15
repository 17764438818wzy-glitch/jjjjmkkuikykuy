"""Read-only control change detection on temporary SQLite databases."""
import importlib.util
from pathlib import Path
import sqlite3

import pytest

HERE = Path(__file__).resolve().parents[1]


def load():
    path = HERE / 'revision_witness.py'
    assert path.is_file(), 'the read-only control revision witness is missing'
    spec = importlib.util.spec_from_file_location('synthetic_revision_witness', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_control_database_write_invalidates_the_existing_witness(tmp_path):
    path = tmp_path / 'control.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE task_deletions(task_id TEXT)')
    witness = load().ReadOnlyRevision(path)
    try:
        first = witness.current()
        with sqlite3.connect(path) as db:
            db.execute("INSERT INTO task_deletions VALUES('synthetic')")
        assert witness.current() != first
        assert witness.current() == witness.current()
    finally:
        witness.close()


def test_closed_witness_cannot_authorize_cached_observations(tmp_path):
    path = tmp_path / 'control.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE records(id TEXT)')
    witness = load().ReadOnlyRevision(path)
    try:
        witness.close()
        with pytest.raises(ValueError): witness.current()
    finally:
        witness.close()
