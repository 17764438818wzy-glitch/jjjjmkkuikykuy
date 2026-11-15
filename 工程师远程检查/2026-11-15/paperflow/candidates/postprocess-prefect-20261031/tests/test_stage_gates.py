"""Queue barriers: only temporary local SQLite, no real workers or network."""
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import stage_gates


@pytest.fixture
def gates(tmp_path):
    path = tmp_path / 'control.sqlite3'
    sqlite3.connect(path).close()
    return stage_gates.QueueBarriers(path)


def test_missing_control_is_closed_for_both_sides(gates):
    assert gates.rag_is_held() is False
    assert gates.postprocess_is_open() is False
    with pytest.raises(ValueError): gates.open_postprocessing()


def test_rag_is_held_before_postprocessing_can_open(gates):
    gates.initialize()
    assert gates.rag_is_held() is True
    assert gates.postprocess_is_open() is False
    gates.open_postprocessing()
    assert gates.rag_is_held() is True
    assert gates.postprocess_is_open() is True
    with pytest.raises(PermissionError): gates.require_rag_upload_allowed()


def test_open_fails_closed_if_rag_is_missing_or_corrupted(gates):
    gates.initialize()
    with sqlite3.connect(gates.database) as db:
        db.execute("DELETE FROM paperflow_queue_barriers WHERE name='rag_upload'")
    with pytest.raises(ValueError): gates.open_postprocessing()
    assert gates.rag_is_held() is False
    assert gates.postprocess_is_open() is False
    with pytest.raises(PermissionError): gates.require_rag_upload_allowed()


def test_initialized_closed_gate_is_not_silently_reset_on_second_initialize(gates):
    gates.initialize()
    gates.open_postprocessing()
    gates.initialize()
    assert gates.postprocess_is_open() is True
    assert gates.rag_is_held() is True


def test_concurrent_open_has_one_durable_barrier(gates):
    gates.initialize()
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda _: gates.open_postprocessing(), range(16)))
    assert gates.postprocess_is_open() is True and gates.rag_is_held() is True
    with sqlite3.connect(gates.database) as db:
        rows = db.execute('SELECT name,blocked FROM paperflow_queue_barriers ORDER BY name').fetchall()
    assert rows == [('postprocessing', 0), ('rag_upload', 1)]


def test_manual_rehold_stops_new_admissions(gates):
    gates.initialize(); gates.open_postprocessing()
    gates.hold_postprocessing()
    assert gates.postprocess_is_open() is False and gates.rag_is_held() is True
