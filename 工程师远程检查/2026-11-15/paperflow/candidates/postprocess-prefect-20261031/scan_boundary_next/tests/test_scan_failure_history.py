"""Synthetic proof of what the current receiver metadata does not retain."""

import importlib.util
from pathlib import Path
import sqlite3
from uuid import UUID


def test_failed_scan_then_success_has_same_final_metadata_as_success(tmp_path, monkeypatch):
    workspace = Path(__file__).resolve().parents[4]
    receiver_dir = workspace / 'candidates' / 'local-intake-live-link-20261031'
    monkeypatch.syspath_prepend(str(receiver_dir))
    spec = importlib.util.spec_from_file_location('isolated_receiver_history', receiver_dir / 'local_queue.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    current_time = ['2026-11-01T00:00:00+00:00']
    monkeypatch.setattr(module, '_utc_now', lambda: current_time[0])
    monkeypatch.setattr(module.uuid, 'uuid4', lambda: UUID('11111111-1111-4111-8111-111111111111'))
    source = '22222222-2222-4222-8222-222222222222'

    def history(name, failed):
        current_time[0] = '2026-11-01T00:00:00+00:00'
        queue = module.LocalQueue(tmp_path / name / 'queue.sqlite3', require_retention=True,
                                  require_live_watcher=True)
        queue.start_receiver()
        queue.record_scan(complete=True, source_instance=source)
        current_time[0] = '2026-11-01T00:00:01+00:00'
        queue.record_scan(complete=False, reason='扫描进行中', source_instance=source)
        if failed:
            current_time[0] = '2026-11-01T00:00:02+00:00'
            queue.record_scan(complete=False, reason='OSError')
        current_time[0] = '2026-11-01T00:00:03+00:00'
        queue.record_scan(complete=False, reason='扫描进行中', source_instance=source)
        current_time[0] = '2026-11-01T00:00:04+00:00'
        queue.record_scan(complete=True, source_instance=source)
        with sqlite3.connect(queue.path) as db:
            return dict(db.execute('SELECT key,value FROM metadata'))

    assert history('only-success', False) == history('failure-then-success', True)
