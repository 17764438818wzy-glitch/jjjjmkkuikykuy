import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


SOURCE = Path(__file__).resolve().parents[1] / 'retry_service.py'
spec = importlib.util.spec_from_file_location('retry_scan_candidate', SOURCE)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def snapshot(status, verified):
    return {'receiver_status': 'running', 'upstream_scan_status': status,
            'upstream_scan_verified': verified, 'waiting_llm_total': 1}


class Queue:
    def __init__(self, values):
        self.values = list(values)
        self.calls = 0

    def snapshot(self, **kwargs):
        self.calls += 1
        return self.values.pop(0) if len(self.values) > 1 else self.values[0]


def service(values, *, matches=True):
    instance = module.RetryService.__new__(module.RetryService)
    instance.queue = Queue(values)
    instance.observer = SimpleNamespace(
        _source_matches=lambda _: matches,
        _result_tasks=lambda: {'task': {}},
        _build_results=lambda _snapshot, _tasks: {
            'llm_available': True, 'partial': False, 'llm_partial': False,
            'seen_handoffs': 1, 'llm_buckets': {'failed': [{'group_id': 'folder'}]},
        },
    )
    return instance


def test_bulk_waits_for_in_progress_scan(monkeypatch):
    monkeypatch.setattr(module.time, 'sleep', lambda _: None)
    obj = service([snapshot('incomplete', False), snapshot('complete', True)])
    assert obj._failed_snapshot('all') == [{'group_id': 'folder'}]
    assert obj.queue.calls == 2


def test_bulk_still_requires_a_matching_source(monkeypatch):
    monkeypatch.setattr(module.time, 'sleep', lambda _: None)
    obj = service([snapshot('incomplete', False), snapshot('complete', True)], matches=False)
    with pytest.raises(module.RetryStopped, match='上游接收状态待核对'):
        obj._failed_snapshot('all')


def test_disconnected_receiver_is_not_treated_as_a_scan():
    value = snapshot('incomplete', False)
    value['receiver_status'] = 'interrupted'
    obj = service([value])
    with pytest.raises(module.RetryStopped, match='上游接收状态待核对'):
        obj._failed_snapshot('all')
    assert obj.queue.calls == 1


def test_permanently_incomplete_scan_has_a_bounded_wait(monkeypatch):
    moments = iter([0, 0, 15])
    monkeypatch.setattr(module.time, 'monotonic', lambda: next(moments))
    monkeypatch.setattr(module.time, 'sleep', lambda _: None)
    obj = service([snapshot('incomplete', False)])
    with pytest.raises(module.RetryStopped, match='扫描'):
        obj._failed_snapshot('all')
    assert obj.queue.calls == 2
