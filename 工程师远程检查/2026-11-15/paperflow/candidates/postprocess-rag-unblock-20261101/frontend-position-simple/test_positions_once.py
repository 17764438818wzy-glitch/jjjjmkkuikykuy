import importlib.util
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

here = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('simple_position_candidate', here / 'ticket_positions.py')
positions = importlib.util.module_from_spec(spec)
spec.loader.exec_module(positions)


def test_one_observation_uses_one_control_snapshot_and_one_prefect_read(monkeypatch):
    task_id = str(uuid4())
    groups = ['a' * 32, 'b' * 32, 'c' * 32]
    data = {(task_id, group): {'folder': {'parent_run_id': str(uuid4()), 'task_run_id': str(uuid4())},
        'job': None, 'related': [], 'downstream': None, 'expected': expected}
        for group, expected in zip(groups, ('waiting_rag_upload', 'rag_in_progress', None))}
    calls = []
    def snapshot(_projector):
        calls.append('snapshot')
        return data, True
    def read_prefect(*_args):
        calls.append('prefect')
        return {}, {}, SimpleNamespace(id='root'), SimpleNamespace(id='folder'), None
    monkeypatch.setattr(positions, '_snapshot', snapshot)
    monkeypatch.setattr(positions, '_position', lambda _p, record, *_: (record['expected'], 'passed'))
    result = positions.observe(SimpleNamespace(_prefect=read_prefect))
    assert calls == ['snapshot', 'prefect']
    assert result['counts']['waiting_rag_upload'] == 1
    assert result['counts']['rag_in_progress'] == 1
    assert result['unresolved_count'] == 1 and result['partial'] is True
