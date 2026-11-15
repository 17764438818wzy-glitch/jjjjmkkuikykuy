from contextlib import nullcontext
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from paperflow_prefect.llm_queue import raw_partition as module


def test_manually_retried_part_uses_existing_plan(monkeypatch, tmp_path):
    model = tmp_path / 'model'
    model.mkdir()
    (model / 'partition-state.json').write_text('{}', encoding='utf8')
    rows = [{'id': 'part-001', 'request_sha256': 'a' * 64},
            {'id': 'part-002', 'request_sha256': 'b' * 64}]
    state = {'requests': rows}
    plan = {'mode': 'partitioned', 'parts': [
        {'id': 'part-001', 'body': {'part': 1}},
        {'id': 'part-002', 'body': {'part': 2}},
    ]}
    monkeypatch.setattr(module, 'work_locks', lambda *_: nullcontext())
    monkeypatch.setattr(module, 'verify_plan', lambda *_, **__: None)
    monkeypatch.setattr(module, '_frozen_state', lambda *_, **__: (state, {}))
    monkeypatch.setattr(module, '_update_usage', lambda *_, **__: None)
    sent = []
    def require_completed(_model, row):
        if row['id'] == 'part-002' and 'part-002' not in sent:
            raise ValueError('old failed part')
    monkeypatch.setattr(module, '_require_completed', require_completed)
    def perform(**kwargs):
        ident = kwargs['model'].name
        sent.append(ident)
        return {'response_sha256': 'c' * 64}
    monkeypatch.setattr(module, '_perform_one', perform)
    class Stop:
        def is_set(self): return False
    options = dict(plan_factory=lambda: (plan, {}, {}), plan_receipt={},
                   source_sha256='s', reading_sha256='r', template={}, connection={},
                   work=tmp_path, stop=Stop(), request_once=lambda: None,
                   lease=lambda: None, admit=lambda *args: None, accept=lambda *args: None,
                   check_history=lambda: None, check_source=lambda: None,
                   check_part=lambda _: None, create_plan=lambda: None)
    with pytest.raises(ValueError, match='old failed part'):
        module.run_partition_raw(**options)
    result = module.run_partition_raw(**options, retry_missing=lambda ident: ident == 'part-002' and ident not in sent)
    assert result['status'] == 'body_parts_raw_saved'
    assert sent == ['part-001', 'part-002']
