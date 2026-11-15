"""Freeze the whole-body input before sending; do not inspect model replies."""
import copy
import importlib.util
from pathlib import Path
import sys

import pytest


HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
def load(name):
    spec = importlib.util.spec_from_file_location(name, HERE / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


frozen = load('frozen_body_plan')
contract = load('segment_plan_contract')


@pytest.fixture
def plan():
    source = {'source_sha256': 'a' * 64}
    span = {'block_id': 'b1', 'start': 0, 'end': 3}
    result = {'schema': 'preextract-input-plan/1', 'mode': 'single',
              'parts': [{'ranges': [span], 'asset_ids': [], 'capacity': {},
                         'bundle': {'source': source, 'blocks': []},
                         'body': {'model': 'fake', 'messages': []}}],
              'coverage': {'complete': True, 'block_count': 1, 'character_count': 3,
                           'ranges': [span], 'asset_ids': []}}
    return result, contract.describe_plan(result, source_sha256='a' * 64, reading_sha256='b' * 64)


def test_single_request_uses_same_frozen_plan_after_restart(plan, tmp_path):
    expected, receipt = plan
    calls = []
    def planner():
        calls.append(1)
        return copy.deepcopy(expected)
    first = frozen.single_plan(tmp_path, receipt, 'a' * 64, 'b' * 64, new_plan_factory=planner)
    (tmp_path / 'model').mkdir()
    (tmp_path / 'model/request.json').write_text('{}', encoding='utf8')
    again = frozen.single_plan(tmp_path, receipt, 'a' * 64, 'b' * 64, new_plan_factory=planner)
    assert first == again == expected and calls == [1]


def test_existing_request_without_frozen_input_never_gets_replanned(plan, tmp_path):
    expected, receipt = plan
    (tmp_path / 'model').mkdir()
    (tmp_path / 'model/request-state.json').write_text('{}', encoding='utf8')
    with pytest.raises(ValueError, match='请求证据'):
        frozen.single_plan(tmp_path, receipt, 'a' * 64, 'b' * 64,
                           new_plan_factory=lambda: copy.deepcopy(expected))


def test_changed_frozen_single_plan_is_rejected(plan, tmp_path):
    expected, receipt = plan
    frozen.single_plan(tmp_path, receipt, 'a' * 64, 'b' * 64, new_plan_factory=lambda: copy.deepcopy(expected))
    path = tmp_path / 'private/llm-frozen-single-plan.json'
    value = frozen._read(path)
    value['plan']['parts'][0]['body']['model'] = 'other'
    path.write_text(__import__('json').dumps(value), encoding='utf8')
    with pytest.raises(ValueError, match='分段计划'):
        frozen.single_plan(tmp_path, receipt, 'a' * 64, 'b' * 64, new_plan_factory=lambda: copy.deepcopy(expected))
