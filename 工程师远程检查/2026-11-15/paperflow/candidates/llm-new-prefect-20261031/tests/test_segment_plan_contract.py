"""Exercise the real upstream planner with synthetic material and no model calls."""
import copy
import importlib.util
from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parents[3]
DESKTOP = Path('F:/论文预处理系统/程序/updates/unified-prefect-20261029-material-r02/desktop')
sys.path.insert(0, str(DESKTOP))
sys.path.insert(0, 'F:/论文预处理系统/程序/desktop')


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


planner = load('body_segment_plan_test', ROOT / 'prefect-paperflow/paperflow_prefect/body_segment_plan.py')
contract = load('segment_plan_contract', Path(__file__).resolve().parents[1] / 'segment_plan_contract.py')


@pytest.fixture
def make_plan(monkeypatch):
    def fake_build(bundle, template, catalog, config, **kwargs):
        text_count = sum(len(row.get('text', '')) for row in bundle.get('blocks', []))
        available = config['context_window'] - 1
        capacity = {'count_basis': 'model_tokenizer', 'text_count': text_count,
                    'text_token_upper_bound': text_count, 'output_budget': 1,
                    'context_reserve_tokens': 0, 'context_window': config['context_window'],
                    'violations': ['context'] if text_count > available else []}
        # The request body is synthetic, but its exact content and ordering are
        # bound exactly as for a real locally constructed request.
        return {'model': 'fake', 'blocks': copy.deepcopy(bundle.get('blocks', []))}, capacity

    monkeypatch.setattr(planner, 'build_request', fake_build)
    sections = [{'id': f's{i}', 'title': f'章节{i}', 'parent_id': None} for i in range(4)]
    bundle = {'source': {'source_id': 'synthetic', 'reading_scope': {}}, 'sections': sections,
              'blocks': [{'id': f'b{i}', 'type': 'paragraph', 'section_id': f's{i}',
                          'text': ('资料' + str(i)) * 8} for i in range(4)],
              'assets': [], 'issues': [], 'formulas': []}

    def make(window):
        return planner.plan_requests(bundle, 'template', {}, {'context_window': window})
    return make


def test_handoff_binds_full_planner_result_for_single_or_many_parts(make_plan):
    for window in (1000, 40):
        plan = make_plan(window)
        assert plan['mode'] == ('single' if window == 1000 else 'partitioned')
        receipt = contract.describe_plan(plan, source_sha256='a' * 64, reading_sha256='b' * 64)
        assert receipt['mode'] == plan['mode'] and receipt['part_count'] == len(plan['parts'])
        assert receipt['source_sha256'] == 'a' * 64 and receipt['reading_sha256'] == 'b' * 64
        assert '资料' not in str(receipt)  # identity metadata, not source or prompts
        contract.verify_plan(plan, receipt, source_sha256='a' * 64, reading_sha256='b' * 64)


def test_replanned_same_count_but_changed_request_is_rejected(make_plan):
    plan = make_plan(40)
    receipt = contract.describe_plan(plan, source_sha256='a' * 64, reading_sha256='b' * 64)
    changed = copy.deepcopy(plan)
    changed['parts'][0]['body']['model'] = 'another-model'
    assert len(changed['parts']) == receipt['part_count']
    with pytest.raises(ValueError, match='分段计划'):
        contract.verify_plan(changed, receipt, source_sha256='a' * 64, reading_sha256='b' * 64)
    with pytest.raises(ValueError, match='分段计划'):
        contract.verify_plan(plan, receipt, source_sha256='c' * 64, reading_sha256='b' * 64)


def test_inconsistent_coverage_is_rejected(make_plan):
    plan = make_plan(40)
    assert len(plan['parts']) > 1
    plan['parts'][0]['ranges'][0]['end'] -= 1
    with pytest.raises(ValueError, match='覆盖'):
        contract.describe_plan(plan, source_sha256='a' * 64, reading_sha256='b' * 64)
