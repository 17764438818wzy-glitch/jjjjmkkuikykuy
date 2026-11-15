"""Build partition evidence with the actual local partitioner, without HTTP."""
import copy
import importlib.util
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKSPACE = ROOT.parents[1]
sys.path[:0] = [str(WORKSPACE / 'prefect-paperflow'),
               str(WORKSPACE / 'prefect-paperflow/paperflow_prefect/clean_overlay'),
               str(WORKSPACE.parent / '程序/updates/unified-prefect-20261029-material-r02/desktop'),
               str(WORKSPACE.parent / '程序/updates/unified-prefect-20261029-material-r02/core'),
               str(WORKSPACE.parent / '程序/desktop')]
from paperflow_prefect import body_segment_plan as segmenter
spec = importlib.util.spec_from_file_location('isolated_named_body_segment', ROOT / 'named_next/body_adapter.py')
body = importlib.util.module_from_spec(spec)
spec.loader.exec_module(body)


def make_plan(monkeypatch):
    def request(subset, *args, **kwargs):
        # Only token accounting is synthetic. The real segmenter owns ranges,
        # contexts, issue selection, formula gaps and the generated plan.
        return {'messages': [], 'part_scope': subset.get('partition_scope')}, {
            'count_basis': 'model_tokenizer', 'violations': [], 'text_count': 10,
            'context_window': 20000, 'output_budget': 10, 'context_reserve_tokens': 10,
            'text_token_upper_bound': 10}
    monkeypatch.setattr(segmenter, 'build_request', request)
    source = {'source_id': 'source', 'file': 'synthetic.docx', 'source_sha256': 'a'*64,
              'reading_scope': {'extraction_issues': ['issue-one', 'issue-two']}}
    blocks = [dict(id='a', section_id='s1', text='$x$ $x$'+'a'*801, type='paragraph', position={'part': 1}),
              dict(id='b', section_id='s2', text='b'*801, type='paragraph', position={'part': 2})]
    reading = {'source': source, 'blocks': blocks, 'assets': [],
               'sections': [dict(id='s1', title='One', parent_id=None, level=1),
                            dict(id='s2', title='Two', parent_id=None, level=1)],
               'issues': [dict(message='issue-one', position={'part': 1}),
                          dict(message='issue-two', position={'part': 2})],
               'formulas': [dict(position={'part': 1}, latex='x')]}
    plan = segmenter.plan_requests(reading, {}, {}, {}, request_context={'context_overflow_confirmed': True})
    assert plan['mode'] == 'partitioned' and len(plan['parts']) == 2
    assert plan['partition_gaps'] and plan['partition_gaps'][0]['code'] == 'formula_position_unresolved'
    return reading, plan


def receipt(plan):
    return {'schema': 'paperflow-body-input-plan/1', 'source_sha256': 'a'*64, 'reading_sha256': 'b'*64,
            'mode': plan['mode'], 'part_count': len(plan['parts']),
            'parts': [dict(id=f'part-{n:03d}', request_sha256=body._digest(p['body'], sort=False),
                           material_sha256=body._digest(p['bundle']),
                           scope_sha256=body._digest({k:p[k] for k in ('ranges', 'asset_ids')}))
                      for n,p in enumerate(plan['parts'],1)],
            'coverage_sha256': body._digest(plan['coverage']), 'plan_sha256': body._digest(plan)}


def validate(reading, plan):
    body._plan(plan, receipt(plan), 'a'*64, 'b'*64, reading)


def test_real_partitioner_local_issues_and_formula_gap_are_allowed(monkeypatch):
    reading, plan = make_plan(monkeypatch)
    assert plan['parts'][0]['bundle']['source'] != reading['source']
    validate(reading, plan)


@pytest.mark.parametrize('change', ['other_source', 'invented_issue', 'omitted_issue', 'invented_gap'])
def test_only_exact_segmenter_issue_projection_is_accepted(monkeypatch, change):
    reading, plan = make_plan(monkeypatch)
    part = plan['parts'][0]['bundle']
    if change == 'other_source': part['source']['source_id'] = 'other'
    elif change == 'invented_issue':
        part['issues'].append({'message': 'made-up', 'position': {'part': 1}})
        part['source']['reading_scope']['extraction_issues'].append('made-up')
    elif change == 'omitted_issue':
        part['issues'].pop(0)
        part['source']['reading_scope']['extraction_issues'] = list(dict.fromkeys(i['message'] for i in part['issues']))
    else:
        plan['partition_gaps'].append({'code': 'invented', 'position': {'part': 1}, 'message': 'made-up'})
        part['issues'].append(copy.deepcopy(plan['partition_gaps'][-1]))
        part['source']['reading_scope']['extraction_issues'].append('made-up')
    with pytest.raises(ValueError): validate(reading, plan)


def test_unrelated_long_context_cannot_admit_unowned_issue(monkeypatch):
    reading, plan = make_plan(monkeypatch)
    part = plan['parts'][0]['bundle']
    extra = copy.deepcopy(reading['blocks'][1])
    extra.update(source_span={'block_id': extra['id'], 'start': 0, 'end': len(extra['text'])},
                 reading_role='context')
    part['blocks'].append(extra)
    part['issues'].insert(1, copy.deepcopy(reading['issues'][1]))
    part['source']['reading_scope']['extraction_issues'] = list(dict.fromkeys(i['message'] for i in part['issues']))
    with pytest.raises(ValueError): validate(reading, plan)
