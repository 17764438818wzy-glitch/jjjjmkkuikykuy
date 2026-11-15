"""A cell's LF is data; only an extra source LF is a table delimiter.

The first case catches the production bug that rejects the native whole span
197 as if it were 198. Fixtures are portable original table/helper objects.
"""
import copy
import json
from pathlib import Path
import pytest


def real_case():
    return json.loads((Path(__file__).parent / 'fixtures/13c180cd-real-table.json').read_bytes())


def verify(case):
    from paperflow_v3.stages.body_adapter import _verified_whole_table
    return _verified_whole_table(case['asset'], case['subset'], case['original'], case['blocks'])


def native_fragment(case, *, extra_separator=False, current=True):
    """Use a real existing planner to obtain the alternative complete fragment."""
    from preextract_partition import _fragment as legacy_fragment
    from paperflow_prefect.body_segment_plan import _fragment as current_fragment
    row = case['subset']['blocks'][0]
    block = case['blocks'][row['source_span']['block_id']]
    if extra_separator:
        block['text'] += '\n'
    span = dict(row['source_span'], end=len(block['text']))
    fragment = (current_fragment if current else legacy_fragment)(block, span, 'primary')
    fragment['table_context'] = copy.deepcopy(row['table_context'])
    case['subset']['blocks'] = [fragment]
    case['asset']['block_ids'] = [fragment['id']]
    return case


def test_last_cell_LF_keeps_real_whole_span_197():
    case = real_case()
    assert case['original']['structure']['cells'][-1]['text'].endswith('\n')
    assert case['subset']['blocks'][0]['source_span']['end'] == 197
    assert len(next(iter(case['blocks'].values()))['text']) == 197
    assert verify(case) == case['original']


@pytest.mark.parametrize('current', [False, True])
@pytest.mark.parametrize('extra_separator', [False, True])
def test_cell_LF_and_actual_extra_source_separator_both_preserve_all_cells(current, extra_separator):
    case = native_fragment(real_case(), extra_separator=extra_separator, current=current)
    assert case['subset']['blocks'][0]['source_span']['end'] == (198 if extra_separator else 197)
    before = copy.deepcopy(case)
    assert verify(case) == before['original']
    assert case == before


@pytest.mark.parametrize('fault, expected', [
    ('span_start', 'table_projection_row_boundary'),
    ('span_end', 'table_projection_row_boundary'),
    ('source', 'table_projection_source_invalid'),
    ('table_cell', 'table_projection_mismatch'),
    ('table_grid', 'table_projection_mismatch'),
    ('context', 'table_projection_fragment_mismatch'),
    ('missing_context', 'table_projection_fragment_missing'),
    ('position', 'table_projection_fragment_mismatch'),
])
def test_real_table_guards_still_reject_changed_span_source_table_and_context(fault, expected):
    case = real_case()
    row = case['subset']['blocks'][0]
    if fault == 'span_start':
        row['source_span']['start'] = 1
    elif fault == 'span_end':
        row['source_span']['end'] = 196
    elif fault == 'source':
        case['blocks'][row['source_span']['block_id']]['text'] += '外部数据'
    elif fault == 'table_cell':
        case['asset']['structure']['cells'][-1]['text'] = '未经来源绑定的替换'
    elif fault == 'table_grid':
        case['asset']['structure']['grid'][-1][1] = 0
    elif fault == 'context':
        row['table_context']['original_rows'] = [0, 1, 2, 3]
    elif fault == 'missing_context':
        del row['table_context']
    else:
        row['position']['invented'] = 1
    with pytest.raises(ValueError, match='^' + expected + '$'):
        verify(case)
