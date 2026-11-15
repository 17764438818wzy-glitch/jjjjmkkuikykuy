"""Exact native table fragments must merge without losing rows or trusting forged structure."""
import copy
import json
from pathlib import Path
import pytest
from test_body_routing import fixture, save, reference
from test_body_adapter import digest


def table_fixture(root, mutate=None, *, current_positions=True):
    evidence, catalog = fixture(root, parts=2)
    bundle = json.loads(Path(evidence['reading_material']['path']).read_bytes())
    text = '指标\t值\n甲\t10\n乙\t20\n丙\t30'
    block = dict(bundle['blocks'][0], type='table', text=text)
    bundle['blocks'] = [block]
    bundle['sections'] = [bundle['sections'][0]]
    cells = [dict(row=r, column=c, rowspan=1, colspan=1, text=value)
             for r, values in enumerate([['指标', '值'], ['甲', '10'], ['乙', '20'], ['丙', '30']])
             for c, value in enumerate(values)]
    asset = dict(id='A001', kind='table', block_ids=['B001'], section_ids=['S001'],
                 image_path=None, sha256=None, width=None, height=None, caption='原表', text=text,
                 structure=dict(cells=cells, grid=[[0, 1], [2, 3], [4, 5], [6, 7]],
                                header_rows=[0], units='元', notes=['原注']))
    bundle['assets'] = [asset]
    plan = json.loads(Path(evidence['plan']['path']).read_bytes())
    cut = text.index('乙')
    spans = [dict(block_id='B001', start=0, end=cut), dict(block_id='B001', start=cut, end=len(text))]
    for i, part in enumerate(plan['parts']):
        span = spans[i]
        fragment = dict(block, id=f"B001__span_{span['start']}_{span['end']}",
                        text=text[span['start']:span['end']], source_span=span, reading_role='primary',
                        table_context=dict(original_rows=[0, 1] if i == 0 else [2, 3],
                                           header='指标\t值', units='元', notes=['原注']))
        if current_positions:
            fragment['position'] = dict(fragment_start=span['start'], fragment_end=span['end'])
        local_asset = copy.deepcopy(asset)
        local_asset['block_ids'] = [fragment['id']]
        # Hand-derived expectation, independent of the production projection code.
        selected = [0, 1] if i == 0 else [0, 2, 3]
        local_asset['structure'].update(
            cells=[cells[j] for j in ([0, 1, 2, 3] if i == 0 else [0, 1, 4, 5, 6, 7])],
            grid=[[0, 1], [2, 3]] if i == 0 else [[0, 1], [2, 3], [4, 5]],
            source_row_indices=selected, source_header_rows=[0], header_rows=[0])
        subset = dict(copy.deepcopy(bundle), blocks=[fragment], assets=[local_asset])
        if mutate is not None:
            mutate(i, subset)
        request = copy.deepcopy(part['body'])
        request['messages'][1]['content'][0]['text'] = json.dumps({'material': subset}, ensure_ascii=False)
        part.update(bundle=subset, body=request, ranges=[span], asset_ids=['A001'] if i == 0 else [])
        row = evidence['responses'][i]
        row['request'] = save(Path(row['request']['path']), request)
        state = json.loads(Path(row['state']['path']).read_bytes())
        state['request_sha256'] = digest(request, sort=False)
        row['state'] = save(Path(row['state']['path']), state)
    plan['coverage'] = dict(complete=True, ranges=spans, asset_ids=['A001'], block_count=1,
                            character_count=len(text))
    evidence['material'] = save(Path(evidence['material']['path']), bundle)
    evidence['reading_material'] = save(Path(evidence['reading_material']['path']), bundle)
    evidence['plan'] = save(Path(evidence['plan']['path']), plan)
    evidence['plan_identity'] = dict(schema='paperflow-body-input-plan/1',
        source_sha256=bundle['source']['source_sha256'], reading_sha256=evidence['reading_material']['sha256'],
        mode='partitioned', part_count=2,
        parts=[dict(id=p['id'], request_sha256=digest(p['body'], False), material_sha256=digest(p['bundle']),
                    scope_sha256=digest({k:p[k] for k in ('ranges', 'asset_ids')})) for p in plan['parts']],
        coverage_sha256=digest(plan['coverage']), plan_sha256=digest(plan))
    return evidence, bundle


@pytest.mark.parametrize('current_positions', [False, True])
def test_split_table_keeps_all_original_rows_in_whole_material(tmp_path, current_positions):
    from paperflow_v3.stages.body_adapter import _inputs
    evidence, original = table_fixture(tmp_path, current_positions=current_positions)
    before = [Path(r['response']['path']).read_bytes() for r in evidence['responses']]
    _, whole, verified, _ = _inputs(evidence, evidence['identity'])
    assert whole['assets'] == original['assets']
    assert whole['assets'][0]['structure']['grid'] == [[0, 1], [2, 3], [4, 5], [6, 7]]
    assert [x['text'] for x in whole['assets'][0]['structure']['cells']][-2:] == ['丙', '30']
    assert verified[0][1]['assets'][0]['structure']['source_row_indices'] == [0, 1]
    assert verified[1][1]['assets'][0]['structure']['source_row_indices'] == [0, 2, 3]
    assert before == [Path(r['response']['path']).read_bytes() for r in evidence['responses']]


@pytest.mark.parametrize('fault', ['cell', 'grid', 'row', 'header', 'caption', 'extra', 'context', 'pixel',
                                 'position_end', 'position_start', 'position_partial', 'position_page'])
def test_changed_table_projection_is_rejected_even_with_consistent_request_seals(tmp_path, fault):
    from paperflow_v3.stages.body_adapter import _inputs
    def mutate(index, subset):
        if index != 0:
            return
        asset = subset['assets'][0]
        if fault == 'cell': asset['structure']['cells'][2]['text'] = '猜出的数字'
        elif fault == 'grid': asset['structure']['grid'][1][0] = 1
        elif fault == 'row': asset['structure']['source_row_indices'] = [0, 2]
        elif fault == 'header': asset['structure']['source_header_rows'] = []
        elif fault == 'caption': asset['caption'] = '另一张表'
        elif fault == 'extra': asset['invented'] = True
        elif fault == 'context': subset['blocks'][0]['table_context']['original_rows'] = [0, 2]
        elif fault == 'pixel': asset['sha256'] = 'f' * 64
        elif fault == 'position_end': subset['blocks'][0]['position']['fragment_end'] += 1
        elif fault == 'position_start': subset['blocks'][0]['position']['fragment_start'] = 1
        elif fault == 'position_partial': del subset['blocks'][0]['position']['fragment_start']
        else: subset['blocks'][0]['position']['page'] = 999
    evidence, _ = table_fixture(tmp_path, mutate)
    with pytest.raises(ValueError):
        _inputs(evidence, evidence['identity'])


def test_second_primary_fragment_cannot_disappear_from_table_projection(tmp_path):
    from paperflow_v3.stages.body_adapter import _verified_whole_table
    evidence, original = table_fixture(tmp_path)
    plan = json.loads(Path(evidence['plan']['path']).read_bytes())
    subset = copy.deepcopy(plan['parts'][0]['bundle'])
    second = copy.deepcopy(plan['parts'][1]['bundle']['blocks'][0])
    del second['table_context']
    subset['blocks'].append(second)
    with pytest.raises(ValueError, match='table_projection_fragment_missing'):
        _verified_whole_table(subset['assets'][0], subset, original['assets'][0],
                              {b['id']:b for b in original['blocks']})


def test_exact_whole_neighbor_context_does_not_add_projected_rows(tmp_path):
    from paperflow_v3.stages.body_adapter import _verified_whole_table
    evidence, original = table_fixture(tmp_path)
    plan = json.loads(Path(evidence['plan']['path']).read_bytes())
    subset = copy.deepcopy(plan['parts'][0]['bundle'])
    context = copy.deepcopy(original['blocks'][0])
    context.update(source_span=dict(block_id='B001', start=0, end=len(context['text'])), reading_role='context')
    subset['blocks'].append(context)
    assert _verified_whole_table(subset['assets'][0], subset, original['assets'][0],
                                 {b['id']:b for b in original['blocks']}) == original['assets'][0]
