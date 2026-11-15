"""Use the existing postprocessor with synthetic saved model replies."""
import copy
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKSPACE = ROOT.parents[1]
sys.path[:0] = [str(ROOT), str(WORKSPACE / 'candidates/postprocess-prefect-20261031/tests')]
from test_body_adapter import fixture, reference, save, FixtureCounter, RELEASE


@pytest.mark.parametrize('count', [1, 2, 3, 4])
def test_ranked_specialties_keep_up_to_three_and_retain_original_order(tmp_path, monkeypatch, count):
    import socket
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('network forbidden'))
    try:
        from paperflow_v3.stages.body_adapter import process_body
    except ModuleNotFoundError:
        pytest.fail('The candidate body postprocessor is missing')

    evidence, catalog = fixture(tmp_path)
    row = evidence['responses'][0]
    response_path = Path(row['response']['path'])
    candidate = json.loads(response_path.read_text(encoding='utf8'))
    first = candidate['profile']['routing_candidates'][0]
    codes = ['999901', '100001', '300001', '200001'][:count]
    ranked = [dict(copy.deepcopy(first), code=code, name='专业' + code, reason=f'真实顺序{i}')
              for i, code in enumerate(codes, 1)]
    candidate['profile']['routing_candidates'] = ranked
    candidate['profile']['additional_related_specialties'] = []
    catalog['majors'] = [dict(copy.deepcopy(catalog['majors'][0]), code=code, name='专业' + code)
                         for code in codes]
    raw = json.dumps(candidate, ensure_ascii=False)
    response_path.write_text(raw, encoding='utf8')
    row['response'] = reference(response_path)
    state_path = Path(row['state']['path'])
    state = json.loads(state_path.read_text(encoding='utf8'))
    state['response_sha256'] = row['response']['sha256']
    row['state'] = save(state_path, state)
    row['events'] = save(Path(row['events']['path']),
        {'choices': [{'message': {'content': raw}, 'finish_reason': 'stop'}]})

    output = tmp_path / 'processed'
    result = process_body(evidence, output, expected_identity=evidence['identity'],
                          catalog=catalog, paperflow_root=RELEASE / 'core', counter=FixtureCounter())
    profile = json.loads((Path(result['package_path']) / 'profile.json').read_text(encoding='utf8'))
    assert [route['code'] for route in profile['routing_candidates']] == codes[:3]
    assert result['assembly']['routing_candidates_original'] == ranked
    assert response_path.read_text(encoding='utf8') == raw
    assert process_body(evidence, output, expected_identity=evidence['identity'], catalog=catalog,
                        paperflow_root=RELEASE / 'core', counter=FixtureCounter()) == result


def test_misplaced_source_profile_keeps_full_original_ranking(tmp_path, monkeypatch):
    import socket
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('network forbidden'))
    from paperflow_v3.stages.body_adapter import process_body

    evidence, catalog = fixture(tmp_path)
    row = evidence['responses'][0]
    response_path = Path(row['response']['path'])
    candidate = json.loads(response_path.read_text(encoding='utf8'))
    profile = candidate.pop('profile')
    first = profile['routing_candidates'][0]
    codes = ['999901', '100001', '300001', '200001']
    ranked = [dict(copy.deepcopy(first), code=code, name='专业' + code, reason=f'真实顺序{i}')
              for i, code in enumerate(codes, 1)]
    profile['routing_candidates'] = ranked
    profile['additional_related_specialties'] = []
    candidate['source']['profile'] = profile
    catalog['majors'] = [dict(copy.deepcopy(catalog['majors'][0]), code=code, name='专业' + code)
                         for code in codes]
    raw = json.dumps(candidate, ensure_ascii=False)
    response_path.write_text(raw, encoding='utf8')
    row['response'] = reference(response_path)
    state_path = Path(row['state']['path'])
    state = json.loads(state_path.read_text(encoding='utf8'))
    state['response_sha256'] = row['response']['sha256']
    row['state'] = save(state_path, state)
    row['events'] = save(Path(row['events']['path']),
        {'choices': [{'message': {'content': raw}, 'finish_reason': 'stop'}]})

    result = process_body(evidence, tmp_path / 'processed', expected_identity=evidence['identity'],
                          catalog=catalog, paperflow_root=RELEASE / 'core', counter=FixtureCounter())
    published = json.loads((Path(result['package_path']) / 'profile.json').read_text(encoding='utf8'))
    assert [route['code'] for route in published['routing_candidates']] == codes[:3]
    assert result['assembly']['routing_candidates_original'] == ranked
    assert response_path.read_text(encoding='utf8') == raw


def test_partitioned_body_uses_whole_paper_ranking_instead_of_local_lists(tmp_path):
    from paperflow_v3.stages.body_adapter import process_body
    evidence, catalog = fixture(tmp_path, parts=2)
    codes = ['999901', '100001', '300001', '200001']
    catalog['majors'] = [dict(copy.deepcopy(catalog['majors'][0]), code=code, name='专业' + code)
                         for code in codes]
    for index, row in enumerate(evidence['responses']):
        path = Path(row['response']['path'])
        candidate = json.loads(path.read_text(encoding='utf8'))
        route = candidate['profile']['routing_candidates'][0]
        route.update(code=codes[index], name='专业' + codes[index])
        raw = json.dumps(candidate, ensure_ascii=False)
        path.write_text(raw, encoding='utf8')
        row['response'] = reference(path)
        state_path = Path(row['state']['path'])
        state = json.loads(state_path.read_text(encoding='utf8'))
        state['response_sha256'] = row['response']['sha256']
        row['state'] = save(state_path, state)
        row['events'] = save(Path(row['events']['path']),
            {'choices': [{'message': {'content': raw}, 'finish_reason': 'stop'}]})
    ranking = [{'code': code, 'name': '专业' + code, 'reason': '综合全文',
                'unit_ids': ['P001_U001', 'P002_U001'], 'source_refs': []} for code in reversed(codes)]
    rank_path = tmp_path / 'whole-ranking/response.txt'
    rank_path.parent.mkdir()
    rank_path.write_text(json.dumps({'routing_candidates': ranking, 'limitations': []},
                                    ensure_ascii=False), encoding='utf8')
    evidence['whole_paper_ranking'] = {'response': reference(rank_path),
                                      'coverage': ['part-001', 'part-002']}
    result = process_body(evidence, tmp_path / 'processed', expected_identity=evidence['identity'],
                          catalog=catalog, paperflow_root=RELEASE / 'core', counter=FixtureCounter())
    profile = json.loads((Path(result['package_path']) / 'profile.json').read_text(encoding='utf8'))
    assert [row['code'] for row in profile['routing_candidates']] == list(reversed(codes))[:3]
    assert all(row['unit_ids'] == ['P001_U001', 'P002_U001'] for row in profile['routing_candidates'])
    assert result['assembly']['routing_candidates_original'] == ranking


def test_ambiguous_local_unit_id_in_global_ranking_is_not_guessed(tmp_path):
    from paperflow_v3.stages.body_adapter import process_body
    evidence, catalog = fixture(tmp_path, parts=2)
    ranking = {'routing_candidates': [{'code': '080901', 'name': '计算机科学与技术',
        'reason': '全文依据', 'unit_ids': ['U001'], 'source_refs': []}], 'limitations': []}
    path = tmp_path / 'whole-ranking/response.txt'
    path.parent.mkdir()
    raw = json.dumps(ranking, ensure_ascii=False)
    path.write_text(raw, encoding='utf8')
    evidence['whole_paper_ranking'] = {'response': reference(path), 'coverage': ['part-001', 'part-002']}
    result = process_body(evidence, tmp_path / 'processed', expected_identity=evidence['identity'],
                          catalog=catalog, paperflow_root=RELEASE / 'core', counter=FixtureCounter())
    profile = json.loads((Path(result['package_path']) / 'profile.json').read_text(encoding='utf8'))
    assert result['admission_policy'] == 'learning'
    assert profile['routing_candidates'][0]['unit_ids'] == ['U001']
    assert any('提取记录有局部格式' in gap.get('use_limit', '') for gap in profile['gaps'])
    assert path.read_text(encoding='utf8') == raw
