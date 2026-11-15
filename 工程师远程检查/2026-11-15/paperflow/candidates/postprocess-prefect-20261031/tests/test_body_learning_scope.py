"""Learning-policy reading declarations use only synthetic saved responses."""
import json
from pathlib import Path

import pytest

from test_body_adapter import change_response, fixture, image_fixture, run


def test_typed_asset_alias_never_collapses_a_cross_namespace_or_duplicate_id():
    from body_adapter import _typed_reading_aliases
    candidate = {'source': {'reading_scope': {'read_refs': ['main:asset:A', 'main:asset:B'], 'unread_refs': []}}}
    bundle = {'file_ref': 'main', 'source': {}, 'blocks': [{'id': 'A'}], 'sections': [],
              'assets': [{'id': 'A'}, {'id': 'B'}, {'id': 'B'}]}
    transformed, changes = _typed_reading_aliases(candidate, bundle)
    assert transformed == candidate
    assert changes == []


@pytest.mark.parametrize('collision', ['block', 'section', 'file'])
def test_typed_asset_alias_preserves_each_source_namespace(collision):
    from body_adapter import _typed_reading_aliases
    candidate = {'source': {'reading_scope': {'read_refs': ['main:asset:main' if collision == 'file'
                                                            else 'main:asset:X'], 'unread_refs': []}}}
    ident = 'main' if collision == 'file' else 'X'
    bundle = {'file_ref': 'main', 'source': {}, 'blocks': [], 'sections': [], 'assets': [{'id': ident}]}
    if collision == 'block': bundle['blocks'] = [{'id': ident}]
    if collision == 'section': bundle['sections'] = [{'id': ident}]
    transformed, changes = _typed_reading_aliases(candidate, bundle)
    assert transformed == candidate and changes == []


def test_catalog_scope_can_be_context_but_never_research_evidence():
    from body_adapter import _source_refs
    bundle = {'file_ref': 'main', 'source': {'catalog_sha256': 'present'},
              'blocks': [{'id': 'B001'}], 'sections': [], 'assets': []}
    candidate = {'source': {'reading_scope': {'read_refs': ['catalog'], 'unread_refs': []}}, 'source_refs': []}
    catalog = {'majors': [{'code': '080901'}]}
    _source_refs(candidate, bundle, catalog=catalog)
    candidate['source_refs'] = [{'file_ref': 'catalog', 'block_id': 'B001'}]
    with pytest.raises(ValueError, match='invalid_file_ref'):
        _source_refs(candidate, bundle, catalog=catalog)
    candidate['source_refs'] = []
    with pytest.raises(ValueError, match='unknown_reading_ref'):
        _source_refs(candidate, bundle, catalog={'majors': []})


@pytest.mark.parametrize('collision', ['block', 'section', 'file'])
def test_existing_plural_alias_cannot_make_colliding_id_a_read_claim(collision):
    from body_adapter import _constrain_ambiguous_reading, _reading_limits
    ident = 'main' if collision == 'file' else 'X'
    source = {'file_ref': 'main', 'source': {}}
    bundle = {**source, 'blocks': [{'id': ident if collision == 'block' else 'B',
                                   'section_id': ident if collision == 'section' else None}],
              'sections': [{'id': ident}] if collision == 'section' else [], 'assets': [{'id': ident}]}
    report = {'candidate': {'source': {'reading_scope': {'read_refs': [ident], 'unread_refs': [], 'notes': ''}},
                            'profile': {'migration_boundary': {'global_limits': []}}},
              'warnings': [], 'compatibility_report': []}
    raw_scope = {'read_refs': ['main:assets:' + ident], 'unread_refs': []}
    _constrain_ambiguous_reading(report, bundle, raw_scope, catalog=None)
    _reading_limits(report, bundle)
    scope = report['candidate']['source']['reading_scope']
    assert ident not in scope['read_refs']
    assert raw_scope['read_refs'][0] in scope['notes']
    assert any(row['code'] == 'unconfirmed_reading_scope' for row in report['warnings'])


def _catalog():
    return {'schema': 'specialty-routing/1.0', 'majors': [dict(code='080901', name='计算机科学与技术',
        discipline_code='08', discipline='工学', group_code='0809', group='计算机类',
        paper_dataset_id='synthetic-paper', image_dataset_id='synthetic-images')]}


def test_actual_namespaced_block_and_asset_are_resolved_without_inventing_reading(tmp_path):
    evidence, _, _ = image_fixture(tmp_path)
    change_response(evidence['responses'][0], lambda c: c['source']['reading_scope'].update(
        read_refs=['main:B001', 'main:asset:A001'], unread_refs=[]))
    raw_path = Path(evidence['responses'][0]['response']['path'])
    original = raw_path.read_bytes()
    result = run(evidence, tmp_path / 'result', _catalog())
    part = json.loads((tmp_path / 'result/private/part-001-validation.json').read_text(encoding='utf-8'))
    assert result['status'] == 'body_package_verified'
    assert set(part['candidate']['source']['reading_scope']['read_refs']) == {'B001', 'A001'}
    assert any(row['code'] == 'normalized_typed_reading_alias' for row in part['compatibility_report'])
    assert raw_path.read_bytes() == original


def test_missing_reading_declaration_is_recorded_as_a_limit_not_a_read_claim(tmp_path):
    evidence, catalog = fixture(tmp_path)
    change_response(evidence['responses'][0], lambda c: c['source']['reading_scope'].update(
        read_refs=[], unread_refs=[]))
    result = run(evidence, tmp_path / 'result', catalog)
    part = json.loads((tmp_path / 'result/private/part-001-validation.json').read_text(encoding='utf-8'))
    profile = json.loads((Path(result['package_path']) / 'profile.json').read_text(encoding='utf-8'))
    assert part['candidate']['source']['reading_scope']['read_refs'] == []
    assert any(row['code'] == 'unconfirmed_reading_scope' for row in part['warnings'])
    assert any('未声明' in row and '核实' in row for row in profile['migration_boundary']['global_limits'])


def test_unknown_namespaced_asset_is_not_promoted_to_read_evidence(tmp_path):
    evidence, catalog = fixture(tmp_path)
    change_response(evidence['responses'][0], lambda c: c['source']['reading_scope'].update(
        read_refs=['main:B001', 'main:asset:NOT_SUPPLIED'], unread_refs=[]))
    raw_path = Path(evidence['responses'][0]['response']['path'])
    original = raw_path.read_bytes()
    run(evidence, tmp_path / 'result', catalog)
    part = json.loads((tmp_path / 'result/private/part-001-validation.json').read_text(encoding='utf-8'))
    scope = part['candidate']['source']['reading_scope']
    assert 'NOT_SUPPLIED' not in scope['read_refs']
    assert 'main:asset:NOT_SUPPLIED' in scope['notes']
    assert any(row['code'] == 'normalized_unlocated_reading_declaration' for row in part['compatibility_report'])
    assert raw_path.read_bytes() == original
