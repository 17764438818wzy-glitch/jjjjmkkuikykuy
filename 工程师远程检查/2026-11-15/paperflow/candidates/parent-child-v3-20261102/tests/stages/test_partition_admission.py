"""The real adapter must defer whole-paper requirements without weakening source or final validation."""
import json
from pathlib import Path
import pytest
from test_profile_completion_guard import complete_fixture, process, replace_response
from test_body_routing import reference, save


def ranking(evidence, *, codes=('080901',), units=('P001_U001', 'P002_U001')):
    ref = evidence['whole_paper_ranking']['response']
    value = {'routing_candidates': [{'code': code, 'name': '计算机科学与技术',
        'reason': '保存的整篇判断', 'unit_ids': list(units), 'source_refs': []} for code in codes], 'limitations': []}
    evidence['whole_paper_ranking']['response'] = save(Path(ref['path']), value)


@pytest.mark.parametrize('missing', ['learning_units', 'routing_candidates'])
def test_partial_segment_keeps_diagnostic_and_forms_valid_whole_profile(tmp_path, missing):
    evidence, catalog = complete_fixture(tmp_path, parts=2)
    row = evidence['responses'][1]
    raw = replace_response(row, lambda c: c['profile'].update({missing: []}))
    if missing == 'learning_units':
        ranking(evidence, units=('P001_U001',))
    result = process(evidence, catalog, tmp_path/'processed')
    profile = json.loads((Path(result['package_path'])/'profile.json').read_text(encoding='utf8'))
    report = json.loads((tmp_path/'processed/private/part-002-validation.json').read_text(encoding='utf8'))
    assert result['validation']['body_exact_reassembly'] and result['validation']['source_unchanged']
    assert len(profile['learning_units']) == (1 if missing == 'learning_units' else 2)
    assert [r['code'] for r in profile['routing_candidates']] == ['080901']
    assert report['valid'] is False and report['errors']
    assert Path(row['response']['path']).read_text(encoding='utf8') == raw
    assert result['model_requests_added'] == 0


def test_no_useful_units_in_any_segment_still_rejects_whole_profile(tmp_path):
    from paperflow_v3.stages.body_adapter import BodyValidationError
    evidence, catalog = complete_fixture(tmp_path, parts=2)
    for row in evidence['responses']:
        replace_response(row, lambda c: c['profile'].update(learning_units=[]))
    ranking(evidence, units=())
    with pytest.raises(BodyValidationError, match='assembled_candidate_validation_failed'):
        process(evidence, catalog, tmp_path/'processed')
    assert not (tmp_path/'processed/body-result.json').exists()


@pytest.mark.parametrize('codes', [(), ('999999',)])
def test_missing_or_unknown_whole_paper_route_still_rejects(tmp_path, codes):
    from paperflow_v3.stages.body_adapter import BodyValidationError
    evidence, catalog = complete_fixture(tmp_path, parts=2)
    ranking(evidence, codes=codes)
    with pytest.raises(BodyValidationError, match='assembled_candidate_validation_failed'):
        process(evidence, catalog, tmp_path/'processed')
    assert not (tmp_path/'processed/body-result.json').exists()


@pytest.mark.parametrize('fault', ['file', 'sha', 'source_id'])
def test_partition_source_and_structure_faults_never_reach_whole_assembly(tmp_path, fault):
    from paperflow_v3.stages.body_adapter import BodyValidationError
    evidence, catalog = complete_fixture(tmp_path, parts=2)
    row=evidence['responses'][1]
    def mutate(candidate):
        if fault=='file':
            candidate['source'].update(file='different-source.pdf',source_id=None,source_sha256=None)
        elif fault=='sha':candidate['source']['source_sha256']='f'*64
        else:candidate['source']['source_id']='different-source'
    raw=replace_response(row,mutate)
    with pytest.raises(BodyValidationError,match='candidate_validation_failed'):
        process(evidence,catalog,tmp_path/'processed')
    assert Path(row['response']['path']).read_text(encoding='utf8')==raw
    assert not (tmp_path/'processed/body-result.json').exists()


@pytest.mark.parametrize('code,path', [(c,'/source') for c in ['field_type','unknown_field','material_version','image_version',
    'image_evidence','image_binding','asset_identity','future_unrecognized_hard_error']]+[
    ('routing_code','/profile/routing_candidates/0/source'),('routing_code','/profile/routing_candidates/not-an-index/code'),
    ('routing_code','/profile/routing_candidates/00/code'),('learning_unusable','/different/learning_units')])
def test_adapter_never_hides_other_native_hard_diagnostics(tmp_path,monkeypatch,code,path):
    import preextract_contract
    from paperflow_v3.stages.body_adapter import BodyValidationError
    evidence,catalog=complete_fixture(tmp_path,parts=2)
    native_validate=preextract_contract.validate_candidate
    def validate(*args,**kwargs):
        report=native_validate(*args,**kwargs)
        if kwargs.get('partition_material') is not None:
            report['errors'].append({'code':code,'path':path,'message':'native hard diagnostic at the adapter boundary'})
            report['valid']=False
        return report
    monkeypatch.setattr(preextract_contract,'validate_candidate',validate)
    with pytest.raises(BodyValidationError,match='candidate_validation_failed'):
        process(evidence,catalog,tmp_path/'processed')
    assert not (tmp_path/'processed/body-result.json').exists()


@pytest.mark.parametrize('missing', ['learning_units', 'routing_candidates'])
def test_single_response_keeps_full_candidate_requirements(tmp_path, missing):
    from paperflow_v3.stages.body_adapter import BodyValidationError
    evidence,catalog=complete_fixture(tmp_path)
    replace_response(evidence['responses'][0],lambda c:c['profile'].update({missing:[]}))
    with pytest.raises(BodyValidationError,match='candidate_validation_failed'):
        process(evidence,catalog,tmp_path/'processed')
    assert not (tmp_path/'processed/body-result.json').exists()
