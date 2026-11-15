import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

import pytest
from PIL import Image

root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(root / 'candidates/postprocess-prefect-20261031/tests'))
from test_body_adapter import fixture, change_response, FixtureCounter, RELEASE
from preextract_image_alias import AliasContext
sys.path.insert(0, str(root / 'candidates/postprocess-rag-unblock-20261101/body-normalization'))

spec = importlib.util.spec_from_file_location('body_candidate', Path(__file__).with_name('body_adapter.py'))
body = importlib.util.module_from_spec(spec)
spec.loader.exec_module(body)


def test_one_unlocated_learning_reference_is_reported_without_discarding_other_content(tmp_path):
    evidence, catalog = fixture(tmp_path, parts=2)
    bundle = json.loads(Path(evidence['reading_material']['path']).read_text(encoding='utf8'))
    candidate = json.loads(Path(evidence['responses'][0]['response']['path']).read_text(encoding='utf8'))
    extra = copy.deepcopy(candidate['profile']['learning_units'][0])
    extra['id'] = 'U002'
    extra['source_refs'][0]['block_id'] = 'INVENTED'
    candidate['profile']['learning_units'].append(extra)
    raw = json.dumps(candidate, ensure_ascii=False)
    artifacts, _, _ = body._derive({'mode':'single'}, bundle, [(raw, bundle)], AliasContext(), catalog)
    profile = artifacts['candidate']['profile']
    assert [unit['id'] for unit in profile['learning_units']] == ['U001', 'U002']
    assert profile['learning_units'][0]['source_refs'][0]['block_id'] == 'B001'
    assert profile['learning_units'][1]['source_refs'] == []
    assert any('无法定位' in text for text in profile['migration_boundary']['global_limits'])
    assert any(row['code'] == 'unlocated_source_ref' for row in artifacts['part_1_validation']['warnings'])
    assert 'INVENTED' in raw


def test_partial_reference_gap_produces_a_package_with_its_limit_and_original_answer(tmp_path):
    evidence, catalog = fixture(tmp_path)
    def add_gap(candidate):
        extra = copy.deepcopy(candidate['profile']['learning_units'][0])
        extra['id'] = 'U002'
        extra['source_refs'][0]['block_id'] = 'INVENTED'
        candidate['profile']['learning_units'].append(extra)
    change_response(evidence['responses'][0], add_gap)
    original = Path(evidence['responses'][0]['response']['path']).read_bytes()
    result = body.process_body(evidence, tmp_path / 'result', expected_identity=evidence['identity'],
        catalog=catalog, paperflow_root=RELEASE / 'core', counter=FixtureCounter())
    profile = json.loads((Path(result['package_path']) / 'profile.json').read_text(encoding='utf8'))
    assert result['model_requests_added'] == 0
    assert len(profile['learning_units']) == 2
    assert profile['learning_units'][1]['source_refs'] == []
    assert any('无法定位' in text for text in profile['migration_boundary']['global_limits'])
    assert Path(evidence['responses'][0]['response']['path']).read_bytes() == original
    diagnostic = json.loads((tmp_path / 'result/private/part-001-validation.json').read_text(encoding='utf8'))
    assert any(row['code'] == 'unlocated_source_ref' for row in diagnostic['compatibility_report'])
    chunks = json.loads((Path(result['package_path']) / 'chunks.json').read_text(encoding='utf8'))['chunks']
    unit_chunks = [row for row in chunks if row['kind'] == 'learning' and 'U002' in row['unit_ids']]
    assert unit_chunks and all('无法定位' in row['content'] for row in unit_chunks)


def test_unverified_unit_keeps_its_source_limit_even_when_a_step_is_missing(tmp_path):
    evidence, catalog = fixture(tmp_path)
    bundle = json.loads(Path(evidence['reading_material']['path']).read_text(encoding='utf8'))
    candidate = json.loads(Path(evidence['responses'][0]['response']['path']).read_text(encoding='utf8'))
    extra = copy.deepcopy(candidate['profile']['learning_units'][0])
    extra['id'], extra['steps'] = 'U002', []
    extra['source_refs'][0]['file_ref'] = 'unknown-file'
    candidate['profile']['learning_units'].append(extra)
    artifacts, _, _ = body._derive({'mode': 'single'}, bundle,
        [(json.dumps(candidate, ensure_ascii=False), bundle)], AliasContext(), catalog)
    assert any(gap['target_pointer'] == '/profile/learning_units/1' and '来源未能定位' in gap['use_limit']
               for gap in artifacts['candidate']['gaps'])


def test_implicit_main_file_reference_with_real_block_is_kept(tmp_path):
    evidence, catalog = fixture(tmp_path)
    bundle = json.loads(Path(evidence['reading_material']['path']).read_text(encoding='utf8'))
    candidate = json.loads(Path(evidence['responses'][0]['response']['path']).read_text(encoding='utf8'))
    candidate['profile']['learning_units'][0]['source_refs'][0]['file_ref'] = None
    artifacts, _, _ = body._derive({'mode': 'single'}, bundle,
        [(json.dumps(candidate, ensure_ascii=False), bundle)], AliasContext(), catalog)
    assert artifacts['candidate']['profile']['learning_units'][0]['source_refs'][0]['block_id'] == 'B001'


def test_unknown_file_in_one_learning_reference_is_a_local_gap(tmp_path):
    evidence, catalog = fixture(tmp_path)
    bundle = json.loads(Path(evidence['reading_material']['path']).read_text(encoding='utf8'))
    candidate = json.loads(Path(evidence['responses'][0]['response']['path']).read_text(encoding='utf8'))
    extra = copy.deepcopy(candidate['profile']['learning_units'][0])
    extra['id'] = 'U002'
    extra['source_refs'][0]['file_ref'] = 'unknown-file'
    candidate['profile']['learning_units'].append(extra)
    artifacts, _, _ = body._derive({'mode': 'single'}, bundle,
        [(json.dumps(candidate, ensure_ascii=False), bundle)], AliasContext(), catalog)
    units = artifacts['candidate']['profile']['learning_units']
    assert [unit['id'] for unit in units] == ['U001', 'U002']
    assert units[0]['source_refs'] and not units[1]['source_refs']
    assert any('无法定位' in gap['use_limit'] for gap in artifacts['candidate']['gaps'])


def test_real_attachment_reference_is_not_treated_as_unknown(tmp_path):
    evidence, _ = fixture(tmp_path)
    bundle = json.loads(Path(evidence['reading_material']['path']).read_text(encoding='utf8'))
    bundle['attachments'] = [{'file_ref': 'attachment-1'}]
    candidate = json.loads(Path(evidence['responses'][0]['response']['path']).read_text(encoding='utf8'))
    candidate['profile']['learning_units'][0]['source_refs'].append({'file_ref': 'attachment-1'})
    body._source_refs(candidate, bundle, report={'compatibility_report': [], 'warnings': []})
    assert candidate['profile']['learning_units'][0]['source_refs'][-1]['file_ref'] == 'attachment-1'


def test_a_single_unverified_unit_is_not_presented_as_traceable(tmp_path):
    evidence, catalog = fixture(tmp_path)
    change_response(evidence['responses'][0], lambda candidate:
        candidate['profile']['learning_units'][0]['source_refs'][0].update(file_ref='unknown-file'))
    result = body.process_body(evidence, tmp_path / 'result', expected_identity=evidence['identity'],
        catalog=catalog, paperflow_root=RELEASE / 'core', counter=FixtureCounter())
    package = Path(result['package_path'])
    profile = json.loads((package / 'profile.json').read_text(encoding='utf8'))
    chunks = json.loads((package / 'chunks.json').read_text(encoding='utf8'))['chunks']
    assert not profile['learning_units'][0]['source_refs']
    assert any('来源未能定位' in gap['use_limit'] for gap in profile['gaps'])
    assert any('来源未能定位' in row['content'] for row in chunks if row['kind'] == 'learning')
    assert result['model_requests_added'] == 0


def test_single_unit_with_missing_steps_and_source_is_preserved_with_limit(tmp_path):
    evidence, catalog = fixture(tmp_path)
    def remove_details(candidate):
        unit = candidate['profile']['learning_units'][0]
        unit['steps'] = []
        unit['source_refs'][0]['file_ref'] = 'unknown-file'
    change_response(evidence['responses'][0], remove_details)
    result = body.process_body(evidence, tmp_path / 'result', expected_identity=evidence['identity'],
        catalog=catalog, paperflow_root=RELEASE / 'core', counter=FixtureCounter())
    profile = json.loads((Path(result['package_path']) / 'profile.json').read_text(encoding='utf8'))
    assert not profile['learning_units'][0]['steps'] and not profile['learning_units'][0]['source_refs']
    assert any('内容有缺项' in gap['use_limit'] for gap in profile['gaps'])
    assert any('来源未能定位' in gap['use_limit'] for gap in profile['gaps'])
    assert result['model_requests_added'] == 0


def test_incorrect_model_filename_keeps_the_registered_source_and_a_warning(tmp_path):
    evidence, catalog = fixture(tmp_path)
    bundle = json.loads(Path(evidence['reading_material']['path']).read_text(encoding='utf8'))
    candidate = json.loads(Path(evidence['responses'][0]['response']['path']).read_text(encoding='utf8'))
    candidate['source']['file'] = 'incorrect-model-filename.pdf'
    artifacts, _, _ = body._derive({'mode': 'single'}, bundle,
        [(json.dumps(candidate, ensure_ascii=False), bundle)], AliasContext(), catalog)
    report = artifacts['part_1_validation']
    assert artifacts['candidate']['source']['file'] == bundle['source']['file']
    assert any(row['code'] == 'unconfirmed_model_filename' and row['original'] == 'incorrect-model-filename.pdf'
               for row in report['compatibility_report'])


def test_conflicting_source_hash_is_not_replaced_by_a_filename_guess(tmp_path):
    evidence, catalog = fixture(tmp_path)
    bundle = json.loads(Path(evidence['reading_material']['path']).read_text(encoding='utf8'))
    candidate = json.loads(Path(evidence['responses'][0]['response']['path']).read_text(encoding='utf8'))
    candidate['source']['file'] = 'incorrect-model-filename.pdf'
    candidate['source']['source_sha256'] = 'f' * 64
    with pytest.raises(ValueError, match='candidate_validation_failed'):
        body._derive({'mode': 'single'}, bundle,
            [(json.dumps(candidate, ensure_ascii=False), bundle)], AliasContext(), catalog)


def test_missing_figure_identity_is_kept_as_unverified_description(tmp_path):
    from preextract_contract import _blank, load_template
    from preextract_publish import prepare_publication
    evidence, catalog = fixture(tmp_path)
    bundle = json.loads(Path(evidence['reading_material']['path']).read_text(encoding='utf8'))
    candidate = json.loads(Path(evidence['responses'][0]['response']['path']).read_text(encoding='utf8'))
    asset = _blank(load_template()[1]['assets']['items'][0])
    asset.update(id='unknown-figure', kind='diagram', storage='image', evidence_basis='pixels',
                 source_image_id='unknown-image', meaning='模型保留的局部图示说明')
    candidate['assets']['items'] = [asset]
    artifacts, _, _ = body._derive({'mode': 'single'}, bundle,
        [(json.dumps(candidate, ensure_ascii=False), bundle)], AliasContext(), catalog)
    prepared = prepare_publication(artifacts['candidate'], bundle, tmp_path / 'publication',
        paperflow_root=RELEASE / 'core', catalog=catalog, counter=FixtureCounter(), redaction_policy='best_effort',
        candidate_validation=artifacts['validation'], admission_policy='learning')
    row = json.loads((Path(prepared['package_path']) / 'assets.json').read_text(encoding='utf8'))['items'][0]
    assert row['meaning'] == '模型保留的局部图示说明' and row['storage'] == 'description'
    assert not row.get('file') and any('不交付图片像素' in value for value in row['limits'])
    chunks = json.loads((Path(prepared['package_path']) / 'chunks.json').read_text(encoding='utf8'))['chunks']
    chart = [item for item in chunks if item['kind'] == 'chart' and 'unknown-figure' in item['asset_ids']]
    assert '不交付图片像素' in row['description']
    assert all('不交付图片像素' in item['content'] for item in chart)


@pytest.mark.parametrize('source_kind', ['unknown', 'different_image'])
def test_unconfirmed_image_binding_preserves_description_but_does_not_publish_pixels(tmp_path, source_kind):
    from preextract_contract import _blank, load_template, validate_candidate
    evidence, catalog = fixture(tmp_path)
    bundle = json.loads(Path(evidence['reading_material']['path']).read_text(encoding='utf8'))
    candidate = json.loads(Path(evidence['responses'][0]['response']['path']).read_text(encoding='utf8'))
    for ident, color in [('figure-1', 'white'), ('figure-2', 'black')]:
        path = tmp_path / (ident + '.png')
        Image.new('RGB', (8, 8), color).save(path)
        bundle['assets'].append(dict(id=ident, kind='diagram', position={'object_ref': ident}, image_path=str(path),
            sha256=hashlib.sha256(path.read_bytes()).hexdigest(), width=8, height=8,
            block_ids=['B001'], section_ids=['S001']))
    asset = _blank(load_template()[1]['assets']['items'][0])
    asset.update(id='figure-1', kind='diagram', source_image_id='missing' if source_kind == 'unknown' else 'figure-2',
                 storage='image', evidence_basis='pixels', meaning='模型给出的图示说明',
                 source_refs=[dict(file_ref='main', asset_id='figure-1', block_id='B001', section_id='S001')])
    candidate['assets']['items'] = [asset]
    report = validate_candidate(candidate, bundle, catalog, redaction_policy='best_effort', admission_policy='learning')
    assert report['valid'], report['errors']
    row = report['candidate']['assets']['items'][0]
    assert row['storage'] == 'description' and row['source_image_id'] is None and row['evidence_basis'] is None
    assert row['meaning'] == '模型给出的图示说明' and any('不交付图片像素' in value for value in row['limits'])
    if source_kind == 'different_image':
        from preextract_publish import prepare_publication
        artifacts, _, _ = body._derive({'mode': 'single'}, bundle,
            [(json.dumps(candidate, ensure_ascii=False), bundle)], AliasContext(), catalog)
        prepared = prepare_publication(artifacts['candidate'], bundle, tmp_path / 'publication',
            paperflow_root=RELEASE / 'core', catalog=catalog, counter=FixtureCounter(),
            redaction_policy='best_effort', candidate_validation=artifacts['validation'], admission_policy='learning')
        published = json.loads((Path(prepared['package_path']) / 'assets.json').read_text(encoding='utf8'))
        row = published['items'][0]
        assert row['storage'] == 'description' and not row.get('file')
        assert any('不交付图片像素' in value for value in row['limits'])


def test_source_reference_used_by_a_plan_is_not_silently_shifted(tmp_path):
    evidence, _ = fixture(tmp_path)
    bundle = json.loads(Path(evidence['reading_material']['path']).read_text(encoding='utf8'))
    candidate = json.loads(Path(evidence['responses'][0]['response']['path']).read_text(encoding='utf8'))
    candidate['profile']['learning_units'][0]['source_refs'][0]['block_id'] = 'INVENTED'
    candidate['gaps'].append({'target_pointer': '/profile/learning_units/0/source_refs/0/block_id'})
    with pytest.raises(ValueError, match='unknown_source_ref'):
        body._source_refs(candidate, bundle, report={'compatibility_report': [], 'warnings': []})


def test_unread_image_identity_reaches_its_public_chart(tmp_path):
    from preextract_contract import _blank, load_template
    from preextract_publish import prepare_publication
    evidence, catalog = fixture(tmp_path)
    bundle = json.loads(Path(evidence['reading_material']['path']).read_text(encoding='utf8'))
    candidate = json.loads(Path(evidence['responses'][0]['response']['path']).read_text(encoding='utf8'))
    image = tmp_path / 'unread.png'
    Image.new('RGB', (8, 8), 'white').save(image)
    bundle['assets'].append(dict(id='figure-unread', kind='diagram', image_path=str(image),
        sha256=hashlib.sha256(image.read_bytes()).hexdigest(), width=8, height=8,
        block_ids=['B001'], section_ids=['S001']))
    asset = _blank(load_template()[1]['assets']['items'][0])
    asset.update(id='figure-unread', kind='diagram', storage='image', source_image_id='figure-unread',
                 evidence_basis='pixels', meaning='保留图示描述',
                 source_refs=[dict(file_ref='main', asset_id='figure-unread')])
    candidate['assets']['items'] = [asset]
    candidate['source']['reading_scope']['unread_refs'] = ['figure-unread']
    artifacts, _, _ = body._derive({'mode': 'single'}, bundle,
        [(json.dumps(candidate, ensure_ascii=False), bundle)], AliasContext(), catalog)
    assert artifacts['candidate']['assets']['items'][0]['evidence_basis'] is None
    prepared = prepare_publication(artifacts['candidate'], bundle, tmp_path / 'publication',
        paperflow_root=RELEASE / 'core', catalog=catalog, counter=FixtureCounter(),
        redaction_policy='best_effort', candidate_validation=artifacts['validation'], admission_policy='learning')
    package = Path(prepared['package_path'])
    profile = json.loads((package / 'profile.json').read_text(encoding='utf8'))
    assets = json.loads((package / 'assets.json').read_text(encoding='utf8'))['items']
    assert any('figure-unread' in row['use_limit'] and '未读' in row['use_limit'] for row in profile['gaps'])
    assert any('figure-unread' in row['description'] and '未读' in row['description'] for row in assets)


def test_main_document_unread_remains_explicit_in_public_learning(tmp_path):
    from preextract_publish import prepare_publication
    evidence, catalog = fixture(tmp_path)
    bundle = json.loads(Path(evidence['reading_material']['path']).read_text(encoding='utf8'))
    candidate = json.loads(Path(evidence['responses'][0]['response']['path']).read_text(encoding='utf8'))
    candidate['source']['reading_scope']['unread_refs'] = ['main']
    candidate['source']['reading_scope']['read_refs'] = []
    artifacts, _, _ = body._derive({'mode': 'single'}, bundle,
        [(json.dumps(candidate, ensure_ascii=False), bundle)], AliasContext(), catalog)
    prepared = prepare_publication(artifacts['candidate'], bundle, tmp_path / 'publication',
        paperflow_root=RELEASE / 'core', catalog=catalog, counter=FixtureCounter(),
        redaction_policy='best_effort', candidate_validation=artifacts['validation'], admission_policy='learning')
    chunks = json.loads((Path(prepared['package_path']) / 'chunks.json').read_text(encoding='utf8'))['chunks']
    assert any('主文档未读' in row['content'] for row in chunks if row['kind'] == 'learning')
