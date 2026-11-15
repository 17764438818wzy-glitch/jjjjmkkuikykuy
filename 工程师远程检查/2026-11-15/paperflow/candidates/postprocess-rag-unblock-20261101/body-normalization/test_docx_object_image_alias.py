import hashlib
import importlib.util
from pathlib import Path
import sys

from PIL import Image
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_body_normalization import source
from preextract_contract import _blank, load_template

spec = importlib.util.spec_from_file_location('contract_candidate', Path(__file__).with_name('preextract_contract.py'))
contract = importlib.util.module_from_spec(spec)
spec.loader.exec_module(contract)


@pytest.mark.parametrize(('source_ref', 'matches'), [('object_0002', True), ('object_9999', False)])
def test_docx_object_reference_uses_the_same_supplied_image(tmp_path, source_ref, matches):
    candidate, bundle, catalog = source(tmp_path)
    image = tmp_path / 'object.png'
    Image.new('RGB', (8, 8), 'white').save(image)
    asset_id, object_ref = 'docx_object_object_0002_1', 'object_0002'
    bundle['assets'] = [dict(id=asset_id, kind='docx_visual_crop', position={'object_ref': object_ref},
        image_path=str(image), sha256=hashlib.sha256(image.read_bytes()).hexdigest(), width=8, height=8,
        block_ids=['B001'], section_ids=['S001'])]
    _, template = load_template()
    asset = _blank(template['assets']['items'][0])
    asset.update(id=asset_id, kind='diagram', source_image_id=source_ref, storage='image',
                 evidence_basis='pixels', source_refs=[dict(file_ref='main', asset_id=asset_id,
                 block_id='B001', section_id='S001')])
    candidate['assets']['items'] = [asset]
    result = contract.validate_candidate(candidate, bundle, catalog,
        redaction_policy='best_effort', admission_policy='learning')
    if matches:
        assert result['valid'], result['errors']
        assert result['candidate']['assets']['items'][0]['source_image_id'] == asset_id
        assert any(item['code'] == 'normalized_docx_object_image' for item in result['compatibility_report'])
    else:
        assert result['valid'], result['errors']
        row = result['candidate']['assets']['items'][0]
        assert row['source_image_id'] is None and row['storage'] == 'omit'
        assert any(item['code'] == 'unconfirmed_image_binding' for item in result['compatibility_report'])


def test_docx_object_alias_with_another_image_reference_does_not_choose_pixels(tmp_path):
    candidate, bundle, catalog = source(tmp_path)
    image = tmp_path / 'object.png'
    Image.new('RGB', (8, 8), 'white').save(image)
    asset_id, other_id = 'docx_object_object_0002_1', 'docx_object_object_0003_1'
    bundle['assets'] = [dict(id=asset_id, kind='docx_visual_crop', position={'object_ref': 'object_0002'},
        image_path=str(image), sha256=hashlib.sha256(image.read_bytes()).hexdigest(), width=8, height=8,
        block_ids=['B001'], section_ids=['S001']),
        dict(id=other_id, kind='docx_visual_crop', position={'object_ref': 'object_0003'},
        image_path=str(image), sha256=hashlib.sha256(image.read_bytes()).hexdigest(), width=8, height=8,
        block_ids=['B001'], section_ids=['S001'])]
    asset = _blank(load_template()[1]['assets']['items'][0])
    asset.update(id=asset_id, kind='diagram', source_image_id='object_0002', storage='image',
                 evidence_basis='pixels', source_refs=[dict(file_ref='main', asset_id=other_id,
                 block_id='B001', section_id='S001')])
    candidate['assets']['items'] = [asset]
    result = contract.validate_candidate(candidate, bundle, catalog,
        redaction_policy='best_effort', admission_policy='learning')
    assert result['valid'], result['errors']
    row = result['candidate']['assets']['items'][0]
    assert row['source_image_id'] is None and row['storage'] == 'omit'
    assert any(item['code'] == 'unconfirmed_image_binding' for item in result['compatibility_report'])
