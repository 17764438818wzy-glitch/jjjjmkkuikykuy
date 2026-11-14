"""A verified source chart boundary replaces a model's incomplete crop."""
import copy
import io

import fitz
import pytest
from PIL import Image

import evidence_tasks
from auto_evidence import Rejected, _visual_recheck_needed, media_privacy, pixel_box
from test_native_chart_region import add_clip, docx, target


@pytest.mark.parametrize('ambiguous', [False, True])
def test_recovery_uses_only_unique_original_chart_clip_and_still_requires_privacy_review(tmp_path, monkeypatch, ambiguous):
    source = docx(tmp_path)
    source_bytes = source.read_bytes()
    pdf_path = tmp_path / 'render.pdf'
    with fitz.open() as pdf:
        add_clip(pdf)
        if ambiguous:
            add_clip(pdf)
        pdf.save(pdf_path)
    monkeypatch.setattr(evidence_tasks, '_pdf', lambda *args: fitz.open(pdf_path))
    monkeypatch.setattr(evidence_tasks, '_locate', lambda *args: {0})
    monkeypatch.setattr(evidence_tasks, '_graphic_member_locations', lambda *args: (set(), []))
    block = {**target(), 'type': 'paragraph', 'text': '', 'graphic_object': True}
    if ambiguous:
        block['native_data_available'] = True
    meta = {'blocks': [block], 'resources': [], 'issues': [
        {'code': 'native_chart_visual', 'position': copy.deepcopy(block['position'])}]}
    package = tmp_path / 'package'
    (package / 'assets').mkdir(parents=True)
    incomplete = [100/600, 110/800, 400/600, 180/800]

    class Agent:
        def call(self, task, payload, images=()):
            assert task == 'repair'
            return {'decision': 'pass', 'reason': 'Original chart', 'sensitive_boxes': [],
                    'insertions': [{'after_id': 'chart_target', 'kind': 'image', 'page': 1,
                                    'bbox': incomplete, 'text': 'Alpha Beta Gamma',
                                    'description': 'Original bars', 'caption': ''}],
                    'nonresearch_objects': []}

    evidence_tasks.resolve_scoped(meta, package, source, tmp_path / 'work', 80, Agent(), lambda *args: None)
    resource = meta['resources'][0]
    expected_bbox = incomplete if ambiguous else [100/600, 110/800, 400/600, 250/800]
    with fitz.open(pdf_path) as pdf:
        blob = pdf[0].get_pixmap(matrix=fitz.Matrix(1.5, 1.5)).tobytes('png')
    page = Image.open(io.BytesIO(blob)).convert('RGB')
    expected = page.crop(pixel_box(expected_bbox, page.size))
    actual = Image.open(package / resource['local_path']).convert('RGB')
    assert actual.size == expected.size and actual.tobytes() == expected.tobytes()
    proof = resource['crop_provenance'].get('native_chart_region')
    assert bool(proof) is not ambiguous
    if proof:
        assert proof['method'] == 'native_chart_unique_pdf_clip'
        assert 'categories' not in proof
    assert _visual_recheck_needed(resource)
    assert source.read_bytes() == source_bytes

    class WrongDecoration:
        def call(self, task, payload, images=()):
            assert task == 'privacy_image' and payload['phase'] == 'locate_privacy_regions'
            return {'decision': 'pass', 'reason': 'Mistaken decoration verdict',
                    'research': False, 'nonresearch_kind': 'decoration',
                    'sensitive_boxes': [], 'text': '', 'description': ''}

    with pytest.raises(Rejected, match='装饰'):
        media_privacy(meta, package, WrongDecoration(), lambda *args: None)
    assert len(meta['resources']) == 1
    assert not resource.get('masked_observation_verified')
