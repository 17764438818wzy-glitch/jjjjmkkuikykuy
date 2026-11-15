"""Classification defaults are verified without starting the real model or Prefect."""
import io
import json

import fitz
import pytest
from PIL import Image

from paperflow_prefect.first_step_control import UnknownRequest
from paperflow_prefect.group_classification import classify_group, process_one_group
from paperflow_prefect.group_evidence import read_evidence


def make_pdf(path, *, image=False, text=None, pages=1):
    image_bytes = io.BytesIO()
    Image.new('RGB', (20, 20), 'white').save(image_bytes, format='PNG')
    doc = fitz.open()
    for _ in range(pages):
        page = doc.new_page()
        if image:
            page.insert_image(fitz.Rect(20, 50, 180, 200), stream=image_bytes.getvalue())
        if text:
            page.insert_text((20, 30), text)
    doc.save(path)
    doc.close()


class Judge:
    def __init__(self, body='paper.pdf', others=None):
        self.body = body
        self.others = others or {}

    def choose_body(self, rows):
        return {'choice': next((r['file_id'] for r in rows if r['relative'] == self.body), None)}

    def choose_other(self, row, body_id):
        return {'choice': self.others.get(row['relative'], 'uncertain')}


def test_chosen_pdf_and_scans_create_complete_package_with_evidence(tmp_path):
    library = tmp_path / 'source'
    group = library / 'person'
    group.mkdir(parents=True)
    make_pdf(group / 'paper.pdf', text='Synthetic thesis abstract')
    make_pdf(group / 'image-only.pdf', image=True)
    make_pdf(group / 'scan-with-text.pdf', image=True, text='p', pages=3)
    before = {p.name: p.read_bytes() for p in group.iterdir()}

    result = process_one_group(group, library, tmp_path / 'output', lambda: Judge(),
                               read_evidence, verified_new=True)

    assert result.get('handoff'), result
    handoff = json.loads((tmp_path / 'output' / result['handoff']).read_text(encoding='utf8'))
    assert handoff['body']['relative'] == 'paper.pdf'
    assert {r['relative'] for r in handoff['attachments']} == {'image-only.pdf', 'scan-with-text.pdf'}
    assert handoff['unresolved'] == [] and handoff['ready_for_receipt']
    assert result['delivery_complete'] and result['evidence_errors'] == []
    assert len(result['evidence_handoffs']) == 2
    evidence = [json.loads(open(p, encoding='utf8').read()) for p in result['evidence_handoffs']]
    image_only = next(e for e in evidence if e['file_info']['name'] == 'image-only.pdf')
    assert image_only['read_scope']['images']['status'] == 'not_read'
    assert {p.name: p.read_bytes() for p in group.iterdir()} == before


def test_other_files_without_explicit_trash_evidence_default_to_attachment(tmp_path):
    library, group = tmp_path / 'source', tmp_path / 'source' / 'person'
    group.mkdir(parents=True)
    make_pdf(group / 'paper.pdf', text='Synthetic thesis abstract')
    Image.new('RGB', (10, 10), 'white').save(group / 'poster.png')
    (group / 'cover.txt').write_text('synthetic separate cover', encoding='utf8')
    (group / 'draft.txt').write_text('synthetic process draft', encoding='utf8')
    class NoOtherDecision(Judge):
        def choose_other(self, *_):
            raise AssertionError('extra model decision must not be used')
    judge = NoOtherDecision()

    result = classify_group(group, library, judge, read_evidence)

    assert result['body']['relative'] == 'paper.pdf'
    assert {r['relative'] for r in result['attachments']} == {'poster.png', 'cover.txt', 'draft.txt'}
    assert result['trash'] == []
    assert result['unresolved'] == [] and result['ready_for_receipt']


def test_only_image_pdf_cannot_be_demoted_to_an_attachment(tmp_path):
    library, group = tmp_path / 'source', tmp_path / 'source' / 'person'
    group.mkdir(parents=True)
    make_pdf(group / 'paper.pdf', image=True)
    result = classify_group(group, library, Judge(), read_evidence)
    assert result['body'] is None and result['attachments'] == []
    assert [r['relative'] for r in result['unresolved']] == ['paper.pdf']
    assert not result['ready_for_receipt']


def test_failed_word_is_not_hidden_when_foreign_translation_was_selected(tmp_path):
    library, group = tmp_path / 'source', tmp_path / 'source' / 'person'
    group.mkdir(parents=True)
    (group / 'paper.doc').write_bytes(b'broken synthetic Word document')
    make_pdf(group / 'translation.pdf', text='Foreign translation')
    result = classify_group(group, library, Judge(body='translation.pdf'), read_evidence)
    assert result['body']['relative'] == 'translation.pdf'
    assert [r['relative'] for r in result['unresolved']] == ['paper.doc']
    assert not result['ready_for_receipt']


def test_unknown_body_request_is_not_silently_converted_to_attachment(tmp_path):
    library, group = tmp_path / 'source', tmp_path / 'source' / 'person'
    group.mkdir(parents=True)
    make_pdf(group / 'paper.pdf', text='Synthetic thesis abstract')
    (group / 'other.txt').write_text('some evidence', encoding='utf8')

    class UnknownJudge(Judge):
        def choose_body(self, rows):
            raise UnknownRequest('outcome unknown')

    with pytest.raises(UnknownRequest):
        classify_group(group, library, UnknownJudge(), read_evidence)
