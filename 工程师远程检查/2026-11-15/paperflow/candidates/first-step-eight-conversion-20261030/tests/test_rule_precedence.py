"""User-visible precedence, without invoking Nimble or live Prefect."""
import io
import json
import zipfile

import fitz
import pytest
from PIL import Image

from paperflow_prefect.group_classification import classify_group, process_one_group
from paperflow_prefect.group_evidence import read_evidence


def pdf(path, *, poster=False):
    document = fitz.open()
    image = io.BytesIO()
    Image.new('RGB', (20, 20), 'white').save(image, format='PNG')
    for _ in range(3 if poster else 1):
        page = document.new_page(width=842 if poster else 595, height=595 if poster else 842)
        page.insert_text((30, 30), 'poster' if poster else 'Synthetic thesis abstract')
        if poster:
            page.insert_image(fitz.Rect(50, 50, 200, 200), stream=image.getvalue())
    document.save(path)
    document.close()


def docx(path, text):
    xml = ('<w:document xmlns:w="http://schemas.openxmlformats.org/'
           'wordprocessingml/2006/main"><w:body><w:p><w:r><w:t>' + text +
           '</w:t></w:r></w:p></w:body></w:document>')
    with zipfile.ZipFile(path, 'w') as archive:
        archive.writestr('[Content_Types].xml', '<Types/>')
        archive.writestr('word/document.xml', xml)


class Judge:
    def __init__(self):
        self.other_calls = 0

    def choose_body(self, rows):
        return {'choice': next(row['file_id'] for row in rows if row['relative'] == 'paper.pdf')}

    def choose_other(self, *_):
        self.other_calls += 1
        return {'choice': 'attachment'}


def make_group(tmp_path):
    library = tmp_path / 'source'
    group = library / 'person'
    group.mkdir(parents=True)
    pdf(group / 'paper.pdf')
    return library, group


def test_legacy_visual_attachment_needs_no_second_model_decision(tmp_path):
    library, group = make_group(tmp_path)
    pdf(group / 'poster.pdf', poster=True)
    assert read_evidence(group / 'poster.pdf')['visual_heavy']
    judge = Judge()

    handoff = classify_group(group, library, judge, read_evidence)

    assert [row['relative'] for row in handoff['attachments']] == ['poster.pdf']
    assert handoff['ready_for_receipt']
    assert judge.other_calls == 0


def test_other_non_trash_file_defaults_to_attachment_without_second_decision(tmp_path):
    library, group = make_group(tmp_path)
    (group / 'supporting.txt').write_text('supporting material', encoding='utf8')
    judge = Judge()

    handoff = classify_group(group, library, judge, read_evidence)

    assert [row['relative'] for row in handoff['attachments']] == ['supporting.txt']
    assert handoff['ready_for_receipt']
    assert judge.other_calls == 0


def test_evidenced_process_document_keeps_the_legacy_trash_rule(tmp_path):
    library, group = make_group(tmp_path)
    docx(group / '开题报告.docx', '开题报告：研究安排')
    judge = Judge()

    handoff = classify_group(group, library, judge, read_evidence)

    assert [row['relative'] for row in handoff['trash']] == ['开题报告.docx']
    assert handoff['trash'][0]['decision_basis']['method'] == 'filename_and_text'
    assert handoff['ready_for_receipt']
    assert judge.other_calls == 0


def test_identified_body_with_unreadable_separate_cover_keeps_original_and_error(tmp_path):
    library, group = make_group(tmp_path)
    cover = group / '毕业论文封面.doc'
    cover.write_bytes(b'synthetic unreadable cover')
    before = cover.read_bytes()
    judge = Judge()

    def reader(path):
        if path == cover:
            raise RuntimeError('synthetic conversion timeout')
        return read_evidence(path)

    result = process_one_group(group, library, tmp_path / 'output', lambda: judge,
                               reader, verified_new=True)

    assert result.get('handoff'), result
    handoff = json.loads((tmp_path / 'output' / result['handoff']).read_text(encoding='utf8'))
    assert handoff['body']['relative'] == 'paper.pdf'
    assert [row['relative'] for row in handoff['attachments']] == [cover.name]
    assert not handoff['unresolved'] and result['delivery_complete']
    record = json.loads(open(result['evidence_handoffs'][0], encoding='utf8').read())
    assert record['errors'] and '读取失败' in str(record['errors'])
    assert record['file_info']['actual_format'] == 'unknown'
    assert '转换失败' not in str(record['limitations'])
    assert cover.read_bytes() == before and judge.other_calls == 0


@pytest.mark.parametrize('name', [
    '封面设计论文终稿.doc',
    '论文封面及正文.doc',
    '论文正文封面.doc',
])
def test_unreadable_possible_body_is_not_hidden_by_cover_in_name(tmp_path, name):
    library = tmp_path / 'source'
    group = library / 'person'
    group.mkdir(parents=True)
    pdf(group / 'translation.pdf')
    possible_body = group / name
    possible_body.write_bytes(b'synthetic unreadable document')

    class TranslationJudge(Judge):
        def choose_body(self, rows):
            return {'choice': next(row['file_id'] for row in rows
                                   if row['relative'] == 'translation.pdf')}

    def reader(path):
        if path == possible_body:
            raise RuntimeError('synthetic read failure')
        return read_evidence(path)

    handoff = classify_group(group, library, TranslationJudge(), reader)

    assert handoff['body']['relative'] == 'translation.pdf'
    assert [row['relative'] for row in handoff['unresolved']] == [name]
    assert not handoff['attachments']
    assert not handoff['ready_for_receipt']
