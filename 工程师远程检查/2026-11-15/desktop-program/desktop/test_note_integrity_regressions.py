import json
import zipfile

import pytest

from evidence import build_package, export_package

NS = 'xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"'


def paragraph(text):
    return f'<w:p><w:r><w:t>{text}</w:t></w:r></w:p>'


def document(path, notes, body=None, kind='footnote'):
    if body is None:
        body = (paragraph('摘要') + '<w:p><w:r><w:t>正文引用</w:t>'
                f'<w:{kind}Reference w:id="0"/></w:r></w:p>'
                + paragraph('致谢') + paragraph('感谢私人导师。'))
    with zipfile.ZipFile(path, 'w') as archive:
        archive.writestr('word/document.xml', f'<w:document {NS}><w:body>{body}</w:body></w:document>')
        archive.writestr(f'word/{kind}s.xml', f'<w:{kind}s {NS}>{notes}</w:{kind}s>')
    return path


@pytest.mark.parametrize('kind', ['footnote', 'endnote'])
def test_zero_note_survives_acknowledgements_and_final_export(tmp_path, kind):
    notes = (f'<w:{kind} w:id="0">' + paragraph('原始文献，2020，第12页。') + f'</w:{kind}>'
             + f'<w:{kind} w:id="7" w:type="separator"><w:p><w:r><w:separator/></w:r></w:p></w:{kind}>')
    source = document(tmp_path/'source.docx', notes, kind=kind)
    original = source.read_bytes()
    package = tmp_path/'package'
    meta = build_package(source, package, {'id': 'test', 'title': '注释完整性研究'}, redact_content=False)
    meta.update(preprocessing_complete=True, text_privacy_complete=True, privacy_status='ai_passed')
    export_package(package, meta)
    ingest = (package/'ingest.md').read_text(encoding='utf-8')
    assert f'[^{kind}Reference0]: 原始文献，2020，第12页。' in ingest
    assert f'[^{kind}Reference7]' not in ingest
    assert '感谢私人导师' not in ingest
    assert source.read_bytes() == original
    assert json.loads((package/'manifest.json').read_text(encoding='utf-8'))['completeness_status'] == 'complete'


def test_note_preserves_native_table_between_original_paragraphs(tmp_path):
    table = ('<w:tbl><w:tblGrid><w:gridCol/><w:gridCol/></w:tblGrid>'
             '<w:tr><w:tc>' + paragraph('选项') + '</w:tc><w:tc>' + paragraph('比例') + '</w:tc></w:tr>'
             '<w:tr><w:tc>' + paragraph('同意') + '</w:tc><w:tc>' + paragraph('46.51%') + '</w:tc></w:tr></w:tbl>')
    note = '<w:endnote w:id="0">' + paragraph('附录A 问卷统计') + table + paragraph('附录B 访谈提纲') + '</w:endnote>'
    source = document(tmp_path/'source.docx', note, kind='endnote')
    package = tmp_path/'package'
    meta = build_package(source, package, {'id': 'test', 'title': '结构化附录'}, redact_content=False)
    tables = [b for b in meta['blocks'] if b['type'] == 'table']
    assert len(tables) == 1
    assert [c['text'] for c in tables[0]['cells']] == ['选项', '比例', '同意', '46.51%']
    assert tables[0]['position']['part'] == 'word/endnotes.xml'
    assert tables[0]['position']['id'] == '0'
    meta.update(preprocessing_complete=True, text_privacy_complete=True, privacy_status='ai_passed')
    export_package(package, meta)
    ingest = (package/'ingest.md').read_text(encoding='utf-8')
    assert ingest.index('附录A') < ingest.index('46.51%') < ingest.index('附录B')


@pytest.mark.parametrize('notes, error', [
    ('', '缺少'),
    ('<w:footnote w:id="0"><w:p><w:r><w:footnoteRef/></w:r></w:p></w:footnote>', '为空'),
    ('<w:footnote w:id="0">'+paragraph('文献甲')+'</w:footnote>'
     '<w:footnote w:id="0">'+paragraph('文献乙')+'</w:footnote>', '重复'),
])
def test_broken_notes_cannot_be_published_as_complete(tmp_path, notes, error):
    source = document(tmp_path/'source.docx', notes)
    package = tmp_path/'package'
    meta = build_package(source, package, {'id': 'test', 'title': '注释完整性研究'}, redact_content=False)
    meta.update(preprocessing_complete=True, text_privacy_complete=True, privacy_status='ai_passed')
    with pytest.raises(ValueError, match='注释.*'+error):
        export_package(package, meta)
    assert not (package/'manifest.json').exists()


def test_export_rechecks_old_cached_blocks_not_only_new_parser_issues(tmp_path):
    source = tmp_path/'source.txt'
    source.write_text('摘要\n正文[^endnoteReference12]', encoding='utf-8')
    package = tmp_path/'package'
    meta = build_package(source, package, {'id': 'test', 'title': '缓存检查'}, redact_content=False)
    meta.update(preprocessing_complete=True, text_privacy_complete=True)
    with pytest.raises(ValueError, match='注释.*缺少'):
        export_package(package, meta)


def test_reference_only_in_removed_acknowledgements_does_not_block(tmp_path):
    body = paragraph('摘要') + paragraph('保留的研究正文') + paragraph('致谢')
    body += '<w:p><w:r><w:t>感谢。</w:t><w:footnoteReference w:id="9"/></w:r></w:p>'
    source = document(tmp_path/'source.docx', '', body=body)
    package = tmp_path/'package'
    meta = build_package(source, package, {'id': 'test', 'title': '致谢检查'}, redact_content=False)
    meta.update(preprocessing_complete=True, text_privacy_complete=True)
    export_package(package, meta)
    assert (package/'manifest.json').exists()


def test_unused_legacy_separator_definition_does_not_become_missing_research():
    from note_integrity import assert_note_integrity
    assert_note_integrity([{'type':'paragraph','text':'正文'},
                           {'type':'paragraph','text':'[^footnoteReference1]: '}])


def test_multiblock_note_formula_keeps_its_exact_source_and_writing_unit(tmp_path):
    note = ('<w:footnote w:id="0">'+paragraph('原文公式说明')
            + '<w:p xmlns:m="http://schemas.openxmlformats.org/officeDocument/2006/math">'
              '<m:oMath><m:r><m:t>x+y</m:t></m:r></m:oMath></w:p></w:footnote>')
    source=document(tmp_path/'source.docx',note)
    package=tmp_path/'package'
    meta=build_package(source,package,{'id':'test','title':'注释公式'},redact_content=False)
    meta.update(preprocessing_complete=True,text_privacy_complete=True)
    export_package(package,meta)
    equations=list((package/'assets/equations').glob('*.json'))
    assert len(equations)==1
    equation=json.loads(equations[0].read_text(encoding='utf-8'))
    assert equation['block_ids'] and equation['section_ids']
