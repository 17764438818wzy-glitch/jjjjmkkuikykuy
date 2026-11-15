"""Native Word note definitions must remain valid Markdown after export."""
import copy
import pytest

from evidence import build_package, export_package
from note_integrity import assert_note_integrity
from test_note_integrity_regressions import document, paragraph


@pytest.mark.parametrize('kind', ['footnote', 'endnote'])
def test_native_note_definition_with_inherited_list_style_remains_linked(tmp_path, kind):
    inherited = '<w:pPr><w:numPr><w:ilvl w:val="0"/><w:numId w:val="0"/></w:numPr></w:pPr>'
    note = (f'<w:{kind} w:id="0"><w:p>' + inherited
            + '<w:r><w:t>原始文献，2020，第12页。</w:t></w:r></w:p>'
            + f'</w:{kind}>')
    body = (paragraph('摘要') + '<w:p><w:r><w:t>正文引用</w:t>'
            + f'<w:{kind}Reference w:id="0"/></w:r></w:p>'
            + '<w:p>' + inherited + '<w:r><w:t>普通列表内容</w:t></w:r></w:p>')
    source = document(tmp_path/'source.docx', note, body=body, kind=kind)
    package = tmp_path/'package'
    meta = build_package(source, package, {'id':'list-note', 'title':'注释列表样式'}, redact_content=False)
    note_block = next(b for b in meta['blocks'] if b.get('note_label') == kind+'Reference0')
    assert note_block['list']['format'] == 'unknown'
    original_blocks = copy.deepcopy(meta['blocks'])
    meta.update(preprocessing_complete=True, text_privacy_complete=True, privacy_status='ai_passed')
    export_package(package, meta)
    for name in ('ingest.md','document.md'):
        content = (package/name).read_text(encoding='utf-8')
        assert f'\n[^{kind}Reference0]: 原始文献，2020，第12页。' in content
        assert '\n- 普通列表内容' in content
        assert_note_integrity([{'type':'paragraph','text':content}])
    assert meta['blocks'] == original_blocks
