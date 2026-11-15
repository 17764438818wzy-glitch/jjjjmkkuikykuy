import hashlib
import json
from pathlib import Path

import fitz
import pytest
from docx import Document

from local_prepare import prepare_source
from reading_cleanup import ScopeUncertain


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_text_is_read_from_delivery_and_only_clean_material_is_written(tmp_path):
    source = tmp_path / 'delivery' / 'files' / 'paper.txt'
    source.parent.mkdir(parents=True)
    source.write_text('封面作者\n摘要\n摘要内容\n第1章 正文\n正文引用[3]。\n参考文献\n[3] 作者\n附录A\n附录材料\n致谢\n感谢\n', encoding='utf8')
    before = source.read_bytes()
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    work = tmp_path / 'work'
    result = prepare_source(source, sha(source), work, catalog)
    bundle = json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    text = Path(bundle['source']['text_file']).read_text(encoding='utf8')
    assert '摘要内容' in text and '正文引用[3]' in text
    assert all(word not in text for word in ('封面作者', '[3] 作者', '感谢'))
    assert '附录材料' in text
    assert source.read_bytes() == before
    assert not any(p.is_file() and p.read_bytes() == before for p in work.rglob('*'))
    assert bundle['source']['source_sha256'] == hashlib.sha256(before).hexdigest()


def test_docx_parser_excludes_real_tail_and_never_copies_raw_docx(tmp_path):
    source = tmp_path / 'paper.docx'
    doc = Document()
    doc.add_paragraph('封面作者')
    doc.add_heading('摘要', 1)
    doc.add_paragraph('真实摘要。')
    doc.add_heading('第1章 研究', 1)
    doc.add_paragraph('论文方法与引用[1]。')
    doc.add_heading('参考文献', 1)
    doc.add_paragraph('[1] 其他论文')
    doc.add_heading('附录 A', 1)
    doc.add_paragraph('不传送的附件正文')
    doc.add_heading('致谢', 1)
    doc.add_paragraph('感谢所有人')
    doc.save(source)
    original = source.read_bytes()
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    result = prepare_source(source, sha(source), tmp_path / 'work', catalog)
    bundle = json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    text = Path(bundle['source']['text_file']).read_text(encoding='utf8')
    assert '论文方法与引用[1]' in text
    assert '封面作者' not in text and '其他论文' not in text and '感谢所有人' not in text
    assert not any(p.is_file() and p.read_bytes() == original for p in (tmp_path / 'work').rglob('*'))
    assert source.read_bytes() == original


def test_pdf_with_image_only_page_is_not_deemed_blank(tmp_path):
    from PIL import Image
    import io

    source = tmp_path / 'paper.pdf'
    image = Image.new('RGB', (40, 40), 'white')
    image.putpixel((20, 20), (0, 0, 0))
    encoded = io.BytesIO()
    image.save(encoded, format='PNG')
    document = fitz.open()
    page = document.new_page()
    page.insert_image(page.rect, stream=encoded.getvalue())
    document.save(source)
    document.close()
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    result=prepare_source(source, sha(source), tmp_path / 'work', catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert material['readiness']['state']=='review_required'
    assert any(i['code']=='ocr_page_incomplete' for i in material['issues'])
    assert source.exists()


def test_digest_mismatch_stops_before_preparation(tmp_path):
    source = tmp_path / 'paper.txt'
    source.write_text('摘要\n真实内容', encoding='utf8')
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    with pytest.raises(ValueError):
        prepare_source(source, '0' * 64, tmp_path / 'work', catalog)
    assert not (tmp_path / 'work' / 'material.json').exists()


def test_word_line_breaks_are_normalized_before_material_is_built(tmp_path):
    source = tmp_path / 'wrapped.docx'
    doc = Document()
    doc.add_heading('摘要', 1)
    paragraph = doc.add_paragraph('英文 line')
    paragraph.add_run().add_break()
    paragraph.add_run('continues here')
    doc.add_paragraph('')
    doc.add_heading('参考文献', 1)
    doc.add_paragraph('不进入交接的条目')
    doc.save(source)
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    result = prepare_source(source, sha(source), tmp_path / 'work', catalog)
    bundle = json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert any('英文 line continues here' in block['text'] for block in bundle['blocks'])
    assert any(row['kind'] == 'wrapped_line' for row in bundle['cleanup']['layout_changes'])
    assert any(block.get('layout_mapping') for block in bundle['blocks'] if 'continues here' in block['text'])


def test_scanned_page_with_only_a_page_number_is_not_treated_as_text(tmp_path):
    from PIL import Image
    import io
    source = tmp_path / 'scanned.pdf'
    im = Image.new('RGB', (200, 200), 'white')
    im.putpixel((20, 20), (0, 0, 0))
    buf = io.BytesIO()
    im.save(buf, format='PNG')
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((40, 50), 'Abstract')
    page.insert_text((40, 70), 'This is actual research text.')
    page = doc.new_page()
    page.insert_image(page.rect, stream=buf.getvalue())
    page.insert_text((300, 820), '10')
    doc.save(source)
    doc.close()
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    result=prepare_source(source, sha(source), tmp_path / 'work', catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert material['readiness']['state']=='review_required'
    assert any(i['code']=='ocr_page_incomplete' for i in material['issues'])
    assert material['source']['source_sha256']==sha(source)


def test_large_scanned_page_with_superimposed_extra_text_is_still_held(tmp_path):
    from PIL import Image
    import io
    source = tmp_path / 'scan-with-text.pdf'
    image = Image.new('RGB', (200, 200), 'white')
    image.putpixel((20, 20), (0, 0, 0))
    buf = io.BytesIO()
    image.save(buf, format='PNG')
    doc = fitz.open()
    page = doc.new_page()
    page.insert_image(page.rect, stream=buf.getvalue())
    page.insert_text((40, 50), 'Abstract')
    for y in (80, 100, 120):
        page.insert_text((40, y), 'Some unrelated superimposed native text that is not full OCR.')
    doc.save(source)
    doc.close()
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    result=prepare_source(source, sha(source), tmp_path / 'work', catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert any(i['code'] in {'ocr_page_incomplete','ocr_duplicates_native'} for i in material['issues']), material['issues']
    assert not any(i['kind']=='visual_scope' for i in material['readiness']['items'])


def test_scan_split_across_many_small_rasters_is_not_assumed_to_be_body(tmp_path):
    from PIL import Image
    import io
    source = tmp_path / 'tiled-scan.pdf'
    im = Image.new('RGB', (100, 40), 'white')
    im.putpixel((20, 20), (0, 0, 0))
    buf = io.BytesIO()
    im.save(buf, format='PNG')
    doc = fitz.open()
    page = doc.new_page(width=600, height=800)
    page.insert_text((40, 20), 'Abstract')
    page.insert_text((40, 40), 'Native text does not prove every image is research content.')
    for y in range(100, 700, 100):
        page.insert_image(fitz.Rect(50,y,550,y+60), stream=buf.getvalue())
    doc.save(source)
    doc.close()
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    result=prepare_source(source, sha(source), tmp_path / 'work', catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert any(i['code'] in {'ocr_page_incomplete','ocr_duplicates_native'} for i in material['issues']), material['issues']
    assert not any(i['kind']=='visual_scope' for i in material['readiness']['items'])


def test_unverified_pdf_vector_page_cannot_be_sent_as_page_image(tmp_path, monkeypatch):
    from local_prepare import _native_paths
    from local_prepare import plan_clean_material
    from test_capacity_plan import config
    _native_paths()
    import evidence
    def parse(source, redact, resource, issue):
        issue('pdf_vector_review', 'unknown vector text', {'page': 2, 'bbox': [50, 50, 500, 700]})
        return ([{'id':'ab', 'type':'paragraph', 'text':'Abstract',
                  'position':{'page':1, 'bbox':[50, 50, 150, 70]}},
                 {'id':'vector', 'type':'paragraph', 'text':'[unverified vector]',
                  'unresolved_object':True, 'position':{'page':2, 'bbox':[50,50,500,700]}}], [], [])
    monkeypatch.setattr(evidence, 'parse_pdf', parse)
    source = tmp_path / 'vector.pdf'
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((50, 50), 'Abstract')
    page = doc.new_page()
    page.insert_text((50, 50), '10')
    page.draw_line((100, 100), (500, 700))
    doc.save(source)
    doc.close()
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    result = prepare_source(source, sha(source), tmp_path / 'work', catalog)
    assert result['handoff_status'] == 'review_required'
    plan=plan_clean_material(result['material_path'], '模板', catalog, config(200000), tmp_path / 'plan')['plan']
    assert plan['source_completeness']=='partial'
    assert not any(part['asset_ids'] for part in plan['parts'])


def test_text_only_pdf_is_cleaned_without_ocr(tmp_path):
    source = tmp_path/'text-only.pdf'
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((50,50), 'Cover information')
    page = doc.new_page()
    page.insert_text((50,50), 'Abstract')
    page.insert_text((50,80), 'This is research content.')
    page = doc.new_page()
    page.insert_text((50,50), 'References')
    page.insert_text((50,80), 'End section citation.')
    doc.save(source)
    doc.close()
    catalog = tmp_path/'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    result = prepare_source(source, sha(source), tmp_path/'work', catalog)
    material = json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    text = Path(material['source']['text_file']).read_text(encoding='utf8')
    assert 'research content' in text
    assert 'Cover information' not in text and 'End section citation' not in text


def test_pdf_vector_not_found_by_parser_is_still_unverified(tmp_path, monkeypatch):
    from local_prepare import _native_paths
    _native_paths()
    import evidence
    def parse(source, redact, resource, issue):
        return ([{'id':'ab', 'type':'paragraph', 'text':'Abstract',
                  'position':{'page':1, 'bbox':[50,50,150,70]}},
                 {'id':'page', 'type':'paragraph', 'text':'10',
                  'position':{'page':2, 'bbox':[50,50,100,70]}}], [], [])
    monkeypatch.setattr(evidence, 'parse_pdf', parse)
    source = tmp_path / 'vector-unseen.pdf'
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((50, 50), 'Abstract')
    page = doc.new_page()
    page.insert_text((50, 50), '10')
    page.draw_line((100, 100), (500, 700))
    doc.save(source)
    doc.close()
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    result=prepare_source(source, sha(source), tmp_path / 'work', catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert material['readiness']['state']=='review_required'
    assert any(i['code'] in {'ocr_page_incomplete','pdf_visual_incomplete'} for i in material['issues'])


def test_pdf_inline_image_not_in_get_images_is_preserved(tmp_path):
    source = tmp_path / 'inline-image.pdf'
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((50, 50), 'Abstract')
    page.insert_text((50, 70), 'There is additional native research text here.')
    xref = page.get_contents()[0]
    stream = doc.xref_stream(xref)
    inline = b'\nq 100 0 0 100 50 300 cm BI /W 2 /H 2 /CS /RGB /BPC 8 ID ' + bytes([255,0,0]*4) + b' EI Q\n'
    doc.update_stream(xref, stream+inline)
    doc.save(source)
    doc.close()
    with fitz.open(source) as check:
        assert not check[0].get_images(full=True)
        assert any(b.get('type') == 1 for b in check[0].get_text('dict')['blocks'])
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    result = prepare_source(source, sha(source), tmp_path/'work', catalog)
    material = json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert any(a.get('kind') == 'image' and a.get('position', {}).get('page') == 1
               for a in material['assets'])


def test_pdf_displayed_image_is_held_even_if_textpage_does_not_report_it(tmp_path, monkeypatch):
    from PIL import Image
    import io
    source = tmp_path/'partial-image.pdf'
    im = Image.new('RGB', (20, 20), 'red')
    buf = io.BytesIO()
    im.save(buf, format='PNG')
    doc = fitz.open()
    page = doc.new_page(width=600, height=800)
    page.insert_text((50,50), 'Abstract')
    page.insert_image(fitz.Rect(-100,200,100,400), stream=buf.getvalue())
    doc.save(source)
    doc.close()
    real_get_text = fitz.Page.get_text
    def hide_image_blocks(page, option='text', *args, **kwargs):
        value = real_get_text(page, option, *args, **kwargs)
        if option == 'dict':
            value['blocks'] = [b for b in value['blocks'] if b.get('type') != 1]
        return value
    monkeypatch.setattr(fitz.Page, 'get_text', hide_image_blocks)
    catalog = tmp_path/'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    result=prepare_source(source, sha(source), tmp_path/'work', catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert any(i['code']=='pdf_visual_incomplete' for i in material['issues'])


def test_pdf_unrecognized_shading_is_held_for_visual_review(tmp_path, monkeypatch):
    source = tmp_path/'shading.pdf'
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((50,50), 'Abstract')
    page.insert_text((50,80), 'Research body.')
    doc.save(source)
    doc.close()
    original = fitz.Page.get_bboxlog
    def add_shading(page, *args, **kwargs):
        return original(page, *args, **kwargs) + [('fill-shade', (50,100,300,500))]
    monkeypatch.setattr(fitz.Page, 'get_bboxlog', add_shading)
    catalog = tmp_path/'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    result=prepare_source(source, sha(source), tmp_path/'work', catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert any(i['code']=='pdf_visual_incomplete' for i in material['issues'])


def test_old_loaded_request_module_is_not_reused(monkeypatch):
    import sys
    import types
    from local_prepare import _native_paths, PROGRAM
    old = types.ModuleType('preextract_request')
    old.__file__ = str(PROGRAM / 'desktop' / 'preextract_request.py')
    monkeypatch.setitem(sys.modules, 'preextract_request', old)
    with pytest.raises(RuntimeError, match='解析依赖'):
        _native_paths()


def test_nested_delivery_files_cannot_write_back_to_delivery(tmp_path):
    source = tmp_path / 'delivery' / 'files' / 'sub' / 'paper.txt'
    source.parent.mkdir(parents=True)
    source.write_text('摘要\n研究内容', encoding='utf8')
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    work = tmp_path / 'delivery' / 'derived'
    with pytest.raises(ValueError):
        prepare_source(source, sha(source), work, catalog)
    assert not work.exists()


def test_incomplete_work_with_request_history_is_preserved(tmp_path):
    source = tmp_path / 'paper.txt'
    source.write_text('摘要\n研究内容', encoding='utf8')
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    work = tmp_path / 'work'
    history = work / 'model' / 'request-state.json'
    history.parent.mkdir(parents=True)
    history.write_text('{"status":"unknown"}', encoding='utf8')
    with pytest.raises(ValueError):
        prepare_source(source, sha(source), work, catalog)
    assert history.read_text(encoding='utf8') == '{"status":"unknown"}'
    assert not (work / 'material.json').exists()


def test_cached_material_without_required_hashes_cannot_be_reused(tmp_path):
    source = tmp_path / 'paper.txt'
    source.write_text('摘要\n研究内容', encoding='utf8')
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    work = tmp_path / 'work'
    result = prepare_source(source, sha(source), work, catalog)
    manifest = Path(result['material_path'])
    bundle = json.loads(manifest.read_text(encoding='utf8'))
    bundle['file_hashes'] = {}
    manifest.write_text(json.dumps(bundle, ensure_ascii=False), encoding='utf8')
    with pytest.raises(ValueError):
        prepare_source(source, sha(source), work, catalog)


def test_previous_cleanup_version_is_not_reused_or_planned(tmp_path):
    from local_prepare import plan_clean_material
    from test_capacity_plan import config
    source = tmp_path/'paper.txt'; source.write_text('摘要\n研究内容', encoding='utf8')
    catalog = tmp_path/'catalog.json'; catalog.write_text('{"majors": []}', encoding='utf8')
    work = tmp_path/'work'
    result = prepare_source(source, sha(source), work, catalog)
    manifest = Path(result['material_path'])
    value = json.loads(manifest.read_text(encoding='utf8'))
    value['parser_version'] = 'candidate-direct-clean/14'
    value['cleanup']['version'] = 'reading-scope/14'
    manifest.write_text(json.dumps(value, ensure_ascii=False), encoding='utf8')
    refreshed = prepare_source(source, sha(source), work, catalog)
    assert Path(refreshed['material_path']) == work/'attempts'/'attempt-0001'/'material.json'
    with pytest.raises(ValueError, match='版本'):
        plan_clean_material(manifest, '模板', catalog, config(200000), tmp_path/'plan')
    assert json.loads(manifest.read_text(encoding='utf8'))['parser_version'] == 'candidate-direct-clean/14'


def test_repeated_pdf_margins_include_roman_page_numbers():
    from local_prepare import _native_paths
    _native_paths()
    from preextract_material import remove_repeated_margins
    blocks = []
    for page, number in enumerate(['I', 'II', 'III'], 1):
        for name, text, bbox in [('header', '毕业论文标题', [20, 69, 200, 88]),
                                 ('body', '正文保留', [20, 300, 200, 330]),
                                 ('page', number, [100, 915, 120, 930])]:
            blocks.append({'id': f'{name}{page}', 'type': 'paragraph', 'text': text,
                           'position': {'page': page, 'bbox': bbox}})
    kept, removed = remove_repeated_margins(blocks, {1:1000, 2:1000, 3:1000})
    assert [b['id'] for b in kept] == ['body1', 'body2', 'body3']
    assert len(removed) == 6


def test_repeated_pdf_margins_cannot_remove_a_reference_section_heading(tmp_path):
    from local_prepare import _native_paths
    _native_paths()
    from preextract_material import remove_repeated_margins
    headings=[{'id':f'ref{n}','type':'paragraph','text':'References',
               'position':{'page':n,'bbox':[200,40,400,60]}} for n in (2,3,4)]
    kept, removed=remove_repeated_margins(headings, {2:1000,3:1000,4:1000})
    assert [block['id'] for block in kept] == ['ref2','ref3','ref4']
    assert not removed

    source=tmp_path/'repeated-headers.pdf';doc=fitz.open()
    first=doc.new_page(width=600,height=800)
    first.insert_text((50,80),'Abstract');first.insert_text((50,120),'Research body content.')
    for n in (2,3,4):
        page=doc.new_page(width=600,height=800)
        page.insert_text((240,40),'References')
        page.insert_text((50,140),f'Excluded reference citation {n}.')
    doc.save(source);doc.close()
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    result=prepare_source(source,sha(source),tmp_path/'work',catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    body=Path(material['source']['text_file']).read_text(encoding='utf8')
    assert 'Research body content' in body
    assert 'References' not in body and 'Excluded reference citation' not in body


def test_repeated_tail_header_before_abstract_cannot_hide_later_references(tmp_path):
    source=tmp_path/'references-before-abstract.pdf';doc=fitz.open()
    front=doc.new_page(width=600,height=800);front.insert_text((240,40),'References')
    front.insert_text((50,120),'Cover.')
    body_page=doc.new_page(width=600,height=800)
    body_page.insert_text((50,100),'Abstract');body_page.insert_text((50,140),'Research body.')
    for n in (3,4,5):
        page=doc.new_page(width=600,height=800)
        page.insert_text((240,40),'References')
        page.insert_text((50,140),f'Excluded reference citation {n}.')
    doc.save(source);doc.close()
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    result=prepare_source(source,sha(source),tmp_path/'work',catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    body=Path(material['source']['text_file']).read_text(encoding='utf8')
    assert 'Research body' in body and 'Excluded reference citation' not in body


def test_office_conversion_is_temporary_and_does_not_leave_a_full_copy(tmp_path, monkeypatch):
    from local_prepare import _native_paths
    _native_paths()
    import direct_source
    import engine
    monkeypatch.setattr(direct_source, 'detect_format', lambda source: ('doc', 'fixture'))
    converted_paths = []
    def convert(_source, target):
        target = Path(target)
        target.mkdir(parents=True, exist_ok=True)
        result = target / 'converted.docx'
        doc = Document()
        doc.add_heading('摘要', 1)
        doc.add_paragraph('真实研究内容。')
        doc.add_heading('参考文献', 1)
        doc.add_paragraph('不交接的参考文献。')
        doc.save(result)
        converted_paths.append(result)
        return result
    monkeypatch.setattr(engine, 'convert_office', convert)
    source = tmp_path / 'paper.doc'
    source.write_bytes(b'fake original old office document')
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    work = tmp_path / 'work'
    result = prepare_source(source, sha(source), work, catalog)
    bundle = json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert converted_paths and not converted_paths[0].exists()
    assert not list(work.rglob('*.docx'))
    assert str(converted_paths[0]) not in bundle['file_hashes']
    assert bundle['source']['source_sha256'] == sha(source)
    assert bundle['source']['parsed_view']['basis'] == 'temporary_office_conversion'
    assert bundle['source']['parsed_view']['sha256']


def test_native_pdf_with_cover_image_does_not_block_body(tmp_path):
    import io
    from PIL import Image
    picture = io.BytesIO()
    Image.new('RGB', (30, 30), 'blue').save(picture, format='PNG')
    source = tmp_path / 'cover-image.pdf'
    doc = fitz.open()
    cover = doc.new_page(width=600, height=800)
    cover.insert_text((50, 50), 'Cover')
    cover.insert_image(fitz.Rect(50,100,80,130), stream=picture.getvalue())
    body = doc.new_page(width=600, height=800)
    body.insert_text((50, 50), 'Abstract')
    body.insert_text((50, 80), 'Research body with native text.')
    doc.save(source)
    doc.close()
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    result = prepare_source(source, sha(source), tmp_path / 'work', catalog)
    material = json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert any('Research body' in b.get('text', '') for b in material['blocks'])
    assert all(a.get('position', {}).get('page') != 1 for a in material['assets'])
    assert source.is_file()


def test_scanned_pdf_ocr_crops_mixed_scope_and_preserves_body_page(tmp_path, monkeypatch):
    from local_prepare import plan_clean_material
    from test_capacity_plan import config
    import io
    from PIL import Image
    import visual_ocr
    picture = io.BytesIO()
    scan_pixels=Image.new('RGB', (600, 800), 'white');scan_pixels.putpixel((300,400),(0,0,0))
    scan_pixels.save(picture, format='PNG')
    source = tmp_path / 'scan.pdf'
    doc = fitz.open()
    page = doc.new_page(width=600, height=800)
    page.insert_image(page.rect, stream=picture.getvalue())
    doc.save(source)
    doc.close()
    lines = [
        {'text': 'Cover private content', 'bbox': [50,30,300,50], 'confidence': 0.99},
        {'text': 'Abstract', 'bbox': [50,100,180,120], 'confidence': 0.99},
        {'text': 'Research body content.', 'bbox': [50,150,400,180], 'confidence': 0.99},
        {'text': 'References', 'bbox': [50,500,200,520], 'confidence': 0.99},
        {'text': 'Excluded citation', 'bbox': [50,550,250,570], 'confidence': 0.99},
    ]
    monkeypatch.setattr(visual_ocr, 'read_pdf_page', lambda page: lines)
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    result = prepare_source(source, sha(source), tmp_path / 'work', catalog)
    material = json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    text = Path(material['source']['text_file']).read_text(encoding='utf8')
    assert 'Research body content' in text
    assert 'Cover private content' not in text and 'Excluded citation' not in text
    crops = [a for a in material['assets'] if a.get('kind') == 'ocr_page_crop']
    assert len(crops) == 1
    assert crops[0]['position']['bbox'][1] >= 100
    assert crops[0]['position']['bbox'][3] <= 500
    assert any(issue['code'] == 'ocr_block_review' and issue['position'].get('block_id')
               for issue in material['issues'])
    assert not any(i['kind']=='visual_scope' for i in material['readiness']['items'])
    assert any(item['code'] == 'ocr_text_review' for item in material['issues'])
    plan=plan_clean_material(result['material_path'], '模板', catalog, config(200000), tmp_path / 'plan')['plan']
    assert any(part['asset_ids'] for part in plan['parts'])
    assert source.is_file()


def test_single_image_body_runs_ocr_without_permanent_source_copy(tmp_path, monkeypatch):
    from PIL import Image
    import visual_ocr
    from local_prepare import plan_clean_material
    from test_capacity_plan import config
    source = tmp_path/'body.png'
    scan_pixels=Image.new('RGB', (1200, 1600), 'white');scan_pixels.putpixel((600,800),(0,0,0));scan_pixels.save(source)
    lines = [{'text':'Cover information','bbox':[50,30,300,50],'confidence':.99},
             {'text':'Abstract','bbox':[50,100,180,120],'confidence':.99},
             {'text':'Research body content.','bbox':[50,150,400,180],'confidence':.99},
             {'text':'References','bbox':[50,500,200,520],'confidence':.99},
             {'text':'Excluded citation','bbox':[50,550,250,570],'confidence':.99}]
    monkeypatch.setattr(visual_ocr, 'read_pdf_page', lambda page: lines)
    monkeypatch.setattr(visual_ocr, 'read_image', lambda path: [])
    catalog = tmp_path/'catalog.json'; catalog.write_text('{"majors": []}', encoding='utf8')
    before = sha(source)
    result = prepare_source(source, before, tmp_path/'work', catalog)
    material = json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    text = Path(material['source']['text_file']).read_text(encoding='utf8')
    assert 'Research body content' in text
    assert 'Cover information' not in text and 'Excluded citation' not in text
    assert material['source']['parsed_view']['basis'] == 'original_image_memory_pages'
    assert material['source']['parsed_view']['page_mapping'][0]['pixels'] == [1200, 1600]
    assert not any(i['kind']=='visual_scope' for i in material['readiness']['items'])
    assert len([a for a in material['assets'] if a.get('kind') == 'ocr_page_crop']) == 1
    assert sha(source) == before
    assert all(sha(path) != before for path in (tmp_path/'work').rglob('*.png'))
    plan=plan_clean_material(result['material_path'], '模板', catalog, config(200000), tmp_path/'plan')['plan']
    assert any(part['asset_ids'] for part in plan['parts'])


@pytest.mark.parametrize('kind', ['transparent', 'exif_rotated'])
def test_single_image_ocr_respects_display_orientation_and_alpha(tmp_path, monkeypatch, kind):
    from PIL import Image
    import visual_ocr
    if kind == 'transparent':
        source = tmp_path/'scan.png'
        picture=Image.new('RGBA', (1200,1600), (255,0,0,0));picture.putpixel((600,800),(0,0,0,255));picture.save(source)
        expected_original = [1200,1600]
    else:
        source = tmp_path/'scan.jpg'
        picture = Image.new('RGB', (1600,1200), 'white')
        picture.putpixel((800,600),(0,0,0))
        exif = Image.Exif(); exif[274] = 6
        picture.save(source, exif=exif)
        expected_original = [1600,1200]
    seen = []
    def read(page):
        pix = page.get_pixmap(alpha=False, annots=False)
        seen.append((page.rect.width,page.rect.height,tuple(pix.samples[:3])))
        return [{'text':'Abstract','bbox':[50,100,180,120],'confidence':.99},
                {'text':'Research body.','bbox':[50,150,400,180],'confidence':.99}]
    monkeypatch.setattr(visual_ocr, 'read_pdf_page', read)
    monkeypatch.setattr(visual_ocr, 'read_image', lambda path: [])
    catalog = tmp_path/'catalog.json'; catalog.write_text('{"majors": []}', encoding='utf8')
    result = prepare_source(source, sha(source), tmp_path/'work', catalog)
    material = json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    mapping = material['source']['parsed_view']['page_mapping'][0]
    assert mapping['original_pixels'] == expected_original
    assert mapping['pixels'] == [1200,1600]
    assert mapping['exif_orientation'] == (6 if kind == 'exif_rotated' else 1)
    assert seen[0] == (600,800,(255,255,255))


def test_pdf_vector_region_in_body_is_retained_as_cropped_evidence(tmp_path, monkeypatch):
    from local_prepare import _native_paths
    _native_paths()
    import evidence
    def parse(source, redact, resource, issue):
        position = {'page': 2, 'bbox': [80, 100, 500, 500], 'vector_region': True}
        issue('pdf_vector_review', 'vector chart', position)
        return ([{'id': 'ab', 'type': 'paragraph', 'text': 'Abstract',
                  'position': {'page': 1, 'bbox': [50, 40, 150, 60]}},
                 {'id': 'fig', 'type': 'paragraph', 'text': '[vector chart]',
                  'unresolved_object': True, 'position': position}], [], [])
    monkeypatch.setattr(evidence, 'parse_pdf', parse)
    source = tmp_path / 'chart.pdf'
    doc = fitz.open()
    doc.new_page().insert_text((50, 50), 'Abstract')
    page = doc.new_page()
    page.draw_rect(fitz.Rect(100, 120, 400, 400))
    doc.save(source)
    doc.close()
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    result = prepare_source(source, sha(source), tmp_path / 'work', catalog)
    material = json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert any(a.get('kind') == 'pdf_vector_crop' and a['position']['page'] == 2
               for a in material['assets'])
    assert source.is_file()


def test_pdf_vector_crossing_excluded_heading_is_not_sent(tmp_path, monkeypatch):
    from local_prepare import _native_paths
    _native_paths()
    import evidence
    def parse(source, redact, resource, issue):
        pos = {'page': 1, 'bbox': [50, 300, 500, 600], 'vector_region': True}
        issue('pdf_vector_review', 'vector', pos)
        return ([{'id':'ab','type':'paragraph','text':'Abstract','position':{'page':1,'bbox':[50,50,150,70]}},
                 {'id':'fig','type':'paragraph','text':'[vector]','unresolved_object':True,'position':pos},
                 {'id':'ref','type':'paragraph','text':'References','position':{'page':1,'bbox':[50,500,200,520]}},
                 {'id':'entry','type':'paragraph','text':'excluded','position':{'page':1,'bbox':[50,540,200,560]}}], [], [])
    monkeypatch.setattr(evidence, 'parse_pdf', parse)
    source=tmp_path/'mixed.pdf'
    doc=fitz.open()
    page=doc.new_page()
    page.insert_text((50,50), 'Abstract')
    page.draw_rect(fitz.Rect(100,320,400,560))
    page.insert_text((50,500), 'References')
    doc.save(source)
    doc.close()
    catalog=tmp_path/'catalog.json'
    catalog.write_text('{"majors": []}',encoding='utf8')
    result=prepare_source(source,sha(source),tmp_path/'work',catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert any(i['code']=='pdf_visual_incomplete' for i in material['issues'])
    from local_prepare import plan_clean_material
    from test_capacity_plan import config
    plan=plan_clean_material(result['material_path'],'模板',catalog,config(200000),tmp_path/'plan')['plan']
    assert plan['source_completeness']=='partial'
    assert not any(part['asset_ids'] for part in plan['parts'])


def test_word_embedded_picture_is_recovered_from_unique_caption(tmp_path, monkeypatch):
    import io
    from PIL import Image
    from local_prepare import _native_paths
    _native_paths()
    import evidence_render
    from word_visual import recover_word_visuals
    figure = io.BytesIO()
    Image.new('RGB', (120, 80), 'blue').save(figure, format='PNG')
    pdf_path = tmp_path / 'rendered.pdf'
    pdf = fitz.open()
    page = pdf.new_page(width=600, height=800)
    page.insert_image(fitz.Rect(100, 150, 400, 350), stream=figure.getvalue())
    page.insert_text((100, 380), 'Figure 1 Example diagram')
    pdf.save(pdf_path)
    pdf.close()
    monkeypatch.setattr(evidence_render, 'render_office_pdf', lambda source, out: pdf_path)
    position = {'part': 'word/document.xml', 'body_index': 10, 'object_ref': 'object_0001'}
    kept = [{'id': 'object', 'type': 'paragraph', 'text': '[object]', 'position': position},
            {'id': 'caption', 'type': 'paragraph', 'text': 'Figure 1 Example diagram',
             'position': {'part': 'word/document.xml', 'body_index': 11}}]
    issues = [{'code': 'embedded_object_review', 'position': position, 'message': 'object'}]
    images=[]; captured=[]
    def resource(rid, blob, ext, kind, pos):
        captured.append((rid, kind, pos, blob))
        return {'id': rid}
    recover_word_visuals(tmp_path/'source.docx', tmp_path, kept, [], issues, resource, images)
    assert captured and captured[0][1] == 'docx_visual_crop'
    assert issues[0]['code'] == 'docx_visual_crop_review'
    assert any(row.get('resource_id') == captured[0][0] for row in kept)


def test_word_embedded_picture_with_nonunique_caption_stays_pending(tmp_path, monkeypatch):
    import evidence_render
    from word_visual import recover_word_visuals
    pdf_path=tmp_path/'duplicated.pdf'
    pdf=fitz.open()
    for _ in range(2):
        page=pdf.new_page()
        page.insert_text((100,380),'Figure 1 Example diagram')
    pdf.save(pdf_path);pdf.close()
    monkeypatch.setattr(evidence_render,'render_office_pdf',lambda source,out:pdf_path)
    pos={'part':'word/document.xml','body_index':10,'object_ref':'object_0001'}
    kept=[{'id':'object','type':'paragraph','text':'[object]','position':pos},
          {'id':'caption','type':'paragraph','text':'Figure 1 Example diagram',
           'position':{'part':'word/document.xml','body_index':11}}]
    issues=[{'code':'embedded_object_review','position':pos,'message':'object'}]
    recover_word_visuals(tmp_path/'source.docx',tmp_path,kept,[],issues,lambda *a:{'id':'bad'},[])
    assert issues[0]['code']=='embedded_object_review'


def test_pdf_vector_crop_removes_overlapping_footer_without_erasing_drawing(tmp_path):
    import hashlib
    from PIL import Image
    import io
    from pdf_visual import preserve_vectors
    source = tmp_path/'footer.pdf'
    doc = fitz.open()
    page = doc.new_page(width=600,height=800)
    page.draw_line((100,700),(500,700),width=2)
    page.insert_text((290,710),'18',fontsize=12)
    doc.save(source);doc.close()
    before = hashlib.sha256(source.read_bytes()).hexdigest()
    target={'id':'graph','type':'paragraph','text':'[graph]','unresolved_object':True,
            'position':{'page':1,'bbox':[80,650,520,712],'vector_region':True}}
    removed=[{'reason':'repeated_page_margin','block':{'id':'footer','type':'paragraph','text':'18',
             'position':{'page':1,'bbox':[289,698,306,715]}}}]
    issues=[{'code':'pdf_vector_review','position':target['position'].copy(),'message':'graph'}]
    kept=[target];images=[];captured=[]
    def resource(rid,blob,ext,kind,pos):
        captured.append((blob,pos));return {'id':rid}
    preserve_vectors(source,kept,removed,issues,resource,images)
    assert captured and captured[0][1]['suppressed_margin_boxes']
    with Image.open(io.BytesIO(captured[0][0])) as image:
        # The horizontal drawing remains visible through the removed page-number region.
        assert image.convert('RGB').getpixel((420,100)) != (255,255,255)
    assert hashlib.sha256(source.read_bytes()).hexdigest()==before


def test_body_picture_is_not_held_for_image_ocr(tmp_path, monkeypatch):
    import io
    from PIL import Image
    import visual_ocr
    monkeypatch.setattr(visual_ocr, 'read_image', lambda path: [{'text':'References', 'confidence':0.99}])
    picture = tmp_path/'picture.png'
    Image.new('RGB', (200, 100), 'white').save(picture)
    source = tmp_path/'with-picture.docx'
    doc=Document()
    doc.add_heading('摘要',1)
    doc.add_paragraph('论文研究内容。')
    doc.add_picture(str(picture))
    doc.save(source)
    catalog=tmp_path/'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    result = prepare_source(source,sha(source),tmp_path/'work',catalog)
    material = json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert not any(item['kind'] == 'visual_scope' for item in material['readiness']['items'])
    assert any(a.get('image_path') for a in material['assets'])


def test_body_picture_does_not_require_image_ocr(tmp_path, monkeypatch):
    from PIL import Image
    import visual_ocr
    def fail(path):raise RuntimeError('OCR unavailable')
    monkeypatch.setattr(visual_ocr, 'read_image', fail)
    picture=tmp_path/'picture.png'
    Image.new('RGB',(200,100),'white').save(picture)
    source=tmp_path/'picture.docx'
    doc=Document();doc.add_heading('摘要',1);doc.add_picture(str(picture));doc.save(source)
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    result = prepare_source(source,sha(source),tmp_path/'work',catalog)
    material = json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert not any(item['kind'] == 'visual_scope' for item in material['readiness']['items'])
    assert any(a.get('image_path') for a in material['assets'])


@pytest.mark.parametrize('reference_heading', [
    'References', '主要参考文献', '第六章 参考文献', '参考文献\n[1] An example citation.',
])
@pytest.mark.parametrize(('left_end','heading_start'), [(250,330), (320,330), (260,280)])
def test_multicolumn_boundary_cannot_silently_remove_left_column(tmp_path, monkeypatch, reference_heading, left_end, heading_start):
    from local_prepare import _native_paths
    _native_paths()
    import evidence
    def parse(source,redact,resource,issue):
        return ([{'id':'ab','type':'paragraph','text':'Abstract','position':{'page':1,'bbox':[40,20,180,40]}},
                 {'id':'left1','type':'paragraph','text':'Left body beginning.','position':{'page':1,'bbox':[40,60,250,90]}},
                 {'id':'ref','type':'paragraph','text':reference_heading,'position':{'page':1,'bbox':[heading_start,120,500,140]}},
                 {'id':'left2','type':'paragraph','text':'Left body continuation.','position':{'page':1,'bbox':[40,300,left_end,330]}}], [], [])
    monkeypatch.setattr(evidence,'parse_pdf',parse)
    source=tmp_path/'columns.pdf';doc=fitz.open();p=doc.new_page(width=600,height=800)
    p.insert_text((40,30),'Abstract');p.insert_text((40,70),'Left body beginning.')
    p.insert_text((330,130),'References');p.insert_text((40,310),'Left body continuation.')
    doc.save(source);doc.close()
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    from local_prepare import plan_clean_material
    from test_capacity_plan import config
    result = prepare_source(source,sha(source),tmp_path/'work',catalog)
    material = json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    issue = next(item for item in material['readiness']['items'] if item['kind'] == 'unresolved_boundary')
    members = issue['position']['members']
    assert any(m['source_block_id'] == 'left2' and m['disposition'] == 'excluded' for m in members)
    plan=plan_clean_material(result['material_path'], '模板', catalog, config(200000), tmp_path/'plan')['plan']
    assert plan['source_completeness']=='partial'
    assert 'Left body continuation.' not in json.dumps([p['body'] for p in plan['parts']],ensure_ascii=False)
    from test_capacity_plan import synthetic_review
    review = synthetic_review(result['material_path'], tmp_path/'review.json')
    with pytest.raises(ValueError, match='待核'):
        plan_clean_material(result['material_path'], '模板', catalog, config(200000), tmp_path/'wrong-review', review_path=review)


def test_multicolumn_possible_tail_in_kept_text_is_separately_flagged(tmp_path, monkeypatch):
    from local_prepare import _native_paths, plan_clean_material
    from test_capacity_plan import config
    _native_paths()
    import evidence
    def parse(source,redact,resource,issue):
        return ([{'id':'ab','type':'paragraph','text':'Abstract','position':{'page':1,'bbox':[40,20,180,40]}},
                 {'id':'right','type':'paragraph','text':'Possibly excluded tail.','position':{'page':1,'bbox':[350,100,500,120]}},
                 {'id':'ref','type':'paragraph','text':'References','position':{'page':1,'bbox':[40,200,200,220]}},
                 {'id':'entry','type':'paragraph','text':'Citation.','position':{'page':1,'bbox':[40,240,200,260]}}], [], [])
    monkeypatch.setattr(evidence, 'parse_pdf', parse)
    source=tmp_path/'columns.pdf'; doc=fitz.open(); p=doc.new_page(width=600,height=800)
    p.insert_text((40,30),'Abstract'); doc.save(source); doc.close()
    catalog=tmp_path/'catalog.json'; catalog.write_text('{"majors": []}',encoding='utf8')
    result=prepare_source(source,sha(source),tmp_path/'work',catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    item=next(item for item in material['readiness']['items'] if item['kind']=='column_scope_review')
    assert any(m['source_block_id']=='right' and m['disposition']=='retained' for m in item['position']['members'])
    plan=plan_clean_material(result['material_path'],'模板',catalog,config(200000),tmp_path/'plan')['plan']
    assert plan['source_completeness']=='partial'
    content = json.dumps([p['body'] for p in plan['parts']],ensure_ascii=False)
    assert 'Possibly excluded tail.' in content and 'Citation.' not in content
    assert any(g['code'] == 'pdf_column_kept_review' for g in plan['reading_gaps'])


@pytest.mark.parametrize('kind', ['table', 'image', 'unresolved_object'])
@pytest.mark.parametrize(('left_end','heading_start'), [(250,330), (310,280), (500,280)])
def test_multicolumn_removed_research_objects_are_reviewed(tmp_path, kind, left_end, heading_start):
    from pdf_visual import guard_scope_columns
    source=tmp_path/'page.pdf'; doc=fitz.open();doc.new_page(width=600,height=800);doc.save(source);doc.close()
    heading={'id':'ref','type':'paragraph','text':'References','position':{'page':1,'bbox':[heading_start,120,500,140]}}
    obj={'id':'research-object','type':kind,'position':{'page':1,'bbox':[40,300,left_end,500]},
         'resource_id':None if kind == 'table' else 'asset-1',
         'screenshot_ids':['table-screenshot-1'] if kind == 'table' else None}
    issues=[]
    guard_scope_columns(source,[heading,obj],[],[{'reason':'references','block':heading},
                                                {'reason':'references','block':obj}],issues)
    member=issues[0]['position']['members'][0]
    assert member['source_block_id']=='research-object' and member['type']==kind
    assert member['disposition']=='excluded'
    if kind == 'table':assert member['screenshot_ids'] == ['table-screenshot-1']
    else:assert member['resource_id']=='asset-1'


def test_multicolumn_review_reports_exact_scope_slices(tmp_path):
    from pdf_visual import guard_scope_columns
    source=tmp_path/'page.pdf';doc=fitz.open();doc.new_page(width=600,height=800);doc.save(source);doc.close()
    heading={'id':'ref','type':'paragraph','text':'References','position':{'page':1,'bbox':[330,120,500,140]}}
    raw={'id':'mixed','type':'paragraph','text':'研究正文\n参考文献\n条目','position':{'page':1,'bbox':[40,300,250,330]}}
    kept={**raw,'id':'part-kept','text':'研究正文\n','source_scope':{'block_id':'mixed','start':0,'end':5}}
    removed={**raw,'id':'part-removed','text':'参考文献\n条目',
             'source_scope':{'block_id':'mixed','start':5,'end':len(raw['text'])}}
    issues=[]
    guard_scope_columns(source,[heading,raw],[kept],[{'reason':'references','block':heading},
                                                  {'reason':'references','block':removed}],issues)
    member=next(m for i in issues for m in i['position']['members'] if m['source_block_id']=='mixed')
    assert (member['source_start'],member['source_end'],member['disposition']) == (5,len(raw['text']),'excluded')


def test_scan_ocr_pixels_do_not_include_pdf_annotations(tmp_path, monkeypatch):
    import numpy as np
    import visual_ocr
    source=tmp_path/'annotated.pdf';doc=fitz.open();page=doc.new_page(width=200,height=200)
    page.add_freetext_annot(fitz.Rect(20,20,160,60),'References',fontsize=14)
    doc.save(source);doc.close()
    seen=[]
    def engine(image):seen.append(image.copy());return [],[]
    monkeypatch.setattr(visual_ocr,'_engine',lambda:engine)
    with fitz.open(source) as pdf:
        page=pdf[0]
        expected=page.get_pixmap(matrix=fitz.Matrix(2,2),alpha=False,annots=False)
        visual_ocr.read_pdf_page(page)
    assert np.array_equal(seen[0],np.frombuffer(expected.samples,dtype=np.uint8).reshape(expected.height,expected.width,expected.n))


def test_office_conversion_never_uses_outer_persistent_conversion_cache(tmp_path,monkeypatch):
    from local_prepare import _native_paths
    _native_paths()
    import engine
    import material_conversion
    from direct_source import prepare_readable_source
    source=tmp_path/'old.doc';source.write_bytes(b'synthetic source')
    observed=[]
    def convert(source,target):
        observed.append(material_conversion.current())
        out=Path(target);out.mkdir(parents=True,exist_ok=True)
        converted=out/'converted.docx';converted.write_bytes(b'synthetic conversion')
        return converted
    monkeypatch.setattr(engine,'convert_office',convert)
    with material_conversion.preparation_context(tmp_path/'persistent-cache'):
        previous=material_conversion.current()
        prepare_readable_source(source,tmp_path,'doc',tmp_path/'temporary')
        assert material_conversion.current() is previous
    assert observed == [None]
    assert not (tmp_path/'persistent-cache').exists()


def test_pdf_annotation_is_recorded_without_contaminating_body_text(tmp_path):
    source=tmp_path/'annotated.pdf';doc=fitz.open();page=doc.new_page()
    page.insert_text((50,50),'Abstract');page.insert_text((50,80),'Native research body.')
    page.add_freetext_annot(fitz.Rect(50,150,250,200),'References',fontsize=14)
    doc.save(source);doc.close()
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    result=prepare_source(source,sha(source),tmp_path/'work',catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert material['readiness']['state']=='review_required'
    assert any(i['kind']=='pdf_annotation_review' for i in material['readiness']['items'])
    assert 'References' not in Path(material['source']['text_file']).read_text(encoding='utf8')


def test_shared_work_directory_serializes_direct_source_preparation(tmp_path,monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    import threading,time
    from local_prepare import _native_paths
    _native_paths()
    import preextract_material
    original=preextract_material.prepare_material
    state={'active':0,'maximum':0};guard=threading.Lock()
    def slow(*args,**kwargs):
        with guard:
            state['active']+=1;state['maximum']=max(state['maximum'],state['active'])
        try:
            time.sleep(.2)
            return original(*args,**kwargs)
        finally:
            with guard:state['active']-=1
    monkeypatch.setattr(preextract_material,'prepare_material',slow)
    source=tmp_path/'paper.txt';source.write_text('摘要\n研究正文。',encoding='utf8')
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    def run():return prepare_source(source,sha(source),tmp_path/'work',catalog)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(lambda _:run(),range(2)))
    assert state['maximum']==1
    assert results[0]['material_path']==results[1]['material_path']


def test_unresolved_word_visual_keeps_local_body_but_blocks_handoff(tmp_path,monkeypatch):
    from local_prepare import _native_paths,plan_clean_material
    from test_capacity_plan import config
    _native_paths()
    import native_docx
    def parse(source,reading,resource,issue):
        pos={'part':'word/document.xml','body_index':2,'object_ref':'object_1'}
        issue('embedded_object_review','unresolved',pos)
        return ([{'id':'ab','type':'paragraph','text':'摘要','heading_level':1,'position':{'part':'word/document.xml','body_index':0}},
                 {'id':'body','type':'paragraph','text':'保留的正文','position':{'part':'word/document.xml','body_index':1}},
                 {'id':'obj','type':'paragraph','text':'[待核图形]','position':pos}], [], [])
    monkeypatch.setattr(native_docx,'parse_docx',parse)
    source=tmp_path/'body.docx';doc=Document();doc.add_heading('摘要',1);doc.save(source)
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    result=prepare_source(source,sha(source),tmp_path/'work',catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert any('保留的正文' in b.get('text','') for b in material['blocks'])
    assert any(i['kind']=='unresolved_visual' for i in material['readiness']['items'])
    plan=plan_clean_material(result['material_path'],'模板',catalog,config(200000),tmp_path/'plan')['plan']
    assert plan['source_completeness']=='partial'
    assert '保留的正文' in json.dumps([p['body'] for p in plan['parts']],ensure_ascii=False)
