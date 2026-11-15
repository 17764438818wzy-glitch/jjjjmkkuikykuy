import hashlib
import json
from pathlib import Path

from docx import Document
from PIL import Image

from local_prepare import plan_clean_material, prepare_source
from reading_cleanup import clean_body_blocks
from review_requirements import build_readiness


def paragraph(name, text, heading=0):
    return {'id': name, 'type': 'paragraph', 'text': text,
            'heading_level': heading, 'position': {'body_index': len(name)}}


def settings():
    return {'model': 'fixture', 'context_window': 200000, 'max_tokens': 500,
            'max_output_tokens': 500, 'context_reserve_tokens': 200,
            'image_token_upper_bound': 2048, 'max_images': 100,
            'max_total_image_bytes': 2000000, 'max_single_image_bytes': 5000000,
            'max_image_side': 2000, 'max_request_bytes': 8000000,
            'capacity_source': 'synthetic offline fixture'}


def test_appendix_after_references_preserves_objects_and_following_ack_is_removed():
    rows = [paragraph('cover', '封面内容'), paragraph('ab', '摘要', 1),
            paragraph('body', '正文引用[1]。'), paragraph('refs', '参考文献', 1),
            paragraph('ref', '[1] 文献条目'), paragraph('appendix', '附录 A 问卷', 1),
            paragraph('chapter', '第1章 附录问卷', 1), paragraph('text', '附录数据。'),
            {'id': 'table', 'type': 'table', 'cells': [{'text': '附录表格'}],
             'position': {'body_index': 8}},
            {'id': 'image', 'type': 'image', 'resource_id': 'picture',
             'position': {'body_index': 9}},
            paragraph('ack', '致谢', 1), paragraph('thanks', '感谢导师')]
    kept, removed, _ = clean_body_blocks(rows)
    assert [b['id'] for b in kept] == ['ab', 'body', 'appendix', 'chapter', 'text', 'table', 'image']
    assert {r['block']['id'] for r in removed} == {'cover', 'refs', 'ref', 'ack', 'thanks'}


def test_appendix_after_ack_and_mixed_reference_block_are_retained():
    rows = [paragraph('ab', '摘要', 1), paragraph('body', '正文。'),
            paragraph('mixed', '参考文献\n[1] 文献条目\n附录 A\n附录文本', 1),
            paragraph('ack', '致谢', 1), paragraph('thanks', '感谢导师'),
            paragraph('appendix_b', '附录 B', 1), paragraph('more', '第二个附录')]
    kept, removed, _ = clean_body_blocks(rows)
    keep_text = '\n'.join(b.get('text', '') for b in kept)
    removed_text = '\n'.join(r['block'].get('text', '') for r in removed)
    assert '附录 A' in keep_text and '附录文本' in keep_text
    assert '附录 B' in keep_text and '第二个附录' in keep_text
    assert '[1] 文献条目' in removed_text and '感谢导师' in removed_text


def test_positioned_images_need_no_ocr_text_or_recognized_picture_subtype():
    assets = [{'id': str(i), 'kind': kind, 'image_path': 'extracted.png',
               'block_ids': ['b'], 'section_ids': ['s'],
               'position': {'body_index': i},
               'image_scope_audit': {'status': status, 'detected_lines': count}}
              for i, (kind, status, count) in enumerate([
                  ('image', 'ocr_checked_unverified', 0),
                  ('table_screenshot', 'ocr_checked_unverified', 4),
                  ('image', 'ocr_unavailable', None),
                  ('image', 'excluded_heading_detected', 1)])]
    readiness = build_readiness(assets, [], {})
    assert not [item for item in readiness['items'] if item['kind'] == 'visual_scope']


def test_offline_word_plan_keeps_appendix_and_images_without_ocr(tmp_path, monkeypatch):
    import visual_ocr
    monkeypatch.setattr(visual_ocr, 'read_image', lambda path: [])
    body_image = tmp_path / 'body.png'
    appendix_image = tmp_path / 'appendix.png'
    Image.new('RGB', (80, 60), 'blue').save(body_image)
    Image.new('RGB', (80, 60), 'red').save(appendix_image)
    source = tmp_path / 'paper.docx'
    doc = Document()
    doc.add_paragraph('封面私有内容')
    doc.add_heading('摘要', 1)
    doc.add_paragraph('研究正文。')
    doc.add_picture(str(body_image))
    doc.add_heading('参考文献', 1)
    doc.add_paragraph('[1] 文献条目')
    doc.add_heading('附录 A', 1)
    doc.add_paragraph('附录数据。')
    doc.add_picture(str(appendix_image))
    doc.add_table(rows=1, cols=1).cell(0, 0).text = '附录表格数据'
    doc.add_heading('致谢', 1)
    doc.add_paragraph('感谢导师')
    doc.save(source)
    original = source.read_bytes()
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    prepared = prepare_source(source, hashlib.sha256(original).hexdigest(), tmp_path / 'work', catalog)
    material = json.loads(Path(prepared['material_path']).read_text(encoding='utf8'))
    result = plan_clean_material(prepared['material_path'], '模板', catalog, settings(), tmp_path / 'plan')
    plan = result['plan']
    body = json.dumps([part['body'] for part in plan['parts']], ensure_ascii=False)
    assert '研究正文' in body and '附录数据' in body and '附录表格数据' in body
    assert '封面私有内容' not in body and '文献条目' not in body and '感谢导师' not in body
    image_assets = [a for a in material['assets'] if a.get('image_path')]
    image_ids = {a['id'] for a in image_assets}
    planned_ids = {asset_id for part in plan['parts'] for asset_id in part['asset_ids']}
    assert len(image_ids) == 2 and image_ids <= planned_ids
    assert all(a['block_ids'] and a['section_ids'] for a in image_assets)
    assert body.count('image_url') >= 2
    assert source.read_bytes() == original


def test_missing_object_does_not_withhold_another_located_image_and_is_recorded(tmp_path, monkeypatch):
    from local_prepare import _native_paths
    _native_paths()
    import native_docx

    original_parse = native_docx.parse_docx
    def parse_with_missing_object(source, redact, resource, issue):
        blocks, images, formulas = original_parse(source, redact, resource, issue)
        position = images[0]['position']
        issue('image_missing', '同一段落的另一幅图片无法提取', position)
        return blocks, images, formulas
    monkeypatch.setattr(native_docx, 'parse_docx', parse_with_missing_object)
    image = tmp_path / 'available.png'
    Image.new('RGB', (80, 60), 'blue').save(image)
    source = tmp_path / 'paper.docx'
    doc = Document()
    doc.add_heading('摘要', 1)
    doc.add_paragraph('缺图前的正文。')
    doc.add_picture(str(image))
    doc.add_paragraph('缺图后的正文。')
    doc.save(source)
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    prepared = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path / 'work', catalog)
    material = json.loads(Path(prepared['material_path']).read_text(encoding='utf8'))
    plan = plan_clean_material(prepared['material_path'], '模板', catalog, settings(), tmp_path / 'plan')['plan']
    image_ids = {a['id'] for a in material['assets'] if a.get('image_path')}
    planned_ids = {asset_id for part in plan['parts'] for asset_id in part['asset_ids']}
    text = json.dumps([p['body'] for p in plan['parts']], ensure_ascii=False)
    assert image_ids and image_ids <= planned_ids
    assert '缺图前的正文' in text and '缺图后的正文' in text
    assert any(g['code'] == 'image_missing' for g in plan['reading_gaps'])


def test_located_image_is_not_withheld_for_a_pdf_clip_warning(tmp_path, monkeypatch):
    import partial_projection
    source = tmp_path / 'paper.docx'
    image = tmp_path / 'image.png'
    Image.new('RGB', (80, 60), 'blue').save(image)
    doc = Document()
    doc.add_heading('摘要', 1)
    doc.add_paragraph('正文。')
    doc.add_picture(str(image))
    doc.save(source)
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    prepared = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path / 'work', catalog)
    monkeypatch.setattr(partial_projection, 'pdf_image_clip_requirements', lambda material: [
        {'id': 'clip', 'kind': 'visual_scope', 'asset_id': material['assets'][0]['id'],
         'position': material['assets'][0]['position'], 'description': '可见范围需核对'}])
    plan = plan_clean_material(prepared['material_path'], '模板', catalog, settings(), tmp_path / 'plan')['plan']
    assert any(part['asset_ids'] for part in plan['parts'])


def test_ambiguous_table_label_does_not_drop_later_content(tmp_path):
    source = tmp_path / 'paper.docx'
    doc = Document()
    doc.add_heading('摘要', 1)
    doc.add_paragraph('前面的正文。')
    doc.add_table(rows=1, cols=1).cell(0, 0).text = '参考文献'
    doc.add_paragraph('后面的正文继续保留。')
    doc.save(source)
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    prepared = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path / 'work', catalog)
    plan = plan_clean_material(prepared['material_path'], '模板', catalog, settings(), tmp_path / 'plan')['plan']
    content = json.dumps([p['body'] for p in plan['parts']], ensure_ascii=False)
    assert '前面的正文' in content and '后面的正文继续保留' in content


def test_unassociated_image_records_gap_instead_of_stopping_other_material(tmp_path, monkeypatch):
    from local_prepare import _native_paths
    _native_paths()
    import io
    import native_docx

    original_parse = native_docx.parse_docx
    def parse_with_unassociated_image(source, redact, resource, issue):
        result = original_parse(source, redact, resource, issue)
        pixels = io.BytesIO()
        Image.new('RGB', (20, 20), 'blue').save(pixels, format='PNG')
        resource('unassociated', pixels.getvalue(), '.png', 'image',
                 {'part': 'word/document.xml', 'body_index': 999})
        return result
    monkeypatch.setattr(native_docx, 'parse_docx', parse_with_unassociated_image)
    source = tmp_path / 'paper.docx'
    doc = Document()
    doc.add_paragraph('封面')
    doc.add_heading('摘要', 1)
    doc.add_paragraph('正文仍然可以处理。')
    doc.save(source)
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    prepared = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path / 'work', catalog)
    plan = plan_clean_material(prepared['material_path'], '模板', catalog, settings(), tmp_path / 'plan')['plan']
    content = json.dumps([p['body'] for p in plan['parts']], ensure_ascii=False)
    assert '正文仍然可以处理' in content and '封面' not in content
    assert any(g['code'] == 'image_position_unresolved' for g in plan['reading_gaps'])


def test_one_word_paragraph_splits_cover_and_references_without_losing_appendix_images(tmp_path):
    pictures = []
    for color in ('green', 'blue', 'red'):
        path = tmp_path / (color + '.png')
        Image.new('RGB', (60, 60), color).save(path)
        pictures.append(path)
    source = tmp_path / 'mixed.docx'
    doc = Document()
    paragraph = doc.add_paragraph('封面图片')
    paragraph.add_run().add_picture(str(pictures[0]))
    paragraph.add_run('\n摘要\n研究正文 $x+1$。')
    paragraph.add_run().add_picture(str(pictures[1]))
    paragraph.add_run('\n参考文献\n[1] 不应保留的文献 $y+2$。\n附录 A\n附录内容 $z+3$。')
    paragraph.add_run().add_picture(str(pictures[2]))
    doc.add_paragraph('附录后续文字继续保留。')
    doc.save(source)
    original = source.read_bytes()
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    prepared = prepare_source(source, hashlib.sha256(original).hexdigest(), tmp_path / 'work', catalog)
    material = json.loads(Path(prepared['material_path']).read_text(encoding='utf8'))
    plan = plan_clean_material(prepared['material_path'], '模板', catalog, settings(), tmp_path / 'plan')['plan']
    body = json.dumps([p['body'] for p in plan['parts']], ensure_ascii=False)
    assert '研究正文' in body and '附录内容' in body and '附录后续文字继续保留' in body
    assert '$x+1$' in body and '$z+3$' in body and '$y+2$' not in body
    assert '封面图片' not in body and '不应保留的文献' not in body
    retained = [a for a in material['assets'] if a.get('image_path')]
    assert len(retained) == 2
    blocks = {b['id']: b for b in material['blocks']}
    for image in retained:
        assert any('[[IMAGE:' + image['id'] + ']]' in blocks[bid]['text'] for bid in image['block_ids'])
    assert {a['id'] for a in retained} <= {i for p in plan['parts'] for i in p['asset_ids']}
    assert source.read_bytes() == original


def test_clear_reference_table_is_excluded_without_losing_later_appendix():
    rows = [paragraph('ab', '摘要', 1), paragraph('body', '正文。'),
            {'id': 'refs', 'type': 'table', 'position': {'body_index': 3},
             'cells': [{'text': '参考文献'}, {'text': '[1] 应排除的文献'}]},
            paragraph('appendix', '附录 A', 1), paragraph('after', '附录内容。')]
    kept, removed, _ = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert {'ab', 'body', 'appendix', 'after'} <= {r['id'] for r in kept}
    assert 'refs' in {r['block']['id'] for r in removed}


def test_appendix_text_with_numbers_is_not_mistaken_for_a_toc():
    rows = [paragraph('ab', '摘要', 1), paragraph('body', '正文。'),
            paragraph('appendix', '附录 A', 1), paragraph('toc_word', '目录'),
            paragraph('sample1', '样品 A  1'), paragraph('sample2', '样品 B  2'),
            paragraph('after', '以上为测试所需材料。')]
    kept, _, _ = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert {'sample1', 'sample2', 'after'} <= {r['id'] for r in kept}


def test_native_word_formulas_follow_their_scope_within_one_paragraph(tmp_path):
    from docx.oxml import OxmlElement
    source = tmp_path / 'formula.docx'
    doc = Document()
    paragraph = doc.add_paragraph('摘要\n正文公式 ')
    def add_formula(value):
        math = OxmlElement('m:oMath')
        run = OxmlElement('m:r')
        text = OxmlElement('m:t')
        text.text = value
        run.append(text)
        math.append(run)
        paragraph._p.append(math)
    add_formula('x+1')
    paragraph.add_run('\n参考文献\n[1] 引用公式 ')
    add_formula('y+2')
    paragraph.add_run('\n附录 A\n附录公式 ')
    add_formula('z+3')
    doc.save(source)
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    prepared = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path / 'work', catalog)
    material = json.loads(Path(prepared['material_path']).read_text(encoding='utf8'))
    plan = plan_clean_material(prepared['material_path'], '模板', catalog, settings(), tmp_path / 'plan')['plan']
    body = json.dumps([p['body'] for p in plan['parts']], ensure_ascii=False)
    assert {f['latex'] for f in material['formulas']} == {'x+1', 'z+3'}
    assert 'x+1' in body and 'z+3' in body and 'y+2' not in body
    view = json.loads(plan['parts'][0]['body']['messages'][1]['content'][0]['text'])['material']
    by_id = {block['id']: block for block in view['blocks']}
    for formula in view['formulas']:
        assert '$' + formula['latex'] + '$' in by_id[formula['block_id']]['text']


def test_unmarked_image_in_split_source_records_gap_without_guessing_its_scope(tmp_path, monkeypatch):
    from local_prepare import _native_paths
    _native_paths()
    import io
    import native_docx
    original_parse = native_docx.parse_docx
    def parse_with_unmarked_resource(source, redact, resource, issue):
        blocks, images, formulas = original_parse(source, redact, resource, issue)
        pixels = io.BytesIO()
        Image.new('RGB', (20, 20), 'blue').save(pixels, format='PNG')
        resource('unknown_image', pixels.getvalue(), '.png', 'image', blocks[0]['position'])
        blocks[0]['resource_ids'] = ['unknown_image']
        return blocks, images, formulas
    monkeypatch.setattr(native_docx, 'parse_docx', parse_with_unmarked_resource)
    source = tmp_path / 'mixed.docx'
    doc = Document()
    doc.add_paragraph('封面\n摘要\n正文继续。')
    doc.save(source)
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    prepared = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path / 'work', catalog)
    material = json.loads(Path(prepared['material_path']).read_text(encoding='utf8'))
    plan = plan_clean_material(prepared['material_path'], '模板', catalog, settings(), tmp_path / 'plan')['plan']
    assert 'unknown_image' not in {a['id'] for a in material['assets']}
    assert any(g['code'] == 'image_position_unresolved' for g in plan['reading_gaps'])
    assert '正文继续' in json.dumps([p['body'] for p in plan['parts']], ensure_ascii=False)


def test_clipped_pdf_image_uses_visible_page_region_and_keeps_its_position(tmp_path):
    import io
    import fitz
    from PIL import ImageDraw
    picture = Image.new('RGB', (200, 400), 'blue')
    ImageDraw.Draw(picture).rectangle([0, 0, 199, 19], fill='red')
    image_bytes = io.BytesIO()
    picture.save(image_bytes, format='PNG')
    source = tmp_path / 'clipped.pdf'
    pdf = fitz.open()
    first = pdf.new_page(width=600, height=800)
    first.insert_text((40, 60), 'Abstract')
    first.insert_text((40, 90), 'Body text on the first page.')
    second = pdf.new_page(width=600, height=800)
    second.insert_image(fitz.Rect(50, -20, 250, 380), stream=image_bytes.getvalue())
    second.insert_text((40, 430), 'Body text after the figure.')
    pdf.save(source)
    pdf.close()
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    prepared = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path / 'work', catalog)
    material = json.loads(Path(prepared['material_path']).read_text(encoding='utf8'))
    image = next(a for a in material['assets'] if a.get('image_path'))
    with Image.open(image['image_path']) as visible:
        colors = list(visible.convert('RGB').getdata())
    assert any(b > 200 and r < 80 for r, g, b in colors)
    assert not any(r > 200 and b < 80 for r, g, b in colors)
    assert image['position']['page'] == 2 and image['block_ids']
    plan = plan_clean_material(prepared['material_path'], '模板', catalog, settings(), tmp_path / 'plan')['plan']
    assert image['id'] in {asset_id for part in plan['parts'] for asset_id in part['asset_ids']}


def test_word_vector_with_unique_caption_uses_page_picture_when_needed(tmp_path, monkeypatch):
    import fitz
    from local_prepare import _native_paths
    _native_paths()
    import evidence_render
    from word_visual import recover_word_visuals
    rendered = tmp_path / 'rendered.pdf'
    pdf = fitz.open()
    page = pdf.new_page(width=600, height=800)
    page.draw_rect(fitz.Rect(80, 100, 400, 300), color=(0, 0, 1))
    page.insert_text((80, 330), 'Figure 1 Example diagram')
    pdf.save(rendered)
    pdf.close()
    monkeypatch.setattr(evidence_render, 'render_office_pdf', lambda *args: rendered)
    position = {'part': 'word/document.xml', 'body_index': 10, 'object_ref': 'object_0001'}
    kept = [{'id': 'object', 'type': 'paragraph', 'position': position},
            {'id': 'caption', 'type': 'paragraph', 'text': 'Figure 1 Example diagram',
             'position': {'part': 'word/document.xml', 'body_index': 11}}]
    issues = [{'code': 'embedded_object_review', 'position': position, 'message': '待提取图形'}]
    images, captured = [], []
    def resource(rid, data, extension, kind, pos):
        captured.append((rid, data, kind, pos))
        return {'id': rid}
    recover_word_visuals(tmp_path / 'unused.docx', tmp_path, kept, [], issues, resource, images)
    assert captured and captured[0][3]['body_index'] == 10
    assert captured[0][3]['page_fallback'] is True
    assert any(row.get('resource_id') == captured[0][0] for row in kept)


def test_a_heading_like_line_inside_formula_does_not_cut_the_formula():
    rows = [paragraph('mixed', '摘要\n公式 $a\n参考文献\nb$ 仍是正文。\n参考文献\n[1] 条目\n附录 A\n附录正文', 1)]
    kept, removed, _ = clean_body_blocks(rows, continue_on_uncertainty=True)
    text = '\n'.join(b['text'] for b in kept)
    assert '$a\n参考文献\nb$' in text and '附录正文' in text
    assert '[1] 条目' not in text
    assert any('[1] 条目' in r['block']['text'] for r in removed)


def test_clear_chapter_after_references_does_not_hide_later_text():
    rows = [paragraph('ab', '摘要', 1), paragraph('body', '正文。'),
            paragraph('ref', '参考文献', 1), paragraph('entry', '[1] 文献'),
            paragraph('chapter', '第2章 实验', 1), paragraph('after', '后续实验正文。')]
    kept, removed, _ = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert {'chapter', 'after'} <= {b['id'] for b in kept}
    assert {'ref', 'entry'} <= {r['block']['id'] for r in removed}


def test_word_page_fallback_does_not_include_a_reference_continuing_from_previous_page(tmp_path, monkeypatch):
    import fitz
    from local_prepare import _native_paths
    _native_paths()
    import evidence_render
    from word_visual import recover_word_visuals
    rendered = tmp_path / 'mixed-page.pdf'
    pdf = fitz.open()
    pdf.new_page().insert_text((50, 50), 'References')
    page = pdf.new_page()
    page.insert_text((50, 80), '[1] Earlier citation continues here.')
    page.draw_rect(fitz.Rect(80, 150, 400, 300), color=(0, 0, 1))
    page.insert_text((80, 330), 'Figure 1 Example diagram')
    pdf.save(rendered)
    pdf.close()
    monkeypatch.setattr(evidence_render, 'render_office_pdf', lambda *args: rendered)
    position = {'part': 'word/document.xml', 'body_index': 10, 'object_ref': 'object_0001'}
    kept = [{'id': 'object', 'type': 'paragraph', 'position': position},
            {'id': 'caption', 'type': 'paragraph', 'text': 'Figure 1 Example diagram',
             'position': {'part': 'word/document.xml', 'body_index': 11}}]
    removed = [{'reason': 'references', 'block': {'type': 'paragraph',
                'text': '[1] Earlier citation continues here.',
                'position': {'part': 'word/document.xml', 'body_index': 8}}}]
    issues = [{'code': 'embedded_object_review', 'position': position, 'message': '待提取图形'}]
    captured = []
    recover_word_visuals(tmp_path / 'unused.docx', tmp_path, kept, removed, issues,
                         lambda *args: captured.append(args), [])
    assert not captured
    assert issues[0]['code'] == 'embedded_object_review'


def test_word_page_fallback_does_not_guess_about_a_short_reference_tail(tmp_path, monkeypatch):
    import fitz
    from local_prepare import _native_paths
    _native_paths()
    import evidence_render
    from word_visual import recover_word_visuals
    rendered = tmp_path / 'mixed-page.pdf'
    pdf = fitz.open()
    pdf.new_page().insert_text((50, 50), 'References')
    page = pdf.new_page()
    page.insert_text((50, 80), 'RIGHTTAIL')
    page.draw_rect(fitz.Rect(80, 150, 400, 300), color=(0, 0, 1))
    page.insert_text((80, 330), 'Figure 1 Example diagram')
    pdf.save(rendered)
    pdf.close()
    monkeypatch.setattr(evidence_render, 'render_office_pdf', lambda *args: rendered)
    position = {'part': 'word/document.xml', 'body_index': 10, 'object_ref': 'object_0001'}
    kept = [{'id': 'object', 'type': 'paragraph', 'position': position},
            {'id': 'caption', 'type': 'paragraph', 'text': 'Figure 1 Example diagram',
             'position': {'part': 'word/document.xml', 'body_index': 11}}]
    removed = [{'reason': 'references', 'block': {'type': 'paragraph',
                'text': 'Reference text on previous page continues here ending with RIGHTTAIL',
                'position': {'part': 'word/document.xml', 'body_index': 8}}}]
    issues = [{'code': 'embedded_object_review', 'position': position}]
    captured = []
    recover_word_visuals(tmp_path / 'unused.docx', tmp_path, kept, removed, issues,
                         lambda *args: captured.append(args), [])
    assert not captured and issues[0]['code'] == 'embedded_object_review'


def test_word_page_fallback_crops_from_a_located_appendix_heading(tmp_path, monkeypatch):
    import fitz
    from local_prepare import _native_paths
    _native_paths()
    import evidence_render
    from word_visual import recover_word_visuals
    rendered = tmp_path / 'appendix-page.pdf'
    pdf = fitz.open()
    page = pdf.new_page(width=600, height=800)
    page.insert_text((50, 80), 'RIGHTTAIL')
    page.insert_text((50, 130), 'Appendix A')
    page.draw_rect(fitz.Rect(80, 150, 400, 300), color=(0, 0, 1))
    page.insert_text((80, 330), 'Figure 1 Example diagram')
    pdf.save(rendered)
    pdf.close()
    monkeypatch.setattr(evidence_render, 'render_office_pdf', lambda *args: rendered)
    position = {'part': 'word/document.xml', 'body_index': 10, 'object_ref': 'object_0001'}
    kept = [{'id': 'appendix', 'type': 'paragraph', 'heading_level': 1, 'text': 'Appendix A',
             'position': {'part': 'word/document.xml', 'body_index': 9}},
            {'id': 'object', 'type': 'paragraph', 'position': position},
            {'id': 'caption', 'type': 'paragraph', 'text': 'Figure 1 Example diagram',
             'position': {'part': 'word/document.xml', 'body_index': 11}}]
    removed = [{'reason': 'references', 'block': {'type': 'paragraph',
                'text': 'Reference text on previous page continues here ending with RIGHTTAIL',
                'position': {'part': 'word/document.xml', 'body_index': 8}}}]
    issues = [{'code': 'embedded_object_review', 'position': position}]
    captured = []
    def resource(rid, data, extension, kind, pos):
        captured.append((rid, data, kind, pos))
        return {'id': rid}
    recover_word_visuals(tmp_path / 'unused.docx', tmp_path, kept, removed, issues, resource, [])
    assert captured and captured[0][3]['rendered_bbox'][1] > 80


def test_partition_never_splits_image_marker_or_moves_image_to_wrong_fragment(monkeypatch):
    from local_prepare import _native_paths
    _native_paths()
    import preextract_partition
    marker = '[[IMAGE:image_0001]]'
    text = 'A' * 20 + marker + 'B' * 20
    bundle = {'blocks': [{'id': 'b', 'type': 'paragraph', 'text': text, 'section_id': 's',
                          'position': {'body_index': 1}}],
              'assets': [{'id': 'image_0001', 'image_path': 'synthetic',
                          'block_ids': ['b'], 'section_ids': ['s']}],
              'sections': [{'id': 's', 'title': '正文', 'level': 1, 'parent_id': None}],
              'source': {'reading_scope': {}}, 'issues': [], 'formulas': []}
    def build(part, *args, **kwargs):
        value = ''.join(row.get('text', '') for row in part['blocks'])
        return {'text': value}, {'violations': ['context'] if len(value) > 35 else []}
    monkeypatch.setattr(preextract_partition, 'build_request', build)
    plan = preextract_partition.plan_requests(bundle, '', {}, {})
    assert plan['coverage']['complete']
    assert len(plan['parts']) > 1
    owner = next(part for part in plan['parts'] if 'image_0001' in part['asset_ids'])
    assert marker in ''.join(row['text'] for row in owner['bundle']['blocks'] if row.get('reading_role') == 'primary')
    assert all('[[IMAGE:' not in row['text'] or marker in row['text']
               for part in plan['parts'] for row in part['bundle']['blocks'] if row.get('reading_role') == 'primary')


def test_image_only_partition_retains_its_original_marker_as_context(monkeypatch):
    from local_prepare import _native_paths
    _native_paths()
    import preextract_partition
    bundle = {'blocks': [{'id': 'b', 'type': 'paragraph', 'text': '前[[IMAGE:one]]中[[IMAGE:two]]后',
                          'section_id': 's', 'position': {'body_index': 1}}],
              'assets': [{'id': ident, 'image_path': 'synthetic', 'block_ids': ['b'], 'section_ids': ['s']}
                         for ident in ('one', 'two')],
              'sections': [{'id': 's', 'title': '正文', 'level': 1, 'parent_id': None}],
              'source': {'reading_scope': {}}, 'issues': [], 'formulas': []}
    def build(part, *args, **kwargs):
        return {}, {'violations': ['image_count'] if len(part['assets']) > 1 else []}
    monkeypatch.setattr(preextract_partition, 'build_request', build)
    plan = preextract_partition.plan_requests(bundle, '', {}, {})
    assert plan['coverage']['complete']
    owner = next(part for part in plan['parts'] if 'two' in part['asset_ids'])
    assert any('[[IMAGE:two]]' in row['text'] for row in owner['bundle']['blocks'])
    assert any(row.get('reading_role') == 'context' for row in owner['bundle']['blocks'])
    image = next(a for a in owner['bundle']['assets'] if a['id'] == 'two')
    by_id = {row['id']: row for row in owner['bundle']['blocks']}
    assert any('[[IMAGE:two]]' in by_id[bid]['text'] for bid in image['block_ids'])


def test_heading_styled_reference_entry_does_not_reopen_body():
    rows = [paragraph('ab', '摘要', 1), paragraph('body', '正文。'),
            paragraph('refs', '参考文献', 1), paragraph('one', '[1] 第一条'),
            paragraph('styled_citation', '2. A study of methods', 1),
            paragraph('three', '[3] 后续文献'), paragraph('appendix', '附录 A', 1),
            paragraph('appendix_text', '附录内容。')]
    kept, removed, _ = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert {'styled_citation', 'three'} <= {r['block']['id'] for r in removed}
    assert {'appendix', 'appendix_text'} <= {b['id'] for b in kept}


def test_chapter_words_in_reference_entry_do_not_reopen_body():
    for heading_level in (0, 1):
        rows = [paragraph('ab', '摘要', 1), paragraph('body', '正文。'),
                paragraph('refs', '参考文献', 1), paragraph('one', '[1] 第一条'),
                paragraph('citation', 'Chapter 1 Introduction', heading_level),
                paragraph('three', '[3] 后续文献'), paragraph('appendix', '附录 A', 1),
                paragraph('appendix_text', '附录内容。')]
        kept, removed, _ = clean_body_blocks(rows, continue_on_uncertainty=True)
        assert {'citation', 'three'} <= {r['block']['id'] for r in removed}
        assert 'appendix_text' in {b['id'] for b in kept}


def test_reference_continuation_before_later_citation_stays_excluded():
    rows = [paragraph('ab', '摘要', 1), paragraph('body', '正文。'),
            paragraph('refs', '参考文献', 1), paragraph('one', '[1] 第一条'),
            paragraph('citation', 'Chapter 1 Introduction', 1),
            paragraph('continuation', 'A book chapter citation continued on the next line.'),
            paragraph('three', '[3] 后续文献'), paragraph('appendix', '附录 A', 1),
            paragraph('appendix_text', '附录内容。')]
    kept, removed, _ = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert {'citation', 'continuation', 'three'} <= {r['block']['id'] for r in removed}
    assert 'appendix_text' in {b['id'] for b in kept}


def test_many_reference_continuation_paragraphs_stay_excluded():
    rows = [paragraph('ab', '摘要', 1), paragraph('body', '正文。'),
            paragraph('refs', '参考文献', 1), paragraph('one', '[1] 第一条'),
            paragraph('citation', 'Chapter 1 Introduction', 1)]
    rows += [paragraph('continuation' + str(i), 'Further citation continuation.') for i in range(7)]
    rows += [paragraph('three', '[3] 后续文献'), paragraph('appendix', '附录 A', 1),
             paragraph('appendix_text', '附录内容。')]
    kept, removed, _ = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert {'citation', 'three', 'continuation6'} <= {r['block']['id'] for r in removed}
    assert 'appendix_text' in {b['id'] for b in kept}


def test_word_page_fallback_does_not_guess_across_columns(tmp_path, monkeypatch):
    import fitz
    from local_prepare import _native_paths
    _native_paths()
    import evidence_render
    from word_visual import recover_word_visuals
    rendered = tmp_path / 'columns.pdf'
    pdf = fitz.open()
    page = pdf.new_page(width=600, height=800)
    page.insert_text((50, 650), 'RIGHTTAIL KEPTBODY')
    page.insert_text((350, 130), 'Appendix A')
    page.draw_rect(fitz.Rect(350, 150, 550, 300), color=(0, 0, 1))
    page.insert_text((350, 330), 'Figure 1 Example diagram')
    pdf.save(rendered)
    pdf.close()
    monkeypatch.setattr(evidence_render, 'render_office_pdf', lambda *args: rendered)
    position = {'part': 'word/document.xml', 'body_index': 10, 'object_ref': 'object_0001'}
    kept = [{'id': 'appendix', 'type': 'paragraph', 'heading_level': 1, 'text': 'Appendix A',
             'position': {'part': 'word/document.xml', 'body_index': 9}},
            {'id': 'object', 'type': 'paragraph', 'position': position},
            {'id': 'caption', 'type': 'paragraph', 'text': 'Figure 1 Example diagram',
             'position': {'part': 'word/document.xml', 'body_index': 11}}]
    removed = [{'reason': 'references', 'block': {'type': 'paragraph',
                'text': 'Reference text on previous page continues here ending with RIGHTTAIL',
                'position': {'part': 'word/document.xml', 'body_index': 8}}}]
    issues = [{'code': 'embedded_object_review', 'position': position}]
    captured = []
    recover_word_visuals(tmp_path / 'unused.docx', tmp_path, kept, removed, issues,
                         lambda *args: captured.append(args), [])
    assert not captured and issues[0]['code'] == 'embedded_object_review'


def test_word_rendered_figure_does_not_include_excluded_text_overlaid_on_it(tmp_path, monkeypatch):
    import io
    import fitz
    from local_prepare import _native_paths
    _native_paths()
    import evidence_render
    from word_visual import recover_word_visuals
    pixels = io.BytesIO()
    Image.new('RGB', (100, 100), 'blue').save(pixels, format='PNG')
    rendered = tmp_path / 'overlap.pdf'
    pdf = fitz.open()
    page = pdf.new_page(width=600, height=800)
    page.insert_image(fitz.Rect(50, 50, 400, 300), stream=pixels.getvalue(), keep_proportion=False)
    page.insert_text((80, 180), 'EXCLUDED CITATION TEXT', color=(1, 1, 1))
    page.insert_text((80, 330), 'Figure 1 Example diagram')
    pdf.save(rendered)
    pdf.close()
    monkeypatch.setattr(evidence_render, 'render_office_pdf', lambda *args: rendered)
    position = {'part': 'word/document.xml', 'body_index': 10, 'object_ref': 'object_0001'}
    kept = [{'id': 'object', 'type': 'paragraph', 'position': position},
            {'id': 'caption', 'type': 'paragraph', 'text': 'Figure 1 Example diagram',
             'position': {'part': 'word/document.xml', 'body_index': 11}}]
    removed = [{'reason': 'references', 'block': {'type': 'paragraph', 'text': 'EXCLUDED CITATION TEXT',
                'position': {'part': 'word/document.xml', 'body_index': 8}}}]
    issues = [{'code': 'embedded_object_review', 'position': position}]
    captured = []
    recover_word_visuals(tmp_path / 'unused.docx', tmp_path, kept, removed, issues,
                         lambda *args: captured.append(args), [])
    assert not captured and issues[0]['code'] == 'embedded_object_review'


def test_partition_places_formula_only_with_the_fragment_containing_it(monkeypatch):
    from local_prepare import _native_paths
    _native_paths()
    import preextract_partition
    marker = '$E=mc^2$'
    position = {'body_index': 1}
    bundle = {'blocks': [{'id': 'b', 'type': 'paragraph', 'text': 'A' * 20 + marker + 'B' * 20,
                          'section_id': 's', 'position': position}],
              'assets': [], 'formulas': [{'latex': 'E=mc^2', 'position': position}],
              'sections': [{'id': 's', 'title': '正文', 'level': 1, 'parent_id': None}],
              'source': {'reading_scope': {}}, 'issues': []}
    def build(part, *args, **kwargs):
        value = ''.join(row.get('text', '') for row in part['blocks'])
        return {}, {'violations': ['context'] if len(value) > 35 else []}
    monkeypatch.setattr(preextract_partition, 'build_request', build)
    plan = preextract_partition.plan_requests(bundle, '', {}, {})
    assert plan['coverage']['complete'] and len(plan['parts']) > 1
    formula_parts = [part for part in plan['parts'] if part['bundle']['formulas']]
    assert len(formula_parts) == 1
    assert marker in ''.join(row['text'] for row in formula_parts[0]['bundle']['blocks'])


def test_partition_matches_repeated_formulas_to_their_own_occurrences(monkeypatch):
    from local_prepare import _native_paths
    _native_paths()
    import preextract_partition
    position = {'body_index': 1}
    bundle = {'blocks': [{'id': 'b', 'type': 'paragraph',
                          'text': 'A' * 20 + '$x$' + 'B' * 30 + '$x$' + 'C' * 20,
                          'section_id': 's', 'position': position}],
              'assets': [], 'formulas': [{'latex': 'x', 'position': position, 'origin': i} for i in (1, 2)],
              'sections': [{'id': 's', 'title': '正文', 'level': 1, 'parent_id': None}],
              'source': {'reading_scope': {}}, 'issues': []}
    def build(part, *args, **kwargs):
        value = ''.join(row.get('text', '') for row in part['blocks'])
        return {}, {'violations': ['context'] if len(value) > 35 else []}
    monkeypatch.setattr(preextract_partition, 'build_request', build)
    plan = preextract_partition.plan_requests(bundle, '', {}, {})
    assert plan['coverage']['complete']
    formulas = []
    for part in plan['parts']:
        text = ''.join(row['text'] for row in part['bundle']['blocks'])
        assert len(part['bundle']['formulas']) == text.count('$x$')
        formulas.extend(f['origin'] for f in part['bundle']['formulas'])
    assert sorted(formulas) == [1, 2]


def test_partition_records_gap_when_native_formula_occurrence_is_ambiguous(monkeypatch):
    from local_prepare import _native_paths
    _native_paths()
    import preextract_partition
    position = {'body_index': 1}
    bundle = {'blocks': [{'id': 'b', 'type': 'paragraph',
                          'text': 'A' * 40 + '$x$' + 'B' * 40 + '$x$' + 'C' * 20,
                          'section_id': 's', 'position': position}],
              'assets': [], 'formulas': [{'latex': 'x', 'position': position}],
              'sections': [{'id': 's', 'title': '正文', 'level': 1, 'parent_id': None}],
              'source': {'reading_scope': {}}, 'issues': []}
    def build(part, *args, **kwargs):
        value = ''.join(row.get('text', '') for row in part['blocks'])
        return {}, {'violations': ['context'] if len(value) > 30 else []}
    monkeypatch.setattr(preextract_partition, 'build_request', build)
    plan = preextract_partition.plan_requests(bundle, '', {}, {})
    assert plan['coverage']['complete']
    assert not any(part['bundle']['formulas'] for part in plan['parts'])
    assert any(gap['code'] == 'formula_position_unresolved' for gap in plan['partition_gaps'])
    assert sum(''.join(row['text'] for row in part['bundle']['blocks']).count('$x$')
               for part in plan['parts']) == 2


def test_partition_does_not_break_multiline_formula(monkeypatch):
    from local_prepare import _native_paths
    _native_paths()
    import preextract_partition
    position = {'body_index': 1}
    marker = '$a\nb$'
    bundle = {'blocks': [{'id': 'b', 'type': 'paragraph', 'text': 'A' * 10 + marker + 'B' * 10,
                          'section_id': 's', 'position': position}],
              'assets': [], 'formulas': [{'latex': 'a\nb', 'position': position}],
              'sections': [{'id': 's', 'title': '正文', 'level': 1, 'parent_id': None}],
              'source': {'reading_scope': {}}, 'issues': []}
    def build(part, *args, **kwargs):
        value = ''.join(row.get('text', '') for row in part['blocks'])
        return {}, {'violations': ['context'] if len(value) > 15 else []}
    monkeypatch.setattr(preextract_partition, 'build_request', build)
    plan = preextract_partition.plan_requests(bundle, '', {}, {})
    matching = [p for p in plan['parts'] if p['bundle']['formulas']]
    assert len(matching) == 1
    assert marker in ''.join(row['text'] for row in matching[0]['bundle']['blocks'])


def test_formula_position_gap_is_saved_in_the_offline_plan(tmp_path, monkeypatch):
    from docx.oxml import OxmlElement
    from local_prepare import _native_paths
    _native_paths()
    import preextract_partition
    source = tmp_path / 'ambiguous.docx'
    doc = Document()
    doc.add_heading('摘要', 1)
    paragraph = doc.add_paragraph('A' * 40 + '$x$' + 'B' * 40)
    math = OxmlElement('m:oMath')
    run = OxmlElement('m:r')
    text = OxmlElement('m:t')
    text.text = 'x'
    run.append(text)
    math.append(run)
    paragraph._p.append(math)
    paragraph.add_run('C' * 20)
    doc.save(source)
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    prepared = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path / 'work', catalog)
    def build(part, *args, **kwargs):
        value = ''.join(row.get('text', '') for row in part['blocks'])
        return {}, {'violations': ['context'] if len(value) > 30 else []}
    monkeypatch.setattr(preextract_partition, 'build_request', build)
    result = plan_clean_material(prepared['material_path'], '模板', catalog, settings(), tmp_path / 'plan')
    saved = json.loads(Path(result['plan_path']).read_text(encoding='utf8'))
    assert any(gap['code'] == 'formula_position_unresolved' for gap in saved['reading_gaps'])
    assert saved['source_completeness'] == 'partial'


def test_real_offline_partition_keeps_native_formula_with_its_text(tmp_path):
    from docx.oxml import OxmlElement
    source = tmp_path / 'long-formula.docx'
    doc = Document()
    doc.add_heading('摘要', 1)
    paragraph = doc.add_paragraph('A' * 12000)
    math = OxmlElement('m:oMath')
    run = OxmlElement('m:r')
    text = OxmlElement('m:t')
    text.text = 'E=mc^2'
    run.append(text)
    math.append(run)
    paragraph._p.append(math)
    paragraph.add_run('B' * 12000)
    doc.save(source)
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    prepared = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path / 'work', catalog)
    cfg = settings()
    cfg['context_window'] = 14000
    plan = plan_clean_material(prepared['material_path'], '模板', catalog, cfg, tmp_path / 'plan')['plan']
    assert plan['mode'] == 'partitioned'
    found = 0
    for part in plan['parts']:
        view = json.loads(part['body']['messages'][1]['content'][0]['text'])['material']
        blocks = {row['id']: row for row in view['blocks']}
        for formula in view['formulas']:
            assert '$E=mc^2$' in blocks[formula['block_id']]['text']
            found += 1
    assert found == 1


def test_short_body_after_a_confirmed_toc_is_not_dropped():
    rows = [paragraph('ab', '摘要'), paragraph('summary', '摘要内容。'),
            paragraph('toc', '目录'), paragraph('entry1', '1 绪论\t1'),
            paragraph('entry2', '2 方法\t3'), paragraph('chapter', '1 绪论', 1),
            paragraph('body', '简短正文。')]
    for row in rows:
        row['position'] = {'page': None}
    kept, removed, _ = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert {'chapter', 'body'} <= {b['id'] for b in kept}
    assert {'toc', 'entry1', 'entry2'} <= {r['block']['id'] for r in removed}


def test_short_document_without_abstract_keeps_a_clear_first_chapter():
    rows = [paragraph('cover', '封面'), paragraph('chapter', '第一章 绪论', 1),
            paragraph('body', '简短正文。')]
    kept, removed, scope = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert {'chapter', 'body'} <= {b['id'] for b in kept}
    assert 'cover' in {r['block']['id'] for r in removed}
    assert scope['start_kind'] == 'first_chapter_no_abstract'


def test_uncertain_toc_transition_does_not_drop_later_chapter():
    rows = [paragraph('ab', '摘要'), paragraph('summary', '摘要内容。'),
            paragraph('toc', '目录'), paragraph('entry1', '1 绪论\t1'),
            paragraph('entry2', '2 方法\t3'), paragraph('uncertain', '交界处不完整文字'),
            paragraph('chapter', '1 绪论', 1), paragraph('body', '后续正文继续保留。')]
    for row in rows:
        row['position'] = {'page': None}
    kept, removed, scope = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert {'uncertain', 'chapter', 'body'} <= {b['id'] for b in kept}
    assert {'toc', 'entry1', 'entry2'} <= {r['block']['id'] for r in removed}
    assert any(row['source_block_id'] == 'uncertain' for row in scope['transition_fragments'])


def test_uncertain_toc_transition_keeps_a_positioned_image():
    rows = [paragraph('ab', '摘要'), paragraph('toc', '目录'),
            paragraph('entry1', '1 绪论\t1'), paragraph('entry2', '2 方法\t3'),
            {'id': 'image', 'type': 'image', 'resource_id': 'figure',
             'position': {'page': 2, 'bbox': [50, 100, 200, 250]}},
            paragraph('chapter', '1 绪论', 1), paragraph('body', '后续正文。')]
    rows[-2]['position'] = {'page': 3}
    rows[-1]['position'] = {'page': 3}
    kept, _, scope = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert {'image', 'chapter', 'body'} <= {b['id'] for b in kept}
    assert any(row['source_block_id'] == 'image' for row in scope['transition_visuals'])


def test_uncertain_toc_transition_keeps_a_table_and_later_text():
    rows = [paragraph('ab', '摘要'), paragraph('toc', '目录'),
            paragraph('entry1', '1 绪论\t1'), paragraph('entry2', '2 方法\t3'),
            {'id': 'table', 'type': 'table', 'cells': [{'text': '交界表格'}],
             'position': {'body_index': 5}},
            paragraph('chapter', '1 绪论', 1), paragraph('body', '后续正文。')]
    rows[-2]['position'] = {'page': None}
    rows[-1]['position'] = {'page': None}
    kept, _, scope = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert {'table', 'chapter', 'body'} <= {b['id'] for b in kept}
    assert any(row['source_block_id'] == 'table' for row in scope['transition_fragments'])


def test_later_inline_abstract_word_does_not_stop_following_body():
    rows = [paragraph('ab', '摘要', 1), paragraph('body', '正文。'),
            paragraph('inline', '摘要：本节简要说明实验结果。'),
            paragraph('after', '后续研究内容。'), paragraph('refs', '参考文献', 1),
            paragraph('entry', '[1] 文献条目'), paragraph('appendix', '附录 A', 1),
            paragraph('extra', '附录内容。')]
    kept, removed, _ = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert {'inline', 'after', 'appendix', 'extra'} <= {b['id'] for b in kept}
    assert {'refs', 'entry'} <= {r['block']['id'] for r in removed}


def test_local_scope_failure_records_only_the_gap_and_continues_later_text(tmp_path, monkeypatch):
    import reading_cleanup
    original_clean = reading_cleanup.clean_body_blocks
    def fail_one_block(blocks, **kwargs):
        if any(block.get('text') == '模拟局部范围故障' for block in blocks):
            raise reading_cleanup.ScopeUncertain('模拟局部范围故障')
        return original_clean(blocks, **kwargs)
    monkeypatch.setattr(reading_cleanup, 'clean_body_blocks', fail_one_block)
    source = tmp_path / 'paper.docx'
    doc = Document()
    doc.add_heading('摘要', 1)
    doc.add_paragraph('前面正文。')
    doc.add_paragraph('模拟局部范围故障')
    doc.add_paragraph('后面正文继续。')
    doc.add_heading('参考文献', 1)
    doc.add_paragraph('[1] 不应交接的文献')
    doc.add_heading('附录 A', 1)
    doc.add_paragraph('后面附录继续。')
    doc.save(source)
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    prepared = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path / 'work', catalog)
    plan = plan_clean_material(prepared['material_path'], '模板', catalog, settings(), tmp_path / 'plan')['plan']
    content = json.dumps([part['body'] for part in plan['parts']], ensure_ascii=False)
    assert '前面正文' in content and '后面正文继续' in content and '后面附录继续' in content
    assert '不应交接的文献' not in content
    assert any(gap['code'] == 'scope_incomplete' for gap in plan['reading_gaps'])


def test_multiple_local_scope_failures_preserve_reference_state(tmp_path, monkeypatch):
    import reading_cleanup
    original_clean = reading_cleanup.clean_body_blocks
    def fail_selected_blocks(blocks, **kwargs):
        if any(str(block.get('text') or '').startswith('模拟范围故障') for block in blocks):
            raise reading_cleanup.ScopeUncertain('模拟局部故障')
        return original_clean(blocks, **kwargs)
    monkeypatch.setattr(reading_cleanup, 'clean_body_blocks', fail_selected_blocks)
    source = tmp_path / 'paper.docx'
    doc = Document()
    doc.add_heading('摘要', 1)
    doc.add_paragraph('前面正文。')
    doc.add_paragraph('模拟范围故障一')
    doc.add_paragraph('中间正文。')
    doc.add_heading('参考文献', 1)
    doc.add_paragraph('[1] 文献一')
    doc.add_paragraph('模拟范围故障二')
    doc.add_paragraph('[2] 文献二不应交接')
    doc.add_heading('附录 A', 1)
    doc.add_paragraph('附录仍然保留。')
    doc.save(source)
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    prepared = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path / 'work', catalog)
    plan = plan_clean_material(prepared['material_path'], '模板', catalog, settings(), tmp_path / 'plan')['plan']
    content = json.dumps([part['body'] for part in plan['parts']], ensure_ascii=False)
    assert '前面正文' in content and '中间正文' in content and '附录仍然保留' in content
    assert '文献一' not in content and '文献二不应交接' not in content
    assert sum(gap['code'] == 'scope_incomplete' for gap in plan['reading_gaps']) == 2


def test_short_inline_abstract_keeps_body_and_appendix():
    rows = [paragraph('abstract', '摘要：研究'), paragraph('body', '可用正文继续。'),
            paragraph('refs', '参考文献', 1), paragraph('entry', '[1] 文献'),
            paragraph('appendix', '附录 A', 1), paragraph('extra', '附录正文。')]
    kept, removed, _ = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert {'abstract', 'body', 'appendix', 'extra'} <= {b['id'] for b in kept}
    assert {'refs', 'entry'} <= {r['block']['id'] for r in removed}


def test_scope_recovery_keeps_appendix_context(tmp_path, monkeypatch):
    import reading_cleanup
    original = reading_cleanup.clean_body_blocks
    def fail_block(blocks, **kwargs):
        if any(block.get('text') == '局部故障' for block in blocks):
            raise reading_cleanup.ScopeUncertain('局部故障')
        return original(blocks, **kwargs)
    monkeypatch.setattr(reading_cleanup, 'clean_body_blocks', fail_block)
    source = tmp_path / 'paper.docx'
    doc = Document()
    doc.add_heading('摘要', 1)
    doc.add_heading('附录 A', 1)
    for value in ['局部故障', '目录', '物料 A  1', '物料 B  2', '此为材料清单。']:
        doc.add_paragraph(value)
    doc.save(source)
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    prepared = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path / 'work', catalog)
    body = (tmp_path / 'work' / 'private' / 'body.txt').read_text(encoding='utf8')
    assert '物料 A' in body and '物料 B' in body and '此为材料清单' in body


def test_scope_recovery_ignores_headings_inside_failed_formula(tmp_path):
    source = tmp_path / 'paper.docx'
    doc = Document()
    doc.add_heading('摘要', 1)
    doc.add_paragraph('前面正文。')
    doc.add_paragraph('目录\n$x\n参考文献\ny$')
    doc.add_paragraph('后面正文不应丢。')
    doc.add_heading('附录 A', 1)
    doc.add_paragraph('附录数据。')
    doc.save(source)
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path / 'work', catalog)
    body = (tmp_path / 'work' / 'private' / 'body.txt').read_text(encoding='utf8')
    assert '后面正文不应丢' in body and '附录数据' in body


def test_scope_recovery_keeps_notes_referenced_before_failure(tmp_path, monkeypatch):
    from local_prepare import _native_paths
    _native_paths()
    import native_docx
    rows = [paragraph('ab', '摘要'), paragraph('body', '正文引用[^footnoteReference1]。'),
            paragraph('bad', '目录\n不完整目录说明'), paragraph('after', '后面正文。'),
            {'id': 'note', 'type': 'paragraph', 'text': '[^footnoteReference1]: 脚注内容',
             'note_label': 'footnoteReference1', 'position': {'part': 'word/footnotes.xml', 'note_id': '1'}}]
    monkeypatch.setattr(native_docx, 'parse_docx', lambda *args: (rows, [], []))
    source = tmp_path / 'paper.docx'
    Document().save(source)
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    prepared = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path / 'work', catalog)
    body = (tmp_path / 'work' / 'private' / 'body.txt').read_text(encoding='utf8')
    material = json.loads(Path(prepared['material_path']).read_text(encoding='utf8'))
    assert '脚注内容' in body and '后面正文' in body
    assert not any(issue['code'] == 'missing_note' for issue in material['issues'])


def test_no_abstract_still_keeps_clear_appendix(tmp_path):
    source = tmp_path / 'paper.docx'
    doc = Document()
    doc.add_paragraph('前部无法确认。')
    doc.add_heading('附录 A', 1)
    doc.add_paragraph('附录应当保留。')
    doc.save(source)
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    prepared = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path / 'work', catalog)
    body = (tmp_path / 'work' / 'private' / 'body.txt').read_text(encoding='utf8')
    material = json.loads(Path(prepared['material_path']).read_text(encoding='utf8'))
    assert '附录应当保留' in body
    assert any(issue['code'] == 'scope_incomplete' for issue in material['issues'])


def test_no_recognized_start_keeps_readable_text_and_excludes_references(tmp_path):
    source = tmp_path / 'paper.docx'
    doc = Document()
    doc.add_paragraph('没有摘要时仍可读取的研究内容。')
    doc.add_heading('参考文献', 1)
    doc.add_paragraph('[1] 应排除的文献条目')
    doc.add_heading('附录 A', 1)
    doc.add_paragraph('附录正文继续保留。')
    doc.save(source)
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    prepared = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path / 'work', catalog)
    body = (tmp_path / 'work' / 'private' / 'body.txt').read_text(encoding='utf8')
    material = json.loads(Path(prepared['material_path']).read_text(encoding='utf8'))
    assert '没有摘要时仍可读取的研究内容' in body and '附录正文继续保留' in body
    assert '应排除的文献条目' not in body
    assert any(issue['code'] == 'scope_incomplete' for issue in material['issues'])
    plan = plan_clean_material(prepared['material_path'], '模板', catalog, settings(), tmp_path / 'plan')['plan']
    content = json.loads(plan['parts'][0]['body']['messages'][1]['content'][0]['text'])['material']
    assert '边界' in content['reading_context']['instruction']
    assert '仅包含已经确认' not in content['reading_context']['instruction']


def test_more_than_64_necessary_parts_do_not_block_the_paper(monkeypatch):
    from local_prepare import _native_paths
    _native_paths()
    import preextract_partition
    blocks = [{'id': 'b' + str(i), 'type': 'image', 'text': '', 'section_id': 's',
               'position': {'body_index': i}} for i in range(65)]
    assets = [{'id': 'im' + str(i), 'image_path': 'synthetic', 'block_ids': [blocks[i]['id']],
               'section_ids': ['s']} for i in range(65)]
    bundle = {'blocks': blocks, 'assets': assets,
              'sections': [{'id': 's', 'title': '正文', 'level': 1, 'parent_id': None}],
              'source': {'reading_scope': {}}, 'issues': [], 'formulas': []}
    def build(part, *args, **kwargs):
        return {}, {'violations': ['image_count'] if len(part['assets']) > 1 else []}
    monkeypatch.setattr(preextract_partition, 'build_request', build)
    plan = preextract_partition.plan_requests(bundle, '', {}, {})
    assert plan['coverage']['complete'] and len(plan['parts']) == 65
    assert len(plan['coverage']['asset_ids']) == 65
