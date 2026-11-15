import hashlib
import json
from pathlib import Path

import pytest
from test_registered_prepare import registered


def test_zero_line_ocr_does_not_withhold_a_located_body_image():
    from review_requirements import build_readiness
    asset = {'id': 'fig', 'kind': 'image', 'image_path': 'figure.png',
             'block_ids': ['b'], 'section_ids': ['s'],
             'position': {'body_index': 1},
             'image_scope_audit': {'status': 'ocr_checked_unverified', 'detected_lines': 0}}
    readiness = build_readiness([asset], [], {'start_kind': 'abstract'})
    assert not any(row['kind'] == 'visual_scope' and row['asset_id'] == 'fig' for row in readiness['items'])


def test_partial_projection_withholds_any_block_spanning_a_pending_page():
    from partial_projection import project_available_material
    material = {'blocks': [{'id': 'joined', 'type': 'paragraph', 'section_id': 's',
                             'text': 'safe, MUST_NOT_LEAK', 'position': {'page': 1},
                             'source_segments': [
                                 {'position': {'page': 1}, 'target_start': 0, 'target_end': 5},
                                 {'position': {'page': 2}, 'target_start': 5, 'target_end': 19}]},
                            {'id': 'good', 'type': 'paragraph', 'text': 'other', 'position': {'page': 3}}],
                'assets': [], 'sections': [{'id': 's', 'title': 'S', 'block_ids': ['joined']}], 'formulas': []}
    projected, _ = project_available_material(material, [{'kind': 'column_scope_review', 'position': {'page': 2}}])
    assert 'MUST_NOT_LEAK' not in json.dumps(projected, ensure_ascii=False)
    assert any(row['id'] == 'good' for row in projected['blocks'])


def test_partial_projection_drops_unlocated_derived_caption_and_notes_near_pending_page():
    from partial_projection import project_available_material
    material = {'blocks': [{'id': 't', 'type': 'table', 'text': '研究表格数据', 'position': {'page': 1}}],
                'assets': [{'id': 'table', 'kind': 'table', 'block_ids': ['t'], 'position': {'page': 1},
                            'caption': '图注_MUST_NOT_LEAK',
                            'structure': {'notes': '注_MUST_NOT_LEAK', 'units': '单位_MUST_NOT_LEAK', 'cells': []}}],
                'sections': [], 'formulas': []}
    projected, _ = project_available_material(material, [{'kind': 'column_scope_review', 'position': {'page': 2}}])
    assert 'MUST_NOT_LEAK' not in json.dumps(projected, ensure_ascii=False)
    assert '研究表格数据' in json.dumps(projected, ensure_ascii=False)
    projected, _ = project_available_material(material, [{'kind': 'column_scope_review', 'position': {'page': 5}}])
    assert 'MUST_NOT_LEAK' not in json.dumps(projected, ensure_ascii=False)


def test_ocr_text_overlapping_pending_visual_is_withheld_but_other_text_continues():
    from partial_projection import project_available_material
    material={'blocks':[{'id':'bad','type':'paragraph','section_id':'s','heading_level':1,
                         'text':'MUST_NOT_LEAK','source':'cpu_ocr',
                         'position':{'page':2,'bbox':[120,400,450,450],'ocr':True}},
                        {'id':'good','type':'paragraph','section_id':'t','text':'safe continuation',
                         'position':{'page':2,'bbox':[30,650,80,700]}}],
              'assets':[],'sections':[{'id':'s','title':'MUST_NOT_LEAK','block_ids':['bad']},
                                      {'id':'t','title':'safe','block_ids':['good']}], 'formulas':[]}
    pending=[{'kind':'unresolved_visual','position':{'page':2,'bbox':[100,80,500,600]}}]
    projected,_=project_available_material(material,pending)
    content=json.dumps(projected,ensure_ascii=False)
    assert 'MUST_NOT_LEAK' not in content and 'safe continuation' in content


def test_withheld_ocr_visual_text_cannot_leak_through_inferred_table_notes():
    from partial_projection import project_available_material
    bad={'id':'bad','type':'paragraph','text':'MUST_NOT_LEAK','source':'cpu_ocr',
         'position':{'page':2,'bbox':[120,400,450,450],'ocr':True}}
    table={'id':'t','type':'table','text':'研究数据','position':{'page':1}}
    material={'blocks':[table,bad], 'assets':[{'id':'a','kind':'table','block_ids':['t'],
              'position':{'page':1},'structure':{'notes':'MUST_NOT_LEAK','cells':[]}}],
              'sections':[],'formulas':[]}
    projected,_=project_available_material(material,[{'kind':'unresolved_visual',
                                                       'position':{'page':2,'bbox':[100,80,500,600]}}])
    content=json.dumps(projected,ensure_ascii=False)
    assert 'MUST_NOT_LEAK' not in content and '研究数据' in content


def test_uncertain_scan_crop_holds_pixels_and_ocr_but_keeps_native_body_text():
    from review_requirements import build_readiness
    from partial_projection import project_available_material
    issue={'code':'pdf_scan_crop_scope_review','position':{'page':3},'message':'扫描页图像范围待核'}
    readiness=build_readiness([], [issue], {})
    assert readiness['items'][0]['kind']=='unresolved_visual'
    material={'blocks':[{'id':'native','type':'paragraph','text':'可用正文', 'position':{'page':3}},
                        {'id':'ocr','type':'paragraph','text':'待核 OCR', 'source':'cpu_ocr',
                         'position':{'page':3,'ocr':True}}], 'assets':[], 'sections':[], 'formulas':[]}
    projected,_=project_available_material(material,readiness['items'])
    content=json.dumps(projected,ensure_ascii=False)
    assert '可用正文' in content and '待核 OCR' not in content


def test_pending_small_visual_region_does_not_hold_separate_body_figure():
    from partial_projection import project_available_material
    material={'blocks':[{'id':'header','type':'image','text':'','position':{'page':7,'bbox':[0,0,500,2]}},
                        {'id':'figure','type':'image','text':'','position':{'page':7,'bbox':[80,100,400,400]}}],
              'assets':[{'id':'line','kind':'image','image_path':'line.png','block_ids':['header'],
                         'position':{'page':7,'bbox':[0,0,500,2]}},
                        {'id':'diagram','kind':'image','image_path':'figure.png','block_ids':['figure'],
                         'position':{'page':7,'bbox':[80,100,400,400]}}], 'sections':[], 'formulas':[]}
    pending=[{'kind':'unresolved_visual','position':{'page':7,'bbox':[0,0,500,2]}}]
    projected,report=project_available_material(material,pending)
    assert [a['id'] for a in projected['assets']]==['diagram']
    assert [b['id'] for b in projected['blocks']]==['figure']
    assert report['held_asset_count']==1


def test_pending_visual_without_region_keeps_the_conservative_page_hold():
    from partial_projection import project_available_material
    material={'blocks':[{'id':'a','type':'image','text':'','position':{'page':7,'bbox':[80,100,400,400]}}],
              'assets':[{'id':'fig','kind':'image','image_path':'x.png','block_ids':['a'],
                         'position':{'page':7,'bbox':[80,100,400,400]}}], 'sections':[], 'formulas':[]}
    projected,_=project_available_material(material,[{'kind':'unresolved_visual','position':{'page':7}}])
    assert projected['assets']==[] and projected['blocks']==[]


def test_caption_is_retained_when_its_own_safe_native_block_is_located():
    from partial_projection import project_available_material
    material={'blocks':[{'id':'fig','type':'image','text':'','position':{'page':2,'bbox':[80,100,400,300]}},
                        {'id':'caption','type':'paragraph','text':'图 2 流程图',
                         'position':{'page':2,'bbox':[150,305,300,320]}},
                        {'id':'held','type':'paragraph','text':'被扣留文字',
                         'position':{'page':5,'bbox':[100,200,300,220]}}],
              'assets':[{'id':'diagram','kind':'image','image_path':'figure.png','block_ids':['fig'],
                         'position':{'page':2,'bbox':[80,100,400,300]},'caption':'图 2 流程图'},
                        {'id':'other','kind':'image','image_path':'other.png','block_ids':['fig'],
                         'position':{'page':2,'bbox':[80,100,400,300]},'caption':'被扣留文字'}],
              'sections':[], 'formulas':[]}
    projected,_=project_available_material(material,[{'kind':'unresolved_boundary','position':{'page':5}}])
    by_id={a['id']:a for a in projected['assets']}
    assert by_id['diagram']['caption']=='图 2 流程图'
    assert by_id['other']['caption']==''


def test_word_caption_with_own_adjacent_native_source_is_retained():
    from partial_projection import project_available_material
    image = {'part': 'word/document.xml', 'body_index': 39}
    caption = {'part': 'word/document.xml', 'body_index': 40}
    material = {'blocks': [{'id': 'fig', 'type': 'image', 'position': image},
                           {'id': 'caption', 'type': 'paragraph', 'text': '图 1 原生图注',
                            'source': 'native_document', 'position': caption, 'source_position': caption}],
                'assets': [{'id': 'diagram', 'kind': 'image', 'image_path': 'figure.png',
                            'block_ids': ['fig'], 'position': image, 'caption': '图 1 原生图注'}],
                'sections': [], 'formulas': []}
    projected, _ = project_available_material(material, [{'kind': 'unresolved_visual',
                                                          'position': {'part': 'word/document.xml', 'body_index': 60}}])
    assert projected['assets'][0]['caption'] == '图 1 原生图注'


def test_word_caption_not_adjacent_within_content_container_is_unverified():
    from partial_projection import project_available_material
    image = {'part': 'word/document.xml', 'body_index': 39, 'container_path': [1, 0]}
    caption = {'part': 'word/document.xml', 'body_index': 39, 'container_path': [1, 2]}
    material = {'blocks': [{'id': 'fig', 'type': 'image', 'position': image},
                           {'id': 'caption', 'type': 'paragraph', 'text': 'unverified caption',
                            'source': 'native_document', 'position': caption}],
                'assets': [{'id': 'diagram', 'kind': 'image', 'image_path': 'x.png', 'block_ids': ['fig'],
                            'position': image, 'caption': 'unverified caption'}], 'sections': [], 'formulas': []}
    projected, _ = project_available_material(material, [{'kind': 'unresolved_visual',
                       'position': {'part': 'word/document.xml', 'body_index': 60}}])
    assert projected['assets'][0]['caption'] == ''


def test_word_caption_cannot_claim_multiple_origin_blocks_as_one_native_caption():
    from partial_projection import project_available_material
    image = {'part': 'word/document.xml', 'body_index': 39}
    caption = {'part': 'word/document.xml', 'body_index': 40}
    material = {'blocks': [{'id': 'fig', 'type': 'image', 'position': image},
                           {'id': 'caption', 'type': 'paragraph', 'text': 'caption', 'source': 'native_document',
                            'position': caption, 'source_segments': [
                                {'source_block_id': 'a', 'target_start': 0, 'target_end': 3, 'position': caption},
                                {'source_block_id': 'b', 'target_start': 3, 'target_end': 7, 'position': caption}]}],
                'assets': [{'id': 'diagram', 'kind': 'image', 'image_path': 'x.png', 'block_ids': ['fig'],
                            'position': image, 'caption': 'caption'}], 'sections': [], 'formulas': []}
    projected, _ = project_available_material(material, [{'kind': 'unresolved_visual',
                       'position': {'part': 'word/document.xml', 'body_index': 60}}])
    assert projected['assets'][0]['caption'] == ''


def test_paper_abstract_title_is_not_discarded_as_front_matter():
    from reading_cleanup import clean_body_blocks
    from test_relaxed_scope import para
    rows = [para('title', '论文摘要', heading=1), para('ab', '必须保留的摘要内容。'),
            para('chapter', '第一章 绪论', heading=1), para('text', '研究正文。' * 20)]
    kept, _, scope = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert scope['start_kind'] == 'abstract'
    assert any('必须保留' in row.get('text', '') for row in kept)


def test_missing_abstract_fallback_remains_explicitly_incomplete():
    from review_requirements import build_readiness
    readiness = build_readiness([], [], {'start_kind': 'first_chapter_no_abstract',
                                        'fallback_evidence': 'unique_styled_chapter_with_prose'})
    assert any(row['kind'] == 'no_abstract' for row in readiness['items'])


def test_toc_dot_leaders_do_not_start_abstract_and_expose_cover():
    from reading_cleanup import clean_body_blocks
    from test_relaxed_scope import para
    rows = [para('toc', '目录'), para('toc_abs', '摘要'), para('entry', '第1章 绪论 ........ 1'),
            para('cover', '封面作者：张某'), para('real_abs', '摘要'),
            para('ab', '真实摘要。'), para('body', '真实研究正文。')]
    kept, _, _ = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert all('封面作者' not in row.get('text', '') for row in kept)
    assert any('真实摘要' in row.get('text', '') for row in kept)


def test_pdf_narrow_left_column_requires_boundary_review(tmp_path):
    import fitz
    from pdf_visual import guard_scope_columns
    path = tmp_path / 'columns.pdf'
    doc = fitz.open(); doc.new_page(width=600, height=800); doc.save(path); doc.close()
    left1 = {'id': 'l1', 'type': 'paragraph', 'text': 'research', 'position': {'page': 1, 'bbox': [50, 80, 90, 100]}}
    heading = {'id': 'h', 'type': 'paragraph', 'text': 'References', 'position': {'page': 1, 'bbox': [250, 120, 350, 140]}}
    ref = {'id': 'ref', 'type': 'paragraph', 'text': '[1] ref', 'position': {'page': 1, 'bbox': [100, 160, 580, 180]}}
    left2 = {'id': 'l2', 'type': 'paragraph', 'text': 'more research', 'position': {'page': 1, 'bbox': [50, 300, 90, 320]}}
    removed = [{'reason': 'references', 'block': row} for row in (heading, ref, left2)]
    issues = []
    guard_scope_columns(path, [left1, heading, ref, left2], [left1], removed, issues)
    assert any(row['code'] == 'pdf_column_removed_review' for row in issues)


def test_scanned_unheaded_citation_page_is_marked_and_withheld():
    from pdf_visual import suspected_ocr_citation_page
    from review_requirements import build_readiness
    from partial_projection import project_available_material
    lines = [{'text': '[1] Smith et al. 2020.'}, {'text': '[2] Other study 2019.'}]
    assert suspected_ocr_citation_page(lines)
    issues = [{'code': 'ocr_scope_suspect', 'position': {'page': 2}, 'message': 'suspected references'}]
    pending = build_readiness([], issues, {'start_kind': 'abstract'})['items']
    material = {'blocks': [{'id': 'safe', 'type': 'paragraph', 'text': 'Safe research', 'position': {'page': 1}},
                           {'id': 'ref', 'type': 'paragraph', 'text': '[1] reference', 'position': {'page': 2}},
                           {'id': 'tail', 'type': 'paragraph', 'text': 'uncertain tail', 'position': {'page': 3}}],
                'assets': [], 'sections': [], 'formulas': []}
    projected, _ = project_available_material(material, pending)
    content = json.dumps(projected, ensure_ascii=False)
    assert 'Safe research' in content
    assert '[1] reference' not in content and 'uncertain tail' not in content


@pytest.mark.parametrize('lines', [
    [{'text': '1. Smith, J. (2020). Research.'}, {'text': '2. Brown, K. (2019). Results.'}],
    [{'text': 'Smith, J. (2020). Research, Journal of Science.'}],
    [{'text': '[1] Brown et al. DOI: 10.1234/test'}],
])
def test_ocr_citation_shapes_with_missing_heading_are_flagged(lines):
    from pdf_visual import suspected_ocr_citation_page
    assert suspected_ocr_citation_page(lines)


def test_pdf_page_inspection_failure_does_not_stop_later_pages(tmp_path, monkeypatch):
    import fitz
    from pdf_visual import prepare_pages
    path = tmp_path / 'paper.pdf'; doc = fitz.open()
    for text in ('Abstract', 'damaged page', 'later page'):
        page = doc.new_page(); page.insert_text((40, 60), text)
    doc.save(path); doc.close()
    original = fitz.Page.get_drawings
    visited = []
    def sometimes_fails(page, *args, **kwargs):
        visited.append(page.number + 1)
        if page.number == 1:
            raise RuntimeError('local page failure')
        return original(page, *args, **kwargs)
    monkeypatch.setattr(fitz.Page, 'get_drawings', sometimes_fails)
    issues = []
    prepare_pages(path, [], [], [], issues, continue_on_uncertainty=True)
    assert 3 in visited
    assert any(row['code'] == 'pdf_page_incomplete' and row['position']['page'] == 2 for row in issues)


def test_failed_scan_crop_does_not_stop_later_scan_crop(tmp_path, monkeypatch):
    import fitz
    from pdf_visual import preserve_scans
    path = tmp_path / 'paper.pdf'; doc = fitz.open()
    for _ in range(2): doc.new_page()
    doc.save(path); doc.close()
    original = fitz.Page.get_pixmap
    def fail_first(page, *args, **kwargs):
        if page.number == 0: raise RuntimeError('first crop failed')
        return original(page, *args, **kwargs)
    monkeypatch.setattr(fitz.Page, 'get_pixmap', fail_first)
    kept = [{'id': str(n), 'type': 'paragraph', 'text': 'body',
             'position': {'page': n, 'bbox': [40, 60, 300, 100]}} for n in (1, 2)]
    issues = []; images = []
    preserve_scans(path, kept, [], {1, 2}, lambda ident, *args: {'id': ident}, images,
                   issues=issues, continue_on_uncertainty=True)
    assert any((row.get('position') or {}).get('page') == 2 for row in images)
    assert any(row['code'] == 'pdf_visual_incomplete' and row['position']['page'] == 1 for row in issues)


def test_thin_mark_in_original_raster_is_not_called_blank(tmp_path, monkeypatch):
    import io
    import fitz
    from PIL import Image
    import visual_ocr
    from pdf_visual import prepare_pages
    image = Image.new('RGB', (2000, 2000), 'white'); image.putpixel((501, 501), (0, 0, 0))
    stream = io.BytesIO(); image.save(stream, format='PNG')
    path = tmp_path / 'tiny.pdf'; doc = fitz.open(); page = doc.new_page(width=200, height=200)
    page.insert_image(page.rect, stream=stream.getvalue()); doc.save(path); doc.close()
    monkeypatch.setattr(visual_ocr, 'read_pdf_page', lambda page: [])
    issues = []; _, blank = prepare_pages(path, [], [], [], issues, continue_on_uncertainty=True)
    assert 1 not in blank
    assert any(row['code'] == 'ocr_page_incomplete' for row in issues)


def test_partitioned_partial_requests_each_keep_global_gap(tmp_path, monkeypatch):
    from docx import Document
    from PIL import Image
    import visual_ocr
    from local_prepare import prepare_source, plan_clean_material
    from test_capacity_plan import config
    monkeypatch.setattr(visual_ocr, 'read_image', lambda path: [{'text': 'research figure', 'confidence': .9}])
    figure = tmp_path / 'figure.png'; Image.new('RGB', (300, 300), 'blue').save(figure)
    source = tmp_path / 'body.docx'; doc = Document(); doc.add_heading('摘要', 1)
    for _ in range(8):
        doc.add_paragraph('研究方法与实验结果需要完整保留。' * 80)
    doc.add_picture(str(figure)); doc.save(source)
    catalog = tmp_path / 'catalog.json'; catalog.write_text('{"majors": []}', encoding='utf8')
    material = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path / 'work', catalog)
    limits = config(14000); limits['max_image_side'] = 32
    plan = plan_clean_material(material['material_path'], '模板', catalog, limits, tmp_path / 'plan')['plan']
    assert plan['mode'] == 'partitioned' and plan['source_completeness'] == 'partial'
    for part in plan['parts']:
        view = json.loads(part['body']['messages'][1]['content'][0]['text'])['material']
        assert view['reading_context']['source_completeness'] == 'partial'
        assert '完整读取' not in view['reading_context'].get('note', '')


def test_long_plain_text_without_punctuation_is_split_without_loss(tmp_path):
    from local_prepare import prepare_source, plan_clean_material
    from test_capacity_plan import config
    text = 'a' * 25000
    source = tmp_path / 'body.txt'; source.write_text('摘要\n' + text, encoding='utf8')
    catalog = tmp_path / 'catalog.json'; catalog.write_text('{"majors": []}', encoding='utf8')
    material = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path / 'work', catalog)
    plan = plan_clean_material(material['material_path'], '模板', catalog, config(14000), tmp_path / 'plan')['plan']
    assert plan['mode'] == 'partitioned'
    rows = [b for part in plan['parts'] for b in part['bundle']['blocks']
            if b.get('reading_role') == 'primary' and b.get('text', '').startswith('a')]
    assert ''.join(b['text'] for b in rows) == text


def test_registered_retry_direct_entry_keeps_receipt_binding(registered, tmp_path, monkeypatch):
    import local_prepare
    _, receipt_root, _, item = registered
    catalog = tmp_path / 'catalog.json'; catalog.write_text('{"majors": []}', encoding='utf8')
    work = tmp_path / 'work'
    original = local_prepare._prepare_source_unlocked
    monkeypatch.setattr(local_prepare, '_prepare_source_unlocked', lambda *args: (_ for _ in ()).throw(RuntimeError('failure')))
    with pytest.raises(RuntimeError):
        local_prepare.prepare_registered(item, receipt_root=receipt_root, work_dir=work, catalog_path=catalog)
    monkeypatch.setattr(local_prepare, '_prepare_source_unlocked', original)
    binding = json.loads((work / 'registered-binding.json').read_text(encoding='utf8'))
    (work / 'failure.json').write_text('{}', encoding='utf8')
    result = local_prepare.prepare_source(binding['source_path'], binding['source_sha256'], work, catalog)
    assert json.loads((Path(result['material_path']).parent / 'registered-binding.json').read_text(encoding='utf8')) == binding


def test_new_parser_version_reprocesses_old_cached_material_in_a_new_attempt(registered, tmp_path):
    from local_prepare import prepare_registered
    _, root, _, item = registered
    catalog = tmp_path / 'catalog.json'; catalog.write_text('{"majors": []}', encoding='utf8')
    work = tmp_path / 'work'
    first = prepare_registered(item, receipt_root=root, work_dir=work, catalog_path=catalog)
    old_path = Path(first['material_path'])
    old = json.loads(old_path.read_text(encoding='utf8'))
    old['parser_version'] = 'candidate-direct-clean/21'
    old_path.write_text(json.dumps(old, ensure_ascii=False), encoding='utf8')
    old_bytes = old_path.read_bytes()
    second = prepare_registered(item, receipt_root=root, work_dir=work, catalog_path=catalog)
    third = prepare_registered(item, receipt_root=root, work_dir=work, catalog_path=catalog)
    assert Path(second['material_path']) == work/'attempts'/'attempt-0001'/'material.json'
    assert third['material_path'] == second['material_path']
    assert old_path.read_bytes() == old_bytes
    assert json.loads(Path(second['material_path']).read_text(encoding='utf8'))['parser_version'] == 'candidate-direct-clean/25'


def test_offline_plan_keeps_exact_registered_identity(registered, tmp_path):
    from local_prepare import prepare_registered, plan_clean_material
    from test_capacity_plan import config
    _, root, _, item = registered
    catalog = tmp_path / 'catalog.json'; catalog.write_text('{"majors": []}', encoding='utf8')
    result = prepare_registered(item, receipt_root=root, work_dir=tmp_path / 'work', catalog_path=catalog)
    planned = plan_clean_material(result['material_path'], '模板', catalog, config(200000), tmp_path / 'plan')
    saved = json.loads(Path(planned['plan_path']).read_text(encoding='utf8'))
    assert saved['registered_identity']['paper_id'] == item['paper_id']
    assert saved['projection_version'] == 'partial-projection/2'
    assert saved['registered_identity']['receipt_id'] == item['receipt_id']
    assert saved['registered_identity']['binding_sha256']


@pytest.mark.parametrize('bad_box', [[400, 400, 80, 80], [100, 100, 100, 200],
                                     [100, 100, 200, 100], [float('nan'), 1, 2, 3]])
@pytest.mark.parametrize('bad_target', ['pending', 'asset'])
def test_invalid_visual_box_holds_same_page(bad_box, bad_target):
    from partial_projection import project_available_material
    asset_box = bad_box if bad_target == 'asset' else [80, 100, 400, 400]
    pending_box = bad_box if bad_target == 'pending' else [0, 0, 500, 2]
    position = {'page': 7, 'bbox': asset_box}
    material = {'blocks': [{'id': 'a', 'type': 'image', 'position': position}],
                'assets': [{'id': 'fig', 'kind': 'image', 'image_path': 'x.png',
                            'block_ids': ['a'], 'position': position}],
                'sections': [], 'formulas': []}
    projected, _ = project_available_material(material, [{'kind': 'unresolved_visual',
                                                          'position': {'page': 7, 'bbox': pending_box}}])
    assert not projected['assets'] and not projected['blocks']


def test_pending_visual_sanitizes_unlocated_inferred_caption_and_notes():
    from partial_projection import project_available_material
    material = {'blocks': [{'id': 't', 'type': 'table', 'text': '研究数据',
                            'position': {'page': 1, 'bbox': [30, 100, 300, 200]}}],
                'assets': [{'id': 'table', 'kind': 'table', 'block_ids': ['t'],
                            'position': {'page': 1, 'bbox': [30, 100, 300, 200]},
                            'caption': 'MUST_NOT_LEAK', 'structure': {'notes': 'MUST_NOT_LEAK'}}],
                'sections': [], 'formulas': []}
    pending = [{'kind': 'unresolved_visual', 'position': {'page': 2, 'bbox': [10, 10, 100, 100]}}]
    projected, _ = project_available_material(material, pending)
    assert 'MUST_NOT_LEAK' not in json.dumps(projected, ensure_ascii=False)
    assert '研究数据' in json.dumps(projected, ensure_ascii=False)


def test_caption_merged_with_ocr_from_another_page_is_not_native_caption_evidence():
    from partial_projection import project_available_material
    caption = '图 2 MUST_NOT_LEAK'
    image = {'page': 1, 'bbox': [80, 100, 400, 300]}
    caption_position = {'page': 1, 'bbox': [150, 305, 300, 320]}
    material = {'blocks': [{'id': 'fig', 'type': 'image', 'position': image},
                           {'id': 'caption', 'type': 'paragraph', 'text': caption,
                            'source': 'native_document', 'position': caption_position,
                            'source_segments': [{'position': caption_position},
                                                {'position': {'page': 2, 'bbox': [150, 10, 300, 30], 'ocr': True}}]}],
                'assets': [{'id': 'diagram', 'kind': 'image', 'image_path': 'x.png',
                            'block_ids': ['fig'], 'position': image, 'caption': caption}],
                'sections': [], 'formulas': []}
    projected, _ = project_available_material(material, [{'kind': 'unresolved_boundary', 'position': {'page': 3}}])
    assert projected['assets'][0]['caption'] == ''


def test_unlocated_visual_does_not_hide_available_ocr_or_other_page_text():
    from partial_projection import project_available_material
    position = {'page': 2, 'bbox': [10, 10, 100, 100]}
    material = {'blocks': [{'id': 'bad', 'type': 'paragraph', 'text': 'MUST_NOT_LEAK',
                            'source': 'cpu_ocr', 'position': {'page': 2, 'bbox': [20, 20, 80, 30], 'ocr': True}},
                           {'id': 'good', 'type': 'paragraph', 'text': 'safe research',
                            'position': {'page': 3, 'bbox': [10, 10, 100, 20]}},
                           {'id': 'fig', 'type': 'image', 'position': position}],
                'assets': [{'id': 'img', 'kind': 'image', 'image_path': 'x.png',
                            'block_ids': ['fig'], 'position': position}], 'sections': [], 'formulas': []}
    projected, _ = project_available_material(material, [{'kind': 'visual_scope', 'asset_id': 'img',
                                                           'position': position}])
    content = json.dumps(projected, ensure_ascii=False)
    assert 'MUST_NOT_LEAK' in content and 'safe research' in content


def test_unlocated_pending_visual_holds_visuals_and_ocr_not_native_body():
    from partial_projection import project_available_material
    pos = {'page': 2, 'bbox': [10, 10, 100, 100]}
    material = {'blocks': [{'id': 'ocr', 'type': 'paragraph', 'text': 'unknown OCR',
                            'source': 'cpu_ocr', 'position': {**pos, 'ocr': True}},
                           {'id': 'good', 'type': 'paragraph', 'text': 'native research', 'position': pos},
                           {'id': 'fig', 'type': 'image', 'position': pos}],
                'assets': [{'id': 'img', 'kind': 'image', 'image_path': 'x.png',
                            'block_ids': ['fig'], 'position': pos}], 'sections': [], 'formulas': []}
    projected, _ = project_available_material(material, [{'kind': 'unresolved_visual', 'position': {}}])
    content = json.dumps(projected, ensure_ascii=False)
    assert 'unknown OCR' not in content and not projected['assets'] and 'native research' in content


def test_unlocated_identified_image_does_not_hide_other_ocr_or_image():
    from partial_projection import project_available_material
    pos = {'page': 2, 'bbox': [10, 10, 100, 100]}
    material = {'blocks': [{'id': 'ocr', 'type': 'paragraph', 'text': 'unlocated OCR',
                            'source': 'cpu_ocr', 'position': {**pos, 'ocr': True}},
                           {'id': 'safe', 'type': 'paragraph', 'text': 'safe body', 'position': pos},
                           {'id': 'a', 'type': 'image', 'position': {}},
                           {'id': 'b', 'type': 'image', 'position': pos}],
                'assets': [{'id': 'unknown', 'kind': 'image', 'image_path': 'a.png', 'block_ids': ['a'], 'position': {}},
                           {'id': 'separate', 'kind': 'image', 'image_path': 'b.png', 'block_ids': ['b'], 'position': pos}],
                'sections': [], 'formulas': []}
    pending = [{'kind': 'visual_scope', 'asset_id': 'unknown', 'position': {}}]
    projected, _ = project_available_material(material, pending)
    content = json.dumps(projected, ensure_ascii=False)
    assert 'unlocated OCR' in content and 'safe body' in content
    assert [a['id'] for a in projected['assets']] == ['separate']


def test_page_rendered_crop_is_held_when_pending_drawing_may_extend_past_bbox():
    from partial_projection import project_available_material
    crop = {'page': 1, 'bbox': [80, 100, 190, 400]}
    material = {'blocks': [{'id': 'crop', 'type': 'image', 'position': crop}],
                'assets': [{'id': 'rendered', 'kind': 'pdf_vector_crop', 'image_path': 'x.png',
                            'block_ids': ['crop'], 'position': crop}], 'sections': [], 'formulas': []}
    projected, _ = project_available_material(material, [{'kind': 'unresolved_visual',
                                                          'position': {'page': 1, 'bbox': [200, 100, 300, 400]}}])
    assert not projected['assets'] and not projected['blocks']


def test_pdf_image_with_pixels_outside_visible_placement_is_pending(tmp_path):
    import io
    import fitz
    from PIL import Image, ImageDraw
    from partial_projection import pdf_image_clip_requirements, project_available_material
    image = Image.new('RGB', (200, 400), 'blue')
    ImageDraw.Draw(image).rectangle([0, 0, 199, 19], fill='red')
    raw = io.BytesIO(); image.save(raw, format='PNG')
    path = tmp_path / 'clipped.pdf'; pdf = fitz.open(); page = pdf.new_page(width=200, height=1000)
    page.insert_image(fitz.Rect(0, -20, 200, 380), stream=raw.getvalue())
    pdf.save(path); pdf.close()
    pdf = fitz.open(path); original = pdf[0].get_text('dict', sort=True)['blocks'][0]; pdf.close()
    position = {'page': 1, 'block_index': 0, 'bbox': list(original['bbox'])}
    material = {'source': {'file': str(path), 'parsed_view': {'format': 'pdf'}},
                'blocks': [{'id': 'imgblock', 'type': 'image', 'position': position},
                           {'id': 'safe', 'type': 'paragraph', 'text': '研究内容', 'position': {'page': 1}}],
                'assets': [{'id': 'img', 'kind': 'image', 'image_path': 'x.png',
                            'sha256': hashlib.sha256(original['image']).hexdigest(),
                            'position': position, 'block_ids': ['imgblock']}], 'sections': [], 'formulas': []}
    pending = pdf_image_clip_requirements(material)
    assert len(pending) == 1 and pending[0]['asset_id'] == 'img'
    projected, _ = project_available_material(material, pending)
    assert not projected['assets'] and any(row.get('text') == '研究内容' for row in projected['blocks'])


def test_planner_marks_new_visual_geometry_gap_partial(registered, tmp_path, monkeypatch):
    from local_prepare import prepare_registered, plan_clean_material
    import partial_projection
    from test_capacity_plan import config
    _, root, _, item = registered
    catalog = tmp_path / 'catalog.json'; catalog.write_text('{"majors": []}', encoding='utf8')
    result = prepare_registered(item, receipt_root=root, work_dir=tmp_path / 'work', catalog_path=catalog)
    monkeypatch.setattr(partial_projection, 'pdf_image_clip_requirements',
                        lambda material: [{'kind': 'visual_scope', 'asset_id': 'missing',
                                           'position': {'page': 50}, 'description': 'image geometry needs review'}])
    planned = plan_clean_material(result['material_path'], '模板', catalog, config(200000), tmp_path / 'plan')
    assert planned['plan']['source_completeness'] == 'partial'
    assert json.loads(Path(planned['plan_path']).read_text(encoding='utf8'))['projection_report']['pending_count'] == 0
    assert any(row.get('code') == 'pdf_image_clip_review' for row in planned['plan']['reading_gaps'])
