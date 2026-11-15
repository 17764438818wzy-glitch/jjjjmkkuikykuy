from reading_cleanup import clean_body_blocks


def test_split_chinese_abstract_and_toc_headings_preserve_abstract_not_toc():
    from test_relaxed_scope import para
    rows = [para('cover', '封面作者张某'), para('abs', '摘\n要'),
            para('abstract', '需要保留的中文摘要。' * 10),
            para('en', 'Abstract'), para('english', 'English abstract content.'),
            para('toc', '目\n录\n1 绪论..................1'),
            para('entry', '1.1 研究背景..............1\n1.2 研究意义..............2'),
            para('ch', '1 绪论'), para('sub', '1.1 研究背景'),
            para('body', '这里是需要保留的研究正文。' * 20),
            para('ref', '参\n考\n文\n献'), para('citation', '[1] End bibliography.')]
    for index, row in enumerate(rows):
        row['position'] = {'page': 1 if index == 0 else 2 if index < 3 else 3 if index < 5
                           else 4 if index < 7 else 5 if index < 10 else 6}
    kept, removed, scope = clean_body_blocks(rows, continue_on_uncertainty=True)
    content = '\n'.join(row.get('text', '') for row in kept)
    assert scope['abstract_source_id'] == 'abs'
    assert '需要保留的中文摘要' in content and '这里是需要保留的研究正文' in content
    assert '封面作者' not in content and '1.2 研究意义........' not in content
    assert 'End bibliography' not in content


def test_split_reference_headings_are_excluded_and_appendix_is_retained():
    from test_relaxed_scope import para
    for title in ('参考\n文献', '参\n考文献'):
        rows = [para('abs', '摘要'), para('text', '需要保留的研究正文。' * 20),
                para('tail', title), para('tailtext', 'UNWANTED END MATERIAL')]
        kept, _, _ = clean_body_blocks(rows, continue_on_uncertainty=True)
        assert 'UNWANTED' not in '\n'.join(r.get('text', '') for r in kept)
    rows = [para('abs', '摘要'), para('body', '正文'), para('ap', '附\n录 A'), para('extra', '附录数据')]
    kept, _, _ = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert '附录数据' in '\n'.join(r.get('text', '') for r in kept)


def test_split_heading_with_colon_without_inline_text_is_a_heading():
    from test_relaxed_scope import para
    rows = [para('cover', '封面'), para('abs', '摘\n要：'),
            para('body', '需要保留的摘要正文。' * 10)]
    kept, _, scope = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert scope['abstract_source_id'] == 'abs'
    assert any('需要保留' in row.get('text', '') for row in kept)


def test_split_toc_heading_with_colon_is_removed():
    from test_relaxed_scope import para
    rows = [para('abs', '摘要'), para('ab', '需要保留的摘要。'), para('toc', '目\n录：'),
            para('entry', '1 绪论..............1\n1.1 背景..............2'),
            para('ch', '1 绪论', heading=1), para('body', '研究正文。' * 30)]
    kept, _, _ = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert all('............' not in row.get('text', '') for row in kept)
    assert any('研究正文' in row.get('text', '') for row in kept)


def test_split_toc_entry_followed_by_dot_leader_does_not_start_abstract():
    from test_relaxed_scope import para
    rows = [para('fake', '摘\n要\n........1'), para('cover', '封面资料'),
            para('abs', '摘要'), para('body', '真正的摘要正文。' * 10)]
    kept, _, scope = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert scope['abstract_source_id'] == 'abs'
    assert all('封面资料' not in row.get('text', '') for row in kept)


def test_thin_decorative_line_in_multipage_toc_does_not_hide_following_body():
    from test_relaxed_scope import para
    rows = [para('abs', '摘要'), para('ab', '需要保留的摘要。'),
            para('toc', '目\n录\n1 绪论..............1'),
            para('entry', '1.1 研究背景..............1\n1.2 研究意义..............2'),
            {'id': 'thin', 'type': 'image', 'position': {'page': 3, 'bbox': [70, 69, 524, 69.8]}},
            para('more', '2 研究方法..............3\n2.1 具体方法..............4'),
            para('ch', '1 绪论'), para('body', '正常研究正文内容。' * 30)]
    for index, row in enumerate(rows):
        if row['id'] != 'thin':
            row['position'] = {'page': 1 if index < 2 else 2 if index < 4 else 3 if index == 5 else 4}
    kept, removed, _ = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert any('正常研究正文' in row.get('text', '') for row in kept)
    assert all(row['id'] != 'thin' for row in kept)
    assert any(row['block']['id'] == 'thin' for row in removed)


def test_unresolved_toc_image_is_marked_without_hiding_following_body():
    from test_relaxed_scope import para
    rows = [para('abs', '摘要'), para('ab', '需要保留的摘要。'),
            para('toc', '目录\n1 绪论..............1'),
            para('entry', '1.1 研究背景..............1\n1.2 研究意义..............2'),
            {'id': 'uncertain', 'type': 'image', 'position': {'page': 3, 'bbox': [70, 69, 524, 180]}},
            para('more', '2 研究方法..............3\n2.1 具体方法..............4'),
            para('ch', '1 绪论'), para('body', '正常研究正文内容。' * 30)]
    for index, row in enumerate(rows):
        if row['id'] != 'uncertain':
            row['position'] = {'page': 1 if index < 2 else 2 if index < 4 else 3 if index == 5 else 4}
    kept, _, scope = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert any('正常研究正文' in row.get('text', '') for row in kept)
    assert any(row['id'] == 'uncertain' for row in kept)
    assert any(row['source_block_id'] == 'uncertain' for row in scope['transition_visuals'])


def test_toc_heading_with_stray_prefix_is_removed():
    from test_relaxed_scope import para
    rows = [para('abs', '摘要'), para('ab', '需要保留的摘要。'),
            para('toc', '`目录'), para('entry', '1 绪论..............1\n1.1 研究背景..............2'),
            para('ch', '1 绪论', heading=1), para('body', '正常研究正文内容。' * 30)]
    for index, row in enumerate(rows):
        row['position'] = {'page': 1 if index < 2 else 2 if index < 4 else 3}
    kept, _, _ = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert any('正常研究正文' in row.get('text', '') for row in kept)
    assert all('........' not in row.get('text', '') and row['id'] != 'toc' for row in kept)


def block(ident, text, page):
    return {'id':ident, 'type':'paragraph', 'text':text,
            'position':{'page':page, 'bbox':[40, 100, 500, 120]}}


def test_multiple_numbered_toc_entries_in_one_pdf_block_do_not_hide_later_body():
    blocks = [block('abstract', '摘要', 1), block('summary', '研究摘要。', 1),
              block('toc', '目录', 2), block('entry1', '1\n绪论..........1', 2),
              block('entries', '1.1\n研究背景..........1\n2\n方法..........3', 2),
              block('thanks_entry', '致\n谢..........4', 2),
              block('chapter', '1\n绪论', 3),
              block('subheading', '1.1\n研究背景', 3),
              block('body', '本文介绍研究背景、方法与实验结果。' * 8, 3),
              block('refs', '参考文献', 4), block('tail', '[1] 不应交接的参考文献', 4)]
    kept, removed, _ = clean_body_blocks(blocks, continue_on_uncertainty=True)
    assert 'body' in {row['id'] for row in kept}
    assert 'entries' in {row['block']['id'] for row in removed}
    assert 'thanks_entry' in {row['block']['id'] for row in removed}
    assert 'tail' not in {row['id'] for row in kept}


def test_real_first_chapter_with_two_short_subheadings_is_not_mistaken_for_toc():
    blocks = [block('abstract', '摘要', 1), block('summary', '研究摘要。', 1),
              block('toc', '目录', 2), block('entry1', '1  绪论..........1', 2),
              block('entry2', '2  方法..........3', 2), block('chapter', '1  绪论', 3),
              block('heading1', '1.1 研究背景及意义', 3), block('heading2', '1.1.1 研究背景', 3),
              block('body1', '研究背景是很重要的内容。' * 3, 3),
              block('body2', '研究意义也是有价值的内容。' * 3, 3)]
    kept, _, _ = clean_body_blocks(blocks, continue_on_uncertainty=True)
    assert {'body1', 'body2'} <= {row['id'] for row in kept}


def test_toc_heading_and_first_entry_share_pdf_block():
    blocks = [block('abstract', '摘要', 1), block('summary', '研究摘要。', 1),
              block('toc', '目 录\n\n1  绪论 ........1', 2),
              block('entry', '2 方法 ........3', 2), block('chapter', '1 绪论', 3),
              block('body', '真实的研究正文。' * 12, 3)]
    kept, removed, _ = clean_body_blocks(blocks, continue_on_uncertainty=True)
    assert 'body' in {row['id'] for row in kept}
    assert 'toc' in {row['block']['id'] for row in removed}


def test_design_pdf_without_abstract_can_start_at_numbered_research_section():
    import pytest
    from reading_cleanup import ScopeUncertain
    visual=block('visual','[解析说明：此位置有待识别的矢量绘图。]',3)
    visual['position']['vector_region']=True
    blocks=[block('cover','封面',1),block('toc','CONTENTS',2),block('contents','选题背景',2),
            visual,block('first','壹\n选题背景',3),
            block('body','本文研究园林空间并介绍设计过程和实验结果。'*6,4)]
    kept, _, scope=clean_body_blocks(blocks,continue_on_uncertainty=True)
    assert 'body' in {row['id'] for row in kept}
    assert scope['start_kind']=='first_chapter_no_abstract'
    assert scope['transition_visuals'][0]['source_block_id']=='visual'
    with pytest.raises(ScopeUncertain):
        clean_body_blocks([blocks[0]]+blocks[3:],continue_on_uncertainty=True)


def test_transition_vector_before_first_real_chapter_is_recorded_without_stopping_body():
    vector = block('figure', '[解析说明：此位置有待识别的矢量绘图。]', 3)
    vector['position']['vector_region'] = True
    blocks = [block('abstract', '摘要', 1), block('summary', '研究摘要。', 1),
              block('toc', '目录', 2), block('entry1', '1 绪论 ........1', 2),
              block('entry2', '2 方法 ........3', 2), vector, block('chapter', '1 绪论', 3),
              block('body', '真实的研究正文。' * 12, 3)]
    kept, removed, scope = clean_body_blocks(blocks, continue_on_uncertainty=True)
    assert 'body' in {row['id'] for row in kept}
    assert 'figure' in {row['id'] for row in kept}
    assert scope['transition_visuals'][0]['position']['page'] == 3


def test_same_page_toc_transition_vector_does_not_stop_first_chapter():
    vector = block('figure', '[解析说明：此位置有待识别的矢量绘图。]', 2)
    vector['position']['vector_region'] = True
    blocks = [block('abstract', '摘要', 1), block('summary', '研究摘要。', 1),
              block('toc', '目录', 2), block('entry1', '1 绪论 ........1', 2),
              block('entry2', '2 方法 ........3', 2), vector, block('chapter', '1 绪论', 2),
              block('body', '真实的研究正文。' * 12, 2)]
    kept, _, scope = clean_body_blocks(blocks, continue_on_uncertainty=True)
    assert 'body' in {row['id'] for row in kept}
    assert scope['transition_visuals'][0]['position']['page'] == 2


def test_single_stray_word_between_toc_and_first_chapter_is_held_locally():
    def word(ident,text,index):
        return {'id':ident,'type':'paragraph','text':text,
                'position':{'part':'word/document.xml','body_index':index}}
    blocks = [word('abstract','摘要',0),word('summary','研究摘要。',1),word('toc','目录',2),
              word('entry1','1 绪论\t1',3),word('entry2','2 方法\t4',4),word('stray','而',5),
              word('chapter','1 绪论',6),word('sub','1.1 背景',7),
              word('body','研究过程与结果完整保留。'*12,8)]
    kept, removed, scope = clean_body_blocks(blocks,continue_on_uncertainty=True)
    assert 'body' in {row['id'] for row in kept}
    assert 'stray' in {row['id'] for row in kept}
    assert scope['transition_fragments'][0]['source_block_id'] == 'stray'


def test_longer_unlocated_transition_text_is_held_without_stopping_chapter():
    def word(ident,text,index):
        return {'id':ident,'type':'paragraph','text':text,
                'position':{'part':'word/document.xml','body_index':index}}
    blocks = [word('abstract','摘要',0),word('summary','研究摘要。',1),word('toc','目录',2),
              word('entry1','1 绪论\t1',3),word('entry2','2 方法\t4',4),word('stray','章节概述',5),
              word('chapter','1 绪论',6),word('body','研究正文完整保留。'*12,7)]
    kept, _, scope = clean_body_blocks(blocks,continue_on_uncertainty=True)
    assert 'body' in {row['id'] for row in kept}
    assert scope['transition_fragments'][0]['source_block_id'] == 'stray'


def test_multiple_transition_fragments_are_held_without_stopping_chapter():
    def word(ident,text,index):
        return {'id':ident,'type':'paragraph','text':text,
                'position':{'part':'word/document.xml','body_index':index}}
    blocks = [word('abstract','摘要',0),word('summary','研究摘要。',1),word('toc','目录',2),
              word('entry1','1 绪论\t1',3),word('entry2','2 方法\t4',4),
              word('stray1','章节概述',5),word('stray2','说明文字',6),
              word('chapter','1 绪论',7),word('body','研究正文完整保留。'*12,8)]
    kept, _, scope = clean_body_blocks(blocks,continue_on_uncertainty=True)
    assert 'body' in {row['id'] for row in kept}
    assert {row['source_block_id'] for row in scope['transition_fragments']} == {'stray1','stray2'}


def test_stray_word_is_reported_but_later_word_body_is_planned(tmp_path):
    import hashlib
    import json
    from pathlib import Path
    from docx import Document
    from local_prepare import prepare_source, plan_clean_material
    from test_capacity_plan import config
    source = tmp_path/'paper.docx'; doc = Document();doc.add_heading('摘要',1)
    for text in ('研究摘要。','目录','1 绪论\t1','2 方法\t4','罕','1 绪论','1.1 背景',
                 'KEY_BODY_研究过程与结果完整保留。'*12):
        doc.add_paragraph(text)
    doc.save(source)
    catalog = tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    result=prepare_source(source,hashlib.sha256(source.read_bytes()).hexdigest(),tmp_path/'work',catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert any(row['code']=='toc_transition_text' for row in material['issues'])
    assert any(row['kind']=='unresolved_boundary' for row in material['readiness']['items'])
    plan=plan_clean_material(result['material_path'],'模板',catalog,config(200000),tmp_path/'plan')['plan']
    assert plan['source_completeness']=='partial'
    assert 'KEY_BODY_' in json.dumps([part['body'] for part in plan['parts']],ensure_ascii=False)


def test_ocr_figure_text_saying_toc_after_chapter_does_not_hide_following_body():
    picture_text = block('figure_text', '目录', 4)
    picture_text['source'] = 'cpu_ocr';picture_text['position']['ocr'] = True
    picture_text['position']['bbox'] = [130,200,160,220]
    picture_text['position']['image_region'] = [100,100,400,400]
    caption = block('caption', '图3.4.2 品牌设计', 4)
    caption['position']['bbox'] = [180,405,300,420]
    blocks = [block('abstract', '摘要', 1), block('summary', '研究摘要。', 1),
              block('chapter', '第一章 绪论', 2), block('body', '研究过程。', 2),
              picture_text, block('label', '01 手机壳', 4), caption,
              block('later', '后续章节研究内容。', 5)]
    kept, _, _ = clean_body_blocks(blocks, continue_on_uncertainty=True)
    assert {'body', 'later'} <= {row['id'] for row in kept}


def test_numbered_abstract_does_not_certify_ocr_toc_as_figure():
    import pytest
    from reading_cleanup import ScopeUncertain
    toc = block('scan_toc', '目录', 2);toc['source']='cpu_ocr';toc['position']['ocr']=True
    entry1 = block('entry1', '第一章 绪论', 2);entry1['source']='cpu_ocr'
    entry2 = block('entry2', '第二章 方法', 2);entry2['source']='cpu_ocr'
    blocks = [block('abstract','摘要',1),block('numbered','1. 研究目的',1),toc,entry1,entry2,
              block('body','真实后续正文。'*15,3)]
    with pytest.raises(ScopeUncertain):
        clean_body_blocks(blocks,continue_on_uncertainty=True)


def test_unrelated_caption_on_same_page_does_not_certify_ocr_toc_as_figure():
    import pytest
    from reading_cleanup import ScopeUncertain
    toc=block('scan_toc','目录',2);toc['source']='cpu_ocr';toc['position']['ocr']=True
    toc['position']['bbox']=[100,80,150,100];toc['position']['image_region']=[40,40,300,200]
    unrelated=block('figure','图2.1 流程',2);unrelated['position']['bbox']=[200,600,350,620]
    blocks=[block('abstract','摘要',1),block('chapter','第一章 绪论',1),toc,
            block('entry','第二章 方法',2),unrelated,block('later','后续正文。'*15,3)]
    with pytest.raises(ScopeUncertain):
        clean_body_blocks(blocks,continue_on_uncertainty=True)


def test_neighboring_figure_tiles_form_one_region_without_losing_middle_tiles():
    import fitz
    from pdf_visual import grouped_image_regions
    doc=fitz.open();page=doc.new_page(width=600,height=800)
    boxes=[[95,87,303,238],[303,96,499,238],[95,245,300,394],
           [300,244,506,394],[95,404,292,546],[292,410,480,546]]
    regions=grouped_image_regions(page,[{'bbox':box} for box in boxes])
    assert len(regions)==1 and regions[0].contains(fitz.Point(142,278))
    assert regions[0].contains(fitz.Point(470,530))


def test_ocr_point_in_gap_between_tiles_is_not_claimed_by_figure():
    import fitz
    from pdf_visual import ocr_figure_region
    doc=fitz.open();page=doc.new_page(width=600,height=800)
    info=[{'bbox':[10,10,260,260]},{'bbox':[260,260,510,510]}]
    assert ocr_figure_region(page,info,fitz.Point(325,110)) is None
    assert ocr_figure_region(page,info,fitz.Point(100,100)) is not None


def test_rotated_pdf_image_regions_use_unrotated_ocr_coordinates():
    import io
    import fitz
    from PIL import Image
    from pdf_visual import grouped_image_regions
    stream=io.BytesIO();Image.new('RGB',(60,20),'blue').save(stream,format='PNG')
    for angle in (90,270):
        doc=fitz.open();page=doc.new_page(width=200,height=300)
        bounds=fitz.Rect(20,30,80,50);page.insert_image(bounds,stream=stream.getvalue());page.set_rotation(angle)
        regions=grouped_image_regions(page,page.get_image_info())
        assert len(regions)==1 and regions[0] == bounds


def test_year_at_start_of_acknowledgement_is_not_a_research_chapter():
    blocks = [block('abstract', '摘要', 1), block('body', '研究内容。', 1),
              block('thanks', '致谢', 2), block('year', '2023 年夏，我的求学经历。', 2)]
    kept, removed, _ = clean_body_blocks(blocks, continue_on_uncertainty=True)
    assert 'body' in {row['id'] for row in kept}
    assert 'year' in {row['block']['id'] for row in removed}


def test_ambiguous_toc_word_inside_appendix_does_not_drop_appendix():
    appendix_toc = block('appendix_toc', '目录', 3)
    appendix_toc['source'] = 'cpu_ocr';appendix_toc['position']['ocr'] = True
    appendix_label = block('appendix_label', '806 经巾', 3)
    appendix_label['source'] = 'cpu_ocr';appendix_label['position']['ocr'] = True
    appendix_label['ocr_confidence'] = .59
    blocks = [block('abstract', '摘要', 1), block('body', '研究内容。', 1),
              block('appendix', '附录', 3), appendix_toc, block('extra', '01-附件页', 3), appendix_label]
    kept, removed, _ = clean_body_blocks(blocks, continue_on_uncertainty=True)
    assert 'body' in {row['id'] for row in kept}
    assert {'appendix_toc','extra','appendix_label'} <= {row['id'] for row in kept}


def test_low_confidence_appendix_number_is_retained():
    label=block('label','101 试验方法',4);label.update(source='cpu_ocr',ocr_confidence=.59)
    label['position']['ocr']=True
    blocks=[block('abstract','摘要',1),block('body','研究正文。',1),block('appendix','附录',3),label]
    kept, _, _=clean_body_blocks(blocks,continue_on_uncertainty=True)
    assert 'label' in {row['id'] for row in kept}


def test_ocr_research_chapter_after_appendix_is_not_silently_discarded():
    import pytest
    from reading_cleanup import ScopeUncertain
    later = block('later','第二章 试验方法',4);later['source']='cpu_ocr';later['position']['ocr']=True
    blocks = [block('abstract','摘要',1), block('body','研究正文。',1),
              block('appendix','附录',3), block('extra','附录材料。',3), later,
              block('after','后续研究数据。',4)]
    kept, _, _ = clean_body_blocks(blocks,continue_on_uncertainty=True)
    assert {'extra', 'later', 'after'} <= {row['id'] for row in kept}
