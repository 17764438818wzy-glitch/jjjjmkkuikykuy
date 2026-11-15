from reading_cleanup import clean_body_blocks


def para(name, text, *, heading=0, page=1):
    return {'id': name, 'type': 'paragraph', 'text': text,
            'heading_level': heading, 'position': {'page': page}}


def test_front_toc_is_skipped_and_later_abstract_is_retained():
    rows = [para('cover', '封面作者'), para('toc', '目录'),
            para('toc_abstract', '摘要 ........ I'), para('toc_chapter', '第1章 ........ 1'),
            para('abstract', '摘要', heading=1, page=2), para('summary', '真实摘要。', page=2),
            para('body', '研究结果。', page=3), para('refs', '参考文献', heading=1, page=4),
            para('reference', '[1] 不交接的文献', page=4)]
    kept, removed, scope = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert [r['id'] for r in kept] == ['abstract', 'summary', 'body']
    assert 'reference' in [r['block']['id'] for r in removed]
    assert scope['abstract_source_id'] == 'abstract'


def test_short_inline_abstract_with_keywords_is_processed():
    rows = [para('cover', '封面作者'), para('abstract', '摘要：分析模型性能。'),
            para('keywords', '关键词：模型；性能'), para('body', '研究结果。')]
    kept, _, scope = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert [r['id'] for r in kept] == ['abstract', 'keywords', 'body']
    assert scope['abstract_source_id'] == 'abstract'


def test_missing_footnote_does_not_stop_remaining_body():
    rows = [para('abstract', '摘要'), para('body', '正文引用[^footnoteReference1]。'),
            para('after', '研究结论仍保留。')]
    kept, _, scope = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert '研究结论仍保留。' in [r['text'] for r in kept]
    assert scope['missing_note_labels'] == ['footnoteReference1']


def test_unique_styled_introduction_can_start_when_abstract_missing():
    rows = [para('cover', '封面作者'), para('intro', '绪论', heading=1),
            para('body', '本文分析研究方法和实验结果。' * 5)]
    kept, _, scope = clean_body_blocks(rows, continue_on_uncertainty=True)
    assert [r['id'] for r in kept] == ['intro', 'body']
    assert scope['start_kind'] == 'first_chapter_no_abstract'


def test_body_appendix_mention_without_punctuation_does_not_stop():
    rows=[para('a','摘要'),para('b','正文'),para('mention','附录 A 中的数据表明该方法有效'),
          para('later','后续正文')]
    kept,_,scope=clean_body_blocks(rows,continue_on_uncertainty=True)
    assert [row['id'] for row in kept]==['a','b','mention','later']
    assert scope['abstract_source_id']=='a'

def test_repeated_standalone_abstract_heading_does_not_stop_body():
    rows=[para('a','摘要',heading=1,page=1),para('summary','摘要内容。',page=1),
          para('repeat','摘要',page=2),para('body','真实研究内容。',page=2)]
    kept,removed,scope=clean_body_blocks(rows,continue_on_uncertainty=True)
    assert 'body' in [r['id'] for r in kept]
    assert 'repeat' in [r['block']['id'] for r in removed]
    assert scope['duplicate_abstract_count']==1

def test_clear_english_cross_page_continuation_preserves_source_ranges():
    from reading_cleanup import map_clean_range, normalize_layout
    rows=[para('left','The findings suggest,',page=1),para('right','this method is effective.',page=2)]
    rows[0]['position']['bbox']=[50,830,550,980];rows[1]['position']['bbox']=[50,40,550,140]
    kept,changes=normalize_layout(rows,page_heights={1:1000,2:1000})
    assert [b['text'] for b in kept]==['The findings suggest, this method is effective.']
    spans=map_clean_range(kept[0],0,len(kept[0]['text']))
    assert [span['source_block_id'] for span in spans]==['left','right']
    assert any(change['kind']=='cross_page_join' for change in changes)

def test_two_identical_extreme_page_headers_can_be_removed():
    from local_prepare import _native_paths
    _native_paths()
    from preextract_material import remove_repeated_margins
    rows=[para('h1','学位论文',page=1),para('body1','研究正文。',page=1),
          para('h2','学位论文',page=2),para('body2','研究结论。',page=2)]
    rows[0]['position']['bbox']=[100,10,300,30];rows[2]['position']['bbox']=[100,10,300,30]
    rows[1]['position']['bbox']=[50,100,550,200];rows[3]['position']['bbox']=[50,100,550,200]
    kept,removed=remove_repeated_margins(rows,{1:1000,2:1000})
    assert [r['id'] for r in kept]==['body1','body2']
    assert {r['block']['id'] for r in removed}=={'h1','h2'}

def test_short_toc_does_not_block_a_clear_following_chapter():
    rows=[para('a','摘要',page=1),para('summary','摘要内容。',page=1),
          para('toc','目录',page=1),para('entry','参考文献',page=1),
          para('h','1 绪论',heading=1,page=2),para('b','本文分析研究方法并给出实验结果。' * 5,page=2)]
    kept,removed,scope=clean_body_blocks(rows,continue_on_uncertainty=True)
    assert 'b' in [r['id'] for r in kept]
    assert 'entry' in [r['block']['id'] for r in removed]

def test_mid_body_reference_material_heading_does_not_truncate_research():
    rows=[para('a','摘要'),para('b','研究摘要。'),para('h','参考资料',heading=1),
          para('desc','本节解释实验资料的来源和用途。'),para('chapter','第二章 实验',heading=1),
          para('result','实验结果继续保留。'),para('refs','参考文献',heading=1),para('cit','[1] 末尾文献')]
    kept,removed,_=clean_body_blocks(rows,continue_on_uncertainty=True)
    assert 'result' in [r['id'] for r in kept]
    assert 'desc' in [r['id'] for r in kept]
    assert 'cit' in [r['block']['id'] for r in removed]

def test_preserved_merged_table_continuation_is_only_warning():
    from review_requirements import build_readiness
    assets=[{'id':'table','kind':'table','image_path':None,'position':{'body_index':4},
             'structure':{'cells':[{'text':'A','continuation_text':'B'}]}}]
    issues=[{'code':'table_continuation_review','position':{'body_index':4},'message':'续格已保留'}]
    readiness=build_readiness(assets,issues,{'start_kind':'abstract'})
    assert readiness['state']=='ready'
