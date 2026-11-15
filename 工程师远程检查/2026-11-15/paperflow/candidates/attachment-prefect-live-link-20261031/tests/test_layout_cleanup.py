from reading_cleanup import normalize_layout


def p(ident, text, page, bbox=None, **fields):
    return {'id': ident, 'type': 'paragraph', 'text': text, 'heading_level': 0,
            'position': {'page': page, **({'bbox': bbox} if bbox else {})}, **fields}


def test_empty_paragraphs_are_removed_but_table_empty_cells_and_images_remain():
    rows = [p('blank', '    \n\n', 1),
            {'id': 'table', 'type': 'table', 'cells': [{'text': ''}, {'text': '3'}], 'position': {'page': 1}},
            {'id': 'image', 'type': 'image', 'resource_id': 'figure', 'position': {'page': 2}}]
    kept, changes = normalize_layout(rows)
    assert [b['id'] for b in kept] == ['table', 'image']
    assert kept[0]['cells'][0]['text'] == ''
    assert any(c['kind'] == 'empty_paragraph' and c['block_id'] == 'blank' for c in changes)


def test_wrapped_chinese_and_english_are_rejoined_without_squashing_words():
    rows = [p('zh', '这是第一行，\n下一行仍是同一段。', 1),
            p('en', 'The first line\ncontinues here.', 1),
            p('spaces', '研究    数据', 1)]
    kept, changes = normalize_layout(rows)
    assert [b['text'] for b in kept] == ['这是第一行，下一行仍是同一段。', 'The first line continues here.', '研究 数据']
    assert {c['block_id'] for c in changes} == {'zh', 'en', 'spaces'}


def test_headings_lists_code_and_formula_whitespace_are_preserved():
    rows = [p('heading', '第一章\n研究方法', 1, heading_level=1),
            p('list', '1. First\n2. Second', 1, list={'format': 'decimal'}),
            p('code', '```\n x  =  2\n```', 1),
            p('formula', '$a  +  b$\n= c', 1)]
    kept, _ = normalize_layout(rows)
    assert [b['text'] for b in kept] == [b['text'] for b in rows]


def test_indented_code_keeps_whitespace_without_markdown_fences():
    row = p('code', 'if enabled:\n    return  value', 1)
    kept, _ = normalize_layout([row])
    assert kept[0]['text'] == row['text']


def test_clear_english_page_continuation_keeps_both_source_locations():
    rows = [p('left', 'The findings suggest,', 1, [50, 830, 550, 980]),
            p('right', 'this method is effective.', 2, [50, 40, 550, 140])]
    kept, changes = normalize_layout(rows, page_heights={1: 1000, 2: 1000})
    assert [r['text'] for r in kept] == ['The findings suggest, this method is effective.']
    from reading_cleanup import map_clean_range
    assert [s['position']['page'] for s in map_clean_range(kept[0],0,len(kept[0]['text']))] == [1, 2]
    assert any(c['kind'] == 'cross_page_join' for c in changes)


def test_ambiguous_cross_page_boundaries_are_not_joined():
    rows = [p('left', '该方法有效。', 1, [50, 800, 550, 980]),
            p('right', '新的一段。', 2, [50, 30, 550, 100])]
    kept, _ = normalize_layout(rows, page_heights={1: 1000, 2: 1000})
    assert [b['id'] for b in kept] == ['left', 'right']


def test_uncertain_english_hyphenation_is_not_rewritten():
    row = p('hyphen', 'cross-\nsectional analysis', 1)
    kept, _ = normalize_layout([row])
    assert kept[0]['text'] == row['text']


def test_native_pdf_heading_without_style_is_not_joined_with_following_text():
    row = p('head', '第1章 绪论\n研究方法', 1)
    kept, _ = normalize_layout([row])
    assert kept[0]['text'] == row['text']


def test_normalized_range_maps_through_scope_to_original_offsets():
    from reading_cleanup import map_clean_range
    raw = '封面\n摘要\n甲  \n 乙'
    from reading_cleanup import clean_body_blocks
    kept, _, _ = clean_body_blocks([p('mixed', raw, 1)])
    # Test a changed subsection of a retained source block.
    row = kept[0]
    row['text'] = row['text'].replace('摘要\n', '')
    row['source_scope']['start'] += len('摘要\n')
    normalized, _ = normalize_layout([row])
    assert normalized[0]['text'] == '甲乙'
    spans = map_clean_range(normalized[0], 0, 2)
    assert [(s['source_start'], s['source_end']) for s in spans] == [
        (raw.index('甲'), raw.index('甲') + 1), (raw.index('乙'), raw.index('乙') + 1)]
    assert all(s['source_block_id'] == 'mixed' for s in spans)


def test_unambiguous_chinese_cross_page_join_maps_to_both_pages():
    from reading_cleanup import map_clean_range
    rows = [p('left2', '研究结果显示，', 1, [50, 830, 550, 980]),
            p('right2', '该方法具有较高效率。', 2, [50, 40, 550, 140])]
    kept, changes = normalize_layout(rows, page_heights={1: 1000, 2: 1000})
    assert [row['text'] for row in kept] == ['研究结果显示，该方法具有较高效率。']
    spans = map_clean_range(kept[0], 0, len(kept[0]['text']))
    assert [(s['source_block_id'], s['position']['page']) for s in spans] == [('left2', 1), ('right2', 2)]
    assert any(c['kind'] == 'cross_page_join' for c in changes)


def test_cross_page_following_same_page_does_not_lose_source_mapping():
    from reading_cleanup import map_clean_range
    rows=[p('a','甲，',1,[50,830,550,980]),p('b','乙，',2,[50,40,550,140]),
          p('c','丙。',2,[50,45,550,150])]
    kept,_=normalize_layout(rows,page_heights={1:1000,2:1000})
    assert [b['text'] for b in kept]==['甲，乙，','丙。']
    spans=map_clean_range(kept[0],0,len(kept[0]['text']))
    assert [s['source_block_id'] for s in spans]==['a','b']


def test_three_page_continuation_retains_three_original_sources():
    from reading_cleanup import map_clean_range
    rows=[p('a','甲，',1,[50,830,550,980]),p('b','乙，',2,[50,40,550,980]),
          p('c','丙。',3,[50,40,550,140])]
    kept,_=normalize_layout(rows,page_heights={1:1000,2:1000,3:1000})
    assert [b['text'] for b in kept]==['甲，乙，丙。']
    assert [s['source_block_id'] for s in map_clean_range(kept[0],0,len(kept[0]['text']))]==['a','b','c']
