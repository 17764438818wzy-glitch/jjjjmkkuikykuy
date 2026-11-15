import pytest

from reading_cleanup import ScopeUncertain, clean_body_blocks


def paragraph(name, text, *, heading=0, page=1):
    return {'id': name, 'type': 'paragraph', 'text': text,
            'heading_level': heading, 'position': {'page': page}}


def test_excludes_front_and_tail_sections_without_deleting_body_citations():
    blocks = [
        paragraph('cover', '封面：作者'),
        paragraph('ab', '摘要', heading=1),
        paragraph('a', '研究问题与结果。'),
        paragraph('body', '本文引用文献[12]，附录内容在此仅为讨论。'),
        paragraph('refs', '参考文献', heading=1),
        paragraph('r', '[12] 被引用研究'),
        paragraph('appendix', '附录 A 问卷', heading=1),
        paragraph('ax', '附录中的数据'),
        paragraph('ack', '致谢', heading=1),
        paragraph('thanks', '感谢导师'),
    ]
    kept, removed, decisions = clean_body_blocks(blocks)
    assert [b['text'] for b in kept] == ['摘要', '研究问题与结果。', '本文引用文献[12]，附录内容在此仅为讨论。', '附录 A 问卷', '附录中的数据']
    assert {r['block']['id'] for r in removed} == {'cover', 'refs', 'r', 'ack', 'thanks'}
    assert decisions['abstract_source_id'] == 'ab'


def test_inline_abstract_keeps_original_offsets_and_drops_prefix():
    blocks = [paragraph('mixed', '封面信息\n摘要\n正文摘要内容'),
              paragraph('chapter', '第 1 章 研究', heading=1),
              paragraph('text', '正文保留。')]
    kept, removed, decisions = clean_body_blocks(blocks)
    assert kept[0]['text'] == '摘要\n正文摘要内容'
    assert kept[0]['source_scope']['start'] == len('封面信息\n')
    assert removed[0]['block']['text'] == '封面信息\n'
    assert decisions['abstract_source_id'] == 'mixed'


def test_toc_before_abstract_without_proven_boundary_needs_review():
    blocks = [paragraph('toc', '目录'), paragraph('toc_ab', '摘要 ........ I'),
              paragraph('toc_body', '第1章 ......... 1'), paragraph('cover', '宣誓内容'),
              paragraph('actual', '摘 要', heading=1), paragraph('text', '真正摘要。')]
    with pytest.raises(ScopeUncertain):
        clean_body_blocks(blocks)


def test_missing_abstract_is_not_interpreted_as_clean_body():
    with pytest.raises(ScopeUncertain):
        clean_body_blocks([paragraph('title', '封面'), paragraph('body', '第一章 正文')])


def test_research_heading_after_excluded_section_requires_review():
    blocks = [paragraph('a', '摘要'), paragraph('b', '摘要内容。'),
              paragraph('x', '参考文献', heading=1), paragraph('r', '[1] 某文献。'),
              paragraph('chapter', '第 5 章 实验', heading=1), paragraph('result', '研究结果。')]
    with pytest.raises(ScopeUncertain):
        clean_body_blocks(blocks)


def test_excluded_section_contains_its_images_and_tables():
    blocks = [paragraph('abstract', '摘要'), paragraph('body', '正文。'),
              paragraph('tail', '参考文献', heading=1),
              {'id': 'appendix_table', 'type': 'table', 'cells': [{'text': '排除表格'}], 'position': {'page': 3}},
              {'id': 'appendix_image', 'type': 'image', 'resource_id': 'im', 'position': {'page': 3}}]
    kept, removed, _ = clean_body_blocks(blocks)
    assert [b['id'] for b in kept] == ['abstract', 'body']
    assert {r['block']['id'] for r in removed} == {'tail', 'appendix_table', 'appendix_image'}


def test_cross_page_toc_entry_cannot_be_mistaken_for_abstract():
    blocks = [paragraph('toc', '目录', page=1), paragraph('entry', '摘要', heading=1, page=2),
              paragraph('declaration', '声明：封面材料', page=2), paragraph('actual', '摘要', heading=1, page=3)]
    with pytest.raises(ScopeUncertain):
        clean_body_blocks(blocks)


def test_toc_reference_entry_cannot_truncate_following_research():
    blocks = [paragraph('a', '摘要'), paragraph('summary', '摘要内容'), paragraph('toc', '目录'),
              paragraph('entry', '参考文献'), paragraph('h', '1 绪论', heading=1), paragraph('b', '正文内容')]
    with pytest.raises(ScopeUncertain):
        clean_body_blocks(blocks)


def test_appendix_internal_chapter_is_retained():
    blocks = [paragraph('a', '摘要'), paragraph('b', '正文'), paragraph('ap', '附录 A', heading=1),
              paragraph('inside', '第1章 问卷设计', heading=1), paragraph('survey', '问卷内容')]
    kept, _, _ = clean_body_blocks(blocks)
    assert {'ap', 'inside', 'survey'} <= {b['id'] for b in kept}


def test_research_table_and_formula_with_text_are_not_silently_dropped():
    blocks = [paragraph('a', '摘要'),
              {'id': 't', 'type': 'table', 'text': 'a\tb', 'position': {'page': 2}},
              {'id': 'f', 'type': 'formula', 'text': 'x+y', 'position': {'page': 2}}]
    kept, _, _ = clean_body_blocks(blocks)
    assert [b['id'] for b in kept] == ['a', 't', 'f']


def test_mixed_boundary_with_inline_images_requires_review():
    mixed = paragraph('m', '封面[[IMAGE:c]]\n摘要\n正文[[IMAGE:r]]')
    mixed['resource_ids'] = ['c', 'r']
    with pytest.raises(ScopeUncertain):
        clean_body_blocks([mixed])


def test_footnotes_follow_kept_references_instead_of_tail_phase():
    blocks = [paragraph('a', '摘要'), paragraph('b', '正文说明[^footnoteReference1]。'),
              paragraph('x', '附录', heading=1), paragraph('ap', '附录[^footnoteReference2]。'),
              {'id': 'n1', 'type': 'paragraph', 'text': '[^footnoteReference1]: 正文脚注',
               'note_label': 'footnoteReference1', 'position': {'part': 'word/footnotes.xml', 'note_id': '1'}},
              {'id': 'n2', 'type': 'paragraph', 'text': '[^footnoteReference2]: 附录脚注',
               'note_label': 'footnoteReference2', 'position': {'part': 'word/footnotes.xml', 'note_id': '2'}}]
    kept, removed, _ = clean_body_blocks(blocks)
    assert 'n1' in [b['id'] for b in kept]
    assert 'n2' in [b['id'] for b in kept]


@pytest.mark.parametrize('heading', ['第六章 参考文献', '6 参考文献', '参考文献（References）', '鸣谢', '谢辞'])
def test_common_tail_heading_forms_do_not_reach_reading_view(heading):
    blocks = [paragraph('a', '摘要'), paragraph('b', '正文'), paragraph('h', heading, heading=1),
              paragraph('tail', '待排除的章节内容')]
    kept, _, _ = clean_body_blocks(blocks)
    assert '待排除的章节内容' not in [b['text'] for b in kept]


@pytest.mark.parametrize('heading', ['References Cited', 'Works Cited', '参考文献及注释'])
def test_additional_reference_endings_are_excluded(heading):
    rows = [paragraph('ab', '摘要'), paragraph('body', '正文'),
            paragraph('refs', heading, heading=1), paragraph('citation', '条目不得发送')]
    kept, _, _ = clean_body_blocks(rows)
    assert '条目不得发送' not in [r['text'] for r in kept]


def test_appendix_mentioned_in_body_sentence_is_not_a_heading():
    blocks = [paragraph('a', '摘要'), paragraph('b', '正文'),
              paragraph('sentence', '附录 A 中的数据表明该方法有效。'), paragraph('after', '之后仍是正文。')]
    kept, _, _ = clean_body_blocks(blocks)
    assert kept[-1]['text'] == '之后仍是正文。'


def test_removed_source_order_is_original_block_order():
    blocks = [paragraph('cover', '封面'), paragraph('a', '摘要'), paragraph('b', '正文'),
              paragraph('h', '致谢', heading=1), paragraph('x', '感谢')]
    _, removed, _ = clean_body_blocks(blocks)
    assert [(r['block']['id'], r['source_order']) for r in removed] == [('cover', 0), ('h', 3), ('x', 4)]


def test_cover_summary_field_before_real_abstract_is_held():
    rows = [paragraph('cover', '摘要：请填写'), paragraph('cover2', '封面保密内容'),
            paragraph('abstract', '摘要', heading=1), paragraph('body', '真正的摘要内容')]
    with pytest.raises(ScopeUncertain):
        clean_body_blocks(rows)


@pytest.mark.parametrize('heading', ['第8章 附录', '8 附录', '附录 1. 问卷'])
def test_additional_appendix_headings_are_retained(heading):
    rows = [paragraph('ab', '摘要'), paragraph('body', '研究正文'),
            paragraph('tail', heading, heading=1), paragraph('tail_content', '不能交接的材料')]
    kept, _, _ = clean_body_blocks(rows)
    assert '不能交接的材料' in [row['text'] for row in kept]


def test_additional_reference_heading_is_excluded():
    rows = [paragraph('ab', '摘要'), paragraph('body', '研究正文'),
            paragraph('tail', '主要参考文献', heading=1), paragraph('entry', '文献条目')]
    kept, _, _ = clean_body_blocks(rows)
    assert '文献条目' not in [row['text'] for row in kept]


def test_ambiguous_appendix_sentence_without_punctuation_is_not_excluded():
    rows = [paragraph('ab', '摘要'), paragraph('body', '研究正文'),
            paragraph('sentence', '附录 A 中的数据表明该方法有效'),
            paragraph('later', '后续正文')]
    kept, _, _ = clean_body_blocks(rows)
    assert '后续正文' in [row['text'] for row in kept]


def test_unstyled_appendix_after_confirmed_end_sections_is_retained():
    rows = [paragraph('ab', '摘要'), paragraph('body', '研究正文'),
            paragraph('refs', '参考文献'), paragraph('entry', '[1] 文献'),
            paragraph('thanks', '致谢'), paragraph('appendix', '附录 相关设计图纸'),
            paragraph('design', '附录材料')]
    kept, removed, _ = clean_body_blocks(rows)
    assert [row['id'] for row in kept] == ['ab', 'body', 'appendix', 'design']
    assert {'refs', 'entry', 'thanks'}.issubset({row['block']['id'] for row in removed})


def test_mixed_scope_with_formula_requires_review():
    with pytest.raises(ScopeUncertain):
        clean_body_blocks([paragraph('mixed', '封面公式 $private$\n摘要\n摘要内容')])


def test_missing_referenced_footnote_cannot_silently_pass():
    with pytest.raises(ScopeUncertain):
        clean_body_blocks([paragraph('ab', '摘要'), paragraph('body', '说明[^footnoteReference8]')])


def test_docx_toc_can_end_at_repeated_actual_heading_with_following_body():
    rows = [paragraph('ab', '摘要'), paragraph('sum', '摘要内容'),
            paragraph('toc', '目　录', page=None), paragraph('blank', '', page=None),
            paragraph('e1', '1 绒论\t1', page=None), paragraph('e2', '2 方法\t3', page=None),
            paragraph('spacer', '', page=None),
            paragraph('real', '1 绒论', heading=1, page=None),
            paragraph('realtext', '这是有足够上下文的研究正文。' * 4, page=None)]
    kept, removed, _ = clean_body_blocks(rows)
    assert [row['id'] for row in kept] == ['ab', 'sum', 'real', 'realtext']
    assert {'toc', 'e1', 'e2'}.issubset({row['block']['id'] for row in removed})


def test_toc_cannot_end_merely_because_a_label_is_repeated():
    rows = [paragraph('ab', '摘要'), paragraph('toc', '目录', page=None),
            paragraph('e1', '1 绒论\t1', page=None), paragraph('e2', '2 方法\t3', page=None),
            paragraph('repeated', '1 绒论', page=None), paragraph('bad', '2 方法\t3', page=None)]
    with pytest.raises(ScopeUncertain):
        clean_body_blocks(rows)


def test_pdf_toc_rows_with_wrapped_number_can_end_at_matching_heading():
    rows = [paragraph('ab', '摘要', page=1), paragraph('sum', '摘要内容', page=1),
            paragraph('toc', '目录', page=2), paragraph('e1', '绪论...........1', page=2),
            paragraph('e2', '1\n研究方法.........2', page=2),
            paragraph('real', '绪论', page=3), paragraph('sub', '1.研究背景', page=3),
            paragraph('text1', '真实研究内容' * 8, page=3),
            paragraph('text2', '真实研究内容' * 8, page=3)]
    kept, _, _ = clean_body_blocks(rows)
    assert [b['id'] for b in kept] == ['ab', 'sum', 'real', 'sub', 'text1', 'text2']


def test_same_page_unpaged_toc_duplicate_cannot_start_body():
    rows = [paragraph('ab', '摘要'), paragraph('toc', '目录', page=2),
            paragraph('e1', '1 绪论\t1', page=2), paragraph('e2', '2 方法\t2', page=2),
            paragraph('duplicate', '1 绪论', heading=1, page=2),
            paragraph('long_unpaged_entry', '目录中被拆开的长标题' * 8, page=2),
            paragraph('refs', '参考文献', page=2), paragraph('real', '1 绪论', heading=1, page=3)]
    with pytest.raises(ScopeUncertain):
        clean_body_blocks(rows)


def test_numbered_research_chapter_after_tail_requires_review():
    rows = [paragraph('ab', '摘要'), paragraph('body', '正文'),
            paragraph('refs', '参考文献'), paragraph('entry', '[1] 条目'),
            paragraph('real', '1 绪论', heading=1), paragraph('research', '研究正文')]
    with pytest.raises(ScopeUncertain):
        clean_body_blocks(rows)


def test_unstyled_word_toc_ends_at_repeated_heading_with_real_prose():
    blocks = [paragraph('ab', '摘要', page=None), paragraph('summary', '摘要内容。', page=None),
              paragraph('toc', '目录', page=None), paragraph('e1', '绪论 ........ 1', page=None),
              paragraph('e2', '第2章 方法 ........ 5', page=None), paragraph('intro', '绪论', page=None),
              paragraph('body', '这是研究的背景和目标，讨论研究方法与实际应用。' * 4, page=None)]
    for index, block in enumerate(blocks):
        block['position']['body_index'] = index
    kept, removed, _ = clean_body_blocks(blocks)
    assert {'intro', 'body'}.issubset({b['id'] for b in kept})
    assert {'toc', 'e1', 'e2'}.issubset({r['block']['id'] for r in removed})


def test_alternate_reference_heading_excludes_list_and_its_images():
    rows = [paragraph('ab', '摘要'), paragraph('body', '研究正文。'),
            paragraph('ref', '参考书目', heading=1), paragraph('citation', '论文引文内容'),
            {'id': 'ref_image', 'type': 'image', 'resource_id': 'ref_pic', 'position': {'page': 3}}]
    kept, removed, _ = clean_body_blocks(rows)
    assert {r['block']['id'] for r in removed}.issuperset({'ref', 'citation', 'ref_image'})
    assert {b['id'] for b in kept} == {'ab', 'body'}


def test_unstyled_inline_abstract_with_keywords_and_substantial_text_is_kept():
    body = '本文针对实际问题提出研究方法，并通过实验验证所得结果。' * 4
    rows = [paragraph('cover', '论文封面'), paragraph('ab', '摘要：' + body),
            paragraph('keywords', '关键词：方法；实验'), paragraph('chapter', '第1章 绪论', heading=1),
            paragraph('content', '正文内容。')]
    kept, removed, _ = clean_body_blocks(rows)
    assert body in ''.join(b.get('text', '') for b in kept)
    assert [r['block']['id'] for r in removed] == ['cover']


def test_reference_year_and_page_range_are_not_research_chapter():
    rows = [paragraph('ab', '摘要'), paragraph('body', '正文。'),
            paragraph('ref', '参考文献', heading=1),
            paragraph('citation', '[1] Example journal.'), paragraph('year', '2021. 3~5.')]
    kept, removed, _ = clean_body_blocks(rows)
    assert {b['id'] for b in kept} == {'ab', 'body'}
    assert {r['block']['id'] for r in removed}.issuperset({'ref', 'citation', 'year'})


def test_first_styled_chapter_can_start_body_when_no_abstract_exists():
    rows = [paragraph('cover', '论文封面'), paragraph('toc', '目录'),
            paragraph('entry', '第一章 绪论 ........ 1'),
            paragraph('chapter', '第一章 绪论', heading=1),
            paragraph('content', '本文讨论实际问题，介绍研究方法并分析其应用。' * 4)]
    kept, removed, decision = clean_body_blocks(rows)
    assert [b['id'] for b in kept] == ['chapter', 'content']
    assert {r['block']['id'] for r in removed} == {'cover', 'toc', 'entry'}
    assert decision['start_kind'] == 'first_chapter_no_abstract'


def test_content_abstract_is_not_discarded_as_front_matter():
    rows = [paragraph('cover','封面'), paragraph('ab','内容摘要',heading=1),
            paragraph('summary','应保留的摘要。'), paragraph('chapter','第一章 绪论',heading=1),
            paragraph('body','正文内容。'*20)]
    kept, removed, decision=clean_body_blocks(rows)
    assert {'ab','summary','chapter','body'}.issubset({r['id'] for r in kept})
    assert decision['start_kind']=='abstract'


def test_reference_heading_inside_table_does_not_pass_as_research():
    rows=[paragraph('ab','摘要'),paragraph('body','正文。'),
          {'id':'table','type':'table','position':{'body_index':2},'cells':[
           {'text':'参考文献'}, {'text':'[1] example'}]}]
    with pytest.raises(ScopeUncertain):
        clean_body_blocks(rows)


def test_common_reference_material_heading_is_excluded():
    rows=[paragraph('ab','摘要'),paragraph('body','正文。'),
          paragraph('refs','参考资料',heading=1),paragraph('citation','[1] Example')]
    kept,removed,_=clean_body_blocks(rows)
    assert {r['id'] for r in kept}=={'ab','body'}
    assert {r['block']['id'] for r in removed}=={'refs','citation'}
