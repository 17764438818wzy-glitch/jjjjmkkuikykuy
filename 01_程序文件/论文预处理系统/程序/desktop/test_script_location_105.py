"""Local object-page regressions found in the 105-result batch."""
import copy
import pytest
from auto_evidence import Rejected
from evidence_tasks import _locate
from test_graphic_regions import graphic
from test_graphics_recovery import docx,parse


def paragraph(text,index):
    return {'type':'paragraph','text':text,'position':{'body_index':index}}


def test_bracketed_graphic_does_not_expand_to_distant_chapter_mentions():
    group=graphic(['独立节点甲数据','独立节点乙数据','研究结论与展望'])
    blocks=[paragraph('这里是图形之前可以唯一定位的完整正文',0),group,
            paragraph('这里是图形之后可以唯一定位的完整正文',2)]
    pages=[blocks[0]['text']+'\n独立节点甲数据\n独立节点乙数据\n研究结论与\n'+blocks[2]['text'],
           '无关正文','无关正文','无关正文','五、研究结论与展望']
    blocks.append(dict(paragraph('五、研究结论与展望',3),heading_level=1))
    before=copy.deepcopy(blocks)
    assert _locate({'position':group['position']},blocks,pages)=={0}
    assert blocks==before


def test_bracketed_graphic_retains_both_split_pages():
    group=graphic(['独立节点甲数据','独立节点乙数据','研究结论与展望'])
    blocks=[paragraph('这里是图形之前可以唯一定位的完整正文',0),group,
            paragraph('这里是图形之后可以唯一定位的完整正文',2)]
    pages=[blocks[0]['text']+'\n独立节点甲数据','独立节点乙数据\n研究结论与\n'+blocks[2]['text'],
           '无关正文','无关正文','无关正文','五、研究结论与展望']
    blocks.append(dict(paragraph('五、研究结论与展望',3),heading_level=1))
    assert _locate({'position':group['position']},blocks,pages)=={0,1}


def test_graphic_without_two_local_members_or_caption_stays_unresolved():
    group=graphic(['唯一节点数据','转换后缺失成员'])
    blocks=[paragraph('这里是图形之前可以唯一定位的完整正文',0),group,
            paragraph('这里是图形之后可以唯一定位的完整正文',2)]
    with pytest.raises(Rejected):
        _locate({'position':group['position']},blocks,[blocks[0]['text']+'唯一节点数据'+blocks[2]['text']])


def test_graphic_unique_caption_and_body_anchors_locate_rasterized_labels():
    group=graphic(['2013年','2016年','2017年'])
    blocks=[paragraph('这里是图形之前可以唯一定位的完整正文',0),group,paragraph('图3.1 华电国际的低碳发展',2)]
    blocks.append(paragraph('此前公司在2016年已经开展其他相关业务',3))
    assert _locate({'position':group['position']},blocks,
                   [blocks[3]['text'],blocks[0]['text'],blocks[2]['text']])=={1,2}


def test_note_object_is_located_from_its_own_note_text():
    pos={'part':'word/endnotes.xml','id':'20','object_ref':'a'}
    note={'type':'paragraph','position':{'part':'word/endnotes.xml','id':'20'},
          'text':'[^endnoteReference20]: 作者.耐心资本、创新投入对企业绩效的影响研究[J].科学决策,2022,(09):55-72.\n[[OBJECT:a]]'}
    image={'type':'image','position':pos}
    assert _locate({'position':pos},[note,image],['普通正文','21【】作者.耐心资本、创新投入对企业绩效的影响研究[J].科学决策,2022,(09):55-72.'])=={1}


def test_note_object_cannot_borrow_an_unrelated_note():
    pos={'part':'word/endnotes.xml','id':'20','object_ref':'a'}
    note={'type':'paragraph','position':{'part':'word/endnotes.xml','id':'19'},'text':'这是另一个注释中足够长而且唯一的文字内容'}
    with pytest.raises(Rejected):
        _locate({'position':pos},[note,{'type':'image','position':pos}],[note['text']])


def test_object_embedded_in_contents_uses_its_own_contents_entry():
    pos={'part':'word/document.xml','body_index':64,'container_path':[1,27],'object_ref':'a'}
    owner=dict(pos);owner.pop('object_ref')
    blocks=[{'type':'paragraph','position':pos,'text':'','graphic_object':True},
            {'type':'paragraph','position':owner,'text':'小米集团轻资产运营的路径\t11'}]
    assert _locate({'position':pos},blocks,['目录\n3.3 小米集团轻资产运营的路径..........11',
                                          '小米集团轻资产运营的路径\n这是普通正文内容'])=={0}


@pytest.mark.parametrize('tag',['customXmlInsRangeStart','customXmlInsRangeEnd','customXmlDelRangeStart','customXmlDelRangeEnd'])
def test_empty_revision_range_marker_is_not_an_unparsed_body_object(tmp_path,tag):
    blocks,_,issues=parse(docx(tmp_path/'range.docx',f'<w:{tag} w:id="157"/><w:p><w:r><w:t>保留正文</w:t></w:r></w:p>'))
    assert not any(i['code']=='body_container' for i in issues)
    assert any(b.get('text')=='保留正文' for b in blocks)


def test_revision_range_with_unexpected_content_is_not_silently_dropped(tmp_path):
    _,_,issues=parse(docx(tmp_path/'content.docx','<w:customXmlInsRangeStart><w:r><w:t>不能丢弃正文</w:t></w:r></w:customXmlInsRangeStart>'))
    assert any(i['code']=='body_container' for i in issues)

def test_body_anchor_page_is_candidate_but_not_required_graphic_evidence():
    from evidence_tasks import _graphic_member_locations
    group=graphic(['独立节点甲数据','独立节点乙数据'])
    blocks=[paragraph('这里是图形之前可以唯一定位的完整正文',0),group,
            paragraph('这里是图形之后可以唯一定位的完整正文',2)]
    pages=[blocks[0]['text'],'独立节点甲数据\n独立节点乙数据\n'+blocks[2]['text']]
    assert _locate({'position':group['position']},blocks,pages)=={0,1}
    required,_=_graphic_member_locations({'position':group['position']},blocks,pages)
    assert required=={1}


def test_contents_entry_duplicated_on_two_contents_pages_stays_ambiguous():
    pos={'part':'word/document.xml','body_index':64,'container_path':[1,27],'object_ref':'a'}
    owner=dict(pos);owner.pop('object_ref')
    blocks=[{'type':'paragraph','position':pos,'text':'','graphic_object':True},
            {'type':'paragraph','position':owner,'text':'小米集团轻资产运营的路径\t11'}]
    with pytest.raises(Rejected):
        _locate({'position':pos},blocks,['目录\n小米集团轻资产运营的路径..........11']*2)


def test_context_does_not_allow_graphic_regions_over_four_pages():
    from evidence_tasks import _add_region
    group=graphic(['独立节点甲数据','独立节点乙数据'])
    blocks=[paragraph('这里是图形之前可以唯一定位的完整正文',0),group,
            paragraph('这里是图形之后可以唯一定位的完整正文',2)]
    pages=[blocks[0]['text']+'独立节点甲数据','无关正文','无关正文','无关正文','独立节点乙数据'+blocks[2]['text']]
    issue={'position':group['position']}
    with pytest.raises(Rejected):
        _add_region([],_locate(issue,blocks,pages),issue)

def test_contents_object_cannot_borrow_an_entry_from_a_parent_container():
    pos={'part':'word/document.xml','body_index':64,'container_path':[1,27],'object_ref':'a'}
    blocks=[{'type':'paragraph','position':pos,'text':'','graphic_object':True},
            {'type':'paragraph','position':{'part':'word/document.xml','body_index':64},
             'text':'小米集团轻资产运营的路径\t11'}]
    with pytest.raises(Rejected):
        _locate({'position':pos},blocks,['目录\n小米集团轻资产运营的路径..........11'])

@pytest.mark.parametrize('outside_labels',[
    ['第三部分完整图示'],
    ['第三部分完整图示','第四部分完整图示'],
])
def test_floating_graphic_members_beyond_body_bracket_are_retained(outside_labels):
    group=graphic(['独立节点甲数据','独立节点乙数据']+outside_labels)
    blocks=[paragraph('这里是图形之前可以唯一定位的完整正文',0),group,
            paragraph('这里是图形之后可以唯一定位的完整正文',2)]
    pages=[blocks[0]['text']+'\n独立节点甲数据\n独立节点乙数据\n'+blocks[2]['text'],
           '\n'.join(outside_labels)]
    assert _locate({'position':group['position']},blocks,pages)=={0,1}


def test_floating_member_extra_occurrence_beside_known_heading_is_retained():
    group=graphic(['独立节点甲数据','独立节点乙数据','研究结论与展望'])
    heading=dict(paragraph('五、研究结论与展望',3),heading_level=1)
    blocks=[paragraph('这里是图形之前可以唯一定位的完整正文',0),group,
            paragraph('这里是图形之后可以唯一定位的完整正文',2),heading]
    pages=[blocks[0]['text']+'\n独立节点甲数据\n独立节点乙数据\n'+blocks[2]['text'],
           '研究结论与展望\n'+heading['text']]
    assert _locate({'position':group['position']},blocks,pages)=={0,1}

def test_distant_label_in_paginated_prose_is_explained_by_surviving_context():
    group=graphic(['独立节点甲数据','独立节点乙数据','研究结论与展望'])
    prose=paragraph('前面一大段普通正文已经在上页结束。研究结论与展望需要结合样本限制和研究方法进一步展开讨论。',3)
    blocks=[paragraph('这里是图形之前可以唯一定位的完整正文',0),group,
            paragraph('这里是图形之后可以唯一定位的完整正文',2),prose]
    pages=[blocks[0]['text']+'独立节点甲数据独立节点乙数据'+blocks[2]['text'],
           '前面一大段普通正文已经在上页结束。','无关正文','无关正文',
           '研究结论与展望需要结合样本限制和研究方法进一步展开讨论。']
    assert _locate({'position':group['position']},blocks,pages)=={0}


def test_unstyled_heading_needs_contiguous_body_context_to_explain_remote_label():
    group=graphic(['独立节点甲数据','独立节点乙数据','参考文献'])
    blocks=[paragraph('这里是图形之前可以唯一定位的完整正文',0),group,
            paragraph('这里是图形之后可以唯一定位的完整正文',2),paragraph('参考文献',3),
            paragraph('作者一关于企业融资对绩效影响的研究资料2022年',4)]
    pages=[blocks[0]['text']+'独立节点甲数据独立节点乙数据'+blocks[2]['text'],
           '无关正文','无关正文','无关正文',blocks[3]['text']+blocks[4]['text']]
    assert _locate({'position':group['position']},blocks,pages)=={0}

def test_unnumbered_body_heading_does_not_explain_an_extra_graphic_occurrence():
    group=graphic(['独立节点甲数据','独立节点乙数据','研究结论与展望'])
    heading=dict(paragraph('研究结论与展望',3),heading_level=1)
    blocks=[paragraph('这里是图形之前可以唯一定位的完整正文',0),group,
            paragraph('这里是图形之后可以唯一定位的完整正文',2),heading]
    pages=[blocks[0]['text']+'独立节点甲数据独立节点乙数据'+blocks[2]['text'],
           '研究结论与展望\n研究结论与展望']
    assert _locate({'position':group['position']},blocks,pages)=={0,1}
