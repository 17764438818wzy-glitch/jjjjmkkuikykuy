"""Regressions for real caption-field and compound-drawing failures."""
import copy
import pytest
from auto_evidence import Rejected
from evidence_tasks import _locate,_member_label_pages
from test_graphics_recovery import docx, parse


def graphic(labels):
    pos={'body_index':1,'object_ref':'a'}
    return {'id':'g','type':'paragraph','text':'','position':pos,'graphic_object':True,
            'locator_texts':labels,'graphic_members':[{'position':dict(pos),'text':s,'locator_texts':[s]} for s in labels]}


@pytest.mark.parametrize('caption,rendered',[
    ('图 1.1 研究思路','图 错误: 引用源未找到.\n1 研究思路'),
    ('Figure 2.3 Research framework','Figure Error! Reference source not found. 3 Research framework'),
])
def test_field_error_in_caption_number_uses_unchanged_caption_body(caption,rendered):
    block=graphic([caption]);before=copy.deepcopy(block)
    assert _locate({'position':block['position']},[block],['Other page',rendered])=={1}
    assert block==before


def test_caption_field_error_cannot_match_changed_caption_body():
    block=graphic(['图1.1 企业财务风险框架'])
    with pytest.raises(Rejected):
        _locate({'position':block['position']},[block],['图错误:引用源未找到.1 企业财务收益框架'])


def test_caption_field_error_cannot_choose_between_repeated_captions():
    block=graphic(['图1.1 研究思路'])
    with pytest.raises(Rejected):
        _locate({'position':block['position']},[block],['图错误:引用源未找到.1研究思路']*2)


def test_caption_matching_does_not_strip_valid_distinct_numbers():
    block=graphic(['图1.1 企业财务风险框架'])
    with pytest.raises(Rejected):
        _locate({'position':block['position']},[block],['图2.1 企业财务风险框架'])


def test_surviving_caption_number_must_agree_with_original_suffix():
    block=graphic(['图1.1 研究思路'])
    with pytest.raises(Rejected):
        _locate({'position':block['position']},[block],['图错误:引用源未找到.2研究思路'])


def test_spacing_variants_are_not_independent_page_anchors():
    block=graphic(['研究背景','研究 背景'])
    block.pop('graphic_members')
    with pytest.raises(Rejected):
        _locate({'position':block['position']},[block],['正文提及研究背景','实际图形为栅格'])


def test_caption_matching_does_not_use_contents_entry_or_sentence_reference():
    block=graphic(['图1.1 研究思路'])
    with pytest.raises(Rejected):
        _locate({'position':block['position']},[block],['图目录\n图错误:引用源未找到.1研究思路','正文提到图错误:引用源未找到.1研究思路'])


def shape(text,floating):
    inside=f'<w:txbxContent><w:p><w:r><w:t>{text}</w:t></w:r></w:p></w:txbxContent>'
    return '<w:pict><v:rect>'+inside+'</v:rect></w:pict>' if floating else '<w:drawing>'+inside+'</w:drawing>'


def para(*objects):return '<w:p><w:r>'+''.join(objects)+'</w:r></w:p>'


def test_arrows_and_inline_body_form_one_recovery_region_preserving_all_members(tmp_path):
    src=docx(tmp_path/'a.docx',para(shape('连接箭头',True),shape('研究背景',False),shape('研究结论',False)))
    blocks,_,issues=parse(src)
    objects=[b for b in blocks if b.get('graphic_object')]
    assert len(objects)==1
    assert len(objects[0]['graphic_members'])==3
    assert len({m['position']['object_ref'] for m in objects[0]['graphic_members']})==3
    assert sum(i['code']=='embedded_object_review' for i in issues)==1
    assert '研究背景' in objects[0]['text'] and '研究结论' in objects[0]['text']


def test_inline_body_before_arrows_is_also_one_region(tmp_path):
    src=docx(tmp_path/'a.docx',para(shape('研究主体',False),shape('连接箭头',True)))
    blocks,_,_=parse(src)
    assert len([b for b in blocks if b.get('graphic_object')])==1


def test_caption_separates_two_compound_regions(tmp_path):
    src=docx(tmp_path/'a.docx',para(shape('第一图主体',False),shape('第一箭头',True))+
             '<w:p><w:r><w:t>图1 第一张图</w:t></w:r></w:p>'+
             para(shape('第二图主体',False),shape('第二箭头',True)))
    blocks,_,_=parse(src)
    objects=[b for b in blocks if b.get('graphic_object')]
    assert len(objects)==2
    assert '第二图主体' not in objects[0]['text']


def test_independent_inline_figures_do_not_merge_without_floating_members(tmp_path):
    src=docx(tmp_path/'a.docx',para(shape('第一独立图',False),shape('第二独立图',False)))
    blocks,_,issues=parse(src)
    assert len([b for b in blocks if b.get('graphic_object')])==2
    assert sum(i['code']=='embedded_object_review' for i in issues)==2


def test_paragraph_leading_page_break_does_not_split_members_in_that_paragraph(tmp_path):
    content=para(shape('连接箭头',True),shape('研究主体',False)).replace('<w:p>',
        '<w:p><w:pPr><w:pageBreakBefore/></w:pPr>',1)
    src=docx(tmp_path/'a.docx',para(shape('前一页独立图',True))+content)
    objects=[b for b in parse(src)[0] if b.get('graphic_object')]
    assert len(objects)==2
    assert len(objects[1]['graphic_members'])==2


def test_actual_page_break_between_members_still_separates_them(tmp_path):
    src=docx(tmp_path/'a.docx',para(shape('第一页独立图',True),'<w:br w:type="page"/>',shape('第二页独立图',False)))
    assert len([b for b in parse(src)[0] if b.get('graphic_object')])==2


@pytest.mark.parametrize('source,rendered',[
    ("授信核定dian'cha",'授信核定'),
    ('前期技术研究YANJIJIU究','前期技术研究'),
    ('进行成本核算算','进行成本核算'),
    ('业务负责人审核ge','业务负责人审核'),
    ('接收报废固定资产，进行报废处理','后勤部员工接收报废资产'),
])
def test_member_locator_tolerates_conversion_only_shape_text_noise(source,rendered):
    pages=['普通正文',rendered+'\n图形其他内容']
    normalized=['普通正文',rendered+'图形其他内容']
    assert _member_label_pages(source,pages,normalized)=={1}


def test_fuzzy_member_fragment_must_identify_only_one_page():
    label='接收报废固定资产，进行报废处理'
    pages=['接收报废资产','另一处也写接收报废记录']
    assert _member_label_pages(label,pages,pages)==set()


def test_one_unrendered_member_can_use_two_independent_members_only_as_candidate_page():
    block=graphic(['节点甲数据','节点乙数据','转换后缺失成员'])
    page='节点甲数据\n节点乙数据\n完整流程图'
    assert _locate({'position':block['position']},[block],['正文',page])=={1}


def test_one_anchor_cannot_hide_an_unrendered_graphic_member():
    block=graphic(['唯一节点数据','转换后缺失成员'])
    with pytest.raises(Rejected,match='至少两个独立成员'):
        _locate({'position':block['position']},[block],['唯一节点数据'])


def test_bare_drawingml_connector_is_not_a_research_object(tmp_path):
    connector='<w:drawing><a:graphic><a:graphicData><wps:wsp><wps:spPr><a:prstGeom prst="line"><a:avLst/></a:prstGeom></wps:spPr></wps:wsp></a:graphicData></a:graphic></w:drawing>'
    src=docx(tmp_path/'connector.docx',para(connector))
    blocks,_,issues=parse(src)
    assert not any(b.get('graphic_object') for b in blocks)
    assert not any(i['code']=='embedded_object_review' for i in issues)
