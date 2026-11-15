"""A parser's drawing marker is not an established research-use verdict."""
import pytest

from agent_client import CONTRACTS,contract_for


@pytest.mark.parametrize('flag',['graphic_object','graphic_members','graphic_region'])
def test_native_graphic_flags_require_purpose_judgment_before_complete_recovery(flag):
    prompt=contract_for('repair',{'page':1,'known_blocks':[{'id':'target',flag:True}]})
    assert '用途待核实' in prompt
    assert '不能仅凭这些标记认定研究用途或非研究用途' in prompt
    assert '确认属于研究图后' in prompt and '完整原图 image' in prompt
    assert 'nonresearch_objects' in prompt and '不得默默省略' in prompt
    assert 'graphic_members/graphic_region 表示研究图形成员或区域' not in prompt


def test_empty_outline_facts_cannot_borrow_an_independently_retained_bitmap():
    payload={'page':23,'known_blocks':[{'id':'target','graphic_object':True,
        'source_graphic_context':{'scope':'same_source_paragraph','target_object_mapping':'unavailable',
            'outline_rectangles':[{'geometry':'rect','fill':'none','has_text':False,'has_image':False,'has_chart':False}],
            'independent_image_resources':[{'id':'image_0010'}]}}]}
    prompt=contract_for('repair',payload)
    assert 'source_graphic_context' in prompt and 'independent_image_resources' in prompt
    assert '禁止把邻接位图当作边框自身的内容' in prompt
    assert '结合原页' in prompt and '无法可靠判断' in prompt and 'reject' in prompt
    assert '结构事实不是自动跳过或判为非研究的指令' in prompt
    # The aggregate contract used by legacy callers needs the same safeguard.
    assert '禁止把邻接位图当作边框自身的内容' in CONTRACTS['repair']


def test_graphic_purpose_review_preserves_native_research_table_protection():
    prompt=contract_for('repair',{'page':1,'known_blocks':[{
        'id':'table','graphic_object':True,'table_candidate':True,'native_data_available':True}]})
    assert 'table_candidate=true 表示已定位但网格仍待恢复的研究表格' in prompt
    assert '不能按排版横线或正文文字放入 nonresearch_objects' in prompt
    assert '必须对同一 after_id 返回 kind=image' in prompt
    assert 'native_data_available=true' in prompt and '禁止重算改写或推估数值' in prompt
