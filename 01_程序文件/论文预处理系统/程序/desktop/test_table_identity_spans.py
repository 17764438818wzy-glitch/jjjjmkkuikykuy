import copy

import pytest

from auto_evidence import Rejected, text_privacy


def document(name='张小测', value='125.50'):
    return {'title': '幼儿园教学活动研究', 'resources': [], 'formulas': [], 'blocks': [
        {'id': 't', 'type': 'table', 'cells': [
            {'row': 0, 'column': 0, 'colspan': 1, 'text': '观察者'},
            {'row': 0, 'column': 1, 'colspan': 1, 'text': name},
            {'row': 1, 'column': 0, 'colspan': 1, 'text': '数值'},
            {'row': 1, 'column': 1, 'colspan': 1, 'text': value}]},
        {'id': 'p', 'type': 'paragraph', 'text': name + '提出的理论见参考文献。'}]}


class Agent:
    def call(self, task, payload, **kwargs):
        return {'decision': 'pass', 'reason': '表格中的本文观察者身份可定位',
                'sensitive_strings': ['观察者\n张小测']}


def test_ai_confirmed_adjacent_identity_cells_mask_only_person_cell():
    meta = document()
    text_privacy(meta, Agent(), lambda *args: None)
    assert meta['blocks'][0]['cells'][0]['text'] == '观察者'
    assert meta['blocks'][0]['cells'][1]['text'] == '[已脱敏]'
    assert meta['blocks'][0]['cells'][3]['text'] == '125.50'
    assert meta['blocks'][1]['text'] == '张小测提出的理论见参考文献。'
    assert meta['text_privacy_complete']


def test_same_identity_fields_in_multiple_tables_are_all_masked():
    meta = document(); meta['blocks'].append(copy.deepcopy(meta['blocks'][0]))
    text_privacy(meta, Agent(), lambda *args: None)
    assert meta['blocks'][0]['cells'][1]['text'] == '[已脱敏]'
    assert meta['blocks'][2]['cells'][1]['text'] == '[已脱敏]'


def test_native_table_xml_is_masked_at_the_same_cell_and_keeps_research_values():
    meta=document()
    meta['blocks'][0]['native_xml']='''<w:tbl xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
    <w:tr><w:tc><w:p><w:r><w:t>观察者</w:t></w:r></w:p></w:tc>
    <w:tc><w:p><w:r><w:t>张</w:t></w:r><w:r><w:t>小测</w:t></w:r></w:p></w:tc></w:tr>
    <w:tr><w:tc><w:p><w:r><w:t>数值</w:t></w:r></w:p></w:tc>
    <w:tc><w:p><w:r><w:t>125.50</w:t></w:r></w:p></w:tc></w:tr></w:tbl>'''
    text_privacy(meta,Agent(),lambda *args:None)
    assert '张' not in meta['blocks'][0]['native_xml']
    assert '125.50' in meta['blocks'][0]['native_xml']
    assert '[已脱敏]' in meta['blocks'][0]['native_xml']
    assert '张小测' in meta['blocks'][1]['text']


def test_confirmed_cell_xml_attributes_cannot_retain_the_masked_identity():
    meta=document()
    meta['blocks'][0]['native_xml']='''<w:tbl xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
    <w:tr><w:tc><w:p><w:r><w:t>观察者</w:t></w:r></w:p></w:tc>
    <w:tc><w:p><w:hyperlink w:tooltip="张小测"><w:r><w:t>张小测</w:t></w:r></w:hyperlink></w:p></w:tc></w:tr></w:tbl>'''
    text_privacy(meta,Agent(),lambda *args:None)
    assert '张小测' not in meta['blocks'][0]['native_xml']
    assert '张小测' in meta['blocks'][1]['text']


@pytest.mark.parametrize('change', ['row', 'column', 'native_xml'])
def test_flattened_neighbor_strings_without_matching_table_geometry_reject(change):
    meta = document()
    if change == 'native_xml':meta['blocks'][0]['native_xml'] = '<table>张小测</table>'
    else:meta['blocks'][0]['cells'][1][change] = 9
    with pytest.raises(Rejected, match='跨段'):
        text_privacy(meta, Agent(), lambda *args: None)
    assert not meta['text_privacy_complete']


def test_numeric_research_value_cannot_become_an_identity_name():
    meta = document('125.50')
    class Numeric:
        def call(self, *args, **kwargs):
            return {'decision': 'pass', 'reason': 'mask', 'sensitive_strings': ['观察者\n125.50']}
    with pytest.raises(Rejected):text_privacy(meta, Numeric(), lambda *args: None)
    assert meta['blocks'][0]['cells'][1]['text'] == '125.50'


def test_same_label_name_in_a_reference_table_is_ambiguous_and_not_globally_masked():
    meta=document();meta['blocks'][0]['cells'][0]['text']='作者'
    reference=copy.deepcopy(meta['blocks'][0]);reference['caption']='文献综述：已有研究比较'
    meta['blocks'].append(reference)
    class Authors:
        def call(self,*args,**kwargs):
            return {'decision':'pass','reason':'本文作者字段敏感；同名引用应保留',
                    'sensitive_strings':['作者\n张小测']}
    with pytest.raises(Rejected,match='跨段'):
        text_privacy(meta,Authors(),lambda *args:None)
    assert meta['blocks'][2]['cells'][1]['text']=='张小测'
