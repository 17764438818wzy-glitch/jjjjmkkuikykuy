"""Independent source context explains repeated labels without hiding shapes."""
import pytest

from caption_location import label_pages, normalized
from evidence_tasks import _locate
from test_graphic_regions import graphic
from test_script_location_105 import paragraph


def scene(label, other):
    group = graphic(['独立节点甲数据', '独立节点乙数据', label])
    blocks = [paragraph('这里是图形之前可以唯一定位的完整正文', 0), group,
              paragraph('这里是图形之后可以唯一定位的完整正文', 2), other]
    page = blocks[0]['text'] + '\n独立节点甲数据\n独立节点乙数据\n' + label + '\n' + blocks[2]['text']
    return group, blocks, [page, '其他正文', '其他正文', '其他正文']


@pytest.mark.parametrize('extra', ['', '\n人力资源部'])
def test_unique_short_caption_explains_only_its_own_label_occurrence(extra):
    caption = paragraph('图2.2 人力资源部界面', 3)
    group, blocks, pages = scene('人力资源部', caption)
    pages.append(caption['text'] + extra)
    expected = {0} if not extra else {0, 1, 2, 3, 4}
    assert _locate({'position': group['position']}, blocks, pages) == expected


def table():
    values = ['项目说明', '2019年度', '2020年度', '主营业务营业收入比重', '100.00%', '90.00%']
    return {'type': 'table', 'rows': 2, 'columns': 3,
            'grid': [[0, 1, 2], [3, 4, 5]], 'position': {'body_index': 3},
            'cells': [{'row': i // 3, 'column': i % 3, 'rowspan': 1, 'colspan': 1, 'text': value}
                      for i, value in enumerate(values)]}


@pytest.mark.parametrize('extra', ['', '\n100.00%'])
def test_native_table_context_explains_only_its_own_number_occurrence(extra):
    body = table()
    group, blocks, pages = scene('100.00%', body)
    pages.append('\n'.join(cell['text'] for cell in body['cells']) + extra)
    expected = {0} if not extra else {0, 1, 2, 3, 4}
    assert _locate({'position': group['position']}, blocks, pages) == expected


def test_unmerged_row_in_a_table_with_merged_header_is_independent_evidence():
    body = {'type': 'table', 'rows': 2, 'columns': 5,
            'grid': [[0, 0, 0, 0, 0], [1, 2, 3, 4, 5]], 'position': {'body_index': 3},
            'cells': [{'row': 0, 'column': 0, 'rowspan': 1, 'colspan': 5, 'text': '合并标题'},
                      *[{'row': 1, 'column': c, 'rowspan': 1, 'colspan': 1, 'text': text}
                        for c, text in enumerate(['8', '1', '调试检验', '0.9920', '0.0080'])]]}
    group, blocks, pages = scene('调试、检验', body)
    pages.append('合并标题\n8\n1\n调试检验\n0.9920\n0.0080')
    assert _locate({'position': group['position']}, blocks, pages) == {0}


@pytest.mark.parametrize('difference', ['', 'label', 'decimal', 'sign'])
def test_merged_table_uses_only_matching_original_xml_text_order(difference):
    values = ['8', '1', '调试检验', '0.9920', '0.0080']
    body = {'type': 'table', 'source': 'native_ooxml', 'rows': 2, 'columns': 5,
            'grid': [[0, 1, 2, 3, 4], [0, 1, 2, 3, 4]], 'position': {'body_index': 3},
            'cells': [{'row': 0, 'column': c, 'rowspan': 2, 'colspan': 1, 'text': value}
                      for c, value in enumerate(values)]}
    xml_values = list(values)
    if difference == 'label':xml_values[2] = '其他工序'
    if difference == 'decimal':xml_values[3] = '09920'
    if difference == 'sign':xml_values[3] = '-0.9920'
    body['native_xml'] = '<w:tbl xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:tr>' + ''.join(
        '<w:tc><w:p><w:r><w:t>' + value + '</w:t></w:r></w:p></w:tc>' for value in xml_values
    ) + '</w:tr></w:tbl>'
    group, blocks, pages = scene('调试、检验', body)
    pages.append('\n'.join(values))
    expected = {0} if not difference else {0, 1, 2, 3, 4}
    assert _locate({'position': group['position']}, blocks, pages) == expected


@pytest.mark.parametrize('extra', ['', '\n再制造机床'])
def test_other_graphic_requires_longer_unique_native_member_context(extra):
    group, blocks, pages = scene('再制造机床', paragraph('其他正文段落', 3))
    other = graphic(['再制造机床半成品', '装配流程节点'])
    other['id'] = 'independent-graphic'
    other['position'] = {'body_index': 5, 'object_ref': 'independent-object'}
    blocks += [paragraph('另一图形前有可以唯一定位的独立完整正文', 4), other,
               paragraph('图3.1 机床再制造基本流程示意图', 6)]
    pages.append(blocks[-3]['text'] + '\n再制造机床半成品\n装配流程节点\n' + blocks[-1]['text'] + extra)
    expected = {0} if not extra else {0, 1, 2, 3, 4}
    assert _locate({'position': group['position']}, blocks, pages) == expected


@pytest.mark.parametrize('label,text,want', [
    ('100.00%', '1100.00%\n10.000', set()),
    ('10.00%', '100.00%', set()),
    ('100.00%', '实际占比 100.00 %', {0}),
    ('100.00%', '实际数额 10000', set()),
])
def test_numeric_label_requires_the_same_complete_value_and_unit(label, text, want):
    assert label_pages(label, [text], [normalized(text)]) == want
