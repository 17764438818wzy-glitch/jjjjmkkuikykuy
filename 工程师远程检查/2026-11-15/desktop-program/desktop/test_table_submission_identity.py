import copy
from xml.etree import ElementTree as ET

import pytest

from auto_evidence import Rejected, text_privacy


W = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
NAME = '张小测'
ADMISSION = '202612345678'
PHONES = ['13800138001', '13900139002']


def document():
    cells = [
        (0, 0, 1, '专业'), (0, 1, 2, '音乐学'),
        (0, 3, 1, '准考证号'), (0, 4, 1, ADMISSION),
        (0, 5, 1, '姓名'), (0, 6, 1, NAME),
        (1, 0, 1, '论文题目'), (1, 1, 6, '课堂节奏教学研究'),
        (2, 0, 1, '学历'), (2, 1, 1, '本科'),
        (2, 2, 1, '联系电话'), (2, 3, 2, PHONES[0]), (2, 5, 2, PHONES[1]),
        (3, 0, 1, '作者声明'), (3, 1, 6, '本人确认论文内容。作者签名：' + NAME),
        (4, 0, 1, '评阅意见'), (4, 1, 6, '论文评分：90。研究比例：28%。'),
    ]
    block = {'id': 'table_0001', 'type': 'table', 'section': ['参考文献'],
             'cells': [{'row': r, 'column': c, 'colspan': n, 'rowspan': 1, 'text': text}
                       for r, c, n, text in cells]}
    xml(block)
    return {'title': '课堂节奏教学研究', 'resources': [], 'formulas': [], 'blocks': [
        block,
        {'id': 'body', 'type': 'paragraph', 'text': NAME + '提出的公开理论见参考文献。'},
        {'id': 'numbers', 'type': 'table', 'cells': [
            {'row': 0, 'column': 0, 'text': '研究数据'},
            {'row': 0, 'column': 1, 'text': '202698765432'},
            {'row': 0, 'column': 2, 'text': '13700137003'}]},
    ]}


def xml(block):
    root = ET.Element(W + 'tbl')
    for row in range(max(c['row'] for c in block['cells']) + 1):
        tr = ET.SubElement(root, W + 'tr')
        for c in sorted((c for c in block['cells'] if c['row'] == row), key=lambda c: c['column']):
            tc = ET.SubElement(tr, W + 'tc')
            if c.get('colspan', 1) != 1:
                pr = ET.SubElement(tc, W + 'tcPr')
                ET.SubElement(pr, W + 'gridSpan', {W + 'val': str(c['colspan'])})
            ET.SubElement(ET.SubElement(ET.SubElement(tc, W + 'p'), W + 'r'), W + 't').text = c['text']
    block['native_xml'] = ET.tostring(root, encoding='unicode')


def native_cells(block):
    cells = {}
    for row, tr in enumerate(ET.fromstring(block['native_xml']).findall(W + 'tr')):
        column = 0
        for tc in tr.findall(W + 'tc'):
            cells[row, column] = ''.join(node.text or '' for node in tc.iter(W + 't'))
            span = tc.find('./' + W + 'tcPr/' + W + 'gridSpan')
            column += int(span.get(W + 'val')) if span is not None else 1
    return cells


class Agent:
    def __init__(self, spans):
        self.spans = spans

    def call(self, task, payload, **kwargs):
        assert all(s in payload['text'] for s in self.spans)
        return {'decision': 'pass', 'reason': '明确本文提交者身份字段', 'sensitive_strings': self.spans}


@pytest.mark.parametrize('spans, locations', [
    (['准考证号\n' + ADMISSION], [(0, 4)]),
    (['姓名\n' + NAME], [(0, 6)]),
    (['联系电话\n' + '\n'.join(PHONES)], [(2, 3), (2, 5)]),
])
def test_confirmed_submission_identity_masks_exact_cells_despite_inherited_section(spans, locations):
    meta = document()
    before = copy.deepcopy(meta)
    text_privacy(meta, Agent(spans), lambda *args: None)
    for old, new in zip(before['blocks'][0]['cells'], meta['blocks'][0]['cells']):
        target = (old['row'], old['column']) in locations
        assert new['text'] == ('[已脱敏]' if target else old['text'])
        if target:
            assert native_cells(meta['blocks'][0])[old['row'], old['column']] == '[已脱敏]'
    assert meta['blocks'][1:] == before['blocks'][1:]
    assert meta['privacy_check']['table_identity_cells'] == len(locations)
    assert meta['text_privacy_complete']


def test_complete_response_masks_all_private_fields_and_xml_but_preserves_research():
    meta = document()
    spans = ['准考证号\n' + ADMISSION, '姓名\n' + NAME,
             '联系电话\n' + '\n'.join(PHONES), '作者签名：' + NAME]
    text_privacy(meta, Agent(spans), lambda *args: None)
    block = meta['blocks'][0]
    table_text = '\n'.join(c['text'] for c in block['cells']) + block['native_xml']
    assert all(s not in table_text for s in [ADMISSION, NAME, *PHONES])
    assert block['cells'][-1]['text'] == '论文评分：90。研究比例：28%。'
    assert meta['blocks'][1]['text'] == NAME + '提出的公开理论见参考文献。'
    assert meta['blocks'][2]['cells'][1]['text'] == '202698765432'
    assert meta['blocks'][2]['cells'][2]['text'] == '13700137003'


def test_ten_digit_admission_and_blank_signature_still_require_a_complete_native_form():
    meta = document()
    table = meta['blocks'][0]
    admission = '2026123456'
    table['cells'][3]['text'] = admission
    table['cells'][-3]['text'] = '本人确认论文内容。\n作者签名：\n日期：'
    table['cells'][12]['text'] = ''
    xml(table)
    spans = ['准考证号\n' + admission, '姓名\n' + NAME,
             '联系电话\n' + PHONES[0]]
    text_privacy(meta, Agent(spans), lambda *args: None)
    assert meta['privacy_check']['table_identity_cells'] == 3
    assert native_cells(table)[0, 4] == admission  # captured original is untouched
    assert native_cells(meta['blocks'][0])[0, 4] == '[已脱敏]'
    assert meta['blocks'][1]['text'] == NAME + '提出的公开理论见参考文献。'


def split_signature_runs(meta, layout='plain'):
    block = meta['blocks'][0]
    root = ET.fromstring(block['native_xml'])
    cell = root.findall(W + 'tr')[3].findall(W + 'tc')[1]
    for child in list(cell):
        if child.tag == W + 'p': cell.remove(child)
    paragraph = ET.SubElement(cell, W + 'p')
    for text in ['本人确认论文内容。', '作者签名：', NAME, '；研究比例28%，样本90。']:
        if layout == 'paragraph' and text == NAME:
            paragraph = ET.SubElement(cell, W + 'p')
        if layout == 'tab' and text == NAME:
            ET.SubElement(ET.SubElement(paragraph, W + 'r'), W + 'tab')
        ET.SubElement(ET.SubElement(paragraph, W + 'r'), W + 't').text = text
    block['native_xml'] = ET.tostring(root, encoding='unicode')
    # The unknown layout variants deliberately retain the flattened source
    # field, to ensure it cannot authorize editing a different native layout.
    block['cells'][-3]['text'] = '本人确认论文内容。作者签名：' + NAME + '；研究比例28%，样本90。'


def test_exact_signature_span_across_runs_masks_xml_and_preserves_surrounding_nodes():
    meta = document()
    split_signature_runs(meta)
    old = ET.fromstring(meta['blocks'][0]['native_xml'])
    text_privacy(meta, Agent(['作者签名：' + NAME]), lambda *args: None)
    block = meta['blocks'][0]
    new = ET.fromstring(block['native_xml'])
    assert len(list(old.iter())) == len(list(new.iter()))
    assert native_cells(block)[3, 1] == '本人确认论文内容。[已脱敏]；研究比例28%，样本90。'
    assert block['cells'][-3]['text'] == native_cells(block)[3, 1]
    assert meta['blocks'][1]['text'] == NAME + '提出的公开理论见参考文献。'
    for a, b in zip(old.iter(), new.iter()):
        assert (a.tag, a.attrib, a.tail) == (b.tag, b.attrib, b.tail)
        if a.text not in ['作者签名：', NAME]:
            assert a.text == b.text


@pytest.mark.parametrize('layout', ['paragraph', 'tab', 'duplicate_metadata'])
def test_signature_span_cannot_mask_unknown_or_ambiguous_native_layout(layout):
    meta = document()
    split_signature_runs(meta, layout)
    if layout == 'duplicate_metadata':
        meta['blocks'][0]['cells'].append(copy.deepcopy(meta['blocks'][0]['cells'][-3]))
    with pytest.raises(Rejected, match='原生表格'):
        text_privacy(meta, Agent(['作者签名：' + NAME]), lambda *args: None)
    assert not meta['text_privacy_complete']


@pytest.mark.parametrize('feature', ['admission_label', 'signature', 'score', 'phone_label', 'native_xml'])
def test_incomplete_submission_evidence_cannot_override_reference_section(feature):
    meta = document()
    table = meta['blocks'][0]
    if feature == 'native_xml':
        table.pop('native_xml')
    else:
        index, value = {'admission_label': (2, '研究序号'), 'signature': (-3, '研究结论'),
                        'phone_label': (10, '研究统计'),
                        'score': (-1, '研究比例：28%。')}[feature]
        table['cells'][index]['text'] = value
        xml(table)
    with pytest.raises(Rejected, match='跨段'):
        text_privacy(meta, Agent(['姓名\n' + NAME]), lambda *args: None)


@pytest.mark.parametrize('context', ['caption', 'context_before', 'context_after'])
def test_explicit_reference_context_still_rejects_even_with_submission_markers(context):
    meta = document()
    meta['blocks'][0][context] = '文献综述：相关研究比较'
    with pytest.raises(Rejected, match='跨段'):
        text_privacy(meta, Agent(['姓名\n' + NAME]), lambda *args: None)


def test_same_name_in_a_real_reference_table_remains_ambiguous():
    meta = document()
    meta['blocks'].append({'id': 'references', 'type': 'table', 'caption': '参考文献比较',
                          'cells': [{'row': 0, 'column': 0, 'text': '姓名'},
                                    {'row': 0, 'column': 1, 'text': NAME}]})
    with pytest.raises(Rejected, match='跨段'):
        text_privacy(meta, Agent(['姓名\n' + NAME]), lambda *args: None)


def test_same_numeric_identity_in_an_unproved_reference_table_is_ambiguous():
    meta = document()
    meta['blocks'].append({'id': 'references', 'type': 'table', 'caption': '参考文献比较',
                          'cells': [{'row': 0, 'column': 0, 'text': '准考证号'},
                                    {'row': 0, 'column': 1, 'text': ADMISSION}]})
    with pytest.raises(Rejected, match='跨段'):
        text_privacy(meta, Agent(['准考证号\n' + ADMISSION]), lambda *args: None)


@pytest.mark.parametrize('change', ['prose_labels', 'signature_metadata_only', 'score_metadata_only'])
def test_form_markers_need_exact_native_source_and_separate_identity_fields(change):
    meta = document()
    table = meta['blocks'][0]
    if change == 'prose_labels':
        table['cells'][2]['text'] = '研究讨论准考证号的形式'
        table['cells'][10]['text'] = '研究讨论联系电话的形式'
        xml(table)
    else:
        root = ET.fromstring(table['native_xml'])
        row = 3 if change == 'signature_metadata_only' else 4
        cell = root.findall(W + 'tr')[row].findall(W + 'tc')[1]
        next(cell.iter(W + 't')).text = '研究说明'
        table['native_xml'] = ET.tostring(root, encoding='unicode')
    with pytest.raises(Rejected, match='跨段'):
        text_privacy(meta, Agent(['姓名\n' + NAME]), lambda *args: None)


@pytest.mark.parametrize('change', ['numeric_research', 'phone_shape', 'gap', 'other_row', 'duplicate_position'])
def test_contact_values_require_strict_phone_syntax_and_unique_adjacent_geometry(change):
    meta = document()
    table = meta['blocks'][0]
    value = next(c for c in table['cells'] if c['row'] == 2 and c['column'] == 5)
    if change == 'numeric_research': value['text'] = '125.50'
    elif change == 'phone_shape': value['text'] = '12345678901'
    elif change == 'gap': value['column'] = 7
    elif change == 'other_row': value['row'] = 5
    else: table['cells'].append(copy.deepcopy(value))
    spans = ['联系电话\n' + PHONES[0] + '\n' + value['text']]
    # Keep the original XML so metadata-only geometry changes are also rejected.
    with pytest.raises(Rejected, match='跨段'):
        text_privacy(meta, Agent(spans), lambda *args: None)


@pytest.mark.parametrize('value', ['202612345', '20261234567', '2026123456789', '2026.12345678'])
def test_admission_identifier_requires_exact_observed_digit_lengths(value):
    meta = document()
    meta['blocks'][0]['cells'][3]['text'] = value
    xml(meta['blocks'][0])
    with pytest.raises(Rejected, match='跨段'):
        text_privacy(meta, Agent(['准考证号\n' + value]), lambda *args: None)
