import copy
from xml.etree import ElementTree as ET

import pytest

from agent_client import AgentError
from auto_evidence import Rejected, text_privacy

W = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
NAME = '李测'


def native_xml(block):
    root = ET.Element(W + 'tbl')
    for row in range(max(c['row'] for c in block['cells']) + 1):
        tr = ET.SubElement(root, W + 'tr')
        for cell in sorted((c for c in block['cells'] if c['row'] == row), key=lambda c: c['column']):
            tc = ET.SubElement(tr, W + 'tc')
            for line in cell['text'].split('\n'):
                paragraph = ET.SubElement(tc, W + 'p')
                if line:
                    # More than one run must remain intact after masking.
                    for part in [line[:1], line[1:]]:
                        ET.SubElement(ET.SubElement(paragraph, W + 'r'), W + 't').text = part
    block['native_xml'] = ET.tostring(root, encoding='unicode')


def document(label='\n教师', value='\n' + NAME):
    tables = []
    for index in range(3):
        block = {'id': f'table_{index}', 'type': 'table', 'cells': [
            {'row': 0, 'column': column, 'colspan': 1, 'rowspan': 1, 'text': text}
            for column, text in enumerate(['研究比例', '28%', '样本数', '90', label, value])]}
        native_xml(block)
        tables.append(block)
    return {'title': '课堂教学研究', 'resources': [], 'formulas': [], 'blocks': tables + [
        {'id': 'body', 'type': 'paragraph', 'text': NAME + '提出的公开理论见参考文献。'}]}


class Agent:
    def __init__(self, span): self.span = span

    def call(self, task, payload, **kwargs):
        return {'decision': 'pass', 'reason': '观课表教师身份须脱敏', 'sensitive_strings': [self.span]}


def native_cell_text(block, column):
    cell = ET.fromstring(block['native_xml']).find(W + 'tr').findall(W + 'tc')[column]
    return '\n'.join(''.join(t.text or '' for t in p.iter(W + 't')) for p in cell.findall(W + 'p'))


@pytest.mark.parametrize('label,value,full_span', [
    ('\n教师', '\n' + NAME, False),
    ('\n教师\n', '\n' + NAME + '\n', False),
    ('\n\n教师\n', '\n\n' + NAME + '\n\n', True),
    ('教师', NAME, False),
])
def test_exact_observer_span_masks_three_cells_and_preserves_native_empty_paragraphs(label, value, full_span):
    meta = document(label, value)
    before = copy.deepcopy(meta)
    source_span = label + '\n' + value
    span = source_span if full_span else source_span.strip('\n')
    text_privacy(meta, Agent(span), lambda *args: None)
    for old, block in zip(before['blocks'][:3], meta['blocks'][:3]):
        assert block['cells'][:5] == old['cells'][:5]
        assert block['cells'][5]['text'] == value.replace(NAME, '[已脱敏]')
        assert native_cell_text(block, 5) == block['cells'][5]['text']
        assert native_cell_text(block, 4) == label
        assert NAME not in block['native_xml']
        oldnodes = list(ET.fromstring(old['native_xml']).iter())
        newnodes = list(ET.fromstring(block['native_xml']).iter())
        assert len(oldnodes) == len(newnodes)
        for a, b in zip(oldnodes, newnodes):
            assert (a.tag, a.attrib, a.tail) == (b.tag, b.attrib, b.tail)
    assert meta['blocks'][3] == before['blocks'][3]
    assert meta['privacy_check']['table_identity_cells'] == 3


def test_missing_real_separator_is_not_normalized_to_a_match():
    meta = document()
    with pytest.raises(AgentError, match='不匹配原文'):
        text_privacy(meta, Agent('教师\n' + NAME), lambda *args: None)


@pytest.mark.parametrize('value', ['\n125.50', '\n', '\nJohn\nSmith'])
def test_numeric_empty_or_multiline_values_are_not_confirmed_name_cells(value):
    meta = document(value=value)
    span = ('教师\n' + value).rstrip('\n')
    if span == '教师': span += '\n'
    with pytest.raises(Rejected, match='跨段'):
        text_privacy(meta, Agent(span), lambda *args: None)


@pytest.mark.parametrize('context', ['caption', 'section', 'context_before'])
def test_matching_reference_table_remains_ambiguous_with_empty_paragraphs(context):
    meta = document()
    meta['blocks'][2][context] = '参考文献：已有研究比较'
    with pytest.raises(Rejected, match='跨段'):
        text_privacy(meta, Agent('教师\n\n' + NAME), lambda *args: None)


@pytest.mark.parametrize('change', ['no_native_proof', 'metadata_mismatch', 'break', 'nested_table', 'different_row', 'overlapping_cell'])
def test_unknown_native_or_nonadjacent_layout_is_rejected(change):
    meta = document()
    for table in meta['blocks'][:3]:
        if change == 'no_native_proof': table.pop('native_xml')
        elif change == 'metadata_mismatch':
            table['native_xml'] = table['native_xml'].replace('<ns0:p />', '', 1)
        elif change in ['break', 'nested_table']:
            root = ET.fromstring(table['native_xml'])
            cell = root.find(W + 'tr').findall(W + 'tc')[5]
            if change == 'break': ET.SubElement(cell.findall(W + 'p')[1], W + 'br')
            else: ET.SubElement(cell, W + 'tbl')
            table['native_xml'] = ET.tostring(root, encoding='unicode')
        elif change == 'different_row': table['cells'][5]['row'] = 1
        else: table['cells'].append(copy.deepcopy(table['cells'][5]))
    with pytest.raises(Rejected, match='跨段'):
        text_privacy(meta, Agent('教师\n\n' + NAME), lambda *args: None)


def test_unrelated_nonempty_cell_is_not_skipped_to_find_a_name():
    meta = document()
    for table in meta['blocks'][:3]:
        table['cells'][5]['text'] = '\n研究内容'
        table['cells'].append({'row': 0, 'column': 6, 'text': '\n' + NAME})
        native_xml(table)
    with pytest.raises(Rejected, match='跨段'):
        text_privacy(meta, Agent('教师\n\n研究内容\n\n' + NAME), lambda *args: None)
