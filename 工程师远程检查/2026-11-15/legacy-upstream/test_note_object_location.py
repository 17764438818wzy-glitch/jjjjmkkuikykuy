"""Regression: one OOXML note may span several rendered pages."""
from evidence_tasks import _locate_single


def test_note_opening_on_another_page_does_not_override_object_labels():
    opening = '这段尾注开头仅位于第一页，详细说明问卷题项的背景及编制过程。'
    position = {'part': 'word/endnotes.xml', 'id': '0',
                'block_index': 2, 'object_ref': 'object_0001'}
    blocks = [
        {'type': 'paragraph', 'text': '[^endnoteReference0]: ' + opening,
         'position': {'part': 'word/endnotes.xml', 'id': '0', 'block_index': 0}},
        {'type': 'paragraph', 'id': 'object_0001',
         'text': '[解析说明：此位置的原文图形需定向识别。]',
         'position': position, 'graphic_object': True,
         'locator_texts': ['访谈实际流程', '初始资料分析', '形成类别编码']},
    ]
    pages = [opening, '访谈实际流程\n初始资料分析\n形成类别编码']

    assert _locate_single({'position': position}, blocks, pages) == {1}
