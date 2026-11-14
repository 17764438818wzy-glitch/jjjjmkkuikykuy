"""Deterministic regressions from the current batch; no remote model calls."""
import fitz
import pytest

from evidence import retained_blocks
from test_common_pdf import parse_document


@pytest.mark.parametrize('marker', ['1', '29', '123'])
def test_superscript_numeric_footnote_rule_is_not_a_research_figure(tmp_path, marker):
    pdf = fitz.open()
    page = pdf.new_page(width=600, height=800)
    page.insert_text((90, 650), 'Original research body and evidence remain available.', fontsize=12)
    page.draw_line((90, 718), (234, 718), width=.35)
    page.insert_text((90, 730.4), marker, fontsize=6.85)
    marker_end = 90 + fitz.get_text_length(marker, fontsize=6.85)
    page.insert_text((marker_end, 730.4), ' ', fontsize=9)
    page.insert_text((marker_end + 4.4, 734), 'The original source note remains available.', fontsize=9)
    blocks, _, issues = parse_document(pdf, tmp_path)
    assert not any(i['code'] == 'pdf_vector_review' for i in issues)
    assert 'original source note' in ''.join(b.get('text', '') for b in blocks)


@pytest.mark.parametrize('kind', ['same_font', 'separate_same_font_marker', 'nearby_plot', 'middle_of_body'])
def test_numbered_legend_or_plot_rule_is_not_dropped(tmp_path, kind):
    pdf = fitz.open()
    page = pdf.new_page(width=600, height=800)
    page.insert_text((90, 200), 'Original research body and evidence remain available.', fontsize=12)
    y = 400 if kind == 'middle_of_body' else 718
    page.draw_line((90, y), (234, y), width=.35)
    if kind == 'same_font':
        page.insert_text((90, y + 16), '29 The research legend remains available.', fontsize=9)
    elif kind == 'separate_same_font_marker':
        page.insert_text((90, y + 16), '29 ', fontname='cour', fontsize=9)
        page.insert_text((107, y + 16), 'The research legend remains available.', fontsize=9)
    else:
        page.insert_text((90, y + 11), '29', fontsize=6.85)
        page.insert_text((108, y + 16), 'The research legend remains available.', fontsize=9)
    if kind == 'nearby_plot':
        page.draw_line((90, y), (90, y - 60), width=.35)
    _, _, issues = parse_document(pdf, tmp_path)
    assert any(i['code'] == 'pdf_vector_review' for i in issues)


def ack_blocks(*, title_bbox=(90, 80, 120, 94), vector_bbox=(88.4, 78.5, 121.8, 96), table=False):
    vector = {'type': 'paragraph', 'text': '[解析说明：此位置有待识别的矢量绘图。]',
              'unresolved_object': True, 'table_candidate': table,
              'position': {'page': 15, 'bbox': list(vector_bbox), 'vector_region': True}}
    title = {'type': 'paragraph', 'text': '致谢', 'position': {'page': 15, 'bbox': list(title_bbox)}}
    blocks = [
        {'type': 'paragraph', 'text': 'Original research conclusion.', 'position': {'page': 14, 'bbox': [90, 400, 500, 430]}},
        vector, title,
        {'type': 'paragraph', 'text': 'Acknowledgement body.', 'position': {'page': 15, 'bbox': [90, 105, 500, 130]}}]
    return blocks, vector


def test_vector_outline_of_ack_title_cannot_escape_ack_filter_by_sorting_first():
    blocks, vector = ack_blocks()
    kept = retained_blocks(blocks)
    assert vector not in kept
    assert [b['text'] for b in kept] == ['Original research conclusion.']


@pytest.mark.parametrize('kind', ['preceding_research', 'different_page', 'table_candidate'])
def test_ack_filter_preserves_research_objects_before_the_title(kind):
    blocks, vector = ack_blocks(table=kind == 'table_candidate')
    if kind == 'preceding_research':
        vector['position']['bbox'] = [90, 20, 400, 70]
    elif kind == 'different_page':
        vector['position']['page'] = 14
    assert vector in retained_blocks(blocks)
