"""Text-supported rules must not become missing research graphics."""
import fitz
import pytest

from test_common_pdf import parse_document


@pytest.mark.parametrize('filled', [False, True])
def test_footnote_separator_preserves_note_text_without_missing_graphic(tmp_path, filled):
    pdf = fitz.open()
    page = pdf.new_page(width=600, height=800)
    page.insert_text((90, 650), 'The body discusses the original research source.', fontsize=12)
    page.insert_text((90, 734), '① Source note remains available.', fontname='china-s', fontsize=9)
    if filled:
        page.draw_rect((90, 718, 234, 718.6), color=None, fill=(0, 0, 0))
    else:
        page.draw_line((90, 718), (234, 718), width=.5)
    blocks, _, issues = parse_document(pdf, tmp_path)
    assert not any(i['code']=='pdf_vector_review' for i in issues)
    assert 'Source note remains available.' in ''.join(b.get('text','') for b in blocks)


@pytest.mark.parametrize('font_size',[9,16])
def test_frontmatter_thesis_header_rule_does_not_need_majority_page_repetition(tmp_path,font_size):
    pdf = fitz.open()
    page = pdf.new_page(width=600, height=800)
    page.insert_text((180, 65), 'University undergraduate thesis', fontsize=font_size)
    page.draw_rect((90, 78, 510, 78.7), color=None, fill=(0,0,0))
    page.insert_text((250, 115), 'Abstract', fontsize=16)
    for _ in range(5):
        pdf.new_page(width=600, height=800).insert_text((90, 130), 'Research text')
    _, _, issues = parse_document(pdf, tmp_path)
    assert not any(i['code']=='pdf_vector_review' for i in issues)


def test_short_underline_inside_native_prose_does_not_become_a_figure(tmp_path):
    pdf = fitz.open()
    page = pdf.new_page(width=600, height=800)
    page.insert_text((90, 250), 'The original research discusses this topic in detail.', fontsize=12)
    page.draw_line((110, 251), (122, 251), width=.6)
    _, _, issues = parse_document(pdf, tmp_path)
    assert not any(i['code']=='pdf_vector_review' for i in issues)


def test_underline_in_native_prose_split_at_a_note_marker_is_still_text(tmp_path):
    pdf=fitz.open();page=pdf.new_page(width=600,height=800)
    page.insert_text((90,250),'Original body text before',fontname='cour',fontsize=12)
    page.insert_text((264,246),'1',fontsize=6)
    page.insert_text((272,250),'and after it.',fontname='cour',fontsize=12)
    page.draw_line((290,251),(302,251),width=.6)
    _,_,issues=parse_document(pdf,tmp_path)
    assert not any(i['code']=='pdf_vector_review' for i in issues)


@pytest.mark.parametrize('kind', ['unlabelled_rule', 'body_legend', 'nearby_plot', 'small_circle', 'revision_bar'])
def test_unsupported_small_drawing_remains_a_review_target(tmp_path, kind):
    pdf = fitz.open()
    page = pdf.new_page(width=600, height=800)
    page.insert_text((90, 120), 'The original body remains available.', fontsize=12)
    y = 400 if kind=='body_legend' else 718
    if kind in {'body_legend', 'nearby_plot'}:
        page.insert_text((90, y+16), '① Source or diagram legend.', fontname='china-s', fontsize=9)
    if kind=='small_circle':
        page.draw_circle((100, 400), 6)
        page.insert_text((98, 403), '1', fontsize=9)
    elif kind=='revision_bar':
        page.draw_line((81, 270), (81, 292), width=.6)
    else:
        page.draw_line((90, y), (234, y), width=.5)
        if kind=='nearby_plot':
            page.draw_line((90, y), (90, y-60), width=.5)
    _, _, issues = parse_document(pdf, tmp_path)
    assert any(i['code']=='pdf_vector_review' for i in issues)
