"""Regression cases from two evidence-classification failures, without private papers."""
from xml.etree import ElementTree as ET
import fitz
from native_docx import math_needs_review, math_text
from test_common_pdf import parse_document

NS='xmlns:m="http://schemas.openxmlformats.org/officeDocument/2006/math" xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"'

def equation(control='<w:i/>', operand=''):
    return ET.fromstring(f'<m:oMath {NS}><m:sSub><m:sSubPr><m:ctrlPr><w:rPr>{control}</w:rPr></m:ctrlPr></m:sSubPr><m:e><m:r><w:rPr>{operand}</w:rPr><m:t>V</m:t></m:r></m:e><m:sub><m:r><m:t>AB</m:t></m:r></m:sub></m:sSub></m:oMath>')

def test_control_character_italic_does_not_create_missing_formula_work():
    node=equation()
    assert math_text(node)=='{V}_{AB}'
    assert math_needs_review(node) is False

def test_operand_typography_and_unknown_math_still_need_review():
    assert math_needs_review(equation(control='',operand='<w:i/>'))
    assert math_needs_review(equation(control='<w:b/>'))
    node=ET.fromstring(f'<m:oMath {NS}><m:d><m:dPr><m:begChr m:val="["/></m:dPr><m:e><m:r><m:t>x</m:t></m:r></m:e></m:d></m:oMath>')
    assert math_needs_review(node)

def footer_pdf(*,metadata=True,extra_graphic=False,body_rule=False):
    pdf=fitz.open();page=pdf.new_page(width=600,height=800)
    page.insert_text((50,150),'Research article title',fontsize=20)
    page.insert_text((50,200),'Native research body remains available.',fontsize=11)
    y=420 if body_rule else 725
    page.draw_line((50,y),(390,y),width=.4)
    page.insert_text((50,y+12),'Manuscript Received Sept. 24, 2020; Accepted Dec. 14, 2021.' if metadata else 'Observed trend of the experimental series.',fontsize=8)
    page.insert_text((50,y+24),'Copyright 2023. DOI:10.000/example',fontsize=8)
    if extra_graphic:page.draw_circle((100,y-20),10)
    return pdf

def test_first_page_bibliographic_separator_is_not_a_research_figure(tmp_path):
    blocks,resources,issues=parse_document(footer_pdf(),tmp_path)
    assert not any(i['code']=='pdf_vector_review' for i in issues)
    text='\n'.join(b.get('text','') for b in blocks)
    assert 'Manuscript Received' in text and 'DOI:10.000/example' in text
    assert 'Native research body' in text

def test_unsupported_bottom_rule_remains_a_visual_target(tmp_path):
    _,_,issues=parse_document(footer_pdf(metadata=False),tmp_path)
    assert any(i['code']=='pdf_vector_review' for i in issues)

def test_footer_near_a_diagram_does_not_hide_its_paths(tmp_path):
    _,_,issues=parse_document(footer_pdf(extra_graphic=True),tmp_path)
    assert any(i['code']=='pdf_vector_review' for i in issues)

def test_bibliographic_words_do_not_hide_a_body_rule(tmp_path):
    _,_,issues=parse_document(footer_pdf(body_rule=True),tmp_path)
    assert any(i['code']=='pdf_vector_review' for i in issues)
