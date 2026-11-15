"""General regressions discovered during the expanded source sample."""
import io

import fitz
import pytest
from PIL import Image, ImageDraw

from content_rules import detect_title
from test_common_pdf import parse_document


def test_wrapped_title_keeps_short_subtitle_prefix():
    text = '本科毕业论文\n题目：企业合并的商誉研究——以东\n海集团为例\n学院：管理学院\n摘要'
    assert detect_title(text) == '企业合并的商誉研究——以东海集团为例'


def test_explicit_title_keeps_short_legitimate_suffix():
    assert detect_title('题目：企业税务管理研究——税制\n学院：管理学院\n摘要') == '企业税务管理研究——税制'


def test_filename_fallback_can_still_remove_author_suffix():
    assert detect_title('摘要\n正文', '企业税务管理研究-张三.docx') == '企业税务管理研究'


def test_split_cover_field_is_not_appended_to_title():
    text = '本科毕业论文\n题目：基于效率模型的\n企业治理研究\n学\n院:\n管理学院\n专\n业:\n会计学\n摘要'
    assert detect_title(text) == '基于效率模型的企业治理研究'


def test_body_without_title_does_not_promote_section_heading():
    assert detect_title('1  绪论\n研究背景与意义\n本文研究企业合并的信息披露。') == ''


def test_numbered_body_stops_title_continuation():
    assert detect_title('题目：企业合并信息披露研究\n1 绪论\n研究背景与意义') == '企业合并信息披露研究'


def test_pdf_soft_mask_preserves_visible_pixels_on_white_background(tmp_path):
    source = Image.new('RGBA', (80, 40), (0, 0, 0, 0))
    draw = ImageDraw.Draw(source)
    draw.rectangle((20, 10, 40, 25), fill=(0, 0, 0, 255))
    draw.rectangle((50, 10, 65, 25), fill=(200, 40, 20, 128))
    stream = io.BytesIO()
    source.save(stream, format='PNG')
    pdf = fitz.open()
    page = pdf.new_page(width=300, height=300)
    page.insert_text((40, 40), 'Formula with transparent image background.')
    page.insert_image(fitz.Rect(40, 60, 200, 140), stream=stream.getvalue())
    expected_color=page.get_pixmap(matrix=fitz.Matrix(.5,.5)).pixel(75,50)
    _, resources, _ = parse_document(pdf, tmp_path)
    extracted = Image.open(io.BytesIO(next(x['data'] for x in resources if x['kind']=='image'))).convert('RGB')
    assert extracted.size == (80, 40)
    assert extracted.getpixel((2, 2)) == (255, 255, 255), 'Transparent background must not become black'
    assert extracted.getpixel((30, 20)) == (0, 0, 0), 'Opaque formula strokes must remain unchanged'
    # Saving/reopening a PDF can round a premultiplied color channel by one.
    assert max(abs(a-b) for a,b in zip(extracted.getpixel((55,20)),expected_color))<=1, 'Soft alpha must match the original PDF page appearance'


def test_near_white_text_shading_does_not_become_a_research_figure(tmp_path):
    pdf = fitz.open()
    page = pdf.new_page(width=500, height=500)
    for y in (100, 125, 150):
        page.insert_text((40, y), 'Reference entry with text highlighting.', fontsize=10)
    spans = [s for b in page.get_text('dict')['blocks'] for line in b.get('lines',[]) for s in line['spans']]
    for span in spans:
        page.draw_rect(fitz.Rect(span['bbox'])+(-.2,-1.8,.2,1.8), color=None, fill=(.988,.988,.988), overlay=False)
    blocks, _, issues = parse_document(pdf, tmp_path)
    assert not any(x['code']=='pdf_vector_review' for x in issues)
    assert sum('Reference entry' in x.get('text','') for x in blocks)==3


def test_near_white_graphic_away_from_text_remains_a_visual_target(tmp_path):
    pdf = fitz.open()
    page = pdf.new_page(width=500, height=500)
    page.insert_text((40, 100), 'A pale bar is research data, not a text background.', fontsize=10)
    page.draw_rect(fitz.Rect(40, 200, 240, 230),color=None,fill=(.988,.988,.988))
    _, _, issues = parse_document(pdf, tmp_path)
    assert any(x['code']=='pdf_vector_review' for x in issues)


def test_native_variable_formatting_does_not_require_visual_formula_recovery(tmp_path):
    from test_graphics_recovery import docx, parse
    source=docx(tmp_path/'variable.docx','''<w:p><m:oMath xmlns:m="http://schemas.openxmlformats.org/officeDocument/2006/math">
        <m:r><w:rPr><w:rFonts w:ascii="Cambria Math"/><w:color w:val="000000"/><w:szCs w:val="21"/></w:rPr>
        <m:rPr><m:sty m:val="p"/></m:rPr><m:t>Turnover</m:t></m:r></m:oMath></w:p>''')
    blocks,_,issues=parse(source)
    assert any('$Turnover$' in b.get('text','') for b in blocks)
    assert not any(x['code']=='formula_review' for x in issues)


def test_unknown_math_content_still_requires_visual_formula_recovery(tmp_path):
    from test_graphics_recovery import docx, parse
    source=docx(tmp_path/'complex.docx','''<w:p><m:oMath xmlns:m="http://schemas.openxmlformats.org/officeDocument/2006/math">
        <m:nary><m:naryPr><m:chr m:val="∑"/></m:naryPr><m:e><m:r><m:t>x</m:t></m:r></m:e></m:nary>
        </m:oMath></w:p>''')
    _,_,issues=parse(source)
    assert any(x['code']=='formula_review' for x in issues)


def test_math_delimiter_properties_are_not_discarded_as_font_styling(tmp_path):
    from test_graphics_recovery import docx, parse
    source=docx(tmp_path/'delimiter.docx','''<w:p><m:oMath xmlns:m="http://schemas.openxmlformats.org/officeDocument/2006/math">
        <m:d><m:dPr><m:begChr m:val="["/><m:endChr m:val="]"/></m:dPr>
        <m:e><m:r><m:t>x</m:t></m:r></m:e></m:d></m:oMath></w:p>''')
    _,_,issues=parse(source)
    assert any(x['code']=='formula_review' for x in issues)


@pytest.mark.parametrize('property_xml', [
    '<w:vertAlign w:val="superscript"/>',
    '<w:vertAlign w:val="subscript"/>',
    '<w:vanish/>',
])
def test_math_semantic_run_properties_still_require_review(tmp_path,property_xml):
    from test_graphics_recovery import docx, parse
    source=docx(tmp_path/'semantic.docx',f'''<w:p><m:oMath xmlns:m="http://schemas.openxmlformats.org/officeDocument/2006/math">
        <m:r><m:t>x</m:t></m:r><m:r><w:rPr>{property_xml}</w:rPr><m:t>2</m:t></m:r></m:oMath></w:p>''')
    _,_,issues=parse(source)
    assert any(x['code']=='formula_review' for x in issues)


def test_pdf_image_with_differently_sized_soft_mask_is_rendered(tmp_path):
    pdf=fitz.open();page=pdf.new_page(width=100,height=100)
    stream=io.BytesIO();Image.new('RGBA',(10,10),(0,0,0,128)).save(stream,format='PNG')
    xref=page.insert_image(fitz.Rect(10,10,90,90),stream=stream.getvalue())
    mask=int(pdf.xref_get_key(xref,'SMask')[1].split()[0])
    pdf.xref_set_key(mask,'Width','5');pdf.xref_set_key(mask,'Height','5')
    pdf.update_stream(mask,bytes([128])*25)
    expected=page.get_pixmap().pixel(50,50)
    _,resources,_=parse_document(pdf,tmp_path)
    image=Image.open(io.BytesIO(next(x['data'] for x in resources if x['kind']=='image'))).convert('RGB')
    assert image.getpixel((image.width//2,image.height//2))==expected


def test_pdf_transparent_white_strokes_keep_colored_page_backdrop(tmp_path):
    pdf=fitz.open();page=pdf.new_page(width=100,height=100)
    page.draw_rect(page.rect,color=None,fill=(0,0,1))
    source=Image.new('RGBA',(20,20),(0,0,0,0));ImageDraw.Draw(source).rectangle((5,5,14,14),fill='white')
    stream=io.BytesIO();source.save(stream,format='PNG')
    page.insert_image(fitz.Rect(10,10,90,90),stream=stream.getvalue())
    _,resources,_=parse_document(pdf,tmp_path)
    image=Image.open(io.BytesIO(next(x['data'] for x in resources if x['kind']=='image'))).convert('RGB')
    assert image.getpixel((0,0))==(0,0,255)
    assert image.getpixel((image.width//2,image.height//2))==(255,255,255)


@pytest.mark.parametrize('start',['1绪论','1.1 研究背景与意义','第一章 绪论'])
def test_numbered_body_variants_do_not_become_titles(start):
    assert detect_title(start+'\n研究背景与意义\n本文研究企业的信息披露。')==''


def test_image_provenance_hashes_survive_privacy_cleanup():
    from auto_evidence import clean_strings
    from evidence import redactor
    digest='a'*10+'13800138000'+'b'*43
    value={'source_image_sha256':digest,'source_mask_sha256':digest}
    assert clean_strings(value,redactor())==value


@pytest.mark.parametrize('rotation',[0,90,180,270])
def test_pdf_page_rotation_keeps_masked_image_content(tmp_path,rotation):
    pdf=fitz.open();page=pdf.new_page(width=200,height=300)
    original=Image.new('RGBA',(80,40),(0,0,0,0));ImageDraw.Draw(original).rectangle((20,10,60,30),fill='black')
    stream=io.BytesIO();original.save(stream,format='PNG')
    page.insert_image(fitz.Rect(40,60,120,100),stream=stream.getvalue());page.set_rotation(rotation)
    _,resources,_=parse_document(pdf,tmp_path)
    image=Image.open(io.BytesIO(next(x['data'] for x in resources if x['kind']=='image'))).convert('RGB')
    assert 700<=sum(p==(0,0,0) for p in image.getdata())<=1000
    assert image.width*image.height<=3500


def test_pdf_image_placement_rotation_uses_native_pixel_density(tmp_path):
    pdf=fitz.open();page=pdf.new_page(width=200,height=300)
    original=Image.new('RGBA',(80,4),(0,0,0,0));ImageDraw.Draw(original).rectangle((20,1,60,2),fill='black')
    stream=io.BytesIO();original.save(stream,format='PNG')
    page.insert_image(fitz.Rect(40,60,44,140),stream=stream.getvalue(),rotate=90)
    _,resources,_=parse_document(pdf,tmp_path)
    image=Image.open(io.BytesIO(next(x['data'] for x in resources if x['kind']=='image')))
    assert image.size==(4,80)


def test_text_backdrop_in_short_gap_between_reference_spans_is_not_a_figure(tmp_path):
    pdf=fitz.open();page=pdf.new_page(width=300,height=300)
    page.insert_text((40,100),'[8]',fontsize=10)
    page.insert_text((65,100),'Author publication details.',fontsize=10)
    page.draw_rect(fitz.Rect(52,89,66,102),color=None,fill=(.988,.988,.988),overlay=False)
    _,_,issues=parse_document(pdf,tmp_path)
    assert not any(x['code']=='pdf_vector_review' for x in issues)


def test_pale_bar_between_distant_same_row_labels_is_preserved(tmp_path):
    pdf=fitz.open();page=pdf.new_page(width=400,height=300)
    page.insert_text((40,100),'0',fontsize=10);page.insert_text((300,100),'100',fontsize=10)
    page.draw_rect(fitz.Rect(50,89,250,102),color=None,fill=(.988,.988,.988))
    _,_,issues=parse_document(pdf,tmp_path)
    assert any(x['code']=='pdf_vector_review' for x in issues)
