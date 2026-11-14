"""A source clip is usable only when the actual DOCX chart uniquely proves it."""
import copy
import zipfile
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape

import fitz
import pytest

import native_chart_region


def docx(tmp_path, *, categories=('Alpha', 'Beta', 'Gamma'), extent=(300, 140),
         table=False, extra_chart=False, external=False, rotation=0, missing_point=False,
         multilvl=False, populated_extra_level=False):
    drawing = f'''<w:drawing><wp:inline><wp:extent cx="{round(extent[0]*12700)}" cy="{round(extent[1]*12700)}"/>
      <a:graphic><a:graphicData><c:chart r:id="rId7"/></a:graphicData></a:graphic>
      <a:xfrm rot="{rotation}"/></wp:inline></w:drawing>'''
    para = '<w:p><w:r>' + drawing * (2 if extra_chart else 1) + '</w:r></w:p>'
    body = ('<w:tbl><w:tblGrid><w:gridCol/><w:gridCol/></w:tblGrid><w:tr>'
            '<w:tc><w:p><w:r><w:t>Question</w:t></w:r></w:p></w:tc><w:tc>' + para +
            '</w:tc></w:tr></w:tbl>') if table else para
    ns = ('xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main" '
          'xmlns:wp="http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing" '
          'xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" '
          'xmlns:c="http://schemas.openxmlformats.org/drawingml/2006/chart" '
          'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"')
    points = ''.join(f'<c:pt idx="{i}"><c:v>{escape(label)}</c:v></c:pt>'
                     for i, label in enumerate(categories) if not (missing_point and i == 1))
    cache = f'<c:strRef><c:strCache><c:ptCount val="{len(categories)}"/>{points}</c:strCache></c:strRef>'
    if multilvl:
        extra = points if populated_extra_level else ''
        cache = f'<c:multiLvlStrRef><c:multiLvlStrCache><c:ptCount val="{len(categories)}"/><c:lvl>{points}</c:lvl><c:lvl>{extra}</c:lvl></c:multiLvlStrCache></c:multiLvlStrRef>'
    chart = f'''<c:chartSpace {ns}><c:chart><c:plotArea><c:barChart><c:ser><c:cat>{cache}
      </c:cat></c:ser></c:barChart></c:plotArea></c:chart></c:chartSpace>'''
    source = tmp_path / 'source.docx'
    mode = ' TargetMode="External"' if external else ''
    with zipfile.ZipFile(source, 'w') as archive:
        archive.writestr('word/document.xml', f'<w:document {ns}><w:body>{body}</w:body></w:document>')
        archive.writestr('word/_rels/document.xml.rels',
                        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                        '<Relationship Id="rId7" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/chart" '
                        f'Target="charts/chart1.xml"{mode}/></Relationships>')
        archive.writestr('word/charts/chart1.xml', chart)
    return source


def add_clip(pdf, *, labels=('Alpha', 'Beta', 'Gamma'), bounds=(100, 110, 400, 250),
             rotation=0, rectangular=True):
    page = pdf.new_page(width=600, height=800)
    bounds = fitz.Rect(bounds)
    for i, label in enumerate(labels):
        page.insert_text((bounds.x0 + 10 + i * 85, bounds.y1 - 14), label, fontsize=8)
        page.draw_rect((bounds.x0 + 20 + i * 85, bounds.y0 + 15 + i * 10,
                        bounds.x0 + 40 + i * 85, bounds.y1 - 30), color=None, fill=(0, .4, .8))
    contents = b'\n'.join(pdf.xref_stream(xref) for xref in page.get_contents())
    if rectangular:
        clip = f'{bounds.x0} {800-bounds.y1} {bounds.width} {bounds.height} re W n\n'
    else:
        clip = f'{bounds.x0} {800-bounds.y1} m {bounds.x1} {800-bounds.y1} l {bounds.x1} {800-bounds.y0} l h W n\n'
    xref = pdf.get_new_xref()
    pdf.update_object(xref, '<<>>')
    pdf.update_stream(xref, b'q\n' + clip.encode() + contents + b'\nQ')
    page.set_contents(xref)
    page.set_rotation(rotation)
    return page


def target(table=False):
    return {'id': 'chart_target', 'position': {'part': 'word/document.xml', 'body_index': 0,
            **({'row': 0, 'column': 1} if table else {})}}


@pytest.mark.parametrize('table', [False, True])
def test_returns_exact_source_clip_for_unique_chart_without_mutating_inputs(tmp_path, table):
    source = docx(tmp_path, table=table)
    original = source.read_bytes()
    request = target(table)
    before = copy.deepcopy(request)
    with fitz.open() as pdf:
        add_clip(pdf)
        result = native_chart_region.find_native_chart_region(source, request, pdf)
        assert result is not None
        assert result['page_index'] == 0
        assert result['bbox'] == pytest.approx([100/600, 110/800, 400/600, 250/800])
        assert result['provenance']['chart_part'] == 'word/charts/chart1.xml'
        assert result['provenance']['relationship_id'] == 'rId7'
        assert result['provenance']['bbox_pt'] == pytest.approx([100, 110, 400, 250])
    assert source.read_bytes() == original and request == before


@pytest.mark.parametrize('failure', ['duplicate', 'missing_label', 'substring_label', 'extent',
                                    'rotated_page', 'nonrectangular', 'whole_page'])
def test_ambiguous_or_unproven_pdf_region_is_rejected(tmp_path, failure):
    source = docx(tmp_path, extent=(600, 800) if failure == 'whole_page' else (300, 140))
    options = {}
    if failure == 'missing_label': options['labels'] = ('Alpha', 'Beta')
    if failure == 'substring_label': options['labels'] = ('AlphaBeta', 'Gamma')
    if failure == 'extent': options['bounds'] = (100, 110, 390, 250)
    if failure == 'rotated_page': options['rotation'] = 90
    if failure == 'nonrectangular': options['rectangular'] = False
    if failure == 'whole_page': options['bounds'] = (0, 0, 600, 800)
    with fitz.open() as pdf:
        add_clip(pdf, **options)
        if failure == 'duplicate': add_clip(pdf)
        assert native_chart_region.find_native_chart_region(source, target(), pdf) is None


@pytest.mark.parametrize('options', [{'extra_chart': True}, {'external': True}, {'rotation': 5400000},
                                     {'missing_point': True}, {'categories': ('0', '1', '2')}])
def test_ambiguous_or_unsupported_source_object_is_rejected(tmp_path, options):
    source = docx(tmp_path, **options)
    with fitz.open() as pdf:
        add_clip(pdf, labels=options.get('categories', ('Alpha', 'Beta', 'Gamma')))
        assert native_chart_region.find_native_chart_region(source, target(), pdf) is None


def test_wrong_table_cell_does_not_borrow_adjacent_chart(tmp_path):
    source = docx(tmp_path, table=True)
    request = target(True)
    request['position']['column'] = 0
    with fitz.open() as pdf:
        add_clip(pdf)
        assert native_chart_region.find_native_chart_region(source, request, pdf) is None


def test_small_renderer_extent_rounding_is_allowed_but_bbox_is_never_expanded(tmp_path):
    source = docx(tmp_path, extent=(300.1, 140.05))
    with fitz.open() as pdf:
        add_clip(pdf)
        result = native_chart_region.find_native_chart_region(source, target(), pdf)
        assert result is not None
        assert result['provenance']['bbox_pt'] == pytest.approx([100, 110, 400, 250])


@pytest.mark.parametrize('extra_level', [False, True])
def test_single_complete_category_level_with_empty_extra_level_is_only_a_locator(tmp_path, extra_level):
    source = docx(tmp_path, multilvl=True, populated_extra_level=extra_level)
    with fitz.open() as pdf:
        add_clip(pdf)
        result = native_chart_region.find_native_chart_region(source, target(), pdf)
        assert (result is not None) is (not extra_level)


def test_one_chart_cannot_resolve_a_target_containing_multiple_graphic_members(tmp_path):
    source = docx(tmp_path)
    request = target()
    request['graphic_members'] = [{'object_ref': 'one'}, {'object_ref': 'two'}]
    with fitz.open() as pdf:
        add_clip(pdf)
        assert native_chart_region.find_native_chart_region(source, request, pdf) is None


def add_source_chart(source, *, location='body', different_labels=False, missing_extent=False):
    """A distinct chart part and relationship, with deliberately different values."""
    from native_chart_region import W, WP, C, R
    with zipfile.ZipFile(source) as archive:
        files = {name: archive.read(name) for name in archive.namelist()}
    root = ET.fromstring(files['word/document.xml'])
    body = root.find(W + 'body')
    second = copy.deepcopy(body[0])
    second.find('.//' + C + 'chart').set(R + 'id', 'rId8')
    if missing_extent:
        layout = second.find('.//' + WP + 'inline')
        layout.remove(layout.find(WP + 'extent'))
    if location == 'hidden':
        props = ET.SubElement(second.find(W + 'r'), W + 'rPr')
        ET.SubElement(props, W + 'vanish')
    if location == 'container':
        content = ET.Element(W + 'sdt')
        ET.SubElement(content, W + 'sdtContent').append(second)
        body.append(content)
    elif location == 'header':
        header = ET.Element(W + 'hdr')
        header.append(second)
        files['word/header1.xml'] = ET.tostring(header)
    else:
        body.append(second)
    files['word/document.xml'] = ET.tostring(root)
    rels = ET.fromstring(files['word/_rels/document.xml.rels'])
    rel = copy.deepcopy(rels[0])
    rel.set('Id', 'rId8')
    rel.set('Target', 'charts/chart2.xml')
    rels.append(rel)
    files['word/_rels/document.xml.rels'] = ET.tostring(rels)
    if location == 'header':
        header_rels = ET.Element(rels.tag)
        header_rels.append(rel)
        files['word/_rels/header1.xml.rels'] = ET.tostring(header_rels)
    first = ET.fromstring(files['word/charts/chart1.xml'])
    second_chart = copy.deepcopy(first)
    if different_labels:
        for label in second_chart.findall('.//' + C + 'cat//' + C + 'v'):
            label.text = 'Other' + label.text
    for chart, values in ((first, ('10', '20', '30')), (second_chart, ('90', '80', '70'))):
        val = ET.SubElement(chart.find('.//' + C + 'ser'), C + 'val')
        cache = ET.SubElement(val, C + 'numLit')
        ET.SubElement(cache, C + 'ptCount', val='3')
        for index, value in enumerate(values):
            ET.SubElement(ET.SubElement(cache, C + 'pt', idx=str(index)), C + 'v').text = value
    files['word/charts/chart1.xml'] = ET.tostring(first)
    files['word/charts/chart2.xml'] = ET.tostring(second_chart)
    with zipfile.ZipFile(source, 'w') as archive:
        for name, data in files.items():
            archive.writestr(name, data)


@pytest.mark.parametrize('location', ['body', 'container', 'hidden', 'header'])
def test_unique_pdf_clip_cannot_be_borrowed_by_two_source_charts(tmp_path, location):
    source = docx(tmp_path)
    add_source_chart(source, location=location)
    requests = [target()]
    if location == 'body':
        requests.append({'position': {'part': 'word/document.xml', 'body_index': 1}})
    with fitz.open() as pdf:
        add_clip(pdf)
        for request in requests:
            assert native_chart_region.find_native_chart_region(source, request, pdf) is None


def test_unlocatable_other_chart_with_same_labels_prevents_source_uniqueness(tmp_path):
    source = docx(tmp_path)
    add_source_chart(source, location='container', missing_extent=True)
    with fitz.open() as pdf:
        add_clip(pdf)
        assert native_chart_region.find_native_chart_region(source, target(), pdf) is None


def test_other_source_chart_with_distinct_complete_labels_does_not_block(tmp_path):
    source = docx(tmp_path)
    add_source_chart(source, different_labels=True)
    with fitz.open() as pdf:
        add_clip(pdf)
        assert native_chart_region.find_native_chart_region(source, target(), pdf) is not None
