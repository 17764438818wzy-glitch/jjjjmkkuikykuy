"""Synthetic PDFs guard native content preservation, independent of any paper."""
import fitz
import pytest

from evidence import parse_pdf


def parse_document(pdf, tmp_path):
    source = tmp_path / 'source.pdf'
    pdf.save(source)
    pdf.close()
    resources, issues = [], []

    def resource(rid, data, extension, kind, position):
        item = {'id': rid, 'kind': kind, 'position': position, 'data': data}
        resources.append(item)
        return item

    blocks, images, equations = parse_pdf(
        source, lambda value: value, resource,
        lambda code, message, position: issues.append({'code': code, 'position': position}))
    return blocks, resources, issues


def three_line_page(pdf, caption=True, rows=None):
    page = pdf.new_page(width=600, height=800)
    page.insert_text((60, 70), 'Research introduction remains outside the table.')
    if caption:
        page.insert_text((170, 110), 'Table 2 Operating indicators', fontsize=10)
    rows = rows or [
        ['Year', 'Revenue', 'Growth', 'Profit'],
        ['2021', '1,250.00', '-2.50%', '(150)'],
        ['2022', '0.00', '', '-12.30'],
        ['2023', '1200.40', '3.0%', '0']]
    for y in (130, 161, 239):
        # Word-produced three-line tables commonly use adjacent filled segments.
        for left, right in zip((60, 165, 280, 395), (165, 280, 395, 530)):
            page.draw_rect(fitz.Rect(left, y, right, y + .8), color=None, fill=(0, 0, 0))
    for row, baseline in zip(rows, (151, 180, 205, 230)):
        for value, x in zip(row, (70, 180, 300, 420)):
            if value:
                page.insert_text((x, baseline), value, fontsize=10)
    page.insert_text((60, 270), 'Source: original records. The conclusion remains here.', fontsize=10)
    return page


def values(table):
    return [[table['cells'][index]['text'] for index in row] for row in table['grid']]


def test_three_line_native_table_preserves_values_empty_cells_and_coordinates(tmp_path):
    pdf = fitz.open()
    three_line_page(pdf)
    blocks, resources, issues = parse_document(pdf, tmp_path)
    tables = [b for b in blocks if b['type'] == 'table']
    assert len(tables) == 1, 'Captioned three-line table must be extracted natively'
    table = tables[0]
    assert values(table) == [
        ['Year', 'Revenue', 'Growth', 'Profit'],
        ['2021', '1,250.00', '-2.50%', '(150)'],
        ['2022', '0.00', '', '-12.30'],
        ['2023', '1200.40', '3.0%', '0']]
    assert table['source'] == 'pdf_native_three_line'
    assert table['grid_validated'] is True
    assert len(table['row_positions']) == 4
    assert all(c['position']['page'] == 1 and len(c['position']['bbox']) == 4 for c in table['cells'])
    assert table['screenshot_ids'] == [resources[0]['id']]
    assert resources[0]['data'].startswith(b'\x89PNG')
    assert not any(i['code'] in {'pdf_cell_relationship_review', 'pdf_vector_review'} for i in issues)
    prose = '\n'.join(b['text'] for b in blocks if b['type'] == 'paragraph')
    assert 'introduction' in prose and 'conclusion' in prose and 'Table 2' in prose
    assert '1,250.00' not in prose


def test_repeated_margin_rules_and_white_text_backdrops_do_not_create_vector_targets(tmp_path):
    pdf = fitz.open()
    for page_no in range(3):
        page = pdf.new_page(width=600, height=800)
        page.draw_rect(fitz.Rect(45, 65, 555, 66), color=None, fill=(0, 0, 0))
        page.draw_rect(fitz.Rect(50, 110, 550, 200), color=None, fill=(1, 1, 1))
        page.insert_text((60, 140), f'Page {page_no + 1} native body text, no research graphic.')
    blocks, _, issues = parse_document(pdf, tmp_path)
    assert not any(i['code'] == 'pdf_vector_review' for i in issues)
    assert len([b for b in blocks if b['type'] == 'paragraph']) == 3


def test_distinct_real_vector_figures_keep_separate_local_targets(tmp_path):
    pdf = fitz.open()
    page = pdf.new_page(width=600, height=800)
    page.insert_text((50, 100), 'Two diagrams contain research content.')
    page.draw_circle((120, 210), 30)
    page.draw_line((150, 210), (210, 235))
    page.draw_rect(fitz.Rect(350, 470, 470, 510), color=(0, 0, 0), fill=(1, 1, 1))
    blocks, _, issues = parse_document(pdf, tmp_path)
    targets = [i for i in issues if i['code'] == 'pdf_vector_review']
    assert len(targets) == 2, 'Unrelated figures must not expand into one page-sized target'
    assert all(i['position']['bbox'][3] - i['position']['bbox'][1] < 120 for i in targets)
    assert len([b for b in blocks if b.get('unresolved_object')]) == 2


def test_regular_matrix_table_retains_native_extraction(tmp_path):
    pdf = fitz.open()
    page = pdf.new_page()
    for x in (60, 200, 340):
        page.draw_line((x, 120), (x, 210))
    for y in (120, 150, 180, 210):
        page.draw_line((60, y), (340, y))
    for row, y in zip([['Year', 'Amount'], ['2022', '10.00'], ['2023', '-5.00']], (140, 170, 200)):
        for value, x in zip(row, (70, 210)):
            page.insert_text((x, y), value, fontsize=10)
    blocks, _, issues = parse_document(pdf, tmp_path)
    table = next(b for b in blocks if b['type'] == 'table')
    assert values(table) == [['Year', 'Amount'], ['2022', '10.00'], ['2023', '-5.00']]
    assert table['source'] == 'pdf_native_lines'
    assert not any(i['code'] == 'pdf_vector_review' for i in issues)


def test_uncaptioned_three_line_shape_does_not_claim_a_validated_table(tmp_path):
    pdf = fitz.open()
    three_line_page(pdf, caption=False)
    blocks, _, issues = parse_document(pdf, tmp_path)
    assert not any(b.get('source') == 'pdf_native_three_line' for b in blocks)
    assert any(i['code'] == 'pdf_vector_review' for i in issues)


def test_repeated_body_rules_are_not_mistaken_for_margin_decoration(tmp_path):
    pdf = fitz.open()
    for _ in range(3):
        page = pdf.new_page(width=600, height=800)
        page.insert_text((50, 100), 'Body figure with repeated appearance.')
        page.draw_line((60, 420), (500, 420))
    _, _, issues = parse_document(pdf, tmp_path)
    assert len([i for i in issues if i['code'] == 'pdf_vector_review']) == 3


def test_captioned_prose_between_rules_remains_unresolved(tmp_path):
    pdf = fitz.open()
    page = pdf.new_page()
    page.insert_text((60, 100), 'Table 3 Ambiguous source', fontsize=10)
    for y in (120, 151, 221):
        page.draw_line((60, y), (500, y))
    for y, text in zip((140, 170, 200), ('A paragraph with several adjacent words.',
                       'Another prose sentence of different length.',
                       'A third line does not establish columns.')):
        page.insert_text((70, y), text, fontsize=10)
    blocks, _, issues = parse_document(pdf, tmp_path)
    assert not any(b.get('source') == 'pdf_native_three_line' for b in blocks)
    targets = [i for i in issues if i['code'] == 'pdf_vector_review']
    assert len(targets) == 1, 'Failed native validation must leave the full table region for visual recovery'
    assert targets[0]['position']['bbox'][1] <= 120
    assert targets[0]['position']['bbox'][3] >= 221


def test_multiline_body_cells_cannot_be_claimed_as_extra_validated_rows(tmp_path):
    pdf = fitz.open()
    page = three_line_page(pdf)
    # A second line inside every cell cannot be distinguished from a new row
    # by text alignment alone; retain the region for visual interpretation.
    for x, value in zip((70, 180, 300, 420), ('detail', 'detail', 'detail', 'detail')):
        page.insert_text((x, 191), value, fontsize=10)
    blocks, _, issues = parse_document(pdf, tmp_path)
    assert not any(b.get('source') == 'pdf_native_three_line' for b in blocks)
    assert any(i['code'] == 'pdf_vector_review' for i in issues)


def test_one_off_margin_graphic_is_not_removed_as_a_repeated_rule(tmp_path):
    pdf = fitz.open()
    page = pdf.new_page(width=600, height=800)
    page.insert_text((60, 130), 'A single-page source retains ambiguous graphics.')
    page.draw_line((50, 65), (550, 65))
    _, _, issues = parse_document(pdf, tmp_path)
    assert any(i['code'] == 'pdf_vector_review' for i in issues)


def test_table_clip_cannot_silently_truncate_a_source_word(tmp_path):
    pdf = fitz.open()
    three_line_page(pdf, rows=[
        ['Year', 'Revenue', 'Growth', 'Profitabcdefghijklmnopqrstuvwxyz'],
        ['2021', '1,250.00', '-2.50%', '(150)'],
        ['2022', '0.00', '', '-12.30'],
        ['2023', '1200.40', '3.0%', '0']])
    blocks, _, issues = parse_document(pdf, tmp_path)
    assert not any(b.get('source') == 'pdf_native_three_line' for b in blocks)
    assert any(i['code'] == 'pdf_vector_review' for i in issues)
    assert any('Profitabcdefghijklmnopqrstuvwxyz' in b.get('text', '') for b in blocks)


def test_diagram_stroke_inside_native_table_is_not_discarded_as_a_border(tmp_path):
    pdf = fitz.open()
    page = three_line_page(pdf)
    page.draw_line((360, 195), (380, 211))
    blocks, _, issues = parse_document(pdf, tmp_path)
    assert any(b.get('source') == 'pdf_native_three_line' for b in blocks)
    assert any(i['code'] == 'pdf_vector_review' for i in issues)


@pytest.mark.parametrize('shape', ['step', 'rectangle', 'horizontal'])
def test_orthogonal_graphic_inside_three_line_table_remains_a_visual_target(tmp_path, shape):
    pdf = fitz.open()
    page = three_line_page(pdf)
    if shape == 'step':
        path = page.new_shape()
        path.draw_polyline([(350, 196), (365, 196), (365, 206), (380, 206)])
        path.finish(color=(0, 0, 0), closePath=False)
        path.commit()
    elif shape == 'rectangle':
        page.draw_rect(fitz.Rect(350, 190, 380, 210))
    else:
        page.draw_line((350, 200), (380, 200))
    blocks, _, issues = parse_document(pdf, tmp_path)
    assert any(b.get('source') == 'pdf_native_three_line' for b in blocks)
    targets = [i for i in issues if i['code'] == 'pdf_vector_review']
    assert len(targets) == 1, 'Orthogonal research marks inside a table are not grid boundaries'
    assert targets[0]['position']['bbox'][0] <= 350
    assert targets[0]['position']['bbox'][2] >= 380


@pytest.mark.parametrize('shape', ['step', 'rectangle'])
def test_internal_matrix_graphic_is_not_covered_by_the_native_cell_bounds(tmp_path, shape):
    pdf = fitz.open()
    page = pdf.new_page()
    for x in (60, 200, 340):
        page.draw_line((x, 120), (x, 240))
    for y in (120, 160, 200, 240):
        page.draw_line((60, y), (340, y))
    for row, y in zip([['Year', 'Amount'], ['2022', '10.00'], ['2023', '-5.00']], (140, 180, 220)):
        for value, x in zip(row, (70, 210)):
            page.insert_text((x, y), value, fontsize=10)
    if shape == 'step':
        path = page.new_shape()
        path.draw_polyline([(280, 172), (295, 172), (295, 182), (310, 182)])
        path.finish(color=(0, 0, 0), closePath=False)
        path.commit()
    else:
        page.draw_rect(fitz.Rect(280, 170, 310, 190))
    blocks, _, issues = parse_document(pdf, tmp_path)
    assert any(b.get('source') == 'pdf_native_lines' for b in blocks)
    assert any(i['code'] == 'pdf_vector_review' for i in issues)


def test_almost_square_rule_joint_uses_its_known_boundary_not_rounding_orientation(tmp_path):
    pdf = fitz.open()
    page = three_line_page(pdf)
    page.draw_rect(fitz.Rect(165, 161, 165.71998, 161.72003), color=None, fill=(0, 0, 0))
    blocks, _, issues = parse_document(pdf, tmp_path)
    assert any(b.get('source') == 'pdf_native_three_line' for b in blocks)
    assert not any(i['code'] == 'pdf_vector_review' for i in issues)


def test_uncertain_captioned_table_keeps_table_identity_and_native_text(tmp_path):
    pdf = fitz.open()
    page = three_line_page(pdf)
    page.insert_text((70, 216), 'continued label', fontsize=10)
    blocks, _, _ = parse_document(pdf, tmp_path)
    targets = [b for b in blocks if b.get('table_candidate')]
    assert len(targets) == 1
    assert targets[0]['caption'] == 'Table 2 Operating indicators'
    assert '1,250.00' in '\n'.join(targets[0]['locator_texts'])
    assert 'continued label' in '\n'.join(targets[0]['locator_texts'])
    assert targets[0]['position']['bbox'][1] <= 130
    assert targets[0]['position']['bbox'][3] >= 239


def continuation_document(with_previous=True, shifted=False):
    pdf = fitz.open()
    if with_previous:
        page = pdf.new_page(width=600, height=800)
        page.insert_text((170, 610), 'Table 9 Quarterly records', fontsize=10)
        for y in (630, 660, 765):
            page.draw_line((60, y), (530, y))
        for row, y in zip([['Period', 'Income', 'Rate', 'Profit'], ['Q1', '10.00', '-2.50%', '(2)'],
                           ['Q2', '12.00', '', '-3.40'], ['Q3', '14.00', '2.00%', '0']], (651, 687, 720, 751)):
            for text, x in zip(row, (70, 180, 300, 420)):
                if text:page.insert_text((x, y), text, fontsize=10)
    page = pdf.new_page(width=600, height=800)
    offset = 35 if shifted else 0
    for y in (90, 165):
        page.draw_line((60 + offset, y), (530 + offset, y))
    for row, y in zip([['Supplement', '44.00', '-3.00%', '0'], ['Ratio', '5.40%', '', '7.00%']], (120, 150)):
        for text, x in zip(row, (70, 180, 300, 420)):
            if text:page.insert_text((x + offset, y), text, fontsize=10)
    page.insert_text((60, 200), 'The original discussion continues here.')
    return pdf


def test_uncaptioned_page_start_fragment_uses_previous_table_geometry_without_guessing_headers(tmp_path):
    blocks, _, _ = parse_document(continuation_document(), tmp_path)
    fragment = [b for b in blocks if b.get('table_fragment')]
    assert len(fragment) == 1
    assert fragment[0]['table_candidate'] is True
    assert fragment[0]['position']['page'] == 2
    assert fragment[0]['possible_previous_table']['page'] == 1
    assert fragment[0]['caption'] == ''
    assert '5.40%' in '\n'.join(fragment[0]['locator_texts'])
    assert not any(b['type'] == 'table' and b['position']['page'] == 2 for b in blocks)


@pytest.mark.parametrize('with_previous,shifted', [(False, False), (True, True)])
def test_page_start_rules_cannot_claim_continuation_without_matching_previous_geometry(tmp_path, with_previous, shifted):
    blocks, _, issues = parse_document(continuation_document(with_previous, shifted), tmp_path)
    assert not any(b.get('table_fragment') for b in blocks)
    assert any(i['code'] == 'pdf_vector_review' for i in issues)
