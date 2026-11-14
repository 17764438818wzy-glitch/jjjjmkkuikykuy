"""Protect research tables and original page coordinates during material cleanup."""
import copy
import pytest


def paragraph(text, index, **extra):
    return dict(id=f'src_{index:05d}', type='paragraph', text=text,
                position={'part': 'word/document.xml', 'body_index': index}, **extra)


def table(rows, index):
    cells = [dict(row=r, column=c, rowspan=1, colspan=1, text=text)
             for r, row in enumerate(rows) for c, text in enumerate(row)]
    width = len(rows[0])
    return dict(id=f'src_{index:05d}', type='table', rows=len(rows), columns=width,
                cells=cells, grid=[[r*width+c for c in range(width)] for r in range(len(rows))],
                header_rows=[], position={'part': 'word/document.xml', 'body_index': index})


def cover(index=3):
    return table([['二 级 学 院', '工程学院'], ['专 业', '自动化'],
                  ['学号姓名', '某同学 202301001'], ['指 导 教 师', '某老师'],
                  ['完成时间', '2026年5月']], index)


def test_native_cover_table_without_text_is_removed_but_title_and_inline_abstract_stay():
    from preextract_material import clean_blocks
    rows = [paragraph('本科毕业设计（论文）', 0), paragraph('机器人控制系统设计', 1),
            cover(), paragraph('摘 要：研究完整过程', 4), paragraph('1 研究', 5)]
    before = copy.deepcopy(rows)
    kept, removed = clean_blocks(rows)
    assert rows == before  # Detection must not rewrite cells or input evidence.
    assert rows[2] not in kept
    assert rows[1] in kept and rows[3] in kept
    assert any(r['block']['id'] == rows[2]['id'] for r in removed)


def test_research_tables_before_abstract_in_body_and_appendix_are_identical():
    from preextract_material import clean_blocks
    data = [['姓名', '专业', '实验结果'], ['受访者甲', '自动化', '0.125']]
    front = table(data, 4)
    body = table(data, 7)
    appendix = table(data, 11)
    rows = [paragraph('本科毕业论文', 0), paragraph('问卷测量方法研究', 1), cover(),
            front, paragraph('摘要', 5), paragraph('1 研究方法', 6), body,
            paragraph('致谢', 8), paragraph('感谢支持', 9),
            paragraph('附录A 调查问卷', 10), appendix]
    originals = copy.deepcopy([front, body, appendix])
    kept, _ = clean_blocks(rows)
    assert [r for r in kept if r['type'] == 'table'] == originals


def test_acknowledgement_word_inside_a_research_table_is_not_a_section_heading():
    from preextract_material import clean_blocks
    example=table([['致谢']],1)
    rows=[paragraph('1 文本分类实验',0),example,paragraph('该词用于类别标签，频次为12。',2)]
    assert clean_blocks(rows)[0]==rows


def test_degree_topic_without_a_generic_cover_heading_does_not_delete_a_table():
    from preextract_material import clean_blocks
    rows = [paragraph('毕业论文指导效果调查研究', 0), cover(1), paragraph('摘要', 2)]
    assert clean_blocks(rows)[0] == rows


def test_mixed_cover_and_research_table_is_conservatively_retained():
    from preextract_material import clean_blocks
    mixed = table([['姓名', '某人'], ['专业', '化学'], ['指导教师', '某老师'],
                   ['实验结果', '关键数值 1.25']], 2)
    rows = [paragraph('本科毕业论文', 0), paragraph('化学实验设计', 1), mixed, paragraph('摘要', 3)]
    assert mixed in clean_blocks(rows)[0]


@pytest.mark.parametrize('rows',[
    [['学生姓名','受试者甲','受试者乙'],['专业','自动化','化学'],['指导教师','甲导师','乙导师']],
    [['学生姓名','受试者甲'],['专业','自动化'],['指导教师','甲导师']],
])
def test_explicit_research_caption_prevents_cover_form_classification(rows):
    from preextract_material import clean_blocks
    research=table(rows,3)
    blocks=[paragraph('本科毕业论文',0),paragraph('毕业论文指导效果调查研究',1),
            paragraph('表1 调查对象基本资料（实验组与对照组）',2),research,paragraph('摘要',4)]
    assert research in clean_blocks(blocks)[0]


@pytest.mark.parametrize('heading',['1 调查对象与方法','第二章 调查对象','研究方法'])
def test_research_heading_without_an_abstract_ends_cover_detection(heading):
    from preextract_material import clean_blocks
    research=table([['学生姓名','受试者甲'],['专业','自动化'],['指导教师','某导师']],3)
    rows=[paragraph('本科毕业论文',0),paragraph('问卷样本对比研究',1),
          paragraph(heading,2,heading_level=1),research,paragraph('2 结果',4),paragraph('致谢',5)]
    assert research in clean_blocks(rows)[0]


def test_unlabelled_research_table_image_and_notes_after_acknowledgement_are_preserved():
    from preextract_material import clean_blocks
    research = table([['参数', '数值'], ['温度', '35']], 3)
    image = paragraph('[[IMAGE:research_diagram]]', 4)
    note = dict(id='footnote_1', type='paragraph', text='公式推导条件',
                position={'part': 'word/footnotes.xml', 'note_id': 1})
    rows = [paragraph('1 研究', 0), paragraph('致谢', 1), paragraph('感谢支持', 2), research, image, note]
    kept, _ = clean_blocks(rows)
    assert research in kept and image in kept and note in kept


def test_declaration_does_not_retain_a_supervisor_field_styled_as_a_heading():
    from preextract_material import clean_blocks
    rows=[paragraph('本科毕业设计（论文）',0),paragraph('控制方法研究',1),cover(2),
          paragraph('独创性声明',3),paragraph('本人声明',4),
          paragraph('指导教师：某老师',5,heading_level=1),paragraph('摘要：研究内容',6)]
    kept,_=clean_blocks(rows)
    assert rows[5] not in kept and rows[6] in kept and rows[1] in kept


def test_wrapped_short_acknowledgement_tail_uses_its_adjacent_source_paragraph():
    import fitz
    from preextract_page_cleanup import PageCleanup
    with fitz.open() as pdf:
        page=pdf.new_page()
        page.insert_text((30,60),'We would like to express our')
        page.insert_text((30,78),'thanks!')
        removed=[dict(reason='bounded_nonresearch_section',block=paragraph('We would like to express our thanks!',0))]
        cleaner=PageCleanup([],removed)
        assert cleaner(page) is False
        assert len(cleaner.records[0]['boxes'])==2


def test_mixed_reference_acknowledgement_page_keeps_reference_pixels_and_original_page_numbers(tmp_path):
    import fitz
    from preextract_page_cleanup import PageCleanup
    from auto_evidence import source_pages
    source = tmp_path/'mixed.pdf'
    with fitz.open() as pdf:
        page = pdf.new_page(width=400, height=500)
        page.insert_text((30, 60), 'References: research evidence must survive.')
        page.insert_text((30, 250), 'Acknowledgments')
        page.insert_text((30, 280), 'Thanks to everyone who supported the project.')
        page = pdf.new_page(width=400, height=500)
        page.insert_text((30, 60), 'Thanks for the continued support.')
        page = pdf.new_page(width=400, height=500)
        page.insert_text((30, 60), 'Appendix: program and research diagram')
        page.draw_circle((150, 200), 50, color=(0, 0, 1))
        pdf.save(source)
    keep = [paragraph('References: research evidence must survive.', 0),
            paragraph('Appendix: program and research diagram', 4)]
    removed = [dict(reason='bounded_nonresearch_section', block=paragraph(text, i+1))
               for i, text in enumerate(['Acknowledgments',
                   'Thanks to everyone who supported the project.', 'Thanks for the continued support.'])]
    cleaner = PageCleanup(keep, removed)
    records = source_pages(source, tmp_path/'render', page_filter=cleaner, page_transform=cleaner.apply, return_records=True)
    assert [n for n, _ in records] == [1, 3]
    from PIL import Image, ImageChops
    import io
    with fitz.open(source) as pdf:
        original = Image.open(io.BytesIO(pdf[0].get_pixmap(matrix=fitz.Matrix(1.5, 1.5)).tobytes('png')))
        changed = Image.open(io.BytesIO(records[0][1]))
        assert ImageChops.difference(original.crop((0, 0, 600, 300)), changed.crop((0, 0, 600, 300))).getbbox() is None
        assert ImageChops.difference(changed.crop((0, 350, 600, 500)), Image.new('RGB', (600, 150), 'white')).getbbox() is None
        assert records[1][1] == pdf[2].get_pixmap(matrix=fitz.Matrix(1.5, 1.5)).tobytes('png')


def test_shared_research_phrase_and_image_on_ack_page_are_never_erased(tmp_path):
    import fitz
    from preextract_page_cleanup import PageCleanup
    with fitz.open() as pdf:
        page = pdf.new_page()
        page.insert_text((30, 60), 'This repeated phrase is research data.')
        page.draw_circle((200, 200), 40)
        keep = [paragraph('This repeated phrase is research data.', 0)]
        removed = [dict(reason='bounded_nonresearch_section', block=paragraph('This repeated phrase is research data.', 1))]
        original = page.get_pixmap().samples
        assert PageCleanup(keep, removed)(page) is True
        assert original == page.get_pixmap().samples


@pytest.mark.parametrize('y',[20,55])
def test_unknown_vector_in_margin_or_under_removed_text_is_preserved(y):
    import fitz
    from preextract_page_cleanup import PageCleanup
    with fitz.open() as pdf:
        page=pdf.new_page(width=400,height=500)
        page.insert_text((30,60),'Acknowledgments')
        page.draw_line((20,y),(200,y),color=(0,0,1),width=2)
        removed=[dict(reason='bounded_nonresearch_section',block=paragraph('Acknowledgments',0))]
        cleaner=PageCleanup([],removed)
        before=page.get_pixmap().samples
        assert cleaner(page) is True
        after=page.get_pixmap().samples
        # Compare the complete width of the unique blue research line.
        width=400*3
        assert before[(y-2)*width:(y+3)*width]==after[(y-2)*width:(y+3)*width]


def test_only_proven_repeated_margin_graphics_can_leave_with_an_ack_page():
    import fitz
    from preextract_page_cleanup import PageCleanup
    with fitz.open() as pdf:
        for n in range(3):
            p=pdf.new_page(width=400,height=500)
            p.draw_line((20,20),(300,20),width=1)
            p.insert_text((30,60),'Acknowledgments' if n<2 else 'Appendix research')
            if n==1:p.draw_line((20,30),(100,30),color=(0,0,1),width=2)
        cleaner=PageCleanup([paragraph('Appendix research',1)],
                            [dict(reason='bounded_nonresearch_section',block=paragraph('Acknowledgments',0))])
        assert cleaner(pdf[0]) is False
        assert cleaner(pdf[1]) is True  # Unique graphic survives even in the margin.
        assert cleaner(pdf[2]) is True


def test_inspecting_a_transparent_margin_image_does_not_change_its_pixels():
    import fitz,io
    from PIL import Image
    from preextract_page_cleanup import PageCleanup
    picture=Image.new('RGBA',(20,20),(190,25,40,128))
    picture.putpixel((1,1),(255,20,20,255))
    stream=io.BytesIO();picture.save(stream,'PNG')
    with fitz.open() as pdf:
        p=pdf.new_page();p.insert_text((30,90),'Research evidence')
        p.insert_image(fitz.Rect(40,20,60,40),stream=stream.getvalue())
        original=pdf.tobytes()
    # Use separate documents: warming a render cache hides the library defect.
    with fitz.open(stream=original,filetype='pdf') as pdf:before=pdf[0].get_pixmap().samples
    with fitz.open(stream=original,filetype='pdf') as pdf:
        cleaner=PageCleanup([paragraph('Research evidence',0)],[])
        assert cleaner(pdf[0]) is True
        assert before==pdf[0].get_pixmap().samples


def test_pdf_material_path_uses_cleanup_without_renumbering_or_touching_original(tmp_path,monkeypatch):
    import fitz,evidence
    from preextract_material import prepare_material,file_sha
    source=tmp_path/'paper.pdf'
    with fitz.open() as pdf:
        for text in ('Research method and findings','Acknowledgments','Appendix research data'):
            pdf.new_page().insert_text((30,60),text)
        pdf.save(source)
    before=file_sha(source)
    parse=evidence.parse_pdf
    def with_unknown(*args):
        result=parse(*args)
        args[3]('formula_review','unknown visual location',{})
        return result
    monkeypatch.setattr(evidence,'parse_pdf',with_unknown)
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors":[]}',encoding='utf8')
    bundle=prepare_material(source,tmp_path/'work',catalog)
    assert [a['id'] for a in bundle['assets'] if a['kind']=='source_page']==['page_0001','page_0003']
    assert file_sha(source)==before
    assert bundle['cleanup']['page_evidence']['pages'][1]['action']=='omit_nonresearch_page'


@pytest.mark.parametrize('rotation',[90,180,270])
def test_rotated_mixed_page_masks_only_the_recorded_original_region(tmp_path,rotation):
    import fitz,io
    from PIL import Image,ImageChops,ImageDraw
    from preextract_page_cleanup import PageCleanup
    from auto_evidence import source_pages
    source=tmp_path/'rotated.pdf'
    with fitz.open() as pdf:
        p=pdf.new_page(width=300,height=400)
        p.insert_text((30,60),'Retain the research evidence.')
        p.insert_text((30,250),'Acknowledgments')
        p.set_rotation(rotation);pdf.save(source)
    cleaner=PageCleanup([paragraph('Retain the research evidence.',0)],
                        [dict(reason='bounded_nonresearch_section',block=paragraph('Acknowledgments',1))])
    pages=source_pages(source,tmp_path/'render',page_filter=cleaner,page_transform=cleaner.apply,return_records=True)
    assert len(pages)==1 and pages[0][0]==1
    with fitz.open(source) as pdf:
        before=Image.open(io.BytesIO(pdf[0].get_pixmap(matrix=fitz.Matrix(1.5,1.5)).tobytes('png')))
    after=Image.open(io.BytesIO(pages[0][1]))
    diff=ImageChops.difference(before,after)
    assert diff.getbbox() is not None
    # Every changed pixel must be inside the registered nonresearch rectangle.
    for x0,y0,x1,y1 in cleaner.records[0]['pixel_boxes']:
        assert ImageChops.difference(after.crop((x0,y0,x1,y1)),Image.new('RGB',(x1-x0,y1-y0),'white')).getbbox() is None
        ImageDraw.Draw(diff).rectangle((x0,y0,x1-1,y1-1),fill='black')
    assert diff.getbbox() is None


def test_registered_legacy_material_is_reused_without_migration(tmp_path):
    import json
    from preextract_material import prepare_material
    source=tmp_path/'paper.txt';source.write_text('摘要：完整研究材料',encoding='utf8')
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors":[]}',encoding='utf8')
    work=tmp_path/'bound'
    bundle=prepare_material(source,work,catalog,render_missing=False)
    bundle.pop('cleanup')
    manifest=work/'material.json';manifest.write_text(json.dumps(bundle),encoding='utf8')
    before=manifest.read_bytes()
    assert prepare_material(source,work,catalog,render_missing=False)==bundle
    assert manifest.read_bytes()==before


def test_cover_table_child_images_are_excluded_and_body_table_kept(tmp_path):
    from docx import Document
    from PIL import Image
    from preextract_material import prepare_material,file_sha
    source=tmp_path/'paper.docx';picture=tmp_path/'seal.png'
    Image.new('RGB',(20,20),'red').save(picture)
    doc=Document();doc.add_paragraph('本科毕业论文');doc.add_paragraph('研究方法与实验设计')
    form=doc.add_table(rows=3,cols=2)
    for row,values in zip(form.rows,[['学院','工程学院'],['姓名','某人'],['指导教师','某老师']]):
        for cell,value in zip(row.cells,values):cell.text=value
    form.cell(2,1).paragraphs[0].add_run().add_picture(str(picture))
    doc.add_paragraph('摘要：研究内容');doc.add_heading('1 实验',1)
    research=doc.add_table(rows=2,cols=2)
    for row,values in zip(research.rows,[['姓名','实验得分'],['样本甲','0.99']]):
        for cell,value in zip(row.cells,values):cell.text=value
    doc.save(source);before=file_sha(source)
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors":[]}',encoding='utf8')
    bundle=prepare_material(source,tmp_path/'work',catalog,render_missing=False)
    assert [a['kind'] for a in bundle['assets']]==['table']
    assert '0.99' in bundle['assets'][0]['text']
    assert before==file_sha(source)
