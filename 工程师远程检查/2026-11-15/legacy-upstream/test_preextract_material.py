from pathlib import Path
import json
import pytest


def test_conservative_cleaning_keeps_abstract_and_research_appendix():
    from preextract_material import clean_blocks
    rows = [dict(type='paragraph', text=t, position={'line':i}, heading_level=1 if t in ('摘要','1 研究','致谢','附录A 问卷') else 0)
            for i,t in enumerate(['毕业论文','题目：供应链研究','姓名：某人','摘要','摘要研究材料','1 研究','正文材料','致谢','感谢老师','附录A 问卷','调查问题'])]
    kept, removed = clean_blocks(rows)
    text = '\n'.join(r['text'] for r in kept)
    assert '摘要研究材料' in text and '调查问题' in text and '感谢老师' not in text
    assert removed and '题目：供应链研究' in text and '姓名：某人' not in text


def test_uncertain_frontmatter_is_kept():
    from preextract_material import clean_blocks
    rows = [dict(type='paragraph',text=t,position={'line':i}) for i,t in enumerate(['本研究分析跨境供应链','摘要','更多研究'])]
    kept,_ = clean_blocks(rows)
    assert kept == rows


def test_pdf_repeated_margin_cleanup_does_not_remove_body():
    from preextract_material import remove_repeated_margins
    rows=[]
    for page in range(1,4):
        rows.extend([dict(type='paragraph',text='重复页眉',position={'page':page,'bbox':[20,5,100,25]}),
                     dict(type='paragraph',text='重复正文仍保留',position={'page':page,'bbox':[20,200,100,220]})])
    kept,removed=remove_repeated_margins(rows,{1:800,2:800,3:800})
    assert len(removed)==3 and all(r['text']=='重复正文仍保留' for r in kept)


def test_material_native_table_and_image_are_bound_and_original_unchanged(tmp_path):
    from docx import Document
    from PIL import Image
    from preextract_material import prepare_material, file_sha
    picture=tmp_path/'chart.png'; Image.new('RGB',(30,20),'blue').save(picture)
    source=tmp_path/'source.docx';doc=Document();doc.add_heading('摘要',1);doc.add_paragraph('完整摘要')
    doc.add_heading('1 研究',1);doc.add_paragraph('正文材料')
    table=doc.add_table(rows=2,cols=2)
    for row,values in zip(table.rows,[['指标','数值'],['测试','123']]):
        for cell,value in zip(row.cells,values):cell.text=value
    doc.add_picture(str(picture));doc.add_heading('附录A',1);doc.add_paragraph('附录材料');doc.save(source)
    before=file_sha(source);catalog=tmp_path/'catalog.json';catalog.write_text('{"majors":[]}',encoding='utf-8')
    bundle=prepare_material(source,tmp_path/'work',catalog,render_missing=False)
    assert file_sha(source)==before==bundle['source']['source_sha256']
    assert '完整摘要' in ''.join(b['text'] for b in bundle['blocks'])
    assert any('123' in b['text'] for b in bundle['blocks'])
    assert len({b['id'] for b in bundle['blocks']})==len(bundle['blocks'])
    assert any(a.get('image_path') and Path(a['image_path']).is_file() for a in bundle['assets'])
    assert any(a['kind']=='table' for a in bundle['assets'])
    again=prepare_material(source,tmp_path/'work',catalog,render_missing=False)
    assert again['source']['index_sha256']==bundle['source']['index_sha256']


def _pdf(tmp_path):
    import fitz
    source=tmp_path/'three-pages.pdf'
    with fitz.open() as doc:
        for number in range(1,4):
            page=doc.new_page(width=300,height=400)
            page.insert_text((30,50),f'Complete research page {number}')
            if number==2:page.draw_circle((150,180),55,color=(1,0,0),width=3)
        doc.save(source)
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors":[]}',encoding='utf8')
    return source,catalog


def test_native_bitmap_is_normalized_with_original_and_rendition_hashes(tmp_path):
    from pathlib import Path
    from preextract_material import prepare_material
    from PIL import Image
    from docx import Document
    from preextract_request import build_request
    source=tmp_path/'paper.docx';bitmap=tmp_path/'chart.bmp'
    Image.new('RGB',(300,200),'white').save(bitmap)
    document=Document();document.add_paragraph('研究过程');document.add_picture(str(bitmap));document.save(source)
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors":[]}',encoding='utf8')
    bundle=prepare_material(source,tmp_path/'work',catalog)
    asset=next(a for a in bundle['assets'] if a.get('original_image_path'))
    assert Path(asset['original_image_path']).read_bytes()==bitmap.read_bytes()
    assert asset['image_path'].endswith('.png') and asset['original_sha256']!=asset['sha256']
    _,capacity=build_request(bundle,'template',{},dict(model='test',context_window=1000000))
    assert capacity['image_count']==1


def test_native_word_chart_keeps_data_and_gets_page_pixels(tmp_path,monkeypatch):
    import io
    from PIL import Image
    from test_graphics_recovery import chart_doc
    from preextract_material import prepare_material
    from preextract_request import build_request
    source=chart_doc(tmp_path);catalog=tmp_path/'catalog.json'
    catalog.write_text('{"majors":[]}',encoding='utf8')
    pixels=io.BytesIO();Image.new('RGB',(100,100),'white').save(pixels,'PNG')
    calls=[]
    def render(*args,**kwargs):calls.append(args);return [pixels.getvalue()]
    monkeypatch.setattr('auto_evidence.source_pages',render)
    bundle=prepare_material(source,tmp_path/'work',catalog)
    assert len(calls)==1 and any(a['kind']=='table' for a in bundle['assets'])
    issue=next(i for i in bundle['issues'] if i['code']=='native_chart_visual')
    assert issue['page_evidence_ids']==['page_0001']
    _,capacity=build_request(bundle,'template',{},dict(model='test',context_window=1000000))
    assert capacity['image_count']==1


def test_pdf_renders_only_required_original_page_and_retains_generic_limits(tmp_path):
    import fitz
    from preextract_material import prepare_material,file_sha
    source,catalog=_pdf(tmp_path);before=file_sha(source)
    bundle=prepare_material(source,tmp_path/'targeted',catalog,max_pages=1)
    pages=[a for a in bundle['assets'] if a['kind']=='source_page']
    assert [a['id'] for a in pages]==['page_0002']
    assert pages[0]['position']=={'page':2}
    with fitz.open(source) as doc:
        assert Path(pages[0]['image_path']).read_bytes()==doc[1].get_pixmap(matrix=fitz.Matrix(1.5,1.5)).tobytes('png')
    assert all(f'Complete research page {n}' in '\n'.join(b['text'] for b in bundle['blocks']) for n in range(1,4))
    layout=[i for i in bundle['issues'] if i['code']=='pdf_layout_review']
    assert len(layout)==3
    assert layout[0]['page_evidence_ids']==[] and layout[2]['page_evidence_ids']==[]
    assert all(i['page_evidence_ids']==['page_0002'] for i in bundle['issues'] if i['code']=='pdf_vector_review')
    assert before==file_sha(source)==bundle['source']['source_sha256']


@pytest.mark.parametrize('position',[{}, {'page':99}])
def test_pdf_unknown_required_page_keeps_explicit_full_fallback(tmp_path,monkeypatch,position):
    import evidence
    from preextract_material import prepare_material
    source,catalog=_pdf(tmp_path)
    original=evidence.parse_pdf
    def parse_with_unknown(source,redact,resource,issue):
        parsed=original(source,redact,resource,issue)
        issue('formula_review','公式位置未取得',position)
        return parsed
    monkeypatch.setattr(evidence,'parse_pdf',parse_with_unknown)
    bundle=prepare_material(source,tmp_path/'fallback',catalog)
    assert [a['id'] for a in bundle['assets'] if a['kind']=='source_page']==['page_0001','page_0002','page_0003']
    unknown=next(i for i in bundle['issues'] if i['code']=='formula_review')
    assert unknown['page_evidence_ids']==['page_0001','page_0002','page_0003']
    assert unknown['page_evidence_status']=='provided_full_document_fallback'


def test_pdf_old_material_cache_is_not_reused(tmp_path):
    from preextract_material import prepare_material,PARSER_VERSION
    source,catalog=_pdf(tmp_path)
    bundle=prepare_material(source,tmp_path/'cache',catalog,render_missing=False)
    bundle['parser_version']='1.3'
    (tmp_path/'cache'/'material.json').write_text(json.dumps(bundle),encoding='utf8')
    assert PARSER_VERSION!='1.3'
    with pytest.raises(ValueError,match='已有材料版本'):
        prepare_material(source,tmp_path/'cache',catalog,render_missing=False)
