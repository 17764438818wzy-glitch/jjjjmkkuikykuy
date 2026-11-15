import hashlib
import json
from pathlib import Path

import pytest


def test_unsupported_object_does_not_screenshot_whole_paper(tmp_path, monkeypatch):
    from docx import Document
    from docx.oxml import OxmlElement
    import evidence_render
    from evidence import build_package
    doc=Document();doc.add_heading('摘要',1)
    doc.add_paragraph('正文不是研究图片。').add_run()._r.append(OxmlElement('w:object'))
    src=tmp_path/'object.docx';doc.save(src)
    calls=[]
    monkeypatch.setattr(evidence_render,'capture_docx_tables',lambda *a,**kw:calls.append(kw))
    meta=build_package(src,tmp_path/'bundle',{'id':'test','title':'对象测试'},render_screenshots=True)
    assert not calls, 'An unsupported object must not trigger whole-document rendering'
    assert any(i['code']=='embedded_object_review' for i in meta['issues'])
    assert not meta['evaluation_ready']


def test_vml_picture_is_extracted_without_unsupported_object_flag(tmp_path):
    from docx import Document
    from docx.oxml import parse_xml
    from PIL import Image
    from evidence import build_package,sha
    doc=Document();doc.add_heading('摘要',1)
    picture=tmp_path/'original.png';Image.new('RGB',(73,41),'blue').save(picture)
    rid,_=doc.part.get_or_add_image(str(picture))
    node=parse_xml(f'<w:pict xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main" xmlns:v="urn:schemas-microsoft-com:vml" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><v:shape><v:imagedata r:id="{rid}"/></v:shape></w:pict>')
    doc.add_paragraph('图1 原始研究图片').add_run()._r.append(node)
    src=tmp_path/'vml.docx';doc.save(src)
    pkg=tmp_path/'bundle';meta=build_package(src,pkg,{'id':'test','title':'VML图片'})
    assert len(meta['resources'])==1
    assert sha(pkg/meta['resources'][0]['local_path'])==sha(picture)
    assert not any(i['code']=='embedded_object_review' for i in meta['issues'])


def test_review_separates_figures_from_legacy_pages():
    from evidence_ui import resource_group
    assert resource_group({'kind':'image'})=='原始图片'
    assert resource_group({'kind':'source_page'})=='历史整页截图'
    assert resource_group({'kind':'table_source_page'})=='表格核对'
    assert resource_group({'kind':'table_screenshot'})=='表格核对'


def make_document(path):
    from docx import Document
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn
    from PIL import Image
    doc = Document()
    doc.add_paragraph('论文题目：企业经营数据研究')
    doc.add_paragraph('学生姓名：张小测')
    doc.add_heading('摘要', level=1)
    doc.add_paragraph('研究财务数据的原始记录，不改写。')
    doc.add_heading('第一章 经营分析', level=1)
    doc.add_paragraph('表3 经营情况，单位：万元')
    table = doc.add_table(rows=3, cols=3)
    for row, values in zip(table.rows, [['年份','营业收入','净利润'],['2022','100.00','(10)'],['2023','120','—']]):
        for cell, value in zip(row.cells, values): cell.text = value
    doc.add_paragraph('数据来源：公司公开年报。')
    doc.add_paragraph('表4 多级表头，单位：%')
    table = doc.add_table(rows=4, cols=3)
    table.cell(0,0).merge(table.cell(1,0)).text = '项目'
    table.cell(0,1).merge(table.cell(0,2)).text = '2023年'
    table.cell(1,1).text = '同比'
    table.cell(1,2).text = '占比'
    table.cell(2,0).text = '业务一'
    table.cell(2,1).text = '-2.50%'
    table.cell(2,2).text = ''
    table.cell(3,0).text = '业务二'
    table.cell(3,1).text = '0'
    table.cell(3,2).text = '12.0%'
    doc.add_paragraph('图1 收入趋势')
    image = path.with_suffix('.png')
    from PIL import ImageDraw
    figure=Image.new('RGB', (120,80), 'white')
    ImageDraw.Draw(figure).line([(10,65),(50,40),(100,15)],fill='blue',width=2)
    figure.save(image)
    doc.add_picture(str(image))
    p = doc.add_paragraph()
    math = OxmlElement('m:oMath')
    fraction = OxmlElement('m:f')
    for tag, text in [('m:num','100'),('m:den','120')]:
        part = OxmlElement(tag); run = OxmlElement('m:r'); token = OxmlElement('m:t'); token.text = text
        run.append(token); part.append(run); fraction.append(part)
    math.append(fraction); p._p.append(math)
    doc.add_heading('致谢', level=1)
    doc.add_paragraph('感谢张老师。')
    doc.add_heading('参考文献', level=1)
    doc.add_paragraph('[1] 会计研究。')
    doc.save(path)
    return path


def test_native_structure_and_two_exports(tmp_path):
    from evidence import build_package
    src = make_document(tmp_path / 'sample.docx')
    before = src.read_bytes()
    package = tmp_path / 'ACC_000001'
    meta = build_package(src, package, {'id':'UP_test','legacy_id':'ACC_000001','title':'企业经营数据研究','major':'会计学'})
    assert src.read_bytes() == before
    display = (package / 'document.md').read_text(encoding='utf-8')
    ingest = (package / 'ingest.md').read_text(encoding='utf-8')
    assert '张小测' not in display and '感谢张老师' not in display
    assert display.index('第一章') < display.index('表3') < display.index('图1')
    assert '| 2022 | 100.00 | (10) |' in display
    assert r'\frac{100}{120}' in display
    assert '年份=2022' in ingest and '营业收入=100.00' in ingest and '净利润=(10)' in ingest
    assert '同比=-2.50%' in ingest and '占比=[原文空值]' in ingest
    assert '数据来源：公司公开年报' in ingest and '万元' in ingest
    tables = [b for b in meta['blocks'] if b['type']=='table']
    assert len(tables)==2
    assert any(c['rowspan']==2 for c in tables[1]['cells'])
    assert any(c['colspan']==2 for c in tables[1]['cells'])
    figures = [b for b in meta['blocks'] if b['type']=='image']
    assert figures and figures[0]['privacy_status']=='pending'
    assert not list((package/'assets').glob('*'))
    assert meta['review_status']=='needs_review' and not meta['evaluation_ready']
    assert all((package / resource['local_path']).is_file() for resource in meta['resources'])
    assert 'file:' not in ingest and str(tmp_path) not in ingest
    assert '未核对' in ingest
    assert (package/'metadata.json').is_file()
    assert not tables[1]['notes'], 'Do not attribute the previous table source note to this table'


def test_final_ingest_carries_only_safe_preprocessing_handoff_metadata(tmp_path):
    from evidence import HANDOFF_BEGIN,HANDOFF_END,build_package,export_package
    src=tmp_path/'sample.txt';src.write_text('摘要\n企业经营数据研究。\n第一章\n本文分析企业财务绩效。',encoding='utf-8')
    package=tmp_path/'bundle'
    meta=build_package(src,package,{'id':'AUTO_0123456789abcdef','title':'企业经营数据研究','major':'会计学'})
    meta.update(preprocessing_complete=True,text_privacy_complete=True,privacy_status='ai_passed',
                classification={'major':'会计学','method':'original_rules','source':'front_matter',
                                'confidence':'high','title':'不需要交接的题名'},
                source_path=r'C:\private\student.docx')
    export_package(package,meta)
    ingest=(package/'ingest.md').read_text(encoding='utf-8')
    encoded=ingest.split(HANDOFF_BEGIN,1)[1].split(HANDOFF_END,1)[0].strip()
    handoff=json.loads(encoded)
    assert handoff=={'schema':'paper-handoff/1.0','paper_id':'AUTO_0123456789abcdef',
                     'document_id':'AUTO_0123456789abcdef',
                     'stable_paper_id':'paper_'+hashlib.sha256(src.read_bytes()).hexdigest()[:24],
                     'major':'会计学',
                     'classification':{'major':'会计学','method':'original_rules','source':'front_matter','confidence':'high'},
                     'preprocessing_complete':True,'privacy_status':'ai_passed'}
    assert 'C:\\private' not in ingest and '不需要交接的题名' not in encoded


def test_package_change_invalidates_review(tmp_path):
    from evidence import build_package, package_fingerprint
    src = make_document(tmp_path / 'sample.docx')
    p = tmp_path / 'bundle'
    build_package(src,p,{'id':'UP_test','title':'企业经营数据研究'})
    old = package_fingerprint(p)
    (p/'document.md').write_text('changed',encoding='utf-8')
    assert package_fingerprint(p)!=old


def test_manual_media_review_only_exports_verified_text(tmp_path):
    from evidence import build_package,approve_resource
    src=make_document(tmp_path/'sample.docx');p=tmp_path/'bundle'
    m=build_package(src,p,{'id':'UP_test','title':'企业经营数据研究'})
    rid=m['resources'][0]['id']
    m=approve_resource(p,rid)
    assert list((p/'assets').glob('*.png'))
    assert '尚未核对' in (p/'ingest.md').read_text(encoding='utf-8')
    m=approve_resource(p,rid,'图1，横轴：年份；2023年收入120万元。')
    assert '2023年收入120万元' in (p/'ingest.md').read_text(encoding='utf-8')
    assert not m['evaluation_ready'] and not m['completeness_verified']


def test_same_text_different_images_are_not_deduplicated(tmp_path,monkeypatch):
    import io,zipfile
    from PIL import Image
    import evidence
    from engine import Processor,Store,scan_sources
    source=tmp_path/'input';source.mkdir()
    a=make_document(source/'a.docx')
    b=source/'b.docx'
    buffer=io.BytesIO();Image.new('RGB',(120,80),'black').save(buffer,format='PNG')
    with zipfile.ZipFile(a) as old,zipfile.ZipFile(b,'w') as new:
        for info in old.infolist(): new.writestr(info,buffer.getvalue() if info.filename.startswith('word/media/') else old.read(info))
    original=evidence.build_package
    monkeypatch.setattr(evidence,'build_package',lambda src,pkg,identity,**kw:original(src,pkg,identity))
    result=Processor(Store(tmp_path/'out')).run(scan_sources(source))
    assert result['status']=='completed'
    assert len(result['items'])==2, result['events']


def test_package_failure_isolated_per_document(tmp_path,monkeypatch):
    import evidence
    from engine import Processor,Store,scan_sources
    from test_workbench import paper
    source=tmp_path/'input';source.mkdir()
    (source/'a.txt').write_text(paper(),encoding='utf-8')
    (source/'b.txt').write_text(paper('幼儿交流能力的调查研究')+'\n附录\n本次访谈共记录另外十组不同的观察结果。',encoding='utf-8')
    original=evidence.build_package
    def build(src,*args,**kwargs):
        if Path(src).name=='a.txt': raise RuntimeError('synthetic source failure')
        return original(src,*args,**kwargs)
    monkeypatch.setattr(evidence,'build_package',build)
    result=Processor(Store(tmp_path/'out')).run(scan_sources(source))
    assert result['status']=='completed' and len(result['items'])==1
    assert any('synthetic source failure' in e.get('reason','') for e in result['events'])


def test_pdf_does_not_guess_columns_from_spaces(tmp_path):
    import fitz
    from evidence import build_package
    pdf = fitz.open(); page = pdf.new_page()
    page.insert_text((40,40),'Report 2023')
    page.insert_text((40,70),'A       12     B      34')
    src=tmp_path/'sample.pdf';pdf.save(src);pdf.close()
    meta=build_package(src,tmp_path/'bundle',{'id':'UP_pdf','title':'Report 2023'})
    assert not [b for b in meta['blocks'] if b['type']=='table']
    assert meta['review_status']=='needs_review'
    assert any(i['code']=='pdf_layout_review' for i in meta['issues'])


def test_style_based_list_and_content_control_keep_order(tmp_path):
    from docx import Document
    from docx.oxml import OxmlElement
    from evidence import build_package
    doc=Document();doc.add_heading('摘要',1)
    doc.add_paragraph('第一项研究证据',style='List Number')
    doc.add_paragraph('第二项研究证据',style='List Number')
    p=doc.add_paragraph('内容控件中的研究结论')
    sdt=OxmlElement('w:sdt');content=OxmlElement('w:sdtContent');sdt.append(content)
    p._p.addprevious(sdt);content.append(p._p)
    source=tmp_path/'sample.docx';doc.save(source)
    build_package(source,tmp_path/'bundle',{'id':'UP_control','title':'研究结构核对'})
    text=(tmp_path/'bundle/document.md').read_text(encoding='utf-8')
    assert '1. 第一项研究证据' in text and '2. 第二项研究证据' in text
    assert text.index('第二项研究证据')<text.index('内容控件中的研究结论')


def test_engine_builds_one_package_and_uploads_only_ingest(tmp_path,monkeypatch):
    from engine import Processor,Store,scan_sources,approve_items
    from ragflow_client import upload_items
    from test_workbench import paper
    source=tmp_path/'input';source.mkdir()
    (source/'paper.txt').write_text(paper(),encoding='utf-8')
    store=Store(tmp_path/'out');manifest=Processor(store).run(scan_sources(source))
    item=manifest['items'][0]
    assert Path(item['path']).name=='ingest.md'
    assert Path(item['display_path']).name=='document.md'
    assert Path(item['package_path'],'metadata.json').is_file()
    approve_items(store,manifest['run_id'],[item['id']])
    class Client:
        base_url='http://localhost'
        def upload(self,dataset,path):
            assert path.name=='ingest.md'
            return 'one-doc'
    assert upload_items(store,manifest['run_id'],[item['id']],Client(),{'id':'d','name':'学前教育'})==1
    Path(item['display_path']).write_text('evidence changed',encoding='utf-8')
    with pytest.raises(ValueError,match='证据'):
        upload_items(store,manifest['run_id'],[item['id']],Client(),{'id':'d2','name':'学前教育'})
