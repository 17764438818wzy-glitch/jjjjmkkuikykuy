"""Table bar pieces need complete source evidence before individual removal."""
import copy
import io
import re
import zipfile
from pathlib import Path

import fitz
import pytest
from PIL import Image

from evidence import build_package
from evidence_tasks import resolve_scoped
from auto_evidence import Rejected


def fixture(tmp_path, *, single=False, mixed=False, separate=False, text_between=False, icons=False):
    def drawing(rid):
        return f'<w:r><w:drawing><a:blip r:embed="{rid}"/></w:drawing></w:r>'
    first=drawing('r1');second='' if single else drawing('r2')
    if text_between:first+='<w:r><w:t>独立研究标签</w:t></w:r>'
    if separate:content=f'<w:p>{first}</w:p><w:p>{second}</w:p>'
    else:content=f'<w:p>{first}{second}<w:r><w:t>12.5%</w:t></w:r></w:p>'
    xml=f'''<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"
      xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main"
      xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><w:body>
      <w:p><w:r><w:t>统计结果与研究数据</w:t></w:r></w:p><w:tbl><w:tblGrid><w:gridCol/><w:gridCol/></w:tblGrid>
      <w:tr><w:tc><w:p><w:r><w:t>选项</w:t></w:r></w:p></w:tc><w:tc><w:p><w:r><w:t>比例</w:t></w:r></w:p></w:tc></w:tr>
      <w:tr><w:tc><w:p><w:r><w:t>选项甲</w:t></w:r></w:p></w:tc><w:tc>{content}</w:tc></w:tr>
      </w:tbl></w:body></w:document>'''
    rels=''.join(f'<Relationship Id="r{n}" Target="media/image{n}.png"/>' for n in [1,2])
    path=tmp_path/'source.docx'
    with zipfile.ZipFile(path,'w') as z:
        z.writestr('word/document.xml',xml)
        z.writestr('word/_rels/document.xml.rels',f'<Relationships>{rels}</Relationships>')
        for n,size in [(1,(12,12) if icons else (18,12)),(2,(12,12) if icons else (124,12) if not mixed else (124,140))]:
            buffer=io.BytesIO();Image.new('RGB',size,'blue' if n==1 else 'lightgray').save(buffer,format='PNG')
            z.writestr(f'word/media/image{n}.png',buffer.getvalue())
    package=tmp_path/'package'
    return path,package,build_package(path,package,{'id':'fixture','title':'统计研究'},redact_content=False)


def test_adjacent_table_bar_members_enter_existing_complete_region_recovery(tmp_path):
    source,package,meta=fixture(tmp_path)
    table=next(b for b in meta['blocks'] if b['type']=='table')
    assert table.get('graphic_object') is True
    assert table['native_data_available'] is True
    issue=next(i for i in meta['issues'] if i['code']=='embedded_object_review')
    assert issue['position']==table['position']
    images=[b for b in meta['blocks'] if b['type']=='image']
    assert len(images)==2
    assert all(b['position']['object_ref']==table['position']['object_ref'] for b in images)
    assert table['cells'][3]['text'].endswith('12.5%')
    assert len(meta['resources'])==2  # No member is discarded by the parser.


@pytest.mark.parametrize('kwargs',[{'single':True},{'mixed':True},{'separate':True},{'text_between':True},{'icons':True}])
def test_independent_small_image_or_unrelated_picture_keeps_original_path(tmp_path,kwargs):
    _,_,meta=fixture(tmp_path,**kwargs)
    table=next(b for b in meta['blocks'] if b['type']=='table')
    assert not table.get('graphic_object')
    assert not any(i['code']=='embedded_object_review' for i in meta['issues'])
    assert len(meta['resources'])==(1 if kwargs.get('single') else 2)


@pytest.mark.parametrize('complete',[False,True])
def test_source_cells_and_members_survive_until_complete_image_exists(tmp_path,monkeypatch,complete):
    import evidence_tasks
    source,package,meta=fixture(tmp_path)
    table=next(b for b in meta['blocks'] if b['type']=='table')
    before=copy.deepcopy(table['cells']);source_bytes=source.read_bytes()
    grid=copy.deepcopy(table['grid']);native_xml=table['native_xml']
    pdf_path=tmp_path/'render.pdf'
    with fitz.open() as pdf:
        page=pdf.new_page();page.draw_rect(fitz.Rect(50,60,400,180),color=(0,0,1),fill=(0,0,1));pdf.save(pdf_path)
    monkeypatch.setattr(evidence_tasks,'_pdf',lambda *args:fitz.open(pdf_path))
    monkeypatch.setattr(evidence_tasks,'_locate',lambda *args:{0})
    monkeypatch.setattr('native_table_region.find_native_table_region',lambda *args:
        {'page':1,'bbox':[.05,.05,.9,.3],'provenance':{'method':'native_table_unique_pdf_grid'}})

    class Agent:
        def call(self,task,payload,images=()):
            assert task=='repair'
            assert len(payload['known_blocks'])==1
            known=payload['known_blocks'][0]
            assert known['id']==table['id'] and known['native_data_available']
            assert not known['requires_table_replacement']
            if not complete:return {'decision':'reject','reason':'fixture complete region unavailable'}
            return {'decision':'pass','reason':'complete table region','sensitive_boxes':[],
                'insertions':[{'kind':'image','after_id':table['id'],'page':1,'bbox':[.05,.05,.9,.3],
                   'text':'比例12.5%','description':'完整比例表及条形统计图','caption':''}],'nonresearch_objects':[]}
    resolve_scoped(meta,package,source,tmp_path/'work',80,Agent(),lambda *a:None)
    if complete:
        assert len(meta['resources'])==1
        assert not any(r['id'].startswith('image_') for r in meta['resources'])
        assert meta['resources'][0]['visual_integrity']['status']=='not_uniform_black_or_white'
    else:
        assert len(meta['resources'])==2
        assert meta['unrecovered_objects']
    after=next(b for b in meta['blocks'] if b['type']=='table')['cells']
    def plain(cells):return [re.sub(r'\[\[IMAGE:[^]]+\]\]','',c['text']) for c in cells]
    assert plain(after)==plain(before)
    assert table['grid']==grid and table['native_xml']==native_xml
    assert [(c['row'],c['column'],c['rowspan'],c['colspan']) for c in after]==[(c['row'],c['column'],c['rowspan'],c['colspan']) for c in before]
    assert source.read_bytes()==source_bytes


@pytest.mark.parametrize('case',['ignored','missing_second_page'])
def test_incomplete_or_ignored_table_cannot_supersede_members(tmp_path,monkeypatch,case):
    import evidence_tasks
    source,package,meta=fixture(tmp_path)
    table=next(b for b in meta['blocks'] if b['type']=='table')
    before=copy.deepcopy(table['cells'])
    original_ids={r['id'] for r in meta['resources']}
    pdf_path=tmp_path/'render.pdf'
    with fitz.open() as pdf:
        for _ in range(2):
            page=pdf.new_page();page.draw_rect(fitz.Rect(50,60,400,180),color=(0,0,1),fill=(0,0,1))
        pdf.save(pdf_path)
    monkeypatch.setattr(evidence_tasks,'_pdf',lambda *args:fitz.open(pdf_path))
    monkeypatch.setattr(evidence_tasks,'_locate',lambda *args:{0,1} if case=='missing_second_page' else {0})
    monkeypatch.setattr('native_table_region.find_native_table_region',lambda *args:
        {'page':1,'bbox':[.05,.05,.9,.3],'provenance':{'method':'native_table_unique_pdf_grid'}})

    class Agent:
        def call(self,task,payload,images=()):
            assert task=='repair'
            if case=='ignored':
                return {'decision':'pass','reason':'fixture decoration verdict','sensitive_boxes':[],
                    'insertions':[],'nonresearch_objects':[{'after_id':table['id'],'reason':'fixture decoration'}]}
            return {'decision':'pass','reason':'fixture only first page','sensitive_boxes':[],'page_masks':[{'page':1,'boxes':[]},{'page':2,'boxes':[]}],
                'insertions':[{'kind':'image','after_id':table['id'],'page':1,'bbox':[.05,.05,.9,.3],
                    'text':'12.5%','description':'仅表格第一页','caption':''}],'nonresearch_objects':[]}
    resolve_scoped(meta,package,source,tmp_path/'work',80,Agent(),lambda *a:None)
    assert original_ids<={r['id'] for r in meta['resources']}
    assert table['cells']==before
    assert any(u['id']==table['id'] for u in meta.get('unrecovered_objects',[]))


@pytest.mark.parametrize('child',['shape','chart','formula','formula_review'])
def test_fragment_table_keeps_other_embedded_objects_and_native_equations(tmp_path,child):
    source,_,_=fixture(tmp_path)
    with zipfile.ZipFile(source) as z:entries={n:z.read(n) for n in z.namelist()}
    if child=='shape':
        content='<w:r><w:drawing><w:txbxContent><w:p><w:r><w:t>Original research shape</w:t></w:r></w:p></w:txbxContent></w:drawing></w:r>'
    elif child=='chart':
        from test_graphics_recovery import chart_xml
        content='<w:r><w:drawing><c:chart xmlns:c="http://schemas.openxmlformats.org/drawingml/2006/chart" r:id="chart1"/></w:drawing></w:r>'
        entries['word/charts/chart1.xml']=chart_xml().encode()
        entries['word/_rels/document.xml.rels']=entries['word/_rels/document.xml.rels'].replace(b'</Relationships>',b'<Relationship Id="chart1" Target="charts/chart1.xml"/></Relationships>')
    else:
        content='<m:oMath xmlns:m="http://schemas.openxmlformats.org/officeDocument/2006/math"><m:r><m:t>x=12.5</m:t></m:r></m:oMath>'
        if child=='formula_review':content=content.replace('<m:r>','<m:box><m:e><m:r>').replace('</m:r>','</m:r></m:e></m:box>')
    row=f'<w:tr><w:tc><w:p><w:r><w:t>ResearchModel</w:t></w:r></w:p></w:tc><w:tc><w:p>{content}</w:p></w:tc></w:tr>'
    entries['word/document.xml']=entries['word/document.xml'].replace(b'</w:tbl>',row.encode()+b'</w:tbl>')
    with zipfile.ZipFile(source,'w') as z:
        for name,data in entries.items():z.writestr(name,data)
    meta=build_package(source,tmp_path/'coexisting-package',{'id':'fixture','title':'统计研究'},redact_content=False)
    table=next(b for b in meta['blocks'] if b.get('table_picture_fragments'))
    if child in {'formula','formula_review'}:
        assert len(meta['formulas'])==1 and meta['formulas'][0]['latex']=='x=12.5'
        assert 'x=12.5' in meta['formulas'][0]['omml']
        if child=='formula_review':assert any(i['code']=='formula_review' for i in meta['issues'])
    else:
        others=[b for b in meta['blocks'] if b.get('graphic_object') and b is not table]
        assert len(others)==1
        assert others[0]['position']['object_ref']!=table['position']['object_ref']
        assert any(i['position']==others[0]['position'] and i['code'] in {'embedded_object_review','native_chart_visual'} for i in meta['issues'])
        if child=='shape':
            assert 'Original research shape' in others[0]['locator_texts']
            assert any('Original research shape' in c['text'] for c in table['cells'])
        else:assert any(b.get('source')=='native_chart_cache' for b in meta['blocks'])


@pytest.mark.parametrize('proven',[False,True])
def test_table_fragment_crop_requires_unique_complete_table_geometry(tmp_path,monkeypatch,proven):
    import evidence_tasks
    source,package,meta=fixture(tmp_path)
    table=next(b for b in meta['blocks'] if b.get('table_picture_fragments'))
    member_ids={r['id'] for r in meta['resources']}
    pdf_path=tmp_path/'render.pdf'
    with fitz.open() as pdf:
        page=pdf.new_page();page.draw_rect(fitz.Rect(50,60,400,180),color=(0,0,1),fill=(0,0,1));pdf.save(pdf_path)
    monkeypatch.setattr(evidence_tasks,'_pdf',lambda *a:fitz.open(pdf_path))
    monkeypatch.setattr(evidence_tasks,'_locate',lambda *a:{0})
    region={'page':1,'bbox':[.05,.05,.9,.3],'provenance':{'method':'native_table_unique_pdf_grid'}} if proven else None
    monkeypatch.setattr('native_table_region.find_native_table_region',lambda *a:region)
    class Agent:
        def call(self,task,payload,images=()):
            return {'decision':'pass','reason':'fixture partial table crop','sensitive_boxes':[],
                'insertions':[{'kind':'image','after_id':table['id'],'page':1,'bbox':[.1,.1,.2,.15],
                    'text':'12.5%','description':'仅原表一角','caption':''}],'nonresearch_objects':[]}
    resolve_scoped(meta,package,source,tmp_path/'work',80,Agent(),lambda *a:None)
    if proven:
        assert len(meta['resources'])==1
        assert meta['resources'][0]['crop_provenance']['crop_bbox_normalized']==region['bbox']
        assert meta['resources'][0]['crop_provenance']['native_table_region']==region['provenance']
    else:
        assert member_ids<={r['id'] for r in meta['resources']}
        assert any(gap['id']==table['id'] for gap in meta['unrecovered_objects'])


@pytest.mark.parametrize('floating',['drawingml_anchor','vml_absolute'])
def test_table_with_picture_floating_outside_its_border_keeps_original_path(tmp_path,floating):
    source,_,_=fixture(tmp_path)
    with zipfile.ZipFile(source) as z:entries={n:z.read(n) for n in z.namelist()}
    if floating=='drawingml_anchor':
        drawing='''<w:drawing><wp:anchor xmlns:wp="http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing"
          xmlns:pic="http://schemas.openxmlformats.org/drawingml/2006/picture">
          <wp:positionH relativeFrom="page"><wp:posOffset>3810000</wp:posOffset></wp:positionH>
          <wp:positionV relativeFrom="page"><wp:posOffset>3810000</wp:posOffset></wp:positionV>
          <a:graphic><a:graphicData><pic:pic><pic:blipFill><a:blip r:embed="r1"/></pic:blipFill>
          <pic:spPr><a:prstGeom prst="rect"/></pic:spPr></pic:pic></a:graphicData></a:graphic>
          </wp:anchor></w:drawing>'''
    else:
        drawing='''<w:pict xmlns:v="urn:schemas-microsoft-com:vml"><v:shape type="#_x0000_t75"
          style="position: absolute; left:300pt; top:300pt; width:80pt; height:50pt">
          <v:imagedata r:id="r1"/></v:shape></w:pict>'''
    row=f'<w:tr><w:tc><w:p><w:r><w:t>Floating research picture</w:t></w:r></w:p></w:tc><w:tc><w:p><w:r>{drawing}</w:r></w:p></w:tc></w:tr>'
    entries['word/document.xml']=entries['word/document.xml'].replace(b'</w:tbl>',row.encode()+b'</w:tbl>')
    with zipfile.ZipFile(source,'w') as z:
        for name,data in entries.items():z.writestr(name,data)
    meta=build_package(source,tmp_path/'floating-package',{'id':'fixture','title':'统计研究'},redact_content=False)
    table=next(b for b in meta['blocks'] if b['type']=='table')
    assert not table.get('table_picture_fragments')
    assert len(meta['resources'])==3
    assert len([b for b in meta['blocks'] if b['type']=='image'])==3
    assert all(not r['position'].get('object_ref') for r in meta['resources'])
