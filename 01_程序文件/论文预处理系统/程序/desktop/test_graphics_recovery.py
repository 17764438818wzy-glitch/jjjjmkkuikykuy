import io
import zipfile
from pathlib import Path

import pytest
from PIL import Image

from native_docx import parse_docx
from evidence import build_package
from auto_evidence import Rejected


NS='''xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"
xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"
xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main"
xmlns:c="http://schemas.openxmlformats.org/drawingml/2006/chart"
xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006"
xmlns:wps="http://schemas.microsoft.com/office/word/2010/wordprocessingShape"
xmlns:v="urn:schemas-microsoft-com:vml"'''


def docx(path,body,rels='',extra=None):
    with zipfile.ZipFile(path,'w') as z:
        z.writestr('word/document.xml',f'<w:document {NS}><w:body><w:p><w:r><w:t>摘要</w:t></w:r></w:p>{body}</w:body></w:document>')
        z.writestr('word/_rels/document.xml.rels',f'<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">{rels}</Relationships>')
        for name,value in (extra or {}).items():z.writestr(name,value)
    return path


def parse(source):
    issues=[]
    blocks,images,equations=parse_docx(source,lambda s:s,lambda rid,*args:{'id':rid},
        lambda code,message,position:issues.append({'code':code,'position':position,'message':message}))
    return blocks,images,issues


def test_compatibility_graphic_is_read_once_and_empty_fallback_is_not_missing_image(tmp_path):
    src=docx(tmp_path/'source.docx','''<w:p><w:r><mc:AlternateContent><mc:Choice Requires="wps">
      <w:drawing><wps:wsp><wps:txbx><w:txbxContent><w:p><w:r><w:t>供应商关系</w:t></w:r></w:p></w:txbxContent></wps:txbx></wps:wsp></w:drawing>
      </mc:Choice><mc:Fallback><w:pict><v:rect><v:imagedata/><v:textbox><w:txbxContent>
      <w:p><w:r><w:t>供应商关系</w:t></w:r></w:p></w:txbxContent></v:textbox></v:rect></w:pict></mc:Fallback>
      </mc:AlternateContent></w:r></w:p>''')
    before=src.read_bytes();blocks,_,issues=parse(src)
    assert ''.join(b.get('text','') for b in blocks).count('供应商关系')==1
    assert not any(i['code']=='image_missing' for i in issues)
    assert sum(i['code']=='embedded_object_review' for i in issues)==1
    assert src.read_bytes()==before


def test_unknown_compatibility_choice_uses_supported_fallback(tmp_path):
    src=docx(tmp_path/'source.docx','''<w:p><w:r><mc:AlternateContent><mc:Choice Requires="future">
      <w:t>不支持的表示</w:t></mc:Choice><mc:Fallback><w:t>兼容表示原文</w:t></mc:Fallback>
      </mc:AlternateContent></w:r></w:p>''')
    blocks,_,_=parse(src)
    text=''.join(b.get('text','') for b in blocks)
    assert '兼容表示原文' in text and '不支持的表示' not in text


def test_real_missing_image_relationship_is_still_reported(tmp_path):
    src=docx(tmp_path/'source.docx','<w:p><w:r><w:drawing><a:blip r:embed="rMissing"/></w:drawing></w:r></w:p>')
    assert any(i['code']=='image_missing' for i in parse(src)[2])


def chart_xml(count=3,points=None):
    points=points or [(0,'-1.20'),(1,''),(2,'0.32')]
    values=''.join(f'<c:pt idx="{i}"><c:v>{v}</c:v></c:pt>' for i,v in points)
    return f'''<c:chartSpace xmlns:c="http://schemas.openxmlformats.org/drawingml/2006/chart">
      <c:chart><c:plotArea><c:lineChart><c:ser><c:tx><c:v>净利润率</c:v></c:tx>
      <c:cat><c:strLit><c:ptCount val="3"/><c:pt idx="0"><c:v>2021</c:v></c:pt>
      <c:pt idx="1"><c:v>2022</c:v></c:pt><c:pt idx="2"><c:v>2023</c:v></c:pt></c:strLit></c:cat>
      <c:val><c:numLit><c:formatCode>0.00%</c:formatCode><c:ptCount val="{count}"/>{values}</c:numLit></c:val>
      </c:ser></c:lineChart></c:plotArea></c:chart></c:chartSpace>'''


def chart_doc(tmp_path,chart=None):
    return docx(tmp_path/'chart.docx','''<w:p><w:r><w:t>前文保持原样。</w:t></w:r></w:p>
      <w:p><w:r><w:drawing><a:graphic><a:graphicData><c:chart r:id="chart1"/></a:graphicData></a:graphic></w:drawing></w:r></w:p>
      <w:p><w:r><w:t>图1 净利润率变化</w:t></w:r></w:p>''',
      '<Relationship Id="chart1" Target="charts/chart1.xml" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/chart"/>',
      {'word/charts/chart1.xml':chart or chart_xml()})


def test_native_chart_preserves_categories_numeric_lexemes_blanks_and_percent_format(tmp_path):
    source=chart_doc(tmp_path);meta=build_package(source,tmp_path/'package',{'id':'chart','title':'财务趋势研究'},redact_content=False)
    tables=[b for b in meta['blocks'] if b.get('source')=='native_chart_cache']
    assert len(tables)==1
    table=tables[0];texts=[c['text'] for c in table['cells']]
    assert '-1.20' in texts and '' in texts and '0.32' in texts and '0.00%' in texts
    assert all(year in texts for year in ['2021','2022','2023'])
    assert any(i['code']=='native_chart_visual' for i in meta['issues']), 'The original visual evidence must still be retained'
    ingest=(tmp_path/'package/ingest.md').read_text(encoding='utf-8')
    assert '-1.20' in ingest and '0.32' in ingest


def test_incomplete_chart_cache_never_becomes_a_complete_native_table(tmp_path):
    source=chart_doc(tmp_path,chart_xml(points=[(0,'100'),(2,'120')]))
    blocks,_,issues=parse(source)
    assert not any(b.get('source')=='native_chart_cache' for b in blocks)
    assert any(i['code']=='embedded_object_review' for i in issues)


def test_figure_caption_beyond_blank_paragraphs_can_locate_single_page():
    from evidence_tasks import _locate
    blocks=[{'id':'obj','type':'paragraph','text':'','position':{'body_index':1,'object_ref':'object_1'}}]
    blocks += [{'id':f'b{i}','type':'paragraph','text':'','position':{'body_index':i+2}} for i in range(30)]
    blocks += [{'id':'caption','type':'paragraph','text':'图1.1 本文研究框架','position':{'body_index':40}}]
    issue={'position':blocks[0]['position'],'code':'embedded_object_review'}
    assert _locate(issue,blocks,['研究背景与方法','图1.1本文研究框架','下一章研究'])=={1}


def test_locating_short_labels_requires_multiple_agreeing_distinct_labels():
    from evidence_tasks import _locate
    pos={'body_index':1,'object_ref':'object_1'}
    block={'id':'obj','type':'paragraph','text':'供应商甲\n供应商乙\n供应商丙','locator_texts':['供应商甲','供应商乙','供应商丙'],'position':pos}
    assert _locate({'position':pos},[block],['正文提及供应商甲','图中供应商甲供应商乙供应商丙'])=={1}


def test_candidate_pages_are_read_together_once_and_crop_uses_returned_page(tmp_path,monkeypatch):
    import fitz,evidence_tasks
    source=tmp_path/'source.pdf'
    with fitz.open() as pdf:
        for color in [(1,0,0),(0,0,1)]:
            page=pdf.new_page(width=100,height=100);page.draw_rect(page.rect,color=color,fill=color)
        pdf.save(source)
    package=tmp_path/'package';(package/'assets').mkdir(parents=True)
    pos={'body_index':1,'object_ref':'object_1'}
    meta={'blocks':[{'id':'obj','type':'paragraph','text':'[解析说明：图形需识别]','position':pos}],
          'issues':[{'code':'embedded_object_review','position':pos,'status':'open'}],'resources':[]}
    monkeypatch.setattr(evidence_tasks,'_locate',lambda *args:{0,1})
    class Agent:
        calls=[]
        def call(self,task,payload,images=()):
            self.calls.append((payload,len(images)))
            assert payload.get('pages')==[1,2] and len(images)==2
            return {'decision':'pass','reason':'研究图位于第二页','sensitive_boxes':[],
                'page_masks':[{'page':1,'boxes':[]},{'page':2,'boxes':[]}],
                'insertions':[{'after_id':'obj','page':2,'kind':'image','bbox':[.1,.1,.5,.5],
                  'text':'原图未标数值','description':'蓝色区域','caption':'图1'}]}
    agent=Agent();evidence_tasks.resolve_scoped(meta,package,source,tmp_path/'visual',80,agent,lambda *a:None)
    assert len(agent.calls)==1
    assert meta['resources'][0]['position']['page']==2
    with Image.open(package/meta['resources'][0]['local_path']) as image:assert image.getpixel((5,5))==(0,0,255)


def test_same_page_graphic_crop_is_kept_once_after_focused_recovery(tmp_path):
    import fitz, evidence_tasks
    source=tmp_path/'source.pdf'
    with fitz.open() as pdf:
        page=pdf.new_page(width=100,height=100)
        page.draw_rect(page.rect,color=(0,0,1),fill=(0,0,1))
        pdf.save(source)
    package=tmp_path/'package';(package/'assets').mkdir(parents=True)
    positions=[{'page':1,'object_ref':'g1'},{'page':1,'object_ref':'g2'}]
    meta={'blocks':[{'id':'g1','type':'paragraph','text':'流程图左框','graphic_object':True,'position':positions[0]},
                    {'id':'g2','type':'paragraph','text':'流程图右框','graphic_object':True,'position':positions[1]}],
          'issues':[{'code':'embedded_object_review','position':p,'status':'open'} for p in positions],
          'resources':[]}
    class Agent:
        def __init__(self): self.calls=[]
        def call(self,task,payload,images=()):
            self.calls.append(payload)
            if len(self.calls)==1:
                # The first pass finds one connector-sized crop and misses g2.
                return {'decision':'pass','reason':'初步定位','sensitive_boxes':[],
                        'insertions':[{'after_id':'g1','page':1,'kind':'image','bbox':[.1,.1,.2,.2],
                                       'text':'连接箭头','description':'流程图局部箭头'}]}
            # Focused recovery returns the same complete flowchart region for both
            # source objects; the consumer must publish it only once.
            return {'decision':'pass','reason':'完整图形区域','sensitive_boxes':[],
                    'insertions':[{'after_id':'g1','page':1,'kind':'image','bbox':[.1,.1,.9,.9],
                                   'text':'流程图','description':'完整流程图'},
                                  {'after_id':'g2','page':1,'kind':'image','bbox':[.1,.1,.9,.9],
                                   'text':'流程图','description':'完整流程图'}]}
    agent=Agent()
    evidence_tasks.resolve_scoped(meta,package,source,tmp_path/'visual',80,agent,lambda *a:None)
    assert len(agent.calls)==2
    assert len(meta['resources'])==1
    assert len([b for b in meta['blocks'] if b.get('resource_id')])==1
    assert not meta.get('unrecovered_objects')
    assert all(issue['status']=='resolved' for issue in meta['issues'])


def test_single_page_reply_cannot_reference_a_different_page(tmp_path):
    import fitz,evidence_tasks
    source=tmp_path/'source.pdf'
    with fitz.open() as pdf:pdf.new_page();pdf.save(source)
    pos={'page':1}
    meta={'blocks':[{'id':'obj','type':'paragraph','text':'','position':pos}],
        'issues':[{'code':'embedded_object_review','position':pos,'status':'open'}],'resources':[]}
    class Agent:
        def call(self,*args,**kwargs):return {'decision':'pass','reason':'错误页码','sensitive_boxes':[],
            'insertions':[{'after_id':'obj','page':2,'kind':'text','text':'错误页内容'}]}
    with pytest.raises(Rejected,match='页'):
        evidence_tasks.resolve_scoped(meta,tmp_path,source,tmp_path/'visual',80,Agent(),lambda *a:None)
    assert meta['issues'][0]['status']=='open'


def test_one_floating_diagram_keeps_one_target_across_empty_paragraphs(tmp_path):
    shape=lambda text:f'<w:p><w:r><w:pict><v:rect><v:textbox><w:txbxContent><w:p><w:r><w:t>{text}</w:t></w:r></w:p></w:txbxContent></v:textbox></v:rect></w:pict></w:r></w:p>'
    source=docx(tmp_path/'source.docx',shape('研究背景')+'<w:p/>'*20+shape('研究结论')+
        '<w:p><w:r><w:t>图1 研究框架</w:t></w:r></w:p>'+shape('另一张图'))
    blocks,_,issues=parse(source)
    objects=[b for b in blocks if b.get('graphic_object')]
    assert len(objects)==2 and len([i for i in issues if i['code']=='embedded_object_review'])==2
    assert '研究背景' in objects[0]['text'] and '研究结论' in objects[0]['text']
    assert len(objects[0]['member_positions'])==2


def test_previous_paragraph_end_locates_graph_after_long_cross_page_paragraph():
    from evidence_tasks import _locate
    first='前文开始的内容足够长用于唯一定位。';last='前文结束的位置才是图形的前方。'
    following='图形后方的正文也足够用于唯一定位。'
    blocks=[{'id':'before','type':'paragraph','text':first+'中间材料'*40+last,'position':{'body_index':1}},
        {'id':'obj','type':'paragraph','text':'','position':{'body_index':2}},
        {'id':'after','type':'paragraph','text':following,'position':{'body_index':3}}]
    pages=[first+'中间材料'*30,'中间材料'*30+last,following]
    assert _locate({'position':{'body_index':2}},blocks,pages)=={1,2}


def test_formula_can_be_located_by_unchanged_prose_after_math_markup():
    from evidence_tasks import _locate
    text='其中${C}_{i}∈(0,1)$，最终确定指标综合权重和环境层面的综合得分情况。'
    block={'id':'formula','type':'paragraph','text':text,'position':{'body_index':1}}
    assert _locate({'code':'formula_review','position':block['position']},[block],
        ['其他页','其中Ci∈(0,1)，最终确定指标综合权重和环境层面的综合得分情况。'])=={1}


def test_floating_group_preserves_member_picture_reference(tmp_path):
    shape=lambda text,image='':f'<w:p><w:r><w:pict><v:rect>{image}<v:textbox><w:txbxContent><w:p><w:r><w:t>{text}</w:t></w:r></w:p></w:txbxContent></v:textbox></v:rect></w:pict></w:r></w:p>'
    buffer=io.BytesIO();Image.new('RGB',(20,20),'blue').save(buffer,format='PNG')
    src=docx(tmp_path/'source.docx',shape('研究前提')+shape('研究结果','<v:imagedata r:id="img"/>'),
        '<Relationship Id="img" Target="media/image.png"/>',{'word/media/image.png':buffer.getvalue()})
    meta=build_package(src,tmp_path/'package',{'id':'group','title':'图形研究'},redact_content=False)
    assert len(meta['resources'])==1
    assert (tmp_path/'package'/meta['resources'][0]['local_path']).is_file()
    assert 'image_0001' in (tmp_path/'package/document.md').read_text(encoding='utf-8')


def test_explicit_page_break_before_separates_floating_diagrams(tmp_path):
    shape=lambda t:f'<w:p><w:r><w:pict><v:rect><v:textbox><w:txbxContent><w:p><w:r><w:t>{t}</w:t></w:r></w:p></w:txbxContent></v:textbox></v:rect></w:pict></w:r></w:p>'
    src=docx(tmp_path/'source.docx',shape('第一页')+'<w:p><w:pPr><w:pageBreakBefore/></w:pPr></w:p>'+shape('第二页'))
    assert len([b for b in parse(src)[0] if b.get('graphic_object')])==2


def test_error_bar_data_is_not_silently_dropped_by_native_chart_fast_path(tmp_path):
    chart=chart_xml().replace('</c:ser>','<c:errBars><c:plus><c:numLit><c:ptCount val="1"/><c:pt idx="0"><c:v>9.99</c:v></c:pt></c:numLit></c:plus></c:errBars></c:ser>')
    blocks,_,issues=parse(chart_doc(tmp_path,chart))
    assert not any(b.get('source')=='native_chart_cache' for b in blocks)
    assert any(i['code']=='embedded_object_review' for i in issues)


def test_numeric_category_format_is_preserved(tmp_path):
    chart=chart_xml().replace('<c:strLit>','<c:numLit><c:formatCode>yyyy</c:formatCode>').replace('</c:strLit>','</c:numLit>')
    meta=build_package(chart_doc(tmp_path,chart),tmp_path/'package',{'id':'dates','title':'年度研究'},redact_content=False)
    table=next(b for b in meta['blocks'] if b.get('native_chart'))
    assert 'yyyy' in [c['text'] for c in table['cells']]
    assert table['native_chart']['series'][0]['category_format']=='yyyy'


def test_nonresearch_reason_and_locator_texts_share_single_privacy_pass():
    from auto_evidence import text_privacy
    meta={'title':'研究资料','blocks':[{'type':'paragraph','text':'研究正文',
        'nonresearch_reason':'落款为张小测的签名','locator_texts':['张小测制作']}],'resources':[],'formulas':[]}
    class Agent:
        def call(self,task,payload,images=()):
            return {'decision':'pass','reason':'去除身份信息','sensitive_strings':['张小测'] if '张小测' in payload['text'] else []}
    text_privacy(meta,Agent(),lambda *a:None)
    assert '张小测' not in str(meta)


def test_object_cannot_use_another_objects_candidate_page(tmp_path,monkeypatch):
    import fitz,evidence_tasks
    src=tmp_path/'source.pdf'
    with fitz.open() as pdf:pdf.new_page();pdf.new_page();pdf.save(src)
    positions=[{'body_index':1,'object_ref':'a'},{'body_index':2,'object_ref':'b'}]
    meta={'blocks':[{'id':p['object_ref'],'type':'paragraph','text':'','position':p} for p in positions],
        'issues':[{'code':'embedded_object_review','position':p,'status':'open'} for p in positions],'resources':[]}
    monkeypatch.setattr(evidence_tasks,'_locate',lambda i,*a:{0} if i['position']['object_ref']=='a' else {0,1})
    class Agent:
        def call(self,*a,**kw):return {'decision':'pass','reason':'内容恢复','sensitive_boxes':[],
            'page_masks':[{'page':1,'boxes':[]},{'page':2,'boxes':[]}],
            'insertions':[{'after_id':'a','page':2,'kind':'text','text':'错误对应内容'}]}
    with pytest.raises(Rejected,match='页'):
        evidence_tasks.resolve_scoped(meta,tmp_path,src,tmp_path/'visual',80,Agent(),lambda *a:None)


def test_empty_recovery_is_published_with_an_unrecovered_gap(tmp_path):
    import fitz,evidence_tasks
    src=tmp_path/'source.pdf'
    with fitz.open() as pdf:pdf.new_page();pdf.save(src)
    meta={'blocks':[{'id':'g','type':'paragraph','text':'原图待识别','graphic_object':True,'position':{'page':1}}],
        'issues':[{'code':'embedded_object_review','position':{'page':1},'status':'open'}],'resources':[]}
    class Agent:
        calls=0
        def call(self,*a,**kw):
            self.calls+=1
            return {'decision':'pass','reason':'处理完成','sensitive_boxes':[],'insertions':[]}
    agent=Agent();evidence_tasks.resolve_scoped(meta,tmp_path,src,tmp_path/'visual',80,agent,lambda *a:None)
    assert agent.calls==2
    assert meta['issues'][0]['status']=='recorded'
    assert meta['unrecovered_objects'][0]['id']=='g'
    assert meta['completeness_status']=='partial'


def test_missing_graphic_gets_one_focused_recovery_before_recording_gap(tmp_path):
    import fitz,evidence_tasks
    src=tmp_path/'source.pdf'
    with fitz.open() as pdf:
        page=pdf.new_page(width=100,height=100);page.draw_line((20,70),(70,20));pdf.save(src)
    package=tmp_path/'package';(package/'assets').mkdir(parents=True)
    pos={'page':1}
    meta={'blocks':[{'id':'g','type':'paragraph','text':'研究流程图','graphic_object':True,'position':pos}],
          'issues':[{'code':'embedded_object_review','position':pos,'status':'open'}],'resources':[]}
    class Agent:
        calls=[]
        def call(self,task,payload,images=()):
            self.calls.append(payload)
            if len(self.calls)==1:return {'decision':'pass','reason':'遗漏','sensitive_boxes':[],'insertions':[]}
            assert [b['id'] for b in payload['known_blocks']]==['g']
            return {'decision':'pass','reason':'补回原图','sensitive_boxes':[],
                    'insertions':[{'after_id':'g','page':1,'kind':'image','bbox':[.1,.1,.9,.9],
                                   'text':'研究流程','description':'原文流程图'}]}
    agent=Agent();evidence_tasks.resolve_scoped(meta,package,src,tmp_path/'visual',80,agent,lambda *a:None)
    assert len(agent.calls)==2
    assert not meta.get('unrecovered_objects')
    assert any(b.get('resource_id') for b in meta['blocks'])


def test_graphic_business_rejection_becomes_an_explicit_gap(tmp_path):
    import fitz,evidence_tasks
    src=tmp_path/'source.pdf'
    with fitz.open() as pdf:pdf.new_page();pdf.save(src)
    pos={'page':1}
    meta={'blocks':[{'id':'g','type':'paragraph','text':'研究图','graphic_object':True,'position':pos}],
          'issues':[{'code':'embedded_object_review','position':pos,'status':'open'}],'resources':[]}
    class Agent:
        def call(self,*args,**kwargs):return {'decision':'reject','reason':'候选页没有足够像素证明原图'}
    evidence_tasks.resolve_scoped(meta,tmp_path,src,tmp_path/'visual',80,Agent(),lambda *a:None)
    gap=meta['unrecovered_objects'][0]
    assert gap['id']=='g' and '没有足够像素' in gap['reason']
    assert meta['issues'][0]['status']=='recorded'


def test_non_graphic_business_rejection_still_rejects_the_paper(tmp_path):
    import fitz,evidence_tasks
    src=tmp_path/'source.pdf'
    with fitz.open() as pdf:pdf.new_page();pdf.save(src)
    pos={'page':1}
    meta={'blocks':[{'id':'formula','type':'paragraph','text':'公式待核对','position':pos}],
          'issues':[{'code':'formula_review','position':pos,'status':'open'}],'resources':[]}
    class Agent:
        def call(self,*args,**kwargs):return {'decision':'reject','reason':'公式无法辨认'}
    with pytest.raises(Rejected,match='公式无法辨认'):
        evidence_tasks.resolve_scoped(meta,tmp_path,src,tmp_path/'visual',80,Agent(),lambda *a:None)


def test_partial_bitmap_does_not_resolve_composite_graphic(tmp_path,monkeypatch):
    import evidence_tasks
    pos={'body_index':1,'object_ref':'group'}
    meta={'blocks':[{'id':'g','type':'paragraph','text':'流程图文字','graphic_object':True,'position':pos},
        {'id':'pic','type':'image','position':dict(pos)}],
        'issues':[{'code':'embedded_object_review','position':dict(pos),'status':'open'}],'resources':[]}
    def demand_original(*args):raise Rejected('需要完整原图')
    monkeypatch.setattr(evidence_tasks,'_pdf',demand_original)
    with pytest.raises(Rejected,match='完整原图'):
        evidence_tasks.resolve_scoped(meta,tmp_path,tmp_path/'source.docx',tmp_path/'visual',80,object(),lambda *a:None)


@pytest.mark.parametrize('complete',[True,False])
def test_graphic_crop_replaces_partial_bitmap_only_after_pixel_review(tmp_path,complete):
    import fitz,evidence_tasks,io,math
    from auto_evidence import media_privacy
    src=tmp_path/'source.pdf'
    with fitz.open() as pdf:
        page=pdf.new_page();page.draw_line((100,300),(300,100))
        page.insert_text((85,320),'Start');page.insert_text((305,100),'End');pdf.save(src)
        page_pixels=Image.open(io.BytesIO(page.get_pixmap(matrix=fitz.Matrix(1.5,1.5)).tobytes('png'))).convert('RGB')
    original_bytes=src.read_bytes()
    bbox=[.1,.1,.6,.6] if complete else [.1,.28,.32,.4]
    expected=page_pixels.crop((math.floor(bbox[0]*page_pixels.width),math.floor(bbox[1]*page_pixels.height),
                               math.ceil(bbox[2]*page_pixels.width),math.ceil(bbox[3]*page_pixels.height)))
    assert expected.getextrema()!=((255,255),(255,255),(255,255))
    regions=[(x0,y0,x1,y1) for x0,x1 in ((0,math.ceil(expected.width*.55)),(math.floor(expected.width*.45),expected.width))
             for y0,y1 in ((0,math.ceil(expected.height*.55)),(math.floor(expected.height*.45),expected.height))]
    (tmp_path/'assets').mkdir();(tmp_path/'_review').mkdir()
    Image.new('RGB',(20,20),'red').save(tmp_path/'_review/partial.png')
    pos={'page':1,'object_ref':'group'}
    meta={'blocks':[{'id':'g','type':'paragraph','text':'Start End[[IMAGE:partial]]','graphic_object':True,'position':pos},
        {'id':'partial','resource_id':'partial','type':'image','position':dict(pos)}],
        'issues':[{'code':'embedded_object_review','position':dict(pos),'status':'open'}],
        'resources':[{'id':'partial','kind':'image','position':dict(pos),'local_path':'_review/partial.png','privacy_status':'pending'}]}
    class Agent:
        def __init__(self):self.calls=[];self.reviews=[]
        def call(self,task,payload,images=()):
            self.calls.append(task)
            if task=='repair':return {'decision':'pass','reason':'恢复完整研究图','sensitive_boxes':[],
                'insertions':[{'after_id':'g','page':1,'kind':'image','bbox':bbox,
                    'text':'Start End' if complete else 'Start','description':'Original diagram crop'}]}
            assert task=='privacy_image' and len(images)==1
            assert payload['id']=='visual_p1_1' and payload['position']['page']==1
            assert payload['phase'] in {'locate_privacy_regions','verify_masked'}
            region=payload.get('review_region');pixels=expected
            if region:
                assert complete and tuple(region) in regions
                pixels=expected.crop(region);scale=max(1,math.ceil(300/min(pixels.size)))
                if scale>1:pixels=pixels.resize((pixels.width*scale,pixels.height*scale),Image.Resampling.NEAREST)
            actual=Image.open(io.BytesIO(images[0])).convert('RGB')
            assert actual.size==pixels.size and actual.tobytes()==pixels.tobytes()
            self.reviews.append((payload['phase'],region))
            if not complete and payload['phase']=='verify_masked':
                return {'decision':'reject','reason':'The End label and connecting segment are clipped',
                        'failure_kind':'content','research':True,'sensitive_boxes':[],
                        'text':'Start','description':'Partial diagram; the end is outside the crop'}
            return {'decision':'pass','reason':'Actual crop pixels contain no identity','research':True,'sensitive_boxes':[],
                    'text':'' if region else ('Start End' if complete else 'Start'),
                    'description':'Local region checked for identity' if region else 'Original diagram crop'}
    agent=Agent()
    evidence_tasks.resolve_scoped(meta,tmp_path,src,tmp_path/'visual',80,agent,lambda *a:None)
    if not complete:
        with pytest.raises(Rejected,match='End label.*clipped'):
            media_privacy(meta,tmp_path,agent,lambda *a:None)
        assert agent.calls==['repair','privacy_image','privacy_image']
        assert agent.reviews==[('locate_privacy_regions',None),('verify_masked',None)]
        assert not meta['resources'][0].get('masked_observation_verified')
        assert src.read_bytes()==original_bytes
        return
    media_privacy(meta,tmp_path,agent,lambda *a:None)
    assert agent.calls==['repair']+['privacy_image']*6
    assert agent.reviews==[('locate_privacy_regions',None),
                           *(('verify_masked',list(region)) for region in regions),('verify_masked',None)]
    assert len(meta['resources'])==1 and meta['resources'][0]['source']=='original_page_crop'
    assert meta['resources'][0]['masked_observation_verified'] is True
    published=Image.open(tmp_path/meta['resources'][0]['local_path']).convert('RGB')
    assert published.size==expected.size and published.tobytes()==expected.tobytes()
    assert '[[IMAGE:partial]]' not in str(meta['blocks'])
    assert meta['blocks'][0]['text']=='Start End'
    assert src.read_bytes()==original_bytes


def split_graphic(tmp_path):
    labels=[['AlphaStage','BetaStage','GammaStage','DeltaStage'],['EpsilonStage','ZetaStage']]
    def shape(items):
        text=''.join(f'<w:p><w:r><w:t>{s}</w:t></w:r></w:p>' for s in items)
        return '<w:p><w:r><w:pict><v:rect><v:textbox><w:txbxContent>'+text+'</w:txbxContent></v:textbox></v:rect></w:pict></w:r></w:p>'
    src=docx(tmp_path/'source.docx',shape(labels[0])+'<w:p/>'*40+shape(labels[1]))
    meta=build_package(src,tmp_path/'package',{'id':'split','title':'跨页图'},redact_content=False)
    return src,meta,labels


def test_merged_graphic_location_covers_members_across_natural_page_break(tmp_path):
    from evidence_tasks import _locate
    _,meta,labels=split_graphic(tmp_path)
    issue=next(i for i in meta['issues'] if i['code']=='embedded_object_review')
    assert _locate(issue,meta['blocks'],[' '.join(row) for row in labels])=={0,1}


def test_merged_graphic_records_missing_member_pages_as_a_gap(tmp_path,monkeypatch):
    import fitz,evidence_tasks
    src,meta,labels=split_graphic(tmp_path)
    group=next(b for b in meta['blocks'] if b.get('graphic_object'))
    pdf=fitz.open()
    for row in labels:pdf.new_page(width=300,height=300).insert_text((20,40),' '.join(row),fontsize=8)
    monkeypatch.setattr(evidence_tasks,'_pdf',lambda *a:pdf)
    class Agent:
        def call(self,task,payload,images=()):return {'decision':'pass','reason':'仅恢复第一页','sensitive_boxes':[],
            'page_masks':[{'page':p,'boxes':[]} for p in payload['pages']],
            'insertions':[{'after_id':group['id'],'page':1,'kind':'image','bbox':[.02,.05,.98,.3],
                'text':' '.join(labels[0]),'description':'第一页图形'}]}
    evidence_tasks.resolve_scoped(meta,tmp_path/'package',src,tmp_path/'visual',80,Agent(),lambda *a:None)
    assert meta['unrecovered_objects'][0]['id']==group['id']
    assert next(i for i in meta['issues'] if i['code']=='embedded_object_review')['status']=='recorded'


def test_chart_internal_part_names_are_included_in_same_privacy_pass():
    from auto_evidence import text_privacy
    meta={'title':'财务资料','blocks':[{'type':'table','native_chart':{'part':'word/charts/张小测.xml'}}],
        'resources':[],'formulas':[]}
    class Agent:
        def call(self,task,payload,images=()):return {'decision':'pass','reason':'去除作者信息',
            'sensitive_strings':['张小测'] if '张小测' in payload['text'] else []}
    text_privacy(meta,Agent(),lambda *a:None)
    assert '张小测' not in str(meta)


def test_split_graphic_common_labels_are_located_together_not_in_table_of_contents():
    from evidence_tasks import _locate
    pos={'body_index':1,'object_ref':'group'}
    members=[{'position':pos,'text':t,'locator_texts':[t]} for t in
        ['研究背景及意义','文献综述','研究内容和方法','本公司的价值链图形内容']]
    block={'id':'g','type':'paragraph','text':'','position':pos,'graphic_members':members,
        'locator_texts':['本公司的价值链图形内容']}
    pages=['研究背景及意义 文献综述','研究背景及意义','文献综述','研究内容和方法',
        '研究背景及意义 文献综述 研究内容和方法','本公司的价值链图形内容']
    # A unique final member anchors page 6; jointly occurring earlier members anchor page 5.
    assert _locate({'position':pos},[block],pages)=={4,5}


def test_dual_axis_chart_does_not_claim_unmapped_series_units_are_complete(tmp_path):
    chart=chart_xml().replace('</c:plotArea>','<c:valAx><c:axId val="1"/></c:valAx><c:valAx><c:axId val="2"/></c:valAx></c:plotArea>')
    blocks,_,issues=parse(chart_doc(tmp_path,chart))
    assert not any(b.get('source')=='native_chart_cache' for b in blocks)
    assert any(i['code']=='embedded_object_review' for i in issues)
