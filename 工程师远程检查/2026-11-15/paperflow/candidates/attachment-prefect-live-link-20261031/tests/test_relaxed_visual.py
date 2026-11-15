import hashlib
import json
from pathlib import Path

from docx import Document
from PIL import Image

from local_prepare import plan_clean_material, prepare_source


def settings():
    return {'model':'fixture','context_window':200000,'max_tokens':500,'max_output_tokens':500,
            'context_reserve_tokens':200,'image_token_upper_bound':2048,'max_images':100,
            'max_total_image_bytes':2000000,'max_single_image_bytes':5000000,
            'max_image_side':2000,'max_request_bytes':8000000,
            'capacity_source':'synthetic offline fixture'}


def make_picture_paper(tmp_path):
    image=tmp_path/'chart.png'
    Image.new('RGB',(200,200),'blue').save(image)
    source=tmp_path/'body.docx'
    doc=Document();doc.add_heading('摘要',1);doc.add_paragraph('研究正文。');doc.add_picture(str(image));doc.save(source)
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    result=prepare_source(source,hashlib.sha256(source.read_bytes()).hexdigest(),tmp_path/'work',catalog)
    return Path(result['material_path']),catalog


def test_body_figure_with_clear_location_does_not_block_offline_plan(tmp_path, monkeypatch):
    import visual_ocr
    monkeypatch.setattr(visual_ocr,'read_image',lambda path:[{'text':'实验数据','confidence':0.9}])
    material,catalog=make_picture_paper(tmp_path)
    data=json.loads(material.read_text(encoding='utf8'))
    assert not any(item['kind']=='visual_scope' for item in data['readiness']['items'])
    result=plan_clean_material(material,'模板',catalog,settings(),tmp_path/'plan')
    assert result['plan']['coverage']['complete']
    assert result['state']=='not_sent'


def test_located_figure_is_not_held_by_image_ocr(tmp_path, monkeypatch):
    import visual_ocr
    monkeypatch.setattr(visual_ocr,'read_image',lambda path:[{'text':'参考文献','confidence':0.9}])
    material,_=make_picture_paper(tmp_path)
    data=json.loads(material.read_text(encoding='utf8'))
    assert not any(item['kind']=='visual_scope' for item in data['readiness']['items'])

def test_obvious_single_column_reference_page_does_not_create_review(tmp_path):
    import fitz
    from pdf_visual import guard_scope_columns
    source=tmp_path/'paper.pdf';doc=fitz.open();doc.new_page(width=600,height=800);doc.save(source);doc.close()
    heading={'id':'h','type':'paragraph','text':'References','position':{'page':1,'bbox':[250,80,350,100]}}
    line1={'id':'r1','type':'paragraph','text':'[1] A reference.','position':{'page':1,'bbox':[60,120,450,140]}}
    line2={'id':'r2','type':'paragraph','text':'[2] Another reference.','position':{'page':1,'bbox':[60,160,450,180]}}
    issues=[]
    guard_scope_columns(source,[heading,line1,line2],[],[{'reason':'references','block':x} for x in (heading,line1,line2)],issues)
    assert not any(item['code'].startswith('pdf_column_') for item in issues)

def test_rotated_scan_ocr_maps_boxes_back_to_original_page_coordinates(tmp_path, monkeypatch):
    import fitz
    import visual_ocr
    source=tmp_path/'rotated.pdf';doc=fitz.open();page=doc.new_page(width=200,height=300)
    page.set_rotation(90);doc.save(source);doc.close()
    with fitz.open(source) as pdf:
        page=pdf[0]
        original=fitz.Rect(20,30,80,50)
        rotated=original * page.rotation_matrix
        points=[[rotated.x0*2,rotated.y0*2],[rotated.x1*2,rotated.y0*2],
                [rotated.x1*2,rotated.y1*2],[rotated.x0*2,rotated.y1*2]]
        monkeypatch.setattr(visual_ocr,'_engine',lambda:lambda image:([[points,'research',0.9]],None))
        result=visual_ocr.read_pdf_page(page)
    assert result[0]['bbox']==list(original)

def test_rotated_scan_crop_keeps_research_and_excludes_tail_pixels(tmp_path):
    import io
    import fitz
    from PIL import Image
    from pdf_visual import preserve_scans
    source=tmp_path/'rotated.pdf';doc=fitz.open();page=doc.new_page(width=200,height=300)
    page.draw_rect(fitz.Rect(20,30,80,60),fill=(1,0,0))
    page.draw_rect(fitz.Rect(20,230,80,260),fill=(0,0,1))
    page.set_rotation(90);doc.save(source);doc.close()
    kept=[{'id':'body','type':'paragraph','text':'body','position':{'page':1,'bbox':[20,30,80,60]}}]
    excluded={'id':'tail','type':'paragraph','text':'References','position':{'page':1,'bbox':[20,200,120,220]}}
    saved=[]
    def resource(rid,blob,extension,kind,position):
        saved.append(blob);return {'id':rid}
    images=[]
    preserve_scans(source,kept,[{'block':excluded,'reason':'references'}],{1},resource,images)
    im=Image.open(io.BytesIO(saved[0])).convert('RGB')
    red=sum(1 for r,g,b in im.getdata() if r>200 and g<80 and b<80)
    blue=sum(1 for r,g,b in im.getdata() if b>200 and r<80 and g<80)
    assert red>100
    assert blue==0

def test_located_figure_and_body_are_both_planned(tmp_path, monkeypatch):
    import visual_ocr
    monkeypatch.setattr(visual_ocr,'read_image',lambda path:[{'text':'参考文献','confidence':0.9}])
    material,catalog=make_picture_paper(tmp_path)
    result=plan_clean_material(material,'模板',catalog,settings(),tmp_path/'plan')
    plan=result['plan']
    payload=json.loads(plan['parts'][0]['body']['messages'][1]['content'][0]['text'])
    body=json.dumps(payload,ensure_ascii=False)
    assert '研究正文' in body
    assert plan['coverage']['whole_paper_complete'] is False
    assert any(part['asset_ids'] for part in plan['parts'])
    assert '参考文献' not in body
    saved=json.loads(Path(result['plan_path']).read_text(encoding='utf8'))
    assert saved['source_completeness']=='prepared'
    assert saved['state']=='not_sent'

def test_partial_plan_never_leaks_disputed_boundary_page_as_text_or_context(tmp_path, monkeypatch):
    import fitz
    from local_prepare import _native_paths
    _native_paths()
    import evidence
    def parse(source,redact,resource,issue):
        return ([{'id':'ab','type':'paragraph','text':'Abstract','position':{'page':1,'bbox':[40,20,180,40]}},
                 {'id':'first','type':'paragraph','text':'Safe body first page.','position':{'page':1,'bbox':[40,80,500,110]}},
                 {'id':'right','type':'paragraph','text':'DO_NOT_LEAK_BOUNDARY_TEXT','heading_level':1,'position':{'page':2,'bbox':[350,100,500,120]}},
                 {'id':'ref','type':'paragraph','text':'References','position':{'page':2,'bbox':[40,200,200,220]}},
                 {'id':'entry','type':'paragraph','text':'DO_NOT_LEAK_CITATION','position':{'page':2,'bbox':[40,240,200,260]}}], [], [])
    monkeypatch.setattr(evidence,'parse_pdf',parse)
    source=tmp_path/'columns.pdf';doc=fitz.open()
    p=doc.new_page(width=600,height=800);p.insert_text((40,30),'Abstract');p.insert_text((40,90),'Safe body first page.')
    p=doc.new_page(width=600,height=800);p.insert_text((350,110),'DO_NOT_LEAK_BOUNDARY_TEXT');p.insert_text((40,210),'References')
    doc.save(source);doc.close()
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    prepared=prepare_source(source,hashlib.sha256(source.read_bytes()).hexdigest(),tmp_path/'work',catalog)
    material=json.loads(Path(prepared['material_path']).read_text(encoding='utf8'))
    assert any(x['kind']=='column_scope_review' for x in material['readiness']['items']), material['readiness']
    result=plan_clean_material(prepared['material_path'],'模板',catalog,settings(),tmp_path/'plan')
    requests=json.dumps([p['body'] for p in result['plan']['parts']],ensure_ascii=False)
    assert 'Safe body first page.' in requests
    assert 'DO_NOT_LEAK_BOUNDARY_TEXT' in requests
    assert 'DO_NOT_LEAK_CITATION' not in requests
    assert result['plan']['coverage']['whole_paper_complete'] is False

def test_thin_table_rule_fragments_are_grouped_with_research_text(tmp_path):
    import fitz
    from pdf_visual import preserve_table_page_evidence
    source=tmp_path/'table.pdf';doc=fitz.open();p=doc.new_page(width=600,height=800)
    for y in (150,170,190,210):p.draw_line((60,y),(530,y))
    p.insert_text((70,165),'Method A    10')
    p.insert_text((70,185),'Method B    20')
    doc.save(source);doc.close()
    kept=[{'id':'text1','type':'paragraph','text':'Method A    10','position':{'page':1,'bbox':[70,155,250,167]}},
          {'id':'text2','type':'paragraph','text':'Method B    20','position':{'page':1,'bbox':[70,175,250,187]}}]
    resources=[];images=[]
    for i,y in enumerate((150,170,190,210)):
        pos={'page':1,'bbox':[59,y-2,531,y+2],'vector_region':True}
        block={'id':f'line{i}','type':'image','position':pos,'resource_id':f'r{i}','source':'pdf_vector_crop'}
        kept.append(block);images.append(block);resources.append({'id':f'r{i}','position':pos})
    saved=[];issues=[]
    def resource(rid,blob,extension,kind,position):
        saved.append((rid,blob,kind,position));row={'id':rid,'kind':kind,'position':position};resources.append(row);return row
    def issue(code,message,position):issues.append({'code':code,'message':message,'position':position})
    preserve_table_page_evidence(source,kept,[],resources,images,resource,issue)
    assert any(item[2]=='pdf_table_page_crop' for item in saved)
    assert any(block.get('source')=='pdf_table_page_crop' for block in kept)
    assert not any(block.get('id','').startswith('line') for block in kept)
    assert all(any(block.get('id')==name for block in kept) for name in ('text1','text2'))

def test_table_evidence_does_not_include_excluded_reference_area(tmp_path):
    import fitz
    from pdf_visual import preserve_table_page_evidence
    source=tmp_path/'boundary.pdf';doc=fitz.open();p=doc.new_page(width=600,height=800)
    for y in (150,170,190):p.draw_line((60,y),(530,y))
    p.insert_text((70,165),'Data');p.insert_text((70,185),'References')
    doc.save(source);doc.close()
    kept=[{'id':'t1','type':'paragraph','text':'Data','position':{'page':1,'bbox':[70,155,250,165]}},
          {'id':'t2','type':'paragraph','text':'More','position':{'page':1,'bbox':[70,175,250,185]}}]
    for i,y in enumerate((150,170,190)):
        kept.append({'id':f'line{i}','type':'image','position':{'page':1,'bbox':[59,y-2,531,y+2],'vector_region':True},
                     'resource_id':f'r{i}','source':'pdf_vector_crop'})
    removed=[{'reason':'references','block':{'id':'ref','type':'paragraph','text':'References',
              'position':{'page':1,'bbox':[70,175,250,185]}}}]
    saved=[]
    preserve_table_page_evidence(source,kept,removed,[],[],lambda *args:saved.append(args),lambda *args:None)
    assert saved==[]

def test_retained_figure_still_requires_real_image_capacity(tmp_path,monkeypatch):
    import pytest
    import visual_ocr
    monkeypatch.setattr(visual_ocr,'read_image',lambda path:[{'text':'研究图','confidence':0.9}])
    material,catalog=make_picture_paper(tmp_path);cfg=settings();del cfg['max_image_side']
    with pytest.raises(ValueError,match='图片容量'):
        plan_clean_material(material,'模板',catalog,cfg,tmp_path/'plan')

def test_scan_ocr_matching_native_text_is_not_reported_as_failure(tmp_path,monkeypatch):
    import fitz
    from pdf_visual import prepare_pages
    import visual_ocr
    import io
    from PIL import Image
    buf=io.BytesIO();Image.new('RGB',(100,100),'white').save(buf,format='PNG')
    source=tmp_path/'dual-layer.pdf';doc=fitz.open();p=doc.new_page(width=600,height=800)
    p.insert_image(p.rect,stream=buf.getvalue())
    p.insert_text((20,50),'Abstract')
    p.insert_text((20,80),'The existing text covers this research page.')
    doc.save(source);doc.close()
    raw=[{'id':'a','type':'paragraph','text':'Abstract','position':{'page':1,'bbox':[20,38,70,52]}},
         {'id':'b','type':'paragraph','text':'The existing text covers this research page.','position':{'page':1,'bbox':[20,68,190,82]}}]
    lines=[{'text':'Abstract','bbox':[20,38,70,52],'confidence':0.95},
           {'text':'The existing text covers this research page.','bbox':[20,68,190,82],'confidence':0.95}]
    monkeypatch.setattr(visual_ocr,'read_pdf_page',lambda page:lines)
    issues=[];scan,blank=prepare_pages(source,raw,[],[],issues,continue_on_uncertainty=True)
    assert 1 in scan and not blank
    assert not any(i['code']=='ocr_page_incomplete' for i in issues)

def test_interleaved_scan_scope_is_marked_without_stopping_other_scan_pages(tmp_path):
    import fitz
    from pdf_visual import preserve_scans
    source=tmp_path/'scan.pdf';doc=fitz.open();doc.new_page(width=600,height=800);doc.new_page(width=600,height=800);doc.save(source);doc.close()
    kept=[{'id':'a','type':'paragraph','text':'body','position':{'page':1,'bbox':[40,100,500,120]}},
          {'id':'b','type':'paragraph','text':'body later','position':{'page':1,'bbox':[40,300,500,320]}},
          {'id':'c','type':'paragraph','text':'next page','position':{'page':2,'bbox':[40,100,500,120]}}]
    removed=[{'reason':'references','block':{'id':'r','type':'paragraph','text':'References','position':{'page':1,'bbox':[40,200,500,220]}}}]
    issues=[];images=[];saved=[]
    def resource(rid,blob,extension,kind,position):saved.append(position['page']);return {'id':rid}
    preserve_scans(source,kept,removed,{1,2},resource,images,issues=issues,continue_on_uncertainty=True)
    assert saved==[2]
    assert any(i['code']=='pdf_scan_crop_scope_review' and i['position']['page']==1 for i in issues)

def test_bad_vector_region_does_not_prevent_later_region_recovery(tmp_path):
    import fitz
    from pdf_visual import preserve_vectors
    source=tmp_path/'vectors.pdf';doc=fitz.open();doc.new_page(width=600,height=800);doc.new_page(width=600,height=800);doc.save(source);doc.close()
    kept=[{'id':'bad','type':'paragraph','text':'[图]','unresolved_object':True,
           'position':{'page':1,'bbox':[40,100,500,300],'vector_region':True}},
          {'id':'good','type':'paragraph','text':'[图]','unresolved_object':True,
           'position':{'page':2,'bbox':[40,100,500,300],'vector_region':True}}]
    removed=[{'reason':'references','block':{'id':'ref','type':'paragraph','text':'References',
              'position':{'page':1,'bbox':[40,200,500,220]}}}]
    issues=[];images=[];saved=[]
    def resource(rid,blob,extension,kind,position):saved.append(position['page']);return {'id':rid}
    preserve_vectors(source,kept,removed,issues,resource,images,continue_on_uncertainty=True)
    assert saved==[2]
    assert any(i['code']=='pdf_visual_incomplete' and i['position']['page']==1 for i in issues)

def test_large_figure_is_tiled_when_channel_side_is_small(tmp_path):
    import hashlib
    from PIL import Image
    from local_prepare import _native_paths
    _native_paths()
    from preextract_images import prepare_request_images
    image=tmp_path/'figure.png';Image.new('RGB',(1200,900),'blue').save(image)
    asset={'id':'figure','kind':'image','image_path':str(image),'sha256':hashlib.sha256(image.read_bytes()).hexdigest(),
           'width':1200,'height':900,'block_ids':['b'],'section_ids':['s']}
    bundle={'assets':[asset],'blocks':[{'id':'b','object_ids':['figure']}]}
    adapted,report=prepare_request_images(bundle,tmp_path/'plan',{'max_image_side':400,'max_single_image_bytes':1000000})
    tiles=adapted['assets']
    assert len(tiles)>1 and report['images'][0]['tiled'] is True
    assert all(max(tile['width'],tile['height'])<=400 for tile in tiles)
    assert all(tile['image_adaptation']['source_sha256']==asset['sha256'] for tile in tiles)
    assert {tile['id'] for tile in tiles}==set(adapted['blocks'][0]['object_ids'])
    boxes=[tile['image_adaptation']['source_crop'] for tile in tiles]
    assert min(b[0] for b in boxes)==0 and min(b[1] for b in boxes)==0
    assert max(b[2] for b in boxes)==1200 and max(b[3] for b in boxes)==900

def test_tiled_figure_is_split_into_bounded_offline_parts(tmp_path,monkeypatch):
    import visual_ocr
    monkeypatch.setattr(visual_ocr,'read_image',lambda path:[{'text':'研究图','confidence':.9}])
    from PIL import ImageDraw
    source=tmp_path/'body.docx';picture=tmp_path/'large.png'
    canvas=Image.new('RGB',(1200,900),'white');draw=ImageDraw.Draw(canvas)
    for i in range(8):draw.rectangle((i*150,50,i*150+100,800),fill=(i*30,50,255-i*25))
    canvas.save(picture)
    doc=Document();doc.add_heading('摘要',1);doc.add_paragraph('研究正文。');doc.add_picture(str(picture));doc.save(source)
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    body=prepare_source(source,hashlib.sha256(source.read_bytes()).hexdigest(),tmp_path/'work',catalog)
    cfg=settings();cfg['max_image_side']=400;cfg['max_images']=2
    planned=plan_clean_material(body['material_path'],'模板',catalog,cfg,tmp_path/'plan')['plan']
    assert planned['mode']=='partitioned' and len(planned['parts'])>1
    assert planned['coverage']['complete']
    assert all(part['capacity']['image_count']<=2 for part in planned['parts'])
    assert any('__tile_' in ident for ident in planned['coverage']['asset_ids'])

def test_tiled_request_can_be_restored_against_original_image(tmp_path):
    import hashlib
    from PIL import ImageDraw
    from local_prepare import _native_paths
    _native_paths()
    from preextract_images import prepare_request_images,restore_request_images
    from preextract_request import build_request
    canvas=Image.new('RGB',(1200,900),'white');draw=ImageDraw.Draw(canvas);draw.rectangle((0,0,500,500),fill='blue')
    image=tmp_path/'figure.png';canvas.save(image)
    asset={'id':'figure','kind':'image','image_path':str(image),'sha256':hashlib.sha256(image.read_bytes()).hexdigest(),
           'width':1200,'height':900,'block_ids':['b'],'section_ids':['s'],'caption':'Figure'}
    bundle={'assets':[asset],'blocks':[{'id':'b','section_id':'s','type':'image','text':'图','object_ids':['figure']}],
            'sections':[{'id':'s','title':'S','level':1,'parent_id':None}],'source':{},'issues':[],'formulas':[]}
    cfg=settings();cfg['max_image_side']=400
    adapted,report=prepare_request_images(bundle,tmp_path/'plan',cfg)
    request,_=build_request(adapted,'模板',{'majors':[]},cfg,enforce=False)
    restored=restore_request_images(bundle,tmp_path/'plan',request)
    assert [a['id'] for a in restored['assets']]==[a['id'] for a in adapted['assets']]
    assert [a['sha256'] for a in restored['assets']]==[a['sha256'] for a in adapted['assets']]

def test_tiled_rotated_image_restores_using_display_coordinates(tmp_path):
    import hashlib
    from local_prepare import _native_paths
    _native_paths()
    from preextract_images import prepare_request_images,restore_request_images
    from preextract_request import build_request
    image=tmp_path/'rotated.jpg';canvas=Image.new('RGB',(1200,800),'white')
    exif=Image.Exif();exif[274]=6;canvas.save(image,exif=exif)
    asset={'id':'figure','kind':'image','image_path':str(image),'sha256':hashlib.sha256(image.read_bytes()).hexdigest(),
           'width':1200,'height':800,'block_ids':['b'],'section_ids':['s']}
    bundle={'assets':[asset],'blocks':[{'id':'b','section_id':'s','type':'image','text':'图'}],
            'sections':[{'id':'s','title':'S','level':1,'parent_id':None}],'source':{},'issues':[],'formulas':[]}
    cfg=settings();cfg['max_image_side']=400
    adapted,_=prepare_request_images(bundle,tmp_path/'plan',cfg)
    request,_=build_request(adapted,'模板',{'majors':[]},cfg,enforce=False)
    restored=restore_request_images(bundle,tmp_path/'plan',request)
    assert len(restored['assets'])==len(adapted['assets'])

def test_noisy_image_can_tile_to_meet_byte_limit_without_dropping_text(tmp_path):
    import hashlib,random
    from local_prepare import _native_paths
    _native_paths()
    from preextract_images import prepare_request_images
    rng=random.Random(42);pixels=rng.randbytes(800*800*3)
    image=tmp_path/'noisy.png';Image.frombytes('RGB',(800,800),pixels).save(image)
    asset={'id':'fig','kind':'image','image_path':str(image),'sha256':hashlib.sha256(image.read_bytes()).hexdigest(),
           'width':800,'height':800,'block_ids':['b'],'section_ids':['s']}
    bundle={'assets':[asset],'blocks':[{'id':'b','object_ids':['fig']}]}
    adapted,report=prepare_request_images(bundle,tmp_path/'plan',{'max_image_side':2000,'max_single_image_bytes':60000})
    assert len(adapted['assets'])>1 and report['images'][0]['tiled'] is True
    assert all(Path(a['image_path']).stat().st_size<=60000 for a in adapted['assets'])

def test_unusable_image_capacity_holds_image_but_plans_other_body(tmp_path,monkeypatch):
    import visual_ocr
    monkeypatch.setattr(visual_ocr,'read_image',lambda path:[{'text':'研究图','confidence':.9}])
    material,catalog=make_picture_paper(tmp_path);cfg=settings();cfg['max_image_side']=32
    planned=plan_clean_material(material,'模板',catalog,cfg,tmp_path/'plan')
    assert planned['plan']['source_completeness']=='partial'
    assert '研究正文' in json.dumps(planned['plan']['parts'],ensure_ascii=False)
    assert not any(part['asset_ids'] for part in planned['plan']['parts'])
    assert planned['image_report']['held_count']==1

def test_user_stop_is_not_swallowed_by_word_visual_recovery(tmp_path,monkeypatch):
    import pytest
    from local_prepare import _native_paths
    _native_paths()
    import evidence_render
    from word_visual import recover_word_visuals
    def stop(*args,**kwargs):raise InterruptedError('stop requested')
    monkeypatch.setattr(evidence_render,'render_office_pdf',stop)
    position={'part':'word/document.xml','body_index':3,'object_ref':'obj'}
    kept=[{'id':'o','type':'paragraph','position':position},
          {'id':'caption','type':'paragraph','text':'图 1 实验图','position':{'part':'word/document.xml','body_index':4}}]
    issues=[{'code':'embedded_object_review','position':position}]
    with pytest.raises(InterruptedError):
        recover_word_visuals(tmp_path/'x.docx',tmp_path,kept,[],issues,lambda *args:None,[])
