import hashlib
import json
from pathlib import Path

from local_prepare import prepare_source


def test_front_toc_and_missing_note_keep_following_body_in_material(tmp_path):
    source = tmp_path / 'paper.txt'
    source.write_text('封面作者\n目录\n摘要 ........ I\n第1章 ........ 1\n摘要\n研究摘要。\n第1章 研究\n正文引用[^footnoteReference1]。\n研究结论。\n参考文献\n[1] 文献不应出现\n', encoding='utf8')
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    result = prepare_source(source, digest, tmp_path / 'work', catalog)
    material = json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    body = Path(material['source']['text_file']).read_text(encoding='utf8')
    assert '研究结论。' in body and '研究摘要。' in body
    assert '封面作者' not in body and '文献不应出现' not in body
    assert any(issue['code'] == 'missing_note' for issue in material['issues'])
    assert source.read_bytes().decode('utf8').startswith('封面作者')

def test_failed_ocr_page_keeps_other_pages_available_locally_and_marks_scope(tmp_path, monkeypatch):
    import io
    import fitz
    from PIL import Image
    import visual_ocr
    from local_prepare import plan_clean_material
    from test_capacity_plan import config

    picture=Image.new('RGB',(240,240),'white')
    for n in range(30,200):
        picture.putpixel((n,100),(0,0,0))
    stream=io.BytesIO();picture.save(stream,format='PNG')
    source=tmp_path/'scan.pdf';doc=fitz.open()
    p=doc.new_page();p.insert_text((50,80),'Abstract');p.insert_text((50,110),'Clearly retained body.')
    p=doc.new_page();p.insert_image(p.rect,stream=stream.getvalue())
    p=doc.new_page();p.insert_text((50,80),'Later page is locally parsed.')
    p=doc.new_page();p.insert_text((50,80),'References');p.insert_text((50,110),'Unique reference excluded.')
    doc.save(source);doc.close()
    original=source.read_bytes();digest=hashlib.sha256(original).hexdigest()
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    def fail_ocr(page):raise RuntimeError('synthetic OCR failure')
    monkeypatch.setattr(visual_ocr,'read_pdf_page',fail_ocr)
    result=prepare_source(source,digest,tmp_path/'work',catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    index=json.loads(Path(material['source']['index_file']).read_text(encoding='utf8'))
    assert any('Clearly retained body.' in b.get('text','') for b in material['blocks'])
    assert any('Later page is locally parsed.' in b.get('text','') for b in index['source_blocks'])
    assert any('Later page is locally parsed.' in b.get('text','') for b in material['blocks'])
    assert any(i['code']=='ocr_page_incomplete' and i['position']['page']==2 for i in material['issues'])
    assert any(i['code']=='post_ocr_scope_unverified' for i in material['issues'])
    assert material['readiness']['state']=='review_required'
    plan=plan_clean_material(result['material_path'],'模板',catalog,config(200000),tmp_path/'plan')['plan']
    requests=json.dumps([part['body'] for part in plan['parts']],ensure_ascii=False)
    assert plan['source_completeness']=='partial'
    assert 'Later page is locally parsed.' in requests
    assert 'Unique reference excluded.' not in requests
    assert '后续范围' in requests
    assert source.read_bytes()==original

def test_ocr_gap_warning_is_present_in_each_long_text_part(tmp_path, monkeypatch):
    import io
    import fitz
    from PIL import Image
    import visual_ocr
    from local_prepare import plan_clean_material
    from test_capacity_plan import config

    picture=Image.new('RGB',(240,240),'white')
    for n in range(30,200):picture.putpixel((n,100),(0,0,0))
    stream=io.BytesIO();picture.save(stream,format='PNG')
    source=tmp_path/'long.pdf';doc=fitz.open()
    p=doc.new_page();p.insert_text((50,60),'Abstract');p.insert_text((50,80),'Clearly retained research body.')
    p=doc.new_page();p.insert_image(p.rect,stream=stream.getvalue())
    for _ in range(3):
        p=doc.new_page()
        for i in range(40):p.insert_text((30,40+i*14),'continuous research results ' * 4, fontsize=7)
    doc.save(source);doc.close()
    monkeypatch.setattr(visual_ocr,'read_pdf_page',lambda page:[])
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    material=prepare_source(source,hashlib.sha256(source.read_bytes()).hexdigest(),tmp_path/'work',catalog)
    plan=plan_clean_material(material['material_path'],'模板',catalog,config(14000),tmp_path/'plan')['plan']
    assert plan['mode']=='partitioned' and plan['source_completeness']=='partial'
    for part in plan['parts']:
        view=json.loads(part['body']['messages'][1]['content'][0]['text'])['material']
        assert '后续范围' in view['reading_context']['instruction']
        assert '不得称为整篇完整' in view['reading_context']['instruction']
        assert view['reading_context']['ocr_incomplete_pages'] == [2]
    saved=json.loads((tmp_path/'plan'/'offline-plan.json').read_text(encoding='utf8'))
    assert any(gap['code']=='ocr_page_incomplete' and gap['position']['page']==2 for gap in saved['reading_gaps'])
    assert any(gap['code']=='image_capacity_unavailable' and gap['position']['page']==2 for gap in saved['reading_gaps'])
    import preextract_images
    original_adapt = preextract_images.prepare_request_images
    def simulate_image_hold(*args, **kwargs):
        adapted, report = original_adapt(*args, **kwargs)
        return adapted, dict(report, held_count=1)
    monkeypatch.setattr(preextract_images,'prepare_request_images',simulate_image_hold)
    image_plan=plan_clean_material(material['material_path'],'模板',catalog,config(14000),tmp_path/'plan-image')['plan']
    for part in image_plan['parts']:
        view=json.loads(part['body']['messages'][1]['content'][0]['text'])['material']
        assert '后续范围' in view['reading_context']['instruction']
        assert '部分图片' in view['reading_context']['instruction']

def test_known_reference_heading_on_ocr_failed_page_still_excludes_following_references(tmp_path, monkeypatch):
    import io
    import fitz
    from PIL import Image
    import visual_ocr
    from local_prepare import plan_clean_material
    from test_capacity_plan import config

    picture=Image.new('RGB',(240,240),'white')
    for n in range(30,200):picture.putpixel((n,100),(0,0,0))
    stream=io.BytesIO();picture.save(stream,format='PNG')
    source=tmp_path/'mixed.pdf';doc=fitz.open()
    p=doc.new_page();p.insert_text((50,80),'Abstract');p.insert_text((50,110),'Valid research text.')
    p=doc.new_page();p.insert_image(p.rect,stream=stream.getvalue());p.insert_text((50,80),'References')
    p=doc.new_page();p.insert_text((50,80),'[1] REFERENCE_MUST_NOT_BE_INCLUDED')
    doc.save(source);doc.close()
    monkeypatch.setattr(visual_ocr,'read_pdf_page',lambda page:[])
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    material=prepare_source(source,hashlib.sha256(source.read_bytes()).hexdigest(),tmp_path/'work',catalog)
    plan=plan_clean_material(material['material_path'],'模板',catalog,config(200000),tmp_path/'plan')['plan']
    requests=json.dumps([part['body'] for part in plan['parts']],ensure_ascii=False)
    assert 'Valid research text.' in requests
    assert 'REFERENCE_MUST_NOT_BE_INCLUDED' not in requests
    assert plan['source_completeness']=='partial'

def test_confirmed_review_does_not_release_unreadable_page_as_research(tmp_path, monkeypatch):
    import io
    import fitz
    from PIL import Image
    import visual_ocr
    from local_prepare import plan_clean_material
    from test_capacity_plan import config

    picture=Image.new('RGB',(240,240),'white')
    for n in range(30,200):picture.putpixel((n,100),(0,0,0))
    stream=io.BytesIO();picture.save(stream,format='PNG')
    source=tmp_path/'mixed.pdf';doc=fitz.open()
    p=doc.new_page();p.insert_text((50,80),'Abstract');p.insert_text((50,110),'Valid research text.')
    p=doc.new_page();p.insert_image(p.rect,stream=stream.getvalue());p.insert_text((50,80),'COPYRIGHT_MUST_STAY_OUT')
    p=doc.new_page();p.insert_text((50,80),'Later valid research text.')
    doc.save(source);doc.close()
    monkeypatch.setattr(visual_ocr,'read_pdf_page',lambda page:[])
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    result=prepare_source(source,hashlib.sha256(source.read_bytes()).hexdigest(),tmp_path/'work',catalog)
    path=Path(result['material_path']);data=json.loads(path.read_text(encoding='utf8'))
    assert any('COPYRIGHT_MUST_STAY_OUT' in b.get('text','') for b in data['blocks'])
    decisions=[{'id':item['id'], 'decision':('not_research' if item['kind'] in {'unresolved_boundary','unresolved_visual'} else 'confirmed'),
                'evidence':'source checked'} for item in data['readiness']['items']]
    review=tmp_path/'review.json';review.write_text(json.dumps({'schema':'local-material-review/1',
        'material_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'reviewer':'tester',
        'decisions':decisions}),encoding='utf8')
    plan=plan_clean_material(path,'模板',catalog,config(200000),tmp_path/'plan',review_path=review)['plan']
    requests=json.dumps([part['body'] for part in plan['parts']],ensure_ascii=False)
    assert 'Later valid research text.' in requests
    assert 'COPYRIGHT_MUST_STAY_OUT' not in requests
    assert plan['source_completeness']=='partial'

def test_confirmed_scan_crop_review_still_withholds_uncertain_ocr(tmp_path, monkeypatch):
    import io
    import fitz
    from PIL import Image
    import pdf_visual
    import visual_ocr
    from local_prepare import plan_clean_material
    from test_capacity_plan import config
    picture=Image.new('RGB',(240,240),'white')
    for n in range(30,200):picture.putpixel((n,100),(0,0,0))
    stream=io.BytesIO();picture.save(stream,format='PNG')
    source=tmp_path/'scan.pdf';doc=fitz.open()
    p=doc.new_page();p.insert_text((50,80),'Abstract');p.insert_text((50,110),'Valid research body.')
    p=doc.new_page();p.insert_image(p.rect,stream=stream.getvalue());p.insert_text((50,80),'Native valid text.')
    doc.save(source);doc.close()
    monkeypatch.setattr(visual_ocr,'read_pdf_page',lambda page:[{'text':'OCR_MUST_STAY_OUT',
        'bbox':[50,140,300,160],'confidence':.99}])
    def unresolved(*args, issues, **kwargs):
        issues.append({'code':'pdf_scan_crop_scope_review','position':{'page':2},'message':'图像待核'})
    monkeypatch.setattr(pdf_visual,'preserve_scans',unresolved)
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    result=prepare_source(source,hashlib.sha256(source.read_bytes()).hexdigest(),tmp_path/'work',catalog)
    path=Path(result['material_path']);data=json.loads(path.read_text(encoding='utf8'))
    assert any('OCR_MUST_STAY_OUT' in b.get('text','') for b in data['blocks'])
    decisions=[{'id':item['id'], 'decision':('not_research' if item['kind'] in {'unresolved_boundary','unresolved_visual'} else 'confirmed'),
                'evidence':'source checked'} for item in data['readiness']['items']]
    review=tmp_path/'review.json';review.write_text(json.dumps({'schema':'local-material-review/1',
        'material_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'reviewer':'tester','decisions':decisions}),encoding='utf8')
    plan=plan_clean_material(path,'模板',catalog,config(200000),tmp_path/'plan',review_path=review)['plan']
    content=json.dumps([part['body'] for part in plan['parts']],ensure_ascii=False)
    assert 'Native valid text.' in content and 'OCR_MUST_STAY_OUT' not in content
    assert plan['source_completeness']=='partial'

def test_empty_plan_preserves_ocr_gap_page_and_reason(tmp_path, monkeypatch):
    import io
    import fitz
    from PIL import Image
    import visual_ocr
    from local_prepare import plan_clean_material
    from test_capacity_plan import config

    picture=Image.new('RGB',(240,240),'white')
    for n in range(30,200):picture.putpixel((n,100),(0,0,0))
    stream=io.BytesIO();picture.save(stream,format='PNG')
    source=tmp_path/'scan.pdf';doc=fitz.open();p=doc.new_page()
    p.insert_image(p.rect,stream=stream.getvalue());doc.save(source);doc.close()
    monkeypatch.setattr(visual_ocr,'read_pdf_page',lambda page:[])
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    material=prepare_source(source,hashlib.sha256(source.read_bytes()).hexdigest(),tmp_path/'work',catalog)
    result=plan_clean_material(material['material_path'],'模板',catalog,config(200000),tmp_path/'plan')
    assert result['plan']['mode']=='no_available_material'
    saved=json.loads(Path(result['plan_path']).read_text(encoding='utf8'))
    assert saved['source_completeness']=='partial' and not saved['coverage']['whole_paper_complete']
    assert any(gap['code']=='ocr_page_incomplete' and gap['position']['page']==1 for gap in saved['reading_gaps'])

def test_new_planner_rejects_previous_cleanup_version(tmp_path):
    import pytest
    from local_prepare import plan_clean_material
    from test_capacity_plan import config
    source=tmp_path/'paper.txt';source.write_text('摘要\n有效的研究正文。',encoding='utf8')
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    result=prepare_source(source,hashlib.sha256(source.read_bytes()).hexdigest(),tmp_path/'work',catalog)
    path=Path(result['material_path']);data=json.loads(path.read_text(encoding='utf8'))
    data['parser_version']='candidate-direct-clean/20'
    data['cleanup']['version']='reading-scope/20'
    path.write_text(json.dumps(data,ensure_ascii=False),encoding='utf8')
    with pytest.raises(ValueError,match='版本无法核对'):
        plan_clean_material(path,'模板',catalog,config(200000),tmp_path/'plan')

def test_unique_clear_first_chapter_is_warning_not_whole_paper_review(tmp_path):
    from docx import Document
    source=tmp_path/'no-abstract.docx'
    doc=Document();doc.add_paragraph('封面');doc.add_heading('第一章 绪论',1)
    doc.add_paragraph('本文分析研究问题并给出方法和结果。' * 6);doc.save(source)
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    result=prepare_source(source,hashlib.sha256(source.read_bytes()).hexdigest(),tmp_path/'work',catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert any('本文分析' in b.get('text','') for b in material['blocks'])
    assert any(i['kind']=='no_abstract' for i in material['readiness']['items'])
    assert any(i['code']=='abstract_missing' for i in material['issues'])

def test_table_with_ambiguous_label_keeps_later_content(tmp_path):
    from docx import Document
    source=tmp_path/'mixed.docx';doc=Document();doc.add_heading('摘要',1);doc.add_paragraph('前面确认的研究正文。')
    table=doc.add_table(rows=1,cols=1);table.cell(0,0).text='参考文献'
    doc.add_paragraph('后续仍需在本地保留的文字。');doc.save(source)
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    result=prepare_source(source,hashlib.sha256(source.read_bytes()).hexdigest(),tmp_path/'work',catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    index=json.loads(Path(material['source']['index_file']).read_text(encoding='utf8'))
    assert any('前面确认的研究正文' in b.get('text','') for b in material['blocks'])
    assert any('后续仍需在本地保留' in b.get('text','') for b in index['source_blocks'])
    assert any('后续仍需在本地保留' in b.get('text','') for b in material['blocks'])

def test_pure_white_raster_page_is_skipped_without_ocr_or_review(tmp_path, monkeypatch):
    import io
    import fitz
    from PIL import Image
    import visual_ocr
    buf=io.BytesIO();Image.new('RGB',(200,200),'white').save(buf,format='PNG')
    source=tmp_path/'blank.pdf';doc=fitz.open();p=doc.new_page()
    p.insert_text((40,50),'Abstract');p.insert_text((40,80),'Research content before blank.')
    p=doc.new_page();p.insert_image(p.rect,stream=buf.getvalue())
    p=doc.new_page();p.insert_text((40,50),'Research content after blank.')
    doc.save(source);doc.close()
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    monkeypatch.setattr(visual_ocr,'read_pdf_page',lambda page: (_ for _ in ()).throw(AssertionError('OCR on blank')))
    result=prepare_source(source,hashlib.sha256(source.read_bytes()).hexdigest(),tmp_path/'work',catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert 2 in material['cleanup']['blank_pages']
    assert any('Research content after blank.' in b.get('text','') for b in material['blocks'])
    assert not any(i['code']=='ocr_page_incomplete' for i in material['issues'])
    assert not any((a.get('position') or {}).get('page')==2 for a in material['assets'])

def test_inline_image_preserves_neighboring_prose_without_leaking_link(tmp_path):
    from local_prepare import plan_clean_material
    from test_capacity_plan import config
    source=tmp_path/'paper.md'
    source.write_text('摘要\n研究摘要。\n图前解释重要。![chart](https://example.invalid/private.png)图后结论重要。\n参考文献\n引用条目',encoding='utf8')
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    result=prepare_source(source,hashlib.sha256(source.read_bytes()).hexdigest(),tmp_path/'work',catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    text='\n'.join(b.get('text','') for b in material['blocks'])
    assert '图前解释重要。' in text and '图后结论重要。' in text
    assert 'example.invalid' not in text
    planned=plan_clean_material(result['material_path'],'模板',catalog,config(200000),tmp_path/'plan')
    bodies=json.dumps([part['body'] for part in planned['plan']['parts']],ensure_ascii=False)
    assert '图前解释重要。' in bodies and '图后结论重要。' in bodies
    assert 'example.invalid' not in bodies
    assert planned['plan']['coverage']['whole_paper_complete'] is False

def test_text_only_plan_does_not_require_unneeded_image_limits(tmp_path):
    from local_prepare import plan_clean_material
    from test_capacity_plan import config
    source=tmp_path/'body.txt';source.write_text('摘要\n完整正文。',encoding='utf8')
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    result=prepare_source(source,hashlib.sha256(source.read_bytes()).hexdigest(),tmp_path/'work',catalog)
    settings=config(200000)
    for key in ('image_token_upper_bound','max_images','max_total_image_bytes','max_single_image_bytes','max_image_side'):
        del settings[key]
    planned=plan_clean_material(result['material_path'],'模板',catalog,settings,tmp_path/'plan')
    assert planned['plan']['coverage']['source_scope_fully_planned'] is True
    assert planned['plan']['coverage']['whole_paper_complete'] is False
    assert all(part['capacity']['image_count']==0 for part in planned['plan']['parts'])

def test_failed_local_attempt_retries_in_new_directory_without_overwriting_history(tmp_path):
    source=tmp_path/'paper.txt';source.write_text('摘要\n可继续处理的正文。',encoding='utf8')
    digest=hashlib.sha256(source.read_bytes()).hexdigest()
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    work=tmp_path/'work';work.mkdir()
    pending={'schema':'local-scope-pending/1','state':'review_required',
             'source_path':str(source.resolve()),'source_sha256':digest,'reason':'old failure'}
    history=work/'scope-pending.json';history.write_text(json.dumps(pending),encoding='utf8')
    before=history.read_bytes()
    result=prepare_source(source,digest,work,catalog)
    assert Path(result['material_path']).parent != work
    assert Path(result['material_path']).is_file()
    assert history.read_bytes()==before
    assert source.read_text(encoding='utf8').endswith('可继续处理的正文。')

def test_retry_keeps_identity_check_and_reuses_completed_attempt(tmp_path):
    import pytest
    source=tmp_path/'paper.txt';source.write_text('摘要\n正文。',encoding='utf8')
    digest=hashlib.sha256(source.read_bytes()).hexdigest()
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    work=tmp_path/'work';work.mkdir()
    history=work/'scope-pending.json'
    history.write_text(json.dumps({'source_path':str(source.resolve()),'source_sha256':digest,'reason':'old'}),encoding='utf8')
    first=prepare_source(source,digest,work,catalog)
    second=prepare_source(source,digest,work,catalog)
    assert first['material_path']==second['material_path']
    with pytest.raises(ValueError):prepare_source(source,'0'*64,work,catalog)
    assert history.is_file()

def test_attachment_pdf_preview_limit_is_configurable_and_marks_truncation(tmp_path):
    import fitz
    from attachment_preview import preview_attachment
    source=tmp_path/'attachment.pdf';doc=fitz.open()
    for n in range(6):
        page=doc.new_page();page.insert_text((40,60),f'Page {n+1} with enough native text for preview.')
    doc.save(source);doc.close()
    preview=preview_attachment(source,'pdf',limits={'pdf_pages':5,'text_chars':10000})
    assert len(preview['pages'])==5
    assert preview['total_pages']==6 and preview['truncated'] is True
    assert 'Page 5' in preview['pages'][-1]['text']

def test_missing_vector_on_one_page_does_not_stop_later_body(tmp_path,monkeypatch):
    import fitz
    from local_prepare import _native_paths,plan_clean_material
    from test_capacity_plan import config
    _native_paths();import evidence
    def parse(source,redact,resource,issue):
        return ([{'id':'ab','type':'paragraph','text':'Abstract','position':{'page':1,'bbox':[40,20,180,40]}},
                 {'id':'body','type':'paragraph','text':'Early research text.','position':{'page':1,'bbox':[40,60,200,80]}},
                 {'id':'later','type':'paragraph','text':'Later research continues.','position':{'page':2,'bbox':[40,60,200,80]}}],[],[])
    monkeypatch.setattr(evidence,'parse_pdf',parse)
    source=tmp_path/'missing-vector.pdf';doc=fitz.open();p=doc.new_page(width=600,height=800)
    p.insert_text((40,30),'Abstract');p.insert_text((40,70),'Early research text.');p.draw_rect(fitz.Rect(250,150,500,300),color=(0,0,0))
    p=doc.new_page(width=600,height=800);p.insert_text((40,70),'Later research continues.');doc.save(source);doc.close()
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    result=prepare_source(source,hashlib.sha256(source.read_bytes()).hexdigest(),tmp_path/'work',catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert any('Later research continues.' in b.get('text','') for b in material['blocks'])
    assert any(i['code']=='pdf_visual_incomplete' and i['position']['page']==1 for i in material['issues'])
    planned=plan_clean_material(result['material_path'],'模板',catalog,config(200000),tmp_path/'plan')
    assert planned['plan']['coverage']['whole_paper_complete'] is False

def test_native_pdf_parser_failure_falls_back_to_per_page_text(tmp_path,monkeypatch):
    import fitz
    from local_prepare import _native_paths,plan_clean_material
    from test_capacity_plan import config
    _native_paths();import evidence
    def fail(*args):raise RuntimeError('synthetic local parser failure')
    monkeypatch.setattr(evidence,'parse_pdf',fail)
    source=tmp_path/'fallback.pdf';doc=fitz.open();p=doc.new_page();p.insert_text((40,50),'Abstract');p.insert_text((40,80),'First page body.')
    p=doc.new_page();p.insert_text((40,50),'Later page body.');doc.save(source);doc.close()
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    result=prepare_source(source,hashlib.sha256(source.read_bytes()).hexdigest(),tmp_path/'work',catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert any('First page body.' in b.get('text','') for b in material['blocks'])
    assert any('Later page body.' in b.get('text','') for b in material['blocks'])
    assert any(i['code']=='pdf_visual_incomplete' for i in material['issues'])
    planned=plan_clean_material(result['material_path'],'模板',catalog,config(200000),tmp_path/'plan')
    assert planned['plan']['coverage']['whole_paper_complete'] is False

def test_docx_bad_media_crc_preserves_surrounding_text_and_source(tmp_path):
    import struct
    import zipfile
    from docx import Document
    from PIL import Image
    image=tmp_path/'picture.png';Image.new('RGB',(80,80),'blue').save(image)
    source=tmp_path/'damaged.docx';doc=Document();doc.add_heading('摘要',1);doc.add_paragraph('图片前的研究正文。')
    doc.add_picture(str(image));doc.add_paragraph('图片后的研究结论。');doc.save(source)
    data=bytearray(source.read_bytes());offset=0;changed=False
    while True:
        index=data.find(b'PK\x01\x02',offset)
        if index<0:break
        name_len,extra_len,comment_len=struct.unpack_from('<HHH',data,index+28)
        name=bytes(data[index+46:index+46+name_len]).decode('utf8')
        if name.startswith('word/media/'):
            crc=struct.unpack_from('<I',data,index+16)[0];struct.pack_into('<I',data,index+16,crc^1);changed=True;break
        offset=index+46+name_len+extra_len+comment_len
    assert changed
    source.write_bytes(data);original=source.read_bytes();digest=hashlib.sha256(original).hexdigest()
    with zipfile.ZipFile(source) as z:
        import pytest
        with pytest.raises(zipfile.BadZipFile):z.read(name)
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    result=prepare_source(source,digest,tmp_path/'work',catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    text='\n'.join(b.get('text','') for b in material['blocks'])
    assert '图片前的研究正文。' in text and '图片后的研究结论。' in text
    assert any(i['code']=='image_missing' for i in material['issues'])
    assert source.read_bytes()==original
    assert not any(p.is_file() and p.read_bytes()==original for p in (tmp_path/'work').rglob('*'))

def test_successful_ocr_text_is_warning_not_second_page_gate(tmp_path,monkeypatch):
    import io
    import fitz
    from PIL import Image
    import visual_ocr
    buf=io.BytesIO();picture=Image.new('RGB',(200,200),'white');picture.putpixel((80,80),(0,0,0));picture.save(buf,format='PNG')
    source=tmp_path/'scan.pdf';doc=fitz.open();p=doc.new_page(width=600,height=800);p.insert_image(p.rect,stream=buf.getvalue());doc.save(source);doc.close()
    lines=[{'text':'Abstract','bbox':[50,100,150,120],'confidence':.9},
           {'text':'OCR research body.','bbox':[50,150,300,170],'confidence':.9}]
    monkeypatch.setattr(visual_ocr,'read_pdf_page',lambda page:lines)
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    result=prepare_source(source,hashlib.sha256(source.read_bytes()).hexdigest(),tmp_path/'work',catalog)
    material=json.loads(Path(result['material_path']).read_text(encoding='utf8'))
    assert any(i['code']=='ocr_text_review' for i in material['issues'])
    assert not any(i['kind']=='ocr_text_review' for i in material['readiness']['items'])
    assert not any(i['kind']=='visual_scope' for i in material['readiness']['items'])

def test_image_attachment_ocr_is_local_preview_and_reports_limits(tmp_path,monkeypatch):
    import visual_ocr
    from PIL import Image
    from attachment_preview import preview_attachment
    source=tmp_path/'attachment.png';Image.new('RGB',(100,100),'white').save(source)
    original=source.read_bytes()
    monkeypatch.setattr(visual_ocr,'read_image',lambda path:[{'text':'设计图 42','confidence':.9},{'text':'附加说明','confidence':.8}])
    preview=preview_attachment(source,'png',limits={'text_chars':8})
    assert preview['status']=='source_image'
    assert '设计图 42' in preview['ocr_text']
    assert preview['ocr_status']=='completed_unverified' and preview['truncated'] is True
    assert source.read_bytes()==original
