import hashlib
import json
import socket
from pathlib import Path

import pytest
from docx import Document
from PIL import Image

from local_prepare import prepare_source, plan_clean_material


def make_material(tmp_path, *, repetitions):
    source = tmp_path / 'paper.txt'
    source.write_text('封面作者\n摘要\n摘要内容\n第1章 正文\n' +
                      '\n'.join('完整研究段落，保留方法和结果。' * 20 for _ in range(repetitions)) +
                      '\n参考文献\n不得传入的引用条目\n附录\n需要保留的附录。', encoding='utf8')
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    result = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(),
                            tmp_path / 'material', catalog)
    return Path(result['material_path']), catalog


def config(window):
    return {'model': 'offline-fixture', 'context_window': window, 'max_tokens': 500,
            'max_output_tokens': 500, 'context_reserve_tokens': 200,
            'image_token_upper_bound': 2048, 'max_images': 100,
            'max_total_image_bytes': 2000000, 'max_single_image_bytes': 5000000,
            'max_image_side': 2000, 'max_request_bytes': 8000000,
            'capacity_source': 'synthetic fixture; no real model limit asserted'}


def synthetic_review(material_path, destination):
    path = Path(material_path)
    material = json.loads(path.read_text(encoding='utf8'))
    review = {'schema':'local-material-review/1', 'material_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
              'reviewer':'synthetic fixture inspector', 'decisions':[
                  {'id':item['id'], 'decision':'confirmed', 'evidence':'synthetic image and scope checked'}
                  for item in material['readiness']['items']]}
    destination.write_text(json.dumps(review,ensure_ascii=False),encoding='utf8')
    return destination


def test_short_material_plans_one_request_without_network_or_excluded_sections(tmp_path, monkeypatch):
    material, catalog = make_material(tmp_path, repetitions=1)
    def no_network(*args, **kwargs):
        raise AssertionError('offline planning attempted a network connection')
    monkeypatch.setattr(socket.socket, 'connect', no_network)
    result = plan_clean_material(material, '模板', catalog, config(200000), tmp_path / 'plan')
    assert result['plan']['mode'] == 'single'
    assert result['plan']['coverage']['complete']
    assert result['state'] == 'not_sent'
    content = json.dumps(result['plan']['parts'][0]['body'], ensure_ascii=False)
    assert '完整研究段落' in content
    assert '封面作者' not in content and '不得传入' not in content
    assert '需要保留的附录' in content
    assert not (tmp_path / 'material' / 'model' / 'request-state.json').exists()


def test_long_material_is_partitioned_with_complete_primary_coverage(tmp_path):
    material, catalog = make_material(tmp_path, repetitions=30)
    result = plan_clean_material(material, '模板', catalog, config(14000), tmp_path / 'plan')
    plan = result['plan']
    assert plan['mode'] == 'partitioned' and len(plan['parts']) > 1
    assert plan['coverage']['complete']
    assert all(not part['capacity']['violations'] for part in plan['parts'])
    source = json.loads(material.read_text(encoding='utf8'))
    assert plan['coverage']['character_count'] == sum(len(b.get('text', '')) for b in source['blocks'])
    assert '不得传入' not in json.dumps([part['body'] for part in plan['parts']], ensure_ascii=False)
    assert '需要保留的附录' in json.dumps([part['body'] for part in plan['parts']], ensure_ascii=False)


def test_unknown_model_limits_and_existing_plan_are_not_overwritten(tmp_path):
    material, catalog = make_material(tmp_path, repetitions=1)
    missing = config(200000)
    del missing['context_window']
    with pytest.raises(ValueError, match='容量'):
        plan_clean_material(material, '模板', catalog, missing, tmp_path / 'missing')
    plan = tmp_path / 'existing'
    plan.mkdir()
    history = plan / 'request-state.json'
    history.write_text('unknown', encoding='utf8')
    with pytest.raises(ValueError):
        plan_clean_material(material, '模板', catalog, config(200000), plan)
    assert history.read_text(encoding='utf8') == 'unknown'


def test_images_are_owned_once_when_capacity_requires_multiple_parts(tmp_path):
    source = tmp_path / 'paper.docx'
    doc = Document()
    doc.add_heading('摘要', 1)
    doc.add_paragraph('研究使用以下图片。')
    for color in ('red', 'blue'):
        image = tmp_path / (color + '.png')
        Image.new('RGB', (200, 200), color).save(image)
        doc.add_picture(str(image))
    doc.add_heading('参考文献', 1)
    doc.add_paragraph('不得发送的文献')
    doc.save(source)
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    body = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path/'material', catalog)
    settings = config(200000)
    settings['max_images'] = 1
    review = synthetic_review(body['material_path'], tmp_path/'review.json')
    result = plan_clean_material(body['material_path'], '模板', catalog, settings, tmp_path/'plan',review_path=review)
    plan = result['plan']
    assert plan['mode'] == 'partitioned' and len(plan['parts']) >= 2
    material = json.loads(Path(body['material_path']).read_text(encoding='utf8'))
    assert sorted(plan['coverage']['asset_ids']) == sorted(a['id'] for a in material['assets'])
    assert all(p['capacity']['image_count'] <= 1 for p in plan['parts'])


def test_indivisible_overlong_input_is_held_without_truncation(tmp_path):
    source = tmp_path / 'paper.txt'
    source.write_text('摘要\n$' + 'a'*25000 + '$', encoding='utf8')
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    body = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path/'material', catalog)
    with pytest.raises(ValueError, match='不可再分'):
        plan_clean_material(body['material_path'], '模板', catalog, config(14000), tmp_path/'plan')
    assert not (tmp_path/'plan'/'offline-plan.json').exists()


def test_request_offsets_map_to_recorded_original_parsed_view(tmp_path):
    from local_prepare import map_request_range
    source = tmp_path / 'paper.docx'
    doc = Document()
    doc.add_heading('摘要', 1)
    paragraph = doc.add_paragraph('甲  ')
    paragraph.add_run().add_break()
    paragraph.add_run(' 乙')
    doc.save(source)
    catalog = tmp_path / 'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    body = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path/'material', catalog)
    plan = plan_clean_material(body['material_path'], '模板', catalog, config(200000), tmp_path/'plan')['plan']
    part = plan['parts'][0]
    row = next(b for b in part['bundle']['blocks'] if b['text'] == '甲乙')
    mapped = map_request_range(body['material_path'], part, row['id'], 0, 2)
    assert mapped['source_view']['basis'] == 'direct_original'
    assert ''.join(s['source_text'] for s in mapped['spans']) == '甲乙'
    assert len(mapped['spans']) == 2


def test_split_request_offsets_map_from_saved_plan_to_original_text(tmp_path):
    from local_prepare import map_request_range
    source = tmp_path/'paper.docx'
    doc = Document()
    doc.add_heading('摘要', 1)
    doc.add_paragraph('研究方法与实验结果。' * 1000)
    doc.save(source)
    catalog = tmp_path/'catalog.json'
    catalog.write_text('{"majors": []}', encoding='utf8')
    body = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path/'material', catalog)
    result = plan_clean_material(body['material_path'], '模板', catalog, config(14000), tmp_path/'plan')
    assert result['plan']['mode'] == 'partitioned'
    saved = json.loads(Path(result['plan_path']).read_text(encoding='utf8'))
    for part in saved['parts']:
        view = json.loads(part['body']['messages'][1]['content'][0]['text'])['material']
        candidates = [b for b in view['blocks'] if (b.get('source_span') or {}).get('start',0)>0
                      and b.get('reading_role') == 'primary' and len(b.get('text',''))>=4]
        if candidates:
            row = candidates[0]
            mapped = map_request_range(body['material_path'], part, row['id'], 0, 4)
            assert ''.join(s['source_text'] for s in mapped['spans']) == row['text'][:4]
            assert mapped['spans'][0]['source_start'] > 0
            break
    else:
        pytest.fail('no offset fragment appeared in the saved plan')


def test_image_uses_actual_channel_limit_without_unnecessary_downscale(tmp_path):
    import hashlib
    from PIL import Image
    from local_prepare import _native_paths
    _native_paths()
    from preextract_images import prepare_request_images
    image_path = tmp_path / 'large.png'
    Image.new('RGB', (2400, 50), 'white').save(image_path)
    asset = {'id': 'figure', 'image_path': str(image_path),
             'sha256': hashlib.sha256(image_path.read_bytes()).hexdigest(),
             'width': 2400, 'height': 50}
    result, report = prepare_request_images({'assets': [asset]}, tmp_path / 'plan',
        {'max_image_side': 3000, 'max_single_image_bytes': 8 * 1024 * 1024})
    assert result['assets'][0]['image_path'] == str(image_path)
    assert report['adjusted_count'] == 0
    assert report['max_side'] == 3000


def test_located_figure_is_planned_with_the_body(tmp_path, monkeypatch):
    import visual_ocr
    monkeypatch.setattr(visual_ocr,'read_image',lambda path:[{'text':'参考文献','confidence':.9}])
    source=tmp_path/'body.docx';picture=tmp_path/'picture.png'
    Image.new('RGB',(200,200),'blue').save(picture)
    doc=Document();doc.add_heading('摘要',1);doc.add_paragraph('研究正文。');doc.add_picture(str(picture));doc.save(source)
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    body=prepare_source(source,hashlib.sha256(source.read_bytes()).hexdigest(),tmp_path/'material',catalog)
    material=json.loads(Path(body['material_path']).read_text(encoding='utf8'))
    assert not any(r['kind']=='visual_scope' for r in material['readiness']['items'])
    planned=plan_clean_material(body['material_path'],'模板',catalog,config(200000),tmp_path/'plan')
    assert any(part['asset_ids'] for part in planned['plan']['parts'])


def test_review_must_match_every_pending_item_and_the_exact_material(tmp_path, monkeypatch):
    import visual_ocr
    monkeypatch.setattr(visual_ocr,'read_image',lambda path:[{'text':'参考文献','confidence':.9}])
    source=tmp_path/'body.docx';picture=tmp_path/'picture.png'
    Image.new('RGB',(200,200),'blue').save(picture)
    doc=Document();doc.add_heading('第一章 绪论',1);doc.add_paragraph('研究背景与方法。' * 20);doc.add_picture(str(picture));doc.save(source)
    catalog=tmp_path/'catalog.json';catalog.write_text('{"majors": []}',encoding='utf8')
    body=prepare_source(source,hashlib.sha256(source.read_bytes()).hexdigest(),tmp_path/'material',catalog)
    review=synthetic_review(body['material_path'],tmp_path/'review.json')
    value=json.loads(review.read_text(encoding='utf8'));value['decisions']=[]
    review.write_text(json.dumps(value),encoding='utf8')
    with pytest.raises(ValueError,match='待核'):
        plan_clean_material(body['material_path'],'模板',catalog,config(200000),tmp_path/'blocked',review_path=review)
    review=synthetic_review(body['material_path'],review)
    planned=plan_clean_material(body['material_path'],'模板',catalog,config(200000),tmp_path/'approved',review_path=review)
    saved=json.loads(Path(planned['plan_path']).read_text(encoding='utf8'))
    assert saved['review_sha256']==hashlib.sha256(review.read_bytes()).hexdigest()


def test_review_template_is_blank_bound_and_cannot_approve_material(tmp_path, monkeypatch):
    from local_prepare import write_review_template
    import visual_ocr
    monkeypatch.setattr(visual_ocr, 'read_image', lambda path: [{'text':'参考文献','confidence':.9}])
    picture = tmp_path / 'picture.png'
    Image.new('RGB', (200, 200), 'blue').save(picture)
    source = tmp_path / 'body.docx'
    doc = Document(); doc.add_heading('第一章 绪论', 1); doc.add_paragraph('研究背景与方法。' * 20); doc.add_picture(str(picture)); doc.save(source)
    catalog = tmp_path / 'catalog.json'; catalog.write_text('{"majors": []}', encoding='utf8')
    body = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path/'material', catalog)
    target = tmp_path/'review-template.json'
    write_review_template(body['material_path'], target)
    review = json.loads(target.read_text(encoding='utf8'))
    assert review['reviewer'] == ''
    assert review['material_sha256'] == hashlib.sha256(Path(body['material_path']).read_bytes()).hexdigest()
    assert review['decisions'] and all(not item['decision'] and not item['evidence'] for item in review['decisions'])
    with pytest.raises(ValueError, match='待核'):
        plan_clean_material(body['material_path'], '模板', catalog, config(200000), tmp_path/'plan', review_path=target)
    with pytest.raises(FileExistsError):
        write_review_template(body['material_path'], target)


def test_clear_first_chapter_without_abstract_is_recorded_as_warning(tmp_path):
    source = tmp_path/'body.docx'
    doc = Document(); doc.add_heading('第一章 绪论', 1)
    doc.add_paragraph('研究正文及研究背景。' * 30); doc.save(source)
    catalog = tmp_path/'catalog.json'; catalog.write_text('{"majors": []}', encoding='utf8')
    body = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path/'material', catalog)
    material = json.loads(Path(body['material_path']).read_text(encoding='utf8'))
    assert any(item['code'] == 'abstract_missing' for item in material['issues'])
    result=plan_clean_material(body['material_path'], '模板', catalog, config(200000), tmp_path/'plan')
    assert result['plan']['parts']


@pytest.mark.parametrize('inline_image', [
    '![图](data:image/png;base64,QUJDREVGRw==)',
    '<img src="data:image/png;base64,QUJDREVGRw==">',
    '![图](https://example.invalid/private.png)',
])
def test_text_embedded_images_stay_private_and_need_explicit_review(tmp_path, inline_image):
    source = tmp_path/'paper.md'
    source.write_text('摘要\n完整的正文在此。\n图片相关段落：' + inline_image + '\n参考文献\n文末文献', encoding='utf8')
    catalog = tmp_path/'catalog.json'; catalog.write_text('{"majors": []}', encoding='utf8')
    body = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path/'material', catalog)
    material = json.loads(Path(body['material_path']).read_text(encoding='utf8'))
    assert material['readiness']['state'] == 'review_required'
    assert any(item['kind'] == 'unresolved_visual' for item in material['readiness']['items'])
    assert 'QUJDREVGRw' not in '\n'.join(block['text'] for block in material['blocks'])
    assert 'example.invalid' not in '\n'.join(block['text'] for block in material['blocks'])
    partial=plan_clean_material(body['material_path'], '模板', catalog, config(200000), tmp_path/'plan')['plan']
    partial_view=json.dumps(partial['parts'],ensure_ascii=False)
    assert partial['source_completeness']=='partial'
    assert '完整的正文在此' in partial_view
    assert 'QUJDREVGRw' not in partial_view and 'example.invalid' not in partial_view
    # Synthetic fixture review covers the entire affected paragraph, not just the image.
    review = synthetic_review(body['material_path'], tmp_path/'review.json')
    checked = json.loads(review.read_text(encoding='utf8'))
    for item in checked['decisions']:
        item['decision'] = 'not_research'
    review.write_text(json.dumps(checked, ensure_ascii=False), encoding='utf8')
    plan = plan_clean_material(body['material_path'], '模板', catalog, config(200000),
                               tmp_path/'reviewed-plan', review_path=review)['plan']
    projected = json.dumps(plan['parts'], ensure_ascii=False)
    assert 'QUJDREVGRw' not in projected and 'example.invalid' not in projected
    assert '完整的正文在此' in projected


@pytest.mark.parametrize('field', ['text', 'continuation_text'])
def test_table_embedded_image_is_masked_without_dropping_safe_cells(tmp_path, monkeypatch, field):
    from local_prepare import _native_paths
    _native_paths()
    import native_docx
    original_parse = native_docx.parse_docx
    def parse(*args):
        raw, images, formulas = original_parse(*args)
        table = next(block for block in raw if block.get('type') == 'table')
        table['cells'][1][field] = '![图](data:image/png;base64,QUJDREVGRw==)'
        return raw, images, formulas
    monkeypatch.setattr(native_docx, 'parse_docx', parse)
    source = tmp_path/'paper.docx'
    doc = Document(); doc.add_heading('摘要', 1); doc.add_paragraph('研究正文。')
    table = doc.add_table(rows=1, cols=2); table.cell(0, 0).text = '安全数据 42'; table.cell(0, 1).text = '图形'
    doc.save(source)
    catalog = tmp_path/'catalog.json'; catalog.write_text('{"majors": []}', encoding='utf8')
    body = prepare_source(source, hashlib.sha256(source.read_bytes()).hexdigest(), tmp_path/'material', catalog)
    material = json.loads(Path(body['material_path']).read_text(encoding='utf8'))
    assert material['readiness']['state'] == 'review_required'
    assert any(item['kind'] == 'unresolved_visual' for item in material['readiness']['items'])
    asset = next(a for a in material['assets'] if a['kind'] == 'table')
    assert '安全数据 42' in asset['text']
    assert 'QUJDREVGRw' not in json.dumps(material['blocks'], ensure_ascii=False)
    assert 'QUJDREVGRw' not in json.dumps(asset, ensure_ascii=False)
    partial=plan_clean_material(body['material_path'], '模板', catalog, config(200000), tmp_path/'plan')['plan']
    assert partial['source_completeness']=='partial'
    assert 'QUJDREVGRw' not in json.dumps(partial['parts'],ensure_ascii=False)


def test_small_image_channel_uses_tiles_without_changing_original(tmp_path):
    from local_prepare import _native_paths
    _native_paths()
    from preextract_images import prepare_request_images
    image_path=tmp_path/'figure.png';Image.new('RGB',(2400,1200),'white').save(image_path)
    digest=hashlib.sha256(image_path.read_bytes()).hexdigest()
    asset={'id':'fig','image_path':str(image_path),'sha256':digest,'width':2400,'height':1200}
    adapted,report=prepare_request_images({'assets':[asset]},tmp_path/'plan',
                                          {'max_image_side':128,'max_single_image_bytes':2*1024*1024})
    assert report['images'][0]['tiled'] and len(adapted['assets'])>1
    assert all(max(a['width'],a['height'])<=128 for a in adapted['assets'])
    assert hashlib.sha256(image_path.read_bytes()).hexdigest()==digest


def test_invalid_source_mapping_is_rejected_before_plan():
    from local_prepare import verify_source_spans
    source={'a':{'id':'a','text':'甲','position':{'page':1}}}
    with pytest.raises(ValueError,match='映射'):
        verify_source_spans(source,[{'source_block_id':'a','source_start':0,'source_end':2,'position':{'page':1}}])
