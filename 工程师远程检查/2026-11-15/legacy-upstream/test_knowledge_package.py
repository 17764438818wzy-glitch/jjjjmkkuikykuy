import copy
import csv
import hashlib
import json
from pathlib import Path

import pytest
from PIL import Image
from evidence import export_package, package_fingerprint


def sample(package):
    (package/'assets').mkdir(parents=True)
    Image.new('RGB',(120,80),'navy').save(package/'assets/original.png')
    cells=[{'row':0,'column':0,'rowspan':1,'colspan':3,'text':'经营数据'},
           *[{'row':1,'column':i,'rowspan':1,'colspan':1,'text':s} for i,s in enumerate(['年份','营业收入','净利润'])],
           *[{'row':2,'column':i,'rowspan':1,'colspan':1,'text':s} for i,s in enumerate(['2022','80.60','5.20'])],
           *[{'row':3,'column':i,'rowspan':1,'colspan':1,'text':s} for i,s in enumerate(['2023','91.30',''])]]
    table={'id':'t1','type':'table','position':{'body_index':4},'cells':cells,'grid':[[0,0,0],[1,2,3],[4,5,6],[7,8,9]],
           'rows':4,'columns':3,'header_rows':[0,1],'header_verified':True,'complex':True,'caption':'表3-1 经营数据',
           'units':'亿元','notes':['数据来源：原论文给出的年报数据'],'source':'native_docx','grid_validated':True}
    native=copy.deepcopy(table);native.update(id='chartdata',position={'body_index':5,'object_ref':'chart_1'},
        caption='图3-1 收入趋势',source='native_chart_cache',native_chart={
        'series':[{'name':'营业收入','categories':['2022','2023'],'values':['80.60','91.30'],'format_code':'0.00','category_format':''}],
        'chart_spec':{'source':'native_ooxml','chart_types':['lineChart'],'plots':[{'type':'lineChart','series_indices':[0],'axis_ids':[]}],'axes':[]}})
    image={'id':'image1','type':'image','position':{'body_index':5,'object_ref':'chart_1'},'resource_id':'r1','caption':'图3-1 收入趋势',
           'inline':False,'ocr':{'verified':True,'text':'2022 80.60；2023 91.30','source':'ai_visual_transcription'},
           'description':{'text':'收入趋势图','source':'generated'}}
    return {'id':'AUTO_test','title':'企业盈利能力研究','major':'会计学','category':'经管财会类','source_sha256':'a'*64,
        'original_source_sha256':'b'*64,'preprocessing_complete':True,'text_privacy_complete':True,'privacy_status':'ai_passed',
        'automatic_approval':True,'blocks':[
            {'id':'h1','type':'paragraph','text':'第三章 盈利能力分析','heading_level':1,'position':{'body_index':1}},
            {'id':'h2','type':'paragraph','text':'3.1 盈利规模','heading_level':2,'position':{'body_index':2}},
            {'id':'p1','type':'paragraph','text':'如表3-1和图3-1所示，本文比较收入与利润。','position':{'body_index':3}},
            table,native,image,
            {'id':'p2','type':'paragraph','text':'收入增加；公式 $y=x$ 用于解释分析口径。','position':{'body_index':6}},
            {'id':'h3','type':'paragraph','text':'3.2 经营效率','heading_level':2,'position':{'body_index':7}},
            {'id':'p3','type':'paragraph','text':'这一节的完整分析。','position':{'body_index':8}}],
        'resources':[{'id':'r1','local_path':'assets/original.png','kind':'image','privacy_status':'approved','position':image['position'],
                      'sha256':hashlib.sha256((package/'assets/original.png').read_bytes()).hexdigest()}],
        'formulas':[{'latex':'y=x','omml':'<math><text>y=x</text></math>','position':{'body_index':6}}],'issues':[]}


def read(package,path):return json.loads((package/path).read_text(encoding='utf-8'))


def exported(package,meta):
    export_package(package,meta)
    assert (package/'manifest.json').is_file(), 'Final preprocessing must export an AI-readable manifest'
    return read(package,'manifest.json')


def test_complete_writing_unit_preserves_order_table_lexemes_and_original_image(tmp_path):
    meta=sample(tmp_path);before=copy.deepcopy(meta['blocks']);manifest=exported(tmp_path,meta)
    assert manifest['paper_id']=='paper_'+('b'*24)
    table_entry=next(t for t in manifest['tables'] if t['source_block_id']=='t1')
    table=read(tmp_path,table_entry['path'])
    assert table['cells']==before[3]['cells'] and table['grid']==[[0,0,0],[1,2,3],[4,5,6],[7,8,9]]
    assert table['dataset']['records'][0]['c1']=='80.60' and table['dataset']['records'][1]['c2']==''
    with (tmp_path/table['csv_path']).open(encoding='utf-8-sig',newline='') as f:rows=list(csv.reader(f))
    assert rows[-2:]==[['2022','80.60','5.20'],['2023','91.30','']]
    assert table['preview']['source']=='reconstructed_cell_grid' and (tmp_path/table['preview']['path']).is_file()
    unit_entry=next(u for u in manifest['writing_units'] if u['title']=='3.1 盈利规模')
    unit=read(tmp_path,unit_entry['path'])
    assert [b['source_block_id'] for b in unit['content']]==['h2','p1','t1','chartdata','image1','p2']
    assert '这一节的完整分析' not in json.dumps(unit,ensure_ascii=False)
    assert len(unit['equation_ids'])==1
    fig=read(tmp_path,manifest['figures'][0]['path'])
    assert fig['image']['path']=='assets/original.png'
    assert (tmp_path/fig['image']['path']).read_bytes()==(tmp_path/'assets/original.png').read_bytes()
    assert meta['blocks']==before, 'Export must not rewrite source blocks'
    assert '3.1 盈利规模' in (tmp_path/'ingest.md').read_text(encoding='utf-8')
    assert unit_entry['id'] in (tmp_path/'ingest.md').read_text(encoding='utf-8')


def test_ingest_contains_machine_readable_chart_and_table_recipe_for_rag(tmp_path):
    manifest = exported(tmp_path, sample(tmp_path))
    ingest = (tmp_path/'ingest.md').read_text(encoding='utf-8')
    assets = (tmp_path/'ragflow_assets.md').read_text(encoding='utf-8')
    def handoff(text):
        encoded=text.split('[PAPER_PIPELINE_METADATA_V1]',1)[1].split('[/PAPER_PIPELINE_METADATA_V1]',1)[0].strip()
        return json.loads(encoded)
    for value in (handoff(ingest),handoff(assets)):
        assert value['stable_paper_id']==manifest['paper_id']
        assert value['paper_id']==value['document_id']==manifest['document_id']
    assert '图表/表格写作参考结构' in ingest
    assert '[PAPER_PIPELINE_METADATA_V1]' in ingest[:1200]
    assert manifest['figures'][0]['id'] in ingest
    assert 'chart_spec' in ingest and '营业收入' in ingest
    assert manifest['figures'][0]['id'] in assets
    assert '80.60' in assets and 'related_table_ids' in assets
    assert '[PAPER_PIPELINE_METADATA_V1]' in assets


def test_education_inventory_rejects_mismatched_stable_paper_id_without_changing_legacy_read(tmp_path):
    from knowledge_package import knowledge_files
    manifest=exported(tmp_path,sample(tmp_path))
    path=tmp_path/'ingest.md'
    text=path.read_text(encoding='utf-8')
    start='[PAPER_PIPELINE_METADATA_V1]'
    end='[/PAPER_PIPELINE_METADATA_V1]'
    before,rest=text.split(start,1)
    encoded,after=rest.split(end,1)
    handoff=json.loads(encoded)
    handoff['stable_paper_id']='paper_'+('a'*24)
    path.write_text(before+start+'\n'+json.dumps(handoff,ensure_ascii=False)+'\n'+end+after,encoding='utf-8')
    next(row for row in manifest['files'] if row['path']=='ingest.md')['sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
    manifest.pop('revision')
    manifest['revision']=hashlib.sha256(json.dumps(manifest,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    (tmp_path/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False),encoding='utf-8')
    assert 'ingest.md' in knowledge_files(tmp_path)
    with pytest.raises(ValueError,match='稳定论文编号'):
        knowledge_files(tmp_path,require_stable_paper_id=True)


def test_education_inventory_rejects_legacy_handoff_paper_id_even_with_matching_stable_id(tmp_path):
    from knowledge_package import knowledge_files
    exported(tmp_path,sample(tmp_path))
    assert 'ingest.md' in knowledge_files(tmp_path)
    with pytest.raises(ValueError,match='稳定论文编号'):
        knowledge_files(tmp_path,require_stable_paper_id=True)


def test_education_preprocessing_exports_canonical_paper_id_by_default(tmp_path):
    from knowledge_package import knowledge_files
    meta=sample(tmp_path)
    meta['category']='教育教学类'
    meta['major']='小学教育'
    meta['classification']={'major':'小学教育','method':'original_rules','source':'front_matter','confidence':'high'}
    export_package(tmp_path,meta)
    assert 'ingest.md' in knowledge_files(tmp_path,require_stable_paper_id=True)


def test_large_gap_details_do_not_overflow_handoff_header_and_are_preserved(tmp_path):
    from knowledge_package import knowledge_files
    meta=sample(tmp_path)
    meta.update(category='教育教学类',major='小学教育')
    meta['unrecovered_objects']=[{'id':f'object_{n}','reason':'详细可见性记录'*100} for n in range(13)]
    expected=copy.deepcopy(meta['unrecovered_objects'])
    export_package(tmp_path,meta)
    assert 'ingest.md' in knowledge_files(tmp_path,require_stable_paper_id=True)
    assets=(tmp_path/'ragflow_assets.md').read_text('utf-8')
    raw=assets.split('[PAPER_PIPELINE_METADATA_V1]',1)[1].split('[/PAPER_PIPELINE_METADATA_V1]',1)[0]
    header=json.loads(raw)
    assert len(raw)<8192
    assert header['completeness_status']=='partial'
    assert header['unrecovered_object_count']==13
    assert read(tmp_path,'manifest.json')['unrecovered_objects']==expected
    assert read(tmp_path,'metadata.json')['unrecovered_objects']==expected


def test_education_inventory_checks_asset_handoff_identity_too(tmp_path):
    from knowledge_package import knowledge_files
    export_package(tmp_path,sample(tmp_path),canonical_paper_id=True)
    path=tmp_path/'ragflow_assets.md'
    text=path.read_text(encoding='utf-8')
    start='[PAPER_PIPELINE_METADATA_V1]';end='[/PAPER_PIPELINE_METADATA_V1]'
    before,rest=text.split(start,1);encoded,after=rest.split(end,1)
    handoff=json.loads(encoded);handoff['paper_id']=handoff['document_id']
    path.write_text(before+start+json.dumps(handoff,ensure_ascii=False)+end+after,encoding='utf-8')
    manifest=read(tmp_path,'manifest.json')
    next(row for row in manifest['files'] if row['path']=='ragflow_assets.md')['sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
    manifest.pop('revision')
    manifest['revision']=hashlib.sha256(json.dumps(manifest,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    (tmp_path/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False),encoding='utf-8')
    with pytest.raises(ValueError,match='稳定论文编号'):
        knowledge_files(tmp_path,require_stable_paper_id=True)


def test_image_projection_contains_search_fields_and_manifest_roles(tmp_path):
    manifest = exported(tmp_path, sample(tmp_path))
    projection = (tmp_path/'ragflow_images.md').read_text(encoding='utf-8')
    assert 'document_id: AUTO_test' in projection
    assert 'paper_id: paper_' + ('b' * 24) in projection
    assert 'figure_id: figure_' in projection
    assert 'caption: 图3-1 收入趋势' in projection
    assert 'section_id: section_' in projection
    assert 'section_no: 3.1' in projection
    assert 'section_title: 3.1 盈利规模' in projection
    assert 'chart_type:' in projection
    assert 'chart_spec:' in projection
    assert 'description_text: "收入趋势图"' in projection
    assert 'recognized_text: "2022 80.60；2023 91.30"' in projection
    assert 'image_file: original.png' in projection
    roles = {row['path']: row.get('role') for row in manifest['files']}
    assert roles['ingest.md'] == 'paper_primary'
    assert roles['ragflow_assets.md'] == 'paper_assets'
    assert roles['ragflow_images.md'] == 'image_projection'
    assert roles['assets/original.png'] == 'image'


@pytest.mark.parametrize(('chart_spec','expected_type'), [
    ({'source':'ai_visual_observation','chart_type':'flowchart','series':[]}, 'flowchart'),
    ({'source':'native_ooxml','chart_types':['lineChart'],'plots':[]}, 'lineChart'),
])
def test_image_projection_supports_both_chart_spec_schemas(tmp_path, chart_spec, expected_type):
    meta=sample(tmp_path)
    meta['blocks']=[block for block in meta['blocks'] if block['id']!='chartdata']
    next(block for block in meta['blocks'] if block['id']=='image1')['chart_spec']=chart_spec
    manifest=exported(tmp_path,meta)
    projection=(tmp_path/'ragflow_images.md').read_text(encoding='utf-8')
    assert f'chart_type: {expected_type}' in projection
    assert 'data_status: not_available' in projection
    assert 'related_table_ids: []' in projection


def test_native_figure_and_table_share_one_dataset_but_nearby_table_does_not(tmp_path):
    manifest=exported(tmp_path,sample(tmp_path))
    tables={t['source_block_id']:read(tmp_path,t['path']) for t in manifest['tables']}
    fig=read(tmp_path,manifest['figures'][0]['path'])
    assert fig['chart_spec']['dataset_id']==tables['chartdata']['dataset']['id']
    assert fig['related_table_ids']==[tables['chartdata']['id']]
    assert tables['t1']['id'] not in fig['related_table_ids']
    assert fig['chart_spec']['source']=='native_ooxml'
    assert tables['chartdata']['dataset']['series'][0]['values']==['80.60','91.30']
    relations=read(tmp_path,'metadata/relations.json')['relations']
    assert sum(r['type']=='explicit_reference' for r in relations)==2
    assert any(r['type']=='native_object_data' for r in relations)


def test_ids_and_exports_stay_stable_and_fingerprint_covers_new_files(tmp_path):
    meta=sample(tmp_path);first=exported(tmp_path,meta);stamp=package_fingerprint(tmp_path)
    second=exported(tmp_path,meta)
    assert first==second and stamp==package_fingerprint(tmp_path)
    table=tmp_path/first['tables'][0]['path'];table.write_text('{}',encoding='utf-8')
    with pytest.raises(ValueError):package_fingerprint(tmp_path)


def test_private_or_missing_asset_cannot_be_published(tmp_path):
    meta=sample(tmp_path);meta['resources'][0]['privacy_status']='pending'
    with pytest.raises(ValueError):export_package(tmp_path,meta)
    assert not (tmp_path/'manifest.json').exists()


def test_pending_text_privacy_keeps_legacy_exports_without_knowledge_manifest(tmp_path):
    meta=sample(tmp_path);meta['text_privacy_complete']=False
    export_package(tmp_path,meta)
    assert (tmp_path/'ingest.md').exists() and not (tmp_path/'manifest.json').exists()


def test_visual_only_figure_is_useful_without_fabricating_underlying_data(tmp_path):
    meta=sample(tmp_path);meta['blocks']=[b for b in meta['blocks'] if b['id']!='chartdata']
    manifest=exported(tmp_path,meta);fig=read(tmp_path,manifest['figures'][0]['path'])
    assert fig['related_table_ids']==[] and fig['chart_spec']['dataset_id'] is None
    assert fig['data_status']=='not_available' and fig['image']['path']=='assets/original.png'


def test_ambiguous_caption_is_not_linked_to_arbitrary_table(tmp_path):
    meta=sample(tmp_path);duplicate=copy.deepcopy(meta['blocks'][3]);duplicate['id']='t_duplicate';duplicate['position']={'body_index':9}
    meta['blocks'].append(duplicate)
    exported(tmp_path,meta);relations=read(tmp_path,'metadata/relations.json')['relations']
    assert any(r['type']=='unresolved_reference' and r['reference']=='表3-1' for r in relations)
    assert not any(r['type']=='explicit_reference' and r['reference']=='表3-1' for r in relations)


def test_manifest_escaping_package_is_rejected(tmp_path):
    from evidence import package_fingerprint
    manifest=exported(tmp_path,sample(tmp_path))
    manifest['files'].append({'path':'../outside.txt','sha256':'0'*64})
    (tmp_path/'manifest.json').write_text(json.dumps(manifest),encoding='utf-8')
    with pytest.raises(ValueError):package_fingerprint(tmp_path)


def test_inline_image_keeps_owning_section_when_parser_lists_resource_at_end(tmp_path):
    meta=sample(tmp_path);picture=meta['blocks'].pop(5);picture['inline']=True
    meta['blocks'][2]['text']+=' [[IMAGE:image1]]';meta['blocks'].append(picture)
    manifest=exported(tmp_path,meta)
    first=next(u for u in manifest['writing_units'] if u['title']=='3.1 盈利规模')
    second=next(u for u in manifest['writing_units'] if u['title']=='3.2 经营效率')
    figure=read(tmp_path,manifest['figures'][0]['path'])
    assert figure['section_id']==first['id']
    assert figure['id'] in read(tmp_path,first['path'])['figure_ids']
    assert figure['id'] not in read(tmp_path,second['path'])['figure_ids']
    assert 'image1' not in [b['source_block_id'] for b in read(tmp_path,second['path'])['content']]


def test_inline_screenshot_table_belongs_to_every_writing_unit_that_embeds_it(tmp_path):
    meta=sample(tmp_path);meta['blocks']=[b for b in meta['blocks'] if b['id']!='chartdata']
    picture=next(b for b in meta['blocks'] if b['id']=='image1');meta['blocks'].remove(picture);picture['inline']=True
    picture['structured_table']={'rows':[['年份','收入'],['2023','91.30']],'header_rows':[0],'units':'亿元','notes':[]}
    meta['blocks'][2]['text']+=' [[IMAGE:image1]]'
    meta['blocks'][-1]['text']+=' [[IMAGE:image1]]'
    meta['blocks'].append(picture)
    manifest=exported(tmp_path,meta)
    visual_table=next(t for t in manifest['tables'] if read(tmp_path,t['path'])['source']=='ai_visual_transcription')
    owners=[next(u for u in manifest['writing_units'] if u['title']==title) for title in ['3.1 盈利规模','3.2 经营效率']]
    for owner in owners:
        unit=read(tmp_path,owner['path'])
        assert visual_table['id'] in unit['table_ids']
        assert unit['objects'][visual_table['id']]['path']==visual_table['path']


def test_writing_unit_markdown_keeps_table_rows_contiguous(tmp_path):
    manifest=exported(tmp_path,sample(tmp_path))
    unit=next(u for u in manifest['writing_units'] if u['title']=='3.1 盈利规模')
    markdown=(tmp_path/unit['markdown_path']).read_text(encoding='utf-8')
    assert '| 原第1列 | 原第2列 | 原第3列 |\n| --- | --- | --- |\n| 经营数据 | 经营数据 | 经营数据 |' in markdown
    assert '| 原第1列 | 原第2列 | 原第3列 |\n\n| --- | --- | --- |' not in markdown


def test_txt_line_position_is_preserved_without_source_path(tmp_path):
    meta=sample(tmp_path)
    meta['blocks'].append({'id':'txt_line','type':'paragraph','text':'TXT 原文行',
                           'position':{'line':42,'source_path':r'C:\private\paper.txt'}})
    exported(tmp_path,meta)
    block=next(b for b in read(tmp_path,'metadata/blocks.json')['blocks'] if b['source_block_id']=='txt_line')
    assert block['position']=={'line':42}


def test_docx_footnote_position_attaches_its_equation_to_the_writing_unit(tmp_path):
    meta=sample(tmp_path)
    meta['blocks'].append({'id':'footnote_7','type':'paragraph','text':'脚注公式 z=1',
                           'position':{'part':'word/footnotes.xml','id':'7'}})
    meta['formulas'].append({'latex':'z=1','omml':'<math><text>z=1</text></math>',
                             'position':{'part':'word/footnotes.xml','id':'7'}})
    manifest=exported(tmp_path,meta)
    block=next(b for b in read(tmp_path,'metadata/blocks.json')['blocks'] if b['source_block_id']=='footnote_7')
    equation=next(read(tmp_path,e['path']) for e in manifest['equations'] if read(tmp_path,e['path'])['expression']=='z=1')
    unit=next(read(tmp_path,u['path']) for u in manifest['writing_units'] if u['id']==block['section_id'])
    assert block['position']=={'part':'word/footnotes.xml','id':'7'}
    assert equation['block_ids']==[block['id']]
    assert equation['id'] in unit['equation_ids']


def test_analytical_sentences_starting_with_object_numbers_are_references_not_captions(tmp_path):
    meta=sample(tmp_path)
    meta['blocks'].insert(3,{'id':'caption','type':'paragraph','text':'图3-1 收入趋势','position':{'body_index':3.1}})
    meta['blocks'].insert(4,{'id':'analysis','type':'paragraph','text':'图3-1显示营业收入逐年增加。','position':{'body_index':3.2}})
    meta['blocks'].insert(5,{'id':'unresolved_analysis','type':'paragraph','text':'表9-9显示样本差异明显。','position':{'body_index':3.3}})
    exported(tmp_path,meta)
    blocks={b['source_block_id']:b for b in read(tmp_path,'metadata/blocks.json')['blocks']}
    relations=read(tmp_path,'metadata/relations.json')['relations']
    assert not any(r.get('from_id')==blocks['caption']['id'] for r in relations)
    assert any(r['type']=='explicit_reference' and r.get('from_id')==blocks['analysis']['id'] and r['reference']=='图3-1'
               for r in relations)
    assert any(r['type']=='unresolved_reference' and r.get('from_id')==blocks['unresolved_analysis']['id'] and
               r['reference']=='表9-9' and r['to_id'] is None for r in relations)


def test_export_infers_conservative_headings_without_turning_year_data_into_a_section(tmp_path):
    meta=sample(tmp_path)
    meta['blocks'][0]['heading_level']=0
    meta['blocks'][1]['heading_level']=0
    meta['blocks'].insert(2,{'id':'year_data','type':'paragraph','text':'2023.1 营业收入为91.30亿元',
                             'heading_level':0,'position':{'line':3}})
    manifest=exported(tmp_path,meta)
    titles=[u['title'] for u in manifest['writing_units']]
    assert '第三章 盈利能力分析' in titles
    assert '3.1 盈利规模' in titles
    assert '2023.1 营业收入为91.30亿元' not in titles
    subsection=next(read(tmp_path,u['path']) for u in manifest['writing_units'] if u['title']=='3.1 盈利规模')
    assert 'year_data' in [b['source_block_id'] for b in subsection['content']]
