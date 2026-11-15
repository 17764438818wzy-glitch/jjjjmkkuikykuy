import importlib
import os
import uuid

import psycopg
import pytest


@pytest.fixture
def catalog():
    assert importlib.util.find_spec('article_repository.catalog'), 'persistent catalog is not implemented'
    from article_repository.catalog import Catalog
    dsn=os.environ.get('ARTICLE_TEST_DSN','host=127.0.0.1 port=56164 dbname=postgres user=bench_owner')
    schema='article_test_'+uuid.uuid4().hex
    with psycopg.connect(dsn,autocommit=True) as conn:
        assert 'pg-classification-isolated-20261103' in conn.execute('SHOW data_directory').fetchone()[0]
        conn.execute('CREATE SCHEMA '+schema)
    cat=Catalog(dsn,schema=schema)
    cat.initialize()
    cat.set_majors([{'code':'01','name':'音乐学','paper_dataset_id':'body1','image_dataset_id':'asset1'},
                    {'code':'02','name':'音乐教育','paper_dataset_id':'body2','image_dataset_id':'asset2'}])
    yield cat
    with psycopg.connect(dsn,autocommit=True) as conn:
        conn.execute('DROP SCHEMA '+schema+' CASCADE')


def record(folder='one',title='音乐研究'):
    return {'source_key':'folder:'+folder,'folder_name':folder,'title':title,
            'majors':['01'],'profile':{'article':{'summary':'摘要','keywords':['教学']},
            'applicable_level':{'labels':['本科'],'applicable_audience':['音乐教师']},
            'research_profile':{'object':'课堂','overall_path':'访谈后分析'},
            'learning_units':[{'method':'访谈法','what_to_learn':'访谈设计'}]},
            'files':[],'inventory_complete':False,'qdrant':{},'source':'legacy_catalog'}


def test_equal_body_in_different_folders_keeps_two_rows(catalog):
    a=record(); a['source_sha256']='a'*64
    b=record('two'); b['source_sha256']='a'*64
    catalog.register(a); catalog.register(b); catalog.register(a)
    assert catalog.search()['total']==2


def test_search_combines_exact_major_and_title_only(catalog):
    catalog.register(record('专业关键词','钢琴音乐研究'))
    b=record('two','音乐研究'); b['majors']=['02']; catalog.register(b)
    assert catalog.search(keyword='钢琴',major='01')['total']==1
    assert catalog.search(keyword='专业关键词')['total']==0
    assert catalog.search(major='0')['total']==0


def test_manual_fields_win_after_reprocessing(catalog):
    item=catalog.register(record())
    edited=catalog.edit(item['id'],item['revision'],{'title':'手改标题','fields':{'audience':'新的适用人群'}})
    updated=record(title='重新生成标题'); updated['profile']['article']['summary']='更新摘要'
    catalog.register(updated)
    saved=catalog.detail(item['id'])
    assert saved['title']=='手改标题'
    assert saved['fields']['audience']=='新的适用人群'
    assert saved['fields']['summary']=='更新摘要'
    with pytest.raises(ValueError,match='版本'):
        catalog.edit(item['id'],edited['revision']-1,{'title':'过期修改'})


def test_unknown_files_still_list_and_profile_is_not_recreated(catalog):
    item=catalog.register(record())
    assert item['fields']['methods']=='访谈法'
    assert item['fields']['level']=='本科'
    assert item['fields']['audience']=='音乐教师'
    assert item['availability']=='unknown'
    empty=record('empty'); empty['profile']={}
    item=catalog.register(empty)
    assert item['fields']['methods']==''
    assert catalog.search()['total']==2


def test_retained_partial_profiles_are_explicit_without_fabricating_aggregate(catalog):
    row=record('retained');row['profile']={'format_status':'unstructured_retained',
        'article':{},'retained_profiles':[{'profile':{'article':{'summary':'仅分段摘要'}}}]}
    item=catalog.register(row)
    assert item['structure_status']=='unstructured_retained'
    assert item['fields']['summary']=='' and item['fields']['methods']==''


def test_import_ledger_restores_original_folders_without_changing_article(catalog):
    row=record('prefect');row.update(source='qdrant_prefect',source_sha256='a'*64,
        qdrant={'folder_ids':['old-source','older-source'],'source_folder_id':'old-source'})
    item=catalog.register(row)
    catalog.edit(item['id'],item['revision'],{'title':'人工原题'})
    before=catalog.raw(item['id'])
    with catalog.connect() as conn:
        discovery=conn.execute('SELECT * FROM article_rating_discovery ORDER BY sequence').fetchall()
    assert catalog.imported_folders()=={'old-source','older-source'}
    assert 'new-version-folder' not in catalog.imported_folders()
    assert catalog.raw(item['id'])==before
    with catalog.connect() as conn:
        assert conn.execute('SELECT * FROM article_rating_discovery ORDER BY sequence').fetchall()==discovery
    from psycopg.types.json import Jsonb
    with catalog.connect() as conn:
        conn.execute('UPDATE article SET sync=%s WHERE id=%s',(Jsonb({'qdrant':{'folder_ids':['repository-revision']}}),item['id']))
    assert catalog.imported_folders()=={'old-source','older-source'}
    invalid=record('invalid');invalid.update(source='qdrant_prefect',qdrant={'folder_ids':['invalid-folder'],'source_folder_id':'invalid-folder'})
    catalog.register(invalid)
    assert 'invalid-folder' not in catalog.imported_folders()


def test_importer_registers_only_new_manifest_once_after_restart(catalog,monkeypatch):
    import httpx
    from types import SimpleNamespace
    from test_title_language import inputs
    from article_repository.imports import Importer,new_record
    from paperflow_qdrant.library import stable_id
    run,body=inputs();reg=run['parameters']['registered']
    owner=next(iter(body['dataset_meta_fields'].values()));owner['paperflow_role']='body'
    new=stable_id('task','group','a'*64,'b'*64)
    old=stable_id('task','group','a'*64,'old-manifest')
    prior=new_record(run,body,[],[]);prior['qdrant']={'folder_ids':[old],'source_folder_id':old}
    item=catalog.register(prior)
    calls=[];register=catalog.register
    def observe_register(record):calls.append(record['qdrant']['source_folder_id']);return register(record)
    monkeypatch.setattr(catalog,'register',observe_register)
    monkeypatch.setattr(catalog,'synchronize',lambda *a:pytest.fail('unexpected sync'))
    identity={k:reg[k] for k in ('task_id','group_id','paper_id','receipt_id')}
    identity.update(task_run_id='run',attempt_id='run')
    completion={'flow_run_id':'run','created':'2026-11-12T02:00:00+00:00','data':{'kind':'rag_complete','folder_completion':True,'data':{'status':'complete','identity':identity}}}
    post={'flow_run_id':'run','created':'2026-11-12T01:00:00+00:00','data':{'kind':'postprocessing_complete','data':{'validation_status':'passed','identity':identity,'body':{'manifest_sha256':'b'*64}}}}
    def handler(request):
        import json
        query=json.loads(request.content)
        if request.url.path=='/flow_runs/filter':value=[run]
        else:value=[completion] if query['artifacts']['type']['any_']==['paperflow-v3-folder-completion'] else [post]
        return httpx.Response(200,json=value)
    real_client=httpx.Client
    monkeypatch.setattr(httpx,'Client',lambda **kw:real_client(**kw,transport=httpx.MockTransport(handler)))
    sync=SimpleNamespace(catalog_manifests=lambda:[SimpleNamespace(meta={'folder_id':f,'document':body}) for f in (old,new)])
    importer=Importer(catalog,sync,SimpleNamespace(observe=lambda files:files),'http://test.invalid')
    importer.refresh();importer.refresh()
    assert calls==[new]
    assert set(catalog.raw(item['id'])['base']['qdrant']['folder_ids'])=={old,new}


def test_unknown_major_or_field_cannot_mutate_catalog(catalog):
    item=catalog.register(record())
    with pytest.raises(ValueError): catalog.edit(item['id'],item['revision'],{'majors':['not-a-major']})
    with pytest.raises(ValueError): catalog.edit(item['id'],item['revision'],{'fields':{'source_sha':'oops'}})


def test_older_completed_run_cannot_roll_back_newer_folder(catalog):
    newer=record(title='新版处理结果'); newer['source_updated_at']='2026-11-03T12:00:00+00:00'
    newer['qdrant']={'folder_ids':['new'],'source_folder_id':'new'}
    item=catalog.register(newer)
    older=record(title='旧版处理结果'); older['source_updated_at']='2026-11-02T12:00:00+00:00'
    older['qdrant']={'folder_ids':['old'],'source_folder_id':'old'}
    catalog.register(older)
    value=catalog.detail(item['id'])
    assert value['title']=='新版处理结果'
    assert value['qdrant']['source_folder_ids']==['new']
    assert set(value['qdrant']['folder_ids'])=={'new','old'}

def test_reloading_same_source_keeps_observed_files_and_content_revision(catalog):
    source=record()
    source['files']=[{'id':'body','relative':'original','sha256':'a'*64,'bytes':4,'availability':'unknown'}]
    source['inventory_complete']=True
    item=catalog.register(source)
    observed=[{**source['files'][0],'availability':'available'}]
    catalog.observe_files(item['id'],observed)
    reloaded=catalog.register(source)
    assert reloaded['revision']==item['revision']
    assert reloaded['availability']=='complete'

def test_stale_publication_result_cannot_overwrite_newer_revision(catalog):
    item=catalog.register(record())
    catalog.edit(item['id'],item['revision'],{'title':'新修订'})
    with pytest.raises(ValueError,match='版本'):
        catalog.save_sync(item['id'],item['revision'],{'status':'complete'})
    assert catalog.detail(item['id'])['sync']=={}

@pytest.mark.parametrize('incoming_older',[False,True])
def test_file_recovery_is_independent_of_profile_version(catalog,incoming_older):
    prior=record(title='保留的画像')
    prior['source_updated_at']='2026-11-03T12:00:00+00:00'
    prior['files']=[{'id':'original','sha256':'a'*64,'relative':'objects/a','bytes':4,'availability':'available'}]
    item=catalog.register(prior)
    incoming=record(title='新的画像')
    incoming['source_updated_at']='2026-11-02T12:00:00+00:00' if incoming_older else '2026-11-04T12:00:00+00:00'
    incoming['files']=[{'id':'attachment','sha256':'b'*64,'relative':'objects/b','bytes':5,'availability':'available'}]
    saved=catalog.register(incoming)
    assert saved['title']==('保留的画像' if incoming_older else '新的画像')
    assert {f['id'] for f in saved['files']}=={'original','attachment'}
    assert all(f['availability']=='available' for f in saved['files'])
    assert catalog.register(incoming)['revision']==saved['revision']

def test_file_id_collision_preserves_both_verified_contents(catalog):
    prior=record();prior['files']=[{'id':'body','sha256':'a'*64,'relative':'objects/a','bytes':4,'availability':'available'}]
    catalog.register(prior)
    incoming=record();incoming['files']=[{'id':'body','sha256':'b'*64,'relative':'objects/b','bytes':5,'availability':'available'}]
    saved=catalog.register(incoming)
    assert {f['sha256'] for f in saved['files']}=={'a'*64,'b'*64}
    assert len({f['id'] for f in saved['files']})==2
    assert catalog.register(incoming)['revision']==saved['revision']

def test_stale_availability_scan_cannot_remove_new_file(catalog):
    prior=record();prior['files']=[{'id':'body','sha256':'a'*64,'relative':'objects/a','bytes':4,'availability':'unknown'}]
    item=catalog.register(prior)
    incoming=record();incoming['files']=[{'id':'attachment','sha256':'b'*64,'relative':'objects/b','bytes':5,'availability':'available'}]
    catalog.register(incoming)
    catalog.observe_files(item['id'],[{**prior['files'][0],'availability':'available'}])
    saved=catalog.detail(item['id'])
    assert {f['id'] for f in saved['files']}=={'body','attachment'}
    assert all(f['availability']=='available' for f in saved['files'])

def test_unlinked_attachment_is_retained_without_becoming_an_article(catalog):
    item=record('attachment','附件说明');item['entry_kind']='attachment'
    saved=catalog.register(item)
    assert catalog.detail(saved['id'])['title']=='附件说明'
    assert catalog.search()['total']==0


def test_combined_filters_apply_before_count_and_pagination(catalog):
    target=record('target','可参照论文')
    target['profile']['applicable_level']={'primary_label':'普通本科','labels':['普通本科','重点本科（参照）'],'reason':'只作参照'}
    target['files']=[{'id':'original','name':'论文.docx','kind':'原文','availability':'available'},
                     {'id':'a','name':'报告.pdf','kind':'附件','availability':'available'}]
    catalog.register(target)
    for i in range(35):
        catalog.register(record('newer-'+str(i),'其他论文'))
    result=catalog.search(major='01',level='key_bachelor',file_condition='both',attachment_ext='pdf',page_size=30)
    assert result['total']==1
    assert result['rows'][0]['title']=='可参照论文'
    assert result['rows'][0]['reference']['primary']=='普通本科'
    assert result['rows'][0]['file_summary']['attachment_available']==1


def test_format_matches_the_same_available_attachment_not_original_or_missing_file(catalog):
    source=record()
    source['files']=[{'id':'main','name':'正文.pdf','kind':'原文','availability':'available'},
        {'id':'pdf','name':'报告.pdf','kind':'附件','availability':'missing'},
        {'id':'zip','name':'附件.ZIP','kind':'附件','availability':'available'}]
    catalog.register(source)
    assert catalog.search(file_condition='attachment')['total']==1
    assert catalog.search(attachment_ext='zip')['total']==1
    assert catalog.search(attachment_ext='pdf')['total']==0


def test_manual_labels_and_methods_override_generated_search_and_filter(catalog):
    source=record();source['profile']['learning_units'][0]['method']='旧算法'
    item=catalog.register(source)
    assert catalog.search(keyword='旧算法')['total']==1
    catalog.edit(item['id'],item['revision'],{'fields':{'level':'函授参考','methods':'新算法'}})
    assert catalog.search(level='bachelor')['total']==0
    assert catalog.search(level='correspondence')['total']==1
    assert catalog.search(keyword='旧算法')['total']==0
    assert catalog.search(keyword='新算法')['total']==1


def test_unknown_and_missing_files_are_distinct_and_neither_is_silently_hidden(catalog):
    unknown=record('unknown');catalog.register(unknown)
    missing=record('missing');missing['inventory_complete']=True
    missing['files']=[{'id':'original','name':'原文.docx','kind':'原文','availability':'missing'}]
    catalog.register(missing)
    assert catalog.search()['total']==2
    assert catalog.search(file_condition='unverified')['total']==1
    assert catalog.search(file_condition='original')['total']==0


def test_detail_has_whitelisted_existing_profile_and_lightweight_search_summary(catalog):
    source=record();source['profile']['learning_units'][0]['title']='访谈设计'
    item=catalog.register(source)
    result=catalog.detail(item['id'])
    assert result['profile']['units'][0]['title']=='访谈设计'
    row=catalog.search()['rows'][0]
    assert 'profile' not in row
    assert row['reference']['levels']==['bachelor']


def test_keyword_search_uses_summary_and_keywords_without_searching_folder_or_reason(catalog):
    source=record('私有文件夹名称')
    source['profile']['article']['summary']='灰色关联分析研究'
    source['profile']['applicable_level']['reason']='不用于额外关键词搜索'
    catalog.register(source)
    assert catalog.search(keyword='灰色关联')['total']==1
    assert catalog.search(keyword='教学')['total']==1
    assert catalog.search(keyword='私有文件夹名称')['total']==0


def test_statistics_use_the_same_reference_and_file_rules(catalog):
    source=record();source['files']=[{'id':'a','kind':'附件','name':'报告.pdf','availability':'available'}]
    catalog.register(source)
    summary=catalog.statistics()
    assert summary['total']==1
    assert summary['bachelor']==1
    assert summary['attachments']==1


def test_generated_search_document_tracks_manual_edits_without_rewriting_profile(catalog):
    source=record();item=catalog.register(source)
    with catalog.connect() as conn:
        before=conn.execute('SELECT search_text FROM article WHERE id=%s',(item['id'],)).fetchone()['search_text']
    assert '教学' in before
    catalog.edit(item['id'],item['revision'],{'fields':{'keywords':'新检索词'}})
    with catalog.connect() as conn:
        after=conn.execute('SELECT search_text,base FROM article WHERE id=%s',(item['id'],)).fetchone()
    assert '新检索词' in after['search_text']
    assert '教学' not in after['search_text']
    assert after['base']['profile']==source['profile']


def test_decoded_multiline_labels_have_identical_display_and_sql_results(catalog):
    item=catalog.register(record())
    catalog.edit(item['id'],item['revision'],{'fields':{'level':'不推荐重点本科\n普通本科参考'}})
    assert catalog.detail(item['id'])['reference']['levels']==['bachelor']
    assert catalog.search(level='bachelor')['total']==1
    assert catalog.search(level='key_bachelor')['total']==0


def test_generated_search_uses_source_empty_fallback_but_honors_manual_empty(catalog):
    source=record();source['profile']['article']['summary']='';source['profile']['summary']='回退摘要测试词'
    source['profile']['article']['keywords']=[];source['profile']['keywords']=['回退关键词']
    item=catalog.register(source)
    assert catalog.search(keyword='回退摘要测试词')['total']==1
    assert catalog.search(keyword='回退关键词')['total']==1
    catalog.edit(item['id'],item['revision'],{'fields':{'summary':'','keywords':''}})
    assert catalog.search(keyword='回退摘要测试词')['total']==0
    assert catalog.search(keyword='回退关键词')['total']==0
