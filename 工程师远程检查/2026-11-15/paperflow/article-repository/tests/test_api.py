from fastapi.testclient import TestClient
from test_catalog import catalog,record

def test_failed_sync_retains_last_published_revision_for_safe_retry(catalog,tmp_path):
    from article_repository.api import create_app
    from article_repository.files import Files
    class UnavailableSync:
        def apply(self,item,majors): raise RuntimeError('injected network outage')
    rec=record(); rec['qdrant']={'folder_ids':['original']}
    item=catalog.register(rec)
    catalog.save_sync(item['id'],item['revision'],{'status':'complete','qdrant':{'folder_ids':['published-r2']}})
    client=TestClient(create_app(catalog,Files(str(tmp_path)),UnavailableSync()))
    response=client.patch('/api/library/articles/'+item['id'],json={'revision':item['revision'],'changes':{'title':'R3'}})
    assert response.status_code==200
    value=response.json()
    assert value['title']=='R3'
    assert value['sync']['status']=='failed'
    assert set(value['qdrant']['folder_ids'])=={'original','published-r2'}


def test_query_contract_rejects_unknown_filters_and_passes_combined_conditions(catalog,tmp_path):
    from article_repository.api import create_app
    from article_repository.files import Files
    source=record();source['profile']['applicable_level']['labels']=['重点本科（参照）']
    source['files']=[{'id':'pdf','kind':'附件','name':'报告.pdf','availability':'available'}]
    catalog.register(source)
    client=TestClient(create_app(catalog,Files(str(tmp_path)),object()))
    result=client.get('/api/library/articles',params={'major':'01','level':'key_bachelor','file_condition':'attachment','attachment_ext':'pdf'})
    assert result.status_code==200
    assert result.json()['total']==1
    assert client.get('/api/library/articles',params={'attachment_ext':'zip'}).json()['total']==0
    assert client.get('/api/library/articles',params={'level':'made-up'}).status_code==422
    assert client.get('/api/library/statistics').json()['total']==1


def test_legacy_detail_restores_explicit_primary_but_keeps_manual_corrections(catalog,tmp_path):
    from article_repository.api import create_app
    from article_repository.files import Files
    class Source:
        def read_profile(self,item):
            return {'applicable_level':{'primary_label':'普通本科','labels':['普通本科','重点本科（参照）']},
                'article':{'summary':'原画像摘要'},'learning_units':[{'title':'文本细读','what_to_learn':'引文组织'}]}
    source=record();source['profile']={'applicable_levels':'普通本科；重点本科（参照）'}
    item=catalog.register(source)
    catalog.edit(item['id'],item['revision'],{'fields':{'summary':'保留人工摘要'}})
    client=TestClient(create_app(catalog,Files(str(tmp_path)),Source()))
    value=client.get('/api/library/articles/'+item['id']).json()
    assert value['reference']['primary']=='普通本科'
    assert value['fields']['summary']=='保留人工摘要'
    assert value['profile']['units'][0]['title']=='文本细读'
