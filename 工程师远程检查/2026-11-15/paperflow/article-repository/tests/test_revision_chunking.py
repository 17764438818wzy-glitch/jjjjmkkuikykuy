"""Exercise the real immutable writer with bounded source records and a large profile."""
import copy
import json
import pytest
from qdrant_client import models
from paperflow_qdrant.library import stable_id
from paperflow_qdrant.identity import fingerprint
from test_sync import setup

MAJORS=[{'code':'01','paper_dataset_id':'new','image_dataset_id':'new_assets'}]


def large_source():
    lib,sync,item,old=setup()
    source=copy.deepcopy(sync.manifests(['original'])[0].meta['document'])
    source['id']=stable_id('large-source','body')
    source['meta_fields']['two_pass_profile_json']=json.dumps({
        'article':{'title':'English translated title','summary':'摘要'*2000},
        'learning_units':[{'id':str(i),'method':'研究方法'*500} for i in range(30)]},ensure_ascii=False)
    source['records']=[{'id':'learning_1','content':'原生学习内容','section_ids':['S1'],
        'available':True,'source_refs':[{'block_id':'B1'}],'unit_id':'u1','part_id':'part-001'},
        {'id':'profile_1','content':'原生分块画像','section_ids':['S2'],'available':False},
        {'id':'text','content':'原始正文','section_ids':['S1']}]
    lib.ingest({'folder_id':'large-source','documents':[source]})
    item.update(fields={'summary':'摘要'*2000,'methods':'研究方法'*1500},
        qdrant={'folder_ids':['large-source'],'source_folder_ids':['large-source']},
        overrides={'title':'原文中文题目'})
    item['title']='原文中文题目'
    calls=[]
    original_encode=lib.embedder.encode
    def bounded(texts):
        calls.append(list(texts))
        if any(len(t.encode())>8000 for t in texts):
            raise ValueError('input exceeds embedding context')
        return original_encode(texts)
    lib.embedder.encode=bounded
    return lib,sync,item,source,calls


@pytest.mark.parametrize('overrides',[{'title':'原文中文题目'},{'title':'原文中文题目','fields':{}},{'majors':['01']}])
def test_metadata_only_revision_preserves_bounded_native_records(overrides):
    lib,sync,item,source,calls=large_source()
    item['overrides']=overrides
    before=copy.deepcopy(item)
    result=sync.apply(item,MAJORS)
    doc=lib.documents('new')[0]
    manifest=lib._manifest('new',doc['id'])
    assert manifest['records'][:-1]==source['records']
    assert manifest['records'][-1]=={'id':'repository-title','content':item['title'],'section_ids':[]}
    assert max(len(text.encode()) for batch in calls for text in batch)<8000
    assert len(json.loads(manifest['meta_fields']['two_pass_profile_json'])['learning_units'])==30
    assert lib.document('old',source['id']) is None
    assert result['status']=='complete' and item==before
    count=lib.store.count_documents();number=len(calls)
    sync.apply(item,MAJORS)
    assert lib.store.count_documents()==count and len(calls)==number


def test_explicit_empty_field_remains_a_real_profile_edit():
    lib,sync,item,old=setup()
    item['overrides']={'title':'新标题','fields':{'summary':''}}
    sync.apply(item,MAJORS)
    doc=lib.documents('new')[0]
    assert json.loads(doc['meta_fields']['two_pass_profile_json'])['article']['summary']==''
    records=lib._manifest('new',doc['id'])['records']
    assert records[-1]['id']=='repository-profile'


@pytest.mark.parametrize('complete_v1',[False,True])
def test_fixed_publication_does_not_overwrite_or_leave_v1_active(complete_v1):
    lib,sync,item,source,calls=large_source()
    v1=stable_id('repository-revision',item['id'],str(item['revision']))
    old=copy.deepcopy(source);old['id']=stable_id(v1,source['id'])
    old['meta_fields'].update(repository_article_id=item['id'],repository_revision=item['revision'])
    if complete_v1:
        lib.ingest({'folder_id':v1,'documents':[old]})
    else:
        # Preserve a real incomplete immutable binding left by an embedding failure.
        encode=lib.embedder.encode
        def fail(texts):raise RuntimeError('failed before first embedding')
        lib.embedder.encode=fail
        with pytest.raises(RuntimeError):lib.ingest({'folder_id':v1,'documents':[old]})
        lib.embedder.encode=encode
    binding=lib._existing([stable_id(v1,'binding')])[stable_id(v1,'binding')]
    before=copy.deepcopy(binding.meta)
    result=sync.apply(item,MAJORS)
    assert result['qdrant']['folder_ids']==[stable_id('repository-revision-v2',item['id'],str(item['revision']))]
    current=lib._existing([stable_id(v1,'binding')])[stable_id(v1,'binding')]
    assert current.meta['fingerprint']==before['fingerprint']
    points,_=lib.store._client.scroll(lib.store.index,scroll_filter=models.Filter(must=[
        models.FieldCondition(key='meta.folder_id',match=models.MatchValue(value=v1)),
        models.FieldCondition(key='meta.active',match=models.MatchValue(value=True))]),with_vectors=False,limit=100)
    assert not points


def test_failed_v2_write_keeps_old_readable_and_retries_same_revision():
    lib,sync,item,source,calls=large_source()
    writer=lib.writer.run
    def fail_complete(*,documents):
        if any(d.meta.get('kind')=='folder_complete' for d in documents):raise RuntimeError('injected commit failure')
        return writer(documents=documents)
    lib.writer.run=fail_complete
    with pytest.raises(RuntimeError):sync.apply(item,MAJORS)
    assert lib.document('old',source['id']) is not None
    assert lib.documents('new')==[]
    lib.writer.run=writer
    before_calls=len(calls)
    assert sync.apply(item,MAJORS)['status']=='complete'
    assert len(calls)==before_calls
    assert lib.document('old',source['id']) is None


def test_orphan_v1_with_wrong_article_owner_is_not_silently_withdrawn():
    lib,sync,item,source,calls=large_source()
    v1=stable_id('repository-revision',item['id'],str(item['revision']))
    other=copy.deepcopy(source);other['id']=stable_id(v1,source['id'])
    other['meta_fields'].update(repository_article_id='another-article',repository_revision=item['revision'])
    lib.ingest({'folder_id':v1,'documents':[other]})
    with pytest.raises(ValueError,match='旧修订来源'):
        sync.apply(item,MAJORS)
    assert lib.document('old',source['id']) is not None
    assert lib.document('old',other['id']) is not None
