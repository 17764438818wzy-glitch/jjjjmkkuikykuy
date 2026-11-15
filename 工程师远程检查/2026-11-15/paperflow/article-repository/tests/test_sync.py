import importlib
import json
import sys
from pathlib import Path

import pytest
from haystack_integrations.document_stores.qdrant import QdrantDocumentStore
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'qdrant-migration'))
from paperflow_qdrant.library import Library,stable_id

class Embedder:
    def encode(self,texts): return [[1.,float('新画像' in t),0.5,0.5] for t in texts]

def setup(images=False,attachment=False):
    assert importlib.util.find_spec('article_repository.sync'), 'Qdrant revision sync is not implemented'
    from article_repository.sync import RevisionSync
    lib=Library(QdrantDocumentStore(':memory:',embedding_dim=4,progress_bar=False),Embedder())
    did=stable_id('original','body')
    docs=[{'id':did,'name':'旧标题','dataset_ids':['old'],
        'meta_fields':{'two_pass_profile_json':json.dumps({'article':{'title':'旧标题'}})},
        'dataset_meta_fields':{'old':{'paperflow_role':'body'}},'original':'原始正文',
        'records':[{'id':'text','content':'原始正文','section_ids':['S1']}]}]
    if images:
        image_id=stable_id('original','images')
        docs[0]['dataset_meta_fields']['old']['paperflow_image_associations_json']=json.dumps({
            'dataset_id':'old-assets','document_id':image_id,'chunks':{'image':{'chunk_id':stable_id(image_id,'image')}}})
        docs.append({'id':image_id,'name':'配图','dataset_ids':['old-assets'],'meta_fields':{'paperflow_role':'images'},
            'dataset_meta_fields':{'old-assets':{'paperflow_role':'images'}},'original':'配图说明',
            'records':[{'id':'image','content':'图片说明','image_id':'a'*64,'section_ids':[]}]})
    if attachment:
        docs.append({'id':stable_id('original','attachment'),'name':'附件','dataset_ids':['old-assets'],
            'meta_fields':{'paperflow_role':'attachment:survey'},
            'dataset_meta_fields':{'old-assets':{'parent_body_document_id':did,'paperflow_role':'attachment:survey'}},
            'original':'附件原文','records':[{'id':'attachment','content':'附件说明'}]})
    lib.ingest({'folder_id':'original','documents':docs})
    item={'id':'article','revision':2,'title':'新标题','fields':{'summary':'新画像'},'majors':['01'],
          'qdrant':{'folder_ids':['original']},'overrides':{'title':'新标题','fields':{'summary':'新画像'}}}
    return lib,RevisionSync(lib),item,did

def test_revision_is_readable_before_old_membership_is_withdrawn():
    lib,sync,item,old=setup()
    result=sync.apply(item,[{'code':'01','paper_dataset_id':'new','image_dataset_id':'new_assets'}])
    doc=lib.documents('new')[0]
    assert doc['name']=='新标题'
    assert json.loads(doc['meta_fields']['two_pass_profile_json'])['article']['summary']=='新画像'
    assert lib.original('new',doc['id'])=='原始正文'
    assert lib.document('old',old) is None
    assert result['status']=='complete'
    count=lib.store.count_documents()
    sync.apply(item,[{'code':'01','paper_dataset_id':'new','image_dataset_id':'new_assets'}])
    assert lib.store.count_documents()==count

def test_failed_new_batch_keeps_original_searchable():
    lib,sync,item,old=setup()
    writer=lib.writer.run
    def fail_complete(*,documents):
        if any(d.meta.get('kind')=='folder_complete' for d in documents): raise RuntimeError('injected failed new commit')
        return writer(documents=documents)
    lib.writer.run=fail_complete
    with pytest.raises(RuntimeError): sync.apply(item,[{'code':'01','paper_dataset_id':'new','image_dataset_id':'assets'}])
    assert lib.document('old',old) is not None
    assert lib.documents('new')==[]

def test_unmigrated_record_does_not_trigger_migration():
    lib,sync,item,old=setup()
    item['qdrant']={}
    assert sync.apply(item,[])['status']=='saved_for_migration'
    assert lib.document('old',old) is not None


def test_changed_major_keeps_image_associations_readable():
    lib,sync,item,old=setup(images=True)
    sync.apply(item,[{'code':'01','paper_dataset_id':'new','image_dataset_id':'new_assets'}])
    body=lib.documents('new')[0]
    image=lib.documents('new_assets')[0]
    association=json.loads(body['meta_fields']['paperflow_image_associations_json'])
    assert association['dataset_id']=='new_assets'
    assert association['document_id']==image['id']
    assert association['chunks']['image']['chunk_id']==lib.chunks('new_assets',image['id'])[0]['id']
    assert lib.document('old',old) is None

def test_revision_rebinds_attachment_parent_in_specialty_metadata():
    lib,sync,item,old=setup(attachment=True)
    result=sync.apply(item,[{'code':'01','paper_dataset_id':'new','image_dataset_id':'new_assets'}])
    body=lib.documents('new')[0]
    attachment=lib.documents('new_assets')[0]
    assert attachment['meta_fields']['parent_body_document_id']==body['id']
    assert lib.document('new',attachment['meta_fields']['parent_body_document_id']) is not None
    assert lib.document('old',old) is None
    assert result['status']=='complete'


def test_legacy_profile_enrichment_reads_source_without_creating_revision():
    lib,sync,item,old=setup(images=True)
    count=lib.store.count_documents()
    result=sync.read_profile(item)
    assert result['article']['title']=='旧标题'
    assert lib.store.count_documents()==count
    assert lib.document('old',old) is not None
    assert sync.read_profile({'qdrant':{}}) is None
