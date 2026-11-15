import copy,hashlib,json
import importlib.util
import pytest

from haystack_integrations.document_stores.qdrant import QdrantDocumentStore
from paperflow_qdrant.library import Library,stable_id


def test_attachment_reference_in_same_folder_targets_new_primary_before_retiring_old_folder():
    from legacy_migration import convert
    sha='a'*64
    primary={'id':'old-primary','kb_id':'primary-dataset','name':'primary.md','meta':
        {'paperflow_role':'attachment-card','original_file_id':'raw','attachment_source_sha256':sha}}
    reference={'id':'old-reference','kb_id':'reference-dataset','name':'reference.md','meta':
        {**primary['meta'],'paperflow_role':'attachment-reference','primary_dataset_id':'primary-dataset','primary_document_id':'old-primary'}}
    rows={d['id']:[{'id':'chunk-'+d['id'],'content_with_weight':'完整MD文档ID：old-primary\n原件文件ID：raw'}] for d in (primary,reference)}
    batch,_=convert('new-folder','article',{'identity':[None,None,None]},[primary,reference],rows,base_url='http://gateway',
        object_ids={'raw':sha},document_map={'primary-dataset/old-primary':'prior-folder-primary'})
    new_primary=next(d for d in batch['documents'] if 'primary-dataset' in d['dataset_ids'])
    new_reference=next(d for d in batch['documents'] if 'reference-dataset' in d['dataset_ids'])
    fields={**new_reference['meta_fields'],**new_reference['dataset_meta_fields']['reference-dataset']}
    assert fields['primary_document_id']==new_primary['id']
    assert '完整MD文档ID：'+new_primary['id'] in new_reference['records'][0]['content']


def fixture():
    identity=['task','s'*64,'m'*64]
    profile={'article':{'title':'真实历史文章'},'learning_units':[{'id':'U1','section_ids':['S1'],'steps':['完整步骤']}]}
    payload={'identity':identity,'original':'完整真实原文','profile':profile,'tree':{'nodes':[{'id':'S1','title':'章节'}]},
             'chunks':[{'id':'C1','section_ids':['S1']}], 'assets':[{'id':'A1','sha256':'a'*64}]}
    documents=[];rows={}
    for suffix in ('one','two'):
        for role,rid,text in [('body','C1','原文切片'),('images','asset_A1','图片描述')]:
            did=role+'-'+suffix;ds=role+'-dataset-'+suffix
            meta=dict(zip(('paperflow_task','paperflow_source_sha256','paperflow_manifest_sha256'),identity),paperflow_role=role,paperflow_specialty=suffix)
            if role=='body':meta.update(two_pass_profile_json=json.dumps(profile,ensure_ascii=False),
                two_pass_tree_json=json.dumps(payload['tree'],ensure_ascii=False),
                paperflow_image_associations_json=json.dumps({'dataset_id':'images-dataset-'+suffix,'document_id':'images-'+suffix,'chunks':{'asset_A1':{'chunk_id':'old-image-chunk'}}}))
            documents.append({'id':did,'kb_id':ds,'name':'历史发布名','meta':meta})
            marker=json.dumps(dict(task='task',source_sha256='s'*64,manifest_sha256='m'*64,record=rid),ensure_ascii=False)
            content='[PAPERFLOW]'+marker+'[/PAPERFLOW]\n'+text
            rows[did]=[{'id':'legacy-'+did,'doc_id':did,'content_with_weight':content,'q_1024_vec':[1.,0.,0.,0.],
                        'available_int':1,'chunk_order_int':0,**({'img_id':'old-image-id'} if role=='images' else {})}]
    return payload,documents,rows


def test_multimajor_conversion_preserves_profile_original_sections_images_and_vectors():
    assert importlib.util.find_spec('legacy_migration'),'Legacy conversion is missing'
    from legacy_migration import convert
    payload,documents,rows=fixture()
    batch,vectors=convert('new-folder','article-id',payload,documents,rows,base_url='http://gateway')
    assert len(batch['documents'])==2
    class NoEmbed:
        def encode(self,texts):raise AssertionError('Existing vectors were not reused')
    library=Library(QdrantDocumentStore(':memory:',embedding_dim=4,progress_bar=False),NoEmbed())
    library.ingest(batch,embeddings=vectors)
    body=next(d for d in batch['documents'] if d['meta_fields']['paperflow_role']=='body')
    image=next(d for d in batch['documents'] if d['meta_fields']['paperflow_role']=='images')
    for suffix in ('one','two'):
        ds='body-dataset-'+suffix
        observed=library.document(ds,body['id'])
        fields=observed['meta_fields']
        assert json.loads(fields['two_pass_profile_json'])==payload['profile']
        assert library.original(ds,body['id'])==payload['original']
        assert fields['paperflow_original_sha256']==hashlib.sha256(payload['original'].encode()).hexdigest()
        assert fields['paperflow_section_chunks']['S1']==[stable_id(body['id'],'C1')]
        association=json.loads(fields['paperflow_image_associations_json'])
        assert association['dataset_id']=='images-dataset-'+suffix and association['document_id']==image['id']
        assert association['chunks']['asset_A1']['chunk_id']==stable_id(image['id'],'asset_A1')
        assert library.chunks('images-dataset-'+suffix,image['id'])[0]['image_id']=='a'*64
    assert library.document('unrelated',body['id']) is None


def test_foreign_paper_marker_cannot_be_migrated_into_another_folder():
    assert importlib.util.find_spec('legacy_migration'),'Legacy conversion is missing'
    from legacy_migration import convert
    payload,documents,rows=fixture()
    rows['body-one'][0]['content_with_weight']=rows['body-one'][0]['content_with_weight'].replace('s'*64,'z'*64)
    with pytest.raises(ValueError,match='identity'):
        convert('folder','article',payload,documents,rows,base_url='http://gateway')


@pytest.mark.parametrize('difference',['availability','content'])
def test_differing_professional_records_do_not_expand_scope(difference):
    from legacy_migration import convert
    payload,documents,rows=fixture()
    if difference=='availability':rows['body-one'][0]['available_int']=0
    else:rows['body-one'][0]['content_with_weight']+='专业一额外内容'
    batch,vectors=convert('folder','article',payload,documents,rows,base_url='http://gateway')
    body=[d for d in batch['documents'] if d['meta_fields']['paperflow_role']=='body']
    assert len(body)==2
    assert all(len(d['dataset_ids'])==1 for d in body)
    one=next(d for d in body if 'body-dataset-one' in d['dataset_ids'])
    two=next(d for d in body if 'body-dataset-two' in d['dataset_ids'])
    if difference=='availability':
        assert one['records'][0]['available'] is False and two['records'][0]['available'] is True
    else:
        assert all('专业一额外内容' not in r['content'] for r in two['records'])


def test_recovered_image_associations_use_the_verified_object_without_local_assets():
    from legacy_migration import convert
    payload,documents,rows=fixture();payload['assets']=[]
    for doc in documents:
        if doc['meta']['paperflow_role']=='body':
            value=json.loads(doc['meta']['paperflow_image_associations_json'])
            value['chunks']['asset_A1']['image_id']='old-image-id'
            doc['meta']['paperflow_image_associations_json']=json.dumps(value)
    batch,_=convert('folder','article',payload,documents,rows,base_url='http://gateway',object_ids={'old-image-id':'b'*64})
    for doc in batch['documents']:
        if doc['meta_fields']['paperflow_role']=='body':
            for owner in doc['dataset_meta_fields'].values():
                assert json.loads(owner['paperflow_image_associations_json'])['chunks']['asset_A1']['image_id']=='b'*64


def test_attachment_keeps_card_text_and_rewrites_downloads_without_reusing_changed_vectors():
    from legacy_migration import convert
    payload,documents,rows=fixture()
    oldurl='http://192.168.1.120:8088/api/v1/datasets/x/documents/raw-file'
    documents.append({'id':'attachment','kb_id':'images-dataset-one','name':'附件说明.md','meta':{
        'paperflow_role':'attachment-card','attachment_id':'A99','original_file_id':'raw-file',
        'attachment_source_sha256':'c'*64,'original_download_url':oldurl,'parent_body_document_id':'body-one'}})
    rows['attachment']=[{'id':'att-chunk','content_with_weight':'这是附件画像。\n下载：'+oldurl,'q_1024_vec':[1.,0.,0.,0.],'available_int':1}]
    payload['originals']={'attachment':'这是附件画像和处理说明。'}
    batch,vectors=convert('folder','article',payload,documents,rows,base_url='http://gateway',object_ids={'raw-file':'c'*64})
    card=next(d for d in batch['documents'] if d['meta_fields']['paperflow_role']=='attachment-card')
    fields={**card['meta_fields'],**card['dataset_meta_fields']['images-dataset-one']}
    assert card['original']=='这是附件画像和处理说明。'
    assert fields['original_file_id']=='c'*64 and fields['original_download_url']=='http://gateway/paperflow/objects/'+'c'*64
    assert '8088' not in card['records'][0]['content'] and fields['original_download_url'] in card['records'][0]['content']
    assert stable_id(card['id'],card['records'][0]['id']) not in vectors

def test_attachment_file_url_and_primary_reference_are_rebound_from_explicit_map():
    from legacy_migration import convert
    payload,documents,rows=fixture()
    old='old-file';primary='old-primary';target=stable_id('new-primary')
    documents=[{'id':'card','kb_id':'assets','name':'PFA_MD_card.md','meta':{'paperflow_role':'attachment-reference',
        'original_file_id':old,'attachment_source_sha256':'c'*64,'primary_dataset_id':'primary-ds','primary_document_id':primary}}]
    text='完整MD文档ID：'+primary+'\n原件文件ID：'+old+'\n原件下载接口：http://192.168.1.120:8088/api/v1/files/'+old+'\n'
    rows={'card':[{'id':'chunk','content_with_weight':text,'q_1024_vec':[1.,0.,0.,0.]}]}
    payload['originals']={'card':text}
    batch,vectors=convert('folder','article',payload,documents,rows,base_url='http://gateway',object_ids={old:'c'*64},
        document_map={'primary-ds/'+primary:target})
    card=batch['documents'][0];meta={**card['meta_fields'],**card['dataset_meta_fields']['assets']}
    assert meta['primary_document_id']==target
    assert meta['legacy_primary_document_id']==primary
    assert '8088' not in card['original'] and target in card['original']
    assert 'http://gateway/paperflow/objects/'+'c'*64 in card['records'][0]['content']
    assert vectors=={}
