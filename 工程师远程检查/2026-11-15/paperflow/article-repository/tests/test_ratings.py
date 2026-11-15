import copy
import uuid

import pytest
from fastapi.testclient import TestClient
from test_catalog import catalog,record


def source(catalog, *, legacy=False):
    data=record()
    data['profile']['research_profile']['research_type']='engineering_system'
    if legacy:
        data['profile']={'research_profile':{'research_type':'engineering_system'},'applicable_levels':'本科'}
    data['files']=[{'id':'body','kind':'原文','relative':'body.docx','sha256':'a'*64,'bytes':4,'availability':'available'}]
    return catalog.register(data)


def body(item, *, tier='bachelor'):
    import hashlib
    from pathlib import Path
    rubric=Path(__file__).parents[1]/'article_repository/rubric-definitions.json'
    import json
    definition=json.loads(rubric.read_text(encoding='utf-8'))['engineering_system']
    weights={'A':[5,5,5,5],'B':[10,10,5,5],'C':[5,5,10,5],'D':[5,5,5],'E':[5,5]}
    dims=[]
    for key,maxima in weights.items():
        dims.append({'id':key,'score':{'A':16,'B':24,'C':20,'D':12,'E':8}[key],'reason':'具体证据对应这个维度',
            'subitems':[{'id':f'{key}{i}','level':4,'reason':'主要过程清楚，存在少量缺口'} for i,_ in enumerate(maxima,1)]})
    return {'operation_id':str(uuid.uuid4()),'run_id':'test-run','request_id':'test-request',
        'revision':item['revision'],'research_type':'engineering_system','source_files':[{'file_id':'body','sha256':'a'*64}],
        'rubric_version':definition['version'],'rubric_sha256':definition['sha256'],'model':'fixed-test-model',
        'rating':{'status':'rated','tier':tier,'tier_reason':'实际是成熟方法的针对性关联实现，不按分数定档',
            'dimension_scores':dims,'score':80,'cap_applied':False,'cap_reason':'未触发',
            'brief_reason':'过程与结构可借鉴，部分细节可补充',
            'evidence_refs':[{'file_id':'body','page':2,'section':'方法','note':'具体操作说明'}], 'uncertainties':[]}}


def test_rating_is_independent_and_writer_never_synchronizes(catalog,tmp_path):
    from article_repository.api import create_app
    from article_repository.files import Files
    class Sync:
        def apply(self,*args):raise AssertionError('rating must not publish profile')
    item=source(catalog)
    before=catalog.raw(item['id'])
    client=TestClient(create_app(catalog,Files(str(tmp_path)),Sync()))
    result=client.post('/api/library/articles/'+item['id']+'/rating',json=body(item,tier='correspondence'))
    assert result.status_code==200,result.text
    assert result.json()['rating']['score']==80
    after=catalog.raw(item['id'])
    for key in ('base','overrides','revision','sync','updated_at'):assert before[key]==after[key]
    listing=client.get('/api/library/articles').json()['rows'][0]
    assert listing['rating']['score']==80
    assert listing['reference']['levels']==['correspondence']


def test_operation_replay_is_exactly_once_and_mutation_conflicts(catalog):
    item=source(catalog)
    assert hasattr(catalog,'rate'),'atomic rating writer is missing'
    payload=body(item)
    first=catalog.rate(item['id'],payload)
    replay=catalog.rate(item['id'],copy.deepcopy(payload))
    assert first['operation_id']==replay['operation_id']
    assert replay['replayed'] is True
    with catalog.connect() as conn:assert conn.execute('SELECT count(*) n FROM article_rating').fetchone()['n']==1
    changed=copy.deepcopy(payload)
    changed['rating']['tier']='key_bachelor'
    with pytest.raises(ValueError,match='operation|操作|请求'):catalog.rate(item['id'],changed)


@pytest.mark.parametrize('change',[{'revision':999},{'research_type':'creative_design'},{'source_files':[{'file_id':'body','sha256':'b'*64}]}])
def test_stale_identity_cannot_write_any_rating(catalog,change):
    item=source(catalog)
    assert hasattr(catalog,'rate'),'atomic rating writer is missing'
    payload=body(item);payload.update(change)
    with pytest.raises(ValueError):catalog.rate(item['id'],payload)
    with catalog.connect() as conn:assert conn.execute('SELECT count(*) n FROM article_rating').fetchone()['n']==0


def test_all_views_use_one_current_rating_and_invalidate_on_source_change(catalog,tmp_path):
    from article_repository.api import create_app
    from article_repository.files import Files
    item=source(catalog)
    assert hasattr(catalog,'rate'),'atomic rating writer is missing'
    payload=body(item,tier='key_bachelor');catalog.rate(item['id'],payload)
    client=TestClient(create_app(catalog,Files(str(tmp_path)),object()))
    assert client.get('/api/library/articles/'+item['id']).json()['rating']['score']==80
    assert catalog.search(level='key_bachelor')['total']==1
    assert catalog.search(level='bachelor')['total']==0
    assert catalog.statistics()['bachelor']==0
    # Availability observations preserve content identity and must not invalidate a score.
    files=copy.deepcopy(catalog.raw(item['id'])['base']['files']);files[0]['availability']='missing'
    catalog.observe_files(item['id'],files)
    assert catalog.detail(item['id'])['rating']['score']==80
    # A changed source hash must invalidate even if an importer did not bump revision.
    with catalog.connect() as conn:
        conn.execute("UPDATE article SET base=jsonb_set(base,'{files,0,sha256}',to_jsonb(%s::text)) WHERE id=%s",('b'*64,item['id']))
    assert catalog.detail(item['id'])['rating'] is None
    assert catalog.search(level='key_bachelor')['total']==0
    assert catalog.search(level='bachelor')['total']==1
    assert catalog.rating(item['id'],payload['operation_id'])['current'] is False


def test_legacy_recovery_cannot_override_new_rating(catalog,tmp_path):
    from article_repository.api import create_app
    from article_repository.files import Files
    class Sync:
        def read_profile(self,item):return {'applicable_level':{'primary_label':'函授','labels':['函授']}}
    item=source(catalog,legacy=True)
    assert hasattr(catalog,'rate'),'atomic rating writer is missing'
    catalog.rate(item['id'],body(item,tier='key_bachelor'))
    value=TestClient(create_app(catalog,Files(str(tmp_path)),Sync())).get('/api/library/articles/'+item['id']).json()
    assert value['reference']['levels']==['key_bachelor']
    assert value['rating']['score']==80


@pytest.mark.parametrize('mutation',['sum','level','evidence','cap','rubric'])
def test_invalid_ratings_never_enter_history(catalog,mutation):
    item=source(catalog)
    assert hasattr(catalog,'rate'),'atomic rating writer is missing'
    payload=body(item)
    if mutation=='sum':payload['rating']['score']=81
    if mutation=='level':payload['rating']['dimension_scores'][0]['subitems'][0]['level']=6
    if mutation=='evidence':payload['rating']['evidence_refs'][0]['file_id']='unread-attachment'
    if mutation=='cap':payload['rating'].update(cap_applied=True,score=40,cap_reason='arbitrary')
    if mutation=='rubric':payload['rubric_sha256']='c'*64
    with pytest.raises(ValueError):catalog.rate(item['id'],payload)
    with catalog.connect() as conn:assert conn.execute('SELECT count(*) n FROM article_rating').fetchone()['n']==0


def test_cap_requires_completely_absent_core_not_just_a_flag(catalog):
    item=source(catalog);payload=body(item)
    payload['rating'].update(cap_applied=True,score=50,cap_reason='core supposedly absent')
    with pytest.raises(ValueError,match='核心|封顶'):catalog.rate(item['id'],payload)


def test_new_reference_uses_new_evidence_and_preserves_legacy_separately(catalog):
    data=record();data['profile']['research_profile']['research_type']='engineering_system'
    data['profile']['applicable_level']={'labels':['函授'],'reason':'旧画像的函授依据','limits':'旧范围'}
    data['files']=[{'id':'body','kind':'原文','sha256':'a'*64,'bytes':4}]
    item=catalog.register(data);payload=body(item,tier='key_bachelor');catalog.rate(item['id'],payload)
    value=catalog.detail(item['id'])
    assert value['reference']['labels']==['重点本科参考']
    assert value['reference']['reason']==payload['rating']['tier_reason']
    assert value['legacy_reference']['labels']==['函授']


def test_historical_operation_is_not_the_selected_current_rating(catalog):
    item=source(catalog);old=body(item,tier='bachelor');new=body(item,tier='key_bachelor')
    catalog.rate(item['id'],old);catalog.rate(item['id'],new)
    assert catalog.rating(item['id'],old['operation_id'])['current'] is False
    assert catalog.rating(item['id'],new['operation_id'])['current'] is True


@pytest.mark.parametrize('target',['dimension','subitem','tier','evidence'])
def test_malformed_identifiers_are_validation_errors(catalog,target):
    item=source(catalog);payload=body(item)
    if target=='dimension':payload['rating']['dimension_scores'][0]['id']=[]
    if target=='subitem':payload['rating']['dimension_scores'][0]['subitems'][0]['id']=[]
    if target=='tier':payload['rating']['tier']=[]
    if target=='evidence':payload['rating']['evidence_refs'][0]['file_id']=[]
    with pytest.raises(ValueError):catalog.rate(item['id'],payload)


def profile_body(catalog,item):
    frozen=catalog.rating_input(item['id']);payload=body(item)
    payload.update(evidence_basis='profile',profile_sha256=frozen['profile_sha256'],profile_snapshot=frozen['profile_snapshot'],source_files=[])
    payload['rating']['evidence_refs']=[{'field_path':'/profile_evidence/profile/path','note':'画像中的实际研究路线'}]
    return payload


def test_profile_priority_needs_no_original_and_keeps_all_read_surfaces_consistent(catalog,tmp_path):
    from article_repository.api import create_app
    from article_repository.files import Files
    data=record();data['profile']['research_profile']['research_type']='engineering_system'
    data['profile']['article']['author_degree']='重点大学博士';data['profile']['applicable_level']['score']=99
    item=catalog.register(data);before=catalog.raw(item['id'])
    frozen=catalog.rating_input(item['id'])
    assert 'applicable_level' not in str(frozen['profile_evidence'])
    assert 'author_degree' not in str(frozen['profile_evidence'])
    client=TestClient(create_app(catalog,Files(str(tmp_path)),object()))
    assert client.get('/api/library/articles/'+item['id']+'/rating-input').json()['profile_sha256']==frozen['profile_sha256']
    response=client.post('/api/library/articles/'+item['id']+'/rating',json=profile_body(catalog,item))
    assert response.status_code==200,response.text
    assert client.get('/api/library/articles/'+item['id']).json()['reference']['score']==80
    assert catalog.search(level='bachelor')['rows'][0]['rating']['evidence_basis']=='profile'
    after=catalog.raw(item['id'])
    for key in ('base','overrides','revision','sync','updated_at'):assert before[key]==after[key]


def test_profile_drift_without_revision_rejects_stale_write_and_invalidates_score(catalog):
    item=source(catalog);payload=profile_body(catalog,item);catalog.rate(item['id'],payload)
    with catalog.connect() as conn:
        conn.execute("UPDATE article SET base=jsonb_set(base,'{profile,research_profile,overall_path}',to_jsonb(%s::text)) WHERE id=%s",('changed process',item['id']))
    assert catalog.detail(item['id'])['rating'] is None
    payload['operation_id']=str(uuid.uuid4())
    with pytest.raises(ValueError,match='画像'):catalog.rate(item['id'],payload)


@pytest.mark.parametrize('mutation',['hash','fake_path','fake_original_page','old_answer_path','mixed_file_id','mixed_page'])
def test_profile_rating_requires_actual_snapshot_and_field_evidence(catalog,mutation):
    item=source(catalog);payload=profile_body(catalog,item)
    if mutation=='hash':payload['profile_sha256']='b'*64
    if mutation=='fake_path':payload['rating']['evidence_refs'][0]['field_path']='/profile_evidence/nonexistent'
    if mutation=='fake_original_page':payload['rating']['evidence_refs']=[{'file_id':'body','page':2,'note':'not actually read'}]
    if mutation=='old_answer_path':payload['rating']['evidence_refs'][0]['field_path']='/profile_evidence/applicable_level/labels'
    if mutation=='mixed_file_id':payload['rating']['evidence_refs'][0]['file_id']='fake file'
    if mutation=='mixed_page':payload['rating']['evidence_refs'][0]['page']=999
    with pytest.raises(ValueError):catalog.rate(item['id'],payload)


def test_legacy_effective_profile_reuses_reader_applies_manual_and_binds_hash(catalog,tmp_path):
    from article_repository.api import create_app
    from article_repository.files import Files
    full=record()['profile'];full['research_profile']['research_type']='engineering_system'
    full['learning_units'][0]['method']='old method'
    class Sync:
        def read_profile(self,item):return full
    item=source(catalog,legacy=True)
    item=catalog.edit(item['id'],item['revision'],{'fields':{'methods':'manual method'}})
    client=TestClient(create_app(catalog,Files(str(tmp_path)),Sync()))
    frozen=client.get('/api/library/articles/'+item['id']+'/rating-input').json()
    assert frozen['profile_evidence']['profile']['units'][0]['method']=='manual method'
    payload=profile_body(catalog,item);assert client.post('/api/library/articles/'+item['id']+'/rating',json=payload).status_code==200
    full['research_profile']['overall_path']='new immutable source profile'
    current=client.get('/api/library/articles/'+item['id']).json()
    assert current['rating'] is None
    assert catalog.search()['rows'][0]['rating'] is None
