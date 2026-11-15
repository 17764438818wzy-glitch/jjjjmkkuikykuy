import copy
import uuid

import pytest

from test_catalog import catalog,record
from test_rating_discovery import classification


def claim_body(packet,stage='classification'):
    return {**classification(packet),'stage':stage,'rubric_sha256':'a'*64,'research_type':stage,'request_sha256':'c'*64}


def begin_body(body):return {k:body[k] for k in ('run_id','request_id','operation_id','model','request_sha256')}


def test_version_claim_is_unique_across_runs_and_old_unknown_blocks_new_version(catalog):
    item=catalog.register(record());packet=catalog.rating_input(item['id']);body=claim_body(packet)
    first=catalog.claim_rating(item['id'],body)
    assert first['granted'] is True
    assert catalog.claim_rating(item['id'],body)['granted'] is True  # Same owner before its one send permit.
    assert catalog.begin_rating_claim(item['id'],begin_body(body))['dispatch_granted']
    assert not catalog.begin_rating_claim(item['id'],begin_body(body))['dispatch_granted']
    assert not catalog.claim_rating(item['id'],body)['granted']
    other={**body,'run_id':'different-run','request_id':str(uuid.uuid4()),'operation_id':str(uuid.uuid4())}
    assert catalog.claim_rating(item['id'],other)['granted'] is False
    changed=copy.deepcopy(packet['profile_snapshot']['profile']);changed['article']['summary']='变更画像'
    catalog.observe_rating_profile(item['id'],item['revision'],changed)
    current=catalog.rating_input(item['id'])
    assert catalog.claim_rating(item['id'],claim_body(current))['reason']=='prior_unresolved'


def test_claim_terminal_evidence_allows_new_version_but_never_resends_same_version(catalog):
    item=catalog.register(record());packet=catalog.rating_input(item['id']);body=claim_body(packet)
    catalog.claim_rating(item['id'],body);catalog.begin_rating_claim(item['id'],begin_body(body))
    terminal={k:body[k] for k in ('run_id','request_id','operation_id')}
    terminal['native_state']={'status':'response_complete','finish_reason':'stop','response_sha256':'b'*64,'request_sha256':'c'*64,'client_http_requests':1,'model':body['model']}
    catalog.finish_rating_claim(item['id'],terminal)
    assert catalog.claim_rating(item['id'],body)['granted'] is False
    changed=copy.deepcopy(packet['profile_snapshot']['profile']);changed['article']['summary']='变更画像'
    catalog.observe_rating_profile(item['id'],item['revision'],changed)
    assert catalog.claim_rating(item['id'],claim_body(catalog.rating_input(item['id'])))['granted'] is True


def test_unfinished_claim_cannot_be_released_by_expiry_or_incomplete_stream(catalog):
    item=catalog.register(record());body=claim_body(catalog.rating_input(item['id']));catalog.claim_rating(item['id'],body)
    with pytest.raises(ValueError):catalog.finish_rating_claim(item['id'],{
        **{k:body[k] for k in ('run_id','request_id','operation_id')},'native_state':{'status':'partial_response','finish_reason':None}})


def test_category_publication_completes_matching_claim_in_same_transaction(catalog):
    item=catalog.register(record());packet=catalog.rating_input(item['id']);body=classification(packet)
    claim={**body,'stage':'classification','research_type':'classification','rubric_sha256':'a'*64,'request_sha256':'c'*64}
    catalog.claim_rating(item['id'],claim);catalog.classify(item['id'],body)
    with catalog.connect() as conn:
        assert conn.execute('SELECT state FROM article_rating_claim').fetchone()['state']=='published'


def test_simultaneous_different_run_claims_have_one_owner(catalog):
    from concurrent.futures import ThreadPoolExecutor
    item=catalog.register(record());packet=catalog.rating_input(item['id'])
    bodies=[{**claim_body(packet),'run_id':str(uuid.uuid4())} for _ in range(8)]
    with ThreadPoolExecutor(max_workers=8) as pool:results=list(pool.map(lambda b:catalog.claim_rating(item['id'],b),bodies))
    assert sum(r['granted'] for r in results)==1


def test_score_claim_and_publication_api_share_profile_version(catalog,tmp_path):
    from fastapi.testclient import TestClient
    from article_repository.api import create_app
    from article_repository.files import Files
    from test_ratings import source,profile_body
    item=source(catalog);payload=profile_body(catalog,item)
    claim={k:payload[k] for k in ('operation_id','request_id','run_id','revision','profile_sha256','research_type','model','rubric_sha256')}
    claim.update(stage='rating',request_sha256='c'*64);client=TestClient(create_app(catalog,Files(str(tmp_path)),object()))
    assert client.post('/api/library/articles/'+item['id']+'/rating-claim',json=claim).json()['granted']
    assert client.post('/api/library/articles/'+item['id']+'/rating',json=payload).status_code==200
    with catalog.connect() as conn:assert conn.execute('SELECT state FROM article_rating_claim').fetchone()['state']=='published'
    another={**claim,'run_id':'another-run','request_id':str(uuid.uuid4()),'operation_id':str(uuid.uuid4())}
    assert not client.post('/api/library/articles/'+item['id']+'/rating-claim',json=another).json()['granted']


def test_preexisting_native_rating_without_claim_still_prevents_duplicate_model(catalog):
    from test_ratings import source,profile_body
    item=source(catalog);payload=profile_body(catalog,item);catalog.rate(item['id'],payload)
    claim={k:payload[k] for k in ('revision','profile_sha256','research_type','model','rubric_sha256')}
    claim.update(stage='rating',run_id='new-run',request_id=str(uuid.uuid4()),operation_id=str(uuid.uuid4()),request_sha256='c'*64)
    assert catalog.claim_rating(item['id'],claim)['reason']=='already_rated'


@pytest.mark.parametrize('key,value',[('request_sha256','d'*64),('model','another-model')])
def test_different_native_request_cannot_release_an_unknown_claim(catalog,key,value):
    item=catalog.register(record());body=claim_body(catalog.rating_input(item['id']))
    catalog.claim_rating(item['id'],body);catalog.begin_rating_claim(item['id'],begin_body(body))
    proof={'status':'response_complete','finish_reason':'stop','response_sha256':'b'*64,
        'request_sha256':body['request_sha256'],'model':body['model'],'client_http_requests':1,key:value}
    with pytest.raises(ValueError,match='原模型请求'):
        catalog.finish_rating_claim(item['id'],{**{k:body[k] for k in ('run_id','request_id','operation_id')},'native_state':proof})
    with catalog.connect() as conn:assert conn.execute('SELECT state FROM article_rating_claim').fetchone()['state']=='sending'


def test_new_current_version_revokes_only_a_reservation_that_never_got_send_permit(catalog):
    item=catalog.register(record());packet=catalog.rating_input(item['id']);old=claim_body(packet)
    catalog.claim_rating(item['id'],old)
    changed=copy.deepcopy(packet['profile_snapshot']['profile']);changed['article']['summary']='新画像事实'
    catalog.observe_rating_profile(item['id'],item['revision'],changed)
    current=catalog.rating_input(item['id']);new=claim_body(current)
    assert catalog.claim_rating(item['id'],new)['granted']
    assert not catalog.begin_rating_claim(item['id'],begin_body(old))['dispatch_granted']
    assert catalog.begin_rating_claim(item['id'],begin_body(new))['dispatch_granted']
