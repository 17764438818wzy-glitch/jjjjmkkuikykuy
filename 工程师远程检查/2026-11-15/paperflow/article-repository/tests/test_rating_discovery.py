import copy
import uuid
from datetime import datetime,timedelta,timezone

import pytest

from test_catalog import catalog,record


def test_event_keyset_discovers_register_edit_and_same_revision_profile(catalog):
    item=catalog.register(record());aid=item['id']
    events=catalog.rating_discovery(limit=1)
    assert [r['article_id'] for r in events['rows']]==[aid]
    cursor=events['next_after']
    catalog.register(record())
    assert not catalog.rating_discovery(after=cursor)['rows']
    packet=catalog.rating_input(aid)
    first=catalog.rating_discovery(after=cursor);cursor=first['next_after']
    assert len(first['rows'])==1 and first['rows'][0]['profile_sha256']==packet['profile_sha256']
    catalog.rating_input(aid)
    assert not catalog.rating_discovery(after=cursor)['rows']
    changed=copy.deepcopy(packet['profile_snapshot']['profile']);changed['article']['summary']='完整画像发生变化'
    catalog.observe_rating_profile(aid,item['revision'],changed)
    event=catalog.rating_discovery(after=cursor)['rows'][0]
    assert event['revision']==item['revision'] and event['profile_sha256']!=packet['profile_sha256']
    catalog.edit(aid,item['revision'],{'title':'修订标题'})
    assert catalog.rating_discovery(after=event['sequence'])['rows'][0]['revision']==item['revision']+1


def test_bootstrap_cursor_keeps_new_registration_and_edits_discoverable(catalog):
    first=catalog.register(record('one'));second=catalog.register(record('two'))
    page=catalog.rating_bootstrap((datetime.now(timezone.utc)-timedelta(days=1)).isoformat(),limit=1)
    assert len(page['rows'])==1 and page['next_cursor']
    third=catalog.register(record('three'))
    catalog.edit(second['id'],second['revision'],{'title':'更新'})
    seen={page['rows'][0]['article_id']};cursor=page['next_cursor']
    while cursor:
        more=catalog.rating_bootstrap('',cursor=cursor,limit=1)
        seen.update(r['article_id'] for r in more['rows']);cursor=more['next_cursor']
    seen.update(r['article_id'] for r in catalog.rating_discovery(after=page['event_checkpoint'])['rows'])
    assert seen=={first['id'],second['id'],third['id']}


def classification(packet):
    return {'operation_id':str(uuid.uuid4()),'request_id':str(uuid.uuid4()),'run_id':'test-run',
        'revision':packet['revision'],'profile_sha256':packet['profile_sha256'],
        'research_type':'engineering_system','model':'test-model'}


def test_category_cas_is_atomic_idempotent_and_preserves_originals(catalog):
    source=record();item=catalog.register(source);packet=catalog.rating_input(item['id']);body=classification(packet)
    before=catalog.raw(item['id']);receipt=catalog.classify(item['id'],body)
    assert receipt['revision']==item['revision']+1
    assert catalog.classify(item['id'],body)['replayed']
    assert catalog.classification(item['id'],body['operation_id'])['payload_digest']==receipt['payload_digest']
    after=catalog.raw(item['id']);expected=copy.deepcopy(before['base']);expected['profile']['research_profile']['research_type']='engineering_system'
    assert after['base']==expected and after['overrides']==before['overrides'] and after['sync']==before['sync']
    assert catalog.rating_input(item['id'])['research_type']=='engineering_system'
    with pytest.raises(ValueError):catalog.classify(item['id'],{**body,'research_type':'creative_design'})
    current=catalog.rating_input(item['id'])
    with pytest.raises(ValueError,match='已有有效'):catalog.classify(item['id'],classification(current))


def test_category_cas_rejects_changed_effective_profile_without_touching_revision(catalog):
    item=catalog.register(record());packet=catalog.rating_input(item['id']);body=classification(packet)
    changed=copy.deepcopy(packet['profile_snapshot']['profile']);changed['article']['summary']='新画像'
    catalog.observe_rating_profile(item['id'],item['revision'],changed)
    with pytest.raises(ValueError,match='画像'):catalog.classify(item['id'],body)
    assert catalog.raw(item['id'])['revision']==item['revision']


def test_non_article_records_are_excluded_before_classification_or_rating(catalog):
    source=record();source['entry_kind']='attachment';item=catalog.register(source)
    assert not catalog.rating_discovery()['rows']
    assert not catalog.rating_bootstrap('2026-01-01T00:00:00+08:00')['rows']
    with pytest.raises(ValueError,match='不是文章'):catalog.rating_input(item['id'])
    with pytest.raises(ValueError,match='不是文章'):catalog.classify(item['id'],classification({'revision':1,'profile_sha256':'a'*64}))
