import copy
from pathlib import Path
import sys
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'maintenance'))
from title_cas import correct
from test_catalog import catalog,record

def plan(catalog):
    row=record('cas','English abstract translation')
    row['source_sha256']='a'*64
    item=catalog.register(row);raw=catalog.raw(item['id'])
    return {'id':item['id'],'revision':item['revision'],'old_title':item['title'],
        'source_key':raw['source_key'],'source_sha256':'a'*64,'overrides':{},
        'title':'原文中文题名','evidence':{'kind':'verified_intake_title','source_sha256':'a'*64}},raw

def test_display_correction_does_not_change_base_revision_or_discovery(catalog):
    p,before=plan(catalog)
    with catalog.connect() as conn:
        result=correct(conn,p)
    after=catalog.raw(p['id'])
    assert result['status']=='corrected'
    assert catalog.detail(p['id'])['title']=='原文中文题名'
    assert catalog.search(keyword='原文中文题名')['total']==1
    for key in ('base','revision','sync','updated_at'):assert before[key]==after[key]

def test_stale_review_is_rejected_and_manual_title_is_protected(catalog):
    p,before=plan(catalog);p['source_sha256']='b'*64;p['evidence']['source_sha256']='b'*64
    with pytest.raises(ValueError,match='identity'):
        with catalog.connect() as conn:correct(conn,p)
    assert catalog.raw(p['id'])==before
    catalog.edit(p['id'],p['revision'],{'title':'人工确认题名'})
    before=catalog.raw(p['id'])
    with catalog.connect() as conn:assert correct(conn,p)['status']=='protected_manual_title'
    assert catalog.raw(p['id'])==before
