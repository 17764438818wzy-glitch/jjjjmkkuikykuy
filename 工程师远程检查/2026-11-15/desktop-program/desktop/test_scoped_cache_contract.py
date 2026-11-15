"""Only exportable scoped responses may become durable retry checkpoints."""
import copy

import fitz
import pytest

from auto_evidence import Rejected
from evidence import export_package
from evidence_tasks import resolve_scoped
from engine import digest
from test_stage_resume import cached_client, Transport


def source_page(tmp_path):
    source=tmp_path/'source.pdf'
    with fitz.open() as pdf:
        page=pdf.new_page(width=160,height=120)
        page.draw_line((10,10),(80,90))
        pdf.save(source)
    return source


def recovered(entry):
    return {'decision':'pass','reason':'read original page','sensitive_boxes':[], 'blocks':[entry]}


def metadata():
    return {'id':'fixture','title':'Synthetic research','blocks':[], 'resources':[], 'formulas':[],
            'issues':[{'code':'scanned_page','position':{'page':1},'status':'open'}]}


def test_null_optional_table_fields_export_and_replay_without_network(tmp_path):
    source=source_page(tmp_path)
    entry={'kind':'table','rows':[['Year','Value'],['2024','100.00']],
           'caption':None,'units':None,'notes':None}
    first=cached_client(tmp_path/'data',digest(source),transport=Transport(lambda _:recovered(entry)))
    meta=metadata()
    resolve_scoped(meta,tmp_path,source,tmp_path/'visual',80,first,lambda *a:None)
    export_package(tmp_path,meta)
    block=meta['blocks'][0]
    assert block['caption']=='' and block['units']=='' and block['notes']==[]
    assert [c['text'] for c in block['cells']]==['Year','Value','2024','100.00']
    assert len(first.opener.requests)==1
    def no_network(_):raise AssertionError('Accepted table must replay')
    second=cached_client(tmp_path/'data',digest(source),transport=Transport(no_network))
    again=metadata()
    resolve_scoped(again,tmp_path,source,tmp_path/'visual2',80,second,lambda *a:None)
    export_package(tmp_path,again)
    assert second.last_call_cache_hit and not second.opener.requests
    assert again['blocks'][0]['cells']==block['cells']


@pytest.mark.parametrize('kind,field,value',[
    ('table','caption',{'bad':'shape'}),('table','units',123),
    ('table','notes','must be an array'),('table','notes',[None]),
    ('table','notes',[{'bad':'shape'}]),('image','caption',{'bad':'shape'})])
def test_invalid_optional_scoped_fields_never_become_cache(tmp_path,kind,field,value):
    source=source_page(tmp_path);(tmp_path/'assets').mkdir()
    entry={'kind':'table','rows':[['A','B'],['1','2']]} if kind=='table' else {
        'kind':'image','bbox':[.1,.1,.8,.8],'text':'Original values','description':'Observed diagram'}
    entry[field]=copy.deepcopy(value)
    client=cached_client(tmp_path/'data',digest(source),transport=Transport(lambda _:recovered(entry)))
    with pytest.raises(Rejected):
        resolve_scoped(metadata(),tmp_path,source,tmp_path/'visual',80,client,lambda *a:None)
    client.discard_unaccepted_results()
    assert not list((tmp_path/'data/private/stage_cache').glob('*.json'))
