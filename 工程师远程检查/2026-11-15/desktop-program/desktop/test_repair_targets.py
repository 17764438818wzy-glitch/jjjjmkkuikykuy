import copy
import pytest
from test_common_contracts import client,picture

def response(entry):
    return {'decision':'pass','reason':'原文核对','sensitive_boxes':[],
            'insertions':[{'after_id':'b1','page':1,**entry}]}

@pytest.mark.parametrize('entry,marker',[
    ({'kind':'text','text':'正文','page':7},'page'),
    ({'kind':'table','rows':[['原文']],'after_id':'invented'},'after_id'),
    ({'kind':'table','rows':[['原文']],'after_id':[]},'after_id'),
    ({'kind':'table','rows':[['原文']],'operation':'replace_table'},'整表替换'),
])
def test_invalid_target_is_retried_at_response_boundary(monkeypatch,entry,marker):
    good=response({'kind':'text','text':'完整原文'})
    c=client(monkeypatch,response(entry),good)
    payload={'page':1,'known_blocks':[{'id':'b1','type':'paragraph','candidate_pages':[1]}]}
    assert c.call('repair',payload,[picture()])==good
    assert len(c.opener.requests)==2
    assert marker in c.opener.requests[1]['messages'][-1]['content']
    assert c.opener.requests[0]['messages'][:2]==c.opener.requests[1]['messages'][:2]

def test_replacement_cannot_repeat_same_table(monkeypatch):
    good=response({'kind':'table','rows':[['原文']],'operation':'replace_table'})
    bad=copy.deepcopy(good);bad['insertions']*=2
    c=client(monkeypatch,bad,good)
    assert c.call('repair',{'page':1,'known_blocks':[{'id':'b1','type':'table'}]},[picture()])==good
    assert len(c.opener.requests)==2

def test_page_valid_for_batch_but_not_target_is_retried(monkeypatch):
    good=response({'kind':'text','text':'完整原文'})
    good['page_masks']=[{'page':1,'boxes':[]},{'page':2,'boxes':[]}]
    bad=copy.deepcopy(good);bad['insertions'][0]['page']=2
    c=client(monkeypatch,bad,good)
    payload={'pages':[1,2],'known_blocks':[{'id':'b1','type':'paragraph','candidate_pages':[1]}]}
    assert c.call('repair',payload,[picture(),picture()])==good
    assert len(c.opener.requests)==2
