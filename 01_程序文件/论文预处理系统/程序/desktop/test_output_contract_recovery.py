"""Recover observed official reasoning exhaustion without weakening evidence checks."""
import copy
import json

import pytest

from agent_client import AgentClient, AgentError
from test_common_contracts import Transport, picture, repair
from test_visual_page_protocol_105 import PAYLOAD, GOOD


@pytest.mark.parametrize('task,result', [
    ('repair', repair([.1, .1, .8, .8])),
    ('extract_page', {'decision':'pass','reason':'read','sensitive_boxes':[],
                      'blocks':[{'kind':'text','text':'原文数值 82.31'}]}),
])
def test_official_structural_transcription_requests_answer_budget_not_default_thinking(task,result,tmp_path):
    adapter=AgentClient({'base_url':'https://api.deepseek.com/v1','vision_model':'deepseek-flash',
                         'attempts':3},'fixture',state_root=tmp_path)
    adapter.opener=Transport(result)
    assert adapter.call(task,{'page':1},[picture()])==result
    sent=adapter.opener.requests[0]
    assert sent.get('thinking')=={'type':'enabled'}
    assert sent.get('reasoning_effort')=='low'
    assert sent.get('response_format')=={'type':'json_object'}


@pytest.mark.parametrize('base,task,result', [
    ('https://relay.test/v1','repair',repair([.1,.1,.8,.8])),
    ('https://api.deepseek.com/v1','privacy_image',{'decision':'pass','reason':'safe',
      'research':True,'sensitive_boxes':[],'text':'12%','description':'chart'}),
])
def test_provider_specific_structural_policy_does_not_change_other_tasks(base,task,result,tmp_path):
    adapter=AgentClient({'base_url':base,'vision_model':'deepseek-flash','attempts':3},'fixture',state_root=tmp_path)
    adapter.opener=Transport(result)
    assert adapter.call(task,{'page':1},[picture()])==result
    assert 'thinking' not in adapter.opener.requests[0]
    assert 'reasoning_effort' not in adapter.opener.requests[0]
    assert 'response_format' not in adapter.opener.requests[0]


def test_initial_visual_request_includes_exact_mask_and_object_constraints(monkeypatch):
    from test_common_contracts import client
    adapter=client(monkeypatch,GOOD)
    assert adapter.call('repair',PAYLOAD,[picture(),picture()])==GOOD
    system=adapter.opener.requests[0]['messages'][0]['content']
    constraints=json.loads(system.split('\nPAGE_CONSTRAINTS=',1)[1])
    assert constraints['image_pages']==[8,9]
    assert constraints['target_ids']==['left','right']
    assert constraints['target_candidate_pages']=={'left':[8],'right':[9]}
    assert constraints['page_masks_required_pages']==[8,9]
    assert constraints['sensitive_boxes_must_be_empty'] is True


def test_unknown_object_and_wrong_page_are_still_rejected(monkeypatch):
    from test_common_contracts import client
    bad=copy.deepcopy(GOOD);bad['insertions'][0]['after_id']='invented'
    adapter=client(monkeypatch,bad,bad,bad)
    with pytest.raises(AgentError,match='after_id'):
        adapter.call('repair',PAYLOAD,[picture(),picture()])
    assert len(adapter.opener.requests)==3


def test_single_page_constraint_prohibits_page_masks_and_includes_full_table_bbox(monkeypatch):
    from test_common_contracts import client
    good=repair([.1,.1,.8,.8])
    payload={'pages':[7],'known_blocks':[{'id':'b1','type':'object','table_candidate':True,
      'table_bbox':[.1,.1,.8,.8],'candidate_pages':[7]}]}
    good['insertions'][0]['page']=7
    adapter=client(monkeypatch,good)
    assert adapter.call('repair',payload,[picture()])==good
    system=adapter.opener.requests[0]['messages'][0]['content']
    constraints=json.loads(system.split('\nPAGE_CONSTRAINTS=',1)[1])
    assert constraints['page_masks_required_pages']==[]
    assert constraints['page_masks_must_be_empty'] is True
    assert constraints['target_table_bboxes']=={'b1':[.1,.1,.8,.8]}
