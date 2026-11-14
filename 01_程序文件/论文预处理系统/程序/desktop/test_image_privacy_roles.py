"""Image identity checks must preserve public citations across all review phases."""
import json

import pytest

from test_common_contracts import client, picture


QUOTE='被引学者的教学对话：学者甲说：“让儿童表达自己的想法。”'


@pytest.mark.parametrize('phase',['identify','verify_masked','locate_privacy_regions'])
def test_image_name_role_rules_reach_real_request_in_every_privacy_phase(monkeypatch,phase):
    response={'decision':'pass','reason':'公开引述中的学者姓名为研究文字',
              'research':True,'sensitive_boxes':[],'text':QUOTE,'description':'公开教学对话引文截图'}
    adapter=client(monkeypatch,response)
    result=adapter.call('privacy_image',{'id':'quoted_dialogue','phase':phase,
      'context':{'caption':'某学者公开发表的课堂实录','nearby_text':QUOTE}},[picture()])
    request=adapter.opener.requests[0]
    system=request['messages'][0]['content']
    # These distinctions are part of the emitted model contract, not a local
    # whitelist that silently changes a response or bypasses validation.
    assert all(role in system for role in ('被引学者','文献作者','历史人物','公开经营信息'))
    assert '按人物角色' in system
    assert '私人姓名' in system
    assert '同一姓名' in system
    payload=json.loads(request['messages'][1]['content'][0]['text'])['document_data']
    assert payload['context']['nearby_text']==QUOTE
    assert result['text']==QUOTE
    assert result['sensitive_boxes']==[]


def test_actual_identity_boxes_are_not_removed_by_public_citation_policy(monkeypatch):
    result={'decision':'pass','reason':'同页参与者私人署名有明确身份字段，引用学者处保留',
            'research':True,'sensitive_boxes':[[.1,.2,.3,.4]],
            'text':'参与学生：[已脱敏]\n'+QUOTE,'description':'研究对话及参与者字段'}
    adapter=client(monkeypatch,result)
    assert adapter.call('privacy_image',{'phase':'verify_masked'},[picture()])==result
    assert len(adapter.opener.requests)==1


def test_public_citation_policy_does_not_override_unlocatable_private_identity(monkeypatch):
    result={'decision':'reject','reason':'真实参与者私人姓名存在但无法可靠定位'}
    adapter=client(monkeypatch,result)
    assert adapter.call('privacy_image',{'phase':'locate_privacy_regions'},[picture()])==result
    assert len(adapter.opener.requests)==1
