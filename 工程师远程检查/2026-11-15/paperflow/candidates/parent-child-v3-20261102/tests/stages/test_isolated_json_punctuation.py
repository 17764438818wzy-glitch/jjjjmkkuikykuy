"""Preserve values when recovering one proved extra punctuation line."""
import json
import pytest
import preextract_contract as contract


@pytest.mark.parametrize('newline',['\n','\r\n'])
def test_recover_only_a_quote_between_complete_object_members(newline):
    expected={'units':[{'use_limit':'原有限制','needed_information':['第一条','第二条'],
                       'typed':[None,True,False,0,-3,1.25,'中文𝄞']}],'done':True}
    good=json.dumps(expected,ensure_ascii=False,indent=2)
    raw=good.replace('      "needed_information":', '      ",\n      "needed_information":',1).replace('\n',newline)
    repairs=[]
    assert contract.parse_candidate(raw,repairs=repairs)==expected
    record=next(r for r in repairs if r['policy']=='isolated_member_quote')
    assert record['removed']=='"' and raw[record['offset']]=='"' and record['scalar_sequence_preserved'] is True
    assert record['before_sha256']!=record['after_sha256']


def test_normal_strings_are_unchanged():
    expected={'text':'文字\n",\n文字和 \\" 及中文','units':[{'value':'"','empty':''}]}
    raw=json.dumps(expected,ensure_ascii=False)
    repairs=[]
    assert contract.parse_candidate(raw,repairs=repairs)==expected
    assert not repairs


def test_existing_scalar_guard_still_rejects_repairer_dropping_a_backslash():
    # The mature repairer changes this escape when it also repairs punctuation.
    # Keep rejecting its changed string, rather than broadening the new policy.
    raw=json.dumps({'a':1,'b':'中文𝄞\\"'},ensure_ascii=False,indent=2).replace('  "b":','  ",\n  "b":')
    with pytest.raises(ValueError,match='profile_json_repair_changed_scalar'):
        contract.parse_candidate(raw)


@pytest.mark.parametrize('raw',[
    '{"purpose":"第一段":"第二段"}',
    '{\n"use_limit":\n",\n"needed_information":["a"]\n}',
    '["a",\n",\n"b"]',
    '{"items":["a",\n",\n"b"]}',
    '{"a":"未结束字符串\n",\n"b":"内容"}',
    '{"a":1,\n",\n"b":2,\n",\n"c":3}',
    '{"a":1,", "b":2}',
    '{"a":1,\n",\nb:2}',
    '{"a":"x","a":"y",\n",\n"b":2}',
    '{"a":1,\n",\n"b":}',
    '{"a":1,\n",\n"b":NaN}',
    '{"a":1,\n",\n"b":2]}',
    '{"a":1,\n",\n"b":[2}',
    '{"a":1,\n",\n"b":[2]',
    '{"a":1,\n",\n"b":2,}',
    '{bad:1,\n",\n"b":2}',
])
def test_ambiguous_values_missing_values_and_nonmember_noise_are_rejected(raw):
    with pytest.raises(ValueError):contract.parse_candidate(raw)


def test_existing_closing_and_reference_duplicate_policies_still_work():
    repairs=[]
    assert contract.parse_candidate('{"a":[1,2]',repairs=repairs)=={'a':[1,2]}
    assert any(r['policy']=='close_unclosed_containers' for r in repairs)
    assert contract.parse_candidate('{"a":1,"a":1}')=={'a':1}
