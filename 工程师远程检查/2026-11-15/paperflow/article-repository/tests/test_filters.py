import copy

import pytest

from article_repository import profiles


def test_reference_keeps_explicit_primary_and_includes_key_bachelor_reference():
    profile={'applicable_level':{'primary_label':'普通本科','labels':['普通本科','重点本科（参照）'],
        'reason':'重点本科仅作结构参照，未达到研究生深度'}}
    value=profiles.reference(profile)
    assert value['primary']=='普通本科'
    assert value['levels']==['bachelor','key_bachelor']
    assert value['reference_only'] is True
    assert value['labels']==profile['applicable_level']['labels']


@pytest.mark.parametrize('profile',[
    {'education_level':{'level_as_stated':'重点本科'}},
    {'applicable_level':{'labels':['教学案例'],'reason':'未达到重点本科要求'}},
    {'applicable_level':{'labels':['不适用重点本科']}},
    {'applicable_level':{'labels':['不适用本科']}},
])
def test_reference_does_not_guess_from_author_education_or_negative_reason(profile):
    value=profiles.reference(profile)
    assert value['levels']==[]
    assert value['primary']==''


def test_legacy_labels_and_manual_override_have_same_reference_contract():
    profile={'applicable_levels':'普通本科；重点本科（参照）；函授/继续教育'}
    assert profiles.reference(profile)['levels']==['correspondence','bachelor','key_bachelor']
    assert profiles.reference(profile,{'level':'函授参考'})['levels']==['correspondence']
    assert profiles.reference(profile,{'level':''})['levels']==[]


def test_profile_details_expose_learning_without_private_source_identity_or_mutation():
    profile={'article':{'original_author_field':'private'},'research_profile':{'problem':'选址问题','overall_path':'建模→求解'},
        'learning_units':[{'id':'U1','title':'求解模型','is_core':True,'what_to_learn':'遗传算法求解',
            'materials':[{'description':'区域需求'}],'steps':[{'operation':'编码并求解'}],
            'outputs':[{'content':'分配方案'}],'limitations':['案例不能直接照搬']}],
        'migration_boundary':{'must_recollect':['新需求数据']},
        'gaps':[{'type':'extraction_missing','description':'公式缺失','use_limit':'不能精确复现'}]}
    original=copy.deepcopy(profile)
    result=profiles.details(profile)
    assert result['question']=='选址问题'
    assert result['units'][0]['steps']==['编码并求解']
    assert result['units'][0]['outputs']==['分配方案']
    assert result['needs']==['新需求数据']
    assert result['gaps'][0]['type']=='extraction_missing'
    assert 'private' not in str(result)
    assert profile==original


def test_historical_overall_path_is_not_lost_and_manual_clear_stays_empty():
    profile={'research_process':{'overall_path':'文献梳理→文本细读→人物对比'},
        'migration_boundary':{'reusable_scope':'原来的借鉴内容'}}
    result=profiles.details(profile,{'learn':''})
    assert result['path']=='文献梳理→文本细读→人物对比'
    assert result['learn']==''


def test_manual_empty_audience_does_not_restore_original_audience_labels():
    profile={'applicable_level':{'audience_labels':['英语专业本科生']}}
    assert profiles.details(profile,{'audience':''})['audience']==''
