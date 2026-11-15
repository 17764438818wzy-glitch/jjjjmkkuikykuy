import pytest
from paperflow.package import PackageError,load,save,seal_package,validate_package


@pytest.mark.parametrize('value',['13800138000','test.person@example.com','110101199001011234','学号：202020201040'])
def test_public_pattern_is_advisory_without_rejecting_deepseek(tmp_path,package_factory,value):
    root,task,catalog=package_factory()
    profile=load(root/'profile.json');profile['note']=value;save(root/'profile.json',profile)
    seal_package(root,task,1)
    result=validate_package(root,task,catalog)
    assert result['routes']
    assert value not in str(result.get('compatibility_notes',[]))


def test_llm_private_name_map_is_advisory_and_does_not_reject_its_result(tmp_path,package_factory):
    from paperflow.package import privacy_scan
    root,task,catalog=package_factory()
    profile=load(root/'profile.json');profile['note']='小米公司 作者张三';save(root/'profile.json',profile)
    mapping=tmp_path/'private/redaction_map.json'
    save(mapping,dict(values=[dict(original='张三',replacement='作者A',kind='author'),
                              dict(original='小米公司',kind='research_object',allow_public=True)]))
    task['redaction_map_private']=str(mapping);seal_package(root,task,1)
    assert any(n['rule']=='llm_private_map_residual' for n in privacy_scan(root,task))
    assert validate_package(root,task,catalog)['routes']
    profile['note']='小米公司 作者A';save(root/'profile.json',profile);seal_package(root,task,1)
    assert validate_package(root,task,catalog)['routes']


def test_single_character_in_auxiliary_map_does_not_block_normal_ingestion(tmp_path,package_factory):
    root,task,catalog=package_factory()
    mapping=tmp_path/'private/redaction_map.json'
    save(mapping,dict(values=[dict(original='文',replacement='',kind='author',allow_public=False)]))
    task['redaction_map_private']=str(mapping);seal_package(root,task,1)
    assert validate_package(root,task,catalog)['routes']
    profile=load(root/'profile.json');profile['contact']='13800138000';save(root/'profile.json',profile)
    seal_package(root,task,1)
    assert validate_package(root,task,catalog)['routes']
