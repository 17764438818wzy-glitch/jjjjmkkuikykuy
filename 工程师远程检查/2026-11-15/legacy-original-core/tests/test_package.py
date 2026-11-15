import hashlib
import json

import pytest

from paperflow.package import PackageError, publish_package, validate_package


def test_description_only_image_needs_no_binary_and_no_old_numbers(package_factory):
    root, task, catalog = package_factory()
    result = validate_package(root, task, catalog)
    assert result['routes'][0]['paper_dataset_id'] == 'paper-ds'
    assert result['assets'][0]['storage'] == 'description'
    assert next(c for c in result['chunks'] if c['kind']=='learning')['asset_ids'] == ['fig1']


def test_nested_asset_storage_is_a_format_variant_not_a_new_model_task(package_factory):
    from paperflow.package import load,save,sha,normalize_asset_storage,seal_package
    root,task,catalog=package_factory()
    obj=load(root/'assets.json');a=obj['items'][0]
    description=a.pop('description');a['storage']={'mode':'description','description':description}
    save(root/'assets.json',obj)
    original={n:sha(root/n) for n in ('article.redacted.md','profile.json','tree.json','chunks.json')}
    normalize_asset_storage(root);first=sha(root/'assets.json');normalize_asset_storage(root)
    assert sha(root/'assets.json')==first
    assert load(root/'assets.json')['items'][0]['description']==description
    assert all(sha(root/n)==digest for n,digest in original.items())
    seal_package(root,task,1);assert validate_package(root,task,catalog)['assets'][0]['storage']=='description'


def test_nested_retained_image_fields_are_preserved_exactly(tmp_path):
    from paperflow.package import load,save,normalize_asset_storage
    nested=dict(mode='image',file='assets/chart.png',source_image_id='im_source',description='必要视觉结构',redactions=[])
    save(tmp_path/'assets.json',dict(items=[dict(id='a1',storage=nested)]))
    normalize_asset_storage(tmp_path);a=load(tmp_path/'assets.json')['items'][0]
    assert a['storage']=='image'
    assert all(a[k]==v for k,v in nested.items() if k!='mode')


@pytest.mark.parametrize('mode',['image','description'])
def test_outer_empty_placeholders_do_not_hide_nested_asset_values(tmp_path,mode):
    from paperflow.package import load,save,normalize_asset_storage
    nested=dict(mode=mode,file='assets/a.png',description='模型原描述',source_image_id='im_a')
    save(tmp_path/'assets.json',dict(items=[dict(id='a',file=None,description='',storage=nested)]))
    normalize_asset_storage(tmp_path);a=load(tmp_path/'assets.json')['items'][0]
    assert a['file']=='assets/a.png' and a['description']=='模型原描述'


def test_omitted_decoration_does_not_require_a_research_section(package_factory):
    from paperflow.package import load,save,seal_package
    root,task,catalog=package_factory();obj=load(root/'assets.json')
    obj['items'].append(dict(id='logo',storage='omit',kind='decoration'))
    save(root/'assets.json',obj);seal_package(root,task,1)
    assert validate_package(root,task,catalog)['routes']


def test_auxiliary_reference_differences_do_not_trigger_semantic_repair(package_factory):
    from paperflow.package import load,save,sha,seal_package
    root,task,catalog=package_factory();a=load(root/'assets.json');a['items'][0]['section_id']='model_section_alias'
    c=load(root/'chunks.json');c['chunks'][1].update(section_ids=['another_alias'],asset_ids=['model_asset_alias'])
    save(root/'assets.json',a);save(root/'chunks.json',c);seal_package(root,task,1)
    before={n:sha(root/n) for n in ('assets.json','chunks.json')}
    result=validate_package(root,task,catalog)
    assert len(result['compatibility_notes'])==3
    assert all(sha(root/n)==digest for n,digest in before.items())


def test_changed_published_bytes_are_rejected(package_factory):
    root, task, catalog = package_factory()
    (root / 'article.redacted.md').write_text('changed', encoding='utf-8')
    with pytest.raises(PackageError, match='hash|size'):
        validate_package(root, task, catalog)


def test_manifest_cannot_refer_outside_delivery(package_factory):
    root, task, catalog = package_factory()
    manifest = json.loads((root / 'manifest.json').read_text(encoding='utf-8'))
    manifest['files'].append({'path': '../private.json', 'bytes': 0, 'sha256': 'a' * 64})
    (root / 'manifest.json').write_text(json.dumps(manifest), encoding='utf-8')
    ready = json.loads((root / 'READY.json').read_text(encoding='utf-8'))
    ready['manifest_sha256'] = hashlib.sha256((root / 'manifest.json').read_bytes()).hexdigest()
    (root / 'READY.json').write_text(json.dumps(ready), encoding='utf-8')
    with pytest.raises(PackageError, match='path'):
        validate_package(root, task, catalog)


def test_publish_never_overwrites_an_existing_revision(package_factory, tmp_path):
    root, task, catalog = package_factory()
    target = tmp_path / 'published' / 'r01'
    publish_package(root, target)
    assert (target / 'READY.json').exists()
    other, _, _ = package_factory('different')
    with pytest.raises(PackageError, match='immutable|exists'):
        publish_package(other, target)


def test_unknown_specialty_does_not_fall_back_to_another_library(package_factory):
    root, task, catalog = package_factory()
    catalog['majors'] = []
    result=validate_package(root,task,catalog)
    assert result['routes']==[]
    assert result['pending_routes']==[dict(code='120201',reason='unknown specialty')]


def test_learning_only_chunks_cannot_hide_missing_body(package_factory):
    from paperflow.package import seal_package
    root, task, catalog = package_factory()
    obj=json.loads((root/'chunks.json').read_text(encoding='utf-8'))
    obj['chunks']=[c for c in obj['chunks'] if c['kind']!='body']
    (root/'chunks.json').write_text(json.dumps(obj),encoding='utf-8')
    seal_package(root,task,1)
    with pytest.raises(PackageError,match='body coverage'):
        validate_package(root,task,catalog)


def test_omitted_paragraph_is_a_technical_coverage_error(package_factory):
    from paperflow.package import seal_package
    root, task, catalog = package_factory()
    obj=json.loads((root/'chunks.json').read_text(encoding='utf-8'))
    obj['chunks'][0]['content']='# 研究方法'
    (root/'chunks.json').write_text(json.dumps(obj),encoding='utf-8')
    seal_package(root,task,1)
    with pytest.raises(PackageError,match='body coverage'):
        validate_package(root,task,catalog)
