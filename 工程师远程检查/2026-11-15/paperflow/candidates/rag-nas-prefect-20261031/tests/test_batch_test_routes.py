"""Route planning with synthetic sealed-package and dataset observations."""
import copy
import hashlib
import json

import pytest

import live_rag_selftest  # Binds the existing package implementation, without making requests.
import paperflow.package as package


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True).encode('utf-8')


def record_file(root, manifest, name, value):
    raw = encode(value)
    (root / name).write_bytes(raw)
    manifest['files'] = [entry for entry in manifest['files'] if entry['path'] != name]
    manifest['files'].append({'path': name, 'sha256': hashlib.sha256(raw).hexdigest()})


@pytest.fixture
def case(tmp_path, monkeypatch):
    from batch_test_routes import prepare_test_routes

    majors = [dict(code=f'{n:06d}', name=f'Synthetic {n}',
                   paper_dataset_id=f'{n:032x}', image_dataset_id=f'{n + 100:032x}')
              for n in range(1, 22)]
    catalog = {'majors': majors}
    catalog_path = tmp_path / 'catalog.json'
    raw_catalog = encode(catalog)
    catalog_path.write_bytes(raw_catalog)
    catalog_pin = hashlib.sha256(raw_catalog).hexdigest()
    root = tmp_path / 'sealed-package'
    root.mkdir()
    profile = {'routing_candidates': [{'code': row['code']} for row in majors[:2]],
               'additional_related_specialties': []}
    manifest = {'files': []}
    record_file(root, manifest, 'profile.json', profile)
    packet = {'root': str(root), 'manifest': manifest, 'manifest_sha256': 'a' * 64,
              'profile': profile, 'assets': [], 'routes': copy.deepcopy(majors[:2]),
              'pending_routes': []}
    monkeypatch.setattr(package, 'validate_package', lambda *_args, **_kwargs: packet)
    run_id = 'b' * 32
    targets, metadata = {}, {}
    for n, row in enumerate(majors[:2], start=500):
        target = f'{n:032x}'
        code = row['code']
        targets[code] = {'paper_dataset_id': target}
        metadata[row['paper_dataset_id']] = {'id': row['paper_dataset_id'], 'embedding_model': 'synthetic'}
        metadata[target] = {'id': target, 'name': f'PaperFlow SELFTEST {run_id} {code} paper',
                            'permission': 'me', 'embedding_model': 'synthetic'}

    def call(observer=None, attachments_required=False):
        return prepare_test_routes(root, {}, catalog_path, catalog_sha256=catalog_pin,
                                   run_id=run_id, test_targets=targets,
                                   inspect_dataset=observer or metadata.get, attachments_required=attachments_required)

    return locals()


def test_one_paper_maps_each_specialty_but_does_not_authorize_upload(case):
    result = case['call']()
    assert result['manifest_sha256'] == 'a' * 64
    assert {r['code'] for r in result['routes']} == {r['code'] for r in case['packet']['routes']}
    assert {r['paper_dataset_id'] for r in result['routes']} == {
        r['paper_dataset_id'] for r in case['targets'].values()}
    assert result['upload_authorized'] is False
    assert result['attachments_verified'] is False
    assert result['prefect_connected'] is False
    assert result['body_images_required'] is False


def test_catalog_must_match_independently_supplied_pin_before_observation(case):
    case['catalog_path'].write_bytes(encode({'majors': case['majors'][:1]}))
    with pytest.raises(ValueError, match='catalog.*(hash|digest|pin)'):
        case['call'](lambda _id: pytest.fail('must not inspect targets after catalog changes'))


def test_dataset_in_pinned_catalog_cannot_be_a_test_target(case):
    first = case['packet']['routes'][0]
    case['targets'][first['code']]['paper_dataset_id'] = first['paper_dataset_id']
    with pytest.raises(ValueError, match='formal'):
        case['call']()


def test_distinct_specialties_cannot_share_a_test_dataset(case):
    first, second = [r['code'] for r in case['packet']['routes']]
    case['targets'][second]['paper_dataset_id'] = case['targets'][first]['paper_dataset_id']
    with pytest.raises(ValueError, match='unique'):
        case['call']()


@pytest.mark.parametrize('field,value', [('name', 'some other library'), ('permission', 'team')])
def test_target_must_belong_to_this_run_and_be_private(case, field, value):
    code = case['packet']['routes'][0]['code']
    case['metadata'][case['targets'][code]['paper_dataset_id']][field] = value
    with pytest.raises(ValueError, match='test target'):
        case['call']()


def test_target_must_use_same_embedding_model_as_formal_dataset(case):
    code = case['packet']['routes'][0]['code']
    case['metadata'][case['targets'][code]['paper_dataset_id']]['embedding_model'] = 'different'
    with pytest.raises(ValueError, match='embedding'):
        case['call']()


def test_shared_map_does_not_add_other_specialties_to_one_paper(case):
    for n, row in enumerate(case['majors'][2:], start=600):
        case['targets'][row['code']] = {'paper_dataset_id': f'{n:032x}'}
    result = case['call']()
    assert len(case['targets']) == 21
    assert len(result['routes']) == 2


def test_image_assets_require_an_image_test_target(case):
    case['packet']['assets'] = [{'storage': 'image'}]
    with pytest.raises(ValueError, match='image target'):
        case['call']()


def test_image_target_is_observed_and_embedding_checked(case):
    case['packet']['assets'] = [{'storage': 'image'}]
    for n, row in enumerate(case['packet']['routes'], start=700):
        target = f'{n:032x}'
        case['targets'][row['code']]['image_dataset_id'] = target
        case['metadata'][row['image_dataset_id']] = {'id': row['image_dataset_id'], 'embedding_model': 'image-model'}
        case['metadata'][target] = {'id': target, 'embedding_model': 'image-model', 'permission': 'me',
                                   'name': f"PaperFlow SELFTEST {case['run_id']} {row['code']} image"}
    assert case['call']()['body_images_required'] is True
    first = case['packet']['routes'][0]['code']
    case['metadata'][case['targets'][first]['image_dataset_id']]['embedding_model'] = 'wrong-model'
    with pytest.raises(ValueError, match='embedding'):
        case['call']()


def test_attachment_target_is_retained_even_without_body_images(case):
    with pytest.raises(ValueError, match='image target'):
        case['call'](attachments_required=True)
    for n, row in enumerate(case['packet']['routes'], start=700):
        target = f'{n:032x}'
        case['targets'][row['code']]['image_dataset_id'] = target
        case['metadata'][row['image_dataset_id']] = {'id': row['image_dataset_id'], 'embedding_model': 'image-model'}
        case['metadata'][target] = {'id': target, 'embedding_model': 'image-model', 'permission': 'me',
                                   'name': f"PaperFlow SELFTEST {case['run_id']} {row['code']} image"}
    result = case['call'](attachments_required=True)
    assert result['body_images_required'] is False
    assert {r['image_dataset_id'] for r in result['routes']} == {
        item['image_dataset_id'] for item in case['targets'].values()}


def test_unknown_asset_storage_does_not_silently_skip_images(case):
    case['packet']['assets'] = [{'storage': 'unknown', 'file': 'figure.png'}]
    with pytest.raises(ValueError, match='asset'):
        case['call']()


def test_conflicting_sealed_corrections_must_stop_instead_of_dropping_a_specialty(case):
    correction = {'final_routing_candidates': [{'code': case['majors'][2]['code']}]}
    record_file(case['root'], case['manifest'], 'corrections.json', correction)
    with pytest.raises(ValueError, match='specialty'):
        case['call']()


def test_unsealed_corrections_must_not_be_ignored(case):
    (case['root'] / 'corrections.json').write_bytes(encode({'final_routing_candidates': []}))
    with pytest.raises(ValueError, match='corrections'):
        case['call']()


def test_agreeing_sealed_corrections_are_accepted(case):
    correction = {'final_routing_candidates': case['profile']['routing_candidates']}
    record_file(case['root'], case['manifest'], 'corrections.json', correction)
    assert len(case['call']()['routes']) == 2


def test_real_validator_accepts_a_synthetic_seal_and_rejects_tampering(tmp_path):
    """Exercise the actual strict-seal validator with temporary, invented text."""
    from batch_test_routes import prepare_test_routes

    root = tmp_path / 'package'
    root.mkdir()
    ident = {'task_id': 'synthetic-task', 'source_id': 'synthetic-source', 'source_sha256': 'a' * 64}
    code, formal_id, target_id = '120201', '1' * 32, '2' * 32
    body = '# 研究方法\n通过指标分解解释盈利能力。'
    manifest = {**ident, 'schema': 'paperflow/1.0', 'revision': 'synthetic', 'files': [],
                'publication_files': ['article.redacted.md', 'profile.json', 'tree.json', 'assets.json', 'chunks.json']}
    docs = {
        'article.redacted.md': body,
        'profile.json': {**ident, 'material_disposition': 'keep', 'article': {'title': '方法范例'},
                         'routing_candidates': [{'code': code, 'name': '工商管理'}],
                         'learning_units': [{'id': 'u1', 'title': '指标分解',
                                             'steps': ['取得新数据', '计算指标'], 'section_ids': ['s1']}]},
        'tree.json': {**ident, 'nodes': [{'id': 's1', 'title': '研究方法', 'parent_id': None}]},
        'assets.json': {**ident, 'items': []},
        'chunks.json': {**ident, 'chunks': [{'id': 'body1', 'kind': 'body', 'section_ids': ['s1'],
                                            'asset_ids': [], 'content': body}]},
        'corrections.json': {**ident, 'operations': [], 'final_routing_candidates': [{'code': code}]},
    }
    for name, value in docs.items():
        raw = value.encode('utf-8') if isinstance(value, str) else encode(value)
        (root / name).write_bytes(raw)
        manifest['files'].append({'path': name, 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()})
    manifest_raw = encode(manifest)
    (root / 'manifest.json').write_bytes(manifest_raw)
    (root / 'READY.json').write_bytes(encode({**ident, 'manifest': 'manifest.json',
                                             'manifest_sha256': hashlib.sha256(manifest_raw).hexdigest()}))
    catalog = {'majors': [{'code': code, 'name': '工商管理', 'paper_dataset_id': formal_id,
                           'image_dataset_id': '3' * 32}]}
    catalog_raw = encode(catalog)
    catalog_path = tmp_path / 'catalog.json'
    catalog_path.write_bytes(catalog_raw)
    run = 'b' * 32
    metadata = {formal_id: {'id': formal_id, 'embedding_model': 'synthetic'},
                target_id: {'id': target_id, 'name': f'PaperFlow SELFTEST {run} {code} paper',
                            'permission': 'me', 'embedding_model': 'synthetic'}}

    def call():
        return prepare_test_routes(root, ident, catalog_path,
                                   catalog_sha256=hashlib.sha256(catalog_raw).hexdigest(), run_id=run,
                                   test_targets={code: {'paper_dataset_id': target_id}}, inspect_dataset=metadata.get)

    assert len(call()['routes']) == 1
    (root / 'article.redacted.md').write_text(body + 'tampered', encoding='utf-8')
    with pytest.raises(package.PackageError):
        call()
