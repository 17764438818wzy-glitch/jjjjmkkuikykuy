import hashlib
import json
from pathlib import Path

import pytest

from engine import Store, digest
from evidence import package_fingerprint
from knowledge_package import SCHEMA, encoded, hash_file
from ragflow_client import _upload_package_assets, upload_routed_items


PAPER = {'id': 'paper-kb', 'name': '论文库'}


class RecordingRemote:
    base_url = 'http://localhost'
    api_key = 'fixture-key'
    supports_receipt_validation = True

    def __init__(self):
        self.docs = []
        self.posts = 0

    def upload(self, dataset_id, path, remote_name=None):
        self.posts += 1
        document = {'id': f'doc-{self.posts}', 'name': remote_name or Path(path).name,
                    'dataset_id': dataset_id, 'run': 'UNSTART', 'progress': 0}
        self.docs.append(document)
        return document['id']

    def documents(self, dataset_id, name=None, document_id=None):
        return [doc for doc in self.docs if doc['dataset_id'] == dataset_id
                and (name is None or doc['name'] == name)
                and (document_id is None or doc['id'] == document_id)]


def _package(store, run_id, primary, derived=None, role='paper_assets', with_roles=True,
             derived_name='ragflow_assets.md'):
    package = store.runs / run_id / 'package'
    package.mkdir(parents=True)
    (package / 'document.md').write_text(primary, encoding='utf-8')
    (package / 'ingest.md').write_text(primary, encoding='utf-8')
    (package / 'metadata.json').write_text(
        json.dumps({'id': run_id, 'title': '派生文件测试', 'automatic_approval': False}, ensure_ascii=False),
        encoding='utf-8')
    for name in ('metadata/structure.json', 'metadata/blocks.json', 'metadata/relations.json'):
        path = package / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{}', encoding='utf-8')
    if derived is not None:
        path = package / derived_name
        path.write_text(derived, encoding='utf-8')

    names = ['document.md', 'ingest.md', 'metadata.json', 'metadata/blocks.json',
             'metadata/relations.json', 'metadata/structure.json']
    if derived is not None:
        names.append(derived_name)
    rows = []
    for name in names:
        row = {'path': name, 'sha256': hash_file(package / name)}
        if with_roles and name == 'ingest.md':
            row['role'] = 'paper_primary'
        if with_roles and name == derived_name:
            row['role'] = role
        rows.append(row)
    manifest = {'schema_version': SCHEMA, 'paper_id': f'paper_{run_id}',
                'document_id': run_id, 'source_sha256': 'a' * 64,
                'title': '派生文件测试', 'major': '测试方向', 'files': rows,
                'figures': [], 'handoff': {}}
    manifest['revision'] = hashlib.sha256(encoded(manifest)).hexdigest()
    (package / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False), encoding='utf-8')
    item = {'id': run_id, 'title': '派生文件测试', 'major': '测试方向', 'status': 'pass',
            'automatic_approval': True, 'source_path': str(package / 'source.txt'),
            'source_hash': 'b' * 64, 'path': str(package / 'ingest.md'),
            'package_path': str(package), 'reviewed_hash': digest(package / 'ingest.md'),
            'reviewed_package_hash': package_fingerprint(package), 'uploads': {}}
    store.save({'run_id': run_id, 'status': 'completed', 'items': [item], 'events': []})
    return item


def _upload(store, run_id, config=None):
    client = RecordingRemote()
    messages = []
    count = upload_routed_items(store, run_id, [run_id], client,
                                {'paper': PAPER, 'image': None}, messages.append,
                                **({'upload_config':config} if config is not None else {}))
    return client, messages, count


def test_redundant_derived_file_is_skipped_with_auditable_ratio_and_summary(tmp_path):
    store = Store(tmp_path / 'data')
    line_a = '主文件中已有的完整数据行 2023 营业收入 100 万元'
    line_b = '主文件中已有的完整数据行 2024 营业收入 120 万元'
    _package(store, 'redundant', f'{line_a}\n{line_b}\n', f'{line_a}\n{line_b}\n')

    client, messages, count = _upload(store, 'redundant')

    assert count == 1
    assert len(client.docs) == 1
    assert any('[跳过] 派生文件冗余' in message and '100.0% (2/2 行)' in message
               and '阈值 95%' in message for message in messages)
    assert any('本批跳过 1 个派生文件（全部冗余），上传 1 个正文文件' in message
               for message in messages)


def test_nonredundant_derived_file_is_uploaded_when_policy_is_enabled(tmp_path):
    store = Store(tmp_path / 'data')
    _package(store, 'novel', '主文件已有的完整数据行 2023 营业收入 100 万元\n',
             '派生文件新增的完整研究说明 这里是主文件没有的内容\n')

    client, messages, count = _upload(store, 'novel')

    assert count == 1
    assert len(client.docs) == 2
    assert any(doc['name'].endswith('ragflow_assets.md') for doc in client.docs)
    assert not any('[跳过] 派生文件冗余' in message for message in messages)


def test_partially_redundant_derived_file_is_uploaded(tmp_path):
    store = Store(tmp_path / 'data')
    primary = '\n'.join(f'主文件已有的完整数据行 {i:02d} 营业收入' for i in range(5)) + '\n'
    derived = '\n'.join([*(f'主文件已有的完整数据行 {i:02d} 营业收入' for i in range(3)),
                          '派生文件新增的完整研究说明 第一条',
                          '派生文件新增的完整研究说明 第二条']) + '\n'
    _package(store, 'partial', primary, derived)

    client, messages, count = _upload(store, 'partial')

    assert count == 1
    assert len(client.docs) == 2


def test_disabling_policy_uploads_redundant_derived_file_for_legacy_behavior(tmp_path):
    store = Store(tmp_path / 'data')
    line = '主文件中已有的完整数据行 2023 营业收入 100 万元'
    _package(store, 'legacy-policy', line + '\n', line + '\n')

    client, messages, count = _upload(store, 'legacy-policy', {'skip_derived_upload': False})

    assert count == 1
    assert len(client.docs) == 2
    assert not any('[跳过] 派生文件冗余' in message for message in messages)


def test_primary_role_is_uploaded_without_redundancy_check(tmp_path):
    store = Store(tmp_path / 'data')
    _package(store, 'primary-only', '主文件内容足够长且不需要拿自己做冗余判定\n')

    client, messages, count = _upload(store, 'primary-only')

    assert count == 1
    assert len(client.docs) == 1
    assert not any('子集率' in message for message in messages)


def test_old_package_without_roles_uses_ingest_as_primary_baseline(tmp_path):
    store = Store(tmp_path / 'data')
    line = '旧包主文件已有的完整数据行 2023 营业收入 100 万元'
    _package(store, 'old-package', line + '\n', line + '\n', with_roles=False)

    client, messages, count = _upload(store, 'old-package')

    assert count == 1
    assert len(client.docs) == 1
    assert any('[跳过] 派生文件冗余' in message for message in messages)


def test_unreadable_primary_warns_and_skips_derived_file(tmp_path):
    store = Store(tmp_path / 'data')
    _package(store, 'missing-primary', '主文件内容\n', '派生文件新增的完整研究说明\n')
    manifest = store.load('missing-primary')
    item = manifest['items'][0]
    item['path'] = str(Path(item['path']).with_name('missing-ingest.md'))
    client = RecordingRemote()
    messages = []

    _upload_package_assets(store, manifest, item, 'http://localhost|paper-kb', PAPER, client,
                           target_route='paper', callback=messages.append)

    assert client.docs == []
    assert any('主文件' in message and '跳过' in message for message in messages)


def test_unknown_role_is_an_explicit_error(tmp_path):
    store = Store(tmp_path / 'data')
    _package(store, 'unknown-role', '主文件内容足够长\n', '派生文件内容足够长\n', role='mystery_role')

    with pytest.raises(ValueError, match='未知角色'):
        _upload(store, 'unknown-role')


def _refresh(store, run_id):
    manifest = store.load(run_id)
    item = manifest['items'][0]
    package = Path(item['package_path'])
    inventory = json.loads((package / 'manifest.json').read_text(encoding='utf-8'))
    for row in inventory['files']:
        row['sha256'] = hash_file(package / row['path'])
    inventory.pop('revision', None)
    inventory['revision'] = hashlib.sha256(encoded(inventory)).hexdigest()
    (package / 'manifest.json').write_text(json.dumps(inventory), encoding='utf-8')
    item['reviewed_hash'] = digest(package / 'ingest.md')
    item['reviewed_package_hash'] = package_fingerprint(package)
    store.save(manifest)


def _add_images(store, run_id, primary):
    from PIL import Image
    item = store.load(run_id)['items'][0]
    package = Path(item['package_path'])
    (package / 'assets').mkdir()
    Image.new('RGB', (12, 8), 'blue').save(package / 'assets' / 'figure.png')
    # Exactly matching prose must not suppress the image projection.
    (package / 'ragflow_images.md').write_text(primary, encoding='utf-8')
    metadata = json.loads((package / 'metadata.json').read_text(encoding='utf-8'))
    metadata['resources'] = [{'id': 'figure', 'local_path': 'assets/figure.png',
                              'privacy_status': 'approved'}]
    (package / 'metadata.json').write_text(json.dumps(metadata), encoding='utf-8')
    inventory = json.loads((package / 'manifest.json').read_text(encoding='utf-8'))
    inventory['files'].extend([
        {'path': 'ragflow_images.md', 'role': 'image_projection'},
        {'path': 'assets/figure.png', 'role': 'image'},
    ])
    (package / 'manifest.json').write_text(json.dumps(inventory), encoding='utf-8')
    _refresh(store, run_id)


def test_novel_paragraph_below_threshold_is_uploaded(tmp_path):
    store = Store(tmp_path / 'data')
    primary = '已有研究内容的完整说明 第一个数据观察\n'
    _package(store, 'new-paragraph', primary,
             primary + '新增研究段落包含此前正文没有提供的信息\n')
    client, _, _ = _upload(store, 'new-paragraph')
    assert len(client.docs) == 2


@pytest.mark.parametrize(('matched', 'expected_documents'), [(950, 1), (949, 2)])
def test_threshold_boundary_controls_real_uploads(tmp_path, matched, expected_documents):
    store = Store(tmp_path / 'data')
    primary = '\n'.join(f'existing research observation number {i:04d}' for i in range(1000))
    derived = '\n'.join(
        [f'existing research observation number {i:04d}' for i in range(matched)] +
        [f'new independent research observation {i:04d}' for i in range(1000 - matched)])
    _package(store, 'boundary', primary, derived)
    client, messages, _ = _upload(store, 'boundary')
    assert len(client.docs) == expected_documents
    if matched == 950:
        assert any('95.0% (950/1000 行)' in message for message in messages)


def test_nfkc_and_outer_whitespace_still_skip_redundant_text(tmp_path):
    store = Store(tmp_path / 'data')
    _package(store, 'normalized', '研究数据 ABC123 原始记录保持不变\n',
             '  研究数据 ＡＢＣ１２３ 原始记录保持不变  \n')
    client, messages, _ = _upload(store, 'normalized')
    assert len(client.docs) == 1
    assert any('100.0% (1/1 行)' in message for message in messages)


def test_no_nontrivial_lines_counts_as_redundant(tmp_path):
    store = Store(tmp_path / 'data')
    _package(store, 'short-lines', '正文中的有效长文本需要保留并上传\n', '# 标题\n\n{}\nshort\n')
    client, messages, _ = _upload(store, 'short-lines')
    assert len(client.docs) == 1
    assert any('100.0% (0/0 行)' in message for message in messages)


@pytest.mark.parametrize('redundant', [True, False])
def test_arbitrary_paper_role_and_filename_follow_content(tmp_path, redundant):
    store = Store(tmp_path / 'data')
    primary = 'A complete primary research observation with original data\n'
    derived = primary if redundant else 'New content with additional independent research data\n'
    _package(store, 'generic', primary, derived, role='paper_notes', derived_name='custom_notes.txt')
    client, _, _ = _upload(store, 'generic')
    assert len(client.docs) == (1 if redundant else 2)


def test_image_routes_and_receipt_completion_are_unchanged(tmp_path):
    store = Store(tmp_path / 'data')
    primary = 'Complete image description already present in primary text\n'
    _package(store, 'images', primary, primary)
    _add_images(store, 'images', primary)
    client = RecordingRemote()
    routes = {'paper': PAPER, 'image': {'id': 'image-kb', 'name': '图片库'}}
    upload_routed_items(store, 'images', ['images'], client, routes)
    paper_docs = [d for d in client.docs if d['dataset_id'] == 'paper-kb']
    image_docs = [d for d in client.docs if d['dataset_id'] == 'image-kb']
    assert len(paper_docs) == 1
    assert {d['name'].split('__asset_')[-1] for d in image_docs} == {'ragflow_images.md', 'figure.png'}
    saved = store.load('images')['items'][0]['uploads']['http://localhost|image-kb']
    assert saved['status'] == 'uploaded'
    assert saved['asset_status'] == 'uploaded'
    assert saved['document_id'] in {d['id'] for d in image_docs}


def test_batch_targets_and_selected_item_filter_survive_config_forwarding(tmp_path):
    from auto_ragflow import handoff
    store = Store(tmp_path / 'data')
    primary = 'Complete existing research text used for upload verification\n'
    _package(store, 'selected', primary, primary)
    _add_images(store, 'selected', primary)
    manifest = store.load('selected')
    manifest['items'].append({'id': 'unselected', 'status': 'pass', 'automatic_approval': True})
    targets = {'paper': {'id': 'batch-paper', 'name': '批次正文'},
               'image': {'id': 'batch-image', 'name': '批次图片'}}
    manifest['task_settings'] = {'destinations': targets}
    store.save(manifest)
    client = RecordingRemote()
    client.datasets = lambda: [*targets.values(), {'id': 'mapped-paper', 'name': '映射正文'}]
    handoff(store, 'selected', client, {'测试方向': {'paper': 'mapped-paper', 'image': ''}},
            auto_parse=False, item_ids=['selected'], upload_config={'skip_derived_upload': False})
    assert len(client.docs) == 4
    assert {d['dataset_id'] for d in client.docs} == {'batch-paper', 'batch-image'}
    saved = store.load('selected')['items']
    assert saved[0]['handoff_status'] == 'uploaded'
    assert 'handoff_status' not in saved[1]


def test_same_batch_twice_reuses_primary_images_and_novel_derived(tmp_path):
    store = Store(tmp_path / 'data')
    primary = 'Complete primary research text with existing observations\n'
    _package(store, 'repeat', primary, 'Independent novel research data absent from primary\n')
    _add_images(store, 'repeat', primary)
    client = RecordingRemote()
    routes = {'paper': PAPER, 'image': {'id': 'image-kb', 'name': '图片库'}}
    assert upload_routed_items(store, 'repeat', ['repeat'], client, routes) == 1
    first = store.load('repeat')['items'][0]['uploads']
    assert client.posts == 4
    assert upload_routed_items(store, 'repeat', ['repeat'], client, routes) == 0
    assert client.posts == 4
    second = store.load('repeat')['items'][0]['uploads']
    for key, before in first.items():
        assert second[key]['document_id'] == before['document_id']
        assert second[key]['uploaded_at'] == before['uploaded_at']
        assert second[key]['asset_uploads'] == before['asset_uploads']


def test_legacy_config_defaults_and_saved_override_reach_uploader(tmp_path):
    from auto_config import load_config, save_config
    store = Store(tmp_path / 'data')
    settings = store.root / 'ai_settings.json'
    settings.write_text(json.dumps({'max_tokens_retry_limit': 32768}), encoding='utf-8')
    old_bytes = settings.read_bytes()
    config = load_config(store.root)
    assert settings.read_bytes() == old_bytes
    assert config['skip_derived_upload'] is True
    assert config['derived_redundancy_threshold'] == 0.95
    config.update(skip_derived_upload=False, derived_redundancy_threshold=0.8)
    save_config(store.root, config)
    restored = load_config(store.root)
    assert restored['max_tokens_retry_limit'] == 32768
    assert restored['derived_redundancy_threshold'] == 0.8
    text = 'Complete research data already recorded in the primary file\n'
    _package(store, 'override', text, text)
    client, _, _ = _upload(store, 'override', restored)
    assert len(client.docs) == 2


def test_tagged_package_does_not_upload_unroled_sidecars(tmp_path):
    store = Store(tmp_path / 'data')
    _package(store, 'unroled', 'Complete primary research text for a valid package\n',
             'Novel helper text that has no upload role assigned\n')
    item = store.load('unroled')['items'][0]
    path = Path(item['package_path']) / 'manifest.json'
    manifest = json.loads(path.read_text(encoding='utf-8'))
    next(r for r in manifest['files'] if r['path'] == 'ragflow_assets.md').pop('role')
    path.write_text(json.dumps(manifest), encoding='utf-8')
    _refresh(store, 'unroled')
    client, _, _ = _upload(store, 'unroled')
    assert len(client.docs) == 1
