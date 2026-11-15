"""Sealed-result reuse: no extraction, image encoding or learning rechunking."""
import copy
import hashlib
import json
from pathlib import Path

import pytest
from PIL import Image

from test_preextract_publish import Counter, PAPERFLOW, fixture, prepare
from test_preextract_chunk_consolidation import _tree, _asset


def legacy_package(tmp_path):
    import preextract_publish as publish
    c, b, _ = fixture()
    _tree(c)
    image = tmp_path / 'source.png'
    Image.new('RGB', (31, 23), 'white').save(image)
    b['assets'] = [dict(id='I1', image_path=str(image),
                        sha256=hashlib.sha256(image.read_bytes()).hexdigest(), width=31, height=23)]
    asset = _asset(c)
    asset.update(storage='image', source_image_id='I1')
    c['assets']['items'] = [asset]
    result = prepare(tmp_path, c, b)
    root = Path(result['package_path'])
    packet = result['packet']
    title = packet['profile']['article']['title']
    chunks = [copy.deepcopy(row) for row in packet['chunks'] if row['kind'] not in {'chapter', 'chart'}]
    next(row for row in chunks if row['kind'] == 'learning')['content'] += '\n原有学习记录的格式和上下文必须逐字复用。'
    for index, node in enumerate(packet['tree']['nodes']):
        text = publish._readable(node)
        chunks.append(dict(id=f'chapter_old_{index}', kind='chapter',
            content=f'论文：{title}\n章节：{node["id"]} {node["title"]}\n使用限制：保留旧章节限制\n' + text,
            section_ids=[node['id']], unit_ids=node.get('unit_ids', []), asset_ids=[],
            part_index=0, part_count=1, projection_sha256=hashlib.sha256(text.encode()).hexdigest()))
    items = copy.deepcopy(packet['assets'])
    items[0]['description'] = '学生字段通过主键建立关系。'
    value = {key:items[0][key] for key in sorted(publish.ASSET_FIELDS, reverse=True) if key in items[0]}
    text = publish._readable(value)
    chunks.append(dict(id='chart_old_0', kind='chart',
        content=f'论文：{title}\n图表：A1 实体关系图\n使用限制：保留旧图表限制\n' + text,
        section_ids=['S1'], unit_ids=['U1'], asset_ids=['A1'],
        part_index=0, part_count=1, projection_sha256=hashlib.sha256(text.encode()).hexdigest()))
    for name, key, rows in [('chunks.json', 'chunks', chunks), ('assets.json', 'items', items)]:
        path = root / name
        value = json.loads(path.read_text(encoding='utf8'))
        value[key] = rows
        path.write_text(json.dumps(value, ensure_ascii=False), encoding='utf8')
    package = publish._paperflow(PAPERFLOW)
    package.seal_package(root, result['task'], result['task']['revision'])
    result['packet'] = package.validate_package(root, result['task'], result['catalog'], strict_seal=True)
    result['validation']['manifest_sha256'] = result['packet']['manifest_sha256']
    result['validation']['publication_version'] = 'preextract-publication/1.4'
    result['validation'].pop('retrieval_organization', None)
    return result


def test_reorganization_copies_sealed_learning_body_sources_and_image_bytes(tmp_path, monkeypatch):
    from preextract_reorganize import consolidate_publication
    import preextract_publish as publish
    old = legacy_package(tmp_path)
    root = Path(old['package_path'])
    protected = {p:hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}
    def forbidden(*args, **kwargs):
        raise AssertionError('reorganization must reuse sealed content')
    monkeypatch.setattr(publish, 'prepare_publication', forbidden)
    monkeypatch.setattr(publish, '_mask_images', forbidden)
    new = consolidate_publication(old, tmp_path / 'reorganized', counter=Counter())
    assert new['task']['task_id'] != old['task']['task_id']
    assert new['task']['source_id'] == old['task']['source_id']
    for key in ['profile', 'tree']:
        assert {k:v for k,v in new['packet'][key].items() if k != 'task_id'} == {
            k:v for k,v in old['packet'][key].items() if k != 'task_id'}
    frozen = {'body', 'learning', 'research', 'continuity'}
    assert [r for r in new['packet']['chunks'] if r['kind'] in frozen] == [
        r for r in old['packet']['chunks'] if r['kind'] in frozen]
    chapters = [r for r in new['packet']['chunks'] if r['kind'] == 'chapter']
    assert len(chapters) == 3
    assert next(r for r in chapters if 'S2' in r['section_ids'])['section_ids'] == ['S2', 'S3', 'S4']
    assert all('保留旧章节限制' in r['content'] for r in chapters)
    assert not any(r['kind'] == 'chart' for r in new['packet']['chunks'])
    assert '保留旧图表限制' in new['packet']['assets'][0]['description']
    assert new['validation']['reuse']['source_manifest_sha256'] == old['packet']['manifest_sha256']
    for name in ['article.redacted.md', 'source-map.json']:
        assert (root / name).read_bytes() == (Path(new['package_path']) / name).read_bytes()
    for entry in old['packet']['manifest']['files']:
        if entry['path'].startswith('assets/'):
            assert (root / entry['path']).read_bytes() == (Path(new['package_path']) / entry['path']).read_bytes()
    assert {p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected} == protected


@pytest.mark.parametrize('drift', ['manifest', 'seal'])
def test_sealed_result_drift_stops_before_creating_a_replacement(tmp_path, drift):
    from preextract_reorganize import consolidate_publication
    old = legacy_package(tmp_path)
    if drift == 'manifest':
        old['validation']['manifest_sha256'] = '0' * 64
    else:
        with (Path(old['package_path']) / 'article.redacted.md').open('a', encoding='utf8') as out:
            out.write('不能丢弃的变更')
    target = tmp_path / 'replacement'
    with pytest.raises(ValueError):
        consolidate_publication(old, target, counter=Counter())
    assert not target.exists()
