"""Old native checkpoints must not keep DPI metadata bitmaps invisible."""
import io
import json
from pathlib import Path
import zipfile

import pytest
from PIL import Image

from ai_resume import restore_prepared, save_prepared
from engine import digest
from evidence import build_package
from test_native_picture_crop import FIXTURE, modify_fixture
from test_native_picture_metadata import local_dpi


@pytest.mark.parametrize('keep_crop', [False, True])
@pytest.mark.parametrize('native_rules_version', [2, 3])
def test_older_rules_rebuild_picture_from_immutable_native_source(tmp_path, monkeypatch, keep_crop, native_rules_version):
    work = tmp_path / 'runs/old/private/item'
    work.mkdir(parents=True)
    source = modify_fixture(work, lambda root: local_dpi(root, keep_crop=keep_crop))
    package = work / 'package'
    identity = {'id': 'paper', 'title': '原生图版本迁移回归', 'major': '教育学', 'category': '教育教学类'}
    import native_picture
    with monkeypatch.context() as old_rules:
        # Reproduce the former guard which rejected any BLIP extension.
        old_rules.setattr(native_picture, 'only_compression_metadata', lambda node: not len(node))
        meta = build_package(source, package, identity, redact_content=False)
    assert not meta['resources']
    assert any(issue['code'] == 'embedded_object_review' for issue in meta['issues'])
    preparation = {'raw': '研究正文', 'reliable': True, 'screen': {'decision': 'pass'},
                   'hint_title': identity['title'], 'hint_confidence': 'high',
                   'meta': meta, 'identity': identity}
    checkpoint = save_prepared(work, source, package, {'source_hash': digest(source)}, preparation, 'test-version')
    checkpoint_path = Path(checkpoint['path'])
    saved = json.loads(checkpoint_path.read_text(encoding='utf-8'))
    saved['native_rules_version'] = native_rules_version
    checkpoint_path.write_text(json.dumps(saved, ensure_ascii=False), encoding='utf-8')
    checkpoint['sha256'] = digest(checkpoint_path)
    original_hashes = {p: digest(p) for p in work.rglob('*') if p.is_file()}
    destination = tmp_path / 'retry-work'
    destination.mkdir()

    _, recovered_package, restored = restore_prepared(
        {'path': str(source), 'prepared_checkpoint': checkpoint}, tmp_path, 'test-version', destination)

    assert len(restored['meta']['resources']) == 1
    assert restored['_native_rebuilt'] is True
    assert recovered_package.name == 'package_refreshed'
    resource = restored['meta']['resources'][0]
    recovered = (recovered_package / resource['local_path']).read_bytes()
    with zipfile.ZipFile(FIXTURE) as archive:
        original = archive.read('word/media/image3.png')
    if keep_crop:
        expected = Image.open(io.BytesIO(original)).convert('RGB').crop((0, 100, 1098, 471))
        actual = Image.open(io.BytesIO(recovered)).convert('RGB')
        assert actual.size == (1098, 371)
        assert actual.tobytes() == expected.tobytes()
    else:
        assert recovered == original
    assert resource['privacy_status'] == 'pending'
    assert resource['upload_allowed'] is False
    assert resource['local_path'].startswith('_review/')
    assert not any(issue['code'] == 'embedded_object_review' for issue in restored['meta']['issues'])
    assert all(digest(path) == before for path, before in original_hashes.items())
