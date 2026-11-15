import hashlib
import importlib.util
import json
from pathlib import Path

import pytest


SPEC = importlib.util.spec_from_file_location('bootstrap_business_revision',
    Path(__file__).parents[1] / 'bootstrap_business_revision.py')
revision = importlib.util.module_from_spec(SPEC)
if SPEC.loader is not None and SPEC.origin and Path(SPEC.origin).exists():
    SPEC.loader.exec_module(revision)


def setup_release(tmp_path):
    base = tmp_path / 'runtime'; old = base / 'old' / 'code'; old.mkdir(parents=True)
    rels = ['drives/f/candidates/postprocess-prefect-20261031/body_adapter.py', 'drives/f/core/safe.py']
    rows = []
    for relative in rels:
        path = old.joinpath(*relative.split('/')); path.parent.mkdir(parents=True, exist_ok=True)
        data = ('old ' + relative).encode(); path.write_bytes(data)
        rows.append(dict(source='F:\\' + relative.replace('/', '\\'), relative=relative,
                         bytes=len(data), sha256=hashlib.sha256(data).hexdigest()))
    manifest = base / 'old' / 'code-manifest.json'
    manifest.write_bytes(json.dumps(rows, ensure_ascii=False, separators=(',', ':')).encode())
    patch_dir = base / 'patch'; patch_dir.mkdir()
    patch = patch_dir / 'body_adapter.py'; patch.write_bytes(b'new body adapter')
    return base, old, manifest, patch, rows


def test_only_the_pinned_body_adapter_changes_in_a_new_release(tmp_path):
    base, old, manifest, patch, rows = setup_release(tmp_path)
    result = revision.prepare(base, old, manifest, revision.sha(manifest.read_bytes()), base / 'new',
                              patch, revision.sha(patch.read_bytes()))
    updated = json.loads(Path(result['manifest']).read_bytes())
    assert len(updated) == 2 and updated[1] == rows[1]
    assert updated[0]['sha256'] == revision.sha(patch.read_bytes())
    assert (old / rows[0]['relative']).read_bytes() != patch.read_bytes()
    assert (Path(result['code_root']) / rows[0]['relative']).read_bytes() == patch.read_bytes()


def test_changed_old_source_is_rejected_before_any_new_release(tmp_path):
    base, old, manifest, patch, rows = setup_release(tmp_path)
    (old / rows[1]['relative']).write_bytes(b'altered')
    with pytest.raises(ValueError):
        revision.prepare(base, old, manifest, revision.sha(manifest.read_bytes()), base / 'new',
                         patch, revision.sha(patch.read_bytes()))
    assert not (base / 'new').exists()


def test_manifest_path_traversal_is_never_copied(tmp_path):
    base, old, manifest, patch, rows = setup_release(tmp_path)
    rows[1]['relative'] = '../outside'
    manifest.write_text(json.dumps(rows), encoding='utf8')
    with pytest.raises(ValueError):
        revision.prepare(base, old, manifest, revision.sha(manifest.read_bytes()), base / 'new',
                         patch, revision.sha(patch.read_bytes()))
