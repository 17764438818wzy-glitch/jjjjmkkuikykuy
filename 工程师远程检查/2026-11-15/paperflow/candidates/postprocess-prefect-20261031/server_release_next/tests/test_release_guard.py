"""Only temporary synthetic files are used by these release guard tests."""
import hashlib
import importlib.util
import json
import importlib
import os
from pathlib import Path
import py_compile
import sys

import pytest


HERE = Path(__file__).resolve().parents[1]


def load():
    spec = importlib.util.spec_from_file_location('synthetic_page_release_guard', HERE / 'release_guard.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def example(tmp_path):
    root = tmp_path / 'synthetic-code'
    root.mkdir()
    source = root / 'source.py'
    source.write_text('x = 1\n', encoding='utf8')
    config = tmp_path / 'synthetic-config.json'
    config.write_text('{"synthetic":true}', encoding='utf8')
    files = {str(path.resolve()): hashlib.sha256(path.read_bytes()).hexdigest() for path in (source, config)}
    manifest = {'schema': 'paperflow-page-release/1', 'files': files, 'python': None, 'packages': {}}
    path = tmp_path / 'manifest.json'
    raw = json.dumps(manifest, sort_keys=True, separators=(',', ':')).encode('utf8')
    path.write_bytes(raw)
    guard = load().ReleaseGuard(path, hashlib.sha256(raw).hexdigest(),
        roots=[(root, ('.py',), True)], required_files=[source, config])
    return guard, path, source, config, root


def test_exact_pinned_source_and_config_are_accepted(example):
    guard, *_ = example
    assert guard.verify() is True


def test_direct_source_load_rejects_another_approved_target(example, monkeypatch):
    guard, _manifest, source, *_ = example
    guard.verify()
    monkeypatch.setattr(guard, 'read_verified', lambda _path: b'x = 999\n')
    with pytest.raises(ValueError):
        guard.load_source('_synthetic_retargeted_direct_source', source)
    assert '_synthetic_retargeted_direct_source' not in sys.modules


@pytest.mark.parametrize('which', ['source', 'config', 'manifest', 'new_source'])
def test_changed_or_unlisted_sources_are_rejected(example, which):
    guard, manifest, source, config, root = example
    if which == 'source': source.write_text('x = 2\n', encoding='utf8')
    elif which == 'config': config.write_text('{}', encoding='utf8')
    elif which == 'manifest': manifest.write_text('{}', encoding='utf8')
    else: (root / 'new.py').write_text('x = 3\n', encoding='utf8')
    with pytest.raises(ValueError):
        guard.verify()


def test_required_file_cannot_be_omitted_from_manifest(tmp_path):
    root = tmp_path / 'root'
    root.mkdir()
    required = root / 'source.py'
    required.write_text('pass', encoding='utf8')
    data = json.dumps({'schema': 'paperflow-page-release/1', 'files': {}, 'python': None, 'packages': {}}).encode()
    manifest = tmp_path / 'manifest.json'
    manifest.write_bytes(data)
    with pytest.raises(ValueError):
        load().ReleaseGuard(manifest, hashlib.sha256(data).hexdigest(),
            roots=[(root, ('.py',), True)], required_files=[required]).verify()


def test_managed_import_executes_pinned_source_not_stale_pyc(tmp_path):
    root = tmp_path / 'root'
    root.mkdir()
    source = root / 'synthetic_release_import.py'
    source.write_text('value = 2\n', encoding='utf8')
    original_mtime = source.stat().st_mtime_ns
    py_compile.compile(str(source), doraise=True)
    source.write_text('value = 1\n', encoding='utf8')
    os.utime(source, ns=(original_mtime, original_mtime))
    files = {str(source.resolve()): hashlib.sha256(source.read_bytes()).hexdigest()}
    data = json.dumps({'schema': 'paperflow-page-release/1', 'files': files,
                       'python': None, 'packages': {}}).encode()
    manifest = tmp_path / 'manifest.json'
    manifest.write_bytes(data)
    module = load()
    guard = module.ReleaseGuard(manifest, hashlib.sha256(data).hexdigest(),
        roots=[(root, ('.py',), True)], required_files=[source])
    guard.verify()
    finder = module.PinnedFinder(guard)
    sys.meta_path.insert(0, finder)
    sys.path.insert(0, str(root))
    try:
        imported = importlib.import_module('synthetic_release_import')
        assert imported.value == 1
    finally:
        sys.modules.pop('synthetic_release_import', None)
        sys.meta_path.remove(finder)
        sys.path.remove(str(root))


def test_preloaded_pinned_package_and_its_dynamic_children_reject_drift(tmp_path):
    root = tmp_path / 'package'
    root.mkdir()
    initializer, child = root / '__init__.py', root / 'child.py'
    initializer.write_text('value = "approved"\n', encoding='utf8')
    child.write_text('value = "child"\n', encoding='utf8')
    files = {str(path.resolve()): hashlib.sha256(path.read_bytes()).hexdigest() for path in (initializer, child)}
    raw = json.dumps({'schema': 'paperflow-page-release/1', 'files': files,
                      'python': None, 'packages': {}}).encode()
    manifest = tmp_path / 'manifest.json'
    manifest.write_bytes(raw)
    module = load()
    guard = module.ReleaseGuard(manifest, hashlib.sha256(raw).hexdigest(),
                                roots=[(root, ('.py',), True)], required_files=[])
    guard.verify()
    finder = module.PinnedFinder(guard)
    sys.meta_path.insert(0, finder)
    try:
        package = guard.load_source('_synthetic_preloaded_receipt', initializer)
        assert package.value == 'approved'
        assert importlib.import_module('_synthetic_preloaded_receipt.child').value == 'child'
        sys.modules.pop('_synthetic_preloaded_receipt.child')
        child.write_text('value = "changed"\n', encoding='utf8')
        with pytest.raises(ValueError):
            importlib.import_module('_synthetic_preloaded_receipt.child')
    finally:
        sys.modules.pop('_synthetic_preloaded_receipt', None)
        sys.modules.pop('_synthetic_preloaded_receipt.child', None)
        sys.meta_path.remove(finder)


def test_selective_server_inventory_ignores_unrelated_source_but_rejects_its_import(tmp_path):
    root = tmp_path / 'server'
    root.mkdir()
    needed, unrelated = root / 'needed.py', root / 'unrelated.py'
    needed.write_text('value = 1\n', encoding='utf8')
    unrelated.write_text('value = 2\n', encoding='utf8')
    files = {str(needed.resolve()): hashlib.sha256(needed.read_bytes()).hexdigest()}
    raw = json.dumps({'schema': 'paperflow-page-release/1', 'files': files,
                      'python': None, 'packages': {}}).encode()
    manifest = tmp_path / 'manifest.json'
    manifest.write_bytes(raw)
    module = load()
    guard = module.ReleaseGuard(manifest, hashlib.sha256(raw).hexdigest(),
        roots=[(root, ('.py',), False, ('needed.py',))], required_files=[needed])
    assert guard.verify() is True
    unrelated.write_text('value = 3\n', encoding='utf8')
    assert guard.verify() is True
    finder = module.PinnedFinder(guard)
    sys.meta_path.insert(0, finder)
    sys.path.insert(0, str(root))
    try:
        with pytest.raises(ValueError):
            importlib.import_module('unrelated')
    finally:
        sys.modules.pop('unrelated', None)
        sys.meta_path.remove(finder)
        sys.path.remove(str(root))


def test_static_site_is_not_a_backend_runtime_dependency():
    spec = importlib.util.spec_from_file_location('synthetic_backend_layout', HERE / 'release_layout.py')
    layout = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(layout)
    roots, required = layout.layout(HERE / 'site')
    site = (HERE / 'site').resolve()
    assert not any(Path(entry[0]).resolve() == site for entry in roots)
    assert not any(Path(path).resolve().is_relative_to(site) for path in required)
    assert 'remote_scoped_job.py' not in layout.SERVER_FILES
    assert 'remote_scoped_batch.py' not in layout.SERVER_FILES
    assert 'ssh_rpc_process.py' in layout.SERVER_FILES
