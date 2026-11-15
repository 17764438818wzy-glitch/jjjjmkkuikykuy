"""Isolated source-loading checks; no server, real receipts, or work orders."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import py_compile
import subprocess
import sys

import pytest

HERE = Path(__file__).resolve().parents[1]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def app(tmp_path):
    root = tmp_path / 'historical-app'
    root.mkdir()
    (root / 'dependency.py').write_text("VALUE='safe'\n", encoding='utf8')
    (root / 'server_rpc.py').write_text('import dependency\nprint(dependency.VALUE)\n', encoding='utf8')
    files = {os.path.normcase(str(path.resolve())): sha(path) for path in root.glob('*.py')}
    manifest = tmp_path / 'manifest.json'
    manifest.write_text(json.dumps({'schema': 'paperflow-page-release/1', 'files': files,
                                    'python': None, 'packages': {}}, sort_keys=True), encoding='utf8')
    return root, manifest


def invoke(root, manifest, *, mode='inspect', target=None, target_hash=None, manifest_hash=None, guard=None):
    entry = HERE / 'server_verified_entry.py'
    guard = guard or HERE / 'release_guard.py'
    assert entry.is_file(), 'the guarded server entry is missing'
    target = target or root / 'server_rpc.py'
    args = [sys.executable, '-I', '-B', str(entry), str(guard), sha(guard), str(root), str(manifest),
            manifest_hash or sha(manifest), str(target), target_hash or sha(target), str(root / 'config.json'), 'a' * 64, mode]
    return subprocess.run(args, capture_output=True, text=True, timeout=10)


def test_isolated_python_is_required(app):
    root, manifest = app
    guard = HERE / 'release_guard.py'
    entry = HERE / 'server_verified_entry.py'
    rpc = root / 'server_rpc.py'
    args = [sys.executable, '-B', str(entry), str(guard), sha(guard), str(root), str(manifest),
            sha(manifest), str(rpc), sha(rpc), str(root / 'config.json'), 'a' * 64, 'inspect']
    result = subprocess.run(args, capture_output=True, text=True, timeout=10)
    assert result.returncode != 0 and not result.stdout


def test_executes_checked_app_sources_without_consulting_existing_pyc(app):
    root, manifest = app
    source = root / 'dependency.py'
    before = source.stat().st_mtime_ns
    source.write_text("VALUE='evil'\n", encoding='utf8')
    os.utime(source, ns=(before, before))
    py_compile.compile(str(source), doraise=True)
    source.write_text("VALUE='safe'\n", encoding='utf8')
    os.utime(source, ns=(before, before))
    result = invoke(root, manifest)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == 'safe'


def test_the_fixed_bootstrap_executes_the_same_hashed_guarded_entry(app):
    root, manifest = app
    spec = importlib.util.spec_from_file_location('synthetic_guard_bootstrap', HERE / 'read_only_rpc.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    entry, guard, target = HERE / 'server_verified_entry.py', HERE / 'release_guard.py', root / 'server_rpc.py'
    args = [sys.executable, '-I', '-B', '-c', module.VERSION_BOOTSTRAP, str(entry), sha(entry),
        str(guard), sha(guard), str(root), str(manifest), sha(manifest), str(target), sha(target),
        str(root / 'config.json'), 'a' * 64, 'inspect']
    result = subprocess.run(args, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0 and result.stdout.strip() == 'safe', result.stderr
    args[6] = 'f' * 64
    result = subprocess.run(args, capture_output=True, text=True, timeout=10)
    assert result.returncode != 0 and not result.stdout


@pytest.mark.parametrize('change', ['source', 'extra_source', 'manifest', 'rpc', 'bytecode_only'])
def test_changed_or_unlisted_app_sources_never_execute(app, change):
    root, manifest = app
    expected_manifest = sha(manifest)
    rpc = root / 'server_rpc.py'
    expected_rpc = sha(rpc)
    if change == 'source': (root / 'dependency.py').write_text("VALUE='evil'\n", encoding='utf8')
    elif change == 'extra_source': (root / 'unlisted.py').write_text('pass\n', encoding='utf8')
    elif change == 'manifest': manifest.write_text('{}', encoding='utf8')
    elif change == 'rpc': rpc.write_text('print("evil")\n', encoding='utf8')
    elif change == 'bytecode_only':
        temporary = root / 'unlisted.py'
        temporary.write_text('VALUE=1\n', encoding='utf8')
        py_compile.compile(str(temporary), cfile=str(root / 'unlisted.pyc'), doraise=True)
        temporary.unlink()
        rpc.write_text('import unlisted\nprint(unlisted.VALUE)\n', encoding='utf8')
        # Pin the altered RPC: only the app-local unlisted bytecode must then be rejected.
        doc = json.loads(manifest.read_text(encoding='utf8'))
        doc['files'][os.path.normcase(str(rpc.resolve()))] = sha(rpc)
        manifest.write_text(json.dumps(doc, sort_keys=True), encoding='utf8')
        expected_manifest, expected_rpc = sha(manifest), sha(rpc)
    result = invoke(root, manifest, target_hash=expected_rpc, manifest_hash=expected_manifest)
    assert result.returncode != 0
    assert not result.stdout


def test_each_unread_helper_and_its_app_dependencies_are_checked(app, tmp_path):
    root, manifest = app
    helper = tmp_path / 'unread.py'
    helper.write_text('import dependency\nprint(dependency.VALUE)\n', encoding='utf8')
    original = sha(helper)
    assert invoke(root, manifest, mode='unread', target=helper, target_hash=original).stdout.strip() == 'safe'
    helper.write_text('print("changed")\n', encoding='utf8')
    result = invoke(root, manifest, mode='unread', target=helper, target_hash=original)
    assert result.returncode != 0 and not result.stdout


def test_native_probe_loads_rpc_imports_but_does_not_run_its_business_main(app):
    root, manifest = app
    service = root / 'server_job_service.py'
    service.write_text('def load_config(path, digest): return {"fixed": True}\n'
        'def verify_native_runtime(config):\n assert config == {"fixed": True}\n', encoding='utf8')
    rpc = root / 'server_rpc.py'
    rpc.write_text('import sys, server_job_service\nassert sys.argv[-1] == "inspect"\n'
                   'if __name__ == "__main__": raise RuntimeError("business main")\n',
                   encoding='utf8')
    doc = json.loads(manifest.read_text(encoding='utf8'))
    doc['files'][os.path.normcase(str(rpc.resolve()))] = sha(rpc)
    doc['files'][os.path.normcase(str(service.resolve()))] = sha(service)
    manifest.write_text(json.dumps(doc, sort_keys=True), encoding='utf8')
    result = invoke(root, manifest, mode='native')
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {'schema': 'paperflow-native-preflight/1', 'status': 'passed'}


def test_source_replaced_after_inventory_is_checked_again_before_import(app):
    root, manifest = app
    rpc = root / 'server_rpc.py'
    rpc.write_text('from pathlib import Path\nPath(' + repr(str(root / 'dependency.py')) +
                   ').write_text("VALUE=1\\n")\nimport dependency\nprint(dependency.VALUE)\n', encoding='utf8')
    doc = json.loads(manifest.read_text(encoding='utf8'))
    doc['files'][os.path.normcase(str(rpc.resolve()))] = sha(rpc)
    manifest.write_text(json.dumps(doc, sort_keys=True), encoding='utf8')
    result = invoke(root, manifest)
    assert result.returncode != 0 and not result.stdout


def test_app_module_that_was_already_loaded_is_rejected(app):
    root, manifest = app
    collision = root / 'json.py'
    collision.write_text('pass\n', encoding='utf8')
    doc = json.loads(manifest.read_text(encoding='utf8'))
    doc['files'][os.path.normcase(str(collision.resolve()))] = sha(collision)
    manifest.write_text(json.dumps(doc, sort_keys=True), encoding='utf8')
    result = invoke(root, manifest)
    assert result.returncode != 0 and not result.stdout


def test_app_cannot_add_a_link_to_an_outside_module_after_inventory(app, tmp_path):
    root, manifest = app
    foreign = tmp_path / 'foreign.py'
    foreign.write_text('print("foreign code ran")\n', encoding='utf8')
    probe = tmp_path / 'link-probe.py'
    try:
        probe.symlink_to(foreign)
        probe.unlink()
    except OSError:
        pytest.skip('creating a synthetic file symlink is not supported')
    rpc = root / 'server_rpc.py'
    rpc.write_text('from pathlib import Path\nPath(' + repr(str(root / 'unlisted.py')) + ').symlink_to(' +
        repr(str(foreign)) + ')\nimport unlisted\n', encoding='utf8')
    doc = json.loads(manifest.read_text(encoding='utf8'))
    doc['files'][os.path.normcase(str(rpc.resolve()))] = sha(rpc)
    manifest.write_text(json.dumps(doc, sort_keys=True), encoding='utf8')
    result = invoke(root, manifest)
    assert result.returncode != 0 and not result.stdout


def test_missing_approved_module_cannot_fall_through_to_a_later_finder(app):
    root, manifest = app
    rpc = root / 'server_rpc.py'
    rpc.write_text('from pathlib import Path\nPath(' + repr(str(root / 'dependency.py')) + ').unlink()\n'
        'import sys, importlib.abc, importlib.util\n'
        'class Foreign(importlib.abc.MetaPathFinder, importlib.abc.Loader):\n'
        ' def find_spec(self, name, path=None, target=None):\n'
        '  return importlib.util.spec_from_loader(name, self) if name == "dependency" else None\n'
        ' def exec_module(self, module): print("foreign finder ran")\n'
        'sys.meta_path.append(Foreign())\nimport dependency\n', encoding='utf8')
    doc = json.loads(manifest.read_text(encoding='utf8'))
    doc['files'][os.path.normcase(str(rpc.resolve()))] = sha(rpc)
    manifest.write_text(json.dumps(doc, sort_keys=True), encoding='utf8')
    result = invoke(root, manifest)
    assert result.returncode != 0 and not result.stdout


def test_approved_names_do_not_disappear_if_a_file_is_removed_after_inventory(app, tmp_path):
    root, manifest = app
    rpc = root / 'server_rpc.py'
    rpc.write_text('import sys, importlib.abc, importlib.util\n'
        'class Foreign(importlib.abc.MetaPathFinder, importlib.abc.Loader):\n'
        ' def find_spec(self, name, path=None, target=None):\n'
        '  return importlib.util.spec_from_loader(name, self) if name == "dependency" else None\n'
        ' def exec_module(self, module): print("foreign finder ran")\n'
        'sys.meta_path.append(Foreign())\nimport dependency\n', encoding='utf8')
    doc = json.loads(manifest.read_text(encoding='utf8'))
    doc['files'][os.path.normcase(str(rpc.resolve()))] = sha(rpc)
    manifest.write_text(json.dumps(doc, sort_keys=True), encoding='utf8')
    guard = tmp_path / 'synthetic_guard.py'
    guard.write_text((HERE / 'release_guard.py').read_text(encoding='utf8') + '\n_old_verify = ReleaseGuard.verify\n'
        'def _remove_after_verify(self):\n result = _old_verify(self)\n Path(' + repr(str(root / 'dependency.py')) +
        ').unlink()\n return result\nReleaseGuard.verify = _remove_after_verify\n', encoding='utf8')
    result = invoke(root, manifest, guard=guard)
    assert result.returncode != 0 and not result.stdout


def test_import_must_match_its_own_original_digest_even_if_reader_redirects(app, tmp_path):
    root, manifest = app
    other = root / 'other.py'
    other.write_text('print("wrong approved module ran")\n', encoding='utf8')
    doc = json.loads(manifest.read_text(encoding='utf8'))
    doc['files'][os.path.normcase(str(other.resolve()))] = sha(other)
    manifest.write_text(json.dumps(doc, sort_keys=True), encoding='utf8')
    guard = tmp_path / 'synthetic_guard.py'
    guard.write_text((HERE / 'release_guard.py').read_text(encoding='utf8') + '\n_old_read = ReleaseGuard.read_verified\n'
        'def _redirect(self, source):\n if Path(source).name == "dependency.py": source = Path(' + repr(str(other)) +
        ')\n return _old_read(self, source)\nReleaseGuard.read_verified = _redirect\n', encoding='utf8')
    result = invoke(root, manifest, guard=guard)
    assert result.returncode != 0 and not result.stdout
