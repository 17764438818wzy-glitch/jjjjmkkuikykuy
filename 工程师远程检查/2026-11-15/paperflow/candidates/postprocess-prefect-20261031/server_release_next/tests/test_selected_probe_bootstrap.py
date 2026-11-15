"""Offline tests of the selected observation's local import boundary."""
import importlib.machinery
import importlib.util
import hashlib
import os
from pathlib import Path
import subprocess
import sys
from types import ModuleType, SimpleNamespace

import pytest


HERE = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('test_selected_bootstrap_module', HERE / 'selected_probe_bootstrap.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


@pytest.fixture
def finder(tmp_path):
    source = tmp_path / 'sample.py'
    source.write_text('value = 1', encoding='utf8')
    seen = []
    def read(path):
        seen.append(path)
        return source.read_bytes()
    guard = SimpleNamespace(roots=((tmp_path, ('.py',), False),),
                            _approved={os.path.normcase(str(source)): hashlib.sha256(source.read_bytes()).hexdigest()},
                            read_verified=read)
    loader = SimpleNamespace(_VerifiedLoader=lambda *_args: 'verified-loader')
    return module._SourceOnlyFinder(guard, loader), source, seen


@pytest.mark.parametrize('suffix', ['.pyc', '.pyd'])
def test_compiled_candidate_module_is_rejected(finder, monkeypatch, suffix):
    watcher, source, seen = finder
    replacement = source.with_suffix(suffix)
    monkeypatch.setattr(importlib.machinery.PathFinder, 'find_spec',
                        lambda *_args: importlib.machinery.ModuleSpec('sample', None, origin=str(replacement)))
    with pytest.raises(ImportError):
        watcher.find_spec('sample')
    assert not seen


def test_source_from_outside_cannot_shadow_candidate(finder, monkeypatch, tmp_path):
    watcher, _source, seen = finder
    replacement = tmp_path.parent / 'sample.py'
    monkeypatch.setattr(importlib.machinery.PathFinder, 'find_spec',
                        lambda *_args: importlib.machinery.ModuleSpec('sample', None, origin=str(replacement)))
    with pytest.raises(ImportError):
        watcher.find_spec('sample')
    assert not seen


def test_approved_source_is_verified_before_use(finder, monkeypatch):
    watcher, source, seen = finder
    candidate = importlib.machinery.ModuleSpec('sample', None, origin=str(source))
    monkeypatch.setattr(importlib.machinery.PathFinder, 'find_spec', lambda *_args: candidate)
    assert isinstance(watcher.find_spec('sample').loader, module._ExactSourceLoader)
    assert seen == [source]


def test_symlink_cannot_escape_candidate_root(finder, monkeypatch, tmp_path):
    watcher, source, _seen = finder
    outside = tmp_path.parent / 'outside.py'
    outside.write_text('value = 2', encoding='utf8')
    source.unlink()
    try:
        source.symlink_to(outside)
    except OSError:
        pytest.skip('symlink creation is unavailable')
    monkeypatch.setattr(importlib.machinery.PathFinder, 'find_spec',
                        lambda *_args: importlib.machinery.ModuleSpec('sample', None, origin=str(source)))
    with pytest.raises(ImportError):
        watcher.find_spec('sample')


def test_execution_rechecks_the_named_source_not_another_approved_target(tmp_path, monkeypatch):
    source = tmp_path / 'sample.py'
    source.write_text('value = 1', encoding='utf8')
    approved = source.read_bytes()
    changed = b'value = 2'
    replies = iter((approved, changed))
    guard = SimpleNamespace(roots=((tmp_path, ('.py',), False),),
                            _approved={os.path.normcase(str(source)): hashlib.sha256(approved).hexdigest()},
                            read_verified=lambda _path: next(replies))
    watcher = module._SourceOnlyFinder(guard, None)
    candidate = importlib.machinery.ModuleSpec('sample', None, origin=str(source))
    monkeypatch.setattr(importlib.machinery.PathFinder, 'find_spec', lambda *_args: candidate)
    spec = watcher.find_spec('sample')
    with pytest.raises(ImportError):
        spec.loader.exec_module(ModuleType('sample'))


def test_bootstrap_setup_error_does_not_emit_private_paths(tmp_path):
    marker = 'synthetic-private-key-path'
    result = subprocess.run([sys.executable, '-B', str(HERE / 'selected_probe_bootstrap.py'),
        '--inventory', str(tmp_path / marker), '--inventory-sha256', '0' * 64,
        '--guard-sha256', '0' * 64, '--layout-sha256', '0' * 64],
        capture_output=True, text=True, timeout=10)
    assert result.returncode != 0
    assert marker not in result.stdout + result.stderr
    assert 'observation_unavailable' in result.stdout
    assert not result.stderr


@pytest.mark.parametrize('args', [[], ['--unexpected', 'synthetic-private-key-path'], ['--help']])
def test_bootstrap_parse_errors_emit_no_usage_or_arguments(args):
    result = subprocess.run([sys.executable, '-B', str(HERE / 'selected_probe_bootstrap.py'), *args],
                            capture_output=True, text=True, timeout=10)
    assert result.returncode != 0
    assert 'observation_unavailable' in result.stdout
    assert 'synthetic-private-key-path' not in result.stdout
    assert not result.stderr
