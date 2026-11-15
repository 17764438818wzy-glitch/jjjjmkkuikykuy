from pathlib import Path

import pytest


@pytest.fixture
def resolver(tmp_path, monkeypatch):
    import office_libreoffice as module
    for name in ('LIBREOFFICE_PATH', 'DITY_LIBREOFFICE_PATH'):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delattr(module.executable, '_resolved_program', raising=False)
    binary = tmp_path / 'soffice.com'
    binary.write_bytes(b'fixture never executed')
    calls = []
    monkeypatch.setattr(module.shutil, 'which', lambda name: calls.append(name) or str(binary))
    now = [100.0]
    monkeypatch.setattr(module.time, 'monotonic', lambda: now[0])
    yield module, binary, calls, now
    if hasattr(module.executable, '_resolved_program'):
        delattr(module.executable, '_resolved_program')


def test_repeated_same_environment_avoids_repeated_path_walk(resolver):
    module, binary, calls, now = resolver
    assert module.executable() == binary.resolve()
    assert module.executable() == binary.resolve()
    assert module.executable() == binary.resolve()
    assert len(calls) == 1


def test_environment_change_reselects_existing_explicit_program(resolver, tmp_path, monkeypatch):
    module, binary, calls, now = resolver
    assert module.executable() == binary.resolve()
    explicit = tmp_path / 'explicit.com'
    explicit.write_bytes(b'another fixture never executed')
    monkeypatch.setenv('LIBREOFFICE_PATH', str(explicit))
    assert module.executable() == explicit.resolve()


def test_path_discovery_is_refreshed_after_five_seconds(resolver):
    module, binary, calls, now = resolver
    assert module.executable() == binary.resolve()
    now[0] += 4.9
    assert module.executable() == binary.resolve()
    assert len(calls) == 1
    now[0] += .2
    assert module.executable() == binary.resolve()
    assert len(calls) == 2


def test_missing_cached_program_is_never_returned(resolver):
    module, binary, calls, now = resolver
    assert module.executable() == binary.resolve()
    binary.unlink()
    assert module.executable() != binary.resolve()
    assert len(calls) == 2
