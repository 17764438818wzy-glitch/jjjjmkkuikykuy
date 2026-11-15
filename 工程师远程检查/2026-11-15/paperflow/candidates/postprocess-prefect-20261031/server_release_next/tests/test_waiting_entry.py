"""Tests use a temporary database and source files only."""
import asyncio
import hashlib
import importlib.util
import os
from pathlib import Path
import py_compile
import subprocess
import sys
from types import SimpleNamespace

import httpx


HERE = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_missing_release_pins_fails_closed_without_opening_database(tmp_path):
    cli = load('synthetic_release_cli', HERE / 'waiting_cli.py')
    db = tmp_path / 'must-not-be-created.sqlite3'
    args = SimpleNamespace(db=str(db), max_age_seconds=60, release_manifest=None,
                           release_manifest_sha256=None, remote_config=None,
                           remote_config_sha256=None, site_root=None, guard_sha256=None, layout_sha256=None)
    app, mode = cli.create_release_app(args)
    assert mode == 'closed_release' and not db.exists()
    async def exercise():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://synthetic') as client:
            response = await client.get('/api/v1/local-intake/preprocessing/summary')
            assert response.status_code == 503 and response.json()['status'] == 'release_unavailable'
    asyncio.run(exercise())


def test_bootstrap_ignores_existing_stale_timestamp_bytecode(tmp_path):
    source = tmp_path / 'synthetic_dynamic_contract.py'
    source.write_text('VALUE = 1\n', encoding='utf-8')
    timestamp = source.stat().st_mtime_ns
    py_compile.compile(str(source), doraise=True)
    source.write_text('VALUE = 2\n', encoding='utf-8')
    os.utime(source, ns=(timestamp, timestamp))
    code = '''
import importlib.util, pathlib, sys
runner, target = map(pathlib.Path, sys.argv[1:])
spec = importlib.util.spec_from_file_location("synthetic_runner", runner)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
module.disable_cached_bytecode()
spec = importlib.util.spec_from_file_location("synthetic_dynamic_contract", target)
loaded = importlib.util.module_from_spec(spec)
spec.loader.exec_module(loaded)
assert loaded.VALUE == 2
assert not pathlib.Path(sys.pycache_prefix).exists()
'''
    result = subprocess.run([sys.executable, '-B', '-c', code, str(HERE / 'run_waiting_service.py'), str(source)],
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr


def test_server_entry_does_not_offer_scan_or_watch(tmp_path, monkeypatch):
    cli = load('synthetic_release_cli_no_dispatch', HERE / 'waiting_cli.py')
    class ForbiddenQueue:
        def __init__(self, *args, **kwargs):
            raise AssertionError('a read-only page must not construct a scanner')
    monkeypatch.setitem(sys.modules, 'local_queue', SimpleNamespace(LocalQueue=ForbiddenQueue))
    for command in ('scan', 'watch'):
        try:
            cli.main([command, '--db', str(tmp_path / 'never.sqlite3'), '--receipt-root', str(tmp_path)])
        except SystemExit as exc:
            assert exc.code == 2
        else:
            raise AssertionError('the read-only page entry accepted a scanner command')


def test_projection_entry_refuses_to_replace_the_existing_14205(tmp_path):
    cli = load('synthetic_projection_port_guard', HERE / 'waiting_cli.py')
    try:
        cli.main(['serve', '--db', str(tmp_path / 'never.sqlite3'), '--port', '14205',
                  '--guard-sha256', 'a' * 64, '--layout-sha256', 'b' * 64])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError('the projection may not replace the upstream API')
    assert not (tmp_path / 'never.sqlite3').exists()


def test_history_and_partial_observation_are_explicit_candidate_options(monkeypatch):
    cli = load('synthetic_projection_history_args', HERE / 'waiting_cli.py')
    seen = []
    monkeypatch.setattr(cli, 'create_release_app', lambda args: (seen.append(args) or object(), 'synthetic'))
    import uvicorn
    monkeypatch.setattr(uvicorn, 'run', lambda *_args, **_kwargs: None)
    assert cli.main(['serve', '--db', 'synthetic', '--port', '14206',
        '--guard-sha256', 'a' * 64, '--layout-sha256', 'b' * 64, '--partial-observation',
        '--history-registry', 'synthetic-history', '--history-registry-sha256', 'c' * 64,
        '--history-unread-script', '/synthetic/unread.py', '--history-unread-script-sha256', 'f' * 64,
        '--history-access', 'synthetic-access', '--history-access-sha256', 'e' * 64,
        '--guard-bundle', 'synthetic-guard', '--guard-bundle-sha256', 'a' * 64]) == 0
    assert seen[0].partial_observation is True
    assert seen[0].history_registry == 'synthetic-history' and seen[0].history_unread_script == '/synthetic/unread.py'


def test_incomplete_history_configuration_is_not_silently_used():
    cli = load('synthetic_history_pins', HERE / 'waiting_cli.py')
    assert cli._history_router(SimpleNamespace(), None) is None
    import pytest
    with pytest.raises(ValueError):
        cli._history_router(SimpleNamespace(history_registry='only-one-pin'), None)


def test_candidate_uses_existing_inspect_without_a_version_query_script(monkeypatch):
    cli = load('synthetic_existing_history_cli', HERE / 'waiting_cli.py')
    calls = []
    class Access:
        def __init__(self, *args, **kwargs): calls.append(('access', args, kwargs))
    class Gate:
        def __init__(self, *args): calls.append(('unread', args))
    class Existing:
        def __init__(self, *args, **kwargs): calls.append(('existing_inspect', args, kwargs))
    monkeypatch.setitem(sys.modules, 'read_only_rpc', SimpleNamespace(PinnedAccessBindings=Access,
        UnreadDisclosureGate=Gate, ExistingInspectVersionRouter=Existing))
    args = SimpleNamespace(history_registry='registry', history_registry_sha256='r', history_access='access',
        history_access_sha256='a', history_unread_script='/unread.py', history_unread_script_sha256='u')
    loaded = SimpleNamespace(remote=SimpleNamespace(stage_store=SimpleNamespace(
        inspect_receipt=SimpleNamespace(server={'server_id': 'synthetic'}))))
    assert isinstance(cli._history_router(args, loaded), Existing)
    assert [call[0] for call in calls] == ['access', 'unread', 'existing_inspect']


def test_server_projection_requires_the_guarded_transport(monkeypatch):
    import pytest
    cli = load('synthetic_guarded_entry', HERE / 'waiting_cli.py')
    with pytest.raises(ValueError):
        cli._guarded_delegate(SimpleNamespace(), object(), lambda: None)
    class Guarded:
        def __init__(self, *args): self.args = args
    monkeypatch.setitem(sys.modules, 'read_only_rpc', SimpleNamespace(GuardedAppDelegate=Guarded))
    args = SimpleNamespace(guard_bundle='bundle', guard_bundle_sha256='a' * 64)
    assert isinstance(cli._guarded_delegate(args, object(), lambda: None), Guarded)


def test_release_layout_pins_the_dynamically_loaded_receipt_reader():
    layout = load('synthetic_release_layout', HERE / 'release_layout.py')
    roots, _ = layout.layout(HERE / 'site')
    reader = (layout.CANDIDATES / 'first-step-control-20261029' / 'paperflow_prefect').resolve()
    assert reader.is_dir()
    assert any(entry[0].resolve() == reader and entry[2] and '.py' in entry[1] for entry in roots)


def test_dynamic_completion_reader_executes_only_approved_bytes(tmp_path):
    cli = load('synthetic_release_cli_completion', HERE / 'waiting_cli.py')
    source = tmp_path / 'folder_completion.py'
    source.write_text('def inspect_folder(): return "actual"\n', encoding='utf-8')
    class Guard:
        def read_verified(self, path):
            assert path == source.resolve()
            return b'def inspect_folder(): return "approved"\n'
    assert cli._verified_completion_reader(Guard(), source).inspect_folder() == 'approved'


def test_bootstrap_rejects_changed_source_before_top_level_execution(tmp_path):
    cli = load('synthetic_release_cli_bootstrap', HERE / 'waiting_cli.py')
    marker = tmp_path / 'must-not-be-created.txt'
    source = tmp_path / 'candidate.py'
    source.write_text('from pathlib import Path\nPath(' + repr(str(marker)) + ').write_text("executed")\n',
                      encoding='utf-8')
    try:
        cli._bootstrap('synthetic_untrusted_bootstrap', source, '0' * 64)
    except ValueError:
        pass
    else:
        raise AssertionError('an unapproved bootstrap was executed')
    assert not marker.exists()


def test_runner_rejects_changed_cli_before_execution(tmp_path):
    runner = load('synthetic_release_runner_pins', HERE / 'run_waiting_service.py')
    source = tmp_path / 'candidate.py'
    source.write_bytes(b'pass\n')
    assert runner.verified_source(source, hashlib.sha256(source.read_bytes()).hexdigest()) == b'pass\n'
    try:
        runner.verified_source(source, '0' * 64)
    except ValueError:
        pass
    else:
        raise AssertionError('a changed CLI source was accepted')


def test_explicit_closed_projection_never_connects_remote(tmp_path, monkeypatch):
    cli = load('synthetic_explicit_closed_projection', HERE / 'waiting_cli.py')
    api = load('synthetic_closed_projection_api', HERE / 'waiting_api.py')
    class Queue:
        def __init__(self, *_args, **_kwargs): pass
        def snapshot(self, **_):
            return {'receiver_status': 'running', 'receiver_session_id': 'synthetic',
                    'scan_source_instance': 'synthetic', 'waiting_available': True,
                    'upstream_scan_verified': True, 'upstream_scan_completed_at': '2026-11-01T00:00:00Z',
                    'waiting_llm_total': 0, 'in_progress_count': 0}
    class Observer:
        def summary(self, _): return {'source': 'prefect', 'scope': 'all_tasks', 'available': True, 'counts': {}}
        def preprocessing_results(self, *_): return {'available': True, 'counts': {'passed': 2}}
        def llm_results(self, *_): return {'available': True, 'counts': {'passed': 3}}
    class Guard:
        def __init__(self, *_args, **_kwargs): pass
        def verify(self): return True
        def load_source(self, name, _path): return api if name == '_release_fallback_waiting_api' else object()
    class Finder:
        def __init__(self, _guard): pass
        def find_spec(self, *_args): return None
    layout = SimpleNamespace(layout=lambda: ([], []), PACKAGES=())
    monkeypatch.setattr(cli, '_bootstrap', lambda name, *_: SimpleNamespace(ReleaseGuard=Guard, PinnedFinder=Finder)
                        if name == 'release_guard' else layout)
    monkeypatch.setattr(cli, 'configured_orders', lambda _queue, *, guard: Observer())
    monkeypatch.setitem(sys.modules, 'read_only_queue', SimpleNamespace(open_queue=lambda path: Queue(path)))
    monkeypatch.setattr(sys, 'meta_path', list(sys.meta_path))
    monkeypatch.setattr(sys, 'path', list(sys.path))
    args = SimpleNamespace(db=str(tmp_path / 'never.sqlite3'), max_age_seconds=60,
        release_manifest='synthetic', release_manifest_sha256='a' * 64, remote_config=None,
        remote_config_sha256=None, site_root='synthetic', guard_sha256='b' * 64, layout_sha256='c' * 64,
        closed_projection=True)
    app, mode = cli.create_release_app(args)
    assert mode == 'closed_projection'
    async def exercise():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://synthetic') as client:
            result = await client.get('/api/v1/local-intake/preprocessing/summary')
            assert result.status_code == 200
            value = result.json()
            assert value['preprocessing_results']['available'] is False
            assert value['llm_results']['available'] is False
            assert value['postprocessing_registered_ledger']['available'] is False
    asyncio.run(exercise())
