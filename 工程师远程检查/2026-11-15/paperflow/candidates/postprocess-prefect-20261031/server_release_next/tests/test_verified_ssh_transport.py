"""Synthetic bounded subprocess transport; never opens a network connection."""
import hashlib
import base64
import importlib.util
import json
import os
from pathlib import Path
import py_compile
import subprocess
import sys
import types

import pytest

HERE = Path(__file__).resolve().parents[1]
SERVER = HERE.parents[1] / 'postprocess-server-20261101'


def load():
    path = HERE / 'verified_ssh_transport.py'
    assert path.is_file(), 'the pinned local SSH helper transport is missing'
    spec = importlib.util.spec_from_file_location('synthetic_verified_ssh_transport', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Guard:
    def __init__(self, paths):
        self._approved = {os.path.normcase(str(path.resolve())): hashlib.sha256(path.read_bytes()).hexdigest()
                          for path in paths}
    def read_verified(self, path):
        raw = Path(path).read_bytes()
        if hashlib.sha256(raw).hexdigest() != self._approved[os.path.normcase(str(Path(path).resolve()))]:
            raise ValueError('source changed')
        return raw
    def load_source(self, name, path):
        module = types.ModuleType(name)
        module.__file__ = str(path)
        exec(compile(self.read_verified(path), str(path), 'exec'), module.__dict__)
        return module


@pytest.fixture
def sources(tmp_path):
    client = tmp_path / 'ssh_rpc_client.py'
    client.write_bytes((SERVER / 'ssh_rpc_client.py').read_bytes())
    helper = tmp_path / 'ssh_rpc_process.py'
    helper.write_text('import json,sys,ssh_rpc_client,paramiko\n'
        'r=json.load(sys.stdin)\nprint(json.dumps({"command":r["command"],'
        '"loaded":ssh_rpc_client.call_once.__name__,"paramiko":paramiko.__version__}))\n', encoding='utf8')
    conn = {'host': 'synthetic', 'port': 22, 'username': 'test', 'private_key': str(tmp_path / 'key'),
            'server_key_sha256': 'a' * 64}
    return client, helper, conn


def test_fixed_helper_and_client_bytes_are_used_without_stale_pyc(sources):
    client, helper, conn = sources
    good = client.read_bytes()
    before = client.stat().st_mtime_ns
    evil = b'def call_once(*args, **kwargs): pass\ncall_once.__name__ = "evil"\n'
    evil += b'#' + b'x' * (len(good) - len(evil) - 2) + b'\n'
    assert len(evil) == len(good)
    client.write_bytes(evil)
    os.utime(client, ns=(before, before))
    py_compile.compile(str(client), doraise=True)
    client.write_bytes(good)
    os.utime(client, ns=(before, before))
    guard = Guard([client, helper])
    call = load().build(guard, client, helper)
    result = call(conn, 'synthetic-read', {'hello': 'world'}, timeout=5, max_bytes=100000)
    assert result['command'] == 'synthetic-read' and result['loaded'] == 'call_once'
    assert result['paramiko']


def test_file_replacement_after_source_read_does_not_change_the_child(sources, monkeypatch):
    client, helper, conn = sources
    guard = Guard([client, helper])
    original = guard.read_verified
    def replace_after_read(path):
        raw = original(path)
        if Path(path) == helper:
            helper.write_text('print("changed")\n', encoding='utf8')
        return raw
    monkeypatch.setattr(guard, 'read_verified', replace_after_read)
    call = load().build(guard, client, helper)
    assert call(conn, 'synthetic-read', {}, timeout=5, max_bytes=100000)['command'] == 'synthetic-read'


def test_modified_helper_is_rejected_before_child_execution(sources):
    client, helper, conn = sources
    guard = Guard([client, helper])
    call = load().build(guard, client, helper)
    helper.write_text('print("changed")\n', encoding='utf8')
    with pytest.raises(ValueError):
        call(conn, 'synthetic-read', {}, timeout=5, max_bytes=100000)


def test_approved_target_cannot_be_selected_through_a_different_link(sources, tmp_path):
    client, helper, _conn = sources
    alias = tmp_path / 'linked-client.py'
    try:
        alias.symlink_to(client)
    except OSError:
        pytest.skip('symlink creation is unavailable')
    with pytest.raises(ValueError):
        load().build(Guard([client, helper]), alias, helper)


def test_timeout_during_envelope_serialization_does_not_start_a_child(sources, monkeypatch):
    client, helper, conn = sources
    module = load()
    call = module.build(Guard([client, helper]), client, helper)
    clock = [0.0]
    monkeypatch.setattr(module.time, 'monotonic', lambda: clock[0])
    original = module.json.dumps
    def slow_dumps(*args, **kwargs):
        clock[0] = 2.0
        return original(*args, **kwargs)
    monkeypatch.setattr(module.json, 'dumps', slow_dumps)
    monkeypatch.setattr(module.subprocess, 'Popen', lambda *_a, **_k: pytest.fail('expired call started a child'))
    with pytest.raises(ValueError, match='timed out'):
        call(conn, 'synthetic-read', {}, timeout=1, max_bytes=100000)


@pytest.mark.parametrize(('stderr', 'reason'), [
    (b'isolated SSH request failed: AuthenticationException\n', 'authentication_failed'),
    (b'isolated SSH request failed: RemoteCallError\n', 'request_refused_or_unknown'),
    (b'private response and secret text', 'helper_failed'),
])
def test_failed_helper_reports_only_whitelisted_reason(sources, monkeypatch, stderr, reason):
    client, helper, conn = sources
    module = load()
    call = module.build(Guard([client, helper]), client, helper)
    class Process:
        returncode = 1
        def communicate(self, *_args, **_kwargs):
            return b'', stderr
    monkeypatch.setattr(module.subprocess, 'Popen', lambda *_args, **_kwargs: Process())
    with pytest.raises(Exception) as failure:
        call(conn, 'synthetic-read', {}, timeout=5, max_bytes=100000)
    assert str(failure.value) == 'isolated_ssh:' + reason
    assert failure.value.observation_reason == reason


def test_child_rejects_a_different_same_named_paramiko_package(tmp_path):
    module = load()
    chosen, shadow = tmp_path / 'chosen', tmp_path / 'shadow'
    for root in (chosen, shadow):
        (root / 'paramiko').mkdir(parents=True)
        (root / 'paramiko' / '__init__.py').write_text('VERSION="synthetic"\n', encoding='utf8')
    client = b'pass\n'
    helper = b'import paramiko\nprint("wrong package executed")\n'
    payload = {'client_source': base64.b64encode(client).decode(),
               'helper_source': base64.b64encode(helper).decode(), 'request': {}}
    code = 'import sys;sys.path.insert(0,' + repr(str(shadow)) + ')\n' + module.CHILD_BOOTSTRAP
    args = [sys.executable, '-I', '-B', '-c', code, str(tmp_path / 'client.py'), hashlib.sha256(client).hexdigest(),
            str(tmp_path / 'helper.py'), hashlib.sha256(helper).hexdigest(), str(chosen),
            str(chosen / 'paramiko' / '__init__.py')]
    result = subprocess.run(args, input=json.dumps(payload), capture_output=True, text=True, timeout=5)
    assert result.returncode != 0 and not result.stdout


def test_real_helper_can_report_a_synthetic_auth_failure_without_network(sources):
    client, helper, conn = sources
    helper.write_bytes((SERVER / 'ssh_rpc_process.py').read_bytes())
    module = load()
    before = 'import paramiko\nif os.path.normcase(os.path.realpath(paramiko.__file__)) != expected:'
    after = ('import paramiko\n'
             'def _synthetic_no_network(*args, **kwargs): raise paramiko.AuthenticationException("synthetic")\n'
             'paramiko.SSHClient.connect = _synthetic_no_network\n'
             'if os.path.normcase(os.path.realpath(paramiko.__file__)) != expected:')
    assert before in module.CHILD_BOOTSTRAP
    module.CHILD_BOOTSTRAP = module.CHILD_BOOTSTRAP.replace(before, after)
    call = module.build(Guard([client, helper]), client, helper)
    with pytest.raises(Exception) as failure:
        call(conn, 'synthetic-read', {}, timeout=5, max_bytes=100000)
    assert failure.value.observation_reason == 'authentication_failed'


def test_helper_bootstrap_accepts_non_ascii_source_paths_without_network(tmp_path):
    root = tmp_path / '论文预处理'
    root.mkdir()
    client, helper = root / 'ssh_rpc_client.py', root / 'ssh_rpc_process.py'
    client.write_bytes((SERVER / 'ssh_rpc_client.py').read_bytes())
    helper.write_bytes((SERVER / 'ssh_rpc_process.py').read_bytes())
    module = load()
    before = 'import paramiko\nif os.path.normcase(os.path.realpath(paramiko.__file__)) != expected:'
    after = ('import paramiko\n'
             'def _synthetic_no_network(*args, **kwargs): raise paramiko.AuthenticationException("synthetic")\n'
             'paramiko.SSHClient.connect = _synthetic_no_network\n'
             'if os.path.normcase(os.path.realpath(paramiko.__file__)) != expected:')
    assert before in module.CHILD_BOOTSTRAP
    module.CHILD_BOOTSTRAP = module.CHILD_BOOTSTRAP.replace(before, after)
    call = module.build(Guard([client, helper]), client, helper)
    conn = {'host': 'synthetic', 'port': 22, 'username': 'test', 'private_key': str(root / 'key'),
            'server_key_sha256': 'a' * 64}
    with pytest.raises(Exception) as failure:
        call(conn, 'synthetic-read', {}, timeout=5, max_bytes=100000)
    assert failure.value.observation_reason == 'authentication_failed'


def test_helper_bootstrap_rejects_invalid_utf8_before_loading_sources():
    module = load()
    result = subprocess.run([sys.executable, '-I', '-B', '-c', module.CHILD_BOOTSTRAP],
                            input=b'\xff', capture_output=True, timeout=5)
    assert result.returncode != 0 and not result.stdout
    assert b'UnicodeDecodeError' in result.stderr
