"""Launcher is tested only with a temporary fake entry, never the real API."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


HERE = Path(__file__).resolve().parents[1]
POWERSHELL = shutil.which('powershell.exe') or shutil.which('pwsh')
pytestmark = pytest.mark.skipif(not POWERSHELL, reason='PowerShell is unavailable')


def invoke(tmp_path, runner_sha, closed=True, python_exe=None, port=None, extra=None):
    runner = tmp_path / 'fake_runner.py'
    command = [POWERSHELL, '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass',
        '-File', str(HERE / 'start_verified_page.ps1'), '-PythonExe', python_exe or sys.executable,
        '-RunnerPath', str(runner), '-RunnerSha256', runner_sha, '-CliSha256', 'a' * 64,
        '-GuardSha256', 'b' * 64, '-LayoutSha256', 'c' * 64,
        '-QueueDatabase', str(tmp_path / 'never-opened.sqlite3'), '-Log', str(tmp_path / 'never-written.log'),
        '-ReleaseManifest', str(tmp_path / 'never-read.json'),
        '-ReleaseManifestSha256', 'd' * 64]
    if closed:
        command += ['-ClosedProjection']
    else:
        command += ['-RemoteConfig', str(tmp_path / 'never-read-remote.json'), '-RemoteConfigSha256', 'e' * 64]
    if port is not None:
        command += ['-Port', str(port)]
    if extra:
        command += extra
    return subprocess.run(command, capture_output=True, text=True, encoding='utf8', errors='replace', timeout=20)


def test_launcher_verifies_runner_bytes_and_injects_closed_projection_arguments(tmp_path):
    runner = tmp_path / 'fake_runner.py'
    captured = tmp_path / 'captured.json'
    runner.write_text('import json,sys\nfrom pathlib import Path\nPath(' + repr(str(captured)) +
                      ').write_text(json.dumps(sys.argv[1:]))\n', encoding='utf8')
    result = invoke(tmp_path, hashlib.sha256(runner.read_bytes()).hexdigest())
    assert result.returncode == 0, result.stderr
    args = json.loads(captured.read_text())
    assert args[0] == '--log' and args[2] == '--cli-sha256' and args[4:6] == ['--', 'serve']
    assert '--closed-projection' in args and '--remote-config' not in args
    assert not (tmp_path / 'never-opened.sqlite3').exists()


def test_launcher_refuses_changed_runner_before_any_top_level_code(tmp_path):
    runner = tmp_path / 'fake_runner.py'
    marker = tmp_path / 'must-not-exist'
    runner.write_text('from pathlib import Path\nPath(' + repr(str(marker)) + ').touch()\n', encoding='utf8')
    result = invoke(tmp_path, '0' * 64)
    assert result.returncode != 0
    assert not marker.exists()


def test_launcher_passes_explicit_remote_config_without_opening_it(tmp_path):
    runner = tmp_path / 'fake_runner.py'
    captured = tmp_path / 'captured.json'
    runner.write_text('import json,sys\nfrom pathlib import Path\nPath(' + repr(str(captured)) +
                      ').write_text(json.dumps(sys.argv[1:]))\n', encoding='utf8')
    result = invoke(tmp_path, hashlib.sha256(runner.read_bytes()).hexdigest(), closed=False)
    assert result.returncode == 0, result.stderr
    args = json.loads(captured.read_text())
    assert args[args.index('--remote-config') + 1] == str(tmp_path / 'never-read-remote.json')
    assert args[args.index('--remote-config-sha256') + 1] == 'e' * 64
    assert '--closed-projection' not in args


def test_launcher_waits_for_pythonw_child_to_exit(tmp_path):
    pythonw = Path(sys.executable).with_name('pythonw.exe')
    if not pythonw.is_file():
        pytest.skip('pythonw.exe is unavailable')
    runner = tmp_path / 'fake_runner.py'
    marker = tmp_path / 'finished'
    runner.write_text('import time\nfrom pathlib import Path\ntime.sleep(.7)\nPath(' +
                      repr(str(marker)) + ').touch()\n', encoding='utf8')
    result = invoke(tmp_path, hashlib.sha256(runner.read_bytes()).hexdigest(), python_exe=str(pythonw))
    assert result.returncode == 0, result.stderr
    assert marker.exists(), 'launcher returned before the pythonw child completed'


def test_launcher_preserves_unicode_and_space_paths(tmp_path):
    folder = tmp_path / '资料 with spaces'
    folder.mkdir()
    runner = folder / 'fake_runner.py'
    captured = folder / 'captured.json'
    runner.write_text('import json,sys\nfrom pathlib import Path\nPath(' + repr(str(captured)) +
                      ').write_text(json.dumps(sys.argv[1:]))\n', encoding='utf8')
    result = invoke(folder, hashlib.sha256(runner.read_bytes()).hexdigest())
    assert result.returncode == 0, result.stderr
    args = json.loads(captured.read_text())
    assert args[args.index('--db') + 1] == str(folder / 'never-opened.sqlite3')


def test_launcher_refuses_the_existing_upstream_port(tmp_path):
    runner = tmp_path / 'fake_runner.py'
    marker = tmp_path / 'must-not-exist'
    runner.write_text('from pathlib import Path\nPath(' + repr(str(marker)) + ').touch()\n', encoding='utf8')
    result = invoke(tmp_path, hashlib.sha256(runner.read_bytes()).hexdigest(), port=14205)
    assert result.returncode != 0 and not marker.exists()


def test_launcher_passes_partial_and_pinned_history_options_to_candidate(tmp_path):
    runner = tmp_path / 'fake_runner.py'
    captured = tmp_path / 'captured.json'
    runner.write_text('import json,sys\nfrom pathlib import Path\nPath(' + repr(str(captured)) +
                      ').write_text(json.dumps(sys.argv[1:]))\n', encoding='utf8')
    extra = ['-PartialObservation', '-HistoryRegistry', str(tmp_path / 'history.json'),
             '-HistoryRegistrySha256', '1' * 64, '-HistoryAccess', str(tmp_path / 'access.json'),
             '-HistoryAccessSha256', '3' * 64, '-HistoryUnreadScript', '/synthetic/unread.py',
             '-HistoryUnreadScriptSha256', '4' * 64, '-GuardBundle', str(tmp_path / 'guard.json'),
             '-GuardBundleSha256', '5' * 64]
    result = invoke(tmp_path, hashlib.sha256(runner.read_bytes()).hexdigest(), closed=False, extra=extra)
    assert result.returncode == 0, result.stderr
    args = json.loads(captured.read_text())
    assert '--partial-observation' in args
    assert args[args.index('--history-registry') + 1] == str(tmp_path / 'history.json')
    assert args[args.index('--history-unread-script') + 1] == '/synthetic/unread.py'
    assert args[args.index('--history-access-sha256') + 1] == '3' * 64
    assert args[args.index('--guard-bundle') + 1] == str(tmp_path / 'guard.json')
