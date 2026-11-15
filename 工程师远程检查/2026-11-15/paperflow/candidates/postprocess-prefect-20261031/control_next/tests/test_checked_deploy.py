"""Exercise deployment recovery using only temporary files and fake services."""
import hashlib
import importlib.util
import json
import sqlite3
import sys
import types
from pathlib import Path

import pytest


MODULE_PATH = Path(__file__).resolve().parents[2] / 'deploy_control_checked.py'


@pytest.fixture
def deployment(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location('checked_deploy_test', MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    live = tmp_path / 'live'
    candidate = tmp_path / 'candidate'
    live.mkdir()
    candidate.mkdir()
    (tmp_path / 'deployment-backups').mkdir()
    with sqlite3.connect(tmp_path / 'db.sqlite3') as db:
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('CREATE TABLE test (value INTEGER)')
    hashes = {}
    for name in ('a.py', 'b.py'):
        old, new = ('old-' + name).encode(), ('new-' + name).encode()
        (live / name).write_bytes(old)
        (candidate / name).write_bytes(new)
        hashes[name] = (hashlib.sha256(old).hexdigest(), hashlib.sha256(new).hexdigest())
    fake_source = types.ModuleType('paperflow_prefect.local_preprocessing')
    fake_source._control_paths = lambda: (tmp_path / 'db.sqlite3', None)
    monkeypatch.setitem(sys.modules, 'paperflow_prefect.local_preprocessing', fake_source)
    monkeypatch.setattr(module, 'ROOT', tmp_path)
    monkeypatch.setattr(module, 'WORKSPACE', tmp_path)
    monkeypatch.setattr(module, 'LIVE', live)
    monkeypatch.setattr(module, 'CANDIDATE', candidate)
    monkeypatch.setattr(module, 'FILES', hashes)
    states = {name: 'Running' for name in module.TASKS}
    actions = []
    def power(action, name):
        actions.append((action, name))
        states[name] = 'Running' if action == 'Start' else 'Ready'
    monkeypatch.setattr(module, 'powershell', power)
    monkeypatch.setattr(module, 'task_states', lambda: dict(states))
    monkeypatch.setattr(module, 'service_processes', lambda **_: {123: 1.0} if 'Running' in states.values() else {})
    monkeypatch.setattr(module, 'wait_stopped', lambda previous: None)
    monkeypatch.setattr(module, 'assert_prefect_idle', lambda api: None)
    monkeypatch.setattr(module, 'assert_deployments', lambda api: None)
    monkeypatch.setattr(module, 'assert_control_idle', lambda database: 40)
    monkeypatch.setattr(module, 'assert_closed_gates', lambda database: None)
    monkeypatch.setattr(module, 'health', lambda: {
        'prefect_api_url': 'http://unused', 'instance_id': 'old',
        'summary_index': str(tmp_path / 'db.sqlite3'), 'local_intake_queue_db': 'queue',
        'receipt_root': 'receipts', 'scope': 'test'})
    monkeypatch.setattr(module, 'wait_ready', lambda *args: None)
    return module, live, states, actions


def test_checked_deploy_installs_and_records_backup(deployment, capsys):
    module, live, states, actions = deployment
    module.main()
    result = json.loads(capsys.readouterr().out)
    assert (live / 'a.py').read_text() == 'new-a.py'
    assert (Path(result['backup']) / 'a.py').read_text() == 'old-a.py'
    assert result['unknown_llm_requests_before_restart'] == 40
    assert set(states.values()) == {'Running'}
    assert [action for action, _ in actions] == ['Stop', 'Stop', 'Start', 'Start']


def test_failed_health_check_restores_original_files(deployment, monkeypatch):
    module, live, states, actions = deployment
    calls = []
    def wait(*args):
        calls.append(None)
        if len(calls) == 1:
            raise RuntimeError('synthetic failed health check')
    monkeypatch.setattr(module, 'wait_ready', wait)
    with pytest.raises(RuntimeError, match='已恢复原控制文件及服务'):
        module.main()
    assert (live / 'a.py').read_text() == 'old-a.py'
    assert (live / 'b.py').read_text() == 'old-b.py'
    assert set(states.values()) == {'Running'}


def test_external_file_change_is_never_overwritten(deployment, monkeypatch):
    module, live, states, actions = deployment
    def wait(*args):
        (live / 'a.py').write_text('changed-by-another-session')
        raise RuntimeError('synthetic failed health check')
    monkeypatch.setattr(module, 'wait_ready', wait)
    with pytest.raises(RuntimeError, match='自动恢复未确认'):
        module.main()
    assert (live / 'a.py').read_text() == 'changed-by-another-session'


def test_new_work_prevents_recovery_restart(deployment, monkeypatch):
    module, live, states, actions = deployment
    checks = []
    def idle(database):
        checks.append(None)
        if len(checks) >= 5:
            raise RuntimeError('synthetic new active work')
        return 40
    monkeypatch.setattr(module, 'assert_control_idle', idle)
    monkeypatch.setattr(module, 'wait_ready', lambda *args: (_ for _ in ()).throw(RuntimeError('health')))
    with pytest.raises(RuntimeError, match='自动恢复未确认'):
        module.main()
    assert (live / 'a.py').read_text() == 'new-a.py'
    assert set(states.values()) == {'Running'}
    assert [action for action, _ in actions] == ['Stop', 'Stop', 'Start', 'Start']


def test_control_fence_blocks_other_writers(deployment, tmp_path):
    module, *_ = deployment
    database = tmp_path / 'db.sqlite3'
    with module.control_write_fence(database):
        with sqlite3.connect(database, timeout=.05) as other:
            with pytest.raises(sqlite3.OperationalError, match='locked'):
                other.execute('INSERT INTO test VALUES (1)')
    with sqlite3.connect(database, timeout=.05) as other:
        other.execute('INSERT INTO test VALUES (2)')


def test_interruption_after_replace_is_restored(deployment, monkeypatch):
    module, live, states, _ = deployment
    replace = module.os.replace
    def interrupted(src, dst):
        replace(src, dst)
        if '.postprocessing-update-a.py.tmp' in str(src):
            raise RuntimeError('synthetic interruption after replace')
    monkeypatch.setattr(module.os, 'replace', interrupted)
    with pytest.raises(RuntimeError, match='已恢复原控制文件及服务'):
        module.main()
    assert (live / 'a.py').read_text() == 'old-a.py'
    assert (live / 'b.py').read_text() == 'old-b.py'
    assert set(states.values()) == {'Running'}


def test_corrupted_backup_prevents_stopping_services(deployment, monkeypatch):
    module, live, states, actions = deployment
    copy = module.shutil.copy2
    def corrupt(src, dst):
        copy(src, dst)
        Path(dst).write_text('corrupt')
    monkeypatch.setattr(module.shutil, 'copy2', corrupt)
    with pytest.raises(RuntimeError, match='备份哈希不一致'):
        module.main()
    assert actions == []
    assert (live / 'a.py').read_text() == 'old-a.py'


def test_other_actor_unattempted_change_is_not_reverted(deployment, monkeypatch):
    module, live, states, _ = deployment
    replace = module.os.replace
    def interleaved(src, dst):
        replace(src, dst)
        if '.postprocessing-update-a.py.tmp' in str(src):
            (live / 'b.py').write_text('new-b.py')
            raise RuntimeError('synthetic concurrent update')
    monkeypatch.setattr(module.os, 'replace', interleaved)
    with pytest.raises(RuntimeError, match='自动恢复未确认'):
        module.main()
    assert (live / 'b.py').read_text() == 'new-b.py'


def test_incomplete_previous_install_requires_reconciliation(deployment, tmp_path):
    module, live, states, actions = deployment
    journal = tmp_path / 'deployment-backups' / 'control-install-state.json'
    module.write_install_state(journal, {'status': 'active', 'attempted': ['a.py']})
    with pytest.raises(RuntimeError, match='未完成的控制安装记录'):
        module.main()
    assert actions == []
    assert (live / 'a.py').read_text() == 'old-a.py'


@pytest.mark.parametrize('failed_action', ['Stop', 'Start'])
def test_service_action_failure_recovers_originals(deployment, monkeypatch, failed_action):
    module, live, states, actions = deployment
    power = module.powershell
    failures = []
    def fail_once(action, task):
        if action == failed_action and task == module.TASKS[0] and not failures:
            failures.append((action, task))
            raise RuntimeError('synthetic service failure')
        power(action, task)
    monkeypatch.setattr(module, 'powershell', fail_once)
    with pytest.raises(RuntimeError, match='已恢复原控制文件及服务'):
        module.main()
    assert failures
    assert (live / 'a.py').read_text() == 'old-a.py'
    assert (live / 'b.py').read_text() == 'old-b.py'
    assert set(states.values()) == {'Running'}
