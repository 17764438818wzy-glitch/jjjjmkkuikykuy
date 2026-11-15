"""Synthetic pause-vs-business-gate tests on the existing server ledger."""
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'tests'))
from test_server_supervisor import SyntheticScope, make_job
from remote_jobs import RemoteJobs
from server_supervisor import ServerSupervisor, SupervisionUnavailable
from task_generation_fence import TaskGenerationFence, check_business_gate


def setup_synthetic(tmp_path):
    ledger = RemoteJobs(tmp_path / 'ledger.sqlite3', capacity=8)
    ledger.initialize()
    fence = TaskGenerationFence(ledger)
    fence.install()
    job = make_job()
    ledger.reserve(job)
    supervisor = ServerSupervisor(ledger, cgroup_parent=tmp_path / 'scopes',
        worker_uid=65534, worker_gid=65534, receipt_validator=lambda *_: False,
        clock=lambda: 100.0, monotonic_clock=lambda: 100.0,
        boot_id_provider=lambda: 'synthetic-boot', business_gate_check=check_business_gate)
    supervisor.initialize()
    return ledger, fence, job, supervisor


def test_closed_before_gate_release_prevents_business_launch(tmp_path):
    ledger, fence, job, supervisor = setup_synthetic(tmp_path)
    target = tmp_path / 'business-launched.txt'
    original = SyntheticScope.launch
    def pause_before_gate(scope, *args, **kwargs):
        result = original(scope, *args, **kwargs)
        assert fence.close(job['identity']['task_id'], job['generation'])['jobs']['running'] == 1
        return result
    with patch('server_supervisor.LinuxScope', SyntheticScope), patch.object(SyntheticScope, 'launch', pause_before_gate):
        with pytest.raises(SupervisionUnavailable):
            supervisor.launch(job, [str(target)], timeout_seconds=20)
    assert not target.exists()
    assert ledger.status(job['job_id'])['state'] == 'unknown'


def test_gate_released_before_close_is_counted_as_already_running(tmp_path):
    ledger, fence, job, supervisor = setup_synthetic(tmp_path)
    target = tmp_path / 'business-launched.txt'
    with patch('server_supervisor.LinuxScope', SyntheticScope):
        assert supervisor.launch(job, [str(target)], timeout_seconds=20)['started'] is True
    assert target.read_text() == 'one execution\n'
    observed = fence.close(job['identity']['task_id'], job['generation'])
    assert observed['jobs']['running'] == 1
    assert observed['stopped_or_terminated_not_proven'] is True


def test_permission_before_close_can_execute_later_so_pause_is_not_proven_complete(tmp_path):
    _, fence, job, supervisor = setup_synthetic(tmp_path)
    target = tmp_path / 'business-launched.txt'
    class DelayedScope(SyntheticScope):
        def release_gate(scope):
            # Simulate the pipe byte being issued while the worker has not yet
            # been scheduled to read it and execute its business command.
            scope.permitted = True
        def actual_business_start(scope):
            assert scope.permitted
            scope.output.write_text('business executed after close\n')
    with patch('server_supervisor.LinuxScope', DelayedScope):
        assert supervisor.launch(job, [str(target)], timeout_seconds=20)['started'] is True
    scope = DelayedScope.scopes[str(tmp_path / 'scopes' / job['job_id'])]
    assert not target.exists()
    observed = fence.close(job['identity']['task_id'], job['generation'])
    assert observed['jobs']['running'] == 1
    assert observed['stopped_or_terminated_not_proven'] is True
    scope.actual_business_start()
    assert target.read_text() == 'business executed after close\n'


def test_changed_gate_checker_cannot_authorize_business_launch(tmp_path):
    ledger, _, job, supervisor = setup_synthetic(tmp_path)
    target = tmp_path / 'business-launched.txt'
    supervisor.business_gate_check = lambda *_: True
    with patch('server_supervisor.LinuxScope', SyntheticScope):
        with pytest.raises(SupervisionUnavailable):
            supervisor.launch(job, [str(target)], timeout_seconds=20)
    assert not target.exists()
    assert ledger.status(job['job_id'])['state'] == 'unknown'


def test_removed_gate_checker_cannot_authorize_business_launch(tmp_path):
    ledger, _, job, supervisor = setup_synthetic(tmp_path)
    target = tmp_path / 'business-launched.txt'
    supervisor.business_gate_check = None
    with patch('server_supervisor.LinuxScope', SyntheticScope):
        with pytest.raises(SupervisionUnavailable):
            supervisor.launch(job, [str(target)], timeout_seconds=20)
    assert not target.exists()
    assert ledger.status(job['job_id'])['state'] == 'unknown'


def test_checker_changed_between_identity_check_and_call_cannot_bypass_fence(tmp_path):
    ledger, fence, job, original = setup_synthetic(tmp_path)
    target = tmp_path / 'business-launched.txt'
    fence.close(job['identity']['task_id'], job['generation'])

    class ChangingCheckerSupervisor(ServerSupervisor):
        def __getattribute__(self, name):
            values = object.__getattribute__(self, '__dict__')
            if name == 'business_gate_check' and values.get('_simulate_change'):
                reads = values.get('_checker_reads', 0)
                values['_checker_reads'] = reads + 1
                return check_business_gate if reads == 0 else lambda *_: True
            return super().__getattribute__(name)

    supervisor = object.__new__(ChangingCheckerSupervisor)
    supervisor.__dict__.update(original.__dict__)
    supervisor._simulate_change = True
    with patch('server_supervisor.LinuxScope', SyntheticScope):
        with pytest.raises(SupervisionUnavailable):
            supervisor.launch(job, [str(target)], timeout_seconds=20)
    assert supervisor._checker_reads == 1
    assert not target.exists()
    assert ledger.status(job['job_id'])['state'] == 'unknown'


def test_false_gate_check_cannot_authorize_business_launch(tmp_path, monkeypatch):
    import task_generation_fence as fence_module

    ledger, _, job, supervisor = setup_synthetic(tmp_path)
    target = tmp_path / 'business-launched.txt'
    deny = lambda *_: False
    monkeypatch.setattr(fence_module, 'check_business_gate', deny)
    supervisor.business_gate_check = deny
    with patch('server_supervisor.LinuxScope', SyntheticScope):
        with pytest.raises(SupervisionUnavailable):
            supervisor.launch(job, [str(target)], timeout_seconds=20)
    assert not target.exists()
    assert ledger.status(job['job_id'])['state'] == 'unknown'
