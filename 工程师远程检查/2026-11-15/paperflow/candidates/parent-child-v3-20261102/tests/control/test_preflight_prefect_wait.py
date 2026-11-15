"""Prefect self-check waits for reads but never admits an unknown result."""

import asyncio
import json
import os
import sys
import time
from collections import Counter
from contextlib import asynccontextmanager
from time import monotonic
from types import SimpleNamespace

import pytest
from prefect.client.schemas.objects import WorkerStatus

from paperflow_v3.control import preflight


LIMITS = {name: 2 for name in ("local", "package", "body_llm", "attachment_llm", "total_llm", "rag", "nas")}


class SyntheticPrefect:
    def __init__(self, behavior):
        self.behavior = behavior
        self.calls = Counter()

    async def read_deployment(self, deployment_id):
        self.calls["deployment"] += 1
        return SimpleNamespace(work_pool_name="synthetic", flow_id=deployment_id)

    async def read_artifacts(self, *, limit):
        self.calls["artifacts"] += 1
        assert limit == 1
        return []

    async def read_workers_for_work_pool(self, pool):
        self.calls["workers"] += 1
        assert pool == "synthetic"
        return [SimpleNamespace(status=WorkerStatus.ONLINE)]

    async def read_global_concurrency_limit_by_name(self, name):
        self.calls[name] += 1
        key = name.removeprefix("paperflow-connected-")
        if key == "rag":
            if self.behavior == "slow_success":
                await asyncio.sleep(3.2)
            elif self.behavior == "timeout":
                await asyncio.sleep(10.5)
            elif self.behavior == "error":
                raise RuntimeError("synthetic Prefect read failure")
        value = LIMITS[key] + (key == "rag" and self.behavior == "mismatch")
        return SimpleNamespace(active=True, limit=value, slot_decay_per_second=0)

    async def read_flow(self, flow_id):
        self.calls["flow"] += 1
        return SimpleNamespace(name=flow_id)


def inspect_without_network(monkeypatch, tmp_path, behavior, *, nas=None):
    config_path = tmp_path / "project.json"
    config = {"execution_enabled": True, "limits": LIMITS}
    if nas is not None:
        config["nas_destination"] = "synthetic-nas"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    client = SyntheticPrefect(behavior)
    seen_timeouts = []
    original_wait_for = asyncio.wait_for

    @asynccontextmanager
    async def get_client():
        yield client

    async def record_wait_for(awaitable, *, timeout):
        seen_timeouts.append(timeout)
        return await original_wait_for(awaitable, timeout=timeout)

    monkeypatch.setattr(preflight, "get_client", get_client)
    monkeypatch.setattr(preflight.asyncio, "wait_for", record_wait_for)
    monkeypatch.setattr(preflight, "_deployment_matches", lambda *_args: True)
    if nas is not None:
        original_path = preflight.Path
        monkeypatch.setattr(preflight, "Path", lambda path: nas if path == "synthetic-nas" else original_path(path))
        monkeypatch.setattr(preflight, "os", SimpleNamespace(access=nas.access, W_OK=os.W_OK))
    # All unrelated dependencies fail closed before any real service can be contacted.
    monkeypatch.setitem(sys.modules, "paperflow_prefect", None)
    started = monotonic()
    result = asyncio.run(preflight.inspect_dependencies({
        "deployment_id": "parent", "child_deployment_id": "paper", "project_config": str(config_path),
    }))
    elapsed = monotonic() - started
    checks = {row["name"]: row for row in result["checks"]}
    return result, checks, seen_timeouts, client.calls, elapsed


def test_prefect_limit_read_can_complete_after_three_seconds(monkeypatch, tmp_path):
    result, checks, timeouts, calls, elapsed = inspect_without_network(monkeypatch, tmp_path, "slow_success")
    assert elapsed >= 3
    assert checks["global_limits"]["ready"] is True
    assert all(checks[name]["ready"] for name in
               ("prefect_deployment", "paper_deployment", "prefect_storage", "executor"))
    assert timeouts == [10] * 6
    assert all(calls["paperflow-connected-" + name] == 1 for name in LIMITS)
    assert result["ready"] is False  # Other dependencies were deliberately not supplied.


@pytest.mark.parametrize(("behavior", "reason"), [
    ("timeout", "TimeoutError"), ("error", "RuntimeError"), ("mismatch", "ValueError"),
])
def test_unconfirmed_prefect_limits_still_block_without_retries(monkeypatch, tmp_path, behavior, reason):
    result, checks, timeouts, calls, _elapsed = inspect_without_network(monkeypatch, tmp_path, behavior)
    assert checks["global_limits"]["ready"] is False
    assert reason in checks["global_limits"]["detail"]
    assert result["ready"] is False
    assert timeouts == [10] * 6
    assert all(calls["paperflow-connected-" + name] == 1 for name in LIMITS)


class SyntheticNAS:
    def __init__(self, *, delay=0, access="writable"):
        self.delay = delay
        self.access_result = access
        self.directory_calls = 0
        self.access_calls = 0

    def is_dir(self):
        self.directory_calls += 1
        time.sleep(self.delay)
        return True

    def access(self, path, mode):
        self.access_calls += 1
        assert path is self and mode == os.W_OK
        if self.access_result == "permission_error":
            raise PermissionError("sensitive synthetic path")
        return self.access_result == "writable"


def test_nas_nine_second_directory_check_can_succeed(monkeypatch, tmp_path):
    nas = SyntheticNAS(delay=9.1)
    _result, checks, timeouts, _calls, elapsed = inspect_without_network(monkeypatch, tmp_path, "ok", nas=nas)
    assert elapsed >= 9
    assert checks["nas"]["ready"] is True
    assert timeouts == [10] * 6 + [15]
    assert nas.directory_calls == nas.access_calls == 1


def test_nas_over_fifteen_seconds_stays_unconfirmed(monkeypatch, tmp_path):
    nas = SyntheticNAS(delay=15.2)
    result, checks, timeouts, _calls, _elapsed = inspect_without_network(monkeypatch, tmp_path, "ok", nas=nas)
    assert checks["nas"] == {"name": "nas", "ready": False,
                             "detail": "NAS目录检查超过15秒，状态未确认"}
    assert result["ready"] is False and timeouts == [10] * 6 + [15]
    assert nas.directory_calls == nas.access_calls == 1


@pytest.mark.parametrize(("access", "expected"), [
    ("permission_error", "PermissionError"), ("unwritable", "NAS 目录不可访问或不可写"),
])
def test_nas_access_error_or_nonwritable_stays_blocked(monkeypatch, tmp_path, access, expected):
    nas = SyntheticNAS(access=access)
    result, checks, _timeouts, _calls, _elapsed = inspect_without_network(monkeypatch, tmp_path, "ok", nas=nas)
    assert checks["nas"]["ready"] is False and expected in checks["nas"]["detail"]
    assert "sensitive synthetic path" not in checks["nas"]["detail"]
    assert result["ready"] is False and nas.directory_calls == nas.access_calls == 1


def test_nas_missing_configuration_reports_type_without_value(monkeypatch, tmp_path):
    result, checks, _timeouts, _calls, _elapsed = inspect_without_network(monkeypatch, tmp_path, "ok")
    assert checks["nas"]["ready"] is False and "KeyError" in checks["nas"]["detail"]
    assert result["ready"] is False


def test_local_os_permission_error_is_sanitized(monkeypatch, tmp_path):
    import stat

    local_file = tmp_path / "permission-probe"
    local_file.write_text("local test", encoding="utf-8")
    local_file.chmod(stat.S_IREAD)

    class LocalPermissionError(SyntheticNAS):
        def access(self, path, mode):
            self.access_calls += 1
            assert path is self and mode == os.W_OK
            descriptor = os.open(str(local_file), os.O_WRONLY)
            os.close(descriptor)
            raise AssertionError("the operating system did not reject the read-only file")

    nas = LocalPermissionError()
    try:
        result, checks, _timeouts, _calls, _elapsed = inspect_without_network(monkeypatch, tmp_path, "ok", nas=nas)
    finally:
        local_file.chmod(stat.S_IREAD | stat.S_IWRITE)
    assert checks["nas"]["ready"] is False and "PermissionError" in checks["nas"]["detail"]
    assert str(local_file) not in checks["nas"]["detail"] and result["ready"] is False
