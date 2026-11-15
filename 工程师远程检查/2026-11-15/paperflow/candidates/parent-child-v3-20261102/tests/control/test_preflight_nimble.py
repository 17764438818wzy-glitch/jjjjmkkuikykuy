"""First-step preflight reports the real read-only probe result."""

import asyncio
from contextlib import contextmanager, ExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import subprocess
import sys
from threading import Thread
import time

import psutil
import pytest


FIRST_STEP = Path(__file__).resolve().parents[3] / "first-step-control-20261029"


class UnavailablePrefect:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        pass

    async def read_deployment(self, *_args):
        raise RuntimeError("isolated Prefect")

    async def read_artifacts(self, **_kwargs):
        raise RuntimeError("isolated Prefect")


@contextmanager
def nimble_stub(models, status=200, port=0):
    paths = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            paths.append(self.path)
            value = {"version": "isolated"} if self.path == "/api/version" else {"models": models}
            body = json.dumps(value).encode("utf8")
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_port, paths
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def run_check(tmp_path, monkeypatch, port):
    from paperflow_v3.control import preflight

    monkeypatch.setattr(preflight, "get_client", lambda: UnavailablePrefect())
    config = tmp_path / "isolated.json"
    config.write_text(json.dumps({"nimble_endpoint": f"http://127.0.0.1:{port}/v1/systemone"}), encoding="utf8")
    result = asyncio.run(preflight.inspect_dependencies({"deployment_id": "none", "child_deployment_id": "none",
        "project_config": str(config), "first_step_root": str(FIRST_STEP)}))
    return next(row for row in result["checks"] if row["name"] == "nimble")


def test_missing_nimble_model_reports_readable_reason(tmp_path, monkeypatch):
    from paperflow_v3.control import preflight
    monkeypatch.setattr(preflight, "_start_nimble", lambda: pytest.fail("model absence is not a startup request"),
                        raising=False)
    with nimble_stub([]) as (port, paths):
        check = run_check(tmp_path, monkeypatch, port)
    assert check["ready"] is False
    assert "模型未注册" in check["detail"] and "NimbleUnavailable" not in check["detail"]
    assert paths == ["/api/version", "/api/tags"]


def test_nimble_connection_refusal_identifies_service_to_check(tmp_path, monkeypatch):
    from paperflow_v3.control import preflight
    monkeypatch.setattr(preflight, "_start_nimble", lambda: pytest.fail("unmanaged address must not be started"))
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    check = run_check(tmp_path, monkeypatch, port)
    assert check["ready"] is False
    assert "连接" in check["detail"] and "启动" in check["detail"]
    assert "NimbleUnavailable" not in check["detail"]


def test_refused_managed_local_service_is_started_then_verified(tmp_path, monkeypatch):
    from paperflow_v3.control import preflight

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    monkeypatch.setattr(preflight, "NIMBLE_LOCAL_PORT", port, raising=False)
    with ExitStack() as stack:
        started = []

        def start():
            _, paths = stack.enter_context(nimble_stub([{"name": "nimble:9b-q4_K_M"}], port=port))
            started.append(paths)

        monkeypatch.setattr(preflight, "_start_nimble", start, raising=False)
        check = run_check(tmp_path, monkeypatch, port)
        assert check["ready"] is True
        assert "已自动启动" in check["detail"] and "复检通过" in check["detail"]
        assert len(started) == 1 and started[0] == ["/api/version", "/api/tags"]


def test_startup_failure_remains_unready_and_explains_reason(tmp_path, monkeypatch):
    from paperflow_v3.control import preflight

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    monkeypatch.setattr(preflight, "NIMBLE_LOCAL_PORT", port, raising=False)

    def failed():
        raise preflight.NimbleStartError("启动脚本测试失败")

    monkeypatch.setattr(preflight, "_start_nimble", failed, raising=False)
    check = run_check(tmp_path, monkeypatch, port)
    assert check["ready"] is False
    assert "启动脚本测试失败" in check["detail"]


def test_started_service_with_missing_model_still_fails_preflight(tmp_path, monkeypatch):
    from paperflow_v3.control import preflight

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    monkeypatch.setattr(preflight, "NIMBLE_LOCAL_PORT", port)
    with ExitStack() as stack:
        started = []

        def start():
            _, paths = stack.enter_context(nimble_stub([], port=port))
            started.append(paths)

        monkeypatch.setattr(preflight, "_start_nimble", start)
        check = run_check(tmp_path, monkeypatch, port)
        assert check["ready"] is False and "模型未注册" in check["detail"]
        assert len(started) == 1 and started[0] == ["/api/version", "/api/tags"]


def test_unhealthy_responding_service_is_not_restarted(tmp_path, monkeypatch):
    from paperflow_v3.control import preflight

    monkeypatch.setattr(preflight, "_start_nimble", lambda: pytest.fail("unexpected restart"))
    with nimble_stub([], status=500) as (port, paths):
        monkeypatch.setattr(preflight, "NIMBLE_LOCAL_PORT", port)
        check = run_check(tmp_path, monkeypatch, port)
    assert check["ready"] is False and "状态接口不可用" in check["detail"]
    assert paths == ["/api/version"]


def test_repair_invokes_existing_local_start_script_without_inference(tmp_path, monkeypatch):
    from paperflow_v3.control import preflight

    (tmp_path / "logs").mkdir()
    monkeypatch.setattr(preflight, "NIMBLE_START_SCRIPT", tmp_path / "Start-Nimble.ps1")
    monkeypatch.setattr(preflight, "NIMBLE_START_LOG_DIR", tmp_path / "logs")
    calls = []
    monkeypatch.setattr(preflight.subprocess, "run", lambda command, **options: calls.append((command, options)))
    preflight._start_nimble()
    command, options = calls[0]
    assert command[0] == "powershell.exe"
    assert str(preflight.NIMBLE_START_SCRIPT) in command and "-Quiet" in command
    assert options["check"] and options["timeout"] == 25
    assert options["stdout"] is options["stderr"] and options["stdout"] != subprocess.PIPE


def test_startup_returns_while_powershell_descendant_is_still_running(tmp_path, monkeypatch):
    from paperflow_v3.control import preflight

    (tmp_path / "logs").mkdir()
    child = tmp_path / "synthetic_child.py"
    child.write_text("import time; time.sleep(7)", encoding="utf8")
    pid_file = tmp_path / "child.pid"
    script = tmp_path / "start.ps1"
    quote = lambda value: str(value).replace("'", "''")
    script.write_text("param([switch]$Quiet)\n$ErrorActionPreference = 'Stop'\n"
        + "$p = Start-Process -FilePath '" + quote(sys.executable) + "' -ArgumentList '\"" + quote(child)
        + "\"' -WindowStyle Hidden -PassThru -RedirectStandardOutput '" + quote(tmp_path / "child.out")
        + "' -RedirectStandardError '" + quote(tmp_path / "child.err") + "'\n"
        + "$p.Id | Set-Content -LiteralPath '" + quote(pid_file) + "' -Encoding ASCII\n", encoding="utf-8-sig")
    monkeypatch.setattr(preflight, "NIMBLE_START_SCRIPT", script)
    monkeypatch.setattr(preflight, "NIMBLE_START_LOG_DIR", tmp_path / "logs")
    start = time.monotonic()
    try:
        preflight._start_nimble()
        elapsed = time.monotonic() - start
        assert elapsed < 4, f"startup waited {elapsed:.2f}s for the unrelated child"
        process = psutil.Process(int(pid_file.read_text(encoding="ascii").strip()))
        assert str(child) in process.cmdline()
    finally:
        if pid_file.exists():
            try:
                process = psutil.Process(int(pid_file.read_text(encoding="ascii").strip()))
                if str(child) in process.cmdline():
                    children = process.children(recursive=True)
                    for descendant in reversed(children):
                        descendant.terminate()
                    process.terminate()
                    process.wait(timeout=5)
                    psutil.wait_procs(children, timeout=5)
            except psutil.NoSuchProcess:
                pass


def test_failed_start_keeps_and_reports_the_script_log(tmp_path, monkeypatch):
    from paperflow_v3.control import preflight

    logs = tmp_path / "logs"
    logs.mkdir()
    script = tmp_path / "start.ps1"
    script.write_text("param([switch]$Quiet)\n[Console]::Error.WriteLine('synthetic-startup-failure')\nexit 7\n",
                      encoding="utf-8-sig")
    monkeypatch.setattr(preflight, "NIMBLE_START_SCRIPT", script)
    monkeypatch.setattr(preflight, "NIMBLE_START_LOG_DIR", logs)
    with pytest.raises(preflight.NimbleStartError, match="synthetic-startup-failure") as error:
        preflight._start_nimble()
    saved = list(logs.glob("nimble-preflight-*.log"))
    assert len(saved) == 1 and "synthetic-startup-failure" in saved[0].read_text(encoding="utf8")
    assert str(saved[0]) in str(error.value)


def test_startup_timeout_reports_unconfirmed_status_and_keeps_log(tmp_path, monkeypatch):
    from paperflow_v3.control import preflight

    logs = tmp_path / "logs"
    monkeypatch.setattr(preflight, "NIMBLE_START_LOG_DIR", logs)

    def timed_out(command, **options):
        options["stdout"].write("synthetic startup pending\n")
        options["stdout"].flush()
        raise subprocess.TimeoutExpired(command, 25)

    monkeypatch.setattr(preflight.subprocess, "run", timed_out)
    with pytest.raises(preflight.NimbleStartError, match="服务状态待核对") as error:
        preflight._start_nimble()
    saved = list(logs.glob("nimble-preflight-*.log"))
    assert len(saved) == 1 and "synthetic startup pending" in saved[0].read_text(encoding="utf8")
    assert str(saved[0]) in str(error.value)


@pytest.mark.parametrize("models,expected", [
    ([{"name": "nimble:9b-q4_K_M"}], True),
    ([], False),
])
def test_nimble_probe_uses_only_read_only_service_and_model_listing(tmp_path, monkeypatch, models, expected):
    with nimble_stub(models) as (port, paths):
        check = run_check(tmp_path, monkeypatch, port)
    assert check["ready"] is expected
    assert not expected or "自动启动" not in check["detail"]
    assert paths == ["/api/version", "/api/tags"]
