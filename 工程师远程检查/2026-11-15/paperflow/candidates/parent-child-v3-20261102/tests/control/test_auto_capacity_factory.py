"""The desktop factory wires start-time checks without real network calls."""
import asyncio
import json
import sys
from types import ModuleType, SimpleNamespace

from paperflow_v3.control import api, route_probe, server


def test_desktop_factory_wires_automatic_domestic_account_check(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    candidate = workspace / "candidates" / "parent-child"
    source = candidate / "paperflow_v3" / "control" / "server.py"
    source.parent.mkdir(parents=True)
    monitor = workspace / "candidates" / "first-step-preview-8795" / "workbuddy_monitor.py"
    monitor.parent.mkdir(parents=True)
    monitor.write_text("class Monitor:\n    def test_domestic_accounts(self, model, *, before_batch):\n        before_batch()\n"
                       "        return {'status': 'available', 'usable_uids': ['cn-1', 'cn-2']}\n", encoding="utf-8")
    host = monitor.with_name("host_resources_api.py")
    host.write_text("def read_host_resources(): return {}\n", encoding="utf-8")
    site = workspace / "site"
    site.mkdir()
    model = tmp_path / "model.json"
    model.write_text(json.dumps({"base_url": "http://192.168.1.120:8081/v1", "text_model": "alias"}), encoding="utf-8")
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"model_config": str(model), "settings_root": str(tmp_path)}), encoding="utf-8")
    monkeypatch.setattr(server, "__file__", str(source))
    for name, value in {"PAPERFLOW_V3_PROJECT_CONFIG": str(config), "PAPERFLOW_V3_PROJECT_ROOT": str(tmp_path),
                        "PAPERFLOW_V3_PARENT_DEPLOYMENT_ID": "parent", "PAPERFLOW_V3_CHILD_DEPLOYMENT_ID": "child",
                        "PAPERFLOW_V3_FIRST_STEP_ROOT": str(tmp_path), "PAPERFLOW_V3_SITE_ROOT": str(site)}.items():
        monkeypatch.setenv(name, value)

    package = ModuleType("paperflow_prefect")
    package.__path__ = []
    package.legacy = SimpleNamespace(bind=lambda _: None)
    settings = ModuleType("paperflow_prefect.settings")
    settings.Settings = lambda config: SimpleNamespace(path=lambda key: __import__("pathlib").Path(config[key]))
    auto_config = ModuleType("auto_config")
    auto_config.load_key = lambda _: "fake-project-key"
    monkeypatch.setitem(sys.modules, "paperflow_prefect", package)
    monkeypatch.setitem(sys.modules, "paperflow_prefect.settings", settings)
    monkeypatch.setitem(sys.modules, "auto_config", auto_config)
    monkeypatch.setattr(route_probe, "probe_route", lambda *_: {
        "status": "verified", "model": "cn-model", "per_account_limit": 2,
        "gateway_key_id": "project", "route_fingerprint": "fake", "concurrency_caps": {"user": 140},
    })
    captured = {}

    class App:
        def get(self, path):
            def register(fn):
                captured[path] = fn
                return fn
            return register

        def mount(self, *_args, **_kwargs):
            pass

    def fake_create_app(settings, **kwargs):
        captured.update(settings=settings, **kwargs)
        return App()

    monkeypatch.setattr(api, "create_app", fake_create_app)
    server.create_configured_app()
    config_response = captured["/api-config.json"](SimpleNamespace(base_url="http://127.0.0.1:8796/"))
    assert config_response["automatic_gateway_capacity"] is True
    result = asyncio.run(captured["test_start_accounts"](ensure_current=lambda: asyncio.sleep(0)))
    assert result["successful_count"] == 2
    assert result["account_uids"] == ["cn-1", "cn-2"]
    assert callable(captured["apply_start_capacity"])
