"""Candidate API and existing frontend on one local origin."""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
from urllib.parse import urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles


def create_configured_app():
    config = Path(os.environ["PAPERFLOW_V3_PROJECT_CONFIG"]).resolve()
    project_root = Path(os.environ.get("PAPERFLOW_V3_PROJECT_ROOT", config.parent)).resolve()
    sys.path.insert(0, str(project_root))
    from .api import create_app
    workspace = Path(__file__).resolve().parents[4]
    from .auto_capacity import AutoCapacity, CapacityError
    from .route_probe import probe_route
    monitor_file = workspace / "candidates/first-step-preview-8795/workbuddy_monitor.py"
    monitor_spec = importlib.util.spec_from_file_location("paperflow_v3_workbuddy_monitor", monitor_file)
    monitor_module = importlib.util.module_from_spec(monitor_spec)
    monitor_spec.loader.exec_module(monitor_module)

    def read_route():
        from paperflow_prefect import legacy
        from paperflow_prefect.settings import Settings
        project = Settings(json.loads(config.read_text(encoding="utf-8-sig")))
        legacy.bind(project)
        from auto_config import load_key
        key = load_key(project.path("settings_root"))
        model = json.loads(project.path("model_config").read_text(encoding="utf-8-sig"))
        base = urlsplit(model["base_url"])
        if (not key or base.scheme != "http" or base.hostname != "192.168.1.120" or base.port != 8081
                or base.path.rstrip("/") != "/v1" or base.query or base.fragment or base.username or base.password):
            raise CapacityError("当前论文模型线路未确认，本次未启动")
        route = probe_route(hashlib.sha256(key.encode("utf-8")).hexdigest(), model["text_model"])
        if route.get("status") != "verified":
            raise CapacityError("论文线路未确认：" + route.get("reason", "状态未知"))
        return route

    def read_llm_limit():
        return json.loads(config.read_text(encoding='utf-8-sig')).get('limits', {}).get('total_llm')

    capacity = AutoCapacity(monitor_module.Monitor(), read_route, read_limit=read_llm_limit,
                            credit_pool=json.loads(config.read_text(encoding='utf-8-sig')).get('workbuddy_credit_pool') is True)
    site = Path(os.environ.get("PAPERFLOW_V3_SITE_ROOT",
                workspace / "candidates/prefect-folder-frontend-20261101")).resolve()
    app = create_app({"recovery_scopes": True, "deployment_id": os.environ["PAPERFLOW_V3_PARENT_DEPLOYMENT_ID"],
                      "child_deployment_id": os.environ["PAPERFLOW_V3_CHILD_DEPLOYMENT_ID"],
                      "project_config": str(config), "first_step_root": os.environ["PAPERFLOW_V3_FIRST_STEP_ROOT"],
                      "dashboard_query_config": str(site / "query-config.json")},
                     test_start_accounts=capacity.test_start_accounts, apply_start_capacity=capacity.apply_start_capacity)

    @app.get("/api-config.json")
    def api_config(request: Request):
        root = str(request.base_url).rstrip("/") + "/api/v1/prefect"
        policy = json.loads(config.read_text(encoding='utf-8-sig'))
        return {"tasks_api_url": root + "/tasks", "folder_tickets_api_url": root + "/folder-tickets",
                "control_api_url": root, "automatic_gateway_capacity": True,
                "workbuddy_credit_pool": policy.get('workbuddy_credit_pool') is True,
                "llm_concurrency_ceiling": policy.get('limits', {}).get('total_llm'),
                "per_domestic_account_concurrency": 2}

    # The existing 8795 page also reads this port for host resources. Reuse
    # its existing sampler when the candidate service occupies that port.
    host_file = workspace / "candidates/first-step-preview-8795/host_resources_api.py"
    spec = importlib.util.spec_from_file_location("paperflow_v3_host_resources", host_file)
    host_resources = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(host_resources)

    @app.get("/api/host-resources")
    def host_sample():
        return JSONResponse(host_resources.read_host_resources(), headers={"Cache-Control": "no-store"})

    app.mount("/", StaticFiles(directory=site, html=True), name="candidate-site")
    return app
