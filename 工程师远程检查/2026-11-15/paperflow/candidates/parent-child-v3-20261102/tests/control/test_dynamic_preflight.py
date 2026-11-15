"""The start preflight accepts dynamically assigned LLM slots only."""
import asyncio
from contextlib import asynccontextmanager
import json
from types import SimpleNamespace

from paperflow_v3.control import preflight


def test_preflight_accepts_dynamic_llm_limits_but_checks_other_limits(tmp_path, monkeypatch):
    configured = {"local": 4, "package": 2, "body_llm": 50, "attachment_llm": 20,
                  "total_llm": 10, "rag": 3, "nas": 2}
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"limits": configured, "execution_enabled": False}), encoding="utf-8")
    observed = {**configured, "body_llm": 100, "attachment_llm": 100, "total_llm": 100}

    class Client:
        async def read_deployment(self, _):
            raise ValueError("unavailable in fake")

        async def read_artifacts(self, **_):
            raise ValueError("unavailable in fake")

        async def read_global_concurrency_limit_by_name(self, name):
            value = observed[name.removeprefix("paperflow-connected-")]
            return SimpleNamespace(active=True, limit=value, slot_decay_per_second=0)

    @asynccontextmanager
    async def fake_client():
        yield Client()

    monkeypatch.setattr(preflight, "get_client", fake_client)

    async def global_limit_check():
        result = await preflight.inspect_dependencies({"deployment_id": "fake", "child_deployment_id": "fake",
                                                       "project_config": str(path), "first_step_root": str(tmp_path)})
        return next(item for item in result["checks"] if item["name"] == "global_limits")["ready"]

    assert asyncio.run(global_limit_check()) is True
    observed["local"] = 5
    assert asyncio.run(global_limit_check()) is False
    observed["local"], observed["total_llm"] = 4, 0
    assert asyncio.run(global_limit_check()) is False
