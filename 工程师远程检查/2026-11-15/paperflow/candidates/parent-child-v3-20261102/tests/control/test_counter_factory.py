"""The offline counter is created and closed in the processing thread."""

import asyncio
import json
from pathlib import Path
import sys
import threading
from types import ModuleType, SimpleNamespace


def test_process_step_passes_lazy_counter_factory_to_worker(tmp_path, monkeypatch):
    candidate = Path(__file__).resolve().parents[2]
    monkeypatch.syspath_prepend(str(candidate.parents[1] / "prefect-paperflow"))
    from paperflow_prefect import legacy
    from paperflow_v3.control import flows
    from paperflow_v3.stages import pipeline

    runtime = tmp_path / "runtime.json"
    catalog = tmp_path / "catalog.json"
    runtime.write_text(json.dumps({"synthetic": True}), encoding="utf8")
    catalog.write_text("{}", encoding="utf8")
    scoped = SimpleNamespace(path=lambda name: {"release_root": tmp_path, "runtime_config": runtime,
                                                "catalog_path": catalog, "data_root": tmp_path}[name])
    monkeypatch.setattr(flows, "folder_settings", lambda *_args: scoped)
    monkeypatch.setattr(legacy, "bind", lambda *_args: None)
    monkeypatch.setattr(legacy, "connection", lambda *_args: {"model": "synthetic"})
    calls = []
    module = ModuleType("preextract_pipeline")

    class Counter:
        def __init__(self):
            self.owner = threading.get_ident()
            calls.append(("create", self.owner))

        def count_many(self, _texts):
            assert threading.get_ident() == self.owner
            calls.append(("use", self.owner))
            return [1]

        def close(self):
            assert threading.get_ident() == self.owner
            calls.append(("close", self.owner))

    module.offline_counter = lambda runtime_values, core: Counter()
    monkeypatch.setitem(sys.modules, "preextract_pipeline", module)

    async def process_paper(**kwargs):
        assert not calls
        factory = kwargs["counter_factory"]

        def process():
            counter = factory()
            try:
                assert counter.count_many(["synthetic"]) == [1]
            finally:
                counter.close()

        await asyncio.to_thread(process)
        return {"status": "synthetic"}

    monkeypatch.setattr(pipeline, "process_paper", process_paper)
    result = asyncio.run(flows._process_step({"task_id": "task", "group_id": "group"},
        {"paper": {}}, object(), object(), {}, None))
    assert result == {"status": "synthetic"}
    assert [item[0] for item in calls] == ["create", "use", "close"]
    assert len({item[1] for item in calls}) == 1
