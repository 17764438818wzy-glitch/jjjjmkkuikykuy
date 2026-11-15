"""The paper flow gives its stage adapter the matching first-step root."""

import asyncio
from pathlib import Path
from types import SimpleNamespace


def test_processing_receives_first_step_root_without_changing_the_registered_paper(monkeypatch, tmp_path):
    candidate = Path(__file__).resolve().parents[2]
    monkeypatch.syspath_prepend(str(candidate.parents[1] / "prefect-paperflow"))
    from paperflow_prefect import legacy, records
    from paperflow_v3.control import flows
    from paperflow_v3.stages import pipeline

    class Settings:
        def path(self, _name):
            return tmp_path

    observed = {}

    async def process_paper(**kwargs):
        observed.update(kwargs)
        return {"status": "synthetic"}

    monkeypatch.setattr(flows, "folder_settings", lambda *_args: Settings())
    monkeypatch.setattr(legacy, "bind", lambda *_args: None)
    monkeypatch.setattr(legacy, "connection", lambda *_args: {})
    monkeypatch.setattr(records, "read_json", lambda *_args: {})
    monkeypatch.setattr(pipeline, "process_paper", process_paper)
    monkeypatch.setitem(__import__("sys").modules, "preextract_pipeline", SimpleNamespace(offline_counter=None))

    root = tmp_path / "first-step"
    paper = {"body": {"path": "source.doc"}, "archive_files": [{"path": "source.doc"}]}
    registered = {"paper": paper}
    result = asyncio.run(flows._process_step({"task_id": "task", "group_id": "group"},
        registered, object(), object(), {}, None, first_step_work_root=root))

    assert result == {"status": "synthetic"}
    assert observed["first_step_work_root"] == root
    assert observed["registered"] is registered
    assert paper == {"body": {"path": "source.doc"}, "archive_files": [{"path": "source.doc"}]}
