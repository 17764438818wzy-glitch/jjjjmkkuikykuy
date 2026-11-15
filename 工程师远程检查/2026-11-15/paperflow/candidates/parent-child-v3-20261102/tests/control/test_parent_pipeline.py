"""Real first-step scripts immediately create native children in an isolated server."""

import asyncio
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Barrier, Event, Thread

from prefect import flow
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import ArtifactFilter, FlowRunFilter
from prefect.context import FlowRunContext
from prefect.testing.utilities import prefect_test_harness


FIRST_STEP = Path(__file__).resolve().parents[3] / "first-step-control-20261029"


def test_groups_start_together_and_dispatch_before_the_batch_finishes(tmp_path, monkeypatch):
    from paperflow_v3.control.admission import Admission, begin_control_session
    from paperflow_v3.control import parent as parent_module
    from paperflow_v3.control.receipts import read_receipts

    library = tmp_path / "library"
    (tmp_path / "work").mkdir()
    for name in ("first", "second"):
        group = library / name
        group.mkdir(parents=True)
        (group / "paper.txt").write_text("题目：合成论文\n摘要：合成文字\n关键词：测试", encoding="utf8")
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            calls.append(payload)
            criteria = payload["questions"]["decision"]["criteria"]
            choice = next(key for key in criteria if key != "none")
            raw = json.dumps({"answers": {"decision": {"choice": choice, "confidence": 1,
                "probabilities": {key: 1 if key == choice else 0 for key in criteria}}}}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    original = parent_module.classify_and_register
    original_dispatch = parent_module.schedule_registered_paper
    observed = []
    dispatched = []
    both_started, first_dispatched = Barrier(2), Event()

    def observe(**kwargs):
        observed.append(len(dispatched))
        both_started.wait(timeout=15)
        if kwargs["group_path"].name == "second":
            assert first_dispatched.wait(timeout=30)
        return original(**kwargs)

    async def record_dispatch(*args):
        child_id = await original_dispatch(*args)
        dispatched.append(child_id)
        first_dispatched.set()
        return child_id

    monkeypatch.setattr(parent_module, "classify_and_register", observe)
    monkeypatch.setattr(parent_module, "schedule_registered_paper", record_dispatch)

    @flow(persist_result=False)
    def paper(task_id: str, group_id: str, registered: dict):
        return group_id

    async def exercise(deployment_id):
        await begin_control_session()

        @flow(persist_result=False)
        async def parent():
            task_id = FlowRunContext.get().flow_run.id
            gate = Admission(task_id)
            await gate.start()
            assert await asyncio.to_thread(gate.running_sync) is True
            await parent_module.classify_and_dispatch(
                task_id=task_id, source_paths=[str(library)], first_step_root=FIRST_STEP,
                work_root=tmp_path / "work", history_roots=(tmp_path / "work",),
                legacy_classification_root=tmp_path / "legacy",
                nimble_endpoint=f"http://127.0.0.1:{server.server_port}/v1/systemone",
                nimble_lock_path=tmp_path / "nimble.lock", child_deployment_id=deployment_id,
                admission=gate,
            )
            records = await read_receipts(task_id)
            return task_id, records

        task_id, records = await parent()
        async with get_client() as client:
            children = await client.read_flow_runs(flow_run_filter=FlowRunFilter(
                parent_flow_run_id={"any_": [task_id]}, tags={"all_": ["paperflow-v3-paper"]}))
            artifacts = await client.read_artifacts(artifact_filter=ArtifactFilter(
                flow_run_id={"any_": [task_id]}, type={"any_": ["paperflow-v3-receipt"]}), limit=200)
            for child in children:
                tracking = await client.read_task_run(child.parent_task_run_id)
                assert tracking.flow_run_id == task_id
        assert observed == [0, 0]
        assert len(children) == len(calls) == 2
        assert sum(row["kind"] == "registration" for row in records) == 2
        assert any(row["kind"] == "classification_complete" for row in records)
        starts = [row for row in artifacts if row.data["kind"] == "classification_started"]
        assert {row.data["folder_id"] for row in starts} == {child.parameters["group_id"] for child in children}
        assert len(starts) == 2
        for start in starts:
            group_id = start.data["folder_id"]
            assert start.flow_run_id == task_id and start.data["task_id"] == str(task_id)
            assert start.data["data"] == {"group_id": group_id}
            outcomes = [row for row in artifacts
                        if row.data["kind"] == "classification" and row.data["folder_id"] == group_id]
            assert outcomes and all(start.created <= row.created for row in outcomes)

    try:
        with prefect_test_harness(server_startup_timeout=60):
            with get_client(sync_client=True) as client:
                deployment_id = client.create_deployment(client.create_flow(paper), name="isolated-paper")
            asyncio.run(exercise(deployment_id))
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
