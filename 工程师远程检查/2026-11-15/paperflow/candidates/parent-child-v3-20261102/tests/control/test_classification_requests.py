"""The real first-step judge must not resend a saved or uncertain decision."""

import asyncio
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Event, Thread

from prefect import flow
from prefect.context import FlowRunContext
from prefect.testing.utilities import prefect_test_harness
import pytest


FIRST_STEP = Path(__file__).resolve().parents[3] / "first-step-control-20261029"


def test_paused_parent_allows_inflight_request_to_finish_but_blocks_next(tmp_path):
    from paperflow_v3.control.admission import Admission, begin_control_session
    from paperflow_v3.control.classification import first_step_modules, make_judge
    from paperflow_v3.control.receipts import read_receipts

    requested, release = Event(), Event()
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            calls.append(1)
            requested.set()
            if not release.wait(timeout=20):
                return
            criteria = payload["questions"]["decision"]["criteria"]
            choice = next(key for key in criteria if key != "none")
            raw = json.dumps({"answers": {"decision": {"choice": choice, "confidence": 1,
                "probabilities": {key: 1 if key == choice else 0 for key in criteria}}}}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    endpoint = f"http://127.0.0.1:{server.server_port}/v1/systemone"
    controls = first_step_modules(FIRST_STEP)["control"]
    source = {"file_id": "body", "relative": "paper.txt", "evidence": {"format": "text", "text": "synthetic"}}

    @flow(persist_result=False)
    async def parent():
        task_id = FlowRunContext.get().flow_run.id
        gate = Admission(task_id)
        await gate.start()
        first = make_judge(FIRST_STEP, task_id, "group-a", "snapshot-a", gate, endpoint, tmp_path / "nimble.lock")
        request = asyncio.create_task(asyncio.to_thread(first.choose_body, [source]))
        try:
            assert await asyncio.to_thread(requested.wait, 10)
            await gate.pause()
            release.set()
            assert (await request)["choice"] == "body"
            second = make_judge(FIRST_STEP, task_id, "group-b", "snapshot-b", gate, endpoint, tmp_path / "nimble.lock")
            with pytest.raises(controls.PauseRequested):
                await asyncio.to_thread(second.choose_body, [source])
            records = await read_receipts(task_id)
            assert any(row["kind"] == "classification_model" and row["data"]["state"] == "complete" for row in records)
            assert len(calls) == 1
        finally:
            release.set()
            await asyncio.gather(request, return_exceptions=True)

    try:
        with prefect_test_harness(server_startup_timeout=60):
            asyncio.run(begin_control_session())
            asyncio.run(parent())
    finally:
        release.set()
        server.shutdown()
        thread.join()
        server.server_close()


def test_first_step_reuses_complete_reply_and_stops_on_unknown(tmp_path):
    from paperflow_v3.control.admission import Admission, begin_control_session
    from paperflow_v3.control.classification import first_step_modules, make_judge

    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            calls.append(data)
            if len(calls) == 2:
                self.connection.close()
                return
            criteria = data["questions"]["decision"]["criteria"]
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
    endpoint = f"http://127.0.0.1:{server.server_port}/v1/systemone"
    controls = first_step_modules(FIRST_STEP)["control"]
    source = {"file_id": "body", "relative": "paper.txt", "evidence": {"format": "text", "text": "synthetic"}}

    @flow(persist_result=False)
    def parent():
        task_id = FlowRunContext.get().flow_run.id
        gate = Admission(task_id)
        asyncio.run(begin_control_session())
        asyncio.run(gate.start())

        def judge(snapshot):
            return make_judge(FIRST_STEP, task_id, "group-a", snapshot, gate, endpoint, tmp_path / "nimble.lock")

        assert judge("snapshot-a").choose_body([source])["choice"] == "body"
        assert judge("snapshot-a").choose_body([source])["choice"] == "body"
        assert len(calls) == 1
        with pytest.raises(controls.UnknownRequest):
            judge("snapshot-b").choose_body([source])
        with pytest.raises(controls.UnknownRequest):
            judge("snapshot-b").choose_body([source])
        assert len(calls) == 2
        asyncio.run(gate.pause())
        with pytest.raises(controls.UnknownRequest):
            judge("snapshot-c").choose_body([source])
        assert len(calls) == 2

    try:
        with prefect_test_harness(server_startup_timeout=60):
            parent()
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def test_unknown_for_same_source_blocks_a_second_parent(tmp_path):
    from paperflow_v3.control.admission import Admission, begin_control_session
    from paperflow_v3.control.classification import first_step_modules, make_judge

    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            calls.append(1)
            self.connection.close()

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    endpoint = f"http://127.0.0.1:{server.server_port}/v1/systemone"
    controls = first_step_modules(FIRST_STEP)["control"]
    source = {"file_id": "body", "relative": "paper.txt", "evidence": {"format": "text", "text": "synthetic"}}

    @flow(persist_result=False)
    def parent(snapshot):
        task_id = FlowRunContext.get().flow_run.id
        gate = Admission(task_id)
        asyncio.run(gate.start())
        judge = make_judge(FIRST_STEP, task_id, "same-folder", snapshot, gate, endpoint,
                           tmp_path / "nimble.lock", source_path=tmp_path / "original-group")
        with pytest.raises(controls.UnknownRequest):
            judge.choose_body([source])

    try:
        with prefect_test_harness(server_startup_timeout=60):
            asyncio.run(begin_control_session())
            parent("first")
            parent("different-snapshot")
        assert len(calls) == 1
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
