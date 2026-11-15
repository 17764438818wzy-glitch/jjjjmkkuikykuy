"""Candidate API and native flows with synthetic source, model, RAG and NAS.

The real W2 process_paper and W3 deliver_paper entries run. Only external
services and the offline token counter are replaced; every file is in tmp_path.
"""

import asyncio
from contextlib import asynccontextmanager
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
from threading import Event, Thread
from uuid import uuid4

from docx import Document
from fastapi.testclient import TestClient
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
from prefect.client.schemas.filters import FlowRunFilter
from prefect.flow_engine import run_flow
from prefect.settings import PREFECT_API_URL
from prefect.testing.utilities import prefect_test_harness
import pytest


ROOT = Path(__file__).resolve().parents[2]
WORKSPACE = ROOT.parents[1]
FIRST_STEP = WORKSPACE / "candidates/first-step-control-20261029"
sys.path[:0] = [str(WORKSPACE / "prefect-paperflow"), str(WORKSPACE / "prefect-paperflow/tests"),
                str(WORKSPACE / "candidates/postprocess-prefect-20261031/tests")]


@pytest.mark.parametrize("model_failure", [False, True, "postprocessing", "preprocessing_reviewed"])
def test_api_to_real_paper_entries_and_server_cleanup(tmp_path, monkeypatch, model_failure):
    from paperflow_prefect import legacy
    from paperflow_prefect.settings import Settings
    from paperflow_v3.control import flows, preflight
    from paperflow_v3.control.api import create_app
    from paperflow_v3.control.receipts import read_receipts, save_receipt

    original = Settings.load()
    legacy.bind(original)
    from fixture_profile import _candidate
    from test_attachment_publish import FakeStore
    from test_body_adapter import FixtureCounter
    import preextract_pipeline
    from paperflow_v3.stages import llm_request, pipeline, postprocessing

    llm_slot_entries = []
    if model_failure == "postprocessing":
        original_process_body = postprocessing.process_body
        original_llm_slot = llm_request.admitted_slot
        postprocessing_calls = []

        @asynccontextmanager
        async def observe_llm_slot(admission, limits):
            async with original_llm_slot(admission, limits):
                llm_slot_entries.append(tuple(limits))
                yield

        def fail_once(*args, **kwargs):
            postprocessing_calls.append(True)
            if len(postprocessing_calls) == 1:
                raise RuntimeError("synthetic offline counter failure")
            return original_process_body(*args, **kwargs)

        monkeypatch.setattr(postprocessing, "process_body", fail_once)
        monkeypatch.setattr(llm_request, "admitted_slot", observe_llm_slot)

    preprocessing_entered, preprocessing_continue = Event(), Event()
    model_entered, model_continue = Event(), Event()
    preprocessing_attempts = []
    prepare = pipeline.prepare_registered_folder

    def observed_prepare(*args, **kwargs):
        preprocessing_entered.set()
        if not preprocessing_continue.wait(30):
            raise TimeoutError("synthetic preprocessing barrier timed out")
        preprocessing_attempts.append(True)
        if model_failure == "preprocessing_reviewed" and len(preprocessing_attempts) == 1:
            raise RuntimeError("synthetic local conversion outcome unconfirmed")
        return prepare(*args, **kwargs)

    monkeypatch.setattr(pipeline, "prepare_registered_folder", observed_prepare)

    class Counter(FixtureCounter):
        def close(self):
            pass

    class LocalRag(FakeStore):
        def request(self, method, endpoint, body=None):
            result = super().request(method, endpoint, body)
            return {"id": result["id"]} if method == "PATCH" and isinstance(result, dict) else result

    model_calls, classify_calls = [], []
    data_root = tmp_path / "data"
    data_root.mkdir()
    library = tmp_path / "library"
    group = library / "one"
    group.mkdir(parents=True)
    source = group / "synthetic.docx"
    document = Document()
    document.add_heading("1 排程研究", 1)
    document.add_paragraph("第1项材料为三个作业的到达时刻和持续时间。先按两种规则排序，再记录等待时间，比较平均值。")
    document.save(source)
    nas = tmp_path / "nas"
    nas.mkdir()
    for name in ("classification", "receipts"):
        (tmp_path / name).mkdir()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            raw = json.dumps({"data": [{"id": "synthetic"}]}).encode("utf8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if "questions" in payload:
                classify_calls.append(payload)
                criteria = payload["questions"]["decision"]["criteria"]
                choice = next(key for key in criteria if key != "none")
                answer = {"answers": {"decision": {"choice": choice, "confidence": 1,
                    "probabilities": {key: 1 if key == choice else 0 for key in criteria}}}}
            else:
                model_calls.append(payload)
                if model_failure is True and len(model_calls) == 1:
                    raw = b'{"error":"synthetic temporary failure"}'
                    self.send_response(503)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                    return
                model_entered.set()
                if not model_continue.wait(30):
                    raise TimeoutError("synthetic model barrier timed out")
                readings = list(data_root.rglob("*-reading.json"))
                assert len(readings) == 1
                bundle = json.loads(readings[0].read_text(encoding="utf8"))
                candidate = _candidate({"template": str(original.path("template_path"))}, bundle)
                answer = {"choices": [{"message": {"content": json.dumps(candidate, ensure_ascii=False)},
                                       "finish_reason": "stop"}]}
            raw = json.dumps(answer, ensure_ascii=False).encode("utf8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    endpoint = f"http://127.0.0.1:{server.server_port}"
    connection = {"model": "synthetic", "base_url": endpoint + "/v1", "api_key": "synthetic",
                  "supports_images": True, "context_window": 1000000, "max_tokens": 8192,
                  "max_output_tokens": 128000, "timeout": 30}
    model = tmp_path / "model.json"
    model.write_text(json.dumps({"text_model": "synthetic", "base_url": connection["base_url"],
        "max_tokens": 8192, "preextract_limits": {key: value for key, value in connection.items()
                                                   if key != "api_key"}}), encoding="utf8")
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"schema": "specialty-routing/1.0", "majors": [{
        "code": "080901", "name": "计算机科学与技术", "discipline_code": "08", "discipline": "工学",
        "group_code": "0809", "group": "计算机类", "paper_dataset_id": "synthetic-paper",
        "image_dataset_id": "synthetic-images"}]}, ensure_ascii=False), encoding="utf8")
    rag, connection_roots = LocalRag(), []

    def model_connection(settings):
        connection_roots.append(settings.path("data_root"))
        return dict(connection)

    monkeypatch.setattr(legacy, "connection", model_connection)
    monkeypatch.setattr(legacy, "rag_client", lambda _settings: rag)
    monkeypatch.setattr(preextract_pipeline, "offline_counter", lambda *_args: Counter())

    async def ready(_settings):
        return {"ready": True, "checks": [{"name": "synthetic", "ready": True, "detail": "isolated"}]}

    monkeypatch.setattr(preflight, "inspect_dependencies", ready)
    try:
        with prefect_test_harness(server_startup_timeout=60):
            config = {**original.values, "execution_enabled": True, "rag_backend": "ragflow", "library_root": str(library),
                "data_root": str(data_root), "classification_root": str(tmp_path / "classification"),
                "receipt_root": str(tmp_path / "receipts"), "nas_destination": str(nas),
                "model_config": str(model), "catalog_path": str(catalog), "history_roots": [],
                "nimble_endpoint": endpoint + "/v1/systemone", "prefect_api_url": PREFECT_API_URL.value()}
            config_file = tmp_path / "project.json"
            config_file.write_text(json.dumps(config, ensure_ascii=False), encoding="utf8")
            monkeypatch.setenv("PAPERFLOW_V3_PROJECT_CONFIG", str(config_file))
            monkeypatch.setenv("PAPERFLOW_V3_FIRST_STEP_ROOT", str(FIRST_STEP))
            with get_client(sync_client=True) as client:
                parent_deployment = client.create_deployment(client.create_flow(flows.parent_order), name="parent")
                child_deployment = client.create_deployment(client.create_flow(flows.paper_order), name="paper")
                for name in {name for names in flows.resource_limits().values() for name in names}:
                    client.create_global_concurrency_limit(GlobalConcurrencyLimitCreate(name=name, limit=3, active=True))
            monkeypatch.setenv("PAPERFLOW_V3_CHILD_DEPLOYMENT_ID", str(child_deployment))

            from test_api import capacity_adapters
            app = create_app({"deployment_id": parent_deployment}, **capacity_adapters())
            with TestClient(app) as client:
                created = client.post("/api/v1/prefect/tasks", json={"request_id": str(uuid4()),
                    "task_name": "合成工单", "source_paths": [str(group)]})
                assert created.status_code == 201
                task_id = created.json()["task_id"]
                assert client.post(f"/api/v1/prefect/tasks/{task_id}/start", json={"request_id": str(uuid4()), "intent_seq": 1}).status_code == 200

                async def execute():
                    async with get_client() as prefect:
                        parent_run = await prefect.read_flow_run(task_id)
                    parent = asyncio.create_task(run_flow(flows.parent_order, flow_run=parent_run))
                    child_run = None
                    for _ in range(300):
                        if parent.done():
                            await parent
                            raise AssertionError("parent ended before creating a child")
                        async with get_client() as prefect:
                            children = await prefect.read_flow_runs(flow_run_filter=FlowRunFilter(
                                parent_flow_run_id={"any_": [task_id]}, tags={"all_": ["paperflow-v3-paper"]}))
                        if children:
                            child_run = children[0]
                            break
                        await asyncio.sleep(.1)
                    assert child_run is not None
                    first_child = asyncio.create_task(run_flow(flows.paper_order, flow_run=child_run))
                    assert await asyncio.to_thread(preprocessing_entered.wait, 25)
                    during_preprocessing = client.get("/api/v1/prefect/folder-tickets", params={"task_id": task_id}).json()
                    assert during_preprocessing["folders"][0]["stage"] == "preprocessing_in_progress"
                    preprocessing_continue.set()
                    if model_failure == "preprocessing_reviewed":
                        with pytest.raises(Exception):
                            await asyncio.wait_for(first_child, timeout=60)
                        records = await read_receipts(child_run.id)
                        failure = next(row for row in records if row["kind"] == "preprocessing_failed")
                        assert failure["data"]["retryable"] is False
                        before_review = client.get("/api/v1/prefect/folder-tickets", params={"task_id": task_id}).json()
                        assert before_review["folders"][0]["retryable"] is False
                        registered = child_run.parameters["registered"]
                        paper = registered["paper"]
                        source_hashes = [item["sha256"] for item in (paper["body"], *paper["attachments"])]
                        review_id = str(uuid4())
                        await save_receipt(task_id=task_id, folder_id=registered["group_id"],
                            child_flow_run_id=child_run.id, event_id=review_id, kind="preprocessing_reviewed",
                            data={"identity": {key: failure["data"][key] for key in
                                  ("task_id", "group_id", "paper_id", "receipt_id", "attempt_id")},
                                "failure_event_id": failure["event_id"],
                                "processing_attempt_id": failure["data"]["processing_attempt_id"],
                                "receipt_sha256": registered["receipt_sha256"], "source_sha256": source_hashes,
                                "delivery_sha256": source_hashes,
                                "archive_sha256": [item["sha256"] for item in paper["archive_files"]],
                                "cached_docx_sha256": "3" * 64, "observed_at": "2026-11-02T13:30:00+00:00",
                                "conversion_observation": "no_owned_process_observed",
                                "local_request_history": "none_observed", "downstream_events": "none_observed"})
                        # This review is inserted directly by the fixture, outside
                        # the API path that normally invalidates the query cache.
                        client.portal.call(app.state.query_service.invalidate, task_id)
                        after_review = client.get("/api/v1/prefect/folder-tickets", params={"task_id": task_id}).json()
                        assert after_review["folders"][0]["ticket_id"] == str(child_run.id)
                        # Authorization uses original receipts, independently of
                        # the asynchronous query snapshot's repair coverage.
                        from paperflow_v3.control.retry import retry_decision
                        original_identity = {**failure['data'], 'parent_run_id': task_id,
                                             'task_run_id': str(child_run.id)}
                        assert retry_decision(await read_receipts(child_run.id), identity=original_identity,
                                              registered=registered)['allowed'] is True
                        retry = client.post(f"/api/v1/prefect/tasks/{task_id}/folders/"
                                            f"{child_run.parameters['group_id']}/retry", json={"request_id": str(uuid4())})
                        assert retry.status_code == 200 and retry.json()["ticket_id"] == str(child_run.id)
                        async with get_client() as prefect:
                            child_run = await prefect.read_flow_run(child_run.id)
                        first_child = asyncio.create_task(run_flow(flows.paper_order, flow_run=child_run))
                    if model_failure is True:
                        with pytest.raises(Exception):
                            await asyncio.wait_for(first_child, timeout=60)
                        first_records = await read_receipts(child_run.id)
                        assert any(row["kind"] == "llm_request_failed" for row in first_records)
                        before_retry = client.get("/api/v1/prefect/folder-tickets", params={"task_id": task_id}).json()
                        assert before_retry["folders"][0]["failed_stage"] == "llm"
                        assert before_retry["folders"][0]["retryable"] is True
                        retry = client.post(f"/api/v1/prefect/tasks/{task_id}/folders/"
                                            f"{child_run.parameters['group_id']}/retry", json={"request_id": str(uuid4())})
                        assert retry.status_code == 200 and retry.json()["ticket_id"] == str(child_run.id)
                        async with get_client() as prefect:
                            child_run = await prefect.read_flow_run(child_run.id)
                        first_child = asyncio.create_task(run_flow(flows.paper_order, flow_run=child_run))
                    assert await asyncio.to_thread(model_entered.wait, 30)
                    during_model = client.get("/api/v1/prefect/folder-tickets", params={"task_id": task_id}).json()
                    assert during_model["folders"][0]["stage"] == "llm_in_progress"
                    assert during_model["folders"][0]["llm_body_running"] is True
                    model_continue.set()
                    if model_failure == "postprocessing":
                        with pytest.raises(Exception):
                            await asyncio.wait_for(first_child, timeout=60)
                        first_records = await read_receipts(child_run.id)
                        prior_responses = [row["data"] for row in first_records if row["kind"] == "llm_response_saved"]
                        assert len(prior_responses) == len(model_calls) == 1
                        prior_llm_slots = len(llm_slot_entries)
                        assert prior_llm_slots == 1
                        before_retry = client.get("/api/v1/prefect/folder-tickets", params={"task_id": task_id}).json()
                        assert before_retry["folders"][0]["failed_stage"] == "postprocessing"
                        assert before_retry["folders"][0]["retryable"] is True
                        retry = client.post(f"/api/v1/prefect/tasks/{task_id}/folders/"
                                            f"{child_run.parameters['group_id']}/retry", json={"request_id": str(uuid4())})
                        assert retry.status_code == 200 and retry.json()["ticket_id"] == str(child_run.id)
                        async with get_client() as prefect:
                            child_run = await prefect.read_flow_run(child_run.id)
                        first_child = asyncio.create_task(run_flow(flows.paper_order, flow_run=child_run))
                    await asyncio.wait_for(first_child, timeout=120)
                    await asyncio.wait_for(parent, timeout=15)
                    from paperflow_v3.delivery.progress import local_records, merge_records
                    final_records = merge_records(local_records(child_run.id), await read_receipts(child_run.id))
                    if model_failure == "preprocessing_reviewed":
                        retries = [row["data"] for row in final_records if row["kind"] == "manual_retry"]
                        assert {row["state"] for row in retries} == {"authorized", "running"}
                        assert all(row["review_event_id"] == review_id and
                                   row["failure_event_id"] == failure["event_id"] for row in retries)
                        assert any(row["event_id"] == failure["event_id"] and row["kind"] == "preprocessing_failed"
                                   for row in final_records)
                    if model_failure == "postprocessing":
                        final_responses = [row["data"] for row in final_records if row["kind"] == "llm_response_saved"]
                        assert (len(model_calls), len(llm_slot_entries), len(final_responses)) == (1, prior_llm_slots, 1)
                        assert final_responses == prior_responses
                    return child_run.id, final_records

                child_id, records = asyncio.run(execute())
                client.portal.call(app.state.query_service.invalidate, task_id)
                snapshot = client.get("/api/v1/prefect/folder-tickets", params={"task_id": task_id}).json()
                assert snapshot["task_state"] == "completed" and snapshot["complete"] is True, snapshot
                assert snapshot["folders"][0]["ticket_id"] == str(child_id)
                assert snapshot["folders"][0]["stage"] == "completed"
                assert {row["kind"] for row in records} >= {"preprocessing_complete", "llm_folder_complete",
                    "postprocessing_complete", "stage_work_inventory", "rag-body", "nas", "cleanup"}
                assert any(row["kind"] == "cleanup" and row["data"]["status"] == "complete" for row in records)
                assert classify_calls and len(model_calls) == (2 if model_failure is True else 1) and rag.sent
                assert connection_roots and all(root == data_root for root in connection_roots)
                assert not source.exists() and list(nas.rglob("*.docx"))
    finally:
        preprocessing_continue.set()
        model_continue.set()
        server.shutdown()
        thread.join()
        server.server_close()
