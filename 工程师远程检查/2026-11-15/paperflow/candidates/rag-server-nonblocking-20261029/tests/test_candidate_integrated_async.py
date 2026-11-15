"""Quart + bounded workers + real local MySQL + real ES client over loopback."""
from __future__ import annotations

import asyncio
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading

from elasticsearch import Elasticsearch
import pytest
from quart import Quart, jsonify, request

from test_candidate_bounded_executor import dispatcher, until
from test_candidate_counter_terminal import counting  # noqa: F401
from test_candidate_es_batch import load_method
from test_candidate_ingest_flow import candidate
from test_mysql_named_lock import experiment_gate, isolated_mysql  # noqa: F401


pytestmark = pytest.mark.skipif(__import__("os").environ.get("RAG_ISOLATED_MYSQL") != "1",
                                reason="opt-in disposable MySQL test")


@pytest.fixture
def loopback_es():
    class Handler(BaseHTTPRequestHandler):
        gate = threading.Event()
        lock = threading.Lock()
        requests = 0
        payloads = []
        active = 0
        peak = 0

        def do_PUT(self):
            with self.lock:
                type(self).requests += 1
                type(self).active += 1
                type(self).peak = max(type(self).peak, type(self).active)
            try:
                data = self.rfile.read(int(self.headers["Content-Length"]))
                entries = [json.loads(row) for row in data.splitlines()]
                with self.lock:
                    type(self).payloads.append((self.path, entries))
                assert self.gate.wait(8)
                items = [{"index": {"_index": item["index"]["_index"], "_id": item["index"]["_id"], "status": 201}}
                         for item in entries[::2]]
                body = json.dumps({"errors": False, "items": items}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("X-Elastic-Product", "Elasticsearch")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            finally:
                with self.lock:
                    type(self).active -= 1

        do_POST = do_PUT

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    client = Elasticsearch(f"http://127.0.0.1:{server.server_port}", max_retries=2)
    try:
        yield Handler, load_method(client)[0]
    finally:
        Handler.gate.set()
        client.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def build(doc, req, cid, vector):
    return {"id": cid, "doc_id": doc, "kb_id": "kb", "content_with_weight": req["content"],
            "available_int": 0, f"q_{len(vector)}_vec": vector}


@pytest.mark.asyncio
@pytest.mark.parametrize("workers", [2, 4])
async def test_quart_parallel_pipeline_keeps_health_responsive_and_counts_once(counting, loopback_es, workers):
    db, Document, Knowledgebase, Operation, service, coordinator = counting
    Handler, checked_insert = loopback_es
    module = candidate()
    assert callable(getattr(module, "process_single_async", None)), "async API is not connected to the bounded executor"
    runner = dispatcher(workers, waiting=0)
    model_calls = 0
    model_lock = threading.Lock()
    app = Quart(__name__)

    for i in range(20):
        Document.create(id=f"doc-{i}", kb_id="kb")

    @app.post("/documents/<doc>/chunks")
    async def add(doc):
        req = await request.get_json()

        def vectorize(req):
            nonlocal model_calls
            with model_lock:
                model_calls += 1
            return [0.1, 0.9], 7

        status = await module.process_single_async(runner, db=db, operation_model=Operation, document_model=Document,
            document_service=service, coordinator=coordinator, kb_id="kb", doc_id=doc, req=req,
            vectorize=vectorize, build_chunk=lambda req, cid, vector: build(doc, req, cid, vector),
            index_chunk=lambda chunk: checked_insert([chunk], "test-index", "kb"))
        return jsonify(status)

    @app.get("/health")
    async def health():
        return jsonify(status="ok")

    client = app.test_client()
    tasks = [asyncio.create_task(client.post(f"/documents/doc-{i}/chunks", json={"content": f"content-{i}"}))
             for i in range(20)]
    try:
        await until(lambda: Handler.active == workers, timeout=5)
        await until(lambda: runner.snapshot()["waiting_to_admit"] == 20 - workers, timeout=5)
        health = await asyncio.wait_for(client.get("/health"), timeout=0.5)
        assert (await health.get_json())["status"] == "ok"
        assert Handler.peak == workers
    finally:
        Handler.gate.set()
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        await runner.aclose()
    assert not any(isinstance(row, BaseException) for row in responses)
    payloads = [await row.get_json() for row in responses]
    assert all(row["state"] == "completed" for row in payloads)
    assert Handler.requests == model_calls == 20
    assert Knowledgebase.get_by_id("kb").chunk_num == 20
    assert all(Document.get_by_id(f"doc-{i}").chunk_num == 1 for i in range(20))
    assert runner.snapshot() == {"executing": 0, "queued": 0, "waiting_to_admit": 0}


@pytest.mark.asyncio
async def test_cancelled_quart_request_finishes_original_write_without_resend(counting, loopback_es):
    db, Document, Knowledgebase, Operation, service, coordinator = counting
    Handler, checked_insert = loopback_es
    module = candidate()
    assert callable(getattr(module, "process_single_async", None)), "async API is not connected to the bounded executor"
    runner = dispatcher(2, waiting=0)
    calls = 0

    def vectorize(req):
        nonlocal calls
        calls += 1
        return [0.1, 0.9], 7

    args = dict(db=db, operation_model=Operation, document_model=Document, document_service=service,
                coordinator=coordinator, kb_id="kb", doc_id="doc", req={"content": "original"},
                vectorize=vectorize, build_chunk=lambda req, cid, vector: build("doc", req, cid, vector),
                index_chunk=lambda chunk: checked_insert([chunk], "test-index", "kb"))
    pending = asyncio.create_task(module.process_single_async(runner, **args))
    try:
        await until(lambda: Handler.active == 1, timeout=5)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert runner.snapshot()["executing"] == 1
        Handler.gate.set()
        await until(lambda: runner.snapshot()["executing"] == 0, timeout=5)
        status = await module.process_single_async(runner, **args)
        assert status["state"] == "completed"
    finally:
        Handler.gate.set()
        await runner.aclose()
    assert calls == Handler.requests == 1
    assert Document.get_by_id("doc").chunk_num == Knowledgebase.get_by_id("kb").chunk_num == 1
