"""Exercise the actual candidate route with Quart and isolated external boundaries."""
from __future__ import annotations

import ast
import asyncio
import base64
import binascii
import contextvars
import copy
import datetime
import importlib.util
import logging
import os
import re
from pathlib import Path
import sqlite3
import sys
import threading
import time
import types
from concurrent.futures import ThreadPoolExecutor

import pytest
from pydantic import BaseModel, Field, validator
from quart import Quart, jsonify, request
import xxhash


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / os.getenv("RAG_STAGE4_TEST_SOURCE", "candidate-embedding-only/api/apps/restful_apis/chunk_api.py")


class KnownUnmetRequirement(AssertionError):
    """The exact unsafe condition an experimental candidate must still fix."""


class Vector(list):
    def __mul__(self, factor):
        return Vector(v * factor for v in self)

    __rmul__ = __mul__

    def __add__(self, other):
        return Vector(a + b for a, b in zip(self, other))

    def tolist(self):
        return list(self)


class FixtureState:
    def __init__(self, db_path):
        self.db_path = db_path
        self.gate = threading.Event()
        self.gate.set()
        self.gate_timeout = 1.2
        self.insert_gate = threading.Event()
        self.insert_gate.set()
        self.insert_started = threading.Event()
        self.embedding_slot = threading.Lock()
        self.serialize_embedding = False
        self.embedding_delay = 0.0
        self.insert_delay = 0.0
        self.started = threading.Event()
        self.lock = threading.Lock()
        self.active = 0
        self.peak = 0
        self.seen = {}
        self.model_thread_pairs = []
        self.documents = {}
        self.images = {}
        self.raise_insert = False
        self.report_insert_failure = False
        self.access_allowed = True
        self.statuses = {}
        self.deleted = set()
        with sqlite3.connect(db_path) as db:
            db.execute("CREATE TABLE counters (doc TEXT PRIMARY KEY, chunks INTEGER, tokens INTEGER)")

    def encode(self, texts):
        content = texts[1]
        with self.lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
            self.started.set()
            self.seen[content] = (self.llm_context.current_llm_user(), request.headers.get("X-Trace"))
        try:
            if self.serialize_embedding:
                with self.embedding_slot:
                    time.sleep(self.embedding_delay)
                    self.gate.wait(timeout=self.gate_timeout)
            else:
                self.gate.wait(timeout=self.gate_timeout)
            return [Vector([1.0, 0.0]), Vector([0.0, 1.0])], 2
        finally:
            with self.lock:
                self.active -= 1

    def insert(self, documents, index_name, dataset_id, **kwargs):
        if self.raise_insert:
            raise RuntimeError("isolated injected storage failure")
        if self.report_insert_failure:
            return ["isolated item failure"]
        self.insert_started.set()
        self.insert_gate.wait(timeout=self.gate_timeout)
        time.sleep(self.insert_delay)
        with self.lock:
            for document in documents:
                self.documents[document["id"]] = dict(document)
        return []

    def increment(self, doc_id, kb_id, token_num, chunk_num, duration):
        with sqlite3.connect(self.db_path, timeout=5) as db:
            db.execute(
                "INSERT INTO counters VALUES (?, ?, ?) "
                "ON CONFLICT(doc) DO UPDATE SET chunks=chunks+excluded.chunks, tokens=tokens+excluded.tokens",
                (doc_id, chunk_num, token_num),
            )


class FixtureEmbeddingModel:
    def __init__(self, state):
        self.state = state
        self.created_thread = threading.get_ident()

    def encode(self, texts):
        self.state.model_thread_pairs.append((self.created_thread, threading.get_ident()))
        return self.state.encode(texts)


@pytest.fixture
def harness(tmp_path, monkeypatch):
    state = FixtureState(tmp_path / "counter.sqlite")
    spec = importlib.util.spec_from_file_location("test_llm_context", ROOT / "baseline/common/llm_request_context.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    state.llm_context = module

    tokenizer = types.SimpleNamespace(tokenize=lambda value: value, fine_grained_tokenize=lambda value: value)
    nlp = types.ModuleType("rag.nlp")
    nlp.rag_tokenizer = tokenizer
    nlp.search = types.SimpleNamespace(index_name=lambda tenant: "index-" + tenant)
    monkeypatch.setitem(sys.modules, "rag", types.ModuleType("rag"))
    monkeypatch.setitem(sys.modules, "rag.nlp", nlp)

    def query(**params):
        if params["id"] in state.deleted:
            return []
        return [types.SimpleNamespace(id=params["id"], kb_id=params["kb_id"],
                                      name="Paper " + params["id"],
                                      status=state.statuses.get(params["id"], "0" if params["id"] == "disabled" else "1"))]

    def error(*args, **kwargs):
        return jsonify({"code": 102, "message": kwargs.get("message", args[0] if args else "error")})

    def store_image(dataset, chunk, image):
        state.images[(dataset, chunk)] = image

    namespace = {
        "asyncio": asyncio, "contextvars": contextvars,
        "ThreadPoolExecutor": ThreadPoolExecutor, "SimpleNamespace": types.SimpleNamespace,
        "base64": base64, "binascii": binascii, "datetime": datetime, "logging": logging,
        "BaseModel": BaseModel, "Field": Field, "validator": validator, "xxhash": xxhash,
        "KnowledgebaseService": types.SimpleNamespace(accessible=lambda kb_id, user_id: user_id != "denied" and state.access_allowed),
        "_get_dataset_tenant_id": lambda dataset_id: "owner",
        "DocumentService": types.SimpleNamespace(query=query, get_embd_id=lambda document: "embedding",
                                                   increment_chunk_num=state.increment),
        "get_request_json": lambda: request.get_json(), "is_content_empty": lambda content: not content,
        "get_error_data_result": error, "get_result": lambda **kwargs: jsonify({"code": 0, **kwargs}),
        "_store_chunk_image_or_error": None, "store_chunk_image": store_image,
        "resolve_model_config": lambda *args: {},
        "LLMType": types.SimpleNamespace(EMBEDDING=types.SimpleNamespace(value="embedding")),
        "TenantLLMService": types.SimpleNamespace(model_instance=lambda config: FixtureEmbeddingModel(state)),
        "settings": types.SimpleNamespace(docStoreConn=state, DOC_ENGINE="elasticsearch"),
        "set_llm_request_context": module.set_llm_request_context,
        "reset_llm_request_context": module.reset_llm_request_context,
        "normalize_llm_user_id": module.normalize_llm_user_id,
        "validate_tag_features": lambda value: value,
    }
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    selected = []
    names = {"Chunk", "_encode_with_request_user", "_decode_chunk_image_base64",
             "_store_chunk_image_or_error", "add_chunk"}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            if node.name in names or node.name.startswith("_chunk_insert_"):
                node.decorator_list = [] if node.name == "add_chunk" else node.decorator_list
                selected.append(node)
        elif isinstance(node, ast.Assign):
            if any(isinstance(target, ast.Name) and target.id.startswith("_CHUNK_INSERT_") for target in node.targets):
                selected.append(node)
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(SOURCE), "exec"), namespace)

    app = Quart(__name__)
    app.testing = True

    @app.post("/datasets/<dataset_id>/documents/<document_id>/chunks")
    async def add_route(dataset_id, document_id):
        return await namespace["add_chunk"](request.headers.get("X-Tenant", "allowed"), dataset_id, document_id)

    @app.get("/health")
    async def health():
        return jsonify({"status": "ok"})

    @app.post("/fixture-admin/documents/<document_id>/disable")
    async def disable(document_id):
        state.statuses[document_id] = "0"
        with state.lock:
            for row in state.documents.values():
                if row["doc_id"] == document_id:
                    row["available_int"] = 0
        return jsonify({"status": "disabled"})

    @app.delete("/fixture-admin/documents/<document_id>")
    async def delete(document_id):
        state.deleted.add(document_id)
        with sqlite3.connect(state.db_path) as db:
            db.execute("DELETE FROM counters WHERE doc=?", (document_id,))
        with state.lock:
            state.documents = {key: row for key, row in state.documents.items() if row["doc_id"] != document_id}
        return jsonify({"status": "deleted"})

    yield state, app, namespace
    state.gate.set()
    state.insert_gate.set()
    executor = namespace.get("_CHUNK_INSERT_EXECUTOR")
    if executor:
        executor.shutdown(wait=True, cancel_futures=True)


@pytest.mark.asyncio
async def test_read_request_remains_responsive_during_blocked_embedding(harness):
    state, app, _ = harness
    state.gate.clear()
    client = app.test_client()
    begin = time.monotonic()
    write = asyncio.create_task(client.post("/datasets/ds/documents/paper-1/chunks", json={"content": "first text"}))
    try:
        await asyncio.sleep(0.05)
        response = await client.get("/health")
        elapsed = time.monotonic() - begin
        assert state.started.is_set()
        assert (await response.get_json())["status"] == "ok"
        assert elapsed < 0.6, f"event loop blocked by chunk creation for {elapsed:.3f}s"
    finally:
        state.gate.set()
        await write


@pytest.mark.asyncio
@pytest.mark.parametrize("document,available,image,expected_visible", [
    ("paper-1", True, False, 1),
    ("paper-1", False, True, 0),
    ("disabled", True, True, 0),
])
async def test_chunk_contract_keeps_identity_visibility_and_image(harness, document, available, image, expected_visible):
    state, app, _ = harness
    payload = {"content": "first text", "user_id": "user-a", "available": available}
    if image:
        payload["image_base64"] = base64.b64encode(b"fixture image").decode("ascii")
    response = await app.test_client().post(
        f"/datasets/ds/documents/{document}/chunks", json=payload, headers={"X-Trace": "trace-a"}
    )
    body = await response.get_json()
    assert response.status_code == 200
    assert body["code"] == 0
    chunk = body["data"]["chunk"]
    assert chunk["content"] == "first text"
    assert chunk["document_id"] == document
    if document == "paper-1":
        assert chunk["id"] == "c012585a338a12e9"
    indexed = state.documents[chunk["id"]]
    assert indexed["available_int"] == expected_visible
    assert indexed["q_2_vec"] == pytest.approx([0.1, 0.9])
    assert state.seen["first text"] == ("user-a", "trace-a")
    if image:
        assert state.images[("ds", chunk["id"])] == b"fixture image"
        assert chunk["image_id"] == "ds-" + chunk["id"]
    with sqlite3.connect(state.db_path) as db:
        assert db.execute("SELECT chunks,tokens FROM counters WHERE doc=?", (document,)).fetchone() == (1, 2)


@pytest.mark.asyncio
async def test_denied_request_has_no_model_or_storage_side_effects(harness):
    state, app, _ = harness
    response = await app.test_client().post(
        "/datasets/ds/documents/paper-1/chunks", json={"content": "first text"}, headers={"X-Tenant": "denied"}
    )
    body = await response.get_json()
    assert body["code"] == 102
    assert not state.seen and not state.documents
    with sqlite3.connect(state.db_path) as db:
        assert db.execute("SELECT COUNT(*) FROM counters").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_distinct_requests_keep_user_and_request_context(harness):
    state, app, _ = harness
    client = app.test_client()

    async def send(i):
        return await client.post(
            f"/datasets/ds/documents/paper-{i}/chunks",
            json={"content": f"text-{i}", "user_id": f"user-{i}"}, headers={"X-Trace": f"trace-{i}"},
        )

    responses = await asyncio.gather(*(send(i) for i in range(12)))
    bodies = [await response.get_json() for response in responses]
    assert all(body["code"] == 0 for body in bodies)
    assert state.seen == {f"text-{i}": (f"user-{i}", f"trace-{i}") for i in range(12)}
    assert len(state.documents) == 12
    with sqlite3.connect(state.db_path) as db:
        assert db.execute("SELECT SUM(chunks),SUM(tokens) FROM counters").fetchone() == (12, 24)


@pytest.mark.asyncio
async def test_model_client_is_constructed_and_used_in_worker_thread(harness):
    state, app, _ = harness
    main_thread = threading.get_ident()
    response = await app.test_client().post(
        "/datasets/ds/documents/paper-1/chunks", json={"content": "first text"}
    )
    assert (await response.get_json())["code"] == 0
    assert len(state.model_thread_pairs) == 1
    created, used = state.model_thread_pairs[0]
    assert created == used and used != main_thread


@pytest.mark.asyncio
async def test_cancellation_does_not_free_a_running_worker_or_repeat_write(harness):
    state, app, _ = harness
    state.gate.clear()
    state.gate_timeout = 5
    client = app.test_client()

    async def send(i):
        return await client.post(f"/datasets/ds/documents/paper-{i}/chunks", json={"content": f"text-{i}"})

    tasks = [asyncio.create_task(send(i)) for i in range(20)]
    cancelled = []
    try:
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            with state.lock:
                if state.active == 4:
                    started = list(state.seen)
                    break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("four independent simulated writes did not start")
        cancelled = [int(value.split("-")[1]) for value in started]
        for index in cancelled:
            tasks[index].cancel()
        await asyncio.gather(*(tasks[index] for index in cancelled), return_exceptions=True)
        await asyncio.sleep(0.1)
        with state.lock:
            assert state.active == 4
            assert state.peak == 4
        assert (await (await client.get("/health")).get_json())["status"] == "ok"
    finally:
        state.gate.set()
        await asyncio.gather(*tasks, return_exceptions=True)

    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        with state.lock:
            if state.active == 0:
                break
        await asyncio.sleep(0.01)
    assert state.active == 0
    assert state.peak <= 4
    with sqlite3.connect(state.db_path) as db:
        for index in cancelled:
            assert db.execute("SELECT chunks FROM counters WHERE doc=?", (f"paper-{index}",)).fetchone() in (None, (1,))


@pytest.mark.asyncio
async def test_storage_exception_does_not_increment_chunk_count(harness):
    state, app, _ = harness
    state.raise_insert = True
    with pytest.raises(RuntimeError, match="isolated injected storage failure"):
        await app.test_client().post("/datasets/ds/documents/paper-1/chunks", json={"content": "first text"})
    assert not state.documents
    with sqlite3.connect(state.db_path) as db:
        assert db.execute("SELECT COUNT(*) FROM counters").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_storage_reported_failure_is_not_acknowledged_as_success(harness):
    state, app, _ = harness
    state.report_insert_failure = True
    with pytest.raises(RuntimeError, match="index insertion"):
        await app.test_client().post("/datasets/ds/documents/paper-1/chunks", json={"content": "first text"})
    assert not state.documents
    with sqlite3.connect(state.db_path) as db:
        assert db.execute("SELECT COUNT(*) FROM counters").fetchone()[0] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["disabled", "deleted", "revoked"])
async def test_state_changes_during_embedding_do_not_publish(harness, changed):
    state, app, _ = harness
    state.gate.clear()
    client = app.test_client()
    write = asyncio.create_task(client.post(
        "/datasets/ds/documents/paper-1/chunks",
        json={"content": "first text", "image_base64": base64.b64encode(b"fixture image").decode("ascii")},
    ))
    try:
        deadline = time.monotonic() + 1
        while not state.started.is_set() and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        assert state.started.is_set()
        if changed == "disabled":
            state.statuses["paper-1"] = "0"
        elif changed == "deleted":
            state.deleted.add("paper-1")
        else:
            state.access_allowed = False
        state.gate.set()
        response = await write
        assert (await response.get_json())["code"] != 0
        assert not state.documents
        assert not state.images
        with sqlite3.connect(state.db_path) as db:
            assert db.execute("SELECT COUNT(*) FROM counters").fetchone()[0] == 0
    finally:
        state.gate.set()
        if not write.done():
            await write


def make_real_es_insert(failures):
    class FakeTimeout(Exception):
        pass

    class FakeElasticClient:
        def __init__(self):
            self.calls = 0
            self.options_used = []

        def options(self, **kwargs):
            self.options_used.append(kwargs)
            return self

        def bulk(self, **kwargs):
            self.calls += 1
            if self.calls <= failures:
                raise FakeTimeout("simulated uncertain bulk result")
            return {"errors": False, "items": []}

    source = SOURCE.parents[3] / "rag/utils/es_conn.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "ESConnection")
    method = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "insert")
    namespace = {"copy": copy, "re": re, "ATTEMPT_TIME": 2, "ConnectionTimeout": FakeTimeout,
                 "time": types.SimpleNamespace(sleep=lambda _: None)}
    exec(compile(ast.Module(body=[method], type_ignores=[]), str(source), "exec"), namespace)
    client = FakeElasticClient()
    store = types.SimpleNamespace(es=client, logger=logging.getLogger("fixture.es"), _connect=lambda: None)
    store.insert = types.MethodType(namespace["insert"], store)
    return store, client, FakeTimeout


@pytest.mark.asyncio
async def test_real_es_timeout_is_unknown_and_never_replayed(harness):
    state, app, namespace = harness
    store, client, timeout_error = make_real_es_insert(failures=2)
    namespace["settings"].docStoreConn = store
    with pytest.raises(timeout_error, match="uncertain bulk result"):
        await app.test_client().post("/datasets/ds/documents/paper-1/chunks", json={"content": "first text"})
    assert client.calls == 1
    assert client.options_used == [{"max_retries": 0}]
    with sqlite3.connect(state.db_path) as db:
        assert db.execute("SELECT COUNT(*) FROM counters").fetchone()[0] == 0


def test_other_es_insert_callers_keep_existing_retry_default():
    store, client, _ = make_real_es_insert(failures=1)
    assert store.insert([{"id": "existing-behavior"}], "index-owner", "ds") == []
    assert client.calls == 2
    assert client.options_used == []


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["disable", "delete"])
async def test_document_change_during_storage_write_cannot_leave_a_visible_chunk(harness, action):
    state, app, _ = harness
    state.insert_gate.clear()
    client = app.test_client()
    write = asyncio.create_task(client.post(
        "/datasets/ds/documents/paper-1/chunks", json={"content": "first text"}
    ))
    try:
        deadline = time.monotonic() + 1
        while not state.insert_started.is_set() and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        assert state.insert_started.is_set()
        if action == "disable":
            changed = await client.post("/fixture-admin/documents/paper-1/disable")
            assert (await changed.get_json())["status"] == "disabled"
        else:
            changed = await client.delete("/fixture-admin/documents/paper-1")
            assert (await changed.get_json())["status"] == "deleted"
        state.insert_gate.set()
        response = await write
        await response.get_json()
        assert all(row["available_int"] == 0 for row in state.documents.values())
        if action == "delete":
            assert not state.documents
    finally:
        state.insert_gate.set()
        if not write.done():
            await write


@pytest.mark.asyncio
@pytest.mark.xfail("candidate-embedding-only" in str(SOURCE), strict=True,
                   raises=KnownUnmetRequirement,
                   reason="Safe candidate still waits synchronously for ES writes")
async def test_read_request_remains_responsive_during_blocked_storage(harness):
    state, app, _ = harness
    state.insert_gate.clear()
    client = app.test_client()
    begin = time.monotonic()
    write = asyncio.create_task(client.post("/datasets/ds/documents/paper-1/chunks", json={"content": "first text"}))
    try:
        await asyncio.sleep(0.05)
        response = await client.get("/health")
        elapsed = time.monotonic() - begin
        assert state.insert_started.is_set()
        assert (await response.get_json())["status"] == "ok"
        if elapsed >= 0.6:
            raise KnownUnmetRequirement(f"ES write blocked the event loop for {elapsed:.3f}s")
    finally:
        state.insert_gate.set()
        await write


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["disable", "delete"])
@pytest.mark.xfail(strict=True, raises=KnownUnmetRequirement,
                   reason="External document changes are not fenced against an in-flight ES write")
async def test_background_change_during_storage_write_cannot_leave_visible_chunk(harness, action):
    state, app, _ = harness
    state.insert_gate.clear()

    def external_change():
        if not state.insert_started.wait(timeout=3):
            return
        if action == "disable":
            state.statuses["paper-1"] = "0"
        else:
            state.deleted.add("paper-1")
        with state.lock:
            if action == "disable":
                for row in state.documents.values():
                    if row["doc_id"] == "paper-1":
                        row["available_int"] = 0
            else:
                state.documents.clear()
        state.insert_gate.set()

    worker = threading.Thread(target=external_change)
    worker.start()
    try:
        response = await app.test_client().post(
            "/datasets/ds/documents/paper-1/chunks", json={"content": "first text"}
        )
        await response.get_json()
        if action == "delete":
            if state.documents:
                raise KnownUnmetRequirement("Deleted document has a residual ES chunk")
        else:
            if any(row["available_int"] != 0 for row in state.documents.values()):
                raise KnownUnmetRequirement("Disabled document has a visible ES chunk")
    finally:
        state.insert_gate.set()
        worker.join(timeout=3)


@pytest.mark.asyncio
async def test_simulated_20_request_pipeline_records_real_completions(harness):
    state, app, _ = harness
    state.serialize_embedding = True
    state.embedding_delay = 0.10
    state.insert_delay = 0.06
    client = app.test_client()
    start = time.monotonic()

    async def send(i):
        response = await client.post(f"/datasets/ds/documents/paper-{i}/chunks", json={"content": f"text-{i}"})
        return await response.get_json()

    bodies = await asyncio.gather(*(send(i) for i in range(20)))
    elapsed = time.monotonic() - start
    assert all(body["code"] == 0 for body in bodies)
    assert len(state.documents) == 20
    with sqlite3.connect(state.db_path) as db:
        assert db.execute("SELECT SUM(chunks) FROM counters").fetchone()[0] == 20
    print(f"SIMULATED_PIPELINE completed=20 embedding_slots=1 embedding_delay=0.10s "
          f"es_delay=0.06s elapsed={elapsed:.3f}s requests_per_sec={20 / elapsed:.3f}")
