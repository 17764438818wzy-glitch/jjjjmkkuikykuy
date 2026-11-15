"""Durable batch behavior with disposable MySQL and deterministic fake services."""
import importlib.util
import os
from pathlib import Path
import threading
from concurrent.futures import ThreadPoolExecutor
import asyncio
from urllib.parse import parse_qs, urlsplit

import peewee
import pytest
from quart import Quart, jsonify

from test_candidate_counter_terminal import counting  # noqa: F401
from test_candidate_batch_vectors import Model, candidate as vector_candidate
from test_candidate_bounded_executor import dispatcher, until
from test_candidate_integrated_async import loopback_es  # noqa: F401
from test_candidate_ingest_flow import candidate as single_candidate
from test_candidate_recovery_terminal import setup_recovery  # noqa: F401
from test_mysql_named_lock import experiment_gate, isolated_mysql  # noqa: F401


ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(os.environ.get("RAG_ISOLATED_MYSQL") != "1",
                                reason="opt-in disposable MySQL test")


def candidate():
    path = ROOT / "candidate-server/paperflow_batch_ingest.py"
    assert path.is_file(), "batch orchestration is not connected"
    spec = importlib.util.spec_from_file_location("candidate_batch_ingest", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def setup(counting):
    db, Document, KB, Operation, service, coordinator = counting

    class Batch(peewee.Model):
        id = peewee.CharField(primary_key=True)
        kb_id = peewee.CharField()
        doc_id = peewee.CharField()
        state = peewee.CharField()
        token_num = peewee.IntegerField(null=True)
        chunk_num = peewee.IntegerField()
        duration = peewee.FloatField(default=0)

        class Meta:
            database = db

    db.create_tables([Batch])
    return db, Document, KB, Operation, Batch, service, coordinator


def args_for(setup, n=8):
    db, Document, _, Operation, Batch, service, coordinator = setup
    model = Model()
    calls = {"bulk": 0}
    requests = [{"content": f"synthetic-{i}", "user_id": "test-user"} for i in range(n)]

    def vectorize(reqs):
        return vector_candidate().encode_batches(model, "test-title", reqs,
            max_chunks=n, max_batch_tokens=10000, max_input_tokens=1000, count_tokens=len,
            normalize_user=lambda user: user, encode_with_user=lambda mdl, texts, _: mdl.encode(texts))

    def build(req, cid, vector):
        return {"id": cid, "doc_id": "doc", "kb_id": "kb", "content_with_weight": req["content"],
                f"q_{len(vector)}_vec": vector}

    def index(chunks):
        calls["bulk"] += 1
        return [{"id": c["id"], "state": "indexed", "status": 201} for c in chunks]

    return dict(db=db, operation_model=Operation, batch_model=Batch, document_model=Document,
                document_service=service, coordinator=coordinator, kb_id="kb", doc_id="doc",
                requests=requests, vectorize=vectorize, build_chunk=build, index_batch=index), model, calls


@pytest.mark.parametrize("size", [8, 16])
def test_batch_commits_each_chunk_and_aggregate_tokens_once(setup, size):
    db, Document, KB, Operation, Batch, _, _ = setup
    args, model, calls = args_for(setup, size)
    first = candidate().process_batch(**args)
    second = candidate().process_batch(**args)
    assert first["state"] == second["state"] == "completed"
    assert first["batch_id"] == second["batch_id"]
    assert len(model.calls) == calls["bulk"] == 1
    token_num = sum(len(text) for text in model.calls[0])
    assert (Document.get_by_id("doc").chunk_num, Document.get_by_id("doc").token_num) == (size, token_num)
    assert (KB.get_by_id("kb").chunk_num, KB.get_by_id("kb").token_num) == (size, token_num)
    assert Batch.get_by_id(first["batch_id"]).token_num == token_num
    assert Operation.select().where(Operation.state == "completed").count() == size
    assert all(op.token_num is None and op.storage_status == 201 for op in Operation.select())


def test_partial_result_is_not_retried_or_counted(setup):
    _, Document, _, Operation, Batch, _, _ = setup
    args, model, calls = args_for(setup)

    def partial(chunks):
        calls["bulk"] += 1
        return [{"id": c["id"], "state": "rejected", "status": 429, "error_type": "overload"}
                if i == 3 else {"id": c["id"], "state": "indexed", "status": 201}
                for i, c in enumerate(chunks)]

    args["index_batch"] = partial
    first = candidate().process_batch(**args)
    second = candidate().process_batch(**args)
    assert first["state"] == second["state"] == "partial"
    assert calls["bulk"] == len(model.calls) == 1
    assert Document.get_by_id("doc").chunk_num == 0
    assert Operation.select().where(Operation.state == "batch_indexed").count() == 7
    rejected = Operation.get(Operation.state == "batch_rejected")
    assert rejected.storage_status == 429 and rejected.storage_error_type == "overload"
    assert Batch.get_by_id(first["batch_id"]).token_num is not None


def test_uncertain_send_stays_unknown_and_never_repeats(setup):
    _, Document, _, Operation, Batch, _, _ = setup
    args, model, calls = args_for(setup)

    def timeout(_):
        calls["bulk"] += 1
        raise TimeoutError("simulated timeout after sending bulk")

    args["index_batch"] = timeout
    with pytest.raises(TimeoutError):
        candidate().process_batch(**args)
    assert candidate().process_batch(**args)["state"] == "unknown"
    assert calls["bulk"] == len(model.calls) == 1
    assert Batch.select().get().token_num == sum(len(text) for text in model.calls[0])
    assert Operation.select().where(Operation.state == "batch_unknown").count() == 8
    assert Document.get_by_id("doc").chunk_num == 0


def test_incomplete_or_wrong_index_ack_is_unknown(setup):
    _, Document, _, _, _, _, _ = setup
    args, model, calls = args_for(setup)
    args["index_batch"] = lambda chunks: [{"id": "wrong", "state": "indexed", "status": 201}]
    with pytest.raises(RuntimeError, match="result"):
        candidate().process_batch(**args)
    assert candidate().process_batch(**args)["state"] == "unknown"
    assert len(model.calls) == 1 and Document.get_by_id("doc").chunk_num == 0


def test_overlap_with_prior_batch_cannot_repeat_a_chunk(setup):
    _, Document, _, _, _, _, _ = setup
    args, model, calls = args_for(setup)
    candidate().process_batch(**args)
    overlap = {**args, "requests": args["requests"][1:] + [{"content": "new", "user_id": "test-user"}]}
    with pytest.raises(RuntimeError, match="identity|overlap"):
        candidate().process_batch(**overlap)
    assert calls["bulk"] == len(model.calls) == 1 and Document.get_by_id("doc").chunk_num == 8


@pytest.mark.parametrize("batch_first", [True, False])
def test_single_and_batch_cannot_duplicate_same_chunk(setup, batch_first):
    db, Document, _, Operation, _, service, coordinator = setup
    args, model, calls = args_for(setup)
    single_calls = {"model": 0, "es": 0}

    def single_vector(_):
        single_calls["model"] += 1
        return [0.1, 0.9], 7

    def single_index(chunk):
        single_calls["es"] += 1
        return [{"id": chunk["id"], "state": "indexed", "status": 201}]

    single = dict(db=db, operation_model=Operation, document_model=Document, document_service=service,
        coordinator=coordinator, kb_id="kb", doc_id="doc", req=args["requests"][0],
        vectorize=single_vector, build_chunk=args["build_chunk"], index_chunk=single_index)
    if batch_first:
        candidate().process_batch(**args)
        assert single_candidate().process_single(**single)["state"] == "completed"
        assert single_calls == {"model": 0, "es": 0}
        assert len(model.calls) == calls["bulk"] == 1
        assert Document.get_by_id("doc").chunk_num == 8
    else:
        single_candidate().process_single(**single)
        with pytest.raises(RuntimeError, match="overlap"):
            candidate().process_batch(**args)
        assert single_calls == {"model": 1, "es": 1}
        assert not model.calls and calls["bulk"] == 0
        assert Document.get_by_id("doc").chunk_num == 1


@pytest.mark.parametrize("batch_first", [True, False])
def test_single_and_batch_overlap_while_original_is_running(setup, batch_first):
    db, Document, _, Operation, _, service, coordinator = setup
    args, model, calls = args_for(setup)
    entered, release = threading.Event(), threading.Event()
    single_calls = {"model": 0, "es": 0}

    def single_vector(_):
        single_calls["model"] += 1
        if not batch_first:
            entered.set()
            assert release.wait(5)
        return [0.1, 0.9], 7

    def single_index(chunk):
        single_calls["es"] += 1
        return [{"id": chunk["id"], "state": "indexed", "status": 201}]

    single = dict(db=db, operation_model=Operation, document_model=Document, document_service=service,
        coordinator=coordinator, kb_id="kb", doc_id="doc", req=args["requests"][0],
        vectorize=single_vector, build_chunk=args["build_chunk"], index_chunk=single_index)
    batch_vector = args["vectorize"]

    def held_batch(reqs):
        entered.set()
        assert release.wait(5)
        return batch_vector(reqs)

    args["vectorize"] = held_batch

    def first_write():
        try:
            return (candidate().process_batch(**args) if batch_first
                    else single_candidate().process_single(**single))
        finally:
            db.close()

    with ThreadPoolExecutor(max_workers=1) as pool:
        first = pool.submit(first_write)
        try:
            assert entered.wait(5)
            if batch_first:
                assert single_candidate().process_single(**single)["state"] == "batch_running"
                assert single_calls == {"model": 0, "es": 0}
            else:
                with pytest.raises(RuntimeError, match="overlap"):
                    candidate().process_batch(**args)
                assert not model.calls and calls["bulk"] == 0
        finally:
            release.set()
        assert first.result(timeout=5)["state"] == "completed"
    assert Document.get_by_id("doc").chunk_num == (8 if batch_first else 1)


def test_concurrent_overlapping_batches_are_serialized(setup):
    db, Document, _, _, _, _, _ = setup
    args, model, calls = args_for(setup)
    entered, release = threading.Event(), threading.Event()
    original = args["vectorize"]

    def hold(reqs):
        entered.set()
        assert release.wait(5)
        return original(reqs)

    args["vectorize"] = hold
    overlapping = {**args, "requests": args["requests"][1:] + [{"content": "other", "user_id": "test-user"}]}

    def send(data):
        try:
            return candidate().process_batch(**data)
        finally:
            db.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(send, args)
        try:
            assert entered.wait(5)
            with pytest.raises(RuntimeError, match="overlap"):
                pool.submit(send, overlapping).result(timeout=5)
        finally:
            release.set()
        assert first.result(timeout=5)["state"] == "completed"
    assert calls["bulk"] == len(model.calls) == 1 and Document.get_by_id("doc").chunk_num == 8


def test_counter_failure_resumes_without_second_model_or_bulk(setup, monkeypatch):
    _, Document, _, Operation, Batch, service, _ = setup
    args, model, calls = args_for(setup)
    original = service._increment_chunk_num_in_transaction
    monkeypatch.setattr(service, "_increment_chunk_num_in_transaction",
                        lambda *a: (_ for _ in ()).throw(RuntimeError("counter failure")))
    with pytest.raises(RuntimeError, match="counter failure"):
        candidate().process_batch(**args)
    assert Batch.select().get().state == "storage_confirmed"
    assert Document.get_by_id("doc").chunk_num == 0
    assert Operation.select().where(Operation.state == "batch_indexed").count() == 8
    monkeypatch.setattr(service, "_increment_chunk_num_in_transaction", original)
    assert candidate().process_batch(**args)["state"] == "completed"
    assert calls["bulk"] == len(model.calls) == 1 and Document.get_by_id("doc").chunk_num == 8


def test_terminal_write_failure_rolls_back_all_members_and_counters(setup, monkeypatch):
    _, Document, KB, Operation, Batch, _, _ = setup
    args, model, calls = args_for(setup)
    original = Batch.update
    def fail_terminal(cls, *args, **kwargs):
        if kwargs.get("state") == "completed":
            raise RuntimeError("terminal failure")
        return original(*args, **kwargs)
    monkeypatch.setattr(Batch, "update", classmethod(fail_terminal))
    with pytest.raises(RuntimeError, match="terminal failure"):
        candidate().process_batch(**args)
    assert Batch.select().get().state == "storage_confirmed"
    assert Operation.select().where(Operation.state == "batch_indexed").count() == 8
    assert Document.get_by_id("doc").chunk_num == KB.get_by_id("kb").chunk_num == 0
    monkeypatch.setattr(Batch, "update", original)
    assert candidate().process_batch(**args)["state"] == "completed"
    assert calls["bulk"] == len(model.calls) == 1
    assert Document.get_by_id("doc").chunk_num == 8


def test_member_update_failure_mid_commit_rolls_back_everything(setup, monkeypatch):
    _, Document, KB, Operation, Batch, _, _ = setup
    args, model, calls = args_for(setup)
    original = Operation.update
    updates = 0

    def fail_mid_commit(cls, *args, **kwargs):
        nonlocal updates
        if kwargs.get("state") == "completed":
            updates += 1
            if updates == 4:
                raise RuntimeError("member terminal failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(Operation, "update", classmethod(fail_mid_commit))
    with pytest.raises(RuntimeError, match="member terminal failure"):
        candidate().process_batch(**args)
    assert updates == 4
    assert Batch.select().get().state == "storage_confirmed"
    assert Operation.select().where(Operation.state == "batch_indexed").count() == 8
    assert Document.get_by_id("doc").chunk_num == KB.get_by_id("kb").chunk_num == 0
    monkeypatch.setattr(Operation, "update", original)
    assert candidate().process_batch(**args)["state"] == "completed"
    assert calls["bulk"] == len(model.calls) == 1 and Document.get_by_id("doc").chunk_num == 8


def test_real_batch_member_status_passes_candidate_client_terminal_check(setup, setup_recovery):
    db, _, _, Operation, _, _, coordinator = setup
    _, _, recovery, _ = setup_recovery
    args, _, _ = args_for(setup)

    class StatusClient:
        def __init__(self, chunk_id):
            self.chunk_id = chunk_id

        def request(self, method, endpoint):
            assert method == "GET" and endpoint.endswith(f"/chunks/{self.chunk_id}/operation")
            return coordinator.operation_status(db, Operation, "kb", "doc", self.chunk_id)

    candidate().process_batch(**args)
    for row in Operation.select():
        recovery._require_terminal_write(StatusClient(row.chunk_id), "kb", "doc", row.chunk_id, row.content_sha256)


def test_simultaneous_duplicates_only_one_can_run_model(setup):
    db, Document, _, _, _, _, _ = setup
    args, model, calls = args_for(setup)
    entered, release = threading.Event(), threading.Event()
    original = args["vectorize"]

    def hold(reqs):
        entered.set()
        assert release.wait(5)
        return original(reqs)

    args["vectorize"] = hold

    def send():
        try:
            return candidate().process_batch(**args)
        finally:
            db.close()

    with ThreadPoolExecutor(max_workers=4) as pool:
        first = pool.submit(send)
        try:
            assert entered.wait(5)
            others = [pool.submit(send) for _ in range(3)]
            assert all(f.result(timeout=5)["state"] == "running" for f in others)
        finally:
            release.set()
        assert first.result(timeout=5)["state"] == "completed"
    assert calls["bulk"] == len(model.calls) == 1 and Document.get_by_id("doc").chunk_num == 8


def test_duplicate_chunks_are_rejected_before_model_or_storage(setup):
    args, model, calls = args_for(setup)
    args["requests"] = [args["requests"][0], args["requests"][0]]
    with pytest.raises(ValueError, match="duplicate"):
        candidate().process_batch(**args)
    assert not model.calls and calls["bulk"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("workers,size", [(2, 8), (4, 16)])
async def test_batched_quart_mysql_and_real_es_client_through_loopback(setup, loopback_es, workers, size):
    db, Document, KB, Operation, Batch, service, coordinator = setup
    handler, checked_insert = loopback_es
    runner = dispatcher(workers, waiting=0)
    model = Model()
    app = Quart(__name__)
    total = 8
    for i in range(total):
        Document.create(id=f"paper-{i}", kb_id="kb")

    @app.post("/papers/<doc>")
    async def add(doc):
        requests = [{"content": f"{doc} synthetic chunk {i}", "user_id": "test-user"} for i in range(size)]

        def vectorize(reqs):
            return vector_candidate().encode_batches(model, "test-title", reqs,
                max_chunks=size, max_batch_tokens=10000, max_input_tokens=1000, count_tokens=len,
                normalize_user=lambda user: user, encode_with_user=lambda mdl, texts, _: mdl.encode(texts))

        def build(req, cid, vector):
            return {"id": cid, "doc_id": doc, "kb_id": "kb", "content_with_weight": req["content"],
                    f"q_{len(vector)}_vec": vector}

        status = await candidate().process_batch_async(runner, db=db, operation_model=Operation,
            batch_model=Batch, document_model=Document, document_service=service, coordinator=coordinator,
            kb_id="kb", doc_id=doc, requests=requests, vectorize=vectorize, build_chunk=build,
            index_batch=lambda chunks: checked_insert(chunks, "test-index", "kb"))
        return jsonify(status)

    @app.get("/health")
    async def health():
        return jsonify(status="ok")

    client = app.test_client()
    jobs = [asyncio.create_task(client.post(f"/papers/paper-{i}")) for i in range(total)]
    try:
        await until(lambda: handler.active == workers, timeout=5)
        await until(lambda: runner.snapshot()["waiting_to_admit"] == total - workers, timeout=5)
        assert (await (await asyncio.wait_for(client.get("/health"), timeout=0.5)).get_json())["status"] == "ok"
        assert handler.peak == workers
    finally:
        handler.gate.set()
        responses = await asyncio.gather(*jobs, return_exceptions=True)
        await runner.aclose()
    assert not any(isinstance(row, BaseException) for row in responses)
    payloads = [await row.get_json() for row in responses]
    assert all(row["state"] == "completed" and row["counters_committed"] for row in payloads)
    assert handler.requests == len(model.calls) == total
    assert KB.get_by_id("kb").chunk_num == total * size
    assert all(Document.get_by_id(f"paper-{i}").chunk_num == size for i in range(total))
    assert Operation.select().where(Operation.state == "completed").count() == total * size
    assert all(len(entries) == size * 2 and parse_qs(urlsplit(path).query)["refresh"] == ["wait_for"]
               for path, entries in handler.payloads)
