"""Integrated isolated single-chunk write, using disposable MySQL and fake ES/model."""
import importlib.util
import hashlib
import json
from pathlib import Path

import pytest
import xxhash

from test_candidate_counter_terminal import counting  # noqa: F401
from test_mysql_named_lock import experiment_gate, isolated_mysql  # noqa: F401


ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(__import__("os").environ.get("RAG_ISOLATED_MYSQL") != "1",
                                reason="opt-in disposable MySQL test")


def candidate():
    path = ROOT / "candidate-server/paperflow_ingest.py"
    assert path.is_file(), "the candidate write path is not connected"
    spec = importlib.util.spec_from_file_location("candidate_ingest", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def build(req, cid, vector):
    return {"id": cid, "doc_id": "doc", "kb_id": "kb", "content_with_weight": req["content"],
            f"q_{len(vector)}_vec": vector}


def reserve_for(coordinator, db, Operation, Document, req):
    cid = xxhash.xxh64((req["content"] + "doc").encode("utf8")).hexdigest()
    digest = hashlib.sha256(req["content"].encode("utf8")).hexdigest()
    payload = json.dumps(req, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf8")
    return coordinator.reserve_once(db, Operation, Document, kb_id="kb", doc_id="doc", chunk_id=cid,
                                    content_sha256=digest, payload_sha256=hashlib.sha256(payload).hexdigest())


def test_original_chunk_retry_reuses_terminal_without_model_or_es(counting):
    db, Document, Knowledgebase, Operation, service, coordinator = counting
    module = candidate()
    calls = {"model": 0, "es": 0}

    def vectorize(req):
        calls["model"] += 1
        return [0.1, 0.9], 7

    def index(chunk):
        calls["es"] += 1
        return [{"id": chunk["id"], "state": "indexed", "status": 201}]

    args = dict(db=db, operation_model=Operation, document_model=Document, document_service=service,
                coordinator=coordinator, kb_id="kb", doc_id="doc", req={"content": "original"},
                vectorize=vectorize, build_chunk=build,
                index_chunk=index)
    first = module.process_single(**args)
    second = module.process_single(**args)
    assert first["state"] == second["state"] == "completed"
    assert first["operation_id"] == second["operation_id"]
    assert calls == {"model": 1, "es": 1}
    assert Document.get_by_id("doc").chunk_num == Knowledgebase.get_by_id("kb").chunk_num == 1
    assert Document.get_by_id("doc").token_num == 7


def test_uncertain_es_write_is_not_replayed_or_counted(counting):
    db, Document, _, Operation, service, coordinator = counting
    module = candidate()
    calls = {"model": 0, "es": 0}

    def vectorize(req):
        calls["model"] += 1
        return [0.1, 0.9], 7

    def index(chunk):
        calls["es"] += 1
        raise TimeoutError("simulated ES timeout after send")

    args = dict(db=db, operation_model=Operation, document_model=Document, document_service=service,
                coordinator=coordinator, kb_id="kb", doc_id="doc", req={"content": "original"},
                vectorize=vectorize, build_chunk=build,
                index_chunk=index)
    with pytest.raises(TimeoutError):
        module.process_single(**args)
    retry = module.process_single(**args)
    assert retry["state"] == "unknown"
    assert calls == {"model": 1, "es": 1}
    assert Document.get_by_id("doc").chunk_num == 0
    assert Operation.select().get().token_num == 7


def test_changed_payload_for_same_chunk_is_rejected_before_extra_work(counting):
    db, Document, _, Operation, service, coordinator = counting
    module = candidate()
    calls = {"model": 0, "es": 0}

    def vectorize(req):
        calls["model"] += 1
        return [0.1, 0.9], 7

    def index(chunk):
        calls["es"] += 1
        return [{"id": chunk["id"], "state": "indexed", "status": 201}]

    args = dict(db=db, operation_model=Operation, document_model=Document, document_service=service,
                coordinator=coordinator, kb_id="kb", doc_id="doc", req={"content": "original", "questions": []},
                vectorize=vectorize, build_chunk=build,
                index_chunk=index)
    module.process_single(**args)
    with pytest.raises(RuntimeError, match="identity"):
        module.process_single(**{**args, "req": {"content": "original", "questions": ["different"]}})
    assert calls == {"model": 1, "es": 1}


def test_reserved_but_never_claimed_can_resume_exact_original_operation(counting):
    db, Document, _, Operation, service, coordinator = counting
    module = candidate()
    req = {"content": "original"}
    op = reserve_for(coordinator, db, Operation, Document, req)["operation_id"]
    calls = {"model": 0, "es": 0}

    def vectorize(req):
        calls["model"] += 1
        return [0.1, 0.9], 7

    def index(chunk):
        calls["es"] += 1
        return [{"id": chunk["id"], "state": "indexed", "status": 201}]

    result = module.process_single(db=db, operation_model=Operation, document_model=Document,
        document_service=service, coordinator=coordinator, kb_id="kb", doc_id="doc", req=req,
        vectorize=vectorize, build_chunk=build, index_chunk=index)
    assert result["operation_id"] == op and result["state"] == "completed"
    assert calls == {"model": 1, "es": 1}


def test_confirmed_index_can_complete_accounting_without_resending(counting):
    db, Document, _, Operation, service, coordinator = counting
    module = candidate()
    req = {"content": "original"}
    op = reserve_for(coordinator, db, Operation, Document, req)["operation_id"]
    assert coordinator.claim_once(db, Operation, op)
    coordinator.record_indexed(db, Operation, op, token_num=7, duration=0)

    def no_call(*args):
        pytest.fail("already confirmed write must not be sent or encoded again")

    result = module.process_single(db=db, operation_model=Operation, document_model=Document,
        document_service=service, coordinator=coordinator, kb_id="kb", doc_id="doc", req=req,
        vectorize=no_call, build_chunk=no_call, index_chunk=no_call)
    assert result["operation_id"] == op and result["state"] == "completed"
    assert Document.get_by_id("doc").chunk_num == 1


@pytest.mark.parametrize("changed", ["id", "doc_id", "kb_id", "content_with_weight", "q_2_vec"])
def test_prepared_chunk_must_match_recorded_identity_before_index(counting, changed):
    db, Document, _, Operation, service, coordinator = counting
    module = candidate()
    sent = []

    def invalid(req, cid, vector):
        chunk = build(req, cid, vector)
        chunk[changed] = [9.0, 9.0] if changed == "q_2_vec" else "changed"
        return chunk

    with pytest.raises(RuntimeError, match="identity"):
        module.process_single(db=db, operation_model=Operation, document_model=Document,
            document_service=service, coordinator=coordinator, kb_id="kb", doc_id="doc",
            req={"content": "original"}, vectorize=lambda req: ([0.1, 0.9], 7), build_chunk=invalid,
            index_chunk=lambda chunk: sent.append(chunk))
    assert sent == [] and Document.get_by_id("doc").chunk_num == 0


def test_callback_cannot_change_frozen_request_before_build(counting):
    db, Document, _, Operation, service, coordinator = counting
    module = candidate()
    sent = []
    original = {"content": "original", "questions": ["q"]}

    def vectorize(req):
        req["content"] = "changed"
        req["questions"].append("injected")
        return [0.1, 0.9], 7

    result = module.process_single(db=db, operation_model=Operation, document_model=Document,
        document_service=service, coordinator=coordinator, kb_id="kb", doc_id="doc", req=original,
        vectorize=vectorize, build_chunk=build,
        index_chunk=lambda chunk: (sent.append(chunk) or [{"id": chunk["id"], "state": "indexed", "status": 201}]))
    assert result["state"] == "completed"
    assert original == {"content": "original", "questions": ["q"]}
    assert sent[0]["content_with_weight"] == "original"


def test_concurrent_duplicate_submission_calls_model_and_es_once(counting):
    from concurrent.futures import ThreadPoolExecutor
    import threading

    db, Document, _, Operation, service, coordinator = counting
    module = candidate()
    entered, release = threading.Event(), threading.Event()
    calls, lock = {"model": 0, "es": 0}, threading.Lock()

    def vectorize(req):
        with lock:
            calls["model"] += 1
        entered.set()
        assert release.wait(5)
        return [0.1, 0.9], 7

    def index(chunk):
        with lock:
            calls["es"] += 1
        return [{"id": chunk["id"], "state": "indexed", "status": 201}]

    def send():
        try:
            return module.process_single(db=db, operation_model=Operation, document_model=Document,
                document_service=service, coordinator=coordinator, kb_id="kb", doc_id="doc", req={"content": "original"},
                vectorize=vectorize, build_chunk=build, index_chunk=index)
        finally:
            db.close()

    with ThreadPoolExecutor(max_workers=4) as pool:
        first = pool.submit(send)
        try:
            assert entered.wait(5)
            others = [pool.submit(send) for _ in range(3)]
            for item in others:
                assert item.result(timeout=5)["state"] == "running"
        finally:
            release.set()
        assert first.result(timeout=5)["state"] == "completed"
    assert calls == {"model": 1, "es": 1}
    assert Document.get_by_id("doc").chunk_num == 1


def test_successful_es_then_terminal_db_failure_does_not_resend(counting, monkeypatch):
    db, Document, _, Operation, service, coordinator = counting
    module = candidate()
    calls = {"model": 0, "es": 0}

    def vectorize(req):
        calls["model"] += 1
        return [0.1, 0.9], 7

    def index(chunk):
        calls["es"] += 1
        return [{"id": chunk["id"], "state": "indexed", "status": 201}]

    args = dict(db=db, operation_model=Operation, document_model=Document, document_service=service,
        coordinator=coordinator, kb_id="kb", doc_id="doc", req={"content": "original"},
        vectorize=vectorize, build_chunk=build, index_chunk=index)
    original = coordinator.record_indexed
    monkeypatch.setattr(coordinator, "record_indexed", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("DB unavailable")))
    with pytest.raises(RuntimeError, match="DB unavailable"):
        module.process_single(**args)
    monkeypatch.setattr(coordinator, "record_indexed", original)
    assert module.process_single(**args)["state"] == "running"
    assert calls == {"model": 1, "es": 1} and Document.get_by_id("doc").chunk_num == 0
