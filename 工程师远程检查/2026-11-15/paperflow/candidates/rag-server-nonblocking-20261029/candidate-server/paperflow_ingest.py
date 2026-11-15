"""Candidate single-chunk orchestration; no live route is registered here.

Callers must first enforce RAGFlow access and document-mutation protection.
The callbacks must reuse the native vector, chunk-building and checked ES code.
"""
import copy
import hashlib
import json
import math

import xxhash


async def process_single_async(executor, **kwargs):
    """Run the candidate write off the event loop with worker-local DB cleanup."""
    db = kwargs["db"]

    def run_in_worker():
        opened_here = db.is_closed()
        try:
            return process_single(**kwargs)
        finally:
            if opened_here and not db.is_closed():
                db.close()

    return await executor.run(run_in_worker)


def process_single(*, db, operation_model, document_model, document_service, coordinator,
                   kb_id, doc_id, req, vectorize, build_chunk, index_chunk):
    if not isinstance(req, dict) or not isinstance(req.get("content"), str) or not req["content"].strip():
        raise ValueError("non-empty chunk content is required")
    content = req["content"]
    chunk_id = xxhash.xxh64((content + doc_id).encode("utf8")).hexdigest()
    content_hash = hashlib.sha256(content.encode("utf8")).hexdigest()
    payload = json.dumps(req, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf8")
    payload_hash = hashlib.sha256(payload).hexdigest()
    reservation = coordinator.reserve_once(
        db, operation_model, document_model, kb_id=kb_id, doc_id=doc_id, chunk_id=chunk_id,
        content_sha256=content_hash, payload_sha256=payload_hash)
    op_id = reservation["operation_id"]
    if not coordinator.claim_once(db, operation_model, op_id):
        status = coordinator.operation_status(db, operation_model, kb_id, doc_id, chunk_id)
        if status["state"] == "storage_confirmed":
            coordinator.complete_once(db, operation_model, document_service, op_id)
            return coordinator.operation_status(db, operation_model, kb_id, doc_id, chunk_id)
        return status

    try:
        # Keep the request that was hashed immutable across callbacks.
        frozen_req = json.loads(payload)
        vector, token_num = vectorize(copy.deepcopy(frozen_req))
        if type(token_num) is not int or token_num < 0:
            raise RuntimeError("prepared model token count is invalid")
        coordinator.record_prepared(db, operation_model, op_id, token_num=token_num)
        if hasattr(vector, "tolist"):
            vector = vector.tolist()
        if (not isinstance(vector, list) or not vector
                or any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) for v in vector)
                ):
            raise RuntimeError("prepared vector or token count is invalid")
        chunk = build_chunk(copy.deepcopy(frozen_req), chunk_id, list(vector))
        if (not isinstance(chunk, dict) or "_id" in chunk or chunk.get("id") != chunk_id
                or chunk.get("doc_id") != doc_id or chunk.get("kb_id") != kb_id
                or chunk.get("content_with_weight") != content
                or chunk.get(f"q_{len(vector)}_vec") != vector):
            raise RuntimeError("prepared chunk identity changed")
        result = index_chunk(chunk)
        if (not isinstance(result, list) or len(result) != 1 or not isinstance(result[0], dict)
                or result[0].get("id") != chunk_id or result[0].get("state") != "indexed"
                or result[0].get("status") not in (200, 201)):
            raise RuntimeError("index outcome is not a matching confirmed write")
    except BaseException:
        try:
            coordinator.record_unknown(db, operation_model, op_id)
        except Exception:
            # A failed status update must not mask the original failure; the
            # already claimed row remains non-replayable.
            pass
        raise

    # If either database step fails, leave running/storage_confirmed intact.
    # Retrying this entry point will only read status, not repeat model or ES.
    coordinator.record_indexed(db, operation_model, op_id, token_num=token_num, duration=0)
    coordinator.complete_once(db, operation_model, document_service, op_id)
    return coordinator.operation_status(db, operation_model, kb_id, doc_id, chunk_id)
