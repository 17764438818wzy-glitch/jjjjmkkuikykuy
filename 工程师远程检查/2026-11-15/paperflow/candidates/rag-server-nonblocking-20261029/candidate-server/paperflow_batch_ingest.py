"""Isolated batch coordinator; not registered in any live RAGFlow route.

Only one document is admitted per batch. All members share one durable model
token total; individual token counts cannot be inferred. Uncertain or partial
outcomes require explicit reconciliation and never trigger an automatic retry.
The caller is responsible for the still-unimplemented authorization and
shared document/permission mutation gate.
"""
import copy
import hashlib
import json
import math

import xxhash


def _hash(data):
    return hashlib.sha256(data).hexdigest()


def _payload(req):
    return json.dumps(req, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False).encode("utf8")


def _prepare(requests, kb_id, doc_id, coordinator):
    if (not all(isinstance(v, str) and v for v in (kb_id, doc_id))
            or not isinstance(requests, list) or not 1 <= len(requests) <= 16):
        raise ValueError("one document and a batch of 1 to 16 chunks are required")
    result = []
    for req in requests:
        if not isinstance(req, dict) or not isinstance(req.get("content"), str) or not req["content"].strip():
            raise ValueError("each chunk needs non-empty content")
        payload = _payload(req)
        frozen = json.loads(payload)
        content = frozen["content"]
        cid = xxhash.xxh64((content + doc_id).encode("utf8")).hexdigest()
        result.append({"req": frozen, "chunk_id": cid,
                       "operation_id": coordinator.chunk_operation_id(kb_id, doc_id, cid),
                       "content_sha256": _hash(content.encode("utf8")),
                       "payload_sha256": _hash(payload)})
    if len({row["chunk_id"] for row in result}) != len(result):
        raise ValueError("duplicate chunk identity in batch")
    identities = sorted((row["operation_id"], row["content_sha256"], row["payload_sha256"]) for row in result)
    batch_id = _hash(_payload(["paperflow-chunk-batch/1", kb_id, doc_id, identities]))
    return batch_id, result


def _bind(db, *models):
    if any(model._meta.database is not db for model in models):
        raise RuntimeError("batch database binding differs from document database")
    if db.in_transaction():
        raise RuntimeError("batch operation cannot run inside an outer transaction")


def _reserve(db, batch_model, operation_model, document_model, *, kb_id, doc_id, batch_id, entries):
    _bind(db, batch_model, operation_model, document_model)
    with db.atomic():
        (document_model.select().where((document_model.id == doc_id) &
                                       (document_model.kb_id == kb_id)).for_update().get())
        # The document lock serializes reservations, and plain reads avoid
        # taking a gap lock for not-yet-existing chunk or batch rows.
        existing = batch_model.get_or_none(batch_model.id == batch_id)
        rows = list(operation_model.select().where((operation_model.kb_id == kb_id) &
                    (operation_model.doc_id == doc_id) &
                    operation_model.chunk_id.in_([entry["chunk_id"] for entry in entries])))
        if existing:
            by_id = {row.chunk_id: row for row in rows}
            if (existing.kb_id != kb_id or existing.doc_id != doc_id or existing.chunk_num != len(entries)
                    or len(by_id) != len(entries)):
                raise RuntimeError("existing batch identity conflicts with the original operation")
            for entry in entries:
                row = by_id.get(entry["chunk_id"])
                if (row is None or row.batch_id != batch_id or row.id != entry["operation_id"]
                        or row.content_sha256 != entry["content_sha256"]
                        or row.payload_sha256 != entry["payload_sha256"]):
                    raise RuntimeError("existing chunk identity conflicts with the original batch")
            return False, existing.state
        if rows:
            raise RuntimeError("chunk identity overlaps an existing operation")
        batch_model.create(id=batch_id, kb_id=kb_id, doc_id=doc_id, state="running",
                           token_num=None, chunk_num=len(entries), duration=0)
        for entry in entries:
            operation_model.create(id=entry["operation_id"], kb_id=kb_id, doc_id=doc_id,
                chunk_id=entry["chunk_id"], content_sha256=entry["content_sha256"],
                payload_sha256=entry["payload_sha256"], batch_id=batch_id, state="batch_running",
                token_num=None, chunk_num=1, duration=0, storage_status=None, storage_error_type=None)
        return True, "running"


def _unknown(db, batch_model, operation_model, batch_id):
    _bind(db, batch_model, operation_model)
    with db.atomic():
        batch = batch_model.select().where(batch_model.id == batch_id).for_update().get()
        if batch.state != "running":
            return
        rows = list(operation_model.select().where(operation_model.batch_id == batch_id).order_by(operation_model.id))
        if len(rows) != batch.chunk_num or any(row.state != "batch_running" for row in rows):
            raise RuntimeError("batch members changed before recording uncertainty")
        for row in rows:
            changed = (operation_model.update(state="batch_unknown").where((operation_model.id == row.id) &
                       (operation_model.state == "batch_running")).execute())
            if changed != 1:
                raise RuntimeError("batch member changed before recording uncertainty")
        (batch_model.update(state="unknown").where(batch_model.id == batch_id).execute())


def _record_prepared(db, batch_model, batch_id, token_count):
    """Persist known aggregate model usage before any potentially uncertain ES send."""
    _bind(db, batch_model)
    if type(token_count) is not int or token_count < 0:
        raise RuntimeError("batch token count is invalid")
    with db.atomic():
        batch = batch_model.select().where(batch_model.id == batch_id).for_update().get()
        if batch.state != "running" or (batch.token_num is not None and batch.token_num != token_count):
            raise RuntimeError("batch model usage changed")
        batch.token_num = token_count
        batch.save(only=[batch_model.token_num])


def _record_results(db, batch_model, operation_model, batch_id, entries, outcomes, token_count):
    _bind(db, batch_model, operation_model)
    if type(token_count) is not int or token_count < 0 or not isinstance(outcomes, list):
        raise RuntimeError("batch result token count or item list is invalid")
    expected = {entry["chunk_id"] for entry in entries}
    by_id = {}
    for item in outcomes:
        if not isinstance(item, dict):
            raise RuntimeError("batch result item is invalid")
        cid, state, status = item.get("id"), item.get("state"), item.get("status")
        error = item.get("error_type")
        if (cid not in expected or cid in by_id or type(status) is not int or
                not ((state == "indexed" and status in (200, 201) and error is None) or
                     (state == "rejected" and status >= 400 and isinstance(error, str) and len(error) <= 128))):
            raise RuntimeError("batch result identity or status is invalid")
        by_id[cid] = item
    if set(by_id) != expected:
        raise RuntimeError("batch result is incomplete")
    with db.atomic():
        batch = batch_model.select().where(batch_model.id == batch_id).for_update().get()
        if batch.state != "running" or batch.chunk_num != len(entries) or batch.token_num != token_count:
            raise RuntimeError("batch state changed before its results were recorded")
        # The batch-row lock serializes all transitions of its members. Reading
        # by batch_id with FOR UPDATE can lock a broad secondary-index range
        # and deadlock unrelated batches, so lock each member by primary key
        # only when updating it below.
        rows = list(operation_model.select().where(operation_model.batch_id == batch_id)
                    .order_by(operation_model.id))
        if len(rows) != len(entries) or any(row.state != "batch_running" or row.chunk_id not in by_id for row in rows):
            raise RuntimeError("batch member state changed before result recording")
        for row in rows:
            item = by_id[row.chunk_id]
            row.state = "batch_indexed" if item["state"] == "indexed" else "batch_rejected"
            row.storage_status = item["status"]
            row.storage_error_type = item.get("error_type")
            row.save(only=[operation_model.state, operation_model.storage_status, operation_model.storage_error_type])
        batch.token_num = token_count
        batch.state = "storage_confirmed" if all(item["state"] == "indexed" for item in outcomes) else "partial"
        batch.save(only=[batch_model.token_num, batch_model.state])


def _complete(db, batch_model, operation_model, document_service, batch_id):
    _bind(db, batch_model, operation_model, document_service.model)
    document_service._assert_chunk_counter_database(db)
    hint = batch_model.get_by_id(batch_id)
    with db.atomic():
        (document_service.model.select().where((document_service.model.id == hint.doc_id) &
                                              (document_service.model.kb_id == hint.kb_id)).for_update().get())
        batch = batch_model.select().where(batch_model.id == batch_id).for_update().get()
        if batch.doc_id != hint.doc_id or batch.kb_id != hint.kb_id:
            raise RuntimeError("batch document identity changed")
        if batch.state == "completed":
            return False
        if batch.state != "storage_confirmed" or type(batch.token_num) is not int or batch.token_num < 0:
            raise RuntimeError("batch storage is not fully confirmed")
        rows = list(operation_model.select().where(operation_model.batch_id == batch_id)
                    .order_by(operation_model.id))
        if (len(rows) != batch.chunk_num or any(row.state != "batch_indexed" or row.doc_id != batch.doc_id
                                                or row.kb_id != batch.kb_id for row in rows)):
            raise RuntimeError("batch member results are inconsistent")
        document_service._increment_chunk_num_in_transaction(batch.doc_id, batch.kb_id,
                                                             batch.token_num, batch.chunk_num, batch.duration)
        for row in rows:
            updated = (operation_model.update(state="completed").where((operation_model.id == row.id) &
                       (operation_model.state == "batch_indexed")).execute())
            if updated != 1:
                raise RuntimeError("batch member terminal update is incomplete")
        changed = (batch_model.update(state="completed").where((batch_model.id == batch_id) &
                   (batch_model.state == "storage_confirmed")).execute())
        if changed != 1:
            raise RuntimeError("batch terminal update is incomplete")
        return True


def _status(db, batch_model, operation_model, batch_id):
    _bind(db, batch_model, operation_model)
    with db.atomic():
        batch = batch_model.get_by_id(batch_id)
        rows = list(operation_model.select().where(operation_model.batch_id == batch_id).order_by(operation_model.id))
        if len(rows) != batch.chunk_num:
            raise RuntimeError("batch members are incomplete")
        return {"batch_id": batch_id, "dataset_id": batch.kb_id, "document_id": batch.doc_id,
                "state": batch.state, "counters_committed": batch.state == "completed",
                "token_count": batch.token_num,
                "items": [{"chunk_id": row.chunk_id, "operation_id": row.id, "state": row.state,
                           "status": row.storage_status, "error_type": row.storage_error_type} for row in rows]}


async def process_batch_async(executor, **kwargs):
    db = kwargs["db"]

    def run_in_worker():
        opened_here = db.is_closed()
        try:
            return process_batch(**kwargs)
        finally:
            if opened_here and not db.is_closed():
                db.close()

    return await executor.run(run_in_worker)


def process_batch(*, db, operation_model, batch_model, document_model, document_service, coordinator,
                  kb_id, doc_id, requests, vectorize, build_chunk, index_batch):
    batch_id, entries = _prepare(requests, kb_id, doc_id, coordinator)
    created, state = _reserve(db, batch_model, operation_model, document_model, kb_id=kb_id,
                              doc_id=doc_id, batch_id=batch_id, entries=entries)
    if not created:
        if state == "storage_confirmed":
            _complete(db, batch_model, operation_model, document_service, batch_id)
        return _status(db, batch_model, operation_model, batch_id)
    try:
        prepared = vectorize(copy.deepcopy([entry["req"] for entry in entries]))
        if not isinstance(prepared, dict):
            raise RuntimeError("batch vector result is incomplete")
        token_count = prepared.get("token_count")
        _record_prepared(db, batch_model, batch_id, token_count)
        if (not isinstance(prepared, dict) or not isinstance(prepared.get("items"), list)
                or len(prepared["items"]) != len(entries)):
            raise RuntimeError("batch vector result is incomplete")
        chunks = []
        for index, entry in enumerate(entries):
            item = prepared["items"][index]
            if not isinstance(item, dict) or item.get("index") != index:
                raise RuntimeError("batch vector result order changed")
            vector = item.get("vector")
            if hasattr(vector, "tolist"):
                vector = vector.tolist()
            if (not isinstance(vector, list) or not vector or
                    any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) for v in vector)):
                raise RuntimeError("batch vector result is invalid")
            chunk = build_chunk(copy.deepcopy(entry["req"]), entry["chunk_id"], list(vector))
            if (not isinstance(chunk, dict) or "_id" in chunk or chunk.get("id") != entry["chunk_id"]
                    or chunk.get("doc_id") != doc_id or chunk.get("kb_id") != kb_id
                    or chunk.get("content_with_weight") != entry["req"]["content"]
                    or chunk.get(f"q_{len(vector)}_vec") != vector):
                raise RuntimeError("batch chunk identity changed")
            chunks.append(chunk)
        outcomes = index_batch(copy.deepcopy(chunks))
        _record_results(db, batch_model, operation_model, batch_id, entries, outcomes, token_count)
    except BaseException:
        try:
            _unknown(db, batch_model, operation_model, batch_id)
        except Exception:
            pass
        raise
    status = _status(db, batch_model, operation_model, batch_id)
    if status["state"] == "storage_confirmed":
        _complete(db, batch_model, operation_model, document_service, batch_id)
    return _status(db, batch_model, operation_model, batch_id)
