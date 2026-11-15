"""Candidate transaction boundary for a durable Paperflow write operation.

Caller must only set storage_confirmed after a matching ES acknowledgement.
Unknown writes remain unknown. This module is not connected to live routes.
"""
import hashlib
import json
import math
import re


def _require_binding(db, *models):
    if any(model._meta.database is not db for model in models):
        raise RuntimeError("operation database binding differs from document database")
    if db.in_transaction():
        raise RuntimeError("cannot publish a terminal result inside an outer transaction")


def chunk_operation_id(kb_id, doc_id, chunk_id):
    data = json.dumps(["paperflow-chunk-operation/1", kb_id, doc_id, chunk_id],
                      ensure_ascii=False, separators=(",", ":")).encode("utf8")
    return hashlib.sha256(data).hexdigest()


def reserve_once(db, operation_model, document_model, *, kb_id, doc_id, chunk_id,
                 content_sha256, payload_sha256):
    """Write an immutable intent before any model or remote index operation."""
    _require_binding(db, operation_model, document_model)
    if (not all(isinstance(value, str) and value for value in (kb_id, doc_id, chunk_id))
            or not re.fullmatch(r"[0-9a-f]{64}", content_sha256 or "")
            or not re.fullmatch(r"[0-9a-f]{64}", payload_sha256 or "")):
        raise ValueError("invalid chunk identity or content digest")
    op_id = chunk_operation_id(kb_id, doc_id, chunk_id)
    with db.atomic():
        (document_model.select().where((document_model.id == doc_id)
                                       & (document_model.kb_id == kb_id)).for_update().get())
        # The document row serializes reservations for the same document.
        # A locking lookup for a missing operation would also lock an index gap
        # and can deadlock unrelated documents trying to insert into that gap.
        existing = (operation_model.select()
                    .where((operation_model.kb_id == kb_id) & (operation_model.doc_id == doc_id)
                           & (operation_model.chunk_id == chunk_id))
                    .get_or_none())
        if existing:
            if (existing.id != op_id or existing.content_sha256 != content_sha256
                    or existing.payload_sha256 != payload_sha256):
                raise RuntimeError("chunk identity conflicts with the original operation")
            return {"operation_id": op_id, "created": False, "state": existing.state}
        operation_model.create(id=op_id, kb_id=kb_id, doc_id=doc_id, chunk_id=chunk_id,
                               content_sha256=content_sha256, payload_sha256=payload_sha256,
                               state="reserved", token_num=None, chunk_num=1, duration=0)
        return {"operation_id": op_id, "created": True, "state": "reserved"}


def claim_once(db, operation_model, operation_id):
    """Only the first claimant may run the model and send the index write."""
    _require_binding(db, operation_model)
    with db.atomic():
        operation = operation_model.select().where(operation_model.id == operation_id).for_update().get()
        if operation.state != "reserved":
            return False
        operation.state = "running"
        operation.save(only=[operation_model.state])
        return True


def record_prepared(db, operation_model, operation_id, *, token_num):
    """Persist the known model usage before sending a potentially uncertain write."""
    _require_binding(db, operation_model)
    if type(token_num) is not int or token_num < 0:
        raise ValueError("invalid model token count")
    with db.atomic():
        operation = operation_model.select().where(operation_model.id == operation_id).for_update().get()
        if operation.state != "running":
            raise RuntimeError("only a running operation can record model usage")
        if operation.token_num is not None and operation.token_num != token_num:
            raise RuntimeError("model token count changed")
        operation.token_num = token_num
        operation.save(only=[operation_model.token_num])


def record_indexed(db, operation_model, operation_id, *, token_num, duration):
    """Record a matching, acknowledged index result; no retry is implied."""
    _require_binding(db, operation_model)
    if (type(token_num) is not int or token_num < 0 or isinstance(duration, bool)
            or not isinstance(duration, (int, float)) or not math.isfinite(duration) or duration < 0):
        raise ValueError("invalid embedding count or duration")
    with db.atomic():
        operation = operation_model.select().where(operation_model.id == operation_id).for_update().get()
        if operation.state == "storage_confirmed":
            if operation.token_num != token_num or operation.duration != duration:
                raise RuntimeError("confirmed operation metadata changed")
            return False
        if operation.state != "running":
            raise RuntimeError("an unknown or completed write cannot be confirmed again")
        if operation.token_num is not None and operation.token_num != token_num:
            raise RuntimeError("confirmed model token count changed")
        operation.token_num, operation.duration, operation.state = token_num, duration, "storage_confirmed"
        operation.save(only=[operation_model.token_num, operation_model.duration, operation_model.state])
        return True


def record_unknown(db, operation_model, operation_id):
    _require_binding(db, operation_model)
    with db.atomic():
        operation = operation_model.select().where(operation_model.id == operation_id).for_update().get()
        if operation.state == "unknown":
            return False
        if operation.state != "running":
            raise RuntimeError("only an in-flight write may become unknown")
        operation.state = "unknown"
        operation.save(only=[operation_model.state])
        return True


def operation_status(db, operation_model, kb_id, doc_id, chunk_id):
    """Caller must authorize the dataset/document before exposing this proof."""
    _require_binding(db, operation_model)
    row = operation_model.get_or_none((operation_model.kb_id == kb_id) & (operation_model.doc_id == doc_id)
                                      & (operation_model.chunk_id == chunk_id))
    if row is None:
        raise LookupError("original operation not found")
    if row.id != chunk_operation_id(kb_id, doc_id, chunk_id):
        raise RuntimeError("stored operation identity does not match the original chunk")
    return {"state": row.state, "dataset_id": row.kb_id, "document_id": row.doc_id,
            "chunk_id": row.chunk_id, "operation_id": row.id, "content_sha256": row.content_sha256,
            "counters_committed": row.state == "completed"}


def complete_once(db, operation_model, document_service, operation_id):
    """Commit both native counters and operation terminal state exactly once."""
    if operation_model._meta.database is not db:
        raise RuntimeError("operation database binding differs from counter database")
    document_service._assert_chunk_counter_database(db)
    if db.in_transaction():
        raise RuntimeError("cannot publish a terminal result inside an outer transaction")
    hint = operation_model.get_by_id(operation_id)
    with db.atomic():
        # Match reserve_once: document row first, then operation row. The
        # identity read before locking is checked again before any updates.
        (document_service.model.select()
         .where((document_service.model.id == hint.doc_id) & (document_service.model.kb_id == hint.kb_id))
         .for_update().get())
        operation = (operation_model.select()
                     .where(operation_model.id == operation_id)
                     .for_update()
                     .get())
        if operation.doc_id != hint.doc_id or operation.kb_id != hint.kb_id:
            raise RuntimeError("operation document identity changed during completion")
        if operation.state == "completed":
            return False
        if operation.state != "storage_confirmed":
            raise RuntimeError("write outcome is not confirmed; counters cannot be committed")
        document_service._increment_chunk_num_in_transaction(
            operation.doc_id, operation.kb_id,
            operation.token_num, operation.chunk_num, operation.duration,
        )
        updated = (operation_model.update(state="completed")
                   .where((operation_model.id == operation_id)
                          & (operation_model.state == "storage_confirmed"))
                   .execute())
        if updated != 1:
            raise RuntimeError("operation state changed during counter commit")
        return True
