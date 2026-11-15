"""Exercise the candidate commit boundary on an empty local MySQL database."""
from __future__ import annotations

import ast
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import logging
from pathlib import Path
import threading
from unittest.mock import patch

import peewee
import pytest
from playhouse.pool import PooledMySQLDatabase

from test_mysql_named_lock import experiment_gate, isolated_mysql  # noqa: F401


ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(__import__("os").environ.get("RAG_ISOLATED_MYSQL") != "1",
                                reason="opt-in disposable MySQL test")


@pytest.fixture
def counting(isolated_mysql, experiment_gate):
    module_path = ROOT / "candidate-server/paperflow_chunk_commit.py"
    assert module_path.is_file(), "the durable write-and-counter coordinator is not implemented"
    spec = importlib.util.spec_from_file_location("candidate_chunk_commit", module_path)
    coordinator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(coordinator)
    _, schema = experiment_gate
    db = PooledMySQLDatabase(schema, host="127.0.0.1", port=isolated_mysql, user="root",
                             max_connections=12, stale_timeout=60, autoconnect=True)

    class Base(peewee.Model):
        class Meta:
            database = db

    class Document(Base):
        id = peewee.CharField(primary_key=True)
        kb_id = peewee.CharField()
        token_num = peewee.IntegerField(default=0)
        chunk_num = peewee.IntegerField(default=0)
        process_duration = peewee.FloatField(default=0)

    class Knowledgebase(Base):
        id = peewee.CharField(primary_key=True)
        token_num = peewee.IntegerField(default=0)
        chunk_num = peewee.IntegerField(default=0)

    class Operation(Base):
        id = peewee.CharField(primary_key=True)
        doc_id = peewee.CharField()
        kb_id = peewee.CharField()
        chunk_id = peewee.CharField(null=True)
        content_sha256 = peewee.CharField(null=True)
        payload_sha256 = peewee.CharField(null=True)
        token_num = peewee.IntegerField(null=True, default=0)
        chunk_num = peewee.IntegerField(default=1)
        duration = peewee.FloatField(default=0)
        state = peewee.CharField()
        batch_id = peewee.CharField(null=True, index=True)
        storage_status = peewee.IntegerField(null=True)
        storage_error_type = peewee.CharField(null=True)

        class Meta:
            indexes = ((('kb_id', 'doc_id', 'chunk_id'), True),)

    db.create_tables([Document, Knowledgebase, Operation])
    Document.create(id="doc", kb_id="kb")
    Knowledgebase.create(id="kb")
    source = ROOT / "candidate-server/api/db/services/document_service.py"
    tree = ast.parse(source.read_text(encoding="utf8"))
    original = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "DocumentService")
    methods = [node for node in original.body if isinstance(node, ast.FunctionDef)
               and node.name in {"increment_chunk_num", "_increment_chunk_num_in_transaction", "_assert_chunk_counter_database"}]
    service = ast.ClassDef(name="DocumentService", bases=[], keywords=[], body=methods, decorator_list=[])
    ast.fix_missing_locations(service)
    namespace = {"DB": db, "Knowledgebase": Knowledgebase, "logging": logging}
    exec(compile(ast.Module(body=[service], type_ignores=[]), str(source), "exec"), namespace)
    namespace["DocumentService"].model = Document
    try:
        yield db, Document, Knowledgebase, Operation, namespace["DocumentService"], coordinator
    finally:
        db.close_all()


def test_duplicate_successful_operation_is_counted_exactly_once(counting):
    db, Document, Knowledgebase, Operation, service, coordinator = counting
    Operation.create(id="op", doc_id="doc", kb_id="kb", token_num=7, chunk_num=1, state="storage_confirmed")

    def finish(_):
        try:
            return coordinator.complete_once(db, Operation, service, "op")
        finally:
            db.close()

    with ThreadPoolExecutor(max_workers=4) as pool:
        completed = list(pool.map(finish, range(12)))
    assert completed.count(True) == 1 and completed.count(False) == 11
    assert (Document.get_by_id("doc").token_num, Document.get_by_id("doc").chunk_num) == (7, 1)
    assert (Knowledgebase.get_by_id("kb").token_num, Knowledgebase.get_by_id("kb").chunk_num) == (7, 1)
    assert Operation.get_by_id("op").state == "completed"


def test_count_and_operation_roll_back_together_when_kb_is_missing(counting):
    db, Document, _, Operation, service, coordinator = counting
    Document.update(kb_id="missing").where(Document.id == "doc").execute()
    Operation.create(id="op", doc_id="doc", kb_id="missing", token_num=7, chunk_num=1, state="storage_confirmed")
    with pytest.raises(LookupError):
        coordinator.complete_once(db, Operation, service, "op")
    assert (Document.get_by_id("doc").token_num, Document.get_by_id("doc").chunk_num) == (0, 0)
    assert Operation.get_by_id("op").state == "storage_confirmed"


def test_unknown_write_is_not_counted_or_completed(counting):
    db, Document, _, Operation, service, coordinator = counting
    Operation.create(id="op", doc_id="doc", kb_id="kb", token_num=7, chunk_num=1, state="unknown")
    with pytest.raises(RuntimeError):
        coordinator.complete_once(db, Operation, service, "op")
    assert (Document.get_by_id("doc").token_num, Document.get_by_id("doc").chunk_num) == (0, 0)
    assert Operation.get_by_id("op").state == "unknown"


def test_existing_single_counter_api_keeps_its_behavior(counting):
    _, Document, Knowledgebase, _, service, _ = counting
    assert service.increment_chunk_num("doc", "kb", 9, 1, 0) == 1
    assert (Document.get_by_id("doc").token_num, Document.get_by_id("doc").chunk_num) == (9, 1)
    assert (Knowledgebase.get_by_id("kb").token_num, Knowledgebase.get_by_id("kb").chunk_num) == (9, 1)


def test_concurrent_distinct_operations_all_count_once(counting):
    db, Document, Knowledgebase, Operation, service, coordinator = counting
    for i in range(12):
        Operation.create(id=f"op-{i}", doc_id="doc", kb_id="kb", token_num=3, chunk_num=1, state="storage_confirmed")

    def finish(i):
        try:
            return coordinator.complete_once(db, Operation, service, f"op-{i}")
        finally:
            db.close()

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert all(pool.map(finish, range(12)))
    assert (Document.get_by_id("doc").token_num, Document.get_by_id("doc").chunk_num) == (36, 12)
    assert (Knowledgebase.get_by_id("kb").token_num, Knowledgebase.get_by_id("kb").chunk_num) == (36, 12)


def test_operation_and_counter_cannot_use_different_db_bindings(counting, isolated_mysql, experiment_gate):
    db, Document, _, Operation, service, coordinator = counting
    _, schema = experiment_gate
    Operation.create(id="op", doc_id="doc", kb_id="kb", token_num=7, chunk_num=1, state="storage_confirmed")
    other = PooledMySQLDatabase(schema, host="127.0.0.1", port=isolated_mysql, user="root", max_connections=2)

    class OtherOperation(Operation):
        class Meta:
            database = other
            table_name = Operation._meta.table_name

    try:
        with pytest.raises(RuntimeError, match="database binding"):
            coordinator.complete_once(db, OtherOperation, service, "op")
        assert Document.get_by_id("doc").chunk_num == 0
        assert Operation.get_by_id("op").state == "storage_confirmed"
    finally:
        other.close_all()


def test_outer_transaction_cannot_report_terminal_before_commit(counting):
    db, Document, _, Operation, service, coordinator = counting
    Operation.create(id="op", doc_id="doc", kb_id="kb", token_num=7, chunk_num=1, state="storage_confirmed")
    with db.atomic():
        with pytest.raises(RuntimeError, match="outer transaction"):
            coordinator.complete_once(db, Operation, service, "op")
    assert Document.get_by_id("doc").chunk_num == 0
    assert Operation.get_by_id("op").state == "storage_confirmed"


def test_failure_writing_terminal_rolls_back_already_updated_counters(counting):
    db, Document, Knowledgebase, Operation, service, coordinator = counting
    Operation.create(id="op", doc_id="doc", kb_id="kb", token_num=7, chunk_num=1, state="storage_confirmed")
    with patch.object(Operation, "update", side_effect=RuntimeError("failed to write terminal")):
        with pytest.raises(RuntimeError, match="failed to write terminal"):
            coordinator.complete_once(db, Operation, service, "op")
    assert (Document.get_by_id("doc").token_num, Document.get_by_id("doc").chunk_num) == (0, 0)
    assert (Knowledgebase.get_by_id("kb").token_num, Knowledgebase.get_by_id("kb").chunk_num) == (0, 0)
    assert Operation.get_by_id("op").state == "storage_confirmed"


def test_concurrent_same_chunk_reserves_and_claims_only_once(counting):
    db, Document, _, Operation, _, coordinator = counting
    kwargs = dict(kb_id="kb", doc_id="doc", chunk_id="chunk", content_sha256="a" * 64, payload_sha256="b" * 64)

    def reserve(_):
        try:
            return coordinator.reserve_once(db, Operation, Document, **kwargs)
        finally:
            db.close()

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(reserve, range(12)))
    assert sum(result["created"] for result in results) == 1
    assert len({result["operation_id"] for result in results}) == 1
    assert Operation.select().count() == 1
    operation_id = results[0]["operation_id"]

    def claim(_):
        try:
            return coordinator.claim_once(db, Operation, operation_id)
        finally:
            db.close()

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert sum(pool.map(claim, range(12))) == 1
    assert Operation.get_by_id(operation_id).state == "running"


def test_chunk_identity_cannot_be_reused_with_different_payload(counting):
    db, Document, _, Operation, _, coordinator = counting
    args = dict(kb_id="kb", doc_id="doc", chunk_id="chunk", content_sha256="a" * 64, payload_sha256="b" * 64)
    coordinator.reserve_once(db, Operation, Document, **args)
    with pytest.raises(RuntimeError, match="identity"):
        coordinator.reserve_once(db, Operation, Document, **{**args, "payload_sha256": "c" * 64})
    assert Operation.select().count() == 1


def test_terminal_is_exposed_only_after_index_ack_and_count_commit(counting):
    db, Document, _, Operation, service, coordinator = counting
    reserved = coordinator.reserve_once(db, Operation, Document, kb_id="kb", doc_id="doc", chunk_id="chunk",
                                        content_sha256="a" * 64, payload_sha256="b" * 64)
    op = reserved["operation_id"]
    assert coordinator.claim_once(db, Operation, op)
    assert coordinator.operation_status(db, Operation, "kb", "doc", "chunk")["state"] == "running"
    coordinator.record_indexed(db, Operation, op, token_num=7, duration=0)
    before = coordinator.operation_status(db, Operation, "kb", "doc", "chunk")
    assert before["state"] == "storage_confirmed" and before["counters_committed"] is False
    assert coordinator.complete_once(db, Operation, service, op)
    proof = coordinator.operation_status(db, Operation, "kb", "doc", "chunk")
    assert proof == dict(state="completed", dataset_id="kb", document_id="doc", chunk_id="chunk", operation_id=op,
                         content_sha256="a" * 64, counters_committed=True)
    assert Document.get_by_id("doc").chunk_num == 1


def test_unknown_write_cannot_be_claimed_or_marked_success(counting):
    db, Document, _, Operation, _, coordinator = counting
    op = coordinator.reserve_once(db, Operation, Document, kb_id="kb", doc_id="doc", chunk_id="chunk",
                                  content_sha256="a" * 64, payload_sha256="b" * 64)["operation_id"]
    assert coordinator.claim_once(db, Operation, op)
    coordinator.record_unknown(db, Operation, op)
    assert not coordinator.claim_once(db, Operation, op)
    with pytest.raises(RuntimeError):
        coordinator.record_indexed(db, Operation, op, token_num=7, duration=0)
    assert coordinator.operation_status(db, Operation, "kb", "doc", "chunk")["state"] == "unknown"


def test_reserve_and_complete_use_the_same_lock_order(counting):
    db, Document, _, Operation, service, coordinator = counting
    args = dict(kb_id="kb", doc_id="doc", chunk_id="chunk", content_sha256="a" * 64, payload_sha256="b" * 64)
    op = coordinator.reserve_once(db, Operation, Document, **args)["operation_id"]
    coordinator.claim_once(db, Operation, op)
    coordinator.record_indexed(db, Operation, op, token_num=7, duration=0)
    count_entered, release_count, reserve_reached_op = threading.Event(), threading.Event(), threading.Event()
    errors, values = [], {}
    original_increment = service._increment_chunk_num_in_transaction
    original_select = Operation.select.__func__

    def hold_counter(*args):
        count_entered.set()
        assert release_count.wait(5)
        return original_increment(*args)

    def observe_select(cls, *args, **kwargs):
        if threading.current_thread().name == "reserver":
            reserve_reached_op.set()
        return original_select(cls, *args, **kwargs)

    def complete():
        try:
            values["completed"] = coordinator.complete_once(db, Operation, service, op)
        except BaseException as exc:
            errors.append(exc)
        finally:
            db.close()

    def reserve():
        try:
            values["reserved"] = coordinator.reserve_once(db, Operation, Document, **args)
        except BaseException as exc:
            errors.append(exc)
        finally:
            db.close()

    one = threading.Thread(name="completer", target=complete)
    two = threading.Thread(name="reserver", target=reserve)
    with patch.object(service, "_increment_chunk_num_in_transaction", side_effect=hold_counter), \
         patch.object(Operation, "select", classmethod(observe_select)):
        one.start()
        try:
            assert count_entered.wait(5)
            two.start()
            reserve_reached_op.wait(1.5)
        finally:
            release_count.set()
            one.join(timeout=8)
            if two.ident is not None:
                two.join(timeout=8)
    assert not one.is_alive() and not two.is_alive()
    assert not errors
    assert values["completed"] is True and values["reserved"]["created"] is False
    assert Document.get_by_id("doc").chunk_num == 1


def test_candidate_operation_model_has_unique_document_chunk_identity(isolated_mysql, experiment_gate):
    _, schema = experiment_gate
    db = PooledMySQLDatabase(schema, host="127.0.0.1", port=isolated_mysql, user="root", max_connections=4)

    class DataBaseModel(peewee.Model):
        class Meta:
            database = db

    path = ROOT / "candidate-server/api/db/db_models.py"
    tree = ast.parse(path.read_text(encoding="utf8"))
    node = next((n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "PaperflowChunkOperation"), None)
    assert node is not None, "the persistent operation model is not implemented"
    namespace = {"DataBaseModel": DataBaseModel, "CharField": peewee.CharField,
                 "IntegerField": peewee.IntegerField, "FloatField": peewee.FloatField}
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
    model = namespace["PaperflowChunkOperation"]
    try:
        db.create_tables([model])
        fields = dict(kb_id="kb", doc_id="doc", chunk_id="chunk", content_sha256="a" * 64,
                      payload_sha256="b" * 64, state="reserved", token_num=0, chunk_num=1, duration=0)
        model.create(id="c" * 64, **fields)
        with pytest.raises(peewee.IntegrityError):
            model.create(id="d" * 64, **fields)
    finally:
        db.close_all()
