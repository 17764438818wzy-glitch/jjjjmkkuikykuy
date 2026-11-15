"""Exercise the snapshotted counter method with real Peewee and a temporary database."""
import ast
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import peewee


ROOT = Path(__file__).resolve().parents[1]


def test_original_counter_method_updates_document_and_library_atomically(tmp_path):
    db = peewee.SqliteDatabase(str(tmp_path / "counter-test.sqlite"),
                               pragmas={"journal_mode": "wal", "busy_timeout": 5000})

    class Base(peewee.Model):
        class Meta:
            database = db

    class Knowledgebase(Base):
        id = peewee.TextField(primary_key=True)
        token_num = peewee.IntegerField(default=0)
        chunk_num = peewee.IntegerField(default=0)

    class Document(Base):
        id = peewee.TextField(primary_key=True)
        kb_id = peewee.TextField()
        token_num = peewee.IntegerField(default=0)
        chunk_num = peewee.IntegerField(default=0)
        process_duration = peewee.IntegerField(default=0)

    db.create_tables([Knowledgebase, Document])
    Knowledgebase.create(id="dataset-1")
    Document.create(id="paper-1", kb_id="dataset-1")
    tree = ast.parse((ROOT / "baseline/api/db/services/document_service.py").read_text(encoding="utf-8"))
    service = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "DocumentService")
    counter = next(node for node in service.body if isinstance(node, ast.FunctionDef) and node.name == "increment_chunk_num")
    isolated_class = ast.ClassDef(name="CounterService", bases=[], keywords=[], body=[counter], decorator_list=[])
    module = ast.fix_missing_locations(ast.Module(body=[isolated_class], type_ignores=[]))
    namespace = {"DB": db, "Knowledgebase": Knowledgebase, "logging": logging}
    exec(compile(module, "<snapshotted-document-service>", "exec"), namespace)
    cls = namespace["CounterService"]
    cls.model = Document
    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(lambda _: cls.increment_chunk_num("paper-1", "dataset-1", 2, 1, 0), range(20)))
    document, library = Document.get_by_id("paper-1"), Knowledgebase.get_by_id("dataset-1")
    assert (document.chunk_num, document.token_num) == (20, 40)
    assert (library.chunk_num, library.token_num) == (20, 40)
    db.close()
