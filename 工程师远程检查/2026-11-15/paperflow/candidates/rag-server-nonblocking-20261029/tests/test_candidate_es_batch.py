"""Validate real candidate ES method against isolated bulk responses."""
from __future__ import annotations

import ast
import copy
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import threading
import types
from urllib.parse import parse_qs, urlsplit

from elasticsearch import Elasticsearch
import pytest


ROOT = Path(__file__).resolve().parents[1]


def load_method(client):
    path = ROOT / "candidate-server/rag/utils/es_conn.py"
    tree = ast.parse(path.read_text(encoding="utf8"))
    cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "ESConnection")
    method = next((node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == "insert_checked"), None)
    assert method is not None, "per-item bulk result handling is not implemented"
    exception = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "BatchWriteUncertain")
    namespace = {"copy": copy}
    exec(compile(ast.Module(body=[exception, method], type_ignores=[]), str(path), "exec"), namespace)
    store = types.SimpleNamespace(es=client)
    return types.MethodType(namespace["insert_checked"], store), namespace["BatchWriteUncertain"]


class Client:
    def __init__(self, response=None, error=None):
        self.response, self.error = response, error
        self.calls, self.options_seen = [], []

    def options(self, **kwargs):
        self.options_seen.append(kwargs)
        return self

    def bulk(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return self.response


def success(cid, status=201):
    return {"index": {"_index": "test-index", "_id": cid, "status": status}}


def rejected(cid):
    return {"index": {"_index": "test-index", "_id": cid, "status": 429, "error": {"type": "too_many_requests"}}}


def test_partial_bulk_results_are_mapped_per_chunk_with_one_wait():
    client = Client({"errors": True, "items": [success("c"), rejected("b"), success("a")]})
    insert, _ = load_method(client)
    source = [{"id": "a", "text": "A"}, {"id": "b", "text": "B"}, {"id": "c", "text": "C"}]
    result = insert(source, "test-index", "dataset")
    assert result == [{"id": "a", "state": "indexed", "status": 201},
                      {"id": "b", "state": "rejected", "status": 429, "error_type": "too_many_requests"},
                      {"id": "c", "state": "indexed", "status": 201}]
    assert len(client.calls) == 1 and client.options_seen == [{"max_retries": 0}]
    assert client.calls[0]["refresh"] == "wait_for"
    assert len(client.calls[0]["operations"]) == 6
    assert source[0] == {"id": "a", "text": "A"}


@pytest.mark.parametrize("response", [
    {"errors": False, "items": [success("a")]},
    {"errors": False, "items": [success("a"), success("a")]},
    {"errors": False, "items": [success("a"), success("wrong")]},
    {"errors": False, "items": [success("a"), {"index": {"_id": "b"}}]},
    {"errors": False, "items": [success("a"), rejected("b")]},
    {"errors": False, "items": [success("a"), {"index": {"_index": "other-index", "_id": "b", "status": 201}}]},
    {"errors": False, "items": [success("a"), {"index": {"_id": "b", "status": 201}}]},
    {"errors": False, "items": [success("a"), success("b", 202)]},
])
def test_malformed_bulk_response_stays_unknown(response):
    client = Client(response)
    insert, unknown = load_method(client)
    with pytest.raises(unknown):
        insert([{"id": "a"}, {"id": "b"}], "test-index", "dataset")
    assert len(client.calls) == 1


def test_timeout_stays_unknown_and_is_not_retried():
    client = Client(error=TimeoutError("test timeout"))
    insert, unknown = load_method(client)
    with pytest.raises(unknown):
        insert([{"id": "a"}], "test-index", "dataset")
    assert len(client.calls) == 1 and client.options_seen == [{"max_retries": 0}]


def test_duplicate_chunk_ids_are_rejected_before_sending():
    client = Client({})
    insert, _ = load_method(client)
    with pytest.raises(ValueError):
        insert([{"id": "a"}, {"id": "a"}], "test-index", "dataset")
    assert client.calls == []


def test_real_client_sends_one_bulk_wait_request_to_loopback():
    class Handler(BaseHTTPRequestHandler):
        requests = []

        def do_PUT(self):
            payload = self.rfile.read(int(self.headers["Content-Length"]))
            type(self).requests.append((self.path, payload))
            body = json.dumps({"errors": True, "items": [success("a"), rejected("b")]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("X-Elastic-Product", "Elasticsearch")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_POST = do_PUT

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    client = Elasticsearch(f"http://127.0.0.1:{server.server_port}", max_retries=2, retry_on_status=(503,))
    try:
        insert, _ = load_method(client)
        result = insert([{"id": "a", "text": "A"}, {"id": "b", "text": "B"}], "test-index", "dataset")
        assert [row["state"] for row in result] == ["indexed", "rejected"]
        assert len(Handler.requests) == 1
        path, payload = Handler.requests[0]
        assert parse_qs(urlsplit(path).query)["refresh"] == ["wait_for"]
        assert len(payload.splitlines()) == 4
    finally:
        client.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
