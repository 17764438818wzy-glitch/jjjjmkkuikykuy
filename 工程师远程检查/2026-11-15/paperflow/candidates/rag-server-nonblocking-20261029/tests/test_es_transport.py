"""Check retry behavior using the real ES client and an isolated loopback server."""
import ast
import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import logging
import os
from pathlib import Path
import re
import threading
import time
import types

import elastic_transport
from elasticsearch import ApiError, Elasticsearch
import pytest


ROOT = Path(__file__).resolve().parents[1]


def test_strict_chunk_insert_disables_transport_retries():
    class Handler(BaseHTTPRequestHandler):
        calls = 0

        def do_PUT(self):
            type(self).calls += 1
            body = b'{"error":{"type":"unavailable_shards_exception","reason":"fixture"},"status":503}'
            length = int(self.headers.get("Content-Length", "0"))
            if length:
                self.rfile.read(length)
            self.send_response(503)
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
    variant = os.getenv("RAG_STAGE4_TEST_SOURCE", "candidate-embedding-only/api/apps/restful_apis/chunk_api.py").split("/")[0]
    source = ROOT / variant / "rag/utils/es_conn.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "ESConnection")
    method = next(node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == "insert")
    namespace = {"copy": copy, "re": re, "time": time, "ATTEMPT_TIME": 2,
                 "ConnectionTimeout": elastic_transport.ConnectionTimeout}
    exec(compile(ast.Module(body=[method], type_ignores=[]), str(source), "exec"), namespace)
    store = types.SimpleNamespace(es=client, logger=logging.getLogger("fixture.real_es"), _connect=lambda: None)
    store.insert = types.MethodType(namespace["insert"], store)
    try:
        with pytest.raises(ApiError) as raised:
            store.insert([{"id": "fixture"}], "fixture-index", "fixture-dataset", retry_unknown_write=False)
        assert raised.value.status_code == 503
        assert Handler.calls == 1
    finally:
        client.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
