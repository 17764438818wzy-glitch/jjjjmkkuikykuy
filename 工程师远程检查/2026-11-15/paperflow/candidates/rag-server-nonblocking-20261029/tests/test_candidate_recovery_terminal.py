"""Check that recovery waits for the original server write, using fake data only."""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
from unittest.mock import patch
from urllib.parse import unquote, urlsplit

import pytest


ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT.parents[1] / "prefect-paperflow"


@pytest.fixture
def setup_recovery(monkeypatch):
    monkeypatch.syspath_prepend(str(PROJECT))
    monkeypatch.syspath_prepend(str(PROJECT / "tests"))
    from local_rag import LocalRag
    from paperflow_prefect.legacy import bind
    from paperflow_prefect.records import read_json
    from paperflow_prefect.settings import Settings

    fixture = read_json(PROJECT / "runtime/native-fixture-receipt.json")
    prepared = read_json(Path(fixture["root"]) / "body/prepared-publication.json")
    settings = Settings.load()
    bind(settings)
    # bind() caches its selected release, whereas monkeypatch restores sys.path
    # after each test. Restore these paths for later fixtures in the same pytest
    # process without changing any persisted configuration.
    paths = [settings.path('release_root') / 'desktop', settings.path('program_root') / 'desktop',
             settings.path('release_root') / 'core']
    for path in reversed(paths):
        monkeypatch.syspath_prepend(str(path))
    native_paths = json.loads((settings.path('program_root') / 'config/paths.json').read_text(encoding='utf8'))
    dependencies = Path(native_paths['original_core']) / '.venv/Lib/site-packages'
    if dependencies.is_dir() and str(dependencies) not in sys.path:
        sys.path.append(str(dependencies))
    from preextract_publish import deliver_publication
    path = ROOT / "candidate-client/paperflow_prefect/recovery.py"
    spec = importlib.util.spec_from_file_location("paperflow_prefect.candidate_recovery", path)
    candidate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(candidate)

    class OperationRag(LocalRag):
        def __init__(self):
            super().__init__()
            self.lose_chunk = True
            self.operation_state = "in_progress"
            self.bad_digest = False
            self.bad_identity = None
            self.bad_counters = False
            self.unsupported = False
            self.activation_count = 0

        def request(self, method, endpoint, body=None):
            parts = [unquote(p) for p in urlsplit(endpoint).path.strip("/").split("/")]
            if method == "GET" and len(parts) == 7 and parts[4] == "chunks" and parts[6] == "operation":
                self.calls.append((method, endpoint, copy.deepcopy(body)))
                if self.unsupported:
                    raise ConnectionError("operation status unavailable")
                ds, did, cid = parts[1], parts[3], parts[5]
                chunk = next(c for c in self.chunks[ds, did] if c["id"] == cid)
                digest = hashlib.sha256(chunk["content"].encode("utf8")).hexdigest()
                result = dict(state=self.operation_state, dataset_id=ds, document_id=did, chunk_id=cid,
                              content_sha256="0" * 64 if self.bad_digest else digest,
                              counters_committed=self.operation_state == "completed" and not self.bad_counters)
                encoded = json.dumps(["paperflow-chunk-operation/1", ds, did, cid], ensure_ascii=False,
                                     separators=(",", ":")).encode("utf8")
                result["operation_id"] = hashlib.sha256(encoded).hexdigest()
                if self.bad_identity:
                    result[self.bad_identity] = "another-identity"
                return result
            if method == "PATCH" and body and body.get("enabled") == 1:
                self.activation_count += 1
            return super().request(method, endpoint, body)

    return prepared, deliver_publication, candidate, OperationRag


def interrupted_delivery(prepared, deliver, client, receipt):
    with patch("paperflow_prefect.legacy.connection", side_effect=AssertionError("model credentials requested")):
        with patch("preextract_request.request_once", side_effect=AssertionError("model invoked")):
            with pytest.raises(TimeoutError):
                deliver(prepared, config=None, receipt_path=receipt, client=client)


def recover_without_model(candidate, prepared, client, receipt):
    with patch("paperflow_prefect.legacy.connection", side_effect=AssertionError("model credentials requested")):
        with patch("preextract_request.request_once", side_effect=AssertionError("model invoked")):
            return candidate.recover_publication(prepared, receipt_path=receipt, client=client)


def test_readable_chunk_waits_for_terminal_then_recovers_original(setup_recovery, tmp_path):
    prepared, deliver, candidate, client_class = setup_recovery
    client = client_class()
    receipt = tmp_path / "receipt.json"
    interrupted_delivery(prepared, deliver, client, receipt)
    original_receipt = receipt.read_bytes()
    original_chunk = next(iter(client.chunks.values()))[0]["content"]
    with pytest.raises(candidate.RecoveryPending):
        recover_without_model(candidate, prepared, client, receipt)
    assert receipt.read_bytes() == original_receipt
    assert client.posts == 1 and client.uploads == 1 and client.activation_count == 0
    client.operation_state = "completed"
    result = recover_without_model(candidate, prepared, client, receipt)
    assert result["status"] == "complete"
    assert client.uploads == 1
    all_contents = [c["content"] for rows in client.chunks.values() for c in rows]
    assert all_contents.count(original_chunk) == 1


@pytest.mark.parametrize("failure", ["mismatched_content", "unavailable_status", "dataset_id", "document_id", "chunk_id", "operation_id", "uncommitted_counters"])
def test_incomplete_or_untrusted_terminal_evidence_cannot_activate(setup_recovery, tmp_path, failure):
    prepared, deliver, candidate, client_class = setup_recovery
    client = client_class()
    receipt = tmp_path / "receipt.json"
    interrupted_delivery(prepared, deliver, client, receipt)
    client.operation_state = "completed"
    client.bad_digest = failure == "mismatched_content"
    client.unsupported = failure == "unavailable_status"
    client.bad_identity = failure if failure in {"dataset_id", "document_id", "chunk_id", "operation_id"} else None
    client.bad_counters = failure == "uncommitted_counters"
    previous = receipt.read_bytes()
    with pytest.raises(candidate.RecoveryPending):
        recover_without_model(candidate, prepared, client, receipt)
    assert receipt.read_bytes() == previous
    assert client.posts == 1 and client.activation_count == 0


def test_legacy_written_receipt_does_not_prove_old_write_finished(setup_recovery, tmp_path):
    from paperflow_prefect.records import read_json, write_json
    prepared, deliver, candidate, client_class = setup_recovery
    client = client_class()
    receipt = tmp_path / "receipt.json"
    interrupted_delivery(prepared, deliver, client, receipt)
    state = read_json(receipt)
    target = next(iter(state["targets"].values()))["body"]
    old = next(iter(target["chunks"].values()))
    old.update(status="written", chunk_id=next(iter(client.chunks.values()))[0]["id"])
    write_json(receipt, state)
    previous = receipt.read_bytes()
    with pytest.raises(candidate.RecoveryPending):
        recover_without_model(candidate, prepared, client, receipt)
    assert receipt.read_bytes() == previous
    assert client.posts == 1 and client.activation_count == 0


def test_native_batch_without_terminal_protocol_cannot_reconcile(setup_recovery, tmp_path):
    from paperflow_prefect.records import read_json, write_json
    prepared, deliver, candidate, client_class = setup_recovery
    client = client_class()
    receipt = tmp_path / "receipt.json"
    interrupted_delivery(prepared, deliver, client, receipt)
    state = read_json(receipt)
    state["transport"] = "native_body_batch/1"
    target = next(iter(state["targets"].values()))["body"]
    target["native_batch"] = dict(state="intent", pipeline_id="a" * 32,
                                  transport_sha256="b" * 64, record_count=1)
    write_json(receipt, state)
    previous = receipt.read_bytes()
    with patch("preextract_publish.reconcile_publication", return_value={"status": "complete"}) as reconcile:
        with pytest.raises(candidate.RecoveryPending):
            recover_without_model(candidate, prepared, client, receipt)
    assert not reconcile.called
    assert receipt.read_bytes() == previous
    assert client.posts == 1 and client.activation_count == 0
