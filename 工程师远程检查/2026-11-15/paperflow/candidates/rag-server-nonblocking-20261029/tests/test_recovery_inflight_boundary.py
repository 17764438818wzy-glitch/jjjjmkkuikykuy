"""Show what the existing client cannot infer from one successful readback.

This uses the existing synthetic publication and in-memory RAG double only.
No live model, RAG endpoint, paper, or NAS is contacted.
"""
from pathlib import Path
import threading
from unittest.mock import patch

import pytest


class OriginalWriteStillPending(AssertionError):
    """Recovery activated before the original simulated write finished."""


@pytest.mark.xfail(strict=True, raises=OriginalWriteStillPending,
                   reason="Client has no signal that the old server-side write has finished")
def test_recovery_does_not_activate_while_original_server_write_is_in_flight(tmp_path, monkeypatch):
    project = Path(__file__).resolve().parents[3] / "prefect-paperflow"
    monkeypatch.syspath_prepend(str(project))
    monkeypatch.syspath_prepend(str(project / "tests"))
    from local_rag import LocalRag
    from paperflow_prefect.legacy import bind
    from paperflow_prefect.records import read_json
    from paperflow_prefect.settings import Settings

    # Refuse to regenerate the fixture, which would write outside this candidate.
    fixture = read_json(project / "runtime/native-fixture-receipt.json")
    prepared = read_json(Path(fixture["root"]) / "body/prepared-publication.json")
    bind(Settings.load())
    from preextract_publish import deliver_publication
    from paperflow_prefect.recovery import recover_publication

    release = threading.Event()
    finished = threading.Event()
    worker = None

    class DelayedCompletionRag(LocalRag):
        activated_while_original_write_pending = False

        def request(self, method, endpoint, body=None):
            nonlocal worker
            if method == "PATCH" and body and body.get("enabled") == 1 and worker is not None and not finished.is_set():
                self.activated_while_original_write_pending = True
            try:
                return super().request(method, endpoint, body)
            except TimeoutError:
                if method == "POST" and endpoint.endswith("/chunks"):
                    # ES record is already readable, but model a server worker
                    # still waiting to finish its subsequent SQL count update.
                    worker = threading.Thread(target=lambda: (release.wait(), finished.set()))
                    worker.start()
                raise

    client = DelayedCompletionRag()
    client.lose_chunk = True
    receipt = tmp_path / "receipt.json"
    try:
        with patch("paperflow_prefect.legacy.connection", side_effect=AssertionError("model credentials requested")):
            with patch("preextract_request.request_once", side_effect=AssertionError("model invoked")):
                with pytest.raises(TimeoutError):
                    deliver_publication(prepared, config=None, receipt_path=receipt, client=client)
                assert worker is not None and not finished.is_set()
                result = recover_publication(prepared, receipt_path=receipt, client=client)
        assert result["status"] == "complete"
        if client.activated_while_original_write_pending:
            raise OriginalWriteStillPending("Activated while the original server write remained in flight")
    finally:
        release.set()
        if worker is not None:
            worker.join(timeout=5)
