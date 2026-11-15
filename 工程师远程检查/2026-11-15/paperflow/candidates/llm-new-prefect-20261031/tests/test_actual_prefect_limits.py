"""Prove 50/50/20 leases on an isolated Prefect server, without a model."""
import contextvars
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import threading

from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
from prefect.concurrency.sync import concurrency
from prefect.settings import PREFECT_API_URL
from prefect.testing.utilities import prefect_test_harness


HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parents[1]
sys.path[:0] = [str(HERE),str(ROOT / 'prefect-paperflow')]
from model_limits import TOTAL, BODY, ATTACHMENT, LIMITS, model_lease
from paperflow_prefect.resources import cancellable_concurrency


def test_real_prefect_server_enforces_shared_total_and_attachment_caps():
    stop = threading.Event()
    with prefect_test_harness(server_startup_timeout=60):
        with get_client(sync_client=True) as api:
            for name, limit in LIMITS.items():
                api.create_global_concurrency_limit(GlobalConcurrencyLimitCreate(name=name,limit=limit,active=True))
            selected = PREFECT_API_URL.value()
            def blocked_until_release(full_names, occupied, waiting_kind, lane):
                entered, started = threading.Event(), threading.Event()
                def contender():
                    started.set()
                    with model_lease(waiting_kind, stop, selected, cancellable_concurrency):
                        entered.set()
                with ThreadPoolExecutor(max_workers=1) as worker:
                    with concurrency(full_names, occupy=occupied, strict=True):
                        assert api.read_global_concurrency_limit_by_name(lane).active_slots == occupied
                        pending = worker.submit(contextvars.copy_context().run, contender)
                        assert started.wait(5) and not entered.wait(1)
                    assert entered.wait(15)
                    pending.result(timeout=5)
            # All 50 shared slots full: even an attachment must wait.
            blocked_until_release([TOTAL,BODY],50,'attachment',TOTAL)
            # Only 20 attachments active: the 21st waits while total still has room.
            blocked_until_release([TOTAL,ATTACHMENT],20,'attachment',ATTACHMENT)
            for name in LIMITS:
                assert api.read_global_concurrency_limit_by_name(name).active_slots == 0
