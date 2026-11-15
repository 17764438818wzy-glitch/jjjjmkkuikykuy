"""Synthetic stage adapters use an in-memory Prefect metadata boundary."""
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).parent / 'control'))
from test_folder_pause import stage_store


@pytest.fixture(autouse=True)
def synthetic_stage_metadata(request, monkeypatch):
    # These suites already replace transports and use synthetic UUIDs rather
    # than persisted FlowRuns. Keep only their new stage metadata in memory;
    # native Prefect coordination is exercised separately in control tests.
    if request.path.name in {'test_paper_pipeline.py', 'test_postprocessing.py',
            'test_partitioned_pipeline.py', 'test_llm_retry.py', 'test_counter_thread_lifecycle.py',
            'test_processing_handoff_integration.py', 'test_llm_handoff.py',
            'test_preprocessing_boundary.py', 'test_manual_retry_handoff.py', 'test_partitioned_body.py'}:
        request.getfixturevalue('stage_store')
    if request.path.name in {'test_folder_snapshot_reuse.py', 'test_query_service.py',
            'test_query_read_cost.py', 'test_folder_ticket_fast_read.py'}:
        from paperflow_v3.control import stage_admission
        async def no_new_stage_journal(_task_id, **kwargs):
            return []
        monkeypatch.setattr(stage_admission, '_stage_rows', no_new_stage_journal)


@pytest.fixture(autouse=True)
def synthetic_transport_control_gate(request,monkeypatch):
    if (request.path.parent.name!='stages' and request.path.name!='test_manual_retry_handoff.py') or request.path.name=='test_server_response_receipt.py':return
    # These transports and task IDs are synthetic. Gate behavior is covered by
    # the real isolated-server test and control tests, without a business API.
    from contextlib import contextmanager
    import threading
    from paperflow_v3.stages import llm_request
    locks={}
    @contextmanager
    def gate(name,**kwargs):
        with locks.setdefault(name,threading.RLock()):yield
    monkeypatch.setattr(llm_request,'sync_concurrency',gate)
