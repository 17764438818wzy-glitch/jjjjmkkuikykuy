"""A confirmed shared authentication fault pauses only its parent task."""

import asyncio
from urllib.error import HTTPError, URLError
from uuid import uuid4

from prefect import flow
from prefect.context import FlowRunContext
from prefect.testing.utilities import prefect_test_harness
import pytest


def test_shared_authentication_fault_pauses_one_task_but_content_errors_do_not(monkeypatch):
    from paperflow_v3.control import flows
    from paperflow_v3.control.admission import Admission, begin_control_session
    from paperflow_v3.control.receipts import read_receipts

    async def fail_auth(*_args):
        raise HTTPError("http://synthetic.invalid", 401, "unauthorized", {}, None)

    async def fail_paper(*_args):
        raise ValueError("synthetic paper content failure")

    @flow(persist_result=False)
    async def parent():
        task = str(FlowRunContext.get().flow_run.id)
        current, other = Admission(task), Admission(uuid4())
        await current.start()
        await other.start()

        @flow(persist_result=False)
        async def child():
            child_id = str(FlowRunContext.get().flow_run.id)
            identity = {"task_id": task, "group_id": "group", "paper_id": "paper", "receipt_id": "receipt",
                        "attempt_id": child_id, "parent_run_id": task, "task_run_id": child_id}
            monkeypatch.setattr(flows, "_process_step", fail_auth)
            with pytest.raises(HTTPError):
                await flows.run_paper(identity, {}, object(), current, {}, "unused")
            assert not await current.running() and await other.running()
            records = await read_receipts(task)
            assert any(row["kind"] == "dependency_failure" and row["data"]["dependency"] == "model"
                       for row in records)
            await current.start()
            monkeypatch.setattr(flows, "_process_step", fail_paper)
            with pytest.raises(ValueError):
                await flows.run_paper(identity, {}, object(), current, {}, "unused")
            assert await current.running() and await other.running()

        await child()

    with prefect_test_harness(server_startup_timeout=60):
        asyncio.run(begin_control_session())
        asyncio.run(parent())


def test_rag_authentication_message_requires_a_shared_read_only_confirmation(monkeypatch):
    from paperflow_v3.control import flows

    message = "RAGFlow 认证失败，请检查 API Key 是否有效及是否有知识库访问权限。"

    async def confirmed(*_args):
        return True

    async def healthy(*_args):
        return False

    monkeypatch.setattr(flows, "_rag_unavailable", confirmed)
    assert asyncio.run(flows._shared_dependency_failure(RuntimeError(message), "delivery", object(), object())) == "rag"
    monkeypatch.setattr(flows, "_rag_unavailable", healthy)
    assert asyncio.run(flows._shared_dependency_failure(RuntimeError(message), "delivery", object(), object())) is None
    assert asyncio.run(flows._shared_dependency_failure(ValueError(message), "delivery", object(), object())) is None


def test_model_and_nas_failures_require_confirmation_of_shared_unavailability(monkeypatch):
    from paperflow_v3.control import flows

    async def unavailable(*_args):
        return True

    async def healthy(*_args):
        return False

    error = URLError(ConnectionRefusedError("synthetic"))
    monkeypatch.setattr(flows, "_model_unavailable", healthy)
    assert asyncio.run(flows._shared_dependency_failure(error, "processing", object(), object())) is None
    monkeypatch.setattr(flows, "_model_unavailable", unavailable)
    assert asyncio.run(flows._shared_dependency_failure(error, "processing", object(), object())) == "model"
    assert asyncio.run(flows._shared_dependency_failure(ValueError("bad paper"), "processing", object(), object())) is None
    nas_records = [{"kind": "nas", "data": {"files": {"body": {"status": "failed"}}}}]
    monkeypatch.setattr(flows, "_nas_unavailable", unavailable)
    assert asyncio.run(flows._shared_dependency_failure(OSError("write"), "delivery", object(), object(), nas_records)) == "nas"
    assert asyncio.run(flows._shared_dependency_failure(OSError("local file"), "delivery", object(), object(), [])) is None


def test_classification_network_failure_pauses_only_after_nimble_probe(monkeypatch, tmp_path):
    from pathlib import Path
    from paperflow_v3.control import parent as parent_module
    from paperflow_v3.control.admission import Admission, begin_control_session
    from paperflow_v3.control.classification import first_step_modules
    from paperflow_v3.control.receipts import read_receipts

    first_step = Path(__file__).resolve().parents[3] / "first-step-control-20261029"
    unknown = first_step_modules(first_step)["control"].UnknownRequest
    group = tmp_path / "source" / "one"
    group.mkdir(parents=True)
    (group / "paper.txt").write_text("synthetic", encoding="utf8")

    def failed_classification(**_kwargs):
        raise unknown("outcome unknown") from URLError(ConnectionRefusedError("offline"))

    async def confirmed_unavailable(*_args):
        return True

    monkeypatch.setattr(parent_module, "classify_and_register", failed_classification)
    monkeypatch.setattr(parent_module, "_nimble_unavailable", confirmed_unavailable)

    @flow(persist_result=False)
    async def parent():
        task = FlowRunContext.get().flow_run.id
        current, other = Admission(task), Admission(uuid4())
        await current.start()
        await other.start()
        await parent_module.classify_and_dispatch(task_id=task, source_paths=[str(group)], first_step_root=first_step,
            work_root=tmp_path / "work", history_roots=(), legacy_classification_root=tmp_path / "old",
            nimble_endpoint="http://127.0.0.1:1/v1/systemone", nimble_lock_path=tmp_path / "nimble.lock",
            child_deployment_id=uuid4(), admission=current)
        assert not await current.running() and await other.running()
        records = await read_receipts(task)
        assert any(row["kind"] == "dependency_failure" and row["data"]["dependency"] == "nimble" for row in records)
        assert any(row["kind"] == "classification" and row["data"]["status"] == "unresolved" for row in records)

    with prefect_test_harness(server_startup_timeout=60):
        asyncio.run(begin_control_session())
        asyncio.run(parent())
