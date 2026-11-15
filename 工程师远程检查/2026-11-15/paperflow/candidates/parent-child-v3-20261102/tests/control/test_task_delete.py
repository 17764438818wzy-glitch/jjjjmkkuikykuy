"""Task deletion only touches synthetic task-owned data."""
import asyncio
import hashlib
import json
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient
from prefect import flow, task
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import ArtifactCreate
from prefect.client.schemas.filters import ArtifactFilter
from prefect.exceptions import ObjectNotFound
from prefect.states import Paused
from prefect.testing.utilities import prefect_test_harness
import pytest
import httpx


def test_control_gate_cleanup_is_exact_and_idempotent():
    from paperflow_v3.control.deletion import remove_control_limit
    task_id = uuid4()
    expected = 'paperflow-v3-control-' + str(task_id).replace('-', '')
    removed = []
    class Client:
        async def delete_global_concurrency_limit_by_name(self, name):
            assert name == expected
            removed.append(name)
            if len(removed) > 1:
                response = httpx.Response(404, request=httpx.Request('DELETE', 'http://127.0.0.1/limit'))
                raise ObjectNotFound(httpx.HTTPStatusError('missing', request=response.request, response=response))
    async def exercise():
        await remove_control_limit(Client(), task_id)
        await remove_control_limit(Client(), task_id)
    asyncio.run(exercise())
    assert removed == [expected, expected]


def test_delete_task_clears_owned_data_and_keeps_originals_other_tasks_and_archives(tmp_path):
    from paperflow_v3.control.api import create_app
    from paperflow_v3.delivery.progress import progress_path

    @flow(persist_result=False)
    def parent(task_name: str, source_paths: list[str]):
        return task_name

    @flow(persist_result=False)
    def paper(task_id: str):
        return task_id

    @task
    def link():
        pass

    source = tmp_path / "originals"
    source.mkdir()
    original = source / "paper.docx"
    original.write_bytes(b"unchanged original")
    nas = tmp_path / "nas"
    nas.mkdir()
    archive = nas / "delivered.docx"
    archive.write_bytes(b"unchanged delivered result")
    data = tmp_path / "data"
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"data_root": str(data), "nas_destination": str(nas)}))
    with prefect_test_harness(server_startup_timeout=60):
        with get_client(sync_client=True) as prefect:
            deployment = prefect.create_deployment(prefect.create_flow(parent), name="deletion-test")
            create_request = uuid4()
            a = prefect.create_flow_run_from_deployment(deployment,
                parameters={"task_name": "A", "source_paths": [str(source)]}, state=Paused(),
                idempotency_key="paperflow-v3-task-" + str(create_request))
            from prefect.client.schemas.actions import VariableCreate
            orphan_create_name = "paperflow_v3_operation_" + str(create_request).replace("-", "_")
            prefect.create_variable(VariableCreate(name=orphan_create_name,
                value={"request_id": str(create_request), "action": "create", "target": None, "status": "accepted"}))
            b = prefect.create_flow_run_from_deployment(deployment,
                parameters={"task_name": "B", "source_paths": [str(source)]}, state=Paused())
            parent_link = prefect.create_task_run(link, flow_run_id=a.id, dynamic_key="paper")
            child = prefect.create_flow_run(paper, parameters={"task_id": str(a.id)},
                parent_task_run_id=parent_link.id, state=Paused(), tags=["paperflow-v3-paper"])
            for run in (a, child, b):
                prefect.create_artifact(ArtifactCreate(key="delete-test-" + run.id.hex,
                    type="paperflow-v3-receipt", flow_run_id=run.id, data={"task_id": str(a.id if run.id != b.id else b.id)}))
            for run in (a, b):
                work = data / "paperflow-v3" / str(run.id)
                work.mkdir(parents=True)
                (work / "generated.txt").write_text("temporary")
                prefect.create_variable(__import__("prefect.client.schemas.actions", fromlist=["VariableCreate"]).VariableCreate(
                    name="paperflow_v3_task_" + str(run.id).replace("-", "_"),
                    value={"state": "paused", "session": None}))
                prefect.create_variable(VariableCreate(
                    name="paperflow_v3_classification_summary_" + str(run.id).replace("-", "_"),
                    value={"task_id": str(run.id), "counts": {"passed": 1}}))
            state = progress_path(data, child.id)
            state.parent.mkdir(parents=True)
            state.write_text(json.dumps({"identity": {"task_id": str(a.id), "task_run_id": str(child.id)}}))
        app = create_app({"deployment_id": deployment, "project_config": str(config)})
        with TestClient(app) as client:
            response = client.post(f"/api/v1/prefect/tasks/{a.id}/delete",
                json={"request_id": str(uuid4()), "intent_seq": 1})
            assert response.status_code == 200, response.text
            assert response.json()["deleted"] is True
            assert client.get(f"/api/v1/prefect/tasks/{a.id}/deletion").json()["deleted"] is True
        assert not (data / "paperflow-v3" / str(a.id)).exists()
        assert not state.exists()
        assert (data / "paperflow-v3" / str(b.id) / "generated.txt").read_text() == "temporary"
        assert original.read_bytes() == b"unchanged original"
        assert archive.read_bytes() == b"unchanged delivered result"
        with get_client(sync_client=True) as prefect:
            for run_id in (a.id, child.id):
                with pytest.raises(ObjectNotFound):
                    prefect.read_flow_run(run_id)
            assert prefect.read_flow_run(b.id).id == b.id
            assert prefect.read_artifacts(artifact_filter=ArtifactFilter(
                flow_run_id={"any_": [a.id, child.id]})) == []
            assert len(prefect.read_artifacts(artifact_filter=ArtifactFilter(
                flow_run_id={"any_": [b.id]}))) == 1
            assert prefect.read_variable_by_name(orphan_create_name) is None
            assert prefect.read_variable_by_name("paperflow_v3_task_" + str(a.id).replace("-", "_")) is None
            assert prefect.read_variable_by_name("paperflow_v3_task_" + str(b.id).replace("-", "_")) is not None
            assert prefect.read_variable_by_name("paperflow_v3_classification_summary_" + str(a.id).replace("-", "_")) is None
            assert prefect.read_variable_by_name("paperflow_v3_classification_summary_" + str(b.id).replace("-", "_")) is not None


def test_local_delete_rejects_source_overlap_before_removing_any_file(tmp_path):
    from paperflow_v3.control.deletion import local_cleanup_plan
    task_id = uuid4()
    work = tmp_path / "data" / "paperflow-v3" / str(task_id)
    work.mkdir(parents=True)
    original = work / "original.docx"
    original.write_bytes(b"original")
    with pytest.raises(ValueError, match="source|overlap|original"):
        local_cleanup_plan({"data_root": str(tmp_path / "data"),
            "nas_destination": str(tmp_path / "nas")}, task_id, [str(work)], [])
    assert original.read_bytes() == b"original"


def test_query_deletion_waits_for_snapshot_writer_and_drops_saved_views():
    from paperflow_v3.control.query_service import QueryService
    async def run():
        task_id = str(uuid4())
        service = QueryService({"deployment_id": uuid4()})
        service._last[task_id] = {"folders": ["old"]}
        service._last_classifications[task_id] = {"items": ["old"]}
        service._last_list = {"tasks": [{"task_id": task_id}, {"task_id": "other"}]}
        entered = asyncio.Event()
        release = asyncio.Event()
        completed = asyncio.Event()
        async def writer():
            entered.set()
            await release.wait()
            completed.set()
        running = asyncio.create_task(writer())
        await entered.wait()
        service._snapshot_writers[task_id] = {running}
        forgetting = asyncio.create_task(service.forget_task(task_id))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert not running.cancelled()
        assert not forgetting.done()
        release.set()
        await forgetting
        assert completed.is_set()
        assert task_id not in service._last
        assert task_id not in service._last_classifications
        assert [row["task_id"] for row in service._last_list["tasks"]] == ["other"]
        with pytest.raises(Exception) as error:
            await service.task_details(task_id)
        assert getattr(error.value, "status_code", None) == 404
        await service.close()
    asyncio.run(run())


def test_process_status_unknown_is_not_treated_as_exited(monkeypatch):
    from types import SimpleNamespace
    import psutil
    from paperflow_v3.control import deletion
    from prefect.states import Crashed
    def denied(pid):
        raise psutil.AccessDenied(pid)
    monkeypatch.setattr(psutil, "Process", denied)
    run = SimpleNamespace(id=uuid4(), infrastructure_pid="12345", state=Crashed(), run_count=1)
    with pytest.raises(RuntimeError, match="process|execution|identity"):
        deletion.execution_active(run)


def test_delivery_state_inside_original_directory_is_protected(tmp_path):
    from paperflow_v3.control.deletion import local_cleanup_plan
    from paperflow_v3.delivery.progress import progress_path
    task_id, child_id = uuid4(), uuid4()
    data = tmp_path / "data"
    path = progress_path(data, child_id)
    path.parent.mkdir()
    path.write_text(json.dumps({"identity": {"task_id": str(task_id), "task_run_id": str(child_id)}}))
    with pytest.raises(ValueError, match="source|original|protected"):
        local_cleanup_plan({"data_root": str(data), "nas_destination": str(tmp_path / "nas")},
            task_id, [str(path.parent)], [str(child_id)])
    assert path.exists()


@pytest.mark.parametrize("failure_stage", ["variables", "admission"])
def test_delete_can_resume_after_prefect_cleanup_failure_and_service_restart(tmp_path, monkeypatch, failure_stage):
    import httpx
    from paperflow_v3.control import deletion
    from paperflow_v3.control.api import create_app
    @flow(persist_result=False)
    def parent(task_name: str, source_paths: list[str]):
        return task_name
    source = tmp_path / "source"
    source.mkdir()
    (source / "paper.docx").write_bytes(b"original")
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"data_root": str(tmp_path / "data"), "nas_destination": str(tmp_path / "nas")}))
    remove_variables = deletion.remove_variables
    calls = 0
    async def transient_failure(client, task_id, extra_names=()):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectError("synthetic lost connection")
        await remove_variables(client, task_id, extra_names)
    if failure_stage == "variables":
        monkeypatch.setattr(deletion, "remove_variables", transient_failure)
    else:
        original_delete = deletion.Admission.delete
        async def failed_admission(self):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise httpx.ConnectError("synthetic initial admission failure")
            await original_delete(self)
        monkeypatch.setattr(deletion.Admission, "delete", failed_admission)
    with prefect_test_harness(server_startup_timeout=60):
        with get_client(sync_client=True) as prefect:
            deployment = prefect.create_deployment(prefect.create_flow(parent), name="retry-delete")
            run = prefect.create_flow_run_from_deployment(deployment,
                parameters={"task_name": "A", "source_paths": [str(source)]}, state=Paused())
        settings = {"deployment_id": deployment, "project_config": str(config)}
        with TestClient(create_app(settings)) as client:
            response = client.post(f"/api/v1/prefect/tasks/{run.id}/delete",
                json={"request_id": str(uuid4()), "intent_seq": 1})
            assert response.status_code == 409
            status = client.get(f"/api/v1/prefect/tasks/{run.id}/deletion").json()
            assert status["deleted"] is False and status["status"] == "failed"
        with TestClient(create_app(settings)) as restarted:
            status = restarted.get(f"/api/v1/prefect/tasks/{run.id}/deletion").json()
            assert status["deleted"] is False and status["status"] == "failed"
            response = restarted.post(f"/api/v1/prefect/tasks/{run.id}/delete",
                json={"request_id": str(uuid4()), "intent_seq": 2})
            assert response.status_code == 200 and response.json()["deleted"] is True
        assert (source / "paper.docx").read_bytes() == b"original"
        with get_client(sync_client=True) as prefect:
            assert prefect.read_artifacts(artifact_filter=ArtifactFilter(
                flow_run_id={"any_": [run.id]})) == []

def test_started_run_without_pid_is_unknown():
    from types import SimpleNamespace
    from prefect.states import Crashed
    from paperflow_v3.control.deletion import execution_active
    run = SimpleNamespace(id=uuid4(), infrastructure_pid=None, run_count=1, start_time=None, state=Crashed())
    with pytest.raises(RuntimeError, match="identity|process|execution"):
        execution_active(run)
