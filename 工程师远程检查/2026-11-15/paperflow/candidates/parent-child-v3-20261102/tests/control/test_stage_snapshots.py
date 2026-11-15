"""Stage results are persisted from synthetic receipts on an isolated Prefect server."""

import asyncio
import importlib.util
from contextlib import nullcontext
from uuid import UUID, uuid4

import pytest
from prefect import flow
from prefect.client.orchestration import get_client
from prefect.context import FlowRunContext
from prefect.states import Failed
from prefect.testing.utilities import prefect_test_harness

from paperflow_v3.control.receipts import read_receipts, save_receipt, save_receipt_sync


def test_finished_preprocessing_is_saved_without_a_detail_view():
    assert importlib.util.find_spec("paperflow_v3.control.snapshots") is not None, (
        "stage snapshots must be persisted and readable without a viewer"
    )
    from paperflow_v3.control.snapshots import load_task_snapshot

    @flow(persist_result=False, retries=0)
    async def paper(task_id, group_id, registered):
        child_id = FlowRunContext.get().flow_run.id
        data = {
            **registered,
            "attempt_id": str(child_id),
            "processing_attempt_id": "initial",
            "status": "preprocessing_complete",
        }
        await save_receipt(task_id=task_id, folder_id=group_id, child_flow_run_id=child_id,
                           event_id="preprocessing-started", kind="preprocessing_started", data=data)
        await save_receipt(task_id=task_id, folder_id=group_id, child_flow_run_id=child_id,
                           event_id="preprocessing-complete", kind="preprocessing_complete", data=data)
        return child_id

    @flow(persist_result=False, retries=0)
    async def parent():
        task_id = str(FlowRunContext.get().flow_run.id)
        registered = {"task_id": task_id, "group_id": "group-a", "paper_id": "paper-a",
                      "receipt_id": "receipt-a", "group": "folder-a", "paper_title": "synthetic"}
        child_id = await paper(task_id, "group-a", registered)
        async with get_client() as client:
            child = await client.read_flow_run(child_id)
        return task_id, child

    async def exercise():
        task_id, child = await parent()
        result = await load_task_snapshot(UUID(task_id), [child])
        originals = await read_receipts(child.id)
        return result, originals

    with prefect_test_harness(server_startup_timeout=60):
        result, originals = asyncio.run(exercise())

    assert [row["kind"] for row in originals] == ["preprocessing_complete", "preprocessing_started"]
    folder = result["folders"][0]
    stage = folder["stage_snapshots"]["preprocessing"]
    assert folder["preprocessing_result"] == "passed"
    assert stage["processing_attempt_id"] == "initial"
    assert stage["final"] is True and stage["pending_request_count"] == 0
    assert stage["recorded_at"]
    assert UUID(stage["result_version"])


@pytest.fixture(scope="module")
def isolated_server():
    with prefect_test_harness(server_startup_timeout=60):
        yield


async def make_ticket(events):
    @flow(persist_result=False, retries=0)
    async def paper(task_id, group_id, registered, events):
        child_id = FlowRunContext.get().flow_run.id
        for index, (kind, data) in enumerate(events):
            scope = data.get("_snapshot_scope")
            sync = data.get("_sync", False)
            if scope or sync:
                data = {key: value for key, value in data.items() if key not in {"_snapshot_scope", "_sync"}}
            if data.get("_with_identity"):
                data = {key: value for key, value in data.items() if key != "_with_identity"}
                data["identity"] = {**{key: registered[key] for key in
                                        ("task_id", "group_id", "paper_id", "receipt_id")},
                                    "attempt_id": str(child_id), "parent_run_id": task_id,
                                    "task_run_id": str(child_id)}
            if scope:
                from paperflow_v3.control.snapshots import processing_attempt
            with processing_attempt(scope) if scope else nullcontext():
                args = dict(task_id=task_id, folder_id=group_id, child_flow_run_id=child_id,
                            event_id=f"synthetic-{index}", kind=kind, data=data)
                if sync:
                    await asyncio.to_thread(save_receipt_sync, **args)
                else:
                    await save_receipt(**args)
        return child_id

    @flow(persist_result=False, retries=0)
    async def parent():
        task_id = str(FlowRunContext.get().flow_run.id)
        registered = {"task_id": task_id, "group_id": "group-a", "paper_id": "paper-a",
                      "receipt_id": "receipt-a", "group": "folder-a", "paper_title": "synthetic"}
        await save_receipt(task_id=task_id, folder_id="group-a", child_flow_run_id=UUID(task_id),
                           event_id="classification", kind="classification",
                           data={"group_id": "group-a", "group": "folder-a", "paper_title": "synthetic",
                                 "status": "passed", "registration_status": "registered",
                                 "delivery_complete": True})
        child_id = await paper(task_id, "group-a", registered, events)
        await save_receipt(task_id=task_id, folder_id="task", child_flow_run_id=UUID(task_id),
                           event_id="classification-complete", kind="classification_complete",
                           data={"group_count": 1, "group_ids": ["group-a"], "ungrouped": []})
        async with get_client() as client:
            child = await client.read_flow_run(child_id)
        return UUID(task_id), child

    return await parent()


def test_pause_reads_original_unknown_when_legacy_snapshot_has_no_coverage(isolated_server):
    from prefect.client.schemas.actions import ArtifactCreate
    from prefect.states import Running
    from paperflow_v3.control.snapshots import load_task_snapshot
    from paperflow_v3.control.stage_admission import pause_progress, _stage_rows

    async def exercise():
        task_id, child = await make_ticket([])
        async with get_client() as client:
            await client.create_artifact(ArtifactCreate(key='legacy-pause-unknown-' + uuid4().hex,
                type='paperflow-v3-receipt', flow_run_id=child.id,
                data={'task_id': str(task_id), 'folder_id': 'group-a', 'event_id': 'original-unknown',
                      'kind': 'llm_request_unknown', 'data': {'component_id': 'body', 'request_id': 'original'}}))
            await client.set_flow_run_state(child.id, Running(), force=True)
            child = await client.read_flow_run(child.id)
        before = await load_task_snapshot(task_id, [child], repair=False)
        assert before['folders'][0]['stage'] == 'waiting_preprocessing'
        assert str(child.id) in before['history_targets']
        assert await _stage_rows(task_id, pending_only=False) == []
        assert (await pause_progress(task_id, snapshot=before))['state'] == 'unresolved'
    asyncio.run(exercise())


def test_pause_keeps_legacy_local_completion_pending_unresolved(isolated_server, tmp_path, monkeypatch):
    import json
    from paperflow_prefect.settings import Settings
    from paperflow_v3.delivery.progress import FolderProgress
    from paperflow_v3.control.snapshots import load_task_snapshot
    from paperflow_v3.control.stage_admission import pause_progress

    async def exercise():
        task_id, child = await make_ticket([
            ('preprocessing_complete', {}),
            ('llm_folder_complete', {'status': 'raw_complete', 'ranking_complete': True}),
            ('postprocessing_complete', {'validation_status': 'passed'}),
        ])
        registered = child.parameters['registered']
        identity = {**{key: registered[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id')},
                    'task_run_id': str(child.id), 'parent_run_id': str(task_id), 'attempt_id': str(child.id)}
        data_root = tmp_path / 'isolated-data'
        config = tmp_path / 'isolated-config.json'
        config.write_text(json.dumps({'data_root': str(data_root)}), encoding='utf8')
        monkeypatch.setenv('PAPERFLOW_V3_PROJECT_CONFIG', str(config))
        FolderProgress(Settings({'data_root': str(data_root)}), identity, [], None).save(
            'folder_completion_pending', {'identity': identity, 'status': 'pending', 'stage': 'cleanup'})
        before = await load_task_snapshot(task_id, [child], repair=False)
        assert before['folders'][0]['stage'] == 'failed'
        assert before['folders'][0]['failure_detail'] == '完成结果尚未保存'
        assert (await pause_progress(task_id, snapshot=before))['state'] == 'unresolved'
    asyncio.run(exercise())


def test_folder_completions_use_one_artifact_each_and_project_without_saved_snapshots(isolated_server):
    from prefect.client.schemas.filters import ArtifactFilter
    from paperflow_v3.control.receipts import save_folder_completion, FOLDER_COMPLETION_TYPE
    from paperflow_v3.control.snapshots import load_task_snapshot

    async def exercise():
        task_id, child = await make_ticket([
            ('preprocessing_complete', {}),
            ('llm_folder_complete', {'status': 'raw_complete', 'ranking_complete': True}),
            ('postprocessing_complete', {'validation_status': 'passed'}),
        ])
        registered = child.parameters['registered']
        identity = {**{key: registered[key] for key in ('task_id', 'group_id', 'paper_id', 'receipt_id')},
                    'attempt_id': str(child.id), 'parent_run_id': str(task_id), 'task_run_id': str(child.id)}
        for kind in ('rag_complete', 'nas', 'cleanup'):
            data = {'identity': identity, 'status': 'complete', 'files': {'one': {'status': 'complete'}}}
            args = dict(task_id=task_id, folder_id='group-a', child_flow_run_id=child.id,
                        event_id='folder-' + kind + '-' + str(child.id), kind=kind, data=data)
            first = await asyncio.to_thread(save_folder_completion, **args)
            assert await asyncio.to_thread(save_folder_completion, **args) == first
        for repair in (False, True):
            snapshot = await load_task_snapshot(task_id, [child], repair=repair)
            folder = snapshot['folders'][0]
            assert folder['stage'] == 'completed'
            assert all(folder['stage_snapshots'][stage]['final'] for stage in ('rag', 'nas', 'cleanup'))
        async with get_client() as client:
            rows = await client.read_artifacts(artifact_filter=ArtifactFilter(
                flow_run_id={'any_': [child.id]}), limit=200)
        assert sum(row.type == FOLDER_COMPLETION_TYPE for row in rows) == 3
        assert all(set(row.data['data']) == {'identity', 'status'}
                   for row in rows if row.type == FOLDER_COMPLETION_TYPE)
        assert not any(row.type == 'paperflow-v3-stage-snapshot' and row.data.get('event_kind')
                       in {'rag_complete', 'nas', 'cleanup'} for row in rows)
        assert {row['kind'] for row in await read_receipts(child.id)} >= {'rag_complete', 'nas', 'cleanup'}

    asyncio.run(exercise())


def test_local_cleanup_success_is_not_projected_as_server_completion(isolated_server, monkeypatch):
    from datetime import datetime, timezone
    from paperflow_v3.control.snapshots import load_task_snapshot
    from paperflow_v3.delivery import progress

    record = {'task_id': str(uuid4()), 'folder_id': 'folder', 'kind': 'cleanup', 'event_id': 'local',
              'recorded_at': datetime.now(timezone.utc).isoformat(),
              'data': {'status': 'complete', 'files': {'one': {'status': 'complete'}}}}
    monkeypatch.setattr(progress, 'local_records', lambda _: [record])
    async def exercise():
        task_id, child = await make_ticket([('preprocessing_complete', {})])
        for repair in (False, True):
            snapshot = await load_task_snapshot(task_id, [child], repair=repair)
            assert 'cleanup' not in snapshot['folders'][0]['stage_snapshots']
            assert snapshot['folders'][0]['stage'] != 'completed'
    asyncio.run(exercise())


def test_delivery_save_failure_is_retryable_on_page_without_remote_intermediate(isolated_server, tmp_path, monkeypatch):
    import json
    from types import SimpleNamespace
    from paperflow_v3.control import flows
    from paperflow_v3.control.snapshots import load_task_snapshot
    from paperflow_v3.delivery.progress import FolderProgress

    settings = SimpleNamespace(path=lambda _: tmp_path / 'data',values={})
    config = tmp_path / 'config.json'
    config.write_text(json.dumps({'data_root': str(tmp_path / 'data')}), encoding='utf8')
    monkeypatch.setenv('PAPERFLOW_V3_PROJECT_CONFIG', str(config))
    async def nothing(*args):
        pass
    async def fail_save(identity, *args, **kwargs):
        state = FolderProgress(settings, identity, [], lambda **_: False)
        state.complete('rag_complete', {'identity': identity, 'status': 'complete'})
    monkeypatch.setattr(flows, '_delivery_step', fail_save)
    monkeypatch.setattr(flows, '_shared_dependency_failure', nothing)

    async def exercise():
        task_id, child = await make_ticket([
            ('preprocessing_complete', {}),
            ('llm_folder_complete', {'status': 'raw_complete', 'ranking_complete': True}),
            ('postprocessing_complete', {'validation_status': 'passed'}),
            ('stage_work_inventory', {'_with_identity': True}),
        ])
        from paperflow_v3.control.api import _folder_identity
        identity = _folder_identity(child)
        with pytest.raises(ValueError, match='not saved'):
            await flows.run_paper(identity, child.parameters['registered'], settings,
                                  SimpleNamespace(wait=nothing), {}, tmp_path)
        async with get_client() as client:
            await client.set_flow_run_state(child.id, Failed(message='synthetic final-save failure'), force=True)
            child = await client.read_flow_run(child.id)
        for repair in (False, True):
            folder = (await load_task_snapshot(task_id, [child], repair=repair))['folders'][0]
            assert folder['stage'] == 'failed' and folder['failed_stage'] == 'rag'
            assert folder['retryable'] is True, folder
        assert not any(row['kind'] in {'child_error', 'rag_complete', 'folder_completion_pending'}
                       for row in await read_receipts(child.id))
    asyncio.run(exercise())


def test_failed_and_unknown_requests_keep_the_llm_stage_unsealed(isolated_server):
    from paperflow_v3.control.snapshots import load_task_snapshot

    async def exercise():
        task_id, child = await make_ticket([
            ("preprocessing_complete", {"processing_attempt_id": "initial"}),
            ("llm_request_failed", {"component_id": "body", "request_id": "r1", "kind": "body",
                                    "failure_reason": "http_rejection", "retryable": True}),
            ("llm_request_unknown", {"component_id": "attachment", "request_id": "r2",
                                     "kind": "attachment"}),
        ])
        return await load_task_snapshot(task_id, [child])

    result = asyncio.run(exercise())
    stage = result["folders"][0]["stage_snapshots"]["llm"]
    assert stage["result"] == "failed"
    assert stage["final"] is False and stage["pending_request_count"] == 1
    assert result["has_pending"] is True


def test_late_old_preprocessing_result_cannot_replace_new_retry(isolated_server):
    from paperflow_v3.control.snapshots import load_task_snapshot

    async def exercise():
        task_id, child = await make_ticket([
            ("preprocessing_failed", {"processing_attempt_id": "initial", "failure_reason": "local_io"}),
            ("manual_retry", {"retry_id": "retry-2", "stage": "preprocessing", "state": "authorized"}),
            ("preprocessing_started", {"processing_attempt_id": "retry-2"}),
            ("preprocessing_complete", {"processing_attempt_id": "initial"}),
        ])
        return await load_task_snapshot(task_id, [child])

    folder = asyncio.run(exercise())["folders"][0]
    stage = folder["stage_snapshots"]["preprocessing"]
    assert stage["processing_attempt_id"] == "retry-2"
    assert stage["final"] is False and folder["preprocessing_result"] is None


def test_task_summaries_read_only_saved_results(isolated_server):
    from paperflow_v3.control import snapshots

    assert hasattr(snapshots, "read_task_summaries"), "saved task summaries must be batch-readable"

    async def exercise():
        task_id, _child = await make_ticket([])
        missing_id = uuid4()
        return task_id, missing_id, await snapshots.read_task_summaries([task_id, missing_id])

    task_id, missing_id, summaries = asyncio.run(exercise())
    assert summaries[str(task_id)]["task_id"] == str(task_id)
    assert str(missing_id) not in summaries


def test_snapshot_save_failure_repairs_only_the_missing_result(isolated_server, monkeypatch):
    from paperflow_v3.control import snapshots

    original_save = snapshots.save_receipt_snapshot

    async def fail_finished_snapshot(client, receipt, source):
        if receipt["kind"] == "preprocessing_complete":
            raise RuntimeError("synthetic snapshot storage failure")
        return await original_save(client, receipt, source)

    async def exercise():
        monkeypatch.setattr(snapshots, "save_receipt_snapshot", fail_finished_snapshot)
        task_id, child = await make_ticket([
            ("preprocessing_started", {"processing_attempt_id": "initial"}),
            ("preprocessing_complete", {"processing_attempt_id": "initial"}),
        ])
        before = await snapshots.load_task_snapshot(task_id, [child], repair=False)
        original_rows = await read_receipts(child.id)
        monkeypatch.setattr(snapshots, "save_receipt_snapshot", original_save)
        repaired = await snapshots.load_task_snapshot(task_id, [child], repair=True)
        after_rows = await read_receipts(child.id)
        return before, repaired, original_rows, after_rows

    before, repaired, originals, after = asyncio.run(exercise())
    assert before["folders"][0]["stage_snapshots"]["preprocessing"]["final"] is False
    assert repaired["folders"][0]["stage_snapshots"]["preprocessing"]["final"] is True
    assert repaired["folders"][0]["preprocessing_result"] == "passed"
    assert originals == after


def test_completed_stages_and_classification_are_read_from_persisted_snapshots(isolated_server):
    from paperflow_v3.control.snapshots import load_task_snapshot

    async def exercise():
        task_id, child = await make_ticket([
            ("preprocessing_complete", {"processing_attempt_id": "initial"}),
            ("llm_folder_complete", {"status": "raw_complete", "ranking_complete": True}),
            ("postprocessing_complete", {"validation_status": "passed", "_with_identity": True}),
            ("delivery_stage", {"stage": "waiting_nas_archive", "_with_identity": True}),
            ("nas", {"status": "complete", "files": {"file": {"status": "complete"}},
                     "_with_identity": True}),
            ("cleanup", {"status": "complete", "files": {"file": {"status": "complete"}},
                         "_with_identity": True}),
        ])
        return await load_task_snapshot(task_id, [child])

    result = asyncio.run(exercise())
    folder = result["folders"][0]
    assert result["items"][0]["group_id"] == "group-a"
    assert folder["stage"] == "completed"
    for name in ("preprocessing", "llm", "postprocessing", "rag", "nas", "cleanup"):
        assert folder["stage_snapshots"][name]["final"] is True


def test_attempt_identity_reaches_async_and_threaded_receipts_without_changing_originals(isolated_server):
    from paperflow_v3.control import snapshots

    assert hasattr(snapshots, "processing_attempt"), "retry identity must reach stage and delivery writers"

    async def exercise():
        task_id, child = await make_ticket([
            ("preprocessing_complete", {"processing_attempt_id": "initial"}),
            ("llm_request_failed", {"component_id": "body", "request_id": "old", "kind": "body",
                                    "failure_reason": "http_rejection"}),
            ("manual_retry", {"retry_id": "retry-2", "stage": "llm", "state": "authorized"}),
            ("llm_folder_complete", {"status": "raw_complete", "ranking_complete": True,
                                     "_snapshot_scope": "retry-2"}),
            ("postprocessing_complete", {"validation_status": "passed", "_with_identity": True,
                                         "_snapshot_scope": "retry-2"}),
            ("nas", {"status": "complete", "files": {"x": {"status": "complete"}},
                     "_with_identity": True, "_snapshot_scope": "retry-2", "_sync": True}),
        ])
        return await snapshots.load_task_snapshot(task_id, [child]), await read_receipts(child.id)

    result, originals = asyncio.run(exercise())
    stages = result["folders"][0]["stage_snapshots"]
    assert stages["preprocessing"]["processing_attempt_id"] == "initial"
    for name in ("llm", "postprocessing", "nas"):
        assert stages[name]["processing_attempt_id"] == "retry-2" and stages[name]["final"] is True
    nas = next(row for row in originals if row["kind"] == "nas")
    assert "processing_attempt_id" not in nas["data"]
    assert nas["processing_attempt_id"] == "retry-2"


def test_retry_snapshot_gap_repairs_using_identity_on_original_receipt(isolated_server, monkeypatch):
    from paperflow_v3.control import snapshots

    assert hasattr(snapshots, "processing_attempt"), "retry identity must survive a missed snapshot"
    original_save = snapshots.save_receipt_snapshot

    async def fail_retry_result(client, receipt, source):
        if receipt["kind"] == "postprocessing_complete":
            raise RuntimeError("synthetic missed retry result")
        return await original_save(client, receipt, source)

    async def exercise():
        monkeypatch.setattr(snapshots, "save_receipt_snapshot", fail_retry_result)
        task_id, child = await make_ticket([
            ("manual_retry", {"retry_id": "retry-2", "stage": "postprocessing", "state": "authorized"}),
            ("postprocessing_complete", {"validation_status": "passed", "_with_identity": True,
                                         "_snapshot_scope": "retry-2"}),
        ])
        monkeypatch.setattr(snapshots, "save_receipt_snapshot", original_save)
        result = await snapshots.load_task_snapshot(task_id, [child], repair=True)
        return result, await read_receipts(child.id)

    result, originals = asyncio.run(exercise())
    stage = result["folders"][0]["stage_snapshots"]["postprocessing"]
    assert stage["processing_attempt_id"] == "retry-2" and stage["final"] is True
    assert next(row for row in originals if row["kind"] == "postprocessing_complete")["processing_attempt_id"] == "retry-2"


def test_original_unknown_blocks_final_even_if_its_snapshot_write_failed(isolated_server, monkeypatch):
    from paperflow_v3.control import snapshots

    original_save = snapshots.save_receipt_snapshot

    async def fail_unknown(client, receipt, source):
        if receipt["kind"] == "llm_request_unknown":
            raise RuntimeError("synthetic missed progress snapshot")
        return await original_save(client, receipt, source)

    async def exercise():
        monkeypatch.setattr(snapshots, "save_receipt_snapshot", fail_unknown)
        task_id, child = await make_ticket([
            ("llm_request_failed", {"component_id": "body", "request_id": "r1", "kind": "body",
                                    "failure_reason": "http_rejection"}),
            ("llm_request_unknown", {"component_id": "attachment", "request_id": "r2", "kind": "attachment"}),
            ("llm_folder_complete", {"status": "raw_complete", "ranking_complete": True}),
        ])
        before = await snapshots.load_task_snapshot(task_id, [child])
        monkeypatch.setattr(snapshots, "save_receipt_snapshot", original_save)
        after = await snapshots.load_task_snapshot(task_id, [child], repair=True)
        return before, after

    before, after = asyncio.run(exercise())
    stage = before["folders"][0]["stage_snapshots"]["llm"]
    assert stage["final"] is False and stage["pending_request_count"] is None
    stage = after["folders"][0]["stage_snapshots"]["llm"]
    assert stage["final"] is False and stage["pending_request_count"] is None


def test_confirmed_failed_stage_can_be_final_without_unknown_requests(isolated_server):
    from paperflow_v3.control.snapshots import load_task_snapshot

    async def exercise():
        task_id, child = await make_ticket([
            ("preprocessing_complete", {"processing_attempt_id": "initial"}),
            ("llm_folder_complete", {"status": "raw_complete", "ranking_complete": True}),
            ("postprocessing_component_failed", {"component_id": "body", "attempt_id": "initial",
                                                 "reason": "synthetic failure"}),
            ("child_error", {"stage": "processing", "reason": "synthetic failure"}),
        ])
        return await load_task_snapshot(task_id, [child])

    result = asyncio.run(exercise())
    stage = result["folders"][0]["stage_snapshots"]["postprocessing"]
    assert stage["result"] == "failed" and stage["final"] is True
    assert stage["pending_request_count"] == 0


def test_confirmed_preprocessing_failure_keeps_existing_retry_action(isolated_server):
    from paperflow_v3.control.snapshots import load_task_snapshot

    async def exercise():
        task_id, child = await make_ticket([
            ("preprocessing_failed", {"processing_attempt_id": "initial", "failure_reason": "local_io",
                                      "retryable": True, "reason": "synthetic"}),
        ])
        child = child.model_copy(update={"state": Failed()})
        return await load_task_snapshot(task_id, [child])

    folder = asyncio.run(exercise())["folders"][0]
    assert folder["stage"] == "failed" and folder["retryable"] is True


def test_unknown_child_dispatch_does_not_claim_complete_coverage(isolated_server):
    from paperflow_v3.control.snapshots import load_task_snapshot

    async def exercise():
        task_id, child = await make_ticket([])
        await save_receipt(task_id=task_id, folder_id="group-b", child_flow_run_id=task_id,
                           event_id="group-b-classification", kind="classification",
                           data={"group_id": "group-b", "status": "passed", "registration_status": "registered"})
        await save_receipt(task_id=task_id, folder_id="group-b", child_flow_run_id=task_id,
                           event_id="group-b-dispatch-unknown", kind="child_dispatch", data={"state": "unknown"})
        await save_receipt(task_id=task_id, folder_id="task", child_flow_run_id=task_id,
                           event_id="classification-complete-2", kind="classification_complete",
                           data={"group_count": 2, "group_ids": ["group-a", "group-b"], "ungrouped": []})
        return await load_task_snapshot(task_id, [child])

    result = asyncio.run(exercise())
    assert result["complete"] is False and result["has_pending"] is True


def test_fully_final_paper_does_not_rescan_original_receipts(isolated_server, monkeypatch):
    from paperflow_v3.control import snapshots

    async def exercise():
        task_id, child = await make_ticket([
            ("preprocessing_complete", {"processing_attempt_id": "initial"}),
            ("llm_folder_complete", {"status": "raw_complete", "ranking_complete": True}),
            ("postprocessing_complete", {"validation_status": "passed", "_with_identity": True}),
            ("delivery_stage", {"stage": "waiting_nas_archive", "_with_identity": True}),
            ("nas", {"status": "complete", "files": {"file": {"status": "complete"}}, "_with_identity": True}),
            ("cleanup", {"status": "complete", "files": {"file": {"status": "complete"}}, "_with_identity": True}),
        ])
        before = await snapshots.load_task_snapshot(task_id, [child])

        async def forbidden(*_args, **_kwargs):
            raise AssertionError("a final paper must not read receipt history")

        monkeypatch.setattr(snapshots, "_originals", forbidden)
        after = await snapshots.load_task_snapshot(task_id, [child], repair=True)
        return before, after

    before, after = asyncio.run(exercise())
    assert after["folders"][0]["stage_snapshots"] == before["folders"][0]["stage_snapshots"]
    assert after["as_of"] == before["as_of"]


def test_repair_does_not_persist_final_when_original_request_is_unknown(isolated_server, monkeypatch):
    from prefect.client.schemas.filters import ArtifactCollectionFilter
    from paperflow_v3.control import snapshots

    original_save = snapshots.save_receipt_snapshot

    async def miss_snapshots(client, receipt, source):
        if receipt["kind"] in {"llm_request_unknown", "llm_folder_complete"}:
            raise RuntimeError("synthetic snapshot failure")
        return await original_save(client, receipt, source)

    async def exercise():
        monkeypatch.setattr(snapshots, "save_receipt_snapshot", miss_snapshots)
        task_id, child = await make_ticket([
            ("llm_request_unknown", {"component_id": "body", "request_id": "r1", "kind": "body"}),
            ("llm_folder_complete", {"status": "raw_complete", "ranking_complete": True}),
        ])
        monkeypatch.setattr(snapshots, "save_receipt_snapshot", original_save)
        result = await snapshots.load_task_snapshot(task_id, [child], repair=True)
        async with get_client() as client:
            rows = await client.read_latest_artifacts(artifact_filter=ArtifactCollectionFilter(
                flow_run_id={"any_": [child.id]}, type={"any_": [snapshots.SNAPSHOT_TYPE]}), limit=200)
        return result, rows

    result, rows = asyncio.run(exercise())
    assert result["folders"][0]["stage_snapshots"]["llm"]["final"] is False
    assert all(not row.data["final"] for row in rows if row.data["event_kind"] == "llm_folder_complete")


def test_classification_completion_does_not_wait_for_later_processing(isolated_server):
    from paperflow_v3.control.snapshots import load_task_snapshot

    async def exercise():
        task_id, child = await make_ticket([
            ("preprocessing_started", {"processing_attempt_id": "initial"}),
        ])
        return await load_task_snapshot(task_id, [child])

    result = asyncio.run(exercise())
    assert result["classification_complete"] is True
    assert result["has_pending"] is True


@pytest.mark.parametrize("status,registration_status", [("unresolved", None), ("passed", None)])
def test_classification_not_finished_until_status_and_registration_are_known(isolated_server, status, registration_status):
    from paperflow_v3.control.snapshots import load_task_snapshot

    async def exercise():
        task_id, child = await make_ticket([])
        await save_receipt(task_id=task_id, folder_id="group-a", child_flow_run_id=task_id,
                           event_id="classification-later", kind="classification",
                           data={"group_id": "group-a", "status": status,
                                 "registration_status": registration_status})
        return await load_task_snapshot(task_id, [child])

    result = asyncio.run(exercise())
    assert result["classification_complete"] is False


def test_missing_registered_child_does_not_claim_full_task_coverage(isolated_server):
    from paperflow_v3.control.snapshots import load_task_snapshot

    async def exercise():
        task_id, child = await make_ticket([])
        await save_receipt(task_id=task_id, folder_id="group-b", child_flow_run_id=task_id,
                           event_id="classification-b", kind="classification",
                           data={"group_id": "group-b", "status": "passed", "registration_status": "registered"})
        await save_receipt(task_id=task_id, folder_id="task", child_flow_run_id=task_id,
                           event_id="classification-complete-b", kind="classification_complete",
                           data={"group_count": 2, "group_ids": ["group-a", "group-b"], "ungrouped": []})
        return await load_task_snapshot(task_id, [child])

    result = asyncio.run(exercise())
    assert result["classification_complete"] is True
    assert result["complete"] is False and result["has_pending"] is True


def test_partial_parent_history_is_repaired_without_rewriting_existing_classification(isolated_server, monkeypatch):
    from paperflow_v3.control import snapshots

    original_save = snapshots.save_receipt_snapshot

    async def miss_one_classification(client, receipt, source):
        if receipt["kind"] == "classification" and receipt["folder_id"] == "group-b":
            raise RuntimeError("synthetic missed classification snapshot")
        return await original_save(client, receipt, source)

    async def exercise():
        task_id, child = await make_ticket([])
        before = await snapshots.load_task_snapshot(task_id, [child])
        monkeypatch.setattr(snapshots, "save_receipt_snapshot", miss_one_classification)
        await save_receipt(task_id=task_id, folder_id="group-b", child_flow_run_id=task_id,
                           event_id="classification-b", kind="classification",
                           data={"group_id": "group-b", "status": "failed"})
        await save_receipt(task_id=task_id, folder_id="task", child_flow_run_id=task_id,
                           event_id="classification-complete-b", kind="classification_complete",
                           data={"group_count": 2, "group_ids": ["group-a", "group-b"], "ungrouped": []})
        monkeypatch.setattr(snapshots, "save_receipt_snapshot", original_save)
        after = await snapshots.load_task_snapshot(task_id, [child], repair=True)
        return before, after

    before, after = asyncio.run(exercise())
    assert set(item["group_id"] for item in after["items"]) == {"group-a", "group-b"}
    assert after["classification_complete"] is True
    assert next(item for item in after["items"] if item["group_id"] == "group-a") == before["items"][0]


def test_initial_processing_attempt_preserves_explicit_nondefault_identity(isolated_server):
    from paperflow_v3.control.snapshots import load_task_snapshot

    async def exercise():
        task_id, child = await make_ticket([
            ("preprocessing_complete", {"processing_attempt_id": "initial-child-123"}),
            ("llm_folder_complete", {"processing_attempt_id": "initial-child-123",
                                     "status": "raw_complete", "ranking_complete": True}),
        ])
        return await load_task_snapshot(task_id, [child])

    stages = asyncio.run(exercise())["folders"][0]["stage_snapshots"]
    assert stages["preprocessing"]["processing_attempt_id"] == "initial-child-123"
    assert stages["llm"]["processing_attempt_id"] == "initial-child-123"
    assert stages["llm"]["final"] is True


@pytest.mark.parametrize("error_type", ["TimeoutError", "ConnectionError"])
def test_network_error_does_not_seal_a_failed_stage(isolated_server, error_type):
    from paperflow_v3.control.snapshots import load_task_snapshot

    async def exercise():
        task_id, child = await make_ticket([
            ("preprocessing_complete", {"processing_attempt_id": "initial"}),
            ("llm_folder_complete", {"status": "raw_complete", "ranking_complete": True}),
            ("postprocessing_component_failed", {"component_id": "body", "attempt_id": "initial",
                                                 "error_type": error_type, "reason": "synthetic network error"}),
            ("child_error", {"stage": "processing", "reason": "synthetic network error"}),
        ])
        return await load_task_snapshot(task_id, [child])

    result = asyncio.run(exercise())
    stage = result["folders"][0]["stage_snapshots"]["postprocessing"]
    assert stage["final"] is False and stage["pending_request_count"] is None
    assert result["has_pending"] is True


def test_rag_effects_keep_each_unknown_request_separate(isolated_server):
    from paperflow_v3.control.snapshots import load_task_snapshot

    async def exercise():
        task_id, child = await make_ticket([
            ("rag_effect_intent", {"effect_id": "effect-a"}),
            ("rag_effect_intent", {"effect_id": "effect-b"}),
            ("rag_effect_returned", {"effect_id": "effect-b"}),
        ])
        return await load_task_snapshot(task_id, [child])

    result = asyncio.run(exercise())
    stage = result["folders"][0]["stage_snapshots"]["rag"]
    assert stage["final"] is False and stage["pending_request_count"] == 1
    assert result["has_pending"] is True


def test_late_progress_cannot_replace_completed_llm_attempt(isolated_server):
    from paperflow_v3.control import receipts, snapshots

    async def exercise():
        task_id, child = await make_ticket([])
        record = receipts._artifact(task_id=task_id, folder_id="group-a", child_flow_run_id=child.id,
            event_id="old-start", kind="llm_request_started",
            data={"component_id": "body", "request_id": "old", "kind": "body"})
        record = record.model_copy(update={"key": "paperflow-v3-legacy-old-start"})
        async with get_client() as client:
            source = await client.create_artifact(record)
        await save_receipt(task_id=task_id, folder_id="group-a", child_flow_run_id=child.id,
            event_id="llm-final", kind="llm_folder_complete",
            data={"status": "raw_complete", "ranking_complete": True})
        async with get_client() as client:
            await snapshots.save_receipt_snapshot(client, record.data, source)
        return await snapshots.load_task_snapshot(task_id, [child])

    stage = asyncio.run(exercise())["folders"][0]["stage_snapshots"]["llm"]
    assert stage["final"] is True and stage["result"] == "passed"


def test_saved_requests_do_not_fill_the_active_snapshot_query(isolated_server):
    from paperflow_v3.control import snapshots

    events = []
    for index in range(5):
        data = {"component_id": f"body-{index}", "request_id": f"request-{index}", "kind": "body"}
        events.extend([("llm_request_intent", data), ("llm_response_saved", data)])
    events.append(("llm_request_unknown", {"component_id": "body", "request_id": "unknown", "kind": "body"}))

    async def exercise():
        task_id, child = await make_ticket(events)
        async with get_client() as client:
            rows = await snapshots._collections(client, [child.id], [snapshots.SNAPSHOT_TYPE])
        return await snapshots.load_task_snapshot(task_id, [child]), rows, await read_receipts(child.id)

    result, rows, originals = asyncio.run(exercise())
    llm_rows = [row for row in rows if row.data["stage"] == "llm"]
    assert [row.data["event_kind"] for row in llm_rows] == ["llm_request_unknown"]
    assert result["folders"][0]["stage_snapshots"]["llm"]["pending_request_count"] == 1
    assert len(originals) == len(events)


def test_repair_does_not_create_progress_for_already_saved_requests(isolated_server, monkeypatch):
    from paperflow_v3.control import snapshots

    original_save = snapshots.save_receipt_snapshot

    async def miss_requests(client, receipt, source):
        if receipt["kind"].startswith("llm_"):
            raise RuntimeError("synthetic missed snapshot")
        return await original_save(client, receipt, source)

    async def exercise():
        monkeypatch.setattr(snapshots, "save_receipt_snapshot", miss_requests)
        task_id, child = await make_ticket([
            ("llm_request_intent", {"component_id": "body", "request_id": "saved", "kind": "body"}),
            ("llm_response_saved", {"component_id": "body", "request_id": "saved", "kind": "body"}),
            ("llm_request_unknown", {"component_id": "attachment", "request_id": "unknown", "kind": "attachment"}),
        ])
        monkeypatch.setattr(snapshots, "save_receipt_snapshot", original_save)
        result = await snapshots.load_task_snapshot(task_id, [child], repair=True)
        async with get_client() as client:
            rows = await snapshots._collections(client, [child.id], [snapshots.SNAPSHOT_TYPE])
        return result, rows

    result, rows = asyncio.run(exercise())
    assert [row.data["event_kind"] for row in rows if row.data["stage"] == "llm"] == ["llm_request_unknown"]
    assert result["folders"][0]["stage_snapshots"]["llm"]["pending_request_count"] == 1


def test_parent_classification_and_registration_results_are_on_the_child_view(isolated_server):
    from paperflow_v3.control.snapshots import load_task_snapshot

    async def exercise():
        task_id, child = await make_ticket([])
        await save_receipt(task_id=task_id, folder_id="group-a", child_flow_run_id=task_id,
                           event_id="registration", kind="registration",
                           data={"group_id": "group-a", "registration_status": "registered"})
        return await load_task_snapshot(task_id, [child])

    stages = asyncio.run(exercise())["folders"][0]["stage_snapshots"]
    assert stages["classification"]["final"] is True
    assert stages["registration"]["final"] is True


def test_active_multistage_history_is_scanned_only_on_first_repair(isolated_server, monkeypatch):
    from paperflow_v3.control import snapshots

    async def exercise():
        task_id, child = await make_ticket([
            ("preprocessing_started", {"processing_attempt_id": "initial"}),
            ("preprocessing_complete", {"processing_attempt_id": "initial"}),
            ("llm_request_intent", {"component_id": "body", "request_id": "saved", "kind": "body"}),
            ("llm_response_saved", {"component_id": "body", "request_id": "saved", "kind": "body"}),
            ("llm_request_unknown", {"component_id": "attachment", "request_id": "active", "kind": "attachment"}),
        ])
        calls = []
        original = snapshots._originals

        async def count(client, run_ids):
            calls.append(tuple(run_ids))
            return await original(client, run_ids)

        monkeypatch.setattr(snapshots, "_originals", count)
        first = await snapshots.load_task_snapshot(task_id, [child], repair=True)
        first_count = len(calls)
        second = await snapshots.load_task_snapshot(task_id, [child], repair=True)
        return child, first, second, calls, first_count

    child, first, second, calls, first_count = asyncio.run(exercise())
    assert first_count <= 1 and len(calls) == first_count
    assert first["folders"][0]["stage_snapshots"]["preprocessing"]["final"] is True
    assert second["folders"][0]["stage_snapshots"]["llm"]["pending_request_count"] == 1
    assert not any(child.id in run_ids for run_ids in calls[first_count:])


def test_snapshot_gap_after_history_repair_fetches_only_its_original(isolated_server, monkeypatch):
    from paperflow_v3.control import snapshots

    async def exercise():
        task_id, child = await make_ticket([
            ("preprocessing_complete", {"processing_attempt_id": "initial"}),
        ])
        await snapshots.load_task_snapshot(task_id, [child], repair=True)
        original_save = snapshots.save_receipt_snapshot

        async def miss_unknown(client, receipt, source):
            if receipt["kind"] == "llm_request_unknown":
                raise RuntimeError("synthetic missed snapshot")
            return await original_save(client, receipt, source)

        monkeypatch.setattr(snapshots, "save_receipt_snapshot", miss_unknown)
        source_id = await save_receipt(task_id=task_id, folder_id="group-a", child_flow_run_id=child.id,
                                       event_id="late-unknown", kind="llm_request_unknown",
                                       data={"component_id": "body", "request_id": "late", "kind": "body"})
        monkeypatch.setattr(snapshots, "save_receipt_snapshot", original_save)

        async def forbidden(*_args, **_kwargs):
            raise AssertionError("repairing one missed event must not rescan paper history")

        monkeypatch.setattr(snapshots, "_originals", forbidden)
        result = await snapshots.load_task_snapshot(task_id, [child], repair=True)
        return source_id, result

    source_id, result = asyncio.run(exercise())
    stage = result["folders"][0]["stage_snapshots"]["llm"]
    assert stage["pending_request_count"] == 1
    assert stage["result_version"] and UUID(str(source_id))
    assert result["history_targets"] == []


def test_stage_completion_checks_active_snapshots_without_reading_old_receipts(isolated_server, monkeypatch):
    from paperflow_v3.control import snapshots

    async def exercise():
        task_id, child = await make_ticket([
            ("preprocessing_complete", {"processing_attempt_id": "initial"}),
            ("llm_request_intent", {"component_id": "body", "request_id": "one", "kind": "body"}),
            ("llm_response_saved", {"component_id": "body", "request_id": "one", "kind": "body"}),
            ("rag_effect_intent", {"effect_id": "rag-one"}),
            ("rag_effect_returned", {"effect_id": "rag-one"}),
        ])

        async def forbidden(*_args, **_kwargs):
            raise AssertionError("stage completion must not rescan finished receipt history")

        monkeypatch.setattr(snapshots, "_originals", forbidden)
        for event_id, kind, data in [
            ("llm-final", "llm_folder_complete", {"status": "raw_complete", "ranking_complete": True}),
            ("rag-final", "delivery_stage", {"stage": "waiting_nas_archive"}),
        ]:
            await save_receipt(task_id=task_id, folder_id="group-a", child_flow_run_id=child.id,
                               event_id=event_id, kind=kind, data=data)
        return await snapshots.load_task_snapshot(task_id, [child])

    stages = asyncio.run(exercise())["folders"][0]["stage_snapshots"]
    assert stages["llm"]["final"] is True
    assert stages["rag"]["final"] is True


def test_failure_completion_checks_active_snapshots_without_reading_old_receipts(isolated_server, monkeypatch):
    from paperflow_v3.control import snapshots

    async def exercise():
        task_id, child = await make_ticket([
            ("preprocessing_complete", {"processing_attempt_id": "initial"}),
            ("llm_folder_complete", {"status": "raw_complete", "ranking_complete": True}),
            ("postprocessing_component_failed", {"component_id": "body", "attempt_id": "initial",
                                                 "reason": "synthetic failure"}),
        ])

        async def forbidden(*_args, **_kwargs):
            raise AssertionError("failure completion must not rescan finished receipt history")

        monkeypatch.setattr(snapshots, "_originals", forbidden)
        await save_receipt(task_id=task_id, folder_id="group-a", child_flow_run_id=child.id,
                           event_id="child-error", kind="child_error", data={"stage": "processing"})
        return await snapshots.load_task_snapshot(task_id, [child])

    stage = asyncio.run(exercise())["folders"][0]["stage_snapshots"]["postprocessing"]
    assert stage["final"] is True and stage["result"] == "failed"


def test_classification_items_keep_existing_registration_fields(isolated_server):
    from paperflow_v3.control.snapshots import load_task_snapshot

    async def exercise():
        task_id, child = await make_ticket([])
        return await load_task_snapshot(task_id, [child])

    item = asyncio.run(exercise())["items"][0]
    assert item["registration_status"] == "registered"
    assert item["delivery_complete"] is True


def test_dependency_summary_merges_with_classification_and_clears_reason(isolated_server):
    from paperflow_v3.control.snapshots import read_task_summaries

    async def exercise():
        task_id, _child = await make_ticket([])
        await save_receipt(task_id=task_id, folder_id="task", child_flow_run_id=task_id,
                           event_id="dependency-active", kind="dependency_failure",
                           data={"status": "active", "reason": "synthetic dependency"})
        active = (await read_task_summaries([task_id]))[str(task_id)]
        await save_receipt(task_id=task_id, folder_id="task", child_flow_run_id=task_id,
                           event_id="dependency-cleared", kind="dependency_failure", data={"status": "cleared"})
        cleared = (await read_task_summaries([task_id]))[str(task_id)]
        return active, cleared

    active, cleared = asyncio.run(exercise())
    assert active["stop_reason"] == "synthetic dependency" and active["group_count"] == 1
    assert cleared["stop_reason"] is None and cleared["group_count"] == 1
    assert cleared["as_of"] >= active["as_of"]


def test_targeted_repair_does_not_finalize_over_an_existing_unknown(isolated_server, monkeypatch):
    from paperflow_v3.control import snapshots

    async def exercise():
        task_id, child = await make_ticket([
            ("preprocessing_complete", {"processing_attempt_id": "initial"}),
            ("llm_request_unknown", {"component_id": "body", "request_id": "unknown", "kind": "body"}),
        ])
        await snapshots.load_task_snapshot(task_id, [child], repair=True)
        original_save = snapshots.save_receipt_snapshot

        async def miss_complete(client, receipt, source):
            if receipt["kind"] == "llm_folder_complete":
                raise RuntimeError("synthetic missed final snapshot")
            return await original_save(client, receipt, source)

        monkeypatch.setattr(snapshots, "save_receipt_snapshot", miss_complete)
        await save_receipt(task_id=task_id, folder_id="group-a", child_flow_run_id=child.id,
                           event_id="missed-final", kind="llm_folder_complete",
                           data={"status": "raw_complete", "ranking_complete": True})
        monkeypatch.setattr(snapshots, "save_receipt_snapshot", original_save)
        return await snapshots.load_task_snapshot(task_id, [child], repair=True)

    stage = asyncio.run(exercise())["folders"][0]["stage_snapshots"]["llm"]
    assert stage["final"] is False and stage["pending_request_count"] is None


def test_sync_stage_completion_does_not_read_old_receipts(isolated_server, monkeypatch):
    from paperflow_v3.control import snapshots

    async def exercise():
        task_id, child = await make_ticket([
            ("preprocessing_complete", {"processing_attempt_id": "initial"}),
            ("rag_effect_intent", {"effect_id": "effect-a"}),
            ("rag_effect_returned", {"effect_id": "effect-a"}),
        ])

        def forbidden(*_args, **_kwargs):
            raise AssertionError("sync stage completion must not rescan receipt history")

        with get_client(sync_client=True) as client:
            monkeypatch.setattr(type(client), "read_artifacts", forbidden)
        await asyncio.to_thread(save_receipt_sync, task_id=task_id, folder_id="group-a", child_flow_run_id=child.id,
                                event_id="rag-final-sync", kind="delivery_stage",
                                data={"stage": "waiting_nas_archive"})
        return await snapshots.load_task_snapshot(task_id, [child])

    stage = asyncio.run(exercise())["folders"][0]["stage_snapshots"]["rag"]
    assert stage["final"] is True


def test_new_postprocessing_component_keeps_the_surrounding_retry_identity(isolated_server):
    from paperflow_v3.control.snapshots import load_task_snapshot

    async def exercise():
        task_id, child = await make_ticket([
            ("manual_retry", {"retry_id": "retry-2", "stage": "llm", "state": "authorized"}),
            ("postprocessing_component_started", {"component_id": "body", "attempt_id": "initial",
                                                  "_snapshot_scope": "retry-2"}),
        ])
        return await load_task_snapshot(task_id, [child])

    stage = asyncio.run(exercise())["folders"][0]["stage_snapshots"]["postprocessing"]
    assert stage["processing_attempt_id"] == "retry-2"


@pytest.mark.parametrize("with_prior_intent", [False, True])
def test_newly_discovered_unknown_overrides_only_current_attempts_finality(isolated_server, with_prior_intent):
    from paperflow_v3.control.snapshots import load_task_snapshot

    async def exercise():
        events = [("llm_folder_complete", {"status": "raw_complete", "ranking_complete": True})]
        if with_prior_intent:
            events.append(("llm_request_intent", {"component_id": "attachment", "request_id": "late",
                                                   "kind": "attachment"}))
        events.append(("llm_request_unknown", {"component_id": "attachment", "request_id": "late",
                                               "kind": "attachment"}))
        task_id, child = await make_ticket(events)
        return await load_task_snapshot(task_id, [child])

    result = asyncio.run(exercise())
    stage = result["folders"][0]["stage_snapshots"]["llm"]
    assert stage["final"] is False and stage["pending_request_count"] == 1
    assert result["has_pending"] is True


@pytest.mark.parametrize("use_sync", [False, True])
def test_double_snapshot_write_failure_leaves_unknown_discoverable_without_history_rescan(
        isolated_server, monkeypatch, use_sync):
    from paperflow_v3.control import snapshots

    async def exercise():
        task_id, child = await make_ticket([
            ("preprocessing_complete", {"processing_attempt_id": "initial"}),
            ("llm_request_failed", {"component_id": "body", "request_id": "failed", "kind": "body",
                                    "failure_reason": "http_rejection"}),
        ])
        await snapshots.load_task_snapshot(task_id, [child], repair=True)
        original_async = snapshots.save_receipt_snapshot
        original_sync = snapshots.save_receipt_snapshot_sync
        original_gap = snapshots.snapshot_gap

        async def fail_async(client, receipt, source):
            if receipt["kind"] == "llm_request_unknown":
                raise RuntimeError("synthetic snapshot write failure")
            return await original_async(client, receipt, source)

        def fail_sync(client, receipt, source):
            if receipt["kind"] == "llm_request_unknown":
                raise RuntimeError("synthetic snapshot write failure")
            return original_sync(client, receipt, source)

        def fail_gap(*_args):
            raise RuntimeError("synthetic gap write failure")

        monkeypatch.setattr(snapshots, "save_receipt_snapshot", fail_async)
        monkeypatch.setattr(snapshots, "save_receipt_snapshot_sync", fail_sync)
        monkeypatch.setattr(snapshots, "snapshot_gap", fail_gap)
        args = dict(task_id=task_id, folder_id="group-a", child_flow_run_id=child.id,
                    event_id="missing-unknown", kind="llm_request_unknown",
                    data={"component_id": "attachment", "request_id": "unknown", "kind": "attachment"})
        source_id = (await asyncio.to_thread(save_receipt_sync, **args) if use_sync else
                     await save_receipt(**args))
        monkeypatch.setattr(snapshots, "save_receipt_snapshot", original_async)
        monkeypatch.setattr(snapshots, "save_receipt_snapshot_sync", original_sync)
        monkeypatch.setattr(snapshots, "snapshot_gap", original_gap)
        await save_receipt(task_id=task_id, folder_id="group-a", child_flow_run_id=child.id,
                           event_id="failed-child", kind="child_error", data={"stage": "processing"})
        before = await snapshots.load_task_snapshot(task_id, [child])

        async def forbidden(*_args, **_kwargs):
            raise AssertionError("a missed snapshot must not trigger a full paper history rescan")

        monkeypatch.setattr(snapshots, "_originals", forbidden)
        after = await snapshots.load_task_snapshot(task_id, [child], repair=True)
        originals = await read_receipts(child.id)
        return source_id, before, after, originals

    source_id, before, after, originals = asyncio.run(exercise())
    for result in (before, after):
        stage = result["folders"][0]["stage_snapshots"]["llm"]
        assert stage["final"] is False
        assert result["has_pending"] is True
    assert after["folders"][0]["stage_snapshots"]["llm"]["pending_request_count"] == 1
    assert UUID(str(source_id)) and any(row["kind"] == "llm_request_unknown" for row in originals)


def test_missing_unknown_that_was_later_resolved_allows_targeted_failure_repair(isolated_server, monkeypatch):
    from paperflow_v3.control import snapshots

    async def exercise():
        task_id, child = await make_ticket([
            ("preprocessing_complete", {"processing_attempt_id": "initial"}),
            ("llm_request_failed", {"component_id": "body", "request_id": "failed", "kind": "body",
                                    "failure_reason": "http_rejection"}),
        ])
        await snapshots.load_task_snapshot(task_id, [child], repair=True)
        original_save, original_gap = snapshots.save_receipt_snapshot, snapshots.snapshot_gap

        async def miss_unknown(client, receipt, source):
            if receipt["kind"] == "llm_request_unknown":
                raise RuntimeError("synthetic snapshot failure")
            return await original_save(client, receipt, source)

        def miss_gap(*_args):
            raise RuntimeError("synthetic gap failure")

        monkeypatch.setattr(snapshots, "save_receipt_snapshot", miss_unknown)
        monkeypatch.setattr(snapshots, "snapshot_gap", miss_gap)
        args = dict(task_id=task_id, folder_id="group-a", child_flow_run_id=child.id)
        await save_receipt(**args, event_id="unknown", kind="llm_request_unknown",
                           data={"component_id": "attachment", "request_id": "same", "kind": "attachment"})
        monkeypatch.setattr(snapshots, "save_receipt_snapshot", original_save)
        monkeypatch.setattr(snapshots, "snapshot_gap", original_gap)
        await save_receipt(**args, event_id="child-error", kind="child_error", data={"stage": "processing"})

        async def forbidden(*_args, **_kwargs):
            raise AssertionError("targeted repair must not rescan the entire paper history")

        monkeypatch.setattr(snapshots, "_originals", forbidden)
        unresolved = await snapshots.load_task_snapshot(task_id, [child], repair=True)
        await save_receipt(**args, event_id="saved-response", kind="llm_response_saved",
                           data={"component_id": "attachment", "request_id": "same", "kind": "attachment"})
        resolved = await snapshots.load_task_snapshot(task_id, [child], repair=True)
        return unresolved, resolved

    unresolved, resolved = asyncio.run(exercise())
    assert unresolved["folders"][0]["stage_snapshots"]["llm"]["final"] is False
    stage = resolved["folders"][0]["stage_snapshots"]["llm"]
    assert stage["final"] is True and stage["result"] == "failed"
    assert stage["pending_request_count"] == 0


@pytest.mark.parametrize("stage", ["llm", "rag"])
@pytest.mark.parametrize("use_sync", [False, True])
def test_completion_waits_for_missing_response_snapshot_then_repairs_by_id(isolated_server, monkeypatch, stage, use_sync):
    from paperflow_v3.control import snapshots

    if stage == "llm":
        initial = ("llm_request_intent", {"component_id": "body", "request_id": "r1", "kind": "body"})
        response = ("llm_response_saved", initial[1])
        completion = ("llm_folder_complete", {"status": "raw_complete", "ranking_complete": True})
    else:
        initial = ("rag_effect_intent", {"effect_id": "effect-1"})
        response = ("rag_effect_returned", initial[1])
        completion = ("delivery_stage", {"stage": "waiting_nas_archive"})

    async def exercise():
        task_id, child = await make_ticket([initial])
        await snapshots.load_task_snapshot(task_id, [child], repair=True)
        original_save = snapshots.save_receipt_snapshot

        async def miss_response(client, receipt, source):
            if receipt["kind"] == response[0]:
                raise RuntimeError("synthetic missing response snapshot")
            return await original_save(client, receipt, source)

        monkeypatch.setattr(snapshots, "save_receipt_snapshot", miss_response)
        args = dict(task_id=task_id, folder_id="group-a", child_flow_run_id=child.id)
        await save_receipt(**args, event_id="response", kind=response[0], data=response[1])
        monkeypatch.setattr(snapshots, "save_receipt_snapshot", original_save)
        completion_args = dict(**args, event_id="completion", kind=completion[0], data=completion[1])
        completion_id = (await asyncio.to_thread(save_receipt_sync, **completion_args) if use_sync else
                         await save_receipt(**completion_args))
        async with get_client() as client:
            pending = await snapshots._pending_receipts(client, [child.id])

        async def forbidden(*_args, **_kwargs):
            raise AssertionError("targeted repair must not rescan paper history")

        monkeypatch.setattr(snapshots, "_originals", forbidden)
        before = await snapshots.load_task_snapshot(task_id, [child])
        after = await snapshots.load_task_snapshot(task_id, [child], repair=True)
        return completion_id, pending, before, after

    completion_id, pending, before, after = asyncio.run(exercise())
    assert before["folders"][0]["stage_snapshots"][stage]["final"] is False
    result = after["folders"][0]["stage_snapshots"][stage]
    assert result["final"] is True and result["result"] == "passed"
    assert result["pending_request_count"] == 0
    assert str(completion_id) in {row.data["source_artifact_id"] for row in pending}


def test_failed_index_acknowledgement_repairs_only_the_original_receipt(isolated_server, monkeypatch):
    from prefect.client.orchestration import PrefectClient
    from paperflow_v3.control import receipts, snapshots

    async def exercise():
        task_id, child = await make_ticket([
            ("preprocessing_started", {"processing_attempt_id": "initial"}),
        ])
        await snapshots.load_task_snapshot(task_id, [child], repair=True)
        original_create = PrefectClient.create_artifact

        async def fail_ack(client, artifact):
            if artifact.type == receipts.INDEXED_RECEIPT_TYPE:
                raise RuntimeError("synthetic acknowledgement failure")
            return await original_create(client, artifact)

        monkeypatch.setattr(PrefectClient, "create_artifact", fail_ack)
        source_id = await save_receipt(task_id=task_id, folder_id="group-a", child_flow_run_id=child.id,
                                       event_id="ack-test", kind="preprocessing_complete",
                                       data={"processing_attempt_id": "initial"})
        monkeypatch.setattr(PrefectClient, "create_artifact", original_create)
        before = await snapshots.load_task_snapshot(task_id, [child])

        async def forbidden(*_args, **_kwargs):
            raise AssertionError("an acknowledgement gap must not rescan paper history")

        monkeypatch.setattr(snapshots, "_originals", forbidden)
        after = await snapshots.load_task_snapshot(task_id, [child], repair=True)
        async with get_client() as client:
            original = (await snapshots._originals_by_id(client, [source_id]))[0]
        return before, after, original

    before, after, original = asyncio.run(exercise())
    assert before["folders"][0]["stage_snapshots"]["preprocessing"]["final"] is False
    assert after["folders"][0]["stage_snapshots"]["preprocessing"]["final"] is True
    assert original.key.startswith("paperflow-v3-acktest")
    assert original.data["event_id"] == "ack-test" and original.data["data"] == {"processing_attempt_id": "initial"}


def test_legacy_checked_history_with_silent_unknown_is_verified_once(isolated_server, monkeypatch):
    from prefect.client.schemas.actions import ArtifactCreate
    from paperflow_v3.control import snapshots

    async def exercise():
        task_id, child = await make_ticket([
            ("preprocessing_complete", {"processing_attempt_id": "initial"}),
            ("llm_request_failed", {"component_id": "body", "request_id": "failed", "kind": "body",
                                    "failure_reason": "http_rejection"}),
        ])
        async with get_client() as client:
            await client.create_artifact(ArtifactCreate(
                key="paperflow-v3-legacy-unknown", type="paperflow-v3-receipt", flow_run_id=child.id,
                data={"task_id": str(task_id), "folder_id": "group-a", "event_id": "legacy-unknown",
                      "kind": "llm_request_unknown", "data": {"component_id": "attachment",
                      "request_id": "legacy", "kind": "attachment"}}))
        await save_receipt(task_id=task_id, folder_id="group-a", child_flow_run_id=child.id,
                           event_id="legacy-child-error", kind="child_error", data={"stage": "processing"})
        async with get_client() as client:
            await client.create_artifact(ArtifactCreate(
                key=snapshots._key("history", str(task_id), str(child.id)), type=snapshots.HISTORY_TYPE,
                flow_run_id=child.id, data={"task_id": str(task_id)}))
        calls, original = [], snapshots._originals

        async def counted(client, run_ids):
            calls.append(tuple(run_ids))
            return await original(client, run_ids)

        monkeypatch.setattr(snapshots, "_originals", counted)
        before = await snapshots.load_task_snapshot(task_id, [child])
        repaired = await snapshots.load_task_snapshot(task_id, [child], repair=True)
        after_count = len(calls)
        again = await snapshots.load_task_snapshot(task_id, [child], repair=True)
        return child, before, repaired, again, calls, after_count

    child, before, repaired, again, calls, after_count = asyncio.run(exercise())
    assert str(child.id) in before["history_targets"]
    assert before["has_pending"] is True
    assert before["folders"][0]["stage_snapshots"]["llm"]["final"] is False
    assert repaired["folders"][0]["stage_snapshots"]["llm"]["final"] is False
    assert repaired["folders"][0]["stage_snapshots"]["llm"]["pending_request_count"] == 1
    assert any(child.id in run_ids for run_ids in calls[:after_count])
    assert len(calls) == after_count
    assert again["folders"][0]["stage_snapshots"]["llm"]["pending_request_count"] == 1


def test_rag_effect_intent_after_final_remains_pending(isolated_server):
    from paperflow_v3.control.snapshots import load_task_snapshot

    async def exercise():
        task_id, child = await make_ticket([
            ("delivery_stage", {"stage": "waiting_nas_archive"}),
            ("rag_effect_intent", {"effect_id": "new-unconfirmed-effect"}),
        ])
        return await load_task_snapshot(task_id, [child])

    result = asyncio.run(exercise())
    stage = result["folders"][0]["stage_snapshots"]["rag"]
    assert stage["final"] is False and stage["pending_request_count"] == 1
    assert result["has_pending"] is True


def test_legacy_unknown_without_any_history_marker_is_repaired_once(isolated_server, monkeypatch):
    from prefect.client.schemas.actions import ArtifactCreate
    from paperflow_v3.control import snapshots

    async def exercise():
        task_id, child = await make_ticket([
            ("preprocessing_complete", {"processing_attempt_id": "initial"}),
            ("llm_request_failed", {"component_id": "body", "request_id": "failed", "kind": "body",
                                    "failure_reason": "http_rejection"}),
        ])
        async with get_client() as client:
            await client.create_artifact(ArtifactCreate(
                key="paperflow-v3-legacy-no-marker", type="paperflow-v3-receipt", flow_run_id=child.id,
                data={"task_id": str(task_id), "folder_id": "group-a", "event_id": "legacy-no-marker",
                      "kind": "llm_request_unknown", "data": {"component_id": "attachment",
                      "request_id": "legacy", "kind": "attachment"}}))
        await save_receipt(task_id=task_id, folder_id="group-a", child_flow_run_id=child.id,
                           event_id="child-error", kind="child_error", data={"stage": "processing"})
        # Replace the current derived failure with the previous version's markerless shape.
        async with get_client() as client:
            rows = await snapshots._collections(client, [child.id], [snapshots.SNAPSHOT_TYPE])
            final = next(row for row in rows if row.data.get("stage") == "llm" and row.data.get("final"))
            data = {key: value for key, value in final.data.items() if key != "snapshot_version"}
            await client.create_artifact(ArtifactCreate(key=final.key, type=snapshots.SNAPSHOT_TYPE,
                                                        flow_run_id=child.id, data=data))
        calls, original = [], snapshots._originals

        async def counted(client, run_ids):
            calls.append(tuple(run_ids))
            return await original(client, run_ids)

        monkeypatch.setattr(snapshots, "_originals", counted)
        before = await snapshots.load_task_snapshot(task_id, [child])
        repaired = await snapshots.load_task_snapshot(task_id, [child], repair=True)
        count = len(calls)
        await snapshots.load_task_snapshot(task_id, [child], repair=True)
        return child, before, repaired, calls, count

    child, before, repaired, calls, count = asyncio.run(exercise())
    assert before["has_pending"] is True
    assert repaired["folders"][0]["stage_snapshots"]["llm"]["final"] is False
    assert repaired["folders"][0]["stage_snapshots"]["llm"]["pending_request_count"] == 1
    assert any(child.id in run_ids for run_ids in calls) and len(calls) == count


def test_exact_original_key_lookup_still_finds_suffixed_receipts(isolated_server):
    from paperflow_v3.control.receipts import read_folder_receipts_by_key

    async def exercise():
        task_id, child = await make_ticket([
            ("llm_request_unknown", {"component_id": "body", "request_id": "exact", "kind": "body"}),
        ])
        rows = await read_folder_receipts_by_key([child.id], keys=("paperflow-v3-synthetic0",))
        return child, rows

    child, rows = asyncio.run(exercise())
    assert [row.data["kind"] for row in rows[str(child.id)]] == ["llm_request_unknown"]


def test_query_cannot_miss_unknown_completed_between_saved_and_pending_reads(isolated_server, monkeypatch):
    from prefect.client.schemas.actions import ArtifactCreate
    from paperflow_v3.control import receipts, snapshots

    async def exercise():
        task_id, child = await make_ticket([
            ("llm_folder_complete", {"status": "raw_complete", "ranking_complete": True}),
        ])
        record = receipts._artifact(task_id=task_id, folder_id="group-a", child_flow_run_id=child.id,
                                    event_id="interleaved-unknown", kind="llm_request_unknown",
                                    data={"component_id": "body", "request_id": "interleaved", "kind": "body"})
        async with get_client() as client:
            source = await client.create_artifact(record)
        original, inserted = snapshots._collections, False

        async def interleave(client, run_ids, types):
            nonlocal inserted
            rows = await original(client, run_ids, types)
            if not inserted and snapshots.SNAPSHOT_TYPE in types:
                inserted = True
                await snapshots.save_receipt_snapshot(client, record.data, source)
                await client.create_artifact(ArtifactCreate(key=record.key, type=receipts.INDEXED_RECEIPT_TYPE,
                    flow_run_id=child.id, data={"source_artifact_id": str(source.id)}))
            return rows

        monkeypatch.setattr(snapshots, "_collections", interleave)
        return await snapshots.load_task_snapshot(task_id, [child])

    result = asyncio.run(exercise())
    assert result["has_pending"] is True
    assert result["folders"][0]["stage_snapshots"]["llm"]["final"] is False


def test_finalization_cannot_miss_unknown_completed_between_saved_and_pending_reads(isolated_server, monkeypatch):
    from prefect.client.schemas.actions import ArtifactCreate
    from paperflow_v3.control import receipts, snapshots

    async def exercise():
        task_id, child = await make_ticket([])
        record = receipts._artifact(task_id=task_id, folder_id="group-a", child_flow_run_id=child.id,
                                    event_id="interleaved-unknown", kind="llm_request_unknown",
                                    data={"component_id": "body", "request_id": "interleaved", "kind": "body"})
        async with get_client() as client:
            source = await client.create_artifact(record)
        original, inserted = snapshots._collections, False

        async def interleave(client, run_ids, types):
            nonlocal inserted
            rows = await original(client, run_ids, types)
            if not inserted and snapshots.SNAPSHOT_TYPE in types:
                inserted = True
                await snapshots.save_receipt_snapshot(client, record.data, source)
                await client.create_artifact(ArtifactCreate(key=record.key, type=receipts.INDEXED_RECEIPT_TYPE,
                    flow_run_id=child.id, data={"source_artifact_id": str(source.id)}))
            return rows

        monkeypatch.setattr(snapshots, "_collections", interleave)
        await save_receipt(task_id=task_id, folder_id="group-a", child_flow_run_id=child.id,
                           event_id="final-interleaved", kind="llm_folder_complete",
                           data={"status": "raw_complete", "ranking_complete": True})
        monkeypatch.setattr(snapshots, "_collections", original)
        return await snapshots.load_task_snapshot(task_id, [child])

    result = asyncio.run(exercise())
    assert result["folders"][0]["stage_snapshots"]["llm"]["final"] is False
    assert result["has_pending"] is True


@pytest.mark.parametrize("kind", ["llm_request_intent", "llm_request_started"])
def test_new_llm_request_without_outcome_after_final_is_pending(isolated_server, kind):
    from paperflow_v3.control.snapshots import load_task_snapshot

    async def exercise():
        task_id, child = await make_ticket([
            ("llm_folder_complete", {"status": "raw_complete", "ranking_complete": True}),
            (kind, {"component_id": "attachment", "request_id": "new", "kind": "attachment"}),
        ])
        return await load_task_snapshot(task_id, [child])

    result = asyncio.run(exercise())
    stage = result["folders"][0]["stage_snapshots"]["llm"]
    assert stage["final"] is False and stage["pending_request_count"] == 1
    assert stage["previous_result"]["result"] == "passed" and result["has_pending"] is True


def test_new_postprocessing_component_started_after_final_is_pending(isolated_server):
    from paperflow_v3.control.snapshots import load_task_snapshot

    async def exercise():
        task_id, child = await make_ticket([
            ("postprocessing_complete", {"validation_status": "passed"}),
            ("postprocessing_component_started", {"component_id": "attachment:new", "attempt_id": "initial"}),
        ])
        return await load_task_snapshot(task_id, [child])

    result = asyncio.run(exercise())
    stage = result["folders"][0]["stage_snapshots"]["postprocessing"]
    assert stage["final"] is False and stage["pending_request_count"] == 1
    assert result["has_pending"] is True


def test_old_postprocessing_start_snapshot_arriving_late_keeps_final(isolated_server):
    from prefect.client.schemas.actions import ArtifactCreate
    from paperflow_v3.control import receipts, snapshots

    async def exercise():
        task_id, child = await make_ticket([])
        record = receipts._artifact(task_id=task_id, folder_id="group-a", child_flow_run_id=child.id,
                                    event_id="old-start", kind="postprocessing_component_started",
                                    data={"component_id": "body", "attempt_id": "initial"})
        async with get_client() as client:
            source = await client.create_artifact(record)
        await save_receipt(task_id=task_id, folder_id="group-a", child_flow_run_id=child.id,
                           event_id="post-final", kind="postprocessing_complete", data={"validation_status": "passed"})
        async with get_client() as client:
            await snapshots.save_receipt_snapshot(client, record.data, source)
            await client.create_artifact(ArtifactCreate(key=record.key, type=receipts.INDEXED_RECEIPT_TYPE,
                flow_run_id=child.id, data={"source_artifact_id": str(source.id)}))
        return await snapshots.load_task_snapshot(task_id, [child])

    stage = asyncio.run(exercise())["folders"][0]["stage_snapshots"]["postprocessing"]
    assert stage["final"] is True and stage["result"] == "passed"


@pytest.mark.parametrize("use_sync", [False, True])
def test_pending_receipt_scan_does_not_skip_a_row_when_another_is_acknowledged(use_sync):
    from datetime import datetime, timezone
    from types import SimpleNamespace
    from uuid import uuid4
    from paperflow_v3.control import snapshots

    run_ids, task_id = [uuid4() for _ in range(1 if use_sync else 176)], str(uuid4())
    rows = []
    for index in range(201):
        suffix = f"{index % 16:x}{index:031x}"
        rows.append(SimpleNamespace(
            key=f"paperflow-v3-event-{snapshots.PENDING_RECEIPT_KEY}-{suffix}",
            latest_id=uuid4(), flow_run_id=run_ids[index % len(run_ids)], updated=datetime.now(timezone.utc),
            data={"task_id": task_id, "folder_id": "group-a", "event_id": str(index),
                  "kind": "llm_request_unknown", "data": {"request_id": str(index)}},
        ))
    tail = str(rows[-1].latest_id)

    class Store:
        def __init__(self):
            self.rows, self.calls = list(rows), 0

        def read(self, artifact_filter, limit, offset=0):
            if "paperflow-v3-receipt" not in artifact_filter.type.any_:
                return []
            pattern = artifact_filter.key.like_
            matched = [row for row in self.rows if pattern.strip("%") in row.key]
            page = matched[offset:offset + limit]
            self.calls += 1
            if self.calls == 1:
                self.rows.pop(0)  # Another writer acknowledges one of the first-page rows.
            return page

    store = Store()
    if use_sync:
        class Client:
            def read_latest_artifacts(self, **kwargs):
                return store.read(**kwargs)
        found = snapshots._collections_sync(Client(), run_ids[0])
    else:
        class Client:
            async def read_latest_artifacts(self, **kwargs):
                return store.read(**kwargs)
        found = asyncio.run(snapshots._pending_receipts(Client(), run_ids))
    assert tail in {row.data["source_artifact_id"] for row in found}

@pytest.mark.parametrize('local_unknown',[False,True])
def test_completed_snapshot_keeps_same_fields_with_existing_local_recovery(isolated_server,monkeypatch,local_unknown):
    from pathlib import Path
    from types import SimpleNamespace
    import importlib.util
    from paperflow_v3.control import snapshots
    from paperflow_v3.delivery import progress
    import os
    if not os.environ.get('PAPERFLOW_BASELINE_SNAPSHOTS'):pytest.skip('Provide a frozen snapshot-reader baseline for differential acceptance')
    source=Path(os.environ['PAPERFLOW_BASELINE_SNAPSHOTS'])
    spec=importlib.util.spec_from_file_location('paperflow_v3.control._baseline_snapshot_equivalence',source)
    baseline=importlib.util.module_from_spec(spec);spec.loader.exec_module(baseline)
    async def exercise():
        task_id,child=await make_ticket([
            ('preprocessing_complete',{}),('llm_folder_complete',{'status':'raw_complete','ranking_complete':True}),
            ('postprocessing_complete',{'validation_status':'passed'}),
            ('rag_complete',{'status':'complete','_with_identity':True}),
            ('nas',{'status':'complete','files':{'one':{'status':'complete'}},'_with_identity':True}),
            ('cleanup',{'status':'complete','files':{'one':{'status':'complete'}},'_with_identity':True})])
        identity={**{k:child.parameters['registered'][k] for k in ('task_id','group_id','paper_id','receipt_id')},
                  'task_run_id':str(child.id),'attempt_id':str(child.id),'parent_run_id':str(task_id)}
        local=[{'kind':'cleanup','data':{'identity':identity,'status':'complete','files':{'one':{'status':'complete'}}},
                'task_id':str(task_id),'folder_id':'group-a','recorded_at':'2025-01-01T00:00:00Z','local_only':True}]
        if local_unknown:local.insert(0,{'kind':'rag_effect_intent','data':{'effect_id':'unconfirmed','status':'unknown'},
            'task_id':str(task_id),'folder_id':'group-a','recorded_at':'2099-01-01T00:00:00Z','local_only':True})
        calls=[]
        def local_records(id):calls.append(id);return local
        monkeypatch.setattr(progress,'local_records',local_records)
        expected=await baseline.load_task_snapshot(task_id,[child],repair=False)
        assert calls
        actual=await snapshots.load_task_snapshot(task_id,[child],repair=False)
        return expected,actual
    expected,actual=asyncio.run(exercise())
    # This additive field describes history-read coverage; business projection
    # and every local unknown must remain identical to the frozen reader.
    assert isinstance(actual.pop('history_targets'),list)
    assert actual==expected


def test_saved_snapshot_survives_prefect_server_restart(tmp_path):
    import inspect
    import prefect.settings
    from prefect.events.worker import EventsWorker
    from prefect.logging.handlers import APILogWorker
    from prefect.server.api.server import SubprocessASGIServer
    from prefect.server.database.dependencies import temporary_database_interface
    from prefect.testing.utilities import _find_available_port
    from prefect.utilities.asyncutils import run_coro_as_sync
    from paperflow_v3.control.snapshots import load_task_snapshot

    async def create():
        task_id, child = await make_ticket([
            ("preprocessing_complete", {"processing_attempt_id": "initial"}),
        ])
        result = await load_task_snapshot(task_id, [child])
        return task_id, child.id, result["folders"][0]["stage_snapshots"]["preprocessing"]

    async def read(task_id, child_id):
        async with get_client() as client:
            child = await client.read_flow_run(child_id)
        result = await load_task_snapshot(task_id, [child])
        return result["folders"][0]["stage_snapshots"]["preprocessing"]

    async def drain():
        for worker in (APILogWorker.instance().drain(), EventsWorker.drain_all()):
            if inspect.isawaitable(worker):
                await worker

    database = "sqlite+aiosqlite:///" + (tmp_path / "restart-test.db").as_posix()
    with temporary_database_interface(), prefect.settings.temporary_settings(updates={
        prefect.settings.PREFECT_SERVER_DATABASE_CONNECTION_URL: database,
    }):
        first = SubprocessASGIServer(port=_find_available_port())
        first.start(timeout=60)
        try:
            with prefect.settings.temporary_settings(updates={prefect.settings.PREFECT_API_URL: first.api_url}):
                task_id, child_id, before = asyncio.run(create())
                run_coro_as_sync(drain())
        finally:
            first.stop()
        second = SubprocessASGIServer(port=_find_available_port())
        second.start(timeout=60)
        try:
            with prefect.settings.temporary_settings(updates={prefect.settings.PREFECT_API_URL: second.api_url}):
                after = asyncio.run(read(task_id, child_id))
                run_coro_as_sync(drain())
        finally:
            second.stop()

    assert after == before
