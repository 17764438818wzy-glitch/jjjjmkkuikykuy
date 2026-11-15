"""Query-service checks with a fake Prefect boundary and fake A snapshots."""

import asyncio
import copy
import importlib
import importlib.util
import sys
from contextvars import ContextVar
from datetime import datetime, timezone
from types import ModuleType, SimpleNamespace
from uuid import UUID

import pytest


DEPLOYMENT_ID = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
TASK_ID = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
OTHER_ID = UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")


def test_incomplete_classification_never_enumerates_child_runs(monkeypatch):
    store = FakeSnapshots()
    store.saved[str(TASK_ID)]["classification_complete"] = False
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")

    async def forbidden(task_id):
        raise AssertionError("partial parent classification must not enumerate child runs")

    monkeypatch.setattr(module, "_children", forbidden)

    async def scenario():
        try:
            result = await service.classifications(TASK_ID)
            assert result["complete"] is False and result["has_pending"] is True
            assert result["as_of"] == store.saved[str(TASK_ID)]["as_of"]
            assert result["items"] == store.saved[str(TASK_ID)]["items"]
            assert result["counts"]["failed"] is None
            assert all(not ids for ids in store.read_children)
        finally:
            await service.close()
        assert store.open_contexts == 0

    asyncio.run(scenario())


def test_checked_partial_classifications_do_not_repeat_parent_history_refresh(monkeypatch):
    store=FakeSnapshots();store.saved[str(TASK_ID)].update(classification_complete=False,history_targets=[])
    service=prepare_service(monkeypatch,store)
    async def scenario():
        try:
            result=await service.classifications(TASK_ID)
            await asyncio.sleep(0)
            assert result['complete'] is False and result['has_pending'] is True
            assert result['counts']['failed'] is None
            assert store.repair_calls==[]
        finally:await service.close()
    asyncio.run(scenario())


def test_classification_outage_without_saved_view_does_not_scan_children(monkeypatch):
    store = FakeSnapshots()
    store.error = OSError("isolated outage")
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")

    async def forbidden(task_id):
        raise AssertionError("classification outage must not enumerate child runs")

    monkeypatch.setattr(module, "_children", forbidden)

    async def scenario():
        try:
            result = await service.classifications(TASK_ID)
            assert result["query_status"] == "unavailable"
            assert result["as_of"] is None and result["has_pending"] is None
            assert "items" not in result
        finally:
            await service.close()
        assert store.open_contexts == 0

    asyncio.run(scenario())


def test_classification_bootstrap_cannot_fill_full_detail_refresh_cache(monkeypatch):
    store = FakeSnapshots()
    store.saved[str(TASK_ID)]["classification_complete"] = False
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")

    async def forbidden(task_id):
        raise AssertionError("classification must not read child metadata")

    monkeypatch.setattr(module, "_children", forbidden)

    async def scenario():
        try:
            await service.classifications(TASK_ID)
            await asyncio.gather(*service._history_tasks.values())
            child = SimpleNamespace(id="child-1", parameters={"group_id": "paper-1"})
            await service._refresh(str(TASK_ID), 0, None, 0, [child])
            assert store.repair_calls == [(str(TASK_ID), ()), (str(TASK_ID), ("child-1",))]
        finally:
            await service.close()
        assert store.open_contexts == 0

    asyncio.run(scenario())


def test_checked_final_children_keep_unknown_without_active_full_repair(monkeypatch):
    from prefect.states import Crashed
    store=FakeSnapshots();saved=store.saved[str(TASK_ID)]
    saved.update(history_targets=[],has_pending=True)
    saved['missing']=[{'ticket_id':'child-1','stage':'preprocessing'}]
    service=prepare_service(monkeypatch,store)
    module=importlib.import_module('paperflow_v3.control.query_service')
    async def children(task_id):
        return [SimpleNamespace(id='child-1',state=Crashed(),parameters={'group_id':'paper-1'})]
    monkeypatch.setattr(module,'_children',children)
    async def scenario():
        first=await service.task_details(TASK_ID)
        assert first['has_pending'] is True and first['folders']==saved['folders']
        saved['folders'][0]['preprocessing_result']='passed'
        saved['folders'][0]['stage_snapshots']['preprocessing'].update(final=True,result_version='new-final')
        latest=await service.task_details(TASK_ID)
        assert latest['folders'][0]['preprocessing_result']=='passed'
        assert store.repair_calls==[]
        await service.close()
        assert store.open_contexts==0
    asyncio.run(scenario())


def test_explicit_history_scope_keeps_all_folders_and_distinguishes_new_gap_child(monkeypatch):
    from prefect.states import Crashed
    store=FakeSnapshots();saved=store.saved[str(TASK_ID)]
    saved['history_targets']=['child-1']
    saved['folders'].append({**copy.deepcopy(saved['folders'][0]),'ticket_id':'child-2','folder_id':'paper-2'})
    service=prepare_service(monkeypatch,store)
    module=importlib.import_module('paperflow_v3.control.query_service')
    async def children(task_id):
        return [SimpleNamespace(id='child-'+str(n),state=Crashed(),parameters={'group_id':'paper-'+str(n)}) for n in (1,2)]
    monkeypatch.setattr(module,'_children',children)
    async def scenario():
        first=await service.task_details(TASK_ID)
        assert len(first['folders'])==2 and first['query_status']=='preparing_history'
        await asyncio.gather(*service._history_tasks.values())
        saved['history_targets']=['child-2']
        second=await service.task_details(TASK_ID)
        await asyncio.gather(*service._history_tasks.values())
        assert len(second['folders'])==2
        assert store.repair_calls==[(str(TASK_ID),('child-1',)),(str(TASK_ID),('child-2',))]
        await service.close()
        assert store.open_contexts==0
    asyncio.run(scenario())


@pytest.mark.parametrize('state_name',['Running','Pending','Scheduled'])
def test_explicit_empty_history_still_refreshes_active_children(monkeypatch,state_name):
    from prefect import states
    store=FakeSnapshots();store.saved[str(TASK_ID)]['history_targets']=[]
    service=prepare_service(monkeypatch,store)
    module=importlib.import_module('paperflow_v3.control.query_service')
    async def children(task_id):
        return [SimpleNamespace(id='child-1',state=getattr(states,state_name)(),parameters={'group_id':'paper-1'})]
    monkeypatch.setattr(module,'_children',children)
    async def scenario():
        result=await service.task_details(TASK_ID)
        assert result['has_pending'] is True
        assert store.repair_calls==[(str(TASK_ID),('child-1',))]
        await service.close()
        assert store.open_contexts==0
    asyncio.run(scenario())


def test_explicit_parent_gap_repairs_parent_without_any_children(monkeypatch):
    from prefect.states import Crashed
    store=FakeSnapshots();store.saved[str(TASK_ID)]['history_targets']=[str(TASK_ID)]
    service=prepare_service(monkeypatch,store)
    module=importlib.import_module('paperflow_v3.control.query_service')
    async def children(task_id):
        return [SimpleNamespace(id='child-1',state=Crashed(),parameters={'group_id':'paper-1'})]
    monkeypatch.setattr(module,'_children',children)
    async def scenario():
        result=await service.task_details(TASK_ID)
        await asyncio.gather(*service._history_tasks.values())
        assert len(result['folders'])==1 and result['query_status']=='preparing_history'
        assert store.repair_calls==[(str(TASK_ID),())]
        await service.close()
    asyncio.run(scenario())


def test_list_reads_only_parent_runs_and_saved_summaries(monkeypatch):
    calls = []

    async def read_task_summaries(task_ids):
        calls.append([str(task_id) for task_id in task_ids])
        return {str(TASK_ID): {"task_state": "running"}}

    async def load_task_snapshot(*args, **kwargs):
        raise AssertionError("task listing must not read child snapshots")

    name = "paperflow_v3.control.query_service"
    assert importlib.util.find_spec(name) is not None, "QueryService is not implemented"
    module = importlib.import_module(name)
    monkeypatch.setattr(module, 'read_task_summaries', read_task_summaries)
    monkeypatch.setattr(module, 'load_task_snapshot', load_task_snapshot)

    run = SimpleNamespace(
        id=TASK_ID,
        deployment_id=DEPLOYMENT_ID,
        parameters={"task_name": "假任务 A"},
        created=datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc),
    )

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def read_flow_runs(self, *, flow_run_filter, limit, offset):
            assert flow_run_filter.deployment_id.any_ == [DEPLOYMENT_ID]
            assert limit == 200 and offset == 0
            return [run]

    class Context:
        __var__ = ContextVar("fake_list_request_client", default=None)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

    monkeypatch.setattr(module, "get_client", lambda: Client())
    monkeypatch.setattr(module, "AsyncClientContext", Context)

    async def scenario():
        service = module.QueryService({"deployment_id": str(DEPLOYMENT_ID)})
        try:
            return await service.list_tasks()
        finally:
            await service.close()

    response = asyncio.run(scenario())
    assert calls == [[str(TASK_ID)]]
    assert response["complete"] is True
    assert response["query_status"] == "updated"
    assert response["tasks"] == [{
        "task_id": str(TASK_ID),
        "parent_flow_run_id": str(TASK_ID),
        "task_name": "假任务 A",
        "created_at": "2026-01-02T03:04:00+00:00",
        "task_state": "running",
    }]


def saved_view(task_id, *, final=False, attempt="initial", version="one"):
    folder_id = "paper-1"
    return {
        "folders": [{
            "ticket_id": "child-1", "task_id": str(task_id), "folder_id": folder_id,
            "folder_name": "假论文", "paper_title": "假论文", "task_name": "假任务",
            "stage": "waiting_llm" if final else "preprocessing_in_progress",
            "preprocessing_result": "passed" if final else None,
            "llm_result": None, "postprocessing_result": None, "last_retry": None,
            "stage_snapshots": {"preprocessing": {
                "processing_attempt_id": attempt, "result_version": version,
                "recorded_at": "2026-01-02T03:04:00+00:00", "final": final,
                "pending_request_count": 0,
            }},
        }],
        "items": [{"group_id": folder_id, "folder_name": "假论文", "paper_title": "假论文",
                   "paper_title_candidate": None, "status": "passed", "reason": None}],
        "complete": True, "has_pending": True, "missing": [],
        "as_of": "2026-01-02T03:04:00+00:00",
    }


class FakeSnapshots:
    def __init__(self):
        self.saved = {str(task): saved_view(task) for task in (TASK_ID, OTHER_ID)}
        self.active = copy.deepcopy(self.saved)
        self.repair_calls = []
        self.read_calls = []
        self.read_children = []
        self.error = None
        self.repair_error = None
        self.require_uuid = False
        self.gate = None
        self.started = None
        self.open_contexts = 0

    async def read_task_summaries(self, task_ids):
        return {}

    async def load_task_snapshot(self, task_id, children, *, repair=False):
        if self.require_uuid:
            assert isinstance(task_id, UUID), "A needs native UUID for parent comparisons"
        task_id = str(task_id)
        if self.error:
            raise self.error
        if repair:
            assert self.open_contexts > 0, "shared refresh needs its own client context"
            self.repair_calls.append((task_id, tuple(str(child.id) for child in children)))
            if self.repair_error:
                if self.started:
                    self.started.set()
                raise self.repair_error
            result = copy.deepcopy(self.active[task_id])
            if self.started:
                self.started.set()
            if self.gate:
                await self.gate.wait()
            return result
        self.read_calls.append(task_id)
        self.read_children.append(tuple(str(child.id) for child in children))
        return copy.deepcopy(self.saved[task_id])


def prepare_service(monkeypatch, store):
    snapshots = ModuleType("paperflow_v3.control.snapshots")
    snapshots.read_task_summaries = store.read_task_summaries
    snapshots.load_task_snapshot = store.load_task_snapshot
    monkeypatch.setitem(sys.modules, snapshots.__name__, snapshots)
    module = importlib.import_module("paperflow_v3.control.query_service")
    monkeypatch.setattr(module, "read_task_summaries", store.read_task_summaries)
    monkeypatch.setattr(module, "load_task_snapshot", store.load_task_snapshot)

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def read_flow_run(self, task_id):
            state = SimpleNamespace(is_final=lambda: False, is_crashed=lambda: False,
                                    is_paused=lambda: False, is_failed=lambda: False,
                                    is_completed=lambda: False, is_scheduled=lambda: False,
                                    is_pending=lambda: False)
            return SimpleNamespace(id=task_id, deployment_id=DEPLOYMENT_ID,
                                   parameters={"task_name": "假任务"}, state=state, run_count=1)

    class Context:
        __var__ = ContextVar("fake_request_client", default=None)

        async def __aenter__(self):
            assert self.__var__.get() is None, "refresh inherited the HTTP request client"
            store.open_contexts += 1

        async def __aexit__(self, *args):
            store.open_contexts -= 1

    async def children(task_id):
        return [SimpleNamespace(id="child-1", parameters={"group_id": "paper-1"})]

    class Admission:
        def __init__(self, task_id):
            self.task_id = task_id

        async def running(self):
            return False

    monkeypatch.setattr(module, "get_client", lambda: Client())
    monkeypatch.setattr(module, "_children", children, raising=False)
    monkeypatch.setattr(module, "AsyncClientContext", Context, raising=False)
    monkeypatch.setattr(module, "Admission", Admission, raising=False)
    return module.QueryService({"deployment_id": str(DEPLOYMENT_ID)})


def test_concurrent_details_share_one_refresh_and_release_its_context(monkeypatch):
    store = FakeSnapshots()
    service = prepare_service(monkeypatch, store)

    async def scenario():
        store.gate, store.started = asyncio.Event(), asyncio.Event()
        requests = [asyncio.create_task(service.task_details(TASK_ID)) for _ in range(20)]
        await asyncio.wait_for(store.started.wait(), 1)
        await asyncio.sleep(0)
        assert len(store.repair_calls) == 1
        store.gate.set()
        responses = await asyncio.gather(*requests)
        assert all(row["task_id"] == str(TASK_ID) for row in responses)
        assert len(store.read_calls) == 2
        assert store.open_contexts == 0
        await service.close()

    asyncio.run(scenario())


def test_ten_second_cache_expires_without_a_background_refresh(monkeypatch):
    store = FakeSnapshots()
    service = prepare_service(monkeypatch, store)

    async def scenario():
        loop = asyncio.get_running_loop()
        call_later = loop.call_later
        timers = []

        class Timer:
            def __init__(self, callback, args):
                self.callback, self.args, self.cancelled = callback, args, False

            def cancel(self):
                self.cancelled = True

        def schedule(delay, callback, *args, **kwargs):
            if delay == 10:
                timer = Timer(callback, args)
                timers.append(timer)
                return timer
            return call_later(delay, callback, *args, **kwargs)

        monkeypatch.setattr(loop, "call_later", schedule)
        first = await service.task_details(TASK_ID)
        again = await service.task_details(TASK_ID)
        assert first["query_status"] == "updated"
        assert again["query_status"] == "reused"
        assert len(store.repair_calls) == 1
        assert len(timers) == 1
        timers[0].callback(*timers[0].args)
        await asyncio.sleep(0)
        assert len(store.repair_calls) == 1, "expiry without a viewer must not refresh"
        await service.task_details(TASK_ID)
        assert len(store.repair_calls) == 2
        await service.close()

    asyncio.run(scenario())


def test_invalidation_does_not_refresh_other_tasks(monkeypatch):
    store = FakeSnapshots()
    service = prepare_service(monkeypatch, store)

    async def scenario():
        await service.task_details(TASK_ID)
        await service.task_details(OTHER_ID)
        await service.invalidate(TASK_ID, folder_id="paper-1")
        await service.task_details(TASK_ID)
        other = await service.task_details(OTHER_ID)
        assert [task for task, _ in store.repair_calls].count(str(TASK_ID)) == 2
        assert [task for task, _ in store.repair_calls].count(str(OTHER_ID)) == 1
        assert other["query_status"] == "reused"
        await service.close()

    asyncio.run(scenario())


def test_new_saved_terminal_result_wins_over_cached_progress(monkeypatch):
    store = FakeSnapshots()
    service = prepare_service(monkeypatch, store)

    async def scenario():
        await service.task_details(TASK_ID)
        store.saved[str(TASK_ID)] = saved_view(TASK_ID, final=True, version="two")
        result = await service.task_details(TASK_ID)
        assert len(store.repair_calls) == 1
        assert result["folders"][0]["preprocessing_result"] == "passed"
        assert result["folders"][0]["stage_snapshots"]["preprocessing"]["result_version"] == "two"
        assert result["folders"][0]["stage_snapshots"]["preprocessing"]["recorded_at"] == "2026-01-02T03:04:00+00:00"
        await service.close()

    asyncio.run(scenario())


def test_failed_refresh_returns_last_good_without_changing_time(monkeypatch):
    store = FakeSnapshots()
    service = prepare_service(monkeypatch, store)

    async def scenario():
        old = await service.task_details(TASK_ID)
        store.error = OSError("isolated storage unavailable")
        failed = await service.task_details(TASK_ID)
        assert failed["query_status"] == "stale"
        assert failed["as_of"] == old["as_of"]
        assert failed["folders"] == old["folders"]
        await service.close()

    asyncio.run(scenario())


def test_unavailable_without_old_result_is_not_an_empty_task(monkeypatch):
    store = FakeSnapshots()
    store.error = OSError("isolated storage unavailable")
    service = prepare_service(monkeypatch, store)

    async def scenario():
        failed = await service.task_details(TASK_ID)
        assert failed["query_status"] == "unavailable"
        assert failed["as_of"] is None
        assert failed["has_pending"] is None
        assert "folders" not in failed or failed["folders"] is None
        await service.close()

    asyncio.run(scenario())


def test_classification_completeness_is_independent_of_folder_coverage(monkeypatch):
    store = FakeSnapshots()
    for views in (store.saved, store.active):
        views[str(TASK_ID)]["classification_complete"] = False
        views[str(TASK_ID)]["items"].append({
            "group_id": "paper-2", "folder_name": "第二张", "paper_title": None,
            "paper_title_candidate": None, "status": "failed", "reason": "假失败",
        })
    service = prepare_service(monkeypatch, store)

    async def scenario():
        result = await service.classifications(TASK_ID)
        assert result["complete"] is False
        assert result["counts"] == {"passed": 1, "failed": 1,
                                    "unrecognized": None, "unresolved": None}
        await service.close()

    asyncio.run(scenario())


def test_refresh_does_not_inherit_first_http_request_client(monkeypatch):
    store = FakeSnapshots()
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")

    async def scenario():
        token = module.AsyncClientContext.__var__.set(object())
        try:
            await service.task_details(TASK_ID)
            assert store.open_contexts == 0
        finally:
            module.AsyncClientContext.__var__.reset(token)
            await service.close()

    asyncio.run(scenario())


def test_invalidated_old_inflight_result_cannot_replace_new_attempt(monkeypatch):
    store = FakeSnapshots()
    service = prepare_service(monkeypatch, store)

    async def scenario():
        gate = store.gate = asyncio.Event()
        store.started = asyncio.Event()
        old = asyncio.create_task(service.task_details(TASK_ID))
        await asyncio.wait_for(store.started.wait(), 1)
        await service.invalidate(TASK_ID, folder_id="paper-1")
        newer = saved_view(TASK_ID, attempt="retry-2", version="two")
        store.saved[str(TASK_ID)] = copy.deepcopy(newer)
        store.active[str(TASK_ID)] = copy.deepcopy(newer)
        store.gate = None
        current = await service.task_details(TASK_ID)
        gate.set()
        late = await old
        for row in (current, late):
            stage = row["folders"][0]["stage_snapshots"]["preprocessing"]
            assert stage["processing_attempt_id"] == "retry-2"
        await service.close()

    asyncio.run(scenario())


def test_finished_task_reuses_saved_result_without_history_refresh(monkeypatch):
    store = FakeSnapshots()
    store.saved[str(TASK_ID)] = saved_view(TASK_ID, final=True)
    store.saved[str(TASK_ID)]["has_pending"] = False
    service = prepare_service(monkeypatch, store)

    async def scenario():
        first = await service.task_details(TASK_ID)
        second = await service.task_details(TASK_ID)
        assert not store.repair_calls
        assert first["folders"][0]["preprocessing_result"] == "passed"
        assert second["folders"][0]["stage_snapshots"]["preprocessing"]["recorded_at"] == "2026-01-02T03:04:00+00:00"
        await service.close()

    asyncio.run(scenario())


def test_missing_history_is_reported_before_slow_repair_finishes(monkeypatch):
    store = FakeSnapshots()
    store.saved[str(TASK_ID)]["missing"] = [{"ticket_id": "child-1", "stage": "llm"}]
    service = prepare_service(monkeypatch, store)

    async def scenario():
        gate = store.gate = asyncio.Event()
        store.started = asyncio.Event()
        response = await asyncio.wait_for(service.task_details(TASK_ID), 0.2)
        assert response["query_status"] == "preparing_history"
        await asyncio.wait_for(store.started.wait(), 1)
        gate.set()
        await service.close()

    asyncio.run(scenario())


def test_folder_invalidation_refreshes_only_that_folder(monkeypatch):
    store = FakeSnapshots()
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")

    async def children(task_id):
        return [SimpleNamespace(id="child-1", parameters={"group_id": "paper-1"}),
                SimpleNamespace(id="child-2", parameters={"group_id": "paper-2"})]

    monkeypatch.setattr(module, "_children", children)

    async def scenario():
        await service.task_details(TASK_ID)
        await service.invalidate(TASK_ID, folder_id="paper-1")
        await service.task_details(TASK_ID)
        assert store.repair_calls[0][1] == ("child-1", "child-2")
        assert store.repair_calls[1][1] == ("child-1",)
        await service.close()

    asyncio.run(scenario())


def test_final_preprocessing_does_not_hide_cached_active_llm(monkeypatch):
    store = FakeSnapshots()
    active = store.active[str(TASK_ID)]["folders"][0]
    active["stage"] = "llm_in_progress"
    active["llm_body_running"] = True
    service = prepare_service(monkeypatch, store)

    async def scenario():
        await service.task_details(TASK_ID)
        store.saved[str(TASK_ID)] = saved_view(TASK_ID, final=True, version="two")
        result = await service.task_details(TASK_ID)
        folder = result["folders"][0]
        assert folder["preprocessing_result"] == "passed"
        assert folder["stage_snapshots"]["preprocessing"]["result_version"] == "two"
        assert folder["stage"] == "llm_in_progress" and folder["llm_body_running"] is True
        await service.close()

    asyncio.run(scenario())


def test_invalidating_one_folder_preserves_other_folder_progress(monkeypatch):
    store = FakeSnapshots()
    for views in (store.saved, store.active):
        second = copy.deepcopy(views[str(TASK_ID)]["folders"][0])
        second.update(ticket_id="child-2", folder_id="paper-2")
        views[str(TASK_ID)]["folders"].append(second)
    store.active[str(TASK_ID)]["folders"][1]["stage"] = "llm_in_progress"
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")

    async def children(task_id):
        return [SimpleNamespace(id="child-1", parameters={"group_id": "paper-1"}),
                SimpleNamespace(id="child-2", parameters={"group_id": "paper-2"})]

    monkeypatch.setattr(module, "_children", children)

    async def scenario():
        before = await service.task_details(TASK_ID)
        assert before["folders"][1]["stage"] == "llm_in_progress"
        await service.invalidate(TASK_ID, folder_id="paper-1")
        after = await service.task_details(TASK_ID)
        assert after["folders"][1]["stage"] == "llm_in_progress"
        await service.close()

    asyncio.run(scenario())


def test_list_failure_returns_old_list_or_unavailable(monkeypatch):
    store = FakeSnapshots()
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")
    fail = {"value": False}
    run = SimpleNamespace(id=TASK_ID, deployment_id=DEPLOYMENT_ID,
                          parameters={"task_name": "假任务"},
                          created=datetime(2026, 1, 2, tzinfo=timezone.utc))

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def read_flow_runs(self, **kwargs):
            if fail["value"]:
                raise OSError("offline")
            return [run]

    monkeypatch.setattr(module, "get_client", lambda: Client())

    async def scenario():
        old = await service.list_tasks()
        fail["value"] = True
        stale = await service.list_tasks()
        assert stale["query_status"] == "stale" and stale["as_of"] == old["as_of"]
        assert stale["tasks"] == old["tasks"]
        other = module.QueryService({"deployment_id": str(DEPLOYMENT_ID)})
        unavailable = await other.list_tasks()
        assert unavailable["query_status"] == "unavailable" and unavailable["as_of"] is None
        assert "tasks" not in unavailable or unavailable["tasks"] is None
        await other.close()
        await service.close()

    asyncio.run(scenario())


def test_late_saved_read_after_invalidation_cannot_replace_new_attempt(monkeypatch):
    store = FakeSnapshots()
    original = store.load_task_snapshot
    entered, release = None, None
    count = 0

    async def delayed(task_id, children, *, repair=False):
        nonlocal count
        result = await original(task_id, children, repair=repair)
        if not repair:
            count += 1
            if count == 2:
                entered.set()
                await release.wait()
        return result

    store.load_task_snapshot = delayed
    service = prepare_service(monkeypatch, store)

    async def scenario():
        nonlocal entered, release
        entered, release = asyncio.Event(), asyncio.Event()
        old = asyncio.create_task(service.task_details(TASK_ID))
        await asyncio.wait_for(entered.wait(), 1)
        await service.invalidate(TASK_ID, folder_id="paper-1")
        newer = saved_view(TASK_ID, attempt="retry-2", version="two")
        store.saved[str(TASK_ID)] = copy.deepcopy(newer)
        store.active[str(TASK_ID)] = copy.deepcopy(newer)
        current = await service.task_details(TASK_ID)
        release.set()
        late = await old
        for row in (current, late):
            assert row["folders"][0]["stage_snapshots"]["preprocessing"]["processing_attempt_id"] == "retry-2"
        await service.close()

    asyncio.run(scenario())


def test_history_repair_failure_keeps_old_view_and_does_not_rescan(monkeypatch):
    store = FakeSnapshots()
    store.saved[str(TASK_ID)]["missing"] = [{"ticket_id": "child-1", "stage": "llm"}]
    store.repair_error = OSError("isolated storage unavailable")
    service = prepare_service(monkeypatch, store)

    async def scenario():
        store.started = asyncio.Event()
        first = await service.task_details(TASK_ID)
        assert first["query_status"] == "preparing_history"
        await asyncio.wait_for(store.started.wait(), 1)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        failed = await service.task_details(TASK_ID)
        assert failed["query_status"] == "stale"
        assert failed["as_of"] == "2026-01-02T03:04:00+00:00"
        await service.task_details(TASK_ID)
        assert len(store.repair_calls) == 1
        await service.close()

    asyncio.run(scenario())


def test_one_cancelled_waiter_does_not_cancel_the_shared_refresh(monkeypatch):
    store = FakeSnapshots()
    service = prepare_service(monkeypatch, store)

    async def scenario():
        store.gate, store.started = asyncio.Event(), asyncio.Event()
        first = asyncio.create_task(service.task_details(TASK_ID))
        second = asyncio.create_task(service.task_details(TASK_ID))
        await asyncio.wait_for(store.started.wait(), 1)
        await asyncio.sleep(0)
        first.cancel()
        try:
            await first
        except asyncio.CancelledError:
            pass
        store.gate.set()
        response = await second
        assert response["task_id"] == str(TASK_ID)
        assert len(store.repair_calls) == 1 and store.open_contexts == 0
        await service.close()

    asyncio.run(scenario())


def test_refresh_passes_native_uuid_to_a(monkeypatch):
    store = FakeSnapshots()
    store.require_uuid = True
    service = prepare_service(monkeypatch, store)

    async def scenario():
        await service.task_details(TASK_ID)
        await service.close()

    asyncio.run(scenario())


def test_existing_active_stage_is_not_mistaken_for_missing_history(monkeypatch):
    store = FakeSnapshots()
    store.saved[str(TASK_ID)]["missing"] = [{
        "ticket_id": "child-1", "folder_id": "paper-1", "stage": "preprocessing",
    }]
    service = prepare_service(monkeypatch, store)

    async def scenario():
        result = await service.task_details(TASK_ID)
        assert result["query_status"] == "updated"
        assert len(store.repair_calls) == 1
        await service.close()

    asyncio.run(scenario())


def test_history_repair_after_folder_invalidation_targets_only_that_folder(monkeypatch):
    store = FakeSnapshots()
    store.saved[str(TASK_ID)]["missing"] = [{
        "ticket_id": "child-1", "folder_id": "paper-1", "stage": "llm",
    }]
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")

    async def children(task_id):
        return [SimpleNamespace(id="child-1", parameters={"group_id": "paper-1"}),
                SimpleNamespace(id="child-2", parameters={"group_id": "paper-2"})]

    monkeypatch.setattr(module, "_children", children)

    async def scenario():
        gate = store.gate = asyncio.Event()
        store.started = asyncio.Event()
        await service.task_details(TASK_ID)
        await asyncio.wait_for(store.started.wait(), 1)
        await service.invalidate(TASK_ID, folder_id="paper-1")
        store.gate = None
        store.started = asyncio.Event()
        await service.task_details(TASK_ID)
        await asyncio.wait_for(store.started.wait(), 1)
        assert store.repair_calls[0][1] == ("child-1", "child-2")
        assert store.repair_calls[1][1] == ("child-1",)
        gate.set()
        await service.close()

    asyncio.run(scenario())


def test_folder_generation_is_not_reused_after_full_cache_expiry(monkeypatch):
    store = FakeSnapshots()
    service = prepare_service(monkeypatch, store)

    async def scenario():
        loop = asyncio.get_running_loop()
        original = loop.call_later
        timers = []

        class Timer:
            def __init__(self, callback, args):
                self.callback, self.args = callback, args

            def cancel(self):
                pass

        def schedule(delay, callback, *args, **kwargs):
            if delay == 10:
                timer = Timer(callback, args)
                timers.append(timer)
                return timer
            return original(delay, callback, *args, **kwargs)

        monkeypatch.setattr(loop, "call_later", schedule)
        await service.task_details(TASK_ID)
        await service.invalidate(TASK_ID, folder_id="paper-1")
        store.active[str(TASK_ID)]["folders"][0]["llm_body_running"] = True
        await service.task_details(TASK_ID)
        timers[0].callback(*timers[0].args)
        await service.task_details(TASK_ID)
        await service.invalidate(TASK_ID, folder_id="paper-1")
        store.active[str(TASK_ID)]["folders"][0]["llm_body_running"] = False
        result = await service.task_details(TASK_ID)
        assert len(store.repair_calls) == 4
        assert result["folders"][0]["llm_body_running"] is False
        await service.close()

    asyncio.run(scenario())


def test_activity_refresh_failure_keeps_last_successful_activity(monkeypatch):
    store = FakeSnapshots()
    store.saved[str(TASK_ID)]["folders"][0]["stage"] = "waiting_llm"
    store.active[str(TASK_ID)]["folders"][0]["stage"] = "llm_in_progress"
    service = prepare_service(monkeypatch, store)

    async def scenario():
        old = await service.task_details(TASK_ID)
        assert old["folders"][0]["stage"] == "llm_in_progress"
        await service.invalidate(TASK_ID)
        store.repair_error = OSError("fake outage")
        failed = await service.task_details(TASK_ID)
        assert failed["query_status"] == "stale"
        assert failed["as_of"] == old["as_of"]
        assert failed["folders"][0]["stage"] == "llm_in_progress"
        await service.close()

    asyncio.run(scenario())


def test_new_saved_failure_replaces_cached_failure_details(monkeypatch):
    store = FakeSnapshots()
    for views in (store.saved, store.active):
        views[str(TASK_ID)]["folders"][0].update(failed_stage=None, failure_detail=None)
    service = prepare_service(monkeypatch, store)

    async def scenario():
        await service.task_details(TASK_ID)
        newer = saved_view(TASK_ID, final=True, version="failure-2")
        newer["folders"][0].update(stage="failed", failed_stage="preprocessing",
                                   preprocessing_result="failed", failure_detail="假解析失败")
        store.saved[str(TASK_ID)] = newer
        result = await service.task_details(TASK_ID)
        folder = result["folders"][0]
        assert folder["stage"] == "failed"
        assert folder["failed_stage"] == "preprocessing"
        assert folder["failure_detail"] == "假解析失败"
        await service.close()

    asyncio.run(scenario())


def test_new_attempt_replaces_old_failure_and_retry_identity(monkeypatch):
    store = FakeSnapshots()
    for views in (store.saved, store.active):
        folder = views[str(TASK_ID)]["folders"][0]
        folder.update(stage="failed", failed_stage="llm", llm_result="failed",
                      failure_detail="旧失败", last_retry={"retry_id": "old", "status": "failed"})
        folder["stage_snapshots"]["llm"] = {
            "processing_attempt_id": "old", "result_version": "old-version",
            "recorded_at": "2026-01-02T03:04:00+00:00", "final": True, "pending_request_count": 0,
        }
    service = prepare_service(monkeypatch, store)

    async def scenario():
        await service.task_details(TASK_ID)
        newer = copy.deepcopy(store.saved[str(TASK_ID)])
        folder = newer["folders"][0]
        folder.update(stage="llm_in_progress", failed_stage=None, failure_detail=None,
                      llm_result=None, last_retry={"retry_id": "new", "status": "running"})
        folder["stage_snapshots"]["llm"].update(
            processing_attempt_id="new", result_version="new-version", final=False,
        )
        store.saved[str(TASK_ID)] = newer
        result = await service.task_details(TASK_ID)
        folder = result["folders"][0]
        assert folder["stage"] == "llm_in_progress"
        assert folder["failed_stage"] is None and folder["failure_detail"] is None
        assert folder["last_retry"]["retry_id"] == "new"
        await service.close()

    asyncio.run(scenario())


def test_pause_without_settled_folder_evidence_stays_unresolved(monkeypatch):
    store = FakeSnapshots()
    service = prepare_service(monkeypatch, store)

    async def scenario():
        result = await service.task_details(TASK_ID)
        assert result["desired_state"] == "paused"
        assert result["task_state"] == "unresolved"
        assert result["actions"]["start"]["allowed"] is True
        assert result["actions"]["pause"]["allowed"] is False
        await service.close()

    asyncio.run(scenario())


def test_history_repair_includes_other_missing_folders_after_targeted_invalidation(monkeypatch):
    store = FakeSnapshots()
    saved = store.saved[str(TASK_ID)]
    second = copy.deepcopy(saved["folders"][0])
    second.update(ticket_id="child-2", folder_id="paper-2", stage_snapshots={})
    saved["folders"].append(second)
    saved["missing"] = [{"ticket_id": "child-2", "folder_id": "paper-2", "stage": "preprocessing"}]
    original = store.load_task_snapshot

    async def repair(task_id, children, *, repair=False):
        result = await original(task_id, children, repair=repair)
        if repair and any(str(child.id) == "child-2" for child in children):
            second["stage_snapshots"]["preprocessing"] = copy.deepcopy(
                saved["folders"][0]["stage_snapshots"]["preprocessing"]
            )
            saved["missing"] = []
            store.active[str(TASK_ID)] = copy.deepcopy(saved)
        return result

    store.load_task_snapshot = repair
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")

    async def children(task_id):
        return [SimpleNamespace(id="child-1", parameters={"group_id": "paper-1"}),
                SimpleNamespace(id="child-2", parameters={"group_id": "paper-2"})]

    monkeypatch.setattr(module, "_children", children)

    async def scenario():
        store.started = asyncio.Event()
        await service.invalidate(TASK_ID, folder_id="paper-1")
        result = await service.task_details(TASK_ID)
        assert result["query_status"] == "preparing_history"
        await asyncio.wait_for(store.started.wait(), 1)
        assert store.repair_calls[0][1] == ("child-1", "child-2")
        result = await service.task_details(TASK_ID)
        assert result["query_status"] != "preparing_history"
        assert "preprocessing" in result["folders"][1]["stage_snapshots"]
        await service.close()

    asyncio.run(scenario())


def test_confirmed_final_replaces_cached_unknown_stage(monkeypatch):
    store = FakeSnapshots()
    for views in (store.saved, store.active):
        folder = views[str(TASK_ID)]["folders"][0]
        folder.update(stage="unresolved", llm_result=None, last_retry={"stage": "llm", "status": "unresolved"})
        folder["stage_snapshots"]["llm"] = {
            "processing_attempt_id": "initial", "result_version": "old", "final": False,
            "recorded_at": "2026-01-02T03:04:00+00:00", "pending_request_count": 1,
        }
    service = prepare_service(monkeypatch, store)

    async def scenario():
        await service.task_details(TASK_ID)
        folder = store.saved[str(TASK_ID)]["folders"][0]
        folder.update(stage="waiting_postprocessing", llm_result="passed", last_retry=None)
        folder["stage_snapshots"]["llm"].update(result_version="new", final=True, pending_request_count=0)
        result = await service.task_details(TASK_ID)
        assert result["folders"][0]["stage"] == "waiting_postprocessing"
        assert result["folders"][0]["llm_result"] == "passed"
        assert result["folders"][0]["last_retry"] is None
        await service.close()

    asyncio.run(scenario())


def test_stale_result_does_not_claim_there_are_no_pending_operations(monkeypatch):
    store = FakeSnapshots()
    store.saved[str(TASK_ID)]["has_pending"] = False
    service = prepare_service(monkeypatch, store)

    async def scenario():
        old = await service.task_details(TASK_ID)
        assert old["has_pending"] is False
        store.error = OSError("fake outage")
        failed = await service.task_details(TASK_ID)
        assert failed["query_status"] == "stale" and failed["as_of"] == old["as_of"]
        assert failed["has_pending"] is None
        await service.close()

    asyncio.run(scenario())


def test_new_saved_final_is_visible_even_when_activity_refresh_fails(monkeypatch):
    store = FakeSnapshots()
    service = prepare_service(monkeypatch, store)

    async def scenario():
        old = await service.task_details(TASK_ID)
        await service.invalidate(TASK_ID)
        store.saved[str(TASK_ID)] = saved_view(TASK_ID, final=True, version="confirmed")
        store.repair_error = OSError("fake outage")
        failed = await service.task_details(TASK_ID)
        assert failed["query_status"] == "stale" and failed["as_of"] == old["as_of"]
        assert failed["folders"][0]["preprocessing_result"] == "passed"
        assert failed["folders"][0]["stage_snapshots"]["preprocessing"]["result_version"] == "confirmed"
        store.error = OSError("saved results now unavailable too")
        failed_again = await service.task_details(TASK_ID)
        assert failed_again["query_status"] == "stale" and failed_again["as_of"] == old["as_of"]
        assert failed_again["folders"][0]["preprocessing_result"] == "passed"
        await service.close()

    asyncio.run(scenario())


def test_late_read_without_invalidation_cannot_overwrite_newer_saved_result(monkeypatch):
    store = FakeSnapshots()
    original = store.load_task_snapshot
    entered, release = None, None
    count = 0

    async def delayed(task_id, children, *, repair=False):
        nonlocal count
        result = await original(task_id, children, repair=repair)
        if not repair:
            count += 1
            if count == 2:
                entered.set()
                await release.wait()
        return result

    async def old_summary(task_ids):
        return {str(TASK_ID): {"as_of": "2026-01-02T03:03:00+00:00",
                               "complete": False, "has_pending": False}}

    store.load_task_snapshot = delayed
    store.read_task_summaries = old_summary
    service = prepare_service(monkeypatch, store)

    async def scenario():
        nonlocal entered, release
        entered, release = asyncio.Event(), asyncio.Event()
        # Force overlapping underlying reads; public calls now share one read.
        old = asyncio.create_task(service._read_detail(str(TASK_ID), 0))
        await asyncio.wait_for(entered.wait(), 1)
        newer = saved_view(TASK_ID, final=True, version="confirmed")
        newer["as_of"] = "2026-01-02T03:05:00+00:00"
        store.saved[str(TASK_ID)] = newer
        current = await service._read_detail(str(TASK_ID), 0)
        assert current["folders"][0]["preprocessing_result"] == "passed"
        release.set()
        late = await old
        assert late["as_of"] == newer["as_of"]
        assert late["complete"] == newer["complete"] and late["has_pending"] == newer["has_pending"]
        store.error = OSError("fake outage")
        failed = await service.task_details(TASK_ID)
        assert failed["query_status"] == "stale"
        assert failed["as_of"] == newer["as_of"]
        assert failed["folders"][0]["preprocessing_result"] == "passed"
        await service.close()

    asyncio.run(scenario())


def test_late_older_stage_cannot_replace_final_when_batch_as_of_is_equal(monkeypatch):
    store = FakeSnapshots()
    store.saved[str(TASK_ID)]["as_of"] = "2026-01-02T03:05:00+00:00"
    store.active[str(TASK_ID)] = copy.deepcopy(store.saved[str(TASK_ID)])
    original = store.load_task_snapshot
    entered, release = None, None
    count = 0

    async def delayed(task_id, children, *, repair=False):
        nonlocal count
        result = await original(task_id, children, repair=repair)
        if not repair:
            count += 1
            if count == 2:
                entered.set()
                await release.wait()
        return result

    store.load_task_snapshot = delayed
    service = prepare_service(monkeypatch, store)

    async def scenario():
        nonlocal entered, release
        entered, release = asyncio.Event(), asyncio.Event()
        # Force overlapping underlying reads; public calls now share one read.
        old = asyncio.create_task(service._read_detail(str(TASK_ID), 0))
        await asyncio.wait_for(entered.wait(), 1)
        newer = saved_view(TASK_ID, final=True, version="confirmed")
        newer["as_of"] = "2026-01-02T03:05:00+00:00"
        newer["folders"][0]["stage_snapshots"]["preprocessing"]["recorded_at"] = "2026-01-02T03:04:30+00:00"
        store.saved[str(TASK_ID)] = newer
        assert (await service._read_detail(str(TASK_ID), 0))["folders"][0]["preprocessing_result"] == "passed"
        release.set()
        late = await old
        assert late["folders"][0]["preprocessing_result"] == "passed"
        store.error = OSError("fake outage")
        failed = await service.task_details(TASK_ID)
        assert failed["folders"][0]["preprocessing_result"] == "passed"
        await service.close()

    asyncio.run(scenario())


def test_running_task_uses_existing_business_projection_from_saved_evidence(monkeypatch):
    store = FakeSnapshots()
    for views in (store.saved, store.active):
        snapshot = views[str(TASK_ID)]
        snapshot["classification_complete"] = True
        snapshot["items"][0].update(delivery_complete=True, registration_status="registered")
        snapshot["folders"][0]["stage"] = "llm_in_progress"
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")

    class RunningAdmission:
        def __init__(self, task_id):
            pass

        async def running(self):
            return True

    async def children(task_id):
        state = SimpleNamespace(is_crashed=lambda: False)
        return [SimpleNamespace(id="child-1", state=state, parameters={"group_id": "paper-1"})]

    monkeypatch.setattr(module, "Admission", RunningAdmission)
    monkeypatch.setattr(module, "_children", children)

    async def scenario():
        result = await service.task_details(TASK_ID)
        assert result["desired_state"] == "running" and result["task_state"] == "running"
        store.saved[str(TASK_ID)]["items"][0].pop("delivery_complete")
        result = await service.task_details(TASK_ID)
        assert result["task_state"] == "unresolved"
        await service.close()

    asyncio.run(scenario())


def test_finished_classification_stops_its_own_polling_while_llm_is_active(monkeypatch):
    store = FakeSnapshots()
    for views in (store.saved, store.active):
        views[str(TASK_ID)].update(classification_complete=True, has_pending=True)
    service = prepare_service(monkeypatch, store)

    async def scenario():
        result = await service.classifications(TASK_ID)
        assert result["complete"] is True
        assert result["has_pending"] is False
        store.error = OSError("fake outage")
        stale = await service.classifications(TASK_ID)
        assert stale["query_status"] == "stale" and stale["has_pending"] is None
        await service.close()

    asyncio.run(scenario())


def test_prefect_client_closes_when_first_version_check_cannot_connect(monkeypatch):
    from prefect.client.orchestration import PrefectClient
    from prefect.context import AsyncClientContext as RealContext

    store = FakeSnapshots()
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")
    monkeypatch.setattr(module, "AsyncClientContext", RealContext)
    original_exit = PrefectClient.__aexit__
    closed = []

    async def fail_check(client):
        raise RuntimeError("version endpoint unavailable") from OSError("fake offline")

    async def record_exit(client, *args):
        result = await original_exit(client, *args)
        closed.append(client._closed)
        return result

    monkeypatch.setattr(PrefectClient, "raise_for_api_version_mismatch_once", fail_check)
    monkeypatch.setattr(PrefectClient, "__aexit__", record_exit)

    async def scenario():
        result = await service._refresh_snapshot(str(TASK_ID), 0, None, 0, [])
        assert result is None
        assert closed == [True]
        await service.close()

    asyncio.run(scenario())


def test_admission_read_owns_one_client_without_request_middleware(monkeypatch):
    from prefect.client.orchestration import PrefectClient
    from prefect.context import AsyncClientContext as RealContext
    from paperflow_v3.control.admission import Admission as RealAdmission

    store = FakeSnapshots()
    store.saved[str(TASK_ID)]["has_pending"] = False
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")
    monkeypatch.setattr(module, "AsyncClientContext", RealContext)
    monkeypatch.setattr(module, "Admission", RealAdmission)
    seen = []

    async def no_version_check(client):
        pass

    async def variable(client, name):
        context = RealContext.get()
        assert context is not None and context.client is client
        seen.append(client)
        return None

    monkeypatch.setattr(PrefectClient, "raise_for_api_version_mismatch_once", no_version_check)
    monkeypatch.setattr(PrefectClient, "read_variable_by_name", variable)

    async def scenario():
        result = await service.task_details(TASK_ID)
        assert result["desired_state"] == "paused"
        assert len(seen) == 2 and seen[0] is seen[1] and seen[0]._closed
        await service.close()

    asyncio.run(scenario())


def test_admission_first_version_failure_returns_stale_and_closes_client(monkeypatch):
    from prefect.client.orchestration import PrefectClient
    from prefect.context import AsyncClientContext as RealContext
    from paperflow_v3.control.admission import Admission as RealAdmission

    store = FakeSnapshots()
    store.saved[str(TASK_ID)]["has_pending"] = False
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")
    original_exit = PrefectClient.__aexit__
    closed = []

    async def fail_check(client):
        raise RuntimeError("version endpoint unavailable") from OSError("fake offline")

    async def record_exit(client, *args):
        result = await original_exit(client, *args)
        closed.append(client._closed)
        return result

    monkeypatch.setattr(PrefectClient, "raise_for_api_version_mismatch_once", fail_check)
    monkeypatch.setattr(PrefectClient, "__aexit__", record_exit)

    async def scenario():
        old = await service.task_details(TASK_ID)
        monkeypatch.setattr(module, "AsyncClientContext", RealContext)
        monkeypatch.setattr(module, "Admission", RealAdmission)
        failed = await service.task_details(TASK_ID)
        assert failed["query_status"] == "stale" and failed["as_of"] == old["as_of"]
        assert failed["has_pending"] is None and closed == [True]
        await service.close()

    asyncio.run(scenario())


def test_saved_delivery_final_advances_cached_waiting_stage(monkeypatch):
    async def scenario():
        for stage, waiting, after in (
            ("rag", "waiting_rag_upload", "waiting_nas_archive_delete"),
            ("nas", "waiting_nas_archive_delete", "waiting_cleanup"),
        ):
            store = FakeSnapshots()
            for views in (store.saved, store.active):
                folder = views[str(TASK_ID)]["folders"][0]
                folder["stage"] = waiting
                folder["stage_snapshots"][stage] = {
                    "processing_attempt_id": "initial", "result_version": "old", "final": False,
                    "recorded_at": "2026-01-02T03:04:00+00:00", "pending_request_count": 1,
                }
            service = prepare_service(monkeypatch, store)
            await service.task_details(TASK_ID)
            saved = store.saved[str(TASK_ID)]["folders"][0]
            saved["stage"] = after
            saved["stage_snapshots"][stage].update(result_version="new", final=True, pending_request_count=0)
            result = await service.task_details(TASK_ID)
            assert result["folders"][0]["stage"] == after
            await service.close()

    asyncio.run(scenario())


def test_simultaneous_detail_and_classifications_share_one_176_child_read(monkeypatch):
    store = FakeSnapshots()
    for views in (store.saved, store.active):
        views[str(TASK_ID)]["classification_complete"] = True
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")
    reads = []

    async def children(task_id):
        reads.append(str(task_id))
        return [SimpleNamespace(id=f"child-{number}", parameters={"group_id": f"paper-{number}"})
                for number in range(1, 177)]

    monkeypatch.setattr(module, "_children", children)

    async def scenario():
        store.gate, store.started = asyncio.Event(), asyncio.Event()
        detail = asyncio.create_task(service.task_details(TASK_ID))
        classifications = asyncio.create_task(service.classifications(TASK_ID))
        await asyncio.wait_for(store.started.wait(), 1)
        await asyncio.sleep(0)
        store.gate.set()
        result, classified = await asyncio.gather(detail, classifications)
        assert result["task_id"] == classified["task_id"] == str(TASK_ID)
        assert len(reads) == 1, f"176 child metadata was read {len(reads)} times"
        assert [len(child_ids) for child_ids in store.read_children].count(176) == 2
        assert [len(child_ids) for child_ids in store.read_children].count(0) == 1
        await service.close()

    asyncio.run(scenario())


def test_completed_classification_uses_parent_only_and_preserves_old_result_on_outage(monkeypatch):
    store = FakeSnapshots()
    store.saved[str(TASK_ID)]["classification_complete"] = True
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")

    async def no_children(task_id):
        raise AssertionError("completed classification must not enumerate child runs")

    monkeypatch.setattr(module, "_children", no_children)

    async def scenario():
        first = await service.classifications(TASK_ID)
        assert first["complete"] is True and first["has_pending"] is False
        assert store.read_children == [()]
        store.error = OSError("fake outage")
        stale = await service.classifications(TASK_ID)
        assert stale["query_status"] == "stale" and stale["as_of"] == first["as_of"]
        assert stale["items"] == first["items"] and stale["has_pending"] is None
        await service.close()

    asyncio.run(scenario())


def test_detail_reuses_one_real_prefect_client_across_read_boundaries(monkeypatch):
    from prefect.client.orchestration import PrefectClient, get_client as real_get_client
    from prefect.context import AsyncClientContext as RealContext
    from paperflow_v3.control.admission import Admission as RealAdmission

    store = FakeSnapshots()
    store.saved[str(TASK_ID)]["has_pending"] = False
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")
    seen = []

    async def no_version_check(client):
        pass

    async def read_run(client, task_id):
        seen.append(client)
        state = SimpleNamespace(is_final=lambda: False, is_crashed=lambda: False,
                                is_paused=lambda: False, is_failed=lambda: False,
                                is_completed=lambda: False, is_scheduled=lambda: False,
                                is_pending=lambda: False)
        return SimpleNamespace(id=task_id, deployment_id=DEPLOYMENT_ID,
                               parameters={"task_name": "假任务"}, state=state, run_count=1)

    async def variable(client, name):
        seen.append(client)
        return None

    async def children(task_id):
        async with real_get_client() as client:
            seen.append(client)
        return [SimpleNamespace(id="child-1", parameters={"group_id": "paper-1"})]

    async def snapshot(task_id, children, *, repair=False):
        async with real_get_client() as client:
            seen.append(client)
        return copy.deepcopy(store.saved[str(task_id)])

    async def summaries(task_ids):
        async with real_get_client() as client:
            seen.append(client)
        return {}

    monkeypatch.setattr(PrefectClient, "raise_for_api_version_mismatch_once", no_version_check)
    monkeypatch.setattr(PrefectClient, "read_flow_run", read_run)
    monkeypatch.setattr(PrefectClient, "read_variable_by_name", variable)
    monkeypatch.setattr(module, "get_client", real_get_client)
    monkeypatch.setattr(module, "AsyncClientContext", RealContext)
    monkeypatch.setattr(module, "Admission", RealAdmission)
    monkeypatch.setattr(module, "_children", children)
    monkeypatch.setattr(module, "load_task_snapshot", snapshot)
    monkeypatch.setattr(module, "read_task_summaries", summaries)

    async def scenario():
        result = await service.task_details(TASK_ID)
        assert result["desired_state"] == "paused"
        assert len(seen) == 6 and len({id(client) for client in seen}) == 1
        assert seen[0]._closed
        await service.close()

    asyncio.run(scenario())


def test_parent_classification_reuses_one_real_prefect_client(monkeypatch):
    from prefect.client.orchestration import PrefectClient, get_client as real_get_client
    from prefect.context import AsyncClientContext as RealContext

    store = FakeSnapshots()
    store.saved[str(TASK_ID)]["classification_complete"] = True
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")
    seen = []

    async def no_version_check(client):
        pass

    async def read_run(client, task_id):
        seen.append(client)
        return SimpleNamespace(id=task_id, deployment_id=DEPLOYMENT_ID)

    async def snapshot(task_id, children, *, repair=False):
        assert not children
        async with real_get_client() as client:
            seen.append(client)
        return copy.deepcopy(store.saved[str(task_id)])

    monkeypatch.setattr(PrefectClient, "raise_for_api_version_mismatch_once", no_version_check)
    monkeypatch.setattr(PrefectClient, "read_flow_run", read_run)
    monkeypatch.setattr(module, "get_client", real_get_client)
    monkeypatch.setattr(module, "AsyncClientContext", RealContext)
    monkeypatch.setattr(module, "load_task_snapshot", snapshot)

    async def scenario():
        result = await service.classifications(TASK_ID)
        assert result["complete"] is True
        assert len(seen) == 2 and seen[0] is seen[1] and seen[0]._closed
        await service.close()

    asyncio.run(scenario())


def test_shared_refresh_keeps_its_real_client_when_first_request_is_cancelled(monkeypatch):
    from prefect.client.orchestration import PrefectClient, get_client as real_get_client
    from prefect.context import AsyncClientContext as RealContext

    store = FakeSnapshots()
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")
    request_clients, refresh_clients = [], []
    started, gate = None, None

    async def no_version_check(client):
        pass

    async def read_run(client, task_id):
        request_clients.append(client)
        state = SimpleNamespace(is_final=lambda: False, is_crashed=lambda: False,
                                is_paused=lambda: False, is_failed=lambda: False,
                                is_completed=lambda: False, is_scheduled=lambda: False,
                                is_pending=lambda: False)
        return SimpleNamespace(id=task_id, deployment_id=DEPLOYMENT_ID,
                               parameters={"task_name": "假任务"}, state=state, run_count=1)

    async def snapshot(task_id, children, *, repair=False):
        async with real_get_client() as client:
            if repair:
                refresh_clients.append(client)
                started.set()
                await gate.wait()
                assert not client._closed
        return copy.deepcopy(store.saved[str(task_id)])

    monkeypatch.setattr(PrefectClient, "raise_for_api_version_mismatch_once", no_version_check)
    monkeypatch.setattr(PrefectClient, "read_flow_run", read_run)
    monkeypatch.setattr(module, "get_client", real_get_client)
    monkeypatch.setattr(module, "AsyncClientContext", RealContext)
    monkeypatch.setattr(module, "load_task_snapshot", snapshot)

    async def scenario():
        nonlocal started, gate
        started, gate = asyncio.Event(), asyncio.Event()
        first = asyncio.create_task(service.task_details(TASK_ID))
        second = asyncio.create_task(service.task_details(TASK_ID))
        await asyncio.wait_for(started.wait(), 2)
        await asyncio.sleep(0)
        first.cancel()
        try:
            await first
        except asyncio.CancelledError:
            pass
        assert len(request_clients) == len(refresh_clients) == 1
        assert all(refresh_clients[0] is not client for client in request_clients)
        assert not refresh_clients[0]._closed
        gate.set()
        result = await second
        assert result["task_id"] == str(TASK_ID)
        assert refresh_clients[0]._closed and all(client._closed for client in request_clients)
        await service.close()

    asyncio.run(scenario())


def test_classification_version_failure_closes_client_and_returns_own_stale_view(monkeypatch):
    from prefect.client.orchestration import PrefectClient
    from prefect.context import AsyncClientContext as RealContext

    store = FakeSnapshots()
    store.saved[str(TASK_ID)]["classification_complete"] = True
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")
    original_exit = PrefectClient.__aexit__
    closed = []

    async def fail_check(client):
        raise RuntimeError("version endpoint unavailable") from OSError("fake offline")

    async def record_exit(client, *args):
        result = await original_exit(client, *args)
        closed.append(client._closed)
        return result

    monkeypatch.setattr(PrefectClient, "raise_for_api_version_mismatch_once", fail_check)
    monkeypatch.setattr(PrefectClient, "__aexit__", record_exit)

    async def scenario():
        old = await service.classifications(TASK_ID)
        monkeypatch.setattr(module, "AsyncClientContext", RealContext)
        failed = await service.classifications(TASK_ID)
        assert failed["query_status"] == "stale" and failed["as_of"] == old["as_of"]
        assert failed["has_pending"] is None and closed == [True]
        await service.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("new_as_of", ["2026-01-02T03:04:00+00:00", "2026-01-02T03:05:00+00:00"])
def test_late_incomplete_classification_cannot_replace_newer_complete_view(monkeypatch, new_as_of):
    store = FakeSnapshots()
    store.saved[str(TASK_ID)]["classification_complete"] = False
    service = prepare_service(monkeypatch, store)
    entered, release = None, None

    original_load = service._load_snapshot

    async def gated_parent_snapshot(task_id, children, *, repair):
        result = await original_load(task_id, children, repair=repair)
        if not repair and asyncio.current_task().get_name() == "old-classification":
            assert children == []
            entered.set()
            await release.wait()
        return result

    monkeypatch.setattr(service, "_load_snapshot", gated_parent_snapshot)

    async def scenario():
        nonlocal entered, release
        entered, release = asyncio.Event(), asyncio.Event()
        # Force overlapping underlying reads; public calls now share one read.
        older = asyncio.create_task(service._read_classifications(str(TASK_ID), 0), name="old-classification")
        await asyncio.wait_for(entered.wait(), 1)
        store.saved[str(TASK_ID)].update(classification_complete=True, as_of=new_as_of)
        newer = await service._read_classifications(str(TASK_ID), 0)
        assert newer["complete"] is True
        release.set()
        late = await older
        assert late["complete"] is True and late["as_of"] == newer["as_of"]
        store.error = OSError("fake outage")
        stale = await service.classifications(TASK_ID)
        assert stale["query_status"] == "stale" and stale["complete"] is True
        assert stale["as_of"] == newer["as_of"] and stale["has_pending"] is None
        await service.close()

    asyncio.run(scenario())


def test_classification_falls_back_to_own_old_view_if_detail_becomes_unavailable(monkeypatch):
    store = FakeSnapshots()
    store.saved[str(TASK_ID)]["classification_complete"] = True
    service = prepare_service(monkeypatch, store)

    async def unavailable(task_id):
        return {"task_id": str(task_id), "query_status": "unavailable", "as_of": None,
                "complete": False, "has_pending": None}

    async def scenario():
        old = await service.classifications(TASK_ID)
        store.saved[str(TASK_ID)]["classification_complete"] = False
        monkeypatch.setattr(service, "task_details", unavailable)
        stale = await service.classifications(TASK_ID)
        assert stale["query_status"] == "stale" and stale["as_of"] == old["as_of"]
        assert stale["items"] == old["items"] and stale["has_pending"] is None
        await service.close()

    asyncio.run(scenario())


def test_invalidation_allows_new_incomplete_classification_at_same_timestamp(monkeypatch):
    store = FakeSnapshots()
    store.saved[str(TASK_ID)]["classification_complete"] = True
    service = prepare_service(monkeypatch, store)

    async def new_detail(task_id):
        return {**copy.deepcopy(store.saved[str(TASK_ID)]), "task_id": str(task_id), "query_status": "updated"}

    async def scenario():
        old = await service.classifications(TASK_ID)
        await service.invalidate(TASK_ID)
        store.saved[str(TASK_ID)]["classification_complete"] = False
        monkeypatch.setattr(service, "task_details", new_detail)
        current = await service.classifications(TASK_ID)
        assert old["as_of"] == current["as_of"]
        assert current["complete"] is False and current["has_pending"] is True
        await service.close()

    asyncio.run(scenario())


def test_twenty_concurrent_details_assemble_one_complete_view(monkeypatch):
    store = FakeSnapshots()
    view = store.saved[str(TASK_ID)]
    folder = view["folders"][0]
    item = view["items"][0]
    view["folders"] = [{**copy.deepcopy(folder), "ticket_id": f"child-{number}", "folder_id": f"paper-{number}"}
                       for number in range(1, 177)]
    view["items"] = [{**copy.deepcopy(item), "group_id": f"paper-{number}"} for number in range(1, 177)]
    view["has_pending"] = False
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")
    calls = []
    entered, release = None, None

    async def children(task_id):
        calls.append(str(task_id))
        entered.set()
        await release.wait()
        return [SimpleNamespace(id=f"child-{number}", parameters={"group_id": f"paper-{number}"})
                for number in range(1, 177)]

    monkeypatch.setattr(module, "_children", children)

    async def scenario():
        nonlocal entered, release
        entered, release = asyncio.Event(), asyncio.Event()
        requests = [asyncio.create_task(service.task_details(TASK_ID)) for _ in range(20)]
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.sleep(0)
        release.set()
        responses = await asyncio.gather(*requests)
        assert all(row["task_id"] == str(TASK_ID) for row in responses)
        assert all(len(row["folders"]) == 176 for row in responses)
        assert len(calls) == 1, f"176 child metadata was read {len(calls)} times"
        assert len(store.read_children) == 1 and len(store.read_children[0]) == 176
        await service.close()

    asyncio.run(scenario())


def test_twenty_concurrent_lists_share_one_read_but_next_request_is_fresh(monkeypatch):
    store = FakeSnapshots()
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")
    entered, release = None, None
    reads = []
    runs = [SimpleNamespace(id=TASK_ID, deployment_id=DEPLOYMENT_ID,
                            parameters={"task_name": "假任务"},
                            created=datetime(2026, 1, 2, tzinfo=timezone.utc))]

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def read_flow_runs(self, **kwargs):
            reads.append(1)
            entered.set()
            await release.wait()
            return list(runs)

    monkeypatch.setattr(module, "get_client", lambda: Client())

    async def scenario():
        nonlocal entered, release
        entered, release = asyncio.Event(), asyncio.Event()
        requests = [asyncio.create_task(service.list_tasks()) for _ in range(20)]
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.sleep(0)
        release.set()
        responses = await asyncio.gather(*requests)
        assert all(len(row["tasks"]) == 1 for row in responses)
        assert len(reads) == 1
        runs.append(SimpleNamespace(id=OTHER_ID, deployment_id=DEPLOYMENT_ID,
                                    parameters={"task_name": "新假任务"},
                                    created=datetime(2026, 1, 3, tzinfo=timezone.utc)))
        latest = await service.list_tasks()
        assert len(latest["tasks"]) == 2 and len(reads) == 2
        await service.close()

    asyncio.run(scenario())


def test_list_invalidation_does_not_join_an_older_inflight_read(monkeypatch):
    store = FakeSnapshots()
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")
    entered, release = None, None
    reads = []
    runs = [SimpleNamespace(id=TASK_ID, deployment_id=DEPLOYMENT_ID,
                            parameters={"task_name": "假任务"},
                            created=datetime(2026, 1, 2, tzinfo=timezone.utc))]

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def read_flow_runs(self, **kwargs):
            snapshot = list(runs)
            reads.append(snapshot)
            if len(reads) == 1:
                entered.set()
                await release.wait()
            return snapshot

    monkeypatch.setattr(module, "get_client", lambda: Client())

    async def scenario():
        nonlocal entered, release
        entered, release = asyncio.Event(), asyncio.Event()
        old = asyncio.create_task(service.list_tasks())
        await asyncio.wait_for(entered.wait(), 1)
        runs.append(SimpleNamespace(id=OTHER_ID, deployment_id=DEPLOYMENT_ID,
                                    parameters={"task_name": "新假任务"},
                                    created=datetime(2026, 1, 3, tzinfo=timezone.utc)))
        await service.invalidate(OTHER_ID)
        latest = await service.list_tasks()
        release.set()
        late = await old
        assert len(latest["tasks"]) == len(late["tasks"]) == 2
        await service.close()

    asyncio.run(scenario())


def test_detail_invalidation_during_completion_yield_rechecks_generation(monkeypatch):
    store = FakeSnapshots()
    store.saved[str(TASK_ID)]["has_pending"] = False
    service = prepare_service(monkeypatch, store)
    original_sleep = asyncio.sleep
    entered, release = None, None
    held = False

    async def controlled_sleep(delay):
        nonlocal held
        if delay == 0 and asyncio.current_task().get_name() == "old-detail" and not held:
            held = True
            entered.set()
            await release.wait()
        else:
            await original_sleep(delay)

    monkeypatch.setattr(asyncio, "sleep", controlled_sleep)

    async def scenario():
        nonlocal entered, release
        entered, release = asyncio.Event(), asyncio.Event()
        old = asyncio.create_task(service.task_details(TASK_ID), name="old-detail")
        await asyncio.wait_for(entered.wait(), 1)
        newer = saved_view(TASK_ID, final=True, attempt="retry-2", version="two")
        newer["has_pending"] = False
        store.saved[str(TASK_ID)] = newer
        await service.invalidate(TASK_ID, folder_id="paper-1")
        current = await service.task_details(TASK_ID)
        release.set()
        late = await old
        for row in (current, late):
            assert row["folders"][0]["stage_snapshots"]["preprocessing"]["processing_attempt_id"] == "retry-2"
        await service.close()

    asyncio.run(scenario())


def test_list_invalidation_during_completion_yield_rechecks_generation(monkeypatch):
    store = FakeSnapshots()
    service = prepare_service(monkeypatch, store)
    module = importlib.import_module("paperflow_v3.control.query_service")
    original_sleep = asyncio.sleep
    entered, release = None, None
    held = False
    runs = [SimpleNamespace(id=TASK_ID, deployment_id=DEPLOYMENT_ID,
                            parameters={"task_name": "假任务"},
                            created=datetime(2026, 1, 2, tzinfo=timezone.utc))]

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def read_flow_runs(self, **kwargs):
            return list(runs)

    async def controlled_sleep(delay):
        nonlocal held
        if delay == 0 and asyncio.current_task().get_name() == "old-list" and not held:
            held = True
            entered.set()
            await release.wait()
        else:
            await original_sleep(delay)

    monkeypatch.setattr(module, "get_client", lambda: Client())
    monkeypatch.setattr(asyncio, "sleep", controlled_sleep)

    async def scenario():
        nonlocal entered, release
        entered, release = asyncio.Event(), asyncio.Event()
        old = asyncio.create_task(service.list_tasks(), name="old-list")
        await asyncio.wait_for(entered.wait(), 1)
        runs.append(SimpleNamespace(id=OTHER_ID, deployment_id=DEPLOYMENT_ID,
                                    parameters={"task_name": "新假任务"},
                                    created=datetime(2026, 1, 3, tzinfo=timezone.utc)))
        await service.invalidate(OTHER_ID)
        current = await service.list_tasks()
        release.set()
        late = await old
        assert len(current["tasks"]) == len(late["tasks"]) == 2
        await service.close()

    asyncio.run(scenario())


def test_ten_concurrent_classifications_read_one_parent_snapshot(monkeypatch):
    store = FakeSnapshots()
    store.saved[str(TASK_ID)]["classification_complete"] = True
    original = store.load_task_snapshot
    entered, release = None, None
    calls = []

    async def gated(task_id, children, *, repair=False):
        calls.append((len(children), repair))
        entered.set()
        await release.wait()
        return await original(task_id, children, repair=repair)

    store.load_task_snapshot = gated
    service = prepare_service(monkeypatch, store)

    async def scenario():
        nonlocal entered, release
        entered, release = asyncio.Event(), asyncio.Event()
        requests = [asyncio.create_task(service.classifications(TASK_ID)) for _ in range(10)]
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.sleep(0)
        release.set()
        responses = await asyncio.gather(*requests)
        assert all(row["complete"] is True and row["has_pending"] is False for row in responses)
        assert calls == [(0, False)]
        store.saved[str(TASK_ID)]["items"][0]["status"] = "failed"
        store.saved[str(TASK_ID)]["as_of"] = "2026-01-02T03:05:00+00:00"
        latest = await service.classifications(TASK_ID)
        assert latest["counts"]["failed"] == 1
        assert calls == [(0, False), (0, False)]
        assert store.open_contexts == 0
        await service.close()

    asyncio.run(scenario())


def test_classification_invalidation_during_completion_yield_rechecks_generation(monkeypatch):
    store = FakeSnapshots()
    store.saved[str(TASK_ID)]["classification_complete"] = True
    service = prepare_service(monkeypatch, store)
    original_sleep = asyncio.sleep
    entered, release = None, None
    held = False

    async def controlled_sleep(delay):
        nonlocal held
        if delay == 0 and asyncio.current_task().get_name() == "old-classification" and not held:
            held = True
            entered.set()
            await release.wait()
        else:
            await original_sleep(delay)

    monkeypatch.setattr(asyncio, "sleep", controlled_sleep)

    async def scenario():
        nonlocal entered, release
        entered, release = asyncio.Event(), asyncio.Event()
        old = asyncio.create_task(service.classifications(TASK_ID), name="old-classification")
        await asyncio.wait_for(entered.wait(), 1)
        store.saved[str(TASK_ID)]["items"][0]["status"] = "failed"
        store.saved[str(TASK_ID)]["as_of"] = "2026-01-02T03:05:00+00:00"
        await service.invalidate(TASK_ID)
        current = await service.classifications(TASK_ID)
        release.set()
        late = await old
        assert current["counts"]["failed"] == late["counts"]["failed"] == 1
        await service.close()

    asyncio.run(scenario())


def test_first_cancelled_classification_does_not_cancel_other_waiter(monkeypatch):
    store = FakeSnapshots()
    store.saved[str(TASK_ID)]["classification_complete"] = True
    original = store.load_task_snapshot
    entered, release = None, None
    calls = []

    async def gated(task_id, children, *, repair=False):
        calls.append(1)
        entered.set()
        await release.wait()
        return await original(task_id, children, repair=repair)

    store.load_task_snapshot = gated
    service = prepare_service(monkeypatch, store)

    async def scenario():
        nonlocal entered, release
        entered, release = asyncio.Event(), asyncio.Event()
        first = asyncio.create_task(service.classifications(TASK_ID))
        second = asyncio.create_task(service.classifications(TASK_ID))
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.sleep(0)
        first.cancel()
        try:
            await first
        except asyncio.CancelledError:
            pass
        release.set()
        result = await second
        assert result["complete"] is True and len(calls) == 1
        assert store.open_contexts == 0
        await service.close()

    asyncio.run(scenario())
