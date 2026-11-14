"""Classify selected sources and dispatch native Prefect paper subflows."""

import asyncio
from functools import partial
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlsplit

from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowRunFilter, TaskRunFilter
from prefect.context import FlowRunContext, SyncClientContext
from prefect.deployments.flow_runs import arun_deployment

from .admission import pause_for_dependency
from .stage_admission import FolderStage, _stage_rows, unsettled_requests
from .classification import classify_and_register, first_step_modules, read_classification_records
from .receipts import read_receipts, save_receipt
from .sources import discover_sources
from .classification_summary import ClassificationSummary


CLASSIFICATION_CONCURRENCY = 5


async def restore_dispatch_keys(context):
    """Restore tracking numbers; a resumed parent may still have run_count=1."""
    async with get_client() as client:
        offset = 0
        while True:
            rows = await client.read_task_runs(task_run_filter=TaskRunFilter(
                flow_run_id={"any_": [context.flow_run.id]}), limit=200, offset=offset)
            for row in rows:
                if row.dynamic_key.isdecimal():
                    context.task_run_dynamic_keys[row.task_key] = max(
                        context.task_run_dynamic_keys.get(row.task_key, -1), int(row.dynamic_key))
            offset += len(rows)
            if len(rows) < 200:
                return


async def schedule_registered_paper(deployment_id, registered, admission):
    parent = FlowRunContext.get()
    if parent is None or str(parent.flow_run.id) != registered["task_id"]:
        raise ValueError("论文子流程必须由所属的大工单创建")
    if (registered["status"] != "passed" or registered["delivery_complete"] is not True
            or registered["registration_status"] != "registered"):
        raise ValueError("分类、交付和登记均通过后才能建立论文工单")
    if not await admission.running():
        raise InterruptedError("大工单已暂停，未登记论文工单")
    group_id = registered["group_id"]
    child = await arun_deployment(
        deployment_id,
        parameters={"task_id": registered["task_id"], "group_id": group_id, "registered": registered},
        idempotency_key="paperflow-v3-" + registered["task_id"] + "-" + group_id,
        tags=["paperflow-v3-paper"],
        timeout=0,
        as_subflow=True,
    )
    return child.id


async def _children(task_id):
    async with get_client() as client:
        selected=FlowRunFilter(parent_flow_run_id={"any_": [task_id]},tags={"all_": ["paperflow-v3-paper"]})
        async def page(offset):
            return await client.read_flow_runs(flow_run_filter=selected,limit=200,offset=offset)
        result=await page(0)
        if len(result)<200:return result
        count=await client.count_flow_runs(flow_run_filter=selected)
        for start in range(200,max(count,201),800):
            pages=await asyncio.gather(*(page(offset) for offset in range(start,min(start+800,max(count,201)),200)),return_exceptions=True)
            for rows in pages:
                if isinstance(rows,BaseException):raise rows
                result.extend(rows)
        # Include new arrivals if dispatch advanced while those pages were read.
        while len(result)%200==0:
            rows=await page(len(result));result.extend(rows)
            if len(rows)<200:break
        return result


async def _nimble_unavailable(first_step_root, endpoint, admission):
    if not await admission.running():
        return False
    probe = first_step_modules(first_step_root)["nimble_probe"]
    address = urlsplit(endpoint)
    try:
        await asyncio.wait_for(asyncio.to_thread(probe.probe_service, address.hostname, address.port, probe.MODEL),
                               timeout=7)
    except (probe.NimbleUnavailable, OSError, TimeoutError):
        return True
    return False


def _network_cause(exc):
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(exc, (URLError, TimeoutError, ConnectionError, OSError)):
            return True
        exc = exc.__cause__ or exc.__context__
    return False


async def classify_and_dispatch(*, task_id, source_paths, first_step_root, work_root, history_roots,
                                legacy_classification_root, nimble_endpoint, nimble_lock_path,
                                child_deployment_id, admission, forbidden_roots=()):
    """Save discovery and submit each eligible paper as soon as registration succeeds."""
    context = FlowRunContext.get()
    if context is None or context.flow_run.id != task_id:
        raise ValueError("分类必须在所属父工单中运行")
    from .recovery import read_scope
    if await read_scope(task_id) is not None:
        raise RuntimeError("本任务只恢复现有小工单，父单剩余分类和派发保持暂停")
    await restore_dispatch_keys(context)
    modules = first_step_modules(first_step_root)
    records = await read_receipts(task_id)
    summary = ClassificationSummary(task_id, records=records)
    blocked_groups = {row.data['folder_id'] for row in await _stage_rows(task_id)
                      if str(row.flow_run_id) == str(task_id) and row.data.get('stage') == 'classification'}
    plan = next((row["data"] for row in records if row["kind"] == "classification_plan"), None)
    if plan is None:
        await admission.wait()
        discovered = await asyncio.to_thread(discover_sources, source_paths, first_step_root,
                                             forbidden_roots=[work_root, *forbidden_roots])
        plan = {"source_paths": list(source_paths), **discovered}
        await save_receipt(task_id=task_id, folder_id="task", child_flow_run_id=task_id,
                           event_id=str(task_id) + "-classification-plan", kind="classification_plan", data=plan)
    elif plan["source_paths"] != list(source_paths):
        raise ValueError("资料选择与已保存的分类计划不一致")

    children = {}
    for child in await _children(task_id):
        group_id = child.parameters.get("group_id")
        if not group_id or group_id in children:
            raise ValueError("论文子工单身份重复或缺失，需核对")
        children[group_id] = child.id

    async def save_group(group_id, kind, suffix, data):
        await save_receipt(task_id=task_id, folder_id=group_id, child_flow_run_id=task_id,
                           event_id=str(task_id) + "-" + group_id + "-" + suffix, kind=kind, data=data)
        if kind in {"classification", "classification_started"}:
            summary.record(group_id, data.get("status", "unresolved"))

    async def classify_group(group):
        path, library = Path(group["group_path"]), Path(group["library_root"])
        group_id = modules["classification"]._group_id(path, library)
        if group_id in children:
            return None
        group_history = [row for row in records if row.get('folder_id') == group_id]
        def latest(kind):
            return next((row['data'] for row in group_history if row['kind'] == kind), None)
        registered = latest("registration")
        classification = latest("classification")
        source_state = {"data": latest("classification_source")}
        dispatch = latest("child_dispatch")
        if group_id in blocked_groups or unsettled_requests(group_history, 'classification'):
            if classification is None or classification.get('status') == 'passed':
                summary.record(group_id, 'unresolved')
            return None
        if dispatch and dispatch["state"] in ("intent", "unknown"):
            return None
        if registered is None and classification and (
            classification.get("status") in ("failed", "unrecognized", "unresolved")
            or classification.get("registration_status") == "failed"
        ):
            return None

        read_group = partial(read_classification_records, folder_id=group_id, source_path=path,
                             initial_records=group_history)
        phase = FolderStage(admission, {'task_id': str(task_id), 'group_id': group_id,
            'task_run_id': str(task_id)}, 'classification', read_records=read_group)
        await phase.__aenter__()

        try:
            def classify():
                with SyncClientContext.get_or_create():
                    return classify_and_register(first_step_root=first_step_root,
                        task_id=task_id, group_path=path, library_root=library, work_root=work_root,
                        admission=phase, nimble_endpoint=nimble_endpoint, nimble_lock_path=nimble_lock_path,
                        history_roots=history_roots, legacy_classification_root=legacy_classification_root,
                        source_state=source_state, group_history=group_history)

            while registered is None:
                try:
                    await phase.wait()
                    await save_group(group_id, "classification_started", "classification-started", {"group_id": group_id})
                    row = await modules["resources"].drain_thread(classify)
                except Exception as exc:
                    reason = str(exc) if isinstance(exc, modules["control"].UnknownRequest) else type(exc).__name__
                    await save_group(group_id, "classification", "classification-unknown",
                        {"group_id": group_id, "group": str(path), "status": "unresolved",
                         "reason": "分类或登记结果待核对：" + reason})
                    if (isinstance(exc, modules["control"].UnknownRequest) and _network_cause(exc)
                            and await _nimble_unavailable(first_step_root, nimble_endpoint, admission)):
                        await pause_for_dependency(task_id, group_id, "nimble")
                    phase.mark_unknown(reason)
                    await phase.__aexit__(type(exc), exc, exc.__traceback__)
                    return None
                if row.get("group_id") != group_id:
                    phase.mark_unknown("分类返回的资料组身份不一致")
                    await phase.__aexit__(ValueError, ValueError("资料组身份不一致"), None)
                    raise ValueError("分类返回的资料组身份不一致")
                summary.record(group_id, row.get("status", "unresolved"))
                if row.get("status") == "failed":
                    model = next((saved["data"] for saved in await read_group(task_id)
                                  if saved["folder_id"] == group_id and saved["kind"] == "classification_model"), {})
                    if model.get("state") == "failed" and model.get("http_status") == 401:
                        await pause_for_dependency(task_id, group_id, "nimble")
                if row.get("status") != "passed" or row.get("registration_status") == "failed":
                    if row.get("status") == "unresolved":
                        phase.mark_unknown(row.get("reason") or "分类结果待核对")
                    break
                if row.get("registration_status") == "registered":
                    registered = row
            if registered is None:
                error = RuntimeError(phase.unknown_reason) if phase.unknown_reason else None
                await phase.__aexit__(type(error) if error else None, error, None)
                return None
            return group_id, registered, phase

        except BaseException as exc:
            if not phase.closed:
                phase.mark_unknown("分类阶段结束结果待核对")
                await phase.__aexit__(type(exc), exc, exc.__traceback__)
            raise

    async def dispatch_group(group_id, registered, phase):
        try:
            await phase.wait()
            await save_group(group_id, "child_dispatch", "dispatch-intent", {"state": "intent"})
            try:
                child_id = await schedule_registered_paper(child_deployment_id, registered, phase)
            except InterruptedError:
                await save_group(group_id, "child_dispatch", "dispatch-not-sent", {"state": "not_sent"})
                raise
            except Exception as exc:
                await save_group(group_id, "child_dispatch", "dispatch-unknown",
                                 {"state": "unknown", "reason": type(exc).__name__})
                phase.mark_unknown("论文子工单创建结果待核对")
                await phase.__aexit__(type(exc), exc, exc.__traceback__)
                return
            await save_group(group_id, "child_dispatch", "dispatch-complete",
                             {"state": "complete", "child_flow_run_id": str(child_id)})
            children[group_id] = child_id
        except BaseException as exc:
            if not phase.closed:
                phase.mark_unknown("分类归队结果待核对")
                await phase.__aexit__(type(exc), exc, exc.__traceback__)
            raise
        else:
            await phase.__aexit__(None, None, None)

    publisher = asyncio.create_task(summary.publish())
    classification_finished = False
    try:
        remaining = iter(plan["groups"])
        async with asyncio.TaskGroup() as running:
            async def classify_worker():
                for group in remaining:
                    ready = await classify_group(group)
                    if ready is not None:
                        running.create_task(dispatch_group(*ready))

            for _ in range(min(CLASSIFICATION_CONCURRENCY, len(plan["groups"]))):
                running.create_task(classify_worker())
        classification_finished = True
    finally:
        await summary.close(complete=classification_finished)
        await publisher

    result = {"group_count": len(plan["groups"]), "ungrouped": plan["ungrouped"],
              "group_ids": [modules["classification"]._group_id(Path(group["group_path"]),
                             Path(group["library_root"])) for group in plan["groups"]]}
    await save_receipt(task_id=task_id, folder_id="task", child_flow_run_id=task_id,
                       event_id=str(task_id) + "-classification-complete", kind="classification_complete", data=result)
    return {**result, "children": [str(child_id) for child_id in children.values()]}
