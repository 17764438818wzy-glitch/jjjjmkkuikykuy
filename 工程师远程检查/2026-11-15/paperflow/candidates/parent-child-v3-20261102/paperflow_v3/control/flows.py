"""Deployment entry points for one parent order and its paper subflows."""

import asyncio
from datetime import datetime, timezone
import httpx
import os
from pathlib import Path
import sys
from urllib.error import HTTPError, URLError
from uuid import UUID

from prefect import flow
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowRunFilter
from prefect.context import FlowRunContext, SyncClientContext
from prefect.settings import PREFECT_API_URL

# Prefect's deployed file loader gives this file a temporary package name.
# Canonical imports keep phase grants and receipt context shared with stages.
from paperflow_v3.control.admission import Admission, pause_for_dependency
from paperflow_v3.control.stage_admission import FolderStage
from paperflow_v3.control.stage_queue import StageQueueWait, current_phase, worker_phase, requeue_phase
from paperflow_v3.control.classification import first_step_modules
from paperflow_v3.control.parent import _children, classify_and_dispatch
from paperflow_v3.control.receipts import (processing_attempt, read_folder_receipts, read_folder_receipts_by_key,
                                         read_receipts, read_registration, receipt_data, save_receipt)
from paperflow_v3.control.workspace import folder_settings
from paperflow_v3.stages.recovery import PostprocessingResourceWait
from paperflow_v3.stages.gateway_admission import GatewayCapacityWait


def load_runtime():
    """Load explicit worker configuration without starting a service."""
    project_config = Path(os.environ["PAPERFLOW_V3_PROJECT_CONFIG"]).resolve()
    first_step_root = Path(os.environ["PAPERFLOW_V3_FIRST_STEP_ROOT"]).resolve()
    project_root = Path(os.environ.get("PAPERFLOW_V3_PROJECT_ROOT", project_config.parent)).resolve()
    sys.path.insert(0, str(project_root))
    from paperflow_prefect.settings import Settings
    project = Settings.load(project_config)
    project.require_execution()
    if (not PREFECT_API_URL.value() or
            PREFECT_API_URL.value().rstrip("/") != project["prefect_api_url"].rstrip("/")):
        raise RuntimeError("工单执行器与项目配置的 Prefect 服务不一致")
    # First-step modules load their own Settings class. Bind them to this
    # executor's explicit project configuration, including its reviewed sources.
    os.environ['PAPERFLOW_CONFIG'] = str(project_config)
    return project, first_step_root


def resource_limits():
    prefix = "paperflow-connected-"
    return {
        "preprocessing": [prefix + "local"],
        "body": [prefix + "total_llm", prefix + "body_llm"],
        "attachment": [prefix + "total_llm", prefix + "attachment_llm"],
        "postprocessing": [prefix + "package"],
        "rag": [prefix + "rag"], "nas": [prefix + "nas"],
        "cleanup": [prefix + "cleanup"],
    }


async def _process_step(identity, registered, settings, admission, limits, retry_id, first_step_work_root=None,
                        records=None):
    """Call the existing stage adapter with this child's exclusive work root."""
    scoped = folder_settings(settings, identity["task_id"], identity["group_id"], registered["paper"])
    from paperflow_prefect import legacy
    legacy.bind(scoped)
    from paperflow_prefect.records import read_json
    from preextract_pipeline import offline_counter
    from paperflow_v3.stages.pipeline import process_paper

    core = scoped.path("release_root") / "core"
    # Work files are per paper, but the existing upstream backoff and
    # request-start settings remain shared under the configured data root.
    phase = current_phase()
    native_post = phase is not None and phase.names == tuple(sorted(limits.get('postprocessing', ())))
    connection = None if native_post else legacy.connection(settings)
    def counter_factory():
        return offline_counter(read_json(scoped.path("runtime_config")), core)
    counter_factory.recovery_settings = lambda: read_json(scoped.path("runtime_config"))['embedding_admission']['counter']
    return await process_paper(identity=identity, registered=registered, settings=scoped,
        connection=connection, admission=admission, limits=limits,
        catalog=read_json(scoped.path("catalog_path")), paperflow_root=core,
        counter_factory=counter_factory,
        output_root=scoped.path("data_root") / "postprocessing", retry_id=retry_id,
        first_step_work_root=first_step_work_root, records=records)


async def _delivery_step(identity, registered, settings, admission, limits, first_step_work_root,
                         retry_failed, retry_id=None, cleanup_only=False):
    """Call the real delivery entry on a synchronous business-operation thread."""
    from paperflow_prefect import legacy
    from paperflow_prefect.resources import drain_thread
    legacy.bind(settings)
    from paperflow_v3.delivery.runtime import deliver_paper

    def deliver():
        with SyncClientContext.get_or_create():
            return deliver_paper(identity=identity, registered=registered, settings=settings,
                admission=stage, limits=limits, first_step_work_root=first_step_work_root, retry_failed=retry_failed,
                **({'cleanup_only': True} if cleanup_only else {}))

    async def delivery_records(run_id):
        from paperflow_v3.delivery.progress import local_records, merge_records
        return merge_records(local_records(run_id), await read_receipts(run_id))

    waiting = limits['rag']
    if phase := current_phase():
        if phase.names in {tuple(sorted(limits[name])) for name in ('rag', 'nas', 'cleanup')}:
            waiting = phase.names
    async with FolderStage(admission, identity, 'delivery',
            deferred=True, retry_id=retry_id, wait_resources=waiting,
            read_records=delivery_records) as stage:
        try:
            return await drain_thread(deliver, stop=stage.stop)
        except (httpx.RequestError, TimeoutError):
            stage.mark_unknown('交付请求结果待核对')
            raise
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in {408, 504}:
                stage.mark_unknown('交付请求结果待核对')
            raise


def _latest(records, kind):
    return next((row["data"] for row in records if row["kind"] == kind), None)


_RAG_AUTH_ERROR = "RAGFlow 认证失败，请检查 API Key 是否有效及是否有知识库访问权限。"


async def _rag_unavailable(settings, admission):
    """Confirm a shared RAG fault with the existing guarded read-only client."""
    if not await admission.running():
        return False
    if settings.values.get('rag_backend') == 'qdrant':
        return await _qdrant_unavailable(settings, admission)
    from paperflow_prefect import legacy
    from paperflow_v3.delivery.transport import TaskCheckedClient
    from ragflow_client import RagflowConnectionError, RagflowHTTPError

    def probe():
        client = TaskCheckedClient(legacy.rag_client(settings), admission.running_sync, None)
        return client.request("GET", "/datasets?page=1&page_size=1")

    try:
        await asyncio.to_thread(probe)
    except RagflowConnectionError:
        return True
    except RagflowHTTPError as exc:
        return exc.status_code == 401
    except RuntimeError as exc:
        return type(exc) is RuntimeError and str(exc) == _RAG_AUTH_ERROR
    except Exception:
        return False
    return False


async def _model_unavailable(settings, admission):
    if not await admission.running():
        return False
    from paperflow_prefect import legacy
    try:
        connection = await asyncio.to_thread(legacy.connection, settings)
        base = connection["base_url"].rstrip("/").removesuffix("/chat/completions")
        if not await admission.running():
            return False
        async with httpx.AsyncClient(timeout=5, follow_redirects=False, trust_env=False) as client:
            response = await client.get(base + "/models", headers={"Authorization": "Bearer " + connection["api_key"]})
            response.raise_for_status()
            return not any(row.get("id") == connection["model"] for row in response.json()["data"])
    except (httpx.HTTPError, OSError, ValueError, KeyError, RuntimeError):
        return True


async def _nas_unavailable(settings, admission):
    if not await admission.running():
        return False
    destination = settings.path("nas_destination")
    try:
        available = await asyncio.wait_for(asyncio.to_thread(
            lambda: destination.is_dir() and os.access(destination, os.W_OK)), timeout=5)
    except (OSError, TimeoutError):
        return True
    return not available


async def _qdrant_unavailable(settings,admission):
    if not await admission.running(): return False
    from paperflow_v3.delivery.backend import probe_qdrant
    try:
        await probe_qdrant(settings)
    except (httpx.HTTPError,OSError,ValueError,KeyError):
        return True
    return False


def _qdrant_retryable(exc, settings, identity, records):
    """Only an interrupted request to the bound Qdrant upload can be resumed."""
    gateway = settings.values.get('qdrant_gateway_url', '')
    if settings.values.get('rag_backend') != 'qdrant' or not gateway:
        return False
    stage = _latest(records, 'delivery_stage') or {}
    if stage.get('identity') != identity or stage.get('stage') != 'rag_in_progress':
        return False
    bound = any(row.get('kind') in {'rag-backend', 'rag-qdrant-backend'}
                and row.get('task_id') == identity['task_id'] and row.get('folder_id') == identity['group_id']
                and (row.get('data') or {}).get('backend') == 'qdrant'
                and row['data'].get('gateway_url', '').rstrip('/') == gateway.rstrip('/')
                for row in records)
    if not bound:
        return False
    expected = httpx.URL(gateway)
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        try:
            requested = exc.request.url
            same_origin = (expected.scheme, expected.host, expected.port) == (
                requested.scheme, requested.host, requested.port)
        except (AttributeError, RuntimeError, ValueError):
            same_origin = False
        if same_origin and (isinstance(exc, httpx.RequestError) or
                isinstance(exc, httpx.HTTPStatusError) and
                (exc.response.status_code in (408, 429) or exc.response.status_code >= 500)):
            return True
        exc = exc.__cause__ or exc.__context__
    return False


async def _shared_dependency_failure(exc, stage, settings, admission, records=()):
    """Confirm shared authentication or RAG connectivity; keep paper errors local."""
    if stage == "processing":
        # Fresh model transports use the distinct-folder streak, not a /models probe.
        return None
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        nas = _latest(records, "nas") or {}
        if (stage == "delivery" and isinstance(exc, OSError)
                and any(row.get("status") in ("failed", "in_flight") for row in nas.get("files", {}).values())
                and await _nas_unavailable(settings, admission)):
            return "nas"
        rag = sys.modules.get("ragflow_client")
        rag_error = getattr(rag, "RagflowHTTPError", ()) if rag else ()
        if stage == "delivery" and isinstance(exc, rag_error) and exc.status_code == 401:
            return "rag"
        rag_connection = getattr(rag, "RagflowConnectionError", ()) if rag else ()
        if stage == "delivery" and (isinstance(exc, rag_connection)
                or (type(exc) is RuntimeError and str(exc) == _RAG_AUTH_ERROR)):
            if await _rag_unavailable(settings, admission):
                return "rag"
        is_qdrant_error=False
        if stage=='delivery' and isinstance(exc,httpx.HTTPError):
            try:
                gateway=httpx.URL(settings.values.get('qdrant_gateway_url',''))
                requested=exc.request.url
                is_qdrant_error=bool(gateway.host) and (gateway.scheme,gateway.host,gateway.port)==(
                    requested.scheme,requested.host,requested.port)
            except (AttributeError,RuntimeError,ValueError):
                pass
        if is_qdrant_error:
            if isinstance(exc,httpx.HTTPStatusError) and exc.response.status_code in (401,403):
                return 'rag'
            if (isinstance(exc,httpx.RequestError) or
                    isinstance(exc,httpx.HTTPStatusError) and exc.response.status_code>=500):
                if await _qdrant_unavailable(settings,admission): return 'rag'
        exc = exc.__cause__ or exc.__context__
    return None


async def run_paper(identity, registered, settings, admission, limits, first_step_work_root, retry_id=None,
                    records=None):
    """Run W2 then W3, or resume from the saved cleanup state for this child."""
    if (phase := current_phase()) and phase.names == tuple(sorted(limits['cleanup'])):
        try:
            return await _delivery_step(identity, registered, settings, admission, limits,
                first_step_work_root, retry_id is not None, retry_id=retry_id, cleanup_only=True)
        except (InterruptedError, StageQueueWait):
            raise
        except Exception as exc:
            from paperflow_v3.delivery.progress import FolderProgress
            FolderProgress(settings, identity, await read_receipts(identity['task_run_id']), None).save(
                'delivery_error', {'stage': 'delivery', 'delivery_substage': 'cleanup',
                                   'error_type': type(exc).__name__, 'reason': str(exc)[:500]})
            raise
    while True:
        if records is None:
            records = await read_receipts(identity["task_run_id"])
        cleanup = _latest(records, "cleanup")
        if cleanup and cleanup.get("status") == "complete":
            if cleanup.get("identity") != identity:
                raise ValueError("已保存的清理结果不属于此论文工单")
            if any(row['kind'] == 'cleanup' and row.get('folder_completion') for row in records):
                from paperflow_v3.delivery.progress import FolderProgress
                FolderProgress(settings, identity, records, None)
            return {"status": "complete", "cleanup_status": "complete"}
        if not current_phase():
            await admission.wait()
        stage = "delivery" if cleanup else "processing"
        try:
            llm = _latest(records, "llm_folder_complete") or {}
            post = _latest(records, "postprocessing_complete") or {}
            inventory = _latest(records, "stage_work_inventory") or {}
            if not cleanup and not (llm.get("status") == "raw_complete" and llm.get("ranking_complete") is True
                                    and post.get("validation_status") == "passed"
                                    and inventory.get("identity") == identity):
                await _process_step(identity, registered, settings, admission, limits, retry_id, first_step_work_root,
                                    records=records)
                records = await read_receipts(identity["task_run_id"])
                llm = _latest(records, "llm_folder_complete") or {}
                post = _latest(records, "postprocessing_complete") or {}
                inventory = _latest(records, "stage_work_inventory") or {}
                if not (llm.get("status") == "raw_complete" and llm.get("ranking_complete") is True
                        and post.get("validation_status") == "passed" and inventory.get("identity") == identity):
                    raise ValueError("论文处理服务器凭据不完整")
            if (phase := current_phase()) and phase.names == tuple(sorted(limits['postprocessing'])):
                from paperflow_v3.control.stage_admission import unsettled_requests
                if unsettled_requests(records, 'llm'):
                    raise RuntimeError('原模型请求结果待核对，未交接入库')
                # Completion and inventory are durable. Requeue this same native
                # child before delivery opens any registration or RAG resources.
                raise StageQueueWait(limits['rag'])
            stage = "delivery"
            result = await _delivery_step(identity, registered, settings, admission, limits,
                                          first_step_work_root, retry_id is not None, retry_id=retry_id)
            cleanup = _latest(await read_receipts(identity["task_run_id"]), "cleanup") or {}
            if cleanup.get("status") != "complete" or cleanup.get("identity") != identity:
                raise ValueError("论文清理完成凭据未保存在服务器")
            return result
        except InterruptedError:
            if await admission.running():
                raise
        except StageQueueWait:
            raise
        except GatewayCapacityWait as wait:
            # Capacity was refused before model dispatch. The original child
            # re-enters through admission after releasing every stage/slot.
            if phase := current_phase():
                raise StageQueueWait(phase.names, wait.seconds) from None
            await asyncio.sleep(wait.seconds)
        except PostprocessingResourceWait:
            # The local worker and package slot have been released. Re-enter
            # through the original parent admission and saved-response path.
            if current_phase():
                raise StageQueueWait(limits['postprocessing'], 30) from None
            await asyncio.sleep(30)
        except Exception as exc:
            records = await read_receipts(identity["task_run_id"])
            if stage == "delivery":
                from paperflow_v3.delivery.progress import local_records, merge_records
                records = merge_records(local_records(identity["task_run_id"]), records)
            dependency = await _shared_dependency_failure(exc, stage, settings, admission, records)
            if dependency:
                await pause_for_dependency(identity["task_id"], identity["group_id"], dependency)
            error = {"stage": stage, "error_type": type(exc).__name__, "reason": str(exc)[:500]}
            if stage == "delivery" and _qdrant_retryable(exc, settings, identity, records):
                error.update(backend="qdrant", delivery_substage="rag", retryable=True)
            if stage == 'delivery':
                from paperflow_v3.delivery.progress import FolderProgress
                FolderProgress(settings, identity, records, None).save('delivery_error', error)
            else:
                await save_receipt(task_id=identity["task_id"], folder_id=identity["group_id"],
                    child_flow_run_id=identity["task_run_id"], event_id=identity["task_run_id"] + "-" + stage + "-error",
                    kind="child_error", data=error)
            raise
        records = None


@flow(name="paperflow-v3-paper", persist_result=False, retries=0)
async def paper_order(task_id: str, group_id: str, registered: dict):
    context = FlowRunContext.get()
    child = context.flow_run
    from paperflow_v3.control.recovery import read_scope, check_child
    check_child(await read_scope(task_id), child)
    if child.parent_task_run_id is None:
        raise ValueError("论文工单缺少原生父工单")
    expected_deployment_id = UUID(os.environ["PAPERFLOW_V3_CHILD_DEPLOYMENT_ID"])
    async with get_client() as client:
        parent_task, expected_deployment = await asyncio.gather(
            client.read_task_run(child.parent_task_run_id),
            client.read_deployment(expected_deployment_id))
    if (child.deployment_id != expected_deployment_id
            or child.flow_id != expected_deployment.flow_id
            or child.idempotency_key != "paperflow-v3-" + task_id + "-" + group_id):
        raise ValueError("论文工单部署或幂等身份不一致，需核对")
    if (str(parent_task.flow_run_id) != task_id or registered.get("task_id") != task_id
            or registered.get("group_id") != group_id):
        raise ValueError("论文工单与父工单身份不一致")
    child_id = str(child.id)
    identity = {**{key: registered[key] for key in ("task_id", "group_id", "paper_id", "receipt_id")},
                "attempt_id": child_id, "parent_run_id": task_id, "task_run_id": child_id}
    records = await read_receipts(child.id)
    retry = _latest(records, "manual_retry") or {}
    retry_id = retry.get("retry_id") if retry.get("state") in ("authorized", "running") else None
    if retry.get("state") == "authorized":
        await save_receipt(task_id=task_id, folder_id=group_id, child_flow_run_id=child.id,
                           event_id=retry_id + "-running", kind="manual_retry",
                           data={**retry, "state": "running"})
        records = await read_receipts(child.id)
    settings, _ = load_runtime()
    first_root = settings.path("data_root") / "paperflow-v3" / task_id / "first-step"
    try:
        async with worker_phase(child):
            if current_phase().names != tuple(sorted(resource_limits()['cleanup'])):
                parent_registration = await read_registration(task_id, group_id)
                if parent_registration != registered:
                    raise ValueError("论文登记资料与服务器记录不一致")
            with processing_attempt(retry_id):
                return await run_paper(identity, registered, settings, Admission(task_id), resource_limits(), first_root,
                                       retry_id, records=records)
    except StageQueueWait as wait:
        await requeue_phase(identity, wait)


async def _excluded_classification_groups(task_id, classification):
    """Read explicit source-bound exclusions once, without hiding real tickets."""
    import hashlib
    from paperflow_v3.control.classification import read_classification_records
    from paperflow_v3.control.receipts import read_source_receipts_sync

    prefix = 'paperflow-v3-' + UUID(str(task_id)).hex + 'classificationexcluded%'
    selected = await read_folder_receipts_by_key([task_id], patterns=(prefix,))
    rows = selected[str(task_id)]
    if not rows:
        return set()
    groups = set(classification['group_ids'])
    if len(groups) != classification['group_count']:
        raise ValueError('原发现分组数量不一致，不能采用排除决定')
    modules = first_step_modules(Path(os.environ['PAPERFLOW_V3_FIRST_STEP_ROOT']))
    source = modules['classification']
    excluded = set()
    for row in sorted(rows, key=lambda item: item.created.isoformat(), reverse=True):
        record = receipt_data(row)
        data = record['data']
        group_id = record['folder_id']
        if group_id in excluded:
            continue
        if (str(row.flow_run_id) != str(task_id) or record.get('task_id') != str(task_id)
                or record.get('kind') != 'classification_exclusion' or group_id not in groups
                or data.get('schema') != 'paperflow-classification-exclusion/1'
                or data.get('state') != 'excluded' or data.get('group_id') != group_id
                or not data.get('reason')):
            raise ValueError('分类排除凭据身份不一致')
        UUID(data['authorization_ref'])
        group, library = source._group_paths(data['group_path'], data['library_root'])
        actual_id, snapshot, digest = await asyncio.to_thread(source._classification_snapshot, group, library)
        if (actual_id != group_id or not digest or snapshot != data.get('snapshot_id')
                or digest != data.get('source_digest')):
            raise ValueError('分类排除来源版本已改变')
        source_key = hashlib.sha256(str(group).casefold().encode('utf8')).hexdigest()
        history = await read_classification_records(task_id, group_id, group)
        history += await asyncio.to_thread(read_source_receipts_sync, 'paperflow-v3-firststep' + source_key)
        states = {}
        history.sort(key=lambda saved: datetime.fromisoformat(saved['recorded_at'])
                     if saved.get('recorded_at') else datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        for saved in history:
            kind, value = saved['kind'], saved['data']
            if kind in {'classification_model', 'child_dispatch'}:
                states.setdefault((saved['task_id'], kind, value.get('request_id')), value.get('state'))
        if any(state in {'intent', 'unknown'} for state in states.values()):
            raise ValueError('分类排除仍有原请求待核对')
        excluded.add(group_id)
    return excluded


async def _wait_for_completion(task_id, classification):
    excluded = await _excluded_classification_groups(task_id, classification)
    expected_groups = set(classification['group_ids']) - excluded
    while True:
        expected = len(expected_groups)
        if classification['group_count'] <= 0 or classification['ungrouped']:
            await asyncio.sleep(2)
            continue
        selected = dict(parent_flow_run_id={'any_': [task_id]}, tags={'all_': ['paperflow-v3-paper']})
        async with get_client() as client:
            total, completed_count = await asyncio.gather(
                client.count_flow_runs(flow_run_filter=FlowRunFilter(**selected)),
                client.count_flow_runs(flow_run_filter=FlowRunFilter(**selected, state={'type': {'any_': ['COMPLETED']}})))
        if total != expected or completed_count != expected:
            await asyncio.sleep(2)
            continue
        children = await _children(task_id)
        groups = [child.parameters.get("group_id") for child in children]
        if set(groups) & excluded:
            raise ValueError('已登记论文工单不能作为分类排除项')
        if (classification["group_count"] > 0 and not classification["ungrouped"]
                and len(children) == expected and len(set(groups)) == len(groups)
                and set(groups) == expected_groups
                and all(child.parameters.get("task_id") == str(task_id) and child.state.is_completed() for child in children)):
            selected = await read_folder_receipts_by_key([child.id for child in children],
                                                        patterns=('paperflow-v3-foldercleanup%',))
            records = {key: [receipt_data(row) for row in rows] for key, rows in selected.items()}
            # Older completed tickets can predate the native completion key.
            # Only their own missing proof is read; never all child histories.
            missing = [child.id for child in children if not _latest(records[str(child.id)], 'cleanup')]
            if missing:
                records.update(await read_folder_receipts(missing))
            def completed(child):
                cleanup = _latest(records[str(child.id)], "cleanup") or {}
                identity = cleanup.get("identity") or {}
                return (cleanup.get("status") == "complete" and identity.get("task_id") == str(task_id)
                        and identity.get("group_id") == child.parameters["group_id"]
                        and identity.get("task_run_id") == str(child.id))
            if all(completed(child) for child in children):
                if excluded and await _excluded_classification_groups(task_id, classification) != excluded:
                    raise ValueError('完成前分类排除决定发生变化')
                result = {"status": "complete", "paper_count": len(children)}
                if excluded:
                    result.update(discovered_group_count=classification['group_count'],
                                  excluded_count=len(excluded), excluded_group_ids=sorted(excluded))
                await save_receipt(task_id=task_id, folder_id="task", child_flow_run_id=task_id,
                                   event_id=str(task_id) + "-task-complete", kind="task_complete", data=result)
                return result
        await asyncio.sleep(2)


@flow(name="paperflow-v3-parent", persist_result=False, retries=0)
async def parent_order(task_name: str, source_paths: list[str]):
    task_id = FlowRunContext.get().flow_run.id
    admission = Admission(task_id)
    await admission.wait()
    settings, first_step_root = load_runtime()
    modules = first_step_modules(first_step_root)
    first_settings = modules["settings"].Settings(settings.values)
    shared_data = modules["settings"].first_step_shared_data(first_settings)
    history = modules["registration"].registration_history_roots(
        settings.path("data_root"), settings.values.get("history_roots", ()))
    root = settings.path("data_root") / "paperflow-v3" / str(task_id) / "first-step"
    classification = await classify_and_dispatch(task_id=task_id, source_paths=source_paths,
        first_step_root=first_step_root, work_root=root, history_roots=history,
        legacy_classification_root=settings.path("classification_root"),
        nimble_endpoint=settings["nimble_endpoint"], nimble_lock_path=shared_data / "control-locks" / "nimble-requests.lock",
        child_deployment_id=UUID(os.environ["PAPERFLOW_V3_CHILD_DEPLOYMENT_ID"]), admission=admission,
        forbidden_roots=[settings.path("classification_root"), settings.path("receipt_root")])
    return await _wait_for_completion(task_id, classification)
