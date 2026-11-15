"""Save per-paper outcome and request identities as Prefect server artifacts."""

import asyncio
import logging
import re
from contextlib import contextmanager
from contextvars import ContextVar
from uuid import uuid4

from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import ArtifactCreate
from prefect.client.schemas.filters import ArtifactFilter
from prefect.client.schemas.sorting import ArtifactSort

logger = logging.getLogger(__name__)
_processing_attempt = ContextVar("paperflow_processing_attempt", default=None)
PENDING_RECEIPT_KEY = "pfv3snapshotpending"
INDEXED_RECEIPT_TYPE = "paperflow-v3-receipt-indexed"
FOLDER_COMPLETION_TYPE = "paperflow-v3-folder-completion"
RECEIPT_TYPES = ["paperflow-v3-receipt", FOLDER_COMPLETION_TYPE]
SNAPSHOT_RECEIPT_KINDS = {
    "classification_complete", "task_complete", "dependency_failure", "manual_retry",
    "classification", "classification_started", "registration", "child_dispatch", "preprocessing_reviewed",
    "preprocessing_started", "preprocessing_complete", "preprocessing_failed", "llm_request_intent",
    "llm_request_started", "llm_request_unknown", "llm_request_not_sent", "llm_request_failed",
    "llm_gateway_not_dispatched",
    "llm_response_saved", "llm_folder_complete", "postprocessing_complete", "rag-body", "rag-attachments",
    "rag_effect_intent", "rag_effect_not_sent", "rag_effect_returned", "rag_effect_failed",
    "delivery_stage", "nas", "cleanup", "child_error",
}


@contextmanager
def processing_attempt(attempt_id):
    token = _processing_attempt.set(attempt_id)
    try:
        yield
    finally:
        _processing_attempt.reset(token)


def _artifact(*, task_id, folder_id, child_flow_run_id, event_id, kind, data):
    record = {
        "event_id": str(event_id),
        "task_id": str(task_id),
        "folder_id": folder_id,
        "kind": kind,
        "data": data,
    }
    if _processing_attempt.get():
        record["processing_attempt_id"] = _processing_attempt.get()
    key = "paperflow-v3-" + re.sub(r"[^a-z0-9]", "", str(event_id).lower())
    if kind in SNAPSHOT_RECEIPT_KINDS or kind.startswith("postprocessing_component_"):
        key += "-" + PENDING_RECEIPT_KEY + "-" + uuid4().hex
    return ArtifactCreate(
        key=key,
        type="paperflow-v3-receipt",
        flow_run_id=child_flow_run_id,
        data=record,
    )


async def save_receipt(*, task_id, folder_id, child_flow_run_id, event_id, kind, data):
    record = _artifact(task_id=task_id, folder_id=folder_id, child_flow_run_id=child_flow_run_id,
                       event_id=event_id, kind=kind, data=data)
    async with get_client() as client:
        artifact = await client.create_artifact(record)
        try:
            from .snapshots import save_receipt_snapshot
            indexed = await save_receipt_snapshot(client, record.data, artifact)
        except Exception as exc:
            logger.warning("Failed to save derived stage snapshot: %s", type(exc).__name__)
            try:
                from .snapshots import snapshot_gap
                await client.create_artifact(snapshot_gap(record.data, artifact))
            except Exception as gap_error:
                logger.warning("Failed to record snapshot gap: %s", type(gap_error).__name__)
        else:
            if indexed is not False and PENDING_RECEIPT_KEY in record.key:
                try:
                    await client.create_artifact(ArtifactCreate(
                        key=record.key, type=INDEXED_RECEIPT_TYPE, flow_run_id=artifact.flow_run_id,
                        data={"source_artifact_id": str(artifact.id)}))
                except Exception as exc:
                    logger.warning("Failed to mark receipt snapshot indexed: %s", type(exc).__name__)
    return artifact.id


def save_receipt_sync(*, task_id, folder_id, child_flow_run_id, event_id, kind, data):
    record = _artifact(task_id=task_id, folder_id=folder_id, child_flow_run_id=child_flow_run_id,
                       event_id=event_id, kind=kind, data=data)
    with get_client(sync_client=True) as client:
        artifact = client.create_artifact(record)
        try:
            from .snapshots import save_receipt_snapshot_sync
            indexed = save_receipt_snapshot_sync(client, record.data, artifact)
        except Exception as exc:
            logger.warning("Failed to save derived stage snapshot: %s", type(exc).__name__)
            try:
                from .snapshots import snapshot_gap
                client.create_artifact(snapshot_gap(record.data, artifact))
            except Exception as gap_error:
                logger.warning("Failed to record snapshot gap: %s", type(gap_error).__name__)
        else:
            if indexed is not False and PENDING_RECEIPT_KEY in record.key:
                try:
                    client.create_artifact(ArtifactCreate(
                        key=record.key, type=INDEXED_RECEIPT_TYPE, flow_run_id=artifact.flow_run_id,
                        data={"source_artifact_id": str(artifact.id)}))
                except Exception as exc:
                    logger.warning("Failed to mark receipt snapshot indexed: %s", type(exc).__name__)
    return artifact.id


def save_folder_completion(*, task_id, folder_id, child_flow_run_id, event_id, kind, data):
    """One final artifact; display snapshots are derived when read, never written."""
    if kind not in {"rag_complete", "nas", "cleanup"} or data.get("status") != "complete":
        raise ValueError("only completed folder stages can be published")
    identity = data.get('identity') or {}
    if (identity.get('task_id') != str(task_id) or identity.get('group_id') != folder_id
            or identity.get('task_run_id') != str(child_flow_run_id)):
        raise ValueError('folder completion identity does not match')
    data = {'identity': identity, 'status': 'complete'}
    record = _artifact(task_id=task_id, folder_id=folder_id, child_flow_run_id=child_flow_run_id,
                       event_id=event_id, kind=kind, data=data)
    record.key = "paperflow-v3-" + re.sub(r"[^a-z0-9]", "", event_id.lower())
    record.type = FOLDER_COMPLETION_TYPE
    record.data.pop('processing_attempt_id', None)
    record.data['folder_completion'] = True
    with get_client(sync_client=True) as client:
        existing = client.read_artifacts(artifact_filter=ArtifactFilter(
            key={"any_": [record.key]}, type={"any_": [FOLDER_COMPLETION_TYPE]},
            flow_run_id={"any_": [child_flow_run_id]}), sort=ArtifactSort.CREATED_DESC, limit=1)
        if existing:
            if existing[0].data != record.data:
                raise ValueError("saved folder completion changed")
            return existing[0].id
        return client.create_artifact(record).id


def receipt_data(row):
    return {**row.data, 'recorded_at': row.created.isoformat(),
            '_native_artifact_id': str(row.id)}


async def read_registration(task_id, folder_id):
    """Resolve one registration index to its full original, without parent history."""
    from .snapshots import _key, _originals_by_id, _pending_receipts, SNAPSHOT_TYPE
    from prefect.client.schemas.filters import ArtifactCollectionFilter
    from uuid import UUID

    parent = UUID(str(task_id))
    key = _key(str(parent), str(parent), folder_id, "registration", "initial", "final")
    async with get_client() as client:
        async def index():
            rows = await client.read_latest_artifacts(artifact_filter=ArtifactCollectionFilter(
                flow_run_id={"any_": [parent]}, key={"any_": [key]}, type={"any_": [SNAPSHOT_TYPE]}), limit=1)
            if len(rows) != 1:
                raise ValueError("论文登记索引缺失，需核对")
            row = rows[0]
            data = row.data
            if (str(row.flow_run_id) != str(parent) or data.get("task_id") != str(parent)
                    or data.get("folder_id") != folder_id or data.get("event_kind") != "registration"
                    or data.get("stage") != "registration" or data.get("final") is not True):
                raise ValueError("论文登记索引身份不一致")
            return row

        saved = await index()
        source_id = UUID(saved.data["source_artifact_id"])
        originals = await _originals_by_id(client, [source_id])
        if len(originals) != 1:
            raise ValueError("论文登记原始凭据缺失，需核对")
        source = originals[0]
        record = source.data
        if (source.id != source_id or str(source.flow_run_id) != str(parent)
                or record.get("task_id") != str(parent) or record.get("folder_id") != folder_id
                or record.get("kind") != "registration" or record.get("event_id") != saved.data.get("event_id")
                or not isinstance(record.get("data"), dict)):
            raise ValueError("论文登记原始凭据身份不一致")
        pending = [UUID(row.data["source_artifact_id"]) for row in await _pending_receipts(client, [parent])
                   if row.data.get("folder_id") == folder_id and row.data.get("event_kind") == "registration"
                   and row.data.get("source_artifact_id") != str(source_id)]
        if pending:
            newer = await _originals_by_id(client, pending)
            if len(newer) != len(set(pending)) or any(row.created >= source.created for row in newer):
                raise ValueError("论文登记存在更新的未确认索引，需核对")
        if (await index()).latest_id != saved.latest_id:
            raise ValueError("论文登记索引在核对期间变化，需重新核对")
        return record["data"]


async def read_receipts(child_flow_run_id):
    result = []
    async with get_client() as client:
        while True:
            rows = await client.read_artifacts(
                artifact_filter=ArtifactFilter(
                    flow_run_id={"any_": [child_flow_run_id]},
                    type={"any_": RECEIPT_TYPES},
                ),
                sort=ArtifactSort.CREATED_DESC,
                limit=200,
                offset=len(result),
            )
            result.extend(receipt_data(row) for row in rows)
            if len(rows) < 200:
                return result


def read_receipts_sync(child_flow_run_id):
    result = []
    with get_client(sync_client=True) as client:
        while True:
            rows = client.read_artifacts(
                artifact_filter=ArtifactFilter(
                    flow_run_id={"any_": [child_flow_run_id]},
                    type={"any_": RECEIPT_TYPES},
                ),
                sort=ArtifactSort.CREATED_DESC,
                limit=200,
                offset=len(result),
            )
            result.extend(receipt_data(row) for row in rows)
            if len(rows) < 200:
                return result


def read_source_receipts_sync(key_prefix):
    """Read a source's server request history, including earlier parent tasks."""
    result = []
    with get_client(sync_client=True) as client:
        while True:
            rows = client.read_artifacts(
                artifact_filter=ArtifactFilter(
                    key={"like_": key_prefix + "%"}, type={"any_": RECEIPT_TYPES},
                ),
                sort=ArtifactSort.CREATED_DESC,
                limit=200,
                offset=len(result),
            )
            result.extend(receipt_data(row) for row in rows)
            if len(rows) < 200:
                return result


async def read_folder_receipts(child_flow_run_ids):
    result = {str(run_id): [] for run_id in child_flow_run_ids}
    async with get_client() as client:
        for start in range(0, len(child_flow_run_ids), 200):
            run_ids = child_flow_run_ids[start:start + 200]
            offset = 0
            while True:
                rows = await client.read_artifacts(
                    artifact_filter=ArtifactFilter(
                        flow_run_id={"any_": run_ids}, type={"any_": RECEIPT_TYPES},
                    ),
                    sort=ArtifactSort.CREATED_DESC,
                    limit=200,
                    offset=offset,
                )
                for row in rows:
                    result[str(row.flow_run_id)].append(receipt_data(row))
                offset += len(rows)
                if len(rows) < 200:
                    break
    return result


async def read_folder_receipts_by_key(child_flow_run_ids, *, patterns=(), keys=()):
    """Read selected existing receipt keys without changing the original receipts."""
    result = {str(run_id): [] for run_id in child_flow_run_ids}
    semaphore = asyncio.Semaphore(16)
    async with get_client() as client:
        for start in range(0, len(child_flow_run_ids), 200):
            run_ids = child_flow_run_ids[start:start + 200]
            selections = [({"like_": pattern}, None) for pattern in patterns]
            if keys:
                selections.append(({"any_": list(keys)}, None))
                for key in dict.fromkeys(keys):
                    prefix = key + "-" + PENDING_RECEIPT_KEY + "-"
                    selections.append(({"like_": prefix}, prefix))

            async def read_selection(selection, prefix):
                offset = 0
                while True:
                    async with semaphore:
                        rows = await client.read_artifacts(
                            artifact_filter=ArtifactFilter(
                                flow_run_id={"any_": run_ids}, type={"any_": RECEIPT_TYPES},
                                key=selection,
                            ),
                            sort=ArtifactSort.CREATED_DESC, limit=200, offset=offset,
                        )
                    for row in rows:
                        if prefix is None or row.key.startswith(prefix):
                            result[str(row.flow_run_id)].append(row)
                    offset += len(rows)
                    if len(rows) < 200:
                        break
            async with asyncio.TaskGroup() as tasks:
                for selection, prefix in selections:
                    tasks.create_task(read_selection(selection, prefix))
    return result


async def read_latest_folder_snapshots(child_flow_run_ids):
    """Read only the latest per-paper progress snapshot for each existing kind."""
    result = {str(run_id): [] for run_id in child_flow_run_ids}
    semaphore = asyncio.Semaphore(16)
    async with get_client() as client:
        async def read_one(run_id, prefix):
            async with semaphore:
                rows = await client.read_artifacts(
                    artifact_filter=ArtifactFilter(
                        flow_run_id={"any_": [run_id]}, type={"any_": RECEIPT_TYPES},
                        key={"like_": "paperflow-v3-" + prefix},
                    ), sort=ArtifactSort.CREATED_DESC, limit=1, offset=0,
                )
            if rows:
                result[str(run_id)].append(rows[0])

        for start in range(0, len(child_flow_run_ids), 200):
            async with asyncio.TaskGroup() as tasks:
                for run_id in child_flow_run_ids[start:start + 200]:
                    for prefix in ("ragbody", "ragattachments", "nas", "cleanup", "folderragcomplete", "foldernas", "foldercleanup"):
                        tasks.create_task(read_one(run_id, prefix))
    return result


