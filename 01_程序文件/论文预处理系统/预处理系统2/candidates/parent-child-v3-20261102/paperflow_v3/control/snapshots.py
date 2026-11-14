"""Stage snapshots stored as Prefect artifacts beside the original receipts."""

import asyncio
import copy
import hashlib
import json
from datetime import datetime
from types import SimpleNamespace
from uuid import UUID

from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import ArtifactCreate
from prefect.client.schemas.filters import ArtifactCollectionFilter, ArtifactFilter
from prefect.client.schemas.sorting import ArtifactSort

from .folder_status import folder_status
from .receipts import PENDING_RECEIPT_KEY, processing_attempt, FOLDER_COMPLETION_TYPE
from .retry import retry_decision, uncertain_model_receipt
from paperflow_v3.stages.gateway_admission import has_proof


SNAPSHOT_TYPE = "paperflow-v3-stage-snapshot"
SUMMARY_TYPE = "paperflow-v3-task-summary"
HISTORY_TYPE = "paperflow-v3-history-checked"
HISTORY_VERSION = 2
GAP_TYPE = "paperflow-v3-snapshot-gap"
RESOLVED_TYPE = "paperflow-v3-snapshot-resolved"
STAGES = ("preprocessing", "llm", "postprocessing", "rag", "nas", "cleanup")
NETWORK_UNKNOWN = {"TimeoutError", "ConnectionError", "ConnectTimeout", "ReadTimeout", "ConnectError"}


def _key(*parts):
    digest = hashlib.sha256(json.dumps(parts, ensure_ascii=False).encode("utf8")).hexdigest()
    return "paperflow-v3-snapshot-" + digest


def _snapshot_artifact(receipt, source):
    kind, data = receipt["kind"], receipt.get("data") or {}
    task_id, folder_id = receipt["task_id"], receipt["folder_id"]
    recorded_at = source.created.isoformat()
    if kind in {"classification_complete", "task_complete", "dependency_failure"}:
        summary = {"task_id": task_id, "as_of": recorded_at, "source_kind": kind,
                   "has_pending": kind != "task_complete", "complete": kind == "task_complete"}
        if kind == "task_complete":
            summary["task_state"] = "completed"
        elif kind == "classification_complete":
            summary.update({key: data.get(key) for key in ("group_count", "group_ids", "ungrouped")})
        else:
            summary = {"task_id": task_id, "as_of": recorded_at, "source_kind": kind,
                       "stop_reason": data.get("reason") if data.get("status") == "active" else None}
        return ArtifactCreate(key=_key(task_id, kind), type=SUMMARY_TYPE,
                              flow_run_id=source.flow_run_id, data=summary)

    attempt = data.get("processing_attempt_id") or data.get("retry_id") or receipt.get("processing_attempt_id") or "initial"
    stage, slot, final, result, pending = None, "progress", False, None, None
    if kind == "manual_retry" and data.get("state") in {"authorized", "running"}:
        stage, attempt, slot = "_retry", data["retry_id"], data["retry_id"] + "-" + data["state"]
    elif kind in {"classification", "classification_started"}:
        stage = "classification"
        result = data.get("status") if data.get("status") in {"passed", "failed", "unrecognized"} else None
        final, pending = result is not None, 0 if result is not None else None
        slot = "final" if final else "progress"
    elif kind == "registration":
        stage, slot, final, result, pending = "registration", "final", True, "passed", 0
    elif kind == "child_dispatch":
        stage, slot = "_dispatch", "dispatch"
    elif kind == "preprocessing_reviewed":
        stage, slot = "_review", "review-" + str(data.get("failure_event_id"))
    elif kind in {"preprocessing_started", "preprocessing_complete", "preprocessing_failed"}:
        stage = "preprocessing"
        confirmed_failure = kind == "preprocessing_failed" and data.get("failure_reason") in {
            "local_io", "local_parse", "plan_unavailable"
        }
        final = kind == "preprocessing_complete" or confirmed_failure
        result = "passed" if kind == "preprocessing_complete" else "failed" if confirmed_failure else None
        pending, slot = (0, "final") if final else (None, "progress")
    elif kind in {"llm_request_intent", "llm_request_started", "llm_request_unknown", "llm_request_not_sent",
                  "llm_request_failed", "llm_response_saved", "llm_gateway_not_dispatched"}:
        stage, attempt = "llm", data.get("retry_id") or attempt
        slot = "request-" + str(data.get("request_id"))
        unknown = uncertain_model_receipt(kind, data) or kind == 'llm_gateway_not_dispatched' and not has_proof(data)
        pending = 1 if unknown or kind in {"llm_request_intent", "llm_request_started"} else 0
        result = "failed" if kind == "llm_request_failed" and not unknown else None
        if unknown:
            # Preserve explicit uncertainty beside later projections for this ID.
            slot += '-unresolved'
    elif kind == "llm_folder_complete":
        stage, slot = "llm", "final"
        final = data.get("status") == "raw_complete" and data.get("ranking_complete") is True
        result, pending = ("passed", 0) if final else (None, None)
    elif kind.startswith("postprocessing_component_"):
        stage = "postprocessing"
        if not (data.get("processing_attempt_id") or data.get("retry_id") or receipt.get("processing_attempt_id")):
            attempt = data.get("attempt_id") or attempt
        slot = "component-" + str(data.get("component_id"))
        uncertain = data.get("error_type") in NETWORK_UNKNOWN
        result = "failed" if kind == "postprocessing_component_failed" and not uncertain else None
        pending = None if uncertain else 1 if kind == "postprocessing_component_started" else 0
    elif kind == "postprocessing_complete":
        stage, slot = "postprocessing", "final"
        final = data.get("validation_status") == "passed"
        result, pending = ("passed", 0) if final else (None, None)
    elif kind == "rag_complete":
        stage, slot = "rag", "final"
        final = data.get("status") == "complete"
        result, pending = ("passed", 0) if final else (None, None)
    elif kind in {"rag-body", "rag-attachments"}:
        stage, slot = "rag", kind
    elif kind in {"rag_effect_intent", "rag_effect_not_sent", "rag_effect_returned", "rag_effect_failed"}:
        stage, slot = "rag", "effect-" + str(data.get("effect_id"))
        pending = 1 if kind == "rag_effect_intent" else 0
        result = "failed" if kind == "rag_effect_failed" else None
    elif kind == "delivery_stage":
        states = {"waiting_rag_upload": ("rag", False), "rag_in_progress": ("rag", False),
                  "waiting_nas_archive": ("rag", True), "nas_in_progress": ("nas", False),
                  "waiting_cleanup": ("nas", True), "cleanup_in_progress": ("cleanup", False)}
        if data.get("stage") not in states:
            return None
        stage, final = states[data["stage"]]
        slot, result, pending = ("final", "passed", 0) if final else ("progress", None, None)
    elif kind in {"nas", "cleanup"}:
        stage = kind
        files = data.get("files") or {}
        final = (data.get("status") == "complete" and (receipt.get('folder_completion') or
                 bool(files) and all(value.get("status") == "complete" for value in files.values())))
        result = "passed" if final else "failed" if any(value.get("status") == "failed" for value in files.values()) else None
        pending = 0 if final else sum(value.get("status") in {"in_flight", "unknown"} for value in files.values())
        slot = "final" if final else "progress"
    else:
        return None

    fields = ("component_id", "request_id", "kind", "retry_id", "retry_of", "state", "stage",
              "failure_reason", "reason", "error_type", "retryable", "request_sha256", "request_state_sha256",
              "error_response_sha256", "model_dir", "http_status", "status", "validation_status",
              "ranking_complete", "identity", "group_id", "group", "paper_title", "paper_title_candidate",
              "registration_status", "delivery_complete", "effect_id", "attempt_id", "task_id", "paper_id",
              "receipt_id", "processing_attempt_id", "failure_event_id", "receipt_sha256", "source_sha256",
              "delivery_sha256", "archive_sha256", "conversion_observation", "local_request_history",
              "downstream_events", "cached_docx_sha256", "observed_at", "gateway_binding",
              "gateway_proof_sha256", "native_state_sha256", "endpoint_sha256", "user_model_rerun")
    event_data = {key: data[key] for key in fields if key in data}
    if kind in {"nas", "cleanup"}:
        event_data["files"] = {key: {"status": value.get("status")} for key, value in (data.get("files") or {}).items()}
    if kind in {"rag-body", "rag-attachments"}:
        state_field = "body_delivery_state" if kind == "rag-body" else "delivery_state"
        event_data = {key: {state_field: value.get(state_field)} for key, value in data.items()}
    # Waiting must replace a previously cached started view in normal reads.
    artifact_type = RESOLVED_TYPE if kind in {"llm_response_saved", "llm_request_not_sent"} else SNAPSHOT_TYPE
    return ArtifactCreate(
        key=_key(task_id, str(source.flow_run_id), folder_id, stage, attempt, slot),
        type=artifact_type, flow_run_id=source.flow_run_id,
        data={"task_id": task_id, "folder_id": folder_id, "stage": stage,
              "snapshot_version": HISTORY_VERSION,
              "processing_attempt_id": attempt, "final": final, "pending_request_count": pending,
              "result": result, "recorded_at": recorded_at, "source_artifact_id": str(source.id),
              "event_id": receipt["event_id"], "event_kind": kind, "event_data": event_data},
    )


async def _originals(client, run_ids):
    rows = []
    for start in range(0, len(run_ids), 200):
        offset = 0
        while True:
            page = await client.read_artifacts(
                artifact_filter=ArtifactFilter(flow_run_id={"any_": run_ids[start:start + 200]},
                                               type={"any_": ["paperflow-v3-receipt"]}),
                sort=ArtifactSort.CREATED_DESC, limit=200, offset=offset,
            )
            rows.extend(page)
            offset += len(page)
            if len(page) < 200:
                break
    return rows


async def _originals_by_id(client, ids):
    rows = []
    for start in range(0, len(ids), 200):
        rows.extend(await client.read_artifacts(
            artifact_filter=ArtifactFilter(id={"any_": ids[start:start + 200]},
                                           type={"any_": ["paperflow-v3-receipt"]}),
            sort=ArtifactSort.CREATED_DESC, limit=200,
        ))
    return rows


def snapshot_gap(receipt, source):
    spec = _snapshot_artifact(receipt, source)
    stage = spec.data.get("stage") if spec and spec.type != SUMMARY_TYPE else "_summary"
    return ArtifactCreate(key=_key("gap", str(source.id)), type=GAP_TYPE, flow_run_id=source.flow_run_id,
                          data={"task_id": receipt["task_id"], "folder_id": receipt["folder_id"],
                                "stage": stage, "event_kind": receipt["kind"],
                                "source_artifact_id": str(source.id)})


def _receipt_gap(row):
    source = SimpleNamespace(id=row.latest_id, flow_run_id=row.flow_run_id, created=row.updated)
    gap = snapshot_gap(row.data, source)
    return SimpleNamespace(type=GAP_TYPE, key=row.key, flow_run_id=row.flow_run_id, data=gap.data)


async def _pending_receipts(client, run_ids):
    rows = []
    for start in range(0, len(run_ids), 200):
        prefixes = [""]
        while prefixes:
            prefix = prefixes.pop()
            page = await client.read_latest_artifacts(artifact_filter=ArtifactCollectionFilter(
                flow_run_id={"any_": run_ids[start:start + 200]}, type={"any_": ["paperflow-v3-receipt"]},
                key={"like_": PENDING_RECEIPT_KEY + "-" + prefix}), limit=200)
            if len(page) == 200:
                if len(prefix) == 32:
                    raise RuntimeError("Pending receipt keys cannot be partitioned safely")
                prefixes.extend(prefix + char for char in "0123456789abcdef")
            else:
                rows.extend(_receipt_gap(row) for row in page)
    return rows


def _pending_requests(rows, stage):
    latest, progress = {}, {}
    unknown = set()
    for row in rows:
        kind, data = row.data["kind"], row.data.get("data") or {}
        if stage == "llm" and kind in {"llm_request_intent", "llm_request_started", "llm_request_unknown",
                                       "llm_request_not_sent", "llm_request_failed", "llm_response_saved",
                                       "llm_gateway_not_dispatched"}:
            key = (data.get('component_id'), data.get('request_id'))
            if uncertain_model_receipt(kind, data):
                unknown.add(key)
            if kind == 'llm_gateway_not_dispatched' and not has_proof(data):
                kind = 'llm_request_unknown'
            latest.setdefault(key, kind)
        elif stage == "rag" and kind in {"rag_effect_intent", "rag_effect_not_sent",
                                         "rag_effect_returned", "rag_effect_failed"}:
            latest.setdefault(data.get("effect_id"), kind)
        elif stage == "rag" and kind in {"rag-body", "rag-attachments"}:
            progress.setdefault(kind, data)
    unresolved = {"llm_request_intent", "llm_request_started", "llm_request_unknown", "rag_effect_intent"}
    pending = len({key for key, kind in latest.items() if kind in unresolved} | unknown)
    for kind, values in progress.items():
        field = "body_delivery_state" if kind == "rag-body" else "delivery_state"
        pending += sum(value.get(field) in {"in_flight", "unknown"} for value in values.values())
    return pending


def _unknown_event(kind, data):
    if uncertain_model_receipt(kind, data):
        return True
    if kind == 'llm_gateway_not_dispatched':
        return not has_proof(data)
    if kind in {"llm_request_intent", "llm_request_started", "llm_request_unknown",
                "rag_effect_intent", "postprocessing_component_started"}:
        return True
    if kind == "preprocessing_failed":
        return data.get("failure_reason") not in {"local_io", "local_parse", "plan_unavailable"}
    if kind == "postprocessing_component_failed":
        return data.get("error_type") in NETWORK_UNKNOWN
    if kind in {"rag-body", "rag-attachments"}:
        field = "body_delivery_state" if kind == "rag-body" else "delivery_state"
        return any(value.get(field) in {"unknown", "in_flight"} for value in data.values())
    if kind in {"nas", "cleanup"}:
        return (data.get("status") in {"unknown", "in_flight"} or
                any(value.get("status") in {"unknown", "in_flight"} for value in data.get("files", {}).values()))
    return False


def _legacy_pending(rows, stage):
    if stage in {"llm", "rag"}:
        return _pending_requests(rows, stage)
    if stage == "postprocessing":
        latest = {}
        for row in rows:
            kind, data = row.data["kind"], row.data.get("data") or {}
            if kind.startswith("postprocessing_component_"):
                latest.setdefault(data.get("component_id"), (kind, data))
        return sum(kind == "postprocessing_component_started" or _unknown_event(kind, data)
                   for kind, data in latest.values())
    if stage in {"nas", "cleanup"}:
        latest = next((row.data.get("data") or {} for row in rows if row.data["kind"] == stage), None)
        return int(bool(latest and _unknown_event(stage, latest)))
    if stage == "preprocessing":
        latest = next((row.data for row in rows if row.data["kind"] in {
            "preprocessing_started", "preprocessing_failed", "preprocessing_complete"}), None)
        return int(bool(latest and (latest["kind"] == "preprocessing_started" or
                                   _unknown_event(latest["kind"], latest.get("data") or {}))))
    return 0


def _failed_snapshot(receipt, source, rows):
    if _pending_requests(rows, "llm") or _pending_requests(rows, "rag"):
        return None
    components, files = {}, {}
    for row in rows:
        kind, data = row.data["kind"], row.data.get("data") or {}
        if kind.startswith("postprocessing_component_"):
            components.setdefault(data.get("component_id"), (kind, data.get("error_type")))
        elif kind in {"nas", "cleanup"}:
            files.setdefault(kind, data.get("files") or {})
    if any(kind == "postprocessing_component_started" or error_type in NETWORK_UNKNOWN
           for kind, error_type in components.values()):
        return None
    if any(value.get("status") in {"in_flight", "unknown"}
           for values in files.values() for value in values.values()):
        return None
    projected = folder_status([row.data for row in rows])
    stage = projected["failed_stage"]
    candidates = {
        "preprocessing": {"preprocessing_failed"}, "llm": {"llm_request_failed"},
        "postprocessing": {"postprocessing_component_failed"},
        "rag": {"rag_effect_failed", "rag-body", "rag-attachments"},
        "nas": {"nas"}, "cleanup": {"cleanup"},
    }
    failed = next((row for row in rows if row.data["kind"] in candidates.get(stage, set())), None)
    if failed is None:
        return None
    original = {**failed.data}
    if receipt.get("processing_attempt_id"):
        original["processing_attempt_id"] = receipt["processing_attempt_id"]
    snapshot = _snapshot_artifact(original, failed)
    if snapshot is None:
        return None
    data = {**snapshot.data, "final": True, "result": "failed", "pending_request_count": 0, "stage_summary": True,
            "recorded_at": source.created.isoformat(), "source_artifact_id": str(source.id)}
    return snapshot.model_copy(update={"key": _key(receipt["task_id"], str(source.flow_run_id),
                                                   receipt["folder_id"], stage,
                                                   data["processing_attempt_id"], "final"), "data": data})


def _snapshot_evidence(rows):
    evidence = []
    for row in rows:
        if row.type not in {SNAPSHOT_TYPE, RESOLVED_TYPE} or "event_kind" not in row.data:
            continue
        data = row.data
        evidence.append(SimpleNamespace(
            id=UUID(data["source_artifact_id"]), flow_run_id=row.flow_run_id,
            created=datetime.fromisoformat(data["recorded_at"]),
            data={"kind": data["event_kind"], "data": data["event_data"], "event_id": data["event_id"],
                  "task_id": data["task_id"], "folder_id": data["folder_id"],
                  "processing_attempt_id": data["processing_attempt_id"]},
        ))
    return sorted(evidence, key=lambda row: row.created, reverse=True)


def _checked_snapshot(receipt, source, snapshot, rows):
    from .attempts import active_snapshot_rows
    rows = active_snapshot_rows(rows)
    gaps = [row for row in rows if row.type == GAP_TYPE]
    evidence = _snapshot_evidence(rows)
    if receipt["kind"] == "child_error":
        return None if gaps else _failed_snapshot(receipt, source, evidence)
    if snapshot and snapshot.type == SNAPSHOT_TYPE and snapshot.data["final"] and snapshot.data["stage"] in {"llm", "rag"}:
        stage = snapshot.data["stage"]
        related = [row for row in gaps if row.data["stage"] in {stage, "_summary"}]
        pending = _pending_requests(evidence, stage)
        gap_pending = len({row.data["source_artifact_id"] for row in related
                           if row.data["event_kind"] in {"llm_request_intent", "llm_request_started",
                                                         "llm_request_unknown", "rag_effect_intent"}})
        if pending or related:
            count = None if related and pending else pending or gap_pending or None
            snapshot.data.update(final=False, result=None, pending_request_count=count)
    return snapshot


async def save_receipt_snapshot(client, receipt, source):
    snapshot = _snapshot_artifact(receipt, source)
    final_receipt = bool(snapshot and snapshot.type == SNAPSHOT_TYPE and snapshot.data["final"]
                         and snapshot.data["stage"] in {"llm", "rag"})
    if receipt["kind"] == "child_error" or final_receipt:
        pending = await _pending_receipts(client, [source.flow_run_id])
        rows = await _collections(client, [source.flow_run_id], [SNAPSHOT_TYPE, GAP_TYPE])
        rows.extend(row for row in pending if row.data["source_artifact_id"] != str(source.id))
        snapshot = _checked_snapshot(receipt, source, snapshot, rows)
    if snapshot is not None:
        await client.create_artifact(snapshot)
    return ((snapshot is not None or receipt["kind"] != "child_error")
            and (not final_receipt or snapshot.data["final"]))


def _collections_sync(client, run_id):
    rows, prefixes = [], [""]
    while prefixes:
        prefix = prefixes.pop()
        page = client.read_latest_artifacts(artifact_filter=ArtifactCollectionFilter(
            flow_run_id={"any_": [run_id]}, type={"any_": ["paperflow-v3-receipt"]},
            key={"like_": PENDING_RECEIPT_KEY + "-" + prefix}), limit=200)
        if len(page) == 200:
            if len(prefix) == 32:
                raise RuntimeError("Pending receipt keys cannot be partitioned safely")
            prefixes.extend(prefix + char for char in "0123456789abcdef")
        else:
            rows.extend(_receipt_gap(row) for row in page)
    offset = 0
    while True:
        page = client.read_latest_artifacts(
            artifact_filter=ArtifactCollectionFilter(flow_run_id={"any_": [run_id]},
                                                     type={"any_": [SNAPSHOT_TYPE, GAP_TYPE]}),
            limit=200, offset=offset,
        )
        rows.extend(page)
        offset += len(page)
        if len(page) < 200:
            return rows


def save_receipt_snapshot_sync(client, receipt, source):
    snapshot = _snapshot_artifact(receipt, source)
    final_receipt = bool(snapshot and snapshot.type == SNAPSHOT_TYPE and snapshot.data["final"]
                         and snapshot.data["stage"] in {"llm", "rag"})
    if receipt["kind"] == "child_error" or final_receipt:
        rows = [row for row in _collections_sync(client, source.flow_run_id)
                if row.type != GAP_TYPE or row.data["source_artifact_id"] != str(source.id)]
        snapshot = _checked_snapshot(receipt, source, snapshot, rows)
    if snapshot is not None:
        client.create_artifact(snapshot)
    return ((snapshot is not None or receipt["kind"] != "child_error")
            and (not final_receipt or snapshot.data["final"]))


async def _collections(client, run_ids, types):
    rows = []
    selected_types = list(types)
    if SNAPSHOT_TYPE in types:
        selected_types.append(FOLDER_COMPLETION_TYPE)
    async def batch(ids):
        result=[]
        offset = 0
        while True:
            page = await client.read_latest_artifacts(
                artifact_filter=ArtifactCollectionFilter(flow_run_id={"any_": ids},
                                                          type={"any_": selected_types}),
                limit=200, offset=offset,
            )
            result.extend(_completion_snapshot(row) if row.type == FOLDER_COMPLETION_TYPE else row for row in page)
            offset += len(page)
            if len(page) < 200:
                return result
    for start in range(0,len(run_ids),800):
        results=await asyncio.gather(*(batch(run_ids[offset:offset+200]) for offset in range(start,min(start+800,len(run_ids)),200)),return_exceptions=True)
        for result in results:
            if isinstance(result,BaseException):raise result
            rows.extend(result)
    return rows


def _completion_snapshot(row):
    source = SimpleNamespace(id=row.latest_id, created=row.created, flow_run_id=row.flow_run_id)
    spec = _snapshot_artifact(row.data, source)
    spec.data['folder_completion'] = True
    return SimpleNamespace(type=SNAPSHOT_TYPE, key=spec.key, data=spec.data,
                           latest_id=row.latest_id, created=row.created, flow_run_id=row.flow_run_id)


async def read_task_summaries(task_ids):
    if not task_ids:
        return {}
    async with get_client() as client:
        rows = await _collections(client, list(task_ids), [SUMMARY_TYPE])
    by_task = {}
    for row in sorted(rows, key=lambda item: item.data["as_of"], reverse=True):
        task_id = row.data["task_id"]
        by_task.setdefault(task_id, {}).setdefault(row.data["source_kind"], dict(row.data))
    summaries = {}
    for task_id, sources in by_task.items():
        result = dict(sources.get("task_complete") or sources.get("classification_complete") or
                      sources["dependency_failure"])
        if "classification_complete" in sources and "task_complete" in sources:
            for key in ("group_count", "group_ids", "ungrouped"):
                result[key] = sources["classification_complete"].get(key)
        if "dependency_failure" in sources:
            result["stop_reason"] = sources["dependency_failure"].get("stop_reason")
        result["as_of"] = max(source["as_of"] for source in sources.values())
        summaries[task_id] = result
    return summaries


def _attempt_for(stage, rows):
    retries = [row for row in rows if row.data["stage"] == "_retry"
               and row.data["event_data"].get("state") == "authorized"]
    retries.sort(key=lambda row: row.data["recorded_at"], reverse=True)
    for row in retries:
        retry_id = row.data["processing_attempt_id"]
        if row.data["event_data"].get("stage") == stage or any(
                item.data["stage"] == stage and item.data["processing_attempt_id"] == retry_id for item in rows):
            return retry_id, row
    initial = [row for row in rows if row.data["stage"] == stage]
    first = min(initial, key=lambda row: row.data["recorded_at"], default=None)
    return (first.data["processing_attempt_id"] if first else "initial"), None


def _stage_view(stage, rows, attempt, retry):
    from .attempts import active_snapshot_rows
    rows = active_snapshot_rows(rows)
    completed = [row for row in rows if row.data['stage'] == stage and row.data.get('folder_completion')]
    if completed:
        row = max(completed, key=lambda row: row.data['recorded_at'])
        return {'processing_attempt_id': attempt, 'final': True, 'pending_request_count': 0,
                'result': 'passed', 'recorded_at': row.data['recorded_at'],
                'result_version': str(row.latest_id)}, [row]
    rows = [row for row in rows if row.data["stage"] == stage
            and (row.data["processing_attempt_id"] == attempt or stage == 'llm'
                 and uncertain_model_receipt(row.data['event_kind'], row.data['event_data']))]
    rows.sort(key=lambda row: row.data["recorded_at"], reverse=True)
    finals = [row for row in rows if row.data["final"]]
    requests = [row for row in rows if row.data["event_kind"].startswith("llm_request_")
                and not row.data.get("stage_summary")]
    if stage == "llm":
        requests += [row for row in rows if row.data["event_kind"] in {'llm_response_saved', 'llm_gateway_not_dispatched'}]
    elif stage == "rag":
        requests = [row for row in rows if row.data["event_kind"].startswith("rag_effect_")]
    model_unknown = []
    if stage == 'llm':
        unique = {}
        for row in requests:
            kind, data = row.data['event_kind'], row.data['event_data']
            key = (data.get('component_id'), data.get('request_id'))
            uncertain = uncertain_model_receipt(kind, data) or kind == 'llm_gateway_not_dispatched' and not has_proof(data)
            if uncertain:
                model_unknown.append(row)
            if key not in unique or uncertain:
                unique[key] = row
        requests = list(unique.values())
    pending = max(sum(1 if row in model_unknown else row.data["pending_request_count"] or 0 for row in requests),
                  max((row.data["pending_request_count"] or 0 for row in rows
                       if (row.data["event_kind"] == "llm_folder_complete" or row.data.get("stage_summary"))
                       and not row.data["final"]), default=0))
    failed = any(row.data["event_kind"] == "llm_request_failed" and row not in model_unknown for row in requests)
    if stage == "classification" and rows:
        finals = [rows[0]] if rows[0].data["final"] else []
    later_unknown = list(model_unknown)
    if finals:
        for row in rows:
            if row.data["recorded_at"] <= finals[0].data["recorded_at"]:
                continue
            if _unknown_event(row.data["event_kind"], row.data["event_data"]) and row not in later_unknown:
                later_unknown.append(row)
    chosen = (later_unknown[0] if later_unknown else finals[0] if finals else rows[0] if rows else retry)
    if chosen is None:
        return None, []
    final = bool(finals) and not later_unknown
    result = (None if later_unknown else finals[0].data["result"] if final else
              "failed" if failed else chosen.data.get("result"))
    known_gateway_wait = any(row.data['event_kind'] == 'llm_gateway_not_dispatched' and
                             has_proof(row.data['event_data']) for row in requests)
    count = (0 if final else pending if requests and (pending or known_gateway_wait) else
             None if stage in {"llm", "rag"} and requests else chosen.data.get("pending_request_count"))
    if later_unknown and count in {None, 0}:
        count = sum(row.data.get("pending_request_count") or 1 for row in later_unknown)
    recorded_at = max(row.data['recorded_at'] for row in rows) if later_unknown and rows else chosen.data['recorded_at']
    view = {"processing_attempt_id": attempt, "final": final, "pending_request_count": count,
            "result": result, "recorded_at": recorded_at, "result_version": str(chosen.latest_id)}
    if later_unknown and finals:
        view["previous_result"] = {"result": finals[0].data["result"], "recorded_at": finals[0].data["recorded_at"],
                                   "result_version": str(finals[0].latest_id)}
    selected = [finals[0]] if final else [row for row in rows if not row.data["final"]] if later_unknown else rows
    return view, selected


def _classifications(rows, task_id):
    parent = [row for row in rows if row.flow_run_id == task_id and row.type == SNAPSHOT_TYPE
              and row.data.get("task_id") == str(task_id)]
    classifications = {}
    for row in sorted(parent, key=lambda item: item.data["recorded_at"], reverse=True):
        if row.data["stage"] == "classification":
            classifications.setdefault(row.data["folder_id"], row.data["event_data"])
    batch = next((row.data for row in rows if row.flow_run_id == task_id and row.type == SUMMARY_TYPE
                  and row.data.get("source_kind") == "classification_complete"), None)
    groups = (batch or {}).get("group_ids") or []
    complete = bool(batch and not batch.get("ungrouped") and batch.get("group_count") == len(groups)
                    and len(groups) == len(set(groups)) and set(groups) == set(classifications)
                    and all(data.get("status") in {"failed", "unrecognized"} or
                            (data.get("status") == "passed" and
                             data.get("registration_status") in {"registered", "failed"})
                            for data in classifications.values()))
    return parent, classifications, complete


def _legacy_runs(rows):
    checked = {row.flow_run_id for row in rows if row.type == HISTORY_TYPE
               and row.data.get("version") == HISTORY_VERSION}
    return ({row.flow_run_id for row in rows if row.type == HISTORY_TYPE
             and row.data.get("version") != HISTORY_VERSION} |
            {row.flow_run_id for row in rows if row.type == SNAPSHOT_TYPE and row.data.get("final")
             and row.data.get("snapshot_version") != HISTORY_VERSION and row.flow_run_id not in checked})


def _history_targets(task_id, children, rows, rows_by_run, legacy, *, include_parent=True):
    checked = {row.flow_run_id for row in rows if row.type == HISTORY_TYPE
               and row.data.get("version") == HISTORY_VERSION}
    targets = []
    for child in children:
        if child.id in checked:
            continue
        selected = [row for row in rows_by_run.get(child.id, ()) if row.type == SNAPSHOT_TYPE]
        views = []
        for stage in STAGES:
            attempt, retry = _attempt_for(stage, selected)
            views.append(_stage_view(stage, selected, attempt, retry)[0])
        terminal = (all(view and view["final"] for view in views) or
                    any(view and view["final"] and view["result"] == "failed" for view in views))
        if not terminal or child.id in legacy:
            targets.append(child.id)
    if include_parent and task_id not in checked and (not _classifications(rows, task_id)[2] or task_id in legacy):
        targets.append(task_id)
    return targets


async def load_task_snapshot(task_id, children, *, repair=False, rag_retry_settings=None, include_parent=True):
    """Read stored results; only an explicit repair examines unfinished receipt history."""
    run_ids = [*([task_id] if include_parent else []), *(child.id for child in children)]
    async with get_client() as client:
        types = [SNAPSHOT_TYPE, SUMMARY_TYPE, HISTORY_TYPE, GAP_TYPE]
        pending = await _pending_receipts(client, run_ids)
        rows = await _collections(client, run_ids, types)
        rows.extend(pending)
        rows_by_run = {}
        for row in rows:
            rows_by_run.setdefault(row.flow_run_id, []).append(row)
        legacy = _legacy_runs(rows)
        if repair:
            finished = {(str(row.flow_run_id), row.data.get("folder_id"), row.data.get("stage"),
                         row.data.get("processing_attempt_id"))
                        for row in rows if row.type == SNAPSHOT_TYPE and row.data.get("final")}
            gaps = [row for row in rows if row.type == GAP_TYPE]
            gap_source_ids = {row.data["source_artifact_id"] for row in gaps}
            targets = _history_targets(task_id, children, rows, rows_by_run, legacy, include_parent=include_parent)
            existing = {row.key: row.data for row in rows}
            pending_specs, seen, unresolved_ids = {}, set(), set()
            history = await _originals(client, targets) if targets else []
            gap_ids = [UUID(row.data["source_artifact_id"]) for row in gaps if row.flow_run_id not in targets]
            gap_originals = await _originals_by_id(client, gap_ids) if gap_ids else []
            originals = history + gap_originals
            by_run = {}
            for original in originals:
                by_run.setdefault(original.flow_run_id, []).append(original)
            for run_id in {original.flow_run_id for original in gap_originals}:
                by_run[run_id].extend(_snapshot_evidence(rows_by_run.get(run_id, ())))
                by_run[run_id].sort(key=lambda row: row.created, reverse=True)
            gap_specs = [_snapshot_artifact(original.data, original) for original in gap_originals
                         if original.data["kind"] != "child_error"]
            gap_keys = [spec.key for spec in gap_specs if spec is not None]
            for start in range(0, len(gap_keys), 200):
                latest = await client.read_latest_artifacts(
                    artifact_filter=ArtifactCollectionFilter(key={"any_": gap_keys[start:start + 200]}), limit=200)
                existing.update({row.key: row.data for row in latest})
                for run_id in {row.flow_run_id for row in latest}:
                    if run_id in by_run:
                        by_run[run_id].extend(_snapshot_evidence([row for row in latest if row.flow_run_id == run_id]))
                        by_run[run_id].sort(key=lambda row: row.created, reverse=True)
            from .attempts import active_snapshot_rows, FIELD
            for run_id, evidence in by_run.items():
                if any(row.data.get('kind') == 'manual_retry' and FIELD in (row.data.get('data') or {})
                       for row in evidence):
                    # Repair may mix originals and partial projections. It is
                    # display evidence, never permission to send a model call.
                    projections = [_snapshot_artifact(row.data, row) for row in evidence]
                    projections = [row for row in projections if row is not None]
                    current = active_snapshot_rows(projections)
                    hidden = {row.data['source_artifact_id'] for row in projections} - {
                        row.data['source_artifact_id'] for row in current}
                    active = []
                    for row in evidence:
                        if str(row.id) in hidden:
                            continue
                        if row.data.get('kind') == 'manual_retry' and FIELD in (row.data.get('data') or {}):
                            row = copy.copy(row)
                            row.data = {**row.data, 'data': {key: value for key, value in row.data['data'].items()
                                if key != FIELD}}
                        active.append(row)
                    by_run[run_id] = active
            for original in originals:
                evidence = by_run[original.flow_run_id]
                spec = (_failed_snapshot(original.data, original, evidence)
                        if original.data["kind"] == "child_error" else _snapshot_artifact(original.data, original))
                if spec is None:
                    if original.data["kind"] == "child_error":
                        unresolved_ids.add(str(original.id))
                    continue
                if spec.key in seen:
                    continue
                seen.add(spec.key)
                if (spec.type == SNAPSHOT_TYPE and spec.data["stage"] not in {"classification", "registration"}
                        and (str(original.flow_run_id), spec.data["folder_id"], spec.data["stage"],
                             spec.data["processing_attempt_id"]) in finished
                        and str(original.id) not in gap_source_ids
                        and not (original.flow_run_id in legacy and
                                 _unknown_event(original.data["kind"], original.data.get("data") or {}))):
                    continue
                previous = existing.get(spec.key)
                previous_time = previous.get("recorded_at", previous.get("as_of", "")) if previous else ""
                if previous_time > original.created.isoformat():
                    continue
                if spec.type == RESOLVED_TYPE and not previous:
                    continue
                if spec.type == SNAPSHOT_TYPE and spec.data["final"] and spec.data["stage"] in {"llm", "rag"}:
                    pending = _pending_requests(evidence, spec.data["stage"])
                    if pending:
                        spec.data.update(final=False, result=None, pending_request_count=pending)
                        unresolved_ids.add(str(original.id))
                if previous_time == original.created.isoformat() and (previous.get("final") or not spec.data.get("final")):
                    continue
                pending_specs[spec.key] = spec
            for row in rows:
                if (row.type != SNAPSHOT_TYPE or not row.data.get("final") or row.flow_run_id not in legacy
                        or row.data.get('folder_completion')):
                    continue
                evidence = by_run.get(row.flow_run_id, [])
                pending = _legacy_pending(evidence, row.data["stage"])
                if pending:
                    data = {**row.data, "final": False, "pending_request_count": pending, "stage_summary": True}
                    pending_specs[row.key] = ArtifactCreate(key=row.key, type=SNAPSHOT_TYPE,
                                                            flow_run_id=row.flow_run_id, data=data)
                    failed_source = next((item for item in evidence if str(item.id) == row.data["source_artifact_id"]
                                          and item.data["kind"] == "child_error"), None)
                    if failed_source:
                        gap = snapshot_gap(failed_source.data, failed_source)
                        pending_specs[gap.key] = gap
            for spec in pending_specs.values():
                await client.create_artifact(spec)
            for run_id in targets:
                await client.create_artifact(ArtifactCreate(key=_key("history", str(task_id), str(run_id)),
                    type=HISTORY_TYPE, flow_run_id=run_id,
                    data={"task_id": str(task_id), "version": HISTORY_VERSION}))
            recovered = {str(original.id) for original in originals}
            resolved = False
            for gap in gaps:
                if (gap.data["source_artifact_id"] in recovered and
                        gap.data["source_artifact_id"] not in unresolved_ids):
                    await client.create_artifact(ArtifactCreate(key=gap.key, type=RESOLVED_TYPE,
                        flow_run_id=gap.flow_run_id, data=gap.data))
                    resolved = True
            if pending_specs or targets or resolved:
                pending = await _pending_receipts(client, run_ids)
                rows = await _collections(client, run_ids, types)
                rows.extend(pending)
                rows_by_run = {}
                for row in rows:
                    rows_by_run.setdefault(row.flow_run_id, []).append(row)
                legacy = _legacy_runs(rows)

    parent_rows, classifications, classification_complete = _classifications(rows, task_id)
    parent_by_folder = {}
    for row in parent_rows:
        parent_by_folder.setdefault(row.data["folder_id"], []).append(row)
    items = [{"group_id": data.get("group_id") or group_id, "folder_name": data.get("group") or group_id,
              "paper_title": data.get("paper_title"), "paper_title_candidate": data.get("paper_title_candidate"),
              "status": data.get("status") if data.get("status") in ("passed", "failed", "unrecognized") else "unresolved",
              "registration_status": data.get("registration_status"), "delivery_complete": data.get("delivery_complete"),
              "reason": data.get("reason")} for group_id, data in classifications.items()]

    from paperflow_v3.delivery.progress import local_records, merge_records
    local_retry = {}
    if rag_retry_settings:
        for child in children:
            if not (child.state.is_failed() or child.state.is_crashed()):
                continue
            local = local_records(child.id)
            if any(row.get("kind") == "delivery_error" for row in local):
                local_retry[str(child.id)] = local
    full_retry = {}
    if local_retry:
        from .receipts import read_folder_receipts
        full_retry = await read_folder_receipts([child.id for child in children if str(child.id) in local_retry])

    folders, missing = [], []
    for child in children:
        params, group = child.parameters, child.parameters.get("registered") or {}
        if params.get("task_id") != str(task_id) or not params.get("group_id"):
            continue
        selected = [row for row in rows_by_run.get(child.id, ()) if row.type == SNAPSHOT_TYPE
                    and row.data.get("task_id") == str(task_id)
                    and row.data.get("folder_id") == params["group_id"]]
        from .attempts import active_snapshot_rows
        selected = active_snapshot_rows(selected)
        child_gaps = [row for row in rows_by_run.get(child.id, ()) if row.type == GAP_TYPE]
        views, events = {}, []
        parent_group = parent_by_folder.get(params["group_id"], ())
        for stage in ("classification", "registration"):
            attempt, retry = _attempt_for(stage, parent_group)
            view, _ = _stage_view(stage, parent_group, attempt, retry)
            if view:
                views[stage] = view
        for stage in STAGES:
            attempt, retry = _attempt_for(stage, selected)
            view, relevant = _stage_view(stage, selected, attempt, retry)
            stage_gap = child.id in legacy or any(row.data["stage"] == stage for row in child_gaps)
            if stage_gap and view:
                if view["final"]:
                    view["previous_result"] = {key: view[key] for key in ("result", "recorded_at", "result_version")}
                view.update(final=False, result=None, pending_request_count=None)
                relevant = [row for row in relevant if not row.data["final"]]
            if view:
                views[stage] = view
                events.extend(relevant)
                if not view["final"]:
                    missing.append({"ticket_id": str(child.id), "folder_id": params["group_id"], "stage": stage})
            elif stage == "preprocessing" or stage_gap:
                missing.append({"ticket_id": str(child.id), "folder_id": params["group_id"], "stage": stage})
        events.extend(row for row in selected if row.data["stage"] in {"_retry", "_review"})
        events.sort(key=lambda row: row.data["recorded_at"], reverse=True)
        records = [{"kind": row.data["event_kind"], "data": row.data["event_data"],
                    "task_id": str(task_id), "folder_id": params["group_id"], "event_id": row.data["event_id"],
                    "recorded_at": row.data['recorded_at'],
                    "folder_completion": row.data.get('folder_completion', False)}
                   for row in events]
        identity = {**{key: group.get(key) for key in ("task_id", "group_id", "paper_id", "receipt_id")},
                    "attempt_id": str(child.id), "parent_run_id": str(task_id), "task_run_id": str(child.id)}
        recovery = merge_records(local_records(child.id), records,copy_rows=False)
        projected = folder_status(recovery, is_running=child.state.is_running(),
                                  is_active=not child.state.is_final(), identity=identity)
        retry_rows = (merge_records(local_retry[str(child.id)], full_retry[str(child.id)])
                      if str(child.id) in full_retry else recovery)
        decision = retry_decision(retry_rows, identity=identity, registered=group, **(rag_retry_settings or {}))
        can_retry = (child.state.is_failed() or child.state.is_crashed()) and decision["allowed"]
        if can_retry and decision.get("qdrant_republication") and projected["stage"] == "unresolved":
            projected.update(stage="failed", failed_stage="rag",
                             failure_detail="入库未完成，可使用原后处理包提交至 Qdrant")
        projected.update(retryable=can_retry, retry_reason=None if can_retry else
                         decision["reason"] if not decision["allowed"] else "论文工单尚未停止")
        if child_gaps or child.id in legacy:
            if projected["stage"] == "completed":
                projected.update(stage="unresolved", cleanup_status=None)
            projected.update(retryable=False, retry_reason="回执快照待核对")
        uncertain_post = any(row.data["event_kind"] == "postprocessing_component_failed" and
                             row.data["event_data"].get("error_type") in NETWORK_UNKNOWN for row in events)
        confirmed_post = any(row.data["event_kind"] == "postprocessing_component_failed" and
                             row.data["event_data"].get("error_type") not in NETWORK_UNKNOWN for row in events)
        if uncertain_post and not confirmed_post and projected["failed_stage"] == "postprocessing":
            projected.update(stage="unresolved", failed_stage=None, failure_detail=None,
                             postprocessing_result=None, retryable=False, retry_reason="后处理网络结果待核对")
        retries = [row for row in selected if row.data["stage"] == "_retry"
                   and row.data["event_data"].get("state") == "authorized"]
        retry = max(retries, key=lambda row: row.data["recorded_at"], default=None)
        last_retry = None
        if retry:
            retry_id = retry.data["processing_attempt_id"]
            running_receipt = any(row.data["stage"] == "_retry" and row.data["processing_attempt_id"] == retry_id
                                  and row.data["event_data"].get("state") == "running" for row in selected)
            if child.state.is_scheduled() or child.state.is_pending():
                retry_status = "scheduled"
            elif child.state.is_running():
                retry_status = "running"
            elif not running_receipt:
                retry_status = "dispatch_unknown"
            elif child.state.is_completed() and projected["stage"] == "completed":
                retry_status = "completed"
            elif (child.state.is_failed() or child.state.is_crashed()) and decision["allowed"]:
                retry_status = "failed"
            else:
                retry_status = "unresolved"
            last_retry = {"retry_id": retry_id, "stage": retry.data["event_data"].get("stage"), "status": retry_status}
        folders.append({"ticket_id": str(child.id), "task_id": str(task_id), "folder_id": params["group_id"],
                        "folder_name": group.get("group") or group.get("folder_name") or params["group_id"],
                        "paper_title": group.get("paper_title"), "task_name": None, **projected,
                        "last_retry": last_retry, "stage_snapshots": views})
    group_ids = [child.parameters.get("group_id") for child in children]
    dispatches = {}
    for row in sorted(parent_rows, key=lambda item: item.data["recorded_at"], reverse=True):
        if row.data["stage"] == "_dispatch":
            dispatches.setdefault(row.data["folder_id"], row.data["event_data"])
    registered = {group for group, data in classifications.items()
                  if data.get("status") == "passed" and data.get("registration_status") == "registered"}
    complete = bool(classification_complete and set(group_ids) == registered
                    and len(group_ids) == len(set(group_ids))
                    and all(child.parameters.get("task_id") == str(task_id) and child.parameters.get("group_id")
                            for child in children)
                    and not any(data.get("state") in {"intent", "unknown"} and group not in group_ids
                                for group, data in dispatches.items()))
    gaps = [row for row in rows if row.type == GAP_TYPE]
    if task_id in legacy or any(row.flow_run_id == task_id for row in gaps):
        classification_complete = False
        complete = False
    has_pending = (not complete or bool(missing) or bool(gaps) or bool(legacy)
                   or any(not child.state.is_final() for child in children)
                   or any(folder["stage"] not in {"completed", "failed"} for folder in folders))
    times = [row.data.get("recorded_at") or row.data.get("as_of") for row in rows]
    history_targets = sorted({str(run_id) for run_id in
        [*_history_targets(task_id, children, rows, rows_by_run, legacy, include_parent=include_parent), *(row.flow_run_id for row in gaps)]})
    return {"folders": folders, "items": items, "classification_complete": classification_complete, "coverage": "task" if include_parent else "children",
            "complete": complete, "has_pending": has_pending,
            "as_of": max((time for time in times if time), default=None), "missing": missing,
            "history_targets": history_targets}
