"""Identify a confirmed failed paper operation without replaying unknown work."""

from datetime import datetime
import json
from pathlib import Path


POSTPROCESSING_NETWORK_UNKNOWN = frozenset({"TimeoutError", "ConnectionError", "ConnectTimeout",
                                            "ReadTimeout", "ConnectError"})


def uncertain_model_receipt(kind, data):
    """Old HTTP failures do not prove that a gateway never dispatched a model."""
    if kind == 'llm_request_unknown':
        return True
    if kind == 'llm_gateway_not_dispatched':
        from paperflow_v3.stages.gateway_admission import has_proof
        return not has_proof(data)
    if kind != 'llm_request_failed':
        return False
    status = data.get('http_status')
    if isinstance(status, str) and status.isascii() and status.isdecimal():
        status = int(status)
    return (data.get('failure_reason') == 'explicit_transient_response'
        or isinstance(status, int) and not isinstance(status, bool) and (status in {408, 429} or 500 <= status <= 599)
        or data.get('failure_reason') == 'http_rejection' and status not in {400,401,402,403,404,405,409,413,415,422})


def read_rag_retry_settings(project_config):
    if not project_config:
        return {}
    try:
        config = json.loads(Path(project_config).read_text(encoding="utf-8-sig"))
        if config.get("rag_backend") == "qdrant" and config.get("qdrant_gateway_url"):
            return {"rag_backend": "qdrant", "rag_gateway_url": config["qdrant_gateway_url"]}
    except (OSError, ValueError, TypeError, AttributeError):
        pass
    return {}


def reviewed_preprocessing(records, identity, registered, *, review_event_id=None, failure_event_id=None):
    """Find an identity-bound local review without changing the original failure."""
    if not identity or not registered:
        return None
    keys = ("task_id", "group_id", "paper_id", "receipt_id", "attempt_id")
    if not all(identity.get(key) for key in keys):
        return None

    def sha256(value):
        return (isinstance(value, str) and len(value) == 64
                and all(character in "0123456789abcdef" for character in value))

    try:
        if any(registered[key] != identity[key] for key in keys[:-1]):
            return None
        paper = registered["paper"]
        source = [row["sha256"] for row in (paper["body"], *paper["attachments"])]
        archive = [row["sha256"] for row in paper["archive_files"]]
        receipt_sha = registered["receipt_sha256"]
    except (KeyError, TypeError):
        return None
    if not source or not archive or not all(sha256(value) for value in (receipt_sha, *source, *archive)):
        return None

    failure = next(((index, row) for index, row in enumerate(records) if row.get("kind") in
                    {"preprocessing_started", "preprocessing_failed", "preprocessing_complete"}), None)
    if failure is None or failure[1].get("kind") != "preprocessing_failed":
        return None
    index, failed = failure
    prior = failed.get("data") or {}
    if (prior.get("failure_reason") != "preprocessing_unconfirmed" or prior.get("retryable") is not False
            or not prior.get("processing_attempt_id") or not failed.get("event_id")
            or failed.get("task_id") != identity["task_id"] or failed.get("folder_id") != identity["group_id"]
            or failure_event_id is not None and failed["event_id"] != failure_event_id
            or any(prior.get(key) != identity.get(key) for key in keys)):
        return None
    if any((row.get("kind") or "").startswith(("llm_", "postprocessing_", "rag"))
           or row.get("kind") in {"preprocessing_complete", "nas", "cleanup", "stage_work_inventory"}
           for row in records):
        return None

    for review in records[:index]:
        if review.get("kind") != "preprocessing_reviewed" or not review.get("event_id"):
            continue
        if review_event_id is not None and review["event_id"] != review_event_id:
            continue
        data = review.get("data") or {}
        if (review.get("task_id") != identity["task_id"] or review.get("folder_id") != identity["group_id"]
                or data.get("failure_event_id") != failed["event_id"]
                or data.get("processing_attempt_id") != prior.get("processing_attempt_id")
                or data.get("identity") != {key: identity[key] for key in keys}
                or data.get("receipt_sha256") != receipt_sha or data.get("source_sha256") != source
                or data.get("delivery_sha256") != source or data.get("archive_sha256") != archive
                or data.get("conversion_observation") != "no_owned_process_observed"
                or data.get("local_request_history") != "none_observed"
                or data.get("downstream_events") != "none_observed"):
            continue
        digest = data.get("cached_docx_sha256")
        if not sha256(digest):
            continue
        try:
            observed = datetime.fromisoformat(data["observed_at"].replace("Z", "+00:00"))
            if observed.utcoffset() is None:
                continue
        except (KeyError, AttributeError, TypeError, ValueError):
            continue
        return review
    return None


def _qdrant_retry_context(records, identity, registered, backend, gateway):
    """Recognize a new, identity-bound Qdrant delivery without resolving old writes."""
    if backend != "qdrant" or not gateway or not identity or not registered:
        return False, False, None
    keys = ("task_id", "group_id", "paper_id", "receipt_id")
    if (not all(identity.get(key) and registered.get(key) == identity[key] for key in keys)
            or not identity.get("task_run_id") or identity.get("attempt_id") != identity["task_run_id"]
            or identity.get("parent_run_id") != identity["task_id"]
            or registered.get("status") != "passed" or registered.get("delivery_complete") is not True
            or registered.get("registration_status") != "registered"):
        return False, False, None
    handoff = next((row for row in records if row.get("kind") == "postprocessing_complete"
                    and row.get("task_id") == identity["task_id"]
                    and row.get("folder_id") == identity["group_id"]), None)
    if handoff is None:
        return False, False, None
    try:
        from paperflow_v3.delivery.handoff import require_complete_handoff
        from paperflow_qdrant.identity import stable_id
        paper = registered["paper"]
        require_complete_handoff(paper, identity, handoff["data"])
        for digest in (paper["body"]["sha256"], handoff["data"]["body"]["manifest_sha256"]):
            if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
                raise ValueError("invalid publication identity")
        folder = stable_id(identity["task_id"], identity["group_id"], paper["body"]["sha256"],
                           handoff["data"]["body"]["manifest_sha256"])
    except (ValueError, KeyError, TypeError):
        return False, False, None
    binding = next((row for row in records if row.get("kind") in {"rag-backend", "rag-qdrant-backend"}
                    and row.get("task_id") == identity["task_id"]
                    and row.get("folder_id") == identity["group_id"]
                    and (row.get("data") or {}).get("backend") == "qdrant"
                    and row["data"].get("folder_id") == folder
                    and row["data"].get("gateway_url", "").rstrip("/") == gateway.rstrip("/")), None)
    if any(row.get("kind") == "cleanup" and (row.get("data") or {}).get("status") == "complete"
           and row["data"].get("identity") == identity for row in records):
        return False, False, None
    completed = any(row.get("kind") == "rag_complete" and row.get("folder_completion")
                    and (row.get("data") or {}).get("status") == "complete"
                    and row["data"].get("identity") == identity for row in records)
    legacy = []
    backends = set()
    for index, row in enumerate(records):
        if row.get("kind") not in {"rag-body", "rag-attachments"}:
            continue
        field = "body_delivery_state" if row["kind"] == "rag-body" else "delivery_state"
        for value in (row.get("data") or {}).values():
            if isinstance(value, dict) and value:
                backends.add(value.get("backend", "ragflow"))
            if (isinstance(value, dict) and value.get("backend", "ragflow") == "ragflow"
                    and value.get("ticket_identity") != identity):
                return False, False, None
            if (isinstance(value, dict) and value.get("backend", "ragflow") == "ragflow"
                    and value.get("ticket_identity") == identity
                    and value.get(field) in {"in_flight", "unknown"}):
                legacy.append(index)
    scope = {key: identity[key] for key in ("task_id", "group_id", "paper_id", "receipt_id", "task_run_id")}
    for index, row in enumerate(records):
        if (row.get("kind", "").startswith("rag_effect_")
                and (row.get("data") or {}).get("scope") != scope):
            return False, False, None
        if (row.get("kind") == "rag_effect_intent" and (row.get("data") or {}).get("scope") == scope):
            legacy.append(index)
    if "qdrant" in backends and "ragflow" in backends:
        return False, False, None
    if not binding and legacy and ("qdrant" in backends or any(
            row.get("kind") in {"nas", "cleanup"} for row in records)):
        return False, False, None
    return bool(binding or (legacy and not completed)), bool(binding), min(legacy) if legacy and not completed else None


def retry_decision(records, *, identity=None, registered=None, rag_backend=None, rag_gateway_url=None):
    from .attempts import active_model_records
    records = active_model_records(records, identity)
    def denied(reason):
        return {"allowed": False, "reason": reason, "stage": None}

    requests, effects, components = {}, {}, {}
    unknown_requests = set()
    latest = {}
    for index, row in enumerate(records):
        kind, data = row.get("kind"), row.get("data") or {}
        if kind in {"llm_request_intent", "llm_request_started", "llm_request_unknown", "llm_request_not_sent",
                    "llm_request_failed", "llm_response_saved", "llm_gateway_not_dispatched"}:
            requests.setdefault((data.get("component_id"), data.get("request_id")), (index, kind, data))
            if uncertain_model_receipt(kind, data):
                unknown_requests.add((data.get('component_id'), data.get('request_id')))
        elif kind in {"rag_effect_intent", "rag_effect_not_sent", "rag_effect_returned", "rag_effect_failed"}:
            effects.setdefault(data.get("effect_id"), (index, kind))
        elif kind in {"postprocessing_component_started", "postprocessing_component_not_started",
                      "postprocessing_component_failed", "postprocessing_component_complete",
                      "postprocessing_component_waiting"}:
            components.setdefault(data.get("component_id"), (index, kind, data))
        if kind in {"rag-body", "rag-attachments", "rag-qdrant-body", "rag-qdrant-attachments",
                    "delivery_error", "nas", "cleanup", "manual_retry", "postprocessing_complete",
                    "preprocessing_started", "preprocessing_failed", "preprocessing_complete"}:
            latest.setdefault(kind, (index, data))

    if unknown_requests or any(kind in {"llm_request_intent", "llm_request_started", "llm_request_unknown"}
                              for _, kind, _ in requests.values()):
        return denied("模型原请求结果待核对，不能重发")
    qdrant_context, qdrant_bound, legacy_index = _qdrant_retry_context(
        records, identity, registered, rag_backend, rag_gateway_url)
    if qdrant_context:
        # The old intents remain in history; the selected operation is a new
        # deterministic publication to the independently configured backend.
        effects = {}
        latest["rag-body"] = latest.get("rag-qdrant-body", (0, {}))
        latest["rag-attachments"] = latest.get("rag-qdrant-attachments", (0, {}))
    if any(kind == "rag_effect_intent" for _, kind in effects.values()):
        return denied("入库请求结果待核对，不能重发")
    if any(kind == "postprocessing_component_started" for _, kind, _ in components.values()):
        return denied("后处理结果待核对")
    post_complete = latest.get("postprocessing_complete", (None, {}))[0]
    if any(kind == "postprocessing_component_failed" and data.get("error_type") in POSTPROCESSING_NETWORK_UNKNOWN
           # The offline vocabulary process is closed before this local timeout.
           and not (data.get('component_id')=='body' and data.get('error_type')=='TimeoutError'
                    and data.get('reason')=='offline counter deadline exceeded')
           and (post_complete is None or index < post_complete) for index, kind, data in components.values()):
        return denied("后处理网络结果待核对")

    failures = [(index, "rag") for index, kind in effects.values() if kind == "rag_effect_failed"]
    error = latest.get("delivery_error")
    qdrant_failure = False
    if qdrant_context and not qdrant_bound and legacy_index is not None and error and error[1].get("stage") == "delivery":
        failures.append((error[0], "rag"))
        qdrant_failure = True
    if (qdrant_bound and error and error[1].get("backend") == "qdrant"
            and error[1].get("delivery_substage") == "rag" and error[1].get("retryable") is True):
        failures.append((error[0], "rag"))
        qdrant_failure = True
    failures.extend((index, row['data']['stage']) for index, row in enumerate(records)
                    if row.get('kind') == 'folder_completion_pending'
                    and row['data'].get('identity') == identity and identity is not None
                    and row['data'].get('status') == 'pending'
                    and row['data'].get('stage') in {'rag', 'nas', 'cleanup'})
    review = None
    pre_rows = [(index, kind, data) for kind, (index, data) in latest.items()
                if kind in {"preprocessing_started", "preprocessing_failed", "preprocessing_complete"}]
    if pre_rows:
        index, kind, data = min(pre_rows)
        if kind == "preprocessing_started":
            return denied("前处理结果待核对")
        if kind == "preprocessing_failed":
            reason = data.get("failure_reason")
            if reason == "local_io" and data.get("retryable") is True:
                failures.append((index, "preprocessing"))
            elif reason == "preprocessing_unconfirmed":
                review = reviewed_preprocessing(records, identity, registered)
                if review is None:
                    return denied("前处理身份或结果待核对：" + str(data.get("reason") or "未知"))
                failures.append((index, "preprocessing"))
            elif reason in {"local_parse", "plan_unavailable"}:
                return denied("前处理失败：" + str(data.get("reason") or "无法原样重试"))
            else:
                return denied("前处理身份或结果待核对：" + str(data.get("reason") or "未知"))
    saved = [data for _, kind, data in requests.values() if kind == "llm_response_saved"]
    for index, kind, data in requests.values():
        if kind != "llm_request_failed":
            continue
        if any(response.get("component_id") == data.get("component_id")
               and response.get("request_sha256") == data.get("request_sha256") for response in saved):
            continue
        reason = data.get("failure_reason")
        if reason == "http_payload_too_large" and data.get("retryable") is False:
            return denied("模型请求过大，需调整材料或处理计划，不能原样重试")
        if reason == "output_length_requires_changed_plan" and data.get("retryable") is False:
            return denied("模型输出达到长度上限，需调整处理计划，不能原样重试")
        if (reason not in {"explicit_transient_response", "http_rejection"} or data.get("retryable") is not True
                or not all(data.get(key) for key in ("request_sha256", "request_state_sha256",
                                                      "error_response_sha256", "model_dir"))):
            return denied("模型请求失败证据不完整，需核对")
        failures.append((index, "llm"))
    failures.extend((index, "postprocessing") for index, kind, _ in components.values()
                    if kind == "postprocessing_component_failed" and (post_complete is None or index < post_complete))
    for kind, field, stage in (("rag-body", "body_delivery_state", "rag"),
                               ("rag-attachments", "delivery_state", "rag")):
        if kind not in latest:
            continue
        index, values = latest[kind]
        for value in values.values():
            state = value.get(field)
            if state in ("in_flight", "unknown"):
                return denied("入库目标结果待核对")
            if state == "failed":
                failures.append((index, stage))
    for kind, stage in (("nas", "nas"), ("cleanup", "cleanup")):
        if kind not in latest:
            continue
        index, value = latest[kind]
        states = [item.get("status") for item in value.get("files", {}).values()]
        if any(state in ("in_flight", "unknown") for state in states):
            return denied("文件操作结果待核对")
        if "failed" in states:
            failures.append((index, stage))
    if not failures:
        return denied("没有可确认的失败点")

    previous = latest.get("manual_retry")
    if previous:
        index, value = previous
        if value.get("state") == "authorized":
            return denied("上次重试派发结果待核对")
        if value.get("state") == "running":
            failures = [failure for failure in failures if failure[0] < index]
            if not failures:
                return denied("上次重试尚无新的确定失败结果")
    result = {"allowed": True, "reason": None, "stage": min(failures)[1]}
    if result["stage"] == "rag" and qdrant_failure:
        result["qdrant_republication"] = True
    if result["stage"] == "preprocessing" and review is not None:
        result.update(review_event_id=review["event_id"], failure_event_id=review["data"]["failure_event_id"])
    return result
