"""Project confirmed stage results from one paper's Prefect receipts."""
from .retry import uncertain_model_receipt
from paperflow_v3.stages.gateway_admission import has_proof


def folder_status(records, *, is_running=False, is_active=False, identity=None):
    from .attempts import active_model_records
    records = active_model_records(records, identity)
    pre_event = next((row for row in records if row["kind"] in
                      {"preprocessing_started", "preprocessing_failed", "preprocessing_complete"}), None)
    preprocessing = bool(pre_event and pre_event["kind"] == "preprocessing_complete")
    pre_failure = pre_event["data"] if pre_event and pre_event["kind"] == "preprocessing_failed" else None
    pre_confirmed_failure = bool(pre_failure and pre_failure.get("failure_reason") in
                                 {"local_io", "local_parse", "plan_unavailable"})
    pre_started = next((index for index, row in enumerate(records) if row["kind"] == "preprocessing_started"), None)
    pre_completed = next((index for index, row in enumerate(records) if row["kind"] == "preprocessing_complete"), None)
    uncertain_llm = any(uncertain_model_receipt(row['kind'], row.get('data') or {}) or
        row['kind'] == 'llm_gateway_not_dispatched' and not has_proof(row.get('data') or {}) for row in records)
    llm = not uncertain_llm and any(row["kind"] == "llm_folder_complete" and row["data"].get("status") == "raw_complete"
              and row["data"].get("ranking_complete") is True for row in records)
    post_complete = next((i for i, row in enumerate(records) if row["kind"] == "postprocessing_complete"
                          and row["data"].get("validation_status") == "passed"), None)
    delivery_stage = next(((i, row.get("data") or {}) for i, row in enumerate(records)
                           if row["kind"] == "delivery_stage"), None)
    components = {}
    for index, row in enumerate(records):
        if row["kind"] in {"postprocessing_component_started", "postprocessing_component_not_started",
                           "postprocessing_component_failed", "postprocessing_component_complete",
                           "postprocessing_component_waiting"}:
            components.setdefault(row["data"]["component_id"], (index, row))
    failed = next((row for index, row in components.values()
                   if row["kind"] == "postprocessing_component_failed"
                   and (post_complete is None or index < post_complete)), None)
    post_passed = post_complete is not None and failed is None
    result = {
        "stage": "unresolved", "failed_stage": None, "failure_detail": None,
        "llm_body_running": False if llm else None,
        "llm_attachment_running": False if llm else None,
        "preprocessing_result": "passed" if preprocessing else "failed" if pre_confirmed_failure else None,
        "llm_result": "passed" if llm else None,
        "postprocessing_result": "failed" if failed else "passed" if post_passed else None,
        "preprocessing_detail": None, "llm_detail": None, "postprocessing_detail": None,
        "cleanup_status": None, "retryable": None, "retry_reason": "尚未核对失败点",
    }
    is_active = is_active or is_running
    if is_active and not records:
        result["stage"] = "waiting_preprocessing"
    latest = {}
    requests, effects = {}, {}
    for index, row in enumerate(records):
        kind, data = row["kind"], row.get("data") or {}
        if kind in {"rag-body", "rag-attachments", "rag-qdrant-body", "rag-qdrant-attachments", "nas", "cleanup"}:
            latest.setdefault(kind, data)
        elif kind in {"llm_request_intent", "llm_request_started", "llm_request_not_sent", "llm_request_unknown",
                      "llm_request_failed", "llm_response_saved", "llm_gateway_not_dispatched"}:
            if kind == 'llm_gateway_not_dispatched':
                kind = 'llm_request_not_sent' if has_proof(data) else 'llm_request_unknown'
            requests.setdefault((data.get("component_id"), data.get("request_id")), (index, kind, data))
        elif kind in {"rag_effect_intent", "rag_effect_not_sent", "rag_effect_failed", "rag_effect_returned"}:
            effects.setdefault(data.get("effect_id"), (index, kind))
    qdrant_cutover = any(row.get("kind") == "rag-qdrant-backend"
                         and (row.get("data") or {}).get("backend") == "qdrant"
                         and (identity is None or (row.get("task_id") == identity.get("task_id")
                              and row.get("folder_id") == identity.get("group_id")))
                         for row in records)
    if qdrant_cutover:
        # Old requests remain recorded, but no longer describe this new delivery.
        effects = {}
        latest["rag-body"] = latest.get("rag-qdrant-body", {})
        latest["rag-attachments"] = latest.get("rag-qdrant-attachments", {})
    cleanup = latest.get("cleanup") or {}
    nas = latest.get("nas") or {}
    cleanup_confirmed = any(not row.get('local_only') and row['data'].get('status') == 'complete'
                            for row in records if row['kind'] == 'cleanup')
    pending_completion = next((row['data'] for row in records if row['kind'] == 'folder_completion_pending'
                               and row['data'].get('identity') == identity and identity is not None), None)
    cleanup_states = [row.get("status") for row in cleanup.get("files", {}).values()]
    nas_states = [row.get("status") for row in nas.get("files", {}).values()]
    rag_failed = any(kind == "rag_effect_failed" for _, kind in effects.values()) or any(value.get(field) == "failed" for kind, field in
                     (("rag-body", "body_delivery_state"), ("rag-attachments", "delivery_state"))
                     for value in (latest.get(kind) or {}).values())
    rag_unknown = any(value.get(field) in {"in_flight", "unknown"} for kind, field in
                      (("rag-body", "body_delivery_state"), ("rag-attachments", "delivery_state"))
                      for value in (latest.get(kind) or {}).values())
    llm_failure = next((data for index, kind, data in requests.values() if kind == "llm_request_failed"
                        and not any(other_index < index and other_kind in {
                                        "llm_request_intent", "llm_request_started", "llm_request_unknown", "llm_response_saved"}
                                    and other.get("component_id") == data.get("component_id")
                                    and data.get("request_sha256")
                                    and other.get("request_sha256") == data.get("request_sha256")
                                    for other_index, other_kind, other in requests.values())), None)

    def request_running(request_kind):
        selected = [kind for _, kind, data in requests.values() if data.get("kind") == request_kind]
        if llm:
            return False
        if is_running and "llm_request_started" in selected:
            return True
        if any(kind in {"llm_request_intent", "llm_request_started", "llm_request_unknown"} for kind in selected):
            return None
        return False if selected else None

    result["llm_body_running"] = request_running("body")
    result["llm_attachment_running"] = request_running("attachment")

    if cleanup.get("status") == "complete" and cleanup_confirmed and preprocessing and llm and post_passed:
        result.update(stage="completed", cleanup_status="complete", retryable=False, retry_reason="论文已完成")
    elif pending_completion:
        result.update(stage='failed', failed_stage=pending_completion['stage'], failure_detail='完成结果尚未保存')
    elif pre_failure and not pre_confirmed_failure:
        result.update(stage="unresolved", preprocessing_detail=pre_failure.get("reason"))
    elif pre_confirmed_failure:
        result.update(stage="failed", failed_stage="preprocessing", failure_detail=pre_failure.get("reason"),
                      preprocessing_detail=pre_failure.get("reason"))
    elif "failed" in cleanup_states:
        result.update(stage="failed", failed_stage="cleanup", failure_detail="本机文件清理失败")
    elif "failed" in nas_states:
        result.update(stage="failed", failed_stage="nas", failure_detail="NAS 文件归档失败")
    elif rag_failed:
        result.update(stage="failed", failed_stage="rag", failure_detail="正式目标入库失败")
    elif uncertain_llm:
        pass
    elif failed:
        result.update(stage="failed", failed_stage="postprocessing", failure_detail=failed["data"].get("reason"),
                      retryable=None)
    elif llm_failure:
        reason = llm_failure.get("failure_reason")
        detail = {"http_payload_too_large": "模型请求过大，需调整材料或处理计划",
                  "output_length_requires_changed_plan": "模型输出达到长度上限，需调整处理计划",
                  "http_rejection": "模型服务拒绝请求",
                  "explicit_transient_response": "模型服务暂时拒绝请求"}.get(reason, "模型请求已明确失败")
        if llm_failure.get("http_status"):
            detail += "（HTTP " + str(llm_failure["http_status"]) + "）"
        result.update(stage="failed", failed_stage="llm", llm_result="failed",
                      failure_detail=detail, llm_detail=detail)
    elif any(kind in {"llm_request_intent", "llm_request_unknown"} or
             (kind == "llm_request_started" and not is_running) for _, kind, _ in requests.values()):
        pass
    elif any(row['kind'] == 'rag_complete' and row['data'].get('status') == 'complete'
             and (identity is None or row['data'].get('identity') == identity) for row in records) and not nas and not cleanup:
        result['stage'] = 'waiting_nas_archive_delete' if is_active else 'unresolved'
    elif delivery_stage:
        _, mark = delivery_stage
        stage = mark.get("stage")
        if identity is not None and mark.get("identity") == identity and preprocessing and llm and post_passed:
            if any(kind == "rag_effect_intent" for _, kind in effects.values()) and not (
                    stage == "rag_in_progress" and is_running):
                pass
            elif rag_unknown and stage != "rag_in_progress":
                pass
            elif stage != "nas_in_progress" and any(
                    state in {"in_flight", "unknown"} for state in nas_states):
                pass
            elif stage != "cleanup_in_progress" and any(
                    state in {"in_flight", "unknown"} for state in cleanup_states):
                pass
            elif stage in {"waiting_rag_upload", "waiting_nas_archive", "waiting_cleanup"} and is_active:
                result["stage"] = "waiting_nas_archive_delete" if stage == "waiting_nas_archive" else stage
            elif stage in {"rag_in_progress", "nas_in_progress", "cleanup_in_progress"} and is_running:
                result["stage"] = stage
    elif cleanup:
        result["stage"] = "cleanup_in_progress" if is_running else "unresolved"
    elif nas.get("status") == "complete":
        result["stage"] = "waiting_cleanup" if is_active else "unresolved"
    elif nas:
        result["stage"] = "nas_in_progress" if is_running else "unresolved"
    elif latest.get("rag-body") or latest.get("rag-attachments"):
        result["stage"] = "rag_in_progress" if is_running else "unresolved"
    elif post_passed and llm and is_active:
        result["stage"] = "waiting_rag_upload"
    elif llm and any(row["kind"] == "postprocessing_component_started"
                     for index, row in components.values()
                     if post_complete is None or index < post_complete):
        result["stage"] = "postprocessing_in_progress" if is_running else "unresolved"
    elif llm and is_active:
        result["stage"] = "waiting_postprocessing"
    elif result["llm_body_running"] is True or result["llm_attachment_running"] is True:
        result["stage"] = "llm_in_progress"
    elif is_running and pre_started is not None and (pre_completed is None or pre_started < pre_completed):
        result["stage"] = "preprocessing_in_progress"
    elif preprocessing and is_active:
        result["stage"] = "waiting_llm"
    return result
