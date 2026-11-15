from paperflow_v3.control.folder_status import folder_status


def event(event_kind, **data):
    return {"kind": event_kind, "data": data}


def test_llm_success_does_not_wait_for_postprocessing():
    records = [
        event("postprocessing_component_failed", component_id="body", reason="synthetic failure"),
        event("llm_folder_complete", status="raw_complete", ranking_complete=True),
        event("preprocessing_complete", status="preprocessing_complete"),
    ]
    result = folder_status(records, is_active=True)
    assert result["preprocessing_result"] == "passed"
    assert result["llm_result"] == "passed"
    assert result["postprocessing_result"] == "failed"
    assert result["stage"] == "failed" and result["failed_stage"] == "postprocessing"
    assert result["failure_detail"] == "synthetic failure"


def test_raw_parts_without_confirmed_whole_paper_ranking_are_not_complete():
    records = [event("llm_folder_complete", status="raw_complete"), event("preprocessing_complete")]
    result = folder_status(records)
    assert result["llm_result"] is None
    assert result["llm_body_running"] is None


def test_complete_postprocessing_supersedes_previous_component_failure():
    records = [
        event("postprocessing_complete", validation_status="passed"),
        event("postprocessing_component_complete", component_id="body"),
        event("postprocessing_component_failed", component_id="body", reason="old failure"),
        event("llm_folder_complete", status="raw_complete", ranking_complete=True),
        event("preprocessing_complete"),
    ]
    result = folder_status(records, is_active=True)
    assert result["postprocessing_result"] == "passed"
    assert result["stage"] == "waiting_rag_upload"
    assert result["failed_stage"] is None


def test_cleanup_must_be_complete_before_the_paper_is_complete():
    upstream = [event("postprocessing_complete", validation_status="passed"),
                event("llm_folder_complete", status="raw_complete", ranking_complete=True),
                event("preprocessing_complete")]
    result = folder_status([event("nas", status="complete", files={}), *upstream], is_active=True)
    assert result["stage"] == "waiting_cleanup"
    failed = folder_status([event("cleanup", status="incomplete", files={"x": {"status": "failed"}}), *upstream])
    assert failed["stage"] == "failed" and failed["failed_stage"] == "cleanup"
    done = folder_status([event("cleanup", status="complete", files={}), *upstream])
    assert done["stage"] == "completed" and done["cleanup_status"] == "complete"


def test_delivery_stage_projects_real_queues_and_keeps_unknowns_separate():
    upstream = [event("postprocessing_complete", validation_status="passed"),
                event("llm_folder_complete", status="raw_complete", ranking_complete=True),
                event("preprocessing_complete")]
    identity = {"task_id": "task", "group_id": "group", "task_run_id": "child"}
    stage = lambda value: event("delivery_stage", identity=identity, stage=value)
    complete = folder_status([stage("waiting_nas_archive"), *upstream], is_running=True,
                             is_active=True, identity=identity)
    partial = folder_status([event("rag-body", **{"one": {"body_delivery_state": "complete"}}), *upstream],
                            is_running=True)
    assert complete["stage"] == "waiting_nas_archive_delete"
    assert partial["stage"] == "rag_in_progress"
    waiting_cleanup = folder_status([stage("waiting_cleanup"), event("cleanup", status="incomplete", files={}),
                                     *upstream], is_running=True, is_active=True, identity=identity)
    assert waiting_cleanup["stage"] == "waiting_cleanup"
    stopped = folder_status([stage("nas_in_progress"), *upstream], is_running=False,
                            is_active=False, identity=identity)
    assert stopped["stage"] == "unresolved"
    wrong = folder_status([stage("waiting_nas_archive"), *upstream], is_running=True, is_active=True,
                          identity={**identity, "task_run_id": "other"})
    assert wrong["stage"] == "unresolved"
    uncertain = folder_status([event("rag_effect_intent", effect_id="r1"), stage("waiting_nas_archive"),
                               *upstream], is_running=True, is_active=True, identity=identity)
    assert uncertain["stage"] == "unresolved"
    uncertain_nas = event("nas", status="incomplete", files={"x": {"status": "in_flight"}})
    assert folder_status([stage("waiting_nas_archive"), uncertain_nas, *upstream],
                         is_running=True, identity=identity)["stage"] == "unresolved"
    uncertain_cleanup = event("cleanup", status="incomplete", files={"x": {"status": "unknown"}})
    assert folder_status([stage("waiting_cleanup"), uncertain_cleanup, *upstream],
                         is_running=True, identity=identity)["stage"] == "unresolved"
    uncertain_target = event("rag-body", **{"one": {"body_delivery_state": "unknown"}})
    assert folder_status([stage("waiting_nas_archive"), uncertain_target, *upstream],
                         is_running=True, identity=identity)["stage"] == "unresolved"


def test_live_stage_uses_started_receipts_and_never_counts_an_intent_as_running():
    pre = folder_status([event("preprocessing_started")], is_running=True)
    assert pre["stage"] == "preprocessing_in_progress"
    intent = event("llm_request_intent", component_id="body", request_id="r1", kind="body")
    unknown = folder_status([intent, event("preprocessing_complete")], is_running=True)
    assert unknown["llm_body_running"] is None
    started_body = event("llm_request_started", component_id="body", request_id="r1", kind="body")
    started_attachment = event("llm_request_started", component_id="attachment", request_id="r2", kind="attachment")
    live = folder_status([started_attachment, started_body, intent, event("preprocessing_complete")], is_running=True)
    assert live["stage"] == "llm_in_progress"
    assert live["llm_body_running"] is True and live["llm_attachment_running"] is True
    body_saved = event("llm_response_saved", component_id="body", request_id="r1", kind="body")
    live = folder_status([body_saved, started_attachment, started_body, intent, event("preprocessing_complete")], is_running=True)
    assert live["llm_body_running"] is False and live["llm_attachment_running"] is True
    post = folder_status([event("postprocessing_component_started", component_id="body"),
                          event("llm_folder_complete", status="raw_complete", ranking_complete=True),
                          event("preprocessing_complete")], is_running=True)
    assert post["stage"] == "postprocessing_in_progress"


def test_waiting_stages_follow_the_latest_confirmed_component():
    pre = event("preprocessing_complete")
    saved = event("llm_response_saved", component_id="part-1", request_id="r1", kind="body")
    not_sent = event("llm_request_not_sent", component_id="part-2", request_id="r2", kind="body")
    assert folder_status([not_sent, saved, pre], is_active=True)["stage"] == "waiting_llm"
    assert folder_status([saved, pre], is_active=False)["stage"] == "unresolved"
    unknown = event("llm_request_unknown", component_id="part-3", request_id="r3", kind="body")
    assert folder_status([unknown, saved, pre], is_active=True)["stage"] == "unresolved"
    started = event("llm_request_started", component_id="part-3", request_id="r3", kind="body")
    assert folder_status([started, saved, pre], is_running=True, is_active=True)["stage"] == "llm_in_progress"
    assert folder_status([started, saved, pre], is_active=False)["stage"] == "unresolved"
    llm = event("llm_folder_complete", status="raw_complete", ranking_complete=True)
    completed = event("postprocessing_component_complete", component_id="part-1")
    waiting = event("postprocessing_component_not_started", component_id="part-2")
    assert folder_status([waiting, completed, llm, pre], is_active=True)["stage"] == "waiting_postprocessing"
    working = event("postprocessing_component_started", component_id="part-2")
    assert folder_status([working, completed, llm, pre], is_running=True, is_active=True)["stage"] == "postprocessing_in_progress"
    assert folder_status([working, completed, llm, pre], is_active=False)["stage"] == "unresolved"


def test_successful_model_retry_supersedes_only_its_own_known_failure():
    saved = event("llm_response_saved", component_id="part", request_id="retry", kind="body", request_sha256="same")
    failed = event("llm_request_failed", component_id="part", request_id="original", kind="body", request_sha256="same")
    result = folder_status([event("llm_folder_complete", status="raw_complete", ranking_complete=True),
                            saved, failed, event("preprocessing_complete")])
    assert result["llm_result"] == "passed" and result["failed_stage"] is None
    running = event("llm_request_started", component_id="part", request_id="retry", kind="body", request_sha256="same")
    result = folder_status([running, failed, event("preprocessing_complete")], is_running=True)
    assert result["stage"] == "llm_in_progress" and result["llm_body_running"] is True


def test_preprocessing_failure_and_unconfirmed_identity_are_distinct():
    failed = event("preprocessing_failed", failure_reason="local_io", retryable=True, reason="file busy")
    result = folder_status([failed, event("preprocessing_started")])
    assert result["stage"] == "failed" and result["failed_stage"] == "preprocessing"
    assert result["preprocessing_result"] == "failed" and result["failure_detail"] == "file busy"
    unknown = event("preprocessing_failed", failure_reason="history_unconfirmed", retryable=False, reason="history")
    result = folder_status([unknown, event("preprocessing_started")])
    assert result["stage"] == "unresolved" and result["preprocessing_result"] is None
    resumed = folder_status([event("preprocessing_started"), failed], is_running=True)
    assert resumed["stage"] == "preprocessing_in_progress" and resumed["preprocessing_result"] is None
    completed = folder_status([event("preprocessing_complete"), failed])
    assert completed["preprocessing_result"] == "passed"
