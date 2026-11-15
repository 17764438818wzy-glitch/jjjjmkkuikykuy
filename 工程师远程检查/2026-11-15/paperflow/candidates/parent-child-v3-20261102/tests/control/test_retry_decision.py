"""Only a confirmed failed component may be retried on its existing child."""

from copy import deepcopy

from paperflow_v3.control.retry import retry_decision


def event(kind, **data):
    return {"kind": kind, "data": data}


def test_retry_known_postprocessing_failure_but_never_an_unknown_model_request():
    failed = event("postprocessing_component_failed", component_id="body", reason="invalid")
    assert retry_decision([failed])["allowed"] is True
    unknown = event("llm_request_unknown", component_id="part", request_id="original")
    intent = event("llm_request_intent", component_id="part", request_id="original")
    assert retry_decision([unknown, intent, failed])["allowed"] is False
    saved = event("llm_response_saved", component_id="part", request_id="original")
    assert retry_decision([saved, intent, failed])["allowed"] is True


def test_later_postprocessing_evidence_resolves_the_older_unknown_component():
    unknown = event("postprocessing_component_failed", component_id="body", error_type="TimeoutError")
    completed = event("postprocessing_component_complete", component_id="body")
    other_failure = event("postprocessing_component_failed", component_id="attachment:1", error_type="ValueError")
    assert retry_decision([completed, other_failure, unknown])["stage"] == "postprocessing"


def test_retry_delivery_failures_and_keep_ambiguous_effects_blocked():
    failure = event("nas", files={"body": {"status": "failed"}})
    assert retry_decision([failure])["allowed"] is True
    inflight = event("rag_effect_intent", effect_id="effect")
    assert retry_decision([inflight, failure])["allowed"] is False
    returned = event("rag_effect_returned", effect_id="effect")
    assert retry_decision([returned, inflight, failure])["allowed"] is True
    rejected = event("rag_effect_failed", effect_id="folder-create", result={"status_code": 403})
    assert retry_decision([rejected])["allowed"] is True
    assert retry_decision([event("rag_effect_returned", effect_id="folder-create"), rejected])["allowed"] is False


def test_only_evidenced_model_failure_can_be_manually_retried():
    failed = event("llm_request_failed", component_id="part", request_id="original",
                   failure_reason="explicit_transient_response", request_sha256="request",
                   request_state_sha256="state", error_response_sha256="error", model_dir="synthetic", retryable=True)
    intent = event("llm_request_intent", component_id="part", request_id="original")
    decision = retry_decision([failed, intent])
    assert decision["allowed"] is True and decision["stage"] == "llm"
    incomplete = event("llm_request_failed", component_id="part", request_id="original")
    assert retry_decision([incomplete, intent])["allowed"] is False
    saved = event("llm_response_saved", component_id="part", request_id="retry", request_sha256="request",
                  retry_of="original")
    assert retry_decision([saved, failed, intent])["allowed"] is False


def test_model_rejection_can_retry_but_capacity_and_output_length_cannot():
    evidence = dict(component_id="part", request_id="original", request_sha256="request",
                    request_state_sha256="state", error_response_sha256="error", model_dir="synthetic")
    auth = event("llm_request_failed", **evidence, failure_reason="http_rejection", http_status=401, retryable=True)
    assert retry_decision([auth])["allowed"] is True
    too_large = event("llm_request_failed", **evidence, failure_reason="http_payload_too_large", retryable=False)
    decision = retry_decision([too_large])
    assert decision["allowed"] is False and "过大" in decision["reason"]
    output = event("llm_request_failed", **evidence, failure_reason="output_length_requires_changed_plan", retryable=False)
    decision = retry_decision([output])
    assert decision["allowed"] is False and "长度" in decision["reason"]


def test_a_retry_must_have_new_failure_evidence_before_it_can_be_requested_again():
    failed = event("postprocessing_component_failed", component_id="body")
    authorized = event("manual_retry", state="authorized", retry_id="retry-1")
    running = event("manual_retry", state="running", retry_id="retry-1")
    assert retry_decision([authorized, failed])["allowed"] is False
    assert retry_decision([running, failed])["allowed"] is False
    new_failure = event("postprocessing_component_failed", component_id="body")
    assert retry_decision([new_failure, running, failed])["allowed"] is True


def test_only_confirmed_recoverable_preprocessing_failure_can_retry():
    failure = event("preprocessing_failed", failure_reason="local_io", retryable=True, reason="file busy")
    assert retry_decision([failure])["stage"] == "preprocessing"
    parse = event("preprocessing_failed", failure_reason="local_parse", retryable=False, reason="bad format")
    assert not retry_decision([parse])["allowed"] and "bad format" in retry_decision([parse])["reason"]
    unknown = event("preprocessing_failed", failure_reason="history_unconfirmed", retryable=False, reason="history")
    assert not retry_decision([unknown])["allowed"] and "核对" in retry_decision([unknown])["reason"]
    assert not retry_decision([event("preprocessing_started"), failure])["allowed"]


def reviewed_failure():
    identity = {"task_id": "task", "group_id": "folder", "paper_id": "paper",
                "receipt_id": "receipt", "attempt_id": "child"}
    body_sha, receipt_sha, docx_sha = "1" * 64, "2" * 64, "3" * 64
    registered = {**{key: identity[key] for key in ("task_id", "group_id", "paper_id", "receipt_id")},
        "receipt_sha256": receipt_sha,
        "paper": {"body": {"sha256": body_sha}, "attachments": [], "archive_files": [{"sha256": body_sha}]}}
    failure = {"kind": "preprocessing_failed", "event_id": "failure-1", "task_id": "task", "folder_id": "folder",
        "data": {**identity, "processing_attempt_id": "initial", "failure_reason": "preprocessing_unconfirmed",
                 "retryable": False}}
    review = {"kind": "preprocessing_reviewed", "event_id": "review-1", "task_id": "task", "folder_id": "folder",
        "data": {"identity": identity, "failure_event_id": "failure-1", "processing_attempt_id": "initial",
                 "receipt_sha256": receipt_sha, "source_sha256": [body_sha], "delivery_sha256": [body_sha],
                 "archive_sha256": [body_sha], "cached_docx_sha256": docx_sha,
                 "observed_at": "2026-11-02T13:30:00+00:00",
                 "conversion_observation": "no_owned_process_observed",
                 "local_request_history": "none_observed", "downstream_events": "none_observed"}}
    return identity, registered, failure, review


def test_reviewed_local_preprocessing_can_retry_only_the_bound_failure():
    from paperflow_v3.control.retry import reviewed_preprocessing

    identity, registered, failure, review = reviewed_failure()
    records = [review, failure]
    decision = retry_decision(records, identity=identity, registered=registered)
    assert decision["stage"] == "preprocessing"
    assert decision["review_event_id"] == "review-1" and decision["failure_event_id"] == "failure-1"
    assert reviewed_preprocessing(records, identity, registered,
        review_event_id="review-1", failure_event_id="failure-1") == review
    assert reviewed_preprocessing(records, identity, registered,
        review_event_id="other", failure_event_id="failure-1") is None
    assert not retry_decision(records)["allowed"]
    assert not retry_decision([failure, review], identity=identity, registered=registered)["allowed"]

    for field, value in (("failure_event_id", "other"), ("processing_attempt_id", "other"),
                         ("receipt_sha256", "changed"), ("cached_docx_sha256", "invalid"),
                         ("local_request_history", "unknown"), ("conversion_observation", "unknown")):
        changed = deepcopy(review)
        changed["data"][field] = value
        assert not retry_decision([changed, failure], identity=identity, registered=registered)["allowed"]
    changed = deepcopy(identity)
    changed["attempt_id"] = "another-child"
    assert not retry_decision(records, identity=changed, registered=registered)["allowed"]


def test_review_does_not_hide_downstream_activity_or_unconfirmed_dispatch():
    identity, registered, failure, review = reviewed_failure()
    for unsafe in (event("llm_request_started", component_id="body", request_id="r"),
                   event("llm_request_unknown", component_id="body", request_id="r"),
                   event("rag_effect_intent", effect_id="effect"),
                   event("postprocessing_component_complete", component_id="body"),
                   event("preprocessing_started")):
        assert not retry_decision([unsafe, review, failure], identity=identity, registered=registered)["allowed"]
    old_unknown = event("manual_retry", state="authorized", retry_id="old")
    assert not retry_decision([review, old_unknown, failure],
                              identity=identity, registered=registered)["allowed"]


def test_started_model_request_alone_is_not_treated_as_safe_to_retry():
    started = event("llm_request_started", component_id="body", request_id="unknown")
    failure = event("postprocessing_component_failed", component_id="body")
    assert not retry_decision([started, failure])["allowed"]
