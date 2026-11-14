"""Pure replacement lifecycle planner.

This module turns an explicit request and caller-provided state into intended
lifecycle actions. It owns no persistence, database access, or statement text.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


REPLACED_STATUS = "replaced"
ACTIVE_STATUS = "active"
UPLOADED_STATUS = "uploaded"
FAILED_STATUS = "failed"
WITHDRAWN_STATUS = "withdrawn"
NOT_UPLOADED_STATUS = "not_uploaded"


@dataclass(frozen=True)
class ReplacementRequest:
    old_resource_id: str | None
    new_resource_id: str | None
    reason: str | None = None
    operator: str | None = None
    requested_at: str | None = None
    execution_kind: str = "real"
    real_execution_confirmed: bool = False
    replacement_batch_id: str | None = None
    old_paper_id: str | None = None
    old_version: int | None = None
    new_paper_id: str | None = None
    new_version: int | None = None


@dataclass(frozen=True)
class ReplacementLocalState:
    old_sql_status: str | None = NOT_UPLOADED_STATUS
    old_resource_status: str | None = None
    old_published_to_postgres: bool = False
    old_remote_resource_id: str | None = None
    old_replaced_by_resource_id: str | None = None

    new_sql_status: str | None = NOT_UPLOADED_STATUS
    new_resource_status: str | None = None
    new_published_to_postgres: bool = False
    new_remote_resource_id: str | None = None
    new_replacement_of_resource_id: str | None = None


@dataclass(frozen=True)
class ReplacementLifecycleAction:
    target: str
    operation: str
    fields: Mapping[str, Any]


@dataclass(frozen=True)
class ReplacementLifecyclePlan:
    allowed: bool
    outcome: str
    actions: tuple[ReplacementLifecycleAction, ...] = ()
    blocking_reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    recommended_action: str | None = None


def build_replacement_lifecycle_plan(
    request: ReplacementRequest,
    current_state: ReplacementLocalState,
) -> ReplacementLifecyclePlan:
    request_reasons = _request_blocking_reasons(request)
    if request_reasons:
        return _blocked(request_reasons, recommended_action="fix_replacement_request")

    old_resource_id = _optional_text(request.old_resource_id)
    new_resource_id = _optional_text(request.new_resource_id)
    reason = _optional_text(request.reason)
    operator = _optional_text(request.operator)
    assert old_resource_id is not None
    assert new_resource_id is not None
    assert reason is not None
    assert operator is not None

    if _old_identity_conflict(current_state, old_resource_id):
        return _manual_audit(("replacement_old_identity_conflict",))

    if _old_is_replaced(current_state):
        if _known_new_resource_id(current_state) == new_resource_id:
            return ReplacementLifecyclePlan(
                allowed=True,
                outcome="noop",
                warnings=("replacement_already_applied",),
                recommended_action="skip_replacement_noop",
            )
        return _manual_audit(("old_replacement_conflict_existing_replacement",))

    if _new_identity_conflict(current_state, new_resource_id):
        return _manual_audit(("replacement_new_identity_conflict",))

    old_status_result = _old_status_result(current_state)
    if old_status_result is not None:
        return old_status_result

    new_status_result = _new_status_result(current_state)
    if new_status_result is not None:
        return new_status_result

    return ReplacementLifecyclePlan(
        allowed=True,
        outcome="replacement_allowed",
        actions=_replacement_actions(
            request=request,
            old_resource_id=old_resource_id,
            new_resource_id=new_resource_id,
            reason=reason,
            operator=operator,
        ),
        recommended_action="proceed_replacement",
    )


def _request_blocking_reasons(request: ReplacementRequest) -> tuple[str, ...]:
    reasons: list[str] = []
    old_resource_id = _optional_text(request.old_resource_id)
    new_resource_id = _optional_text(request.new_resource_id)
    if old_resource_id is None:
        reasons.append("replacement_old_resource_id_required")
    if new_resource_id is None:
        reasons.append("replacement_new_resource_id_required")
    if old_resource_id is not None and new_resource_id is not None and old_resource_id == new_resource_id:
        reasons.append("replacement_same_resource_id_forbidden")
    if _optional_text(request.reason) is None:
        reasons.append("replacement_reason_required")
    if _optional_text(request.operator) is None:
        reasons.append("replacement_operator_required")
    if _optional_text(request.execution_kind) != "real":
        reasons.append("replacement_execution_not_real")
    if request.real_execution_confirmed is not True:
        reasons.append("replacement_real_execution_not_confirmed")
    return tuple(reasons)


def _old_identity_conflict(state: ReplacementLocalState, old_resource_id: str) -> bool:
    remote_resource_id = _optional_text(state.old_remote_resource_id)
    return remote_resource_id is not None and remote_resource_id != old_resource_id


def _new_identity_conflict(state: ReplacementLocalState, new_resource_id: str) -> bool:
    remote_resource_id = _optional_text(state.new_remote_resource_id)
    if remote_resource_id is None:
        return False
    if _normalized_status(state.new_sql_status) != NOT_UPLOADED_STATUS:
        return False
    return remote_resource_id != new_resource_id


def _old_is_replaced(state: ReplacementLocalState) -> bool:
    return REPLACED_STATUS in {
        _normalized_status(state.old_sql_status),
        _normalized_status(state.old_resource_status),
    }


def _known_new_resource_id(state: ReplacementLocalState) -> str | None:
    for value in (
        state.old_replaced_by_resource_id,
        state.new_remote_resource_id,
    ):
        text = _optional_text(value)
        if text is not None:
            return text
    return None


def _old_status_result(state: ReplacementLocalState) -> ReplacementLifecyclePlan | None:
    old_sql_status = _normalized_status(state.old_sql_status)
    old_resource_status = _normalized_status(state.old_resource_status)
    if old_sql_status == FAILED_STATUS:
        return _manual_audit(("old_replacement_failed_status_requires_manual_audit",))
    if WITHDRAWN_STATUS in {old_sql_status, old_resource_status}:
        return _manual_audit(("old_replacement_withdrawn_status_requires_manual_audit",))
    if _has_old_uploaded_or_active_proof(state):
        return None
    return _blocked(
        ("old_replacement_requires_uploaded_resource",),
        recommended_action="skip_replacement_not_uploaded",
    )


def _has_old_uploaded_or_active_proof(state: ReplacementLocalState) -> bool:
    if _normalized_status(state.old_sql_status) == UPLOADED_STATUS:
        return True
    if _normalized_status(state.old_resource_status) == ACTIVE_STATUS:
        return True
    return state.old_published_to_postgres and _optional_text(state.old_remote_resource_id) is not None


def _new_status_result(state: ReplacementLocalState) -> ReplacementLifecyclePlan | None:
    new_sql_status = _normalized_status(state.new_sql_status)
    new_resource_status = _normalized_status(state.new_resource_status)
    if REPLACED_STATUS in {new_sql_status, new_resource_status} or WITHDRAWN_STATUS in {
        new_sql_status,
        new_resource_status,
    }:
        return _blocked(
            ("new_replacement_target_terminal_status",),
            recommended_action="manual_audit_required",
        )
    if new_sql_status == UPLOADED_STATUS or new_resource_status == ACTIVE_STATUS or state.new_published_to_postgres:
        return _blocked(
            ("new_replacement_target_already_uploaded",),
            recommended_action="manual_audit_required",
        )
    if new_sql_status in {"", NOT_UPLOADED_STATUS} and new_resource_status in {"", NOT_UPLOADED_STATUS}:
        return None
    return _blocked(
        ("new_replacement_target_not_uploadable",),
        recommended_action="manual_audit_required",
    )


def _replacement_actions(
    *,
    request: ReplacementRequest,
    old_resource_id: str,
    new_resource_id: str,
    reason: str,
    operator: str,
) -> tuple[ReplacementLifecycleAction, ...]:
    replacement_batch_id = _optional_text(request.replacement_batch_id)
    lifecycle_event = _lifecycle_event(
        request=request,
        old_resource_id=old_resource_id,
        new_resource_id=new_resource_id,
        reason=reason,
        operator=operator,
        replacement_batch_id=replacement_batch_id,
    )
    old_link_fields, new_link_fields = _link_fields(request)

    return (
        ReplacementLifecycleAction(
            target="postgres_old_resource",
            operation="update_fields",
            fields={
                "resource_id": old_resource_id,
                "resource_status": REPLACED_STATUS,
                "replacement_reason": reason,
                **old_link_fields,
            },
        ),
        ReplacementLifecycleAction(
            target="postgres_new_resource",
            operation="upload_plan_reference",
            fields={
                "resource_id": new_resource_id,
                "resource_status": ACTIVE_STATUS,
                "replacement_of_resource_id": old_resource_id,
                "replacement_reason": reason,
                **new_link_fields,
            },
        ),
        ReplacementLifecycleAction(
            target="old_metadata",
            operation="merge_fields",
            fields={
                "sql_status": REPLACED_STATUS,
                "sql_upload_record_id": old_resource_id,
                "sql_replacement_record_id": new_resource_id,
                "sql_replacement_reason": reason,
                "sql_replacement_operator": operator,
                "sql_replacement_batch_id": replacement_batch_id,
                "sql_audit_history": [lifecycle_event],
            },
        ),
        ReplacementLifecycleAction(
            target="old_package_manifest",
            operation="merge_fields",
            fields={
                "resource_status": REPLACED_STATUS,
                "replacement_reason": reason,
                "replacement_operator": operator,
                "replacement_batch_id": replacement_batch_id,
                "replaced_by_resource_id": new_resource_id,
                "published_to_postgres": True,
                "published_to_postgres_scope": "preexisting_remote_record",
                "remote_resource_id": old_resource_id,
                "remote_resource_id_scope": "existing_postgres_resource_id",
            },
        ),
        ReplacementLifecycleAction(
            target="old_upload_log",
            operation="append_event",
            fields={
                "event": "replacement_success",
                "old_resource_id": old_resource_id,
                "new_resource_id": new_resource_id,
                "replacement_reason": reason,
                "replacement_operator": operator,
                "replacement_batch_id": replacement_batch_id,
            },
        ),
        ReplacementLifecycleAction(
            target="new_package_manifest",
            operation="merge_fields",
            fields={
                "replacement_of_resource_id": old_resource_id,
                "replacement_reason": reason,
                "replacement_operator": operator,
                "replacement_batch_id": replacement_batch_id,
            },
        ),
        ReplacementLifecycleAction(
            target="new_upload_log",
            operation="append_event",
            fields={
                "event": "replacement_created",
                "old_resource_id": old_resource_id,
                "new_resource_id": new_resource_id,
                "replacement_reason": reason,
                "replacement_operator": operator,
                "replacement_batch_id": replacement_batch_id,
            },
        ),
    )


def _lifecycle_event(
    *,
    request: ReplacementRequest,
    old_resource_id: str,
    new_resource_id: str,
    reason: str,
    operator: str,
    replacement_batch_id: str | None,
) -> dict[str, Any]:
    return {
        "action": "replacement_success",
        "old_resource_id": old_resource_id,
        "new_resource_id": new_resource_id,
        "replacement_reason": reason,
        "replacement_operator": operator,
        "replacement_batch_id": replacement_batch_id,
        "requested_at": _optional_text(request.requested_at),
        "execution_kind": _optional_text(request.execution_kind),
    }


def _link_fields(request: ReplacementRequest) -> tuple[dict[str, Any], dict[str, Any]]:
    old_paper_id = _optional_text(request.old_paper_id)
    new_paper_id = _optional_text(request.new_paper_id)
    if (
        old_paper_id is None
        or new_paper_id is None
        or request.old_version is None
        or request.new_version is None
    ):
        return {}, {}
    return (
        {
            "replaced_by_paper_id": new_paper_id,
            "replaced_by_version": request.new_version,
        },
        {
            "replaces_paper_id": old_paper_id,
            "replaces_version": request.old_version,
        },
    )


def _blocked(
    reasons: tuple[str, ...],
    *,
    recommended_action: str,
) -> ReplacementLifecyclePlan:
    return ReplacementLifecyclePlan(
        allowed=False,
        outcome="blocked",
        blocking_reasons=reasons,
        recommended_action=recommended_action,
    )


def _manual_audit(reasons: tuple[str, ...]) -> ReplacementLifecyclePlan:
    return ReplacementLifecyclePlan(
        allowed=False,
        outcome="manual_audit_required",
        blocking_reasons=reasons,
        recommended_action="manual_audit_required",
    )


def _normalized_status(value: str | None) -> str:
    return (_optional_text(value) or "").lower()


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
