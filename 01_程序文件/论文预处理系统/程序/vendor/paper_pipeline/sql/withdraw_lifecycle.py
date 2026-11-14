"""Pure withdraw lifecycle planner.

This module builds intended lifecycle actions from an explicit request and a
caller-provided state snapshot. It performs no I/O and owns no database access.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Mapping


WITHDRAWN_STATUS = "withdrawn"
ACTIVE_STATUS = "active"
UPLOADED_STATUS = "uploaded"
FAILED_STATUS = "failed"
REPLACED_STATUS = "replaced"
NOT_UPLOADED_STATUS = "not_uploaded"


@dataclass(frozen=True)
class WithdrawRequest:
    resource_id: str | None
    upload_batch_id: str | None = None
    reason: str | None = None
    operator: str | None = None
    requested_at: str | None = None
    execution_kind: str = "real"
    real_execution_confirmed: bool = False
    withdraw_batch_id: str | None = None


@dataclass(frozen=True)
class WithdrawLocalState:
    metadata_sql_status: str | None = NOT_UPLOADED_STATUS
    manifest_published_to_postgres: bool = False
    manifest_resource_status: str | None = None
    manifest_remote_resource_id: str | None = None
    manifest_upload_batch_id: str | None = None
    postgres_resource_status: str | None = None


@dataclass(frozen=True)
class WithdrawLifecycleAction:
    target: str
    operation: str
    fields: Mapping[str, Any]


@dataclass(frozen=True)
class WithdrawLifecyclePlan:
    allowed: bool
    outcome: str
    actions: tuple[WithdrawLifecycleAction, ...] = ()
    blocking_reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    recommended_action: str | None = None


def build_withdraw_lifecycle_plan(
    request: WithdrawRequest,
    current_state: WithdrawLocalState,
) -> WithdrawLifecyclePlan:
    request_reasons = _request_blocking_reasons(request)
    if request_reasons:
        return _blocked(request_reasons, recommended_action="fix_withdraw_request")

    resource_id = _required_text(request.resource_id)
    upload_batch_id = _optional_text(request.upload_batch_id)
    reason = _required_text(request.reason)
    operator = _required_text(request.operator)

    if _identity_conflict(
        current_state,
        resource_id=resource_id,
        upload_batch_id=upload_batch_id,
    ):
        return _manual_audit(("withdraw_identity_conflict",))

    if _already_withdrawn(current_state):
        return WithdrawLifecyclePlan(
            allowed=True,
            outcome="noop",
            warnings=("withdraw_already_applied",),
            recommended_action="skip_withdraw_noop",
        )

    if _has_replaced_status(current_state):
        return _manual_audit(("withdraw_replaced_status_requires_manual_audit",))

    has_remote_or_active_proof = _has_uploaded_or_active_proof(
        current_state,
        resource_id=resource_id,
    )
    if _normalized_status(current_state.metadata_sql_status) == FAILED_STATUS:
        if not has_remote_or_active_proof:
            return _manual_audit(("withdraw_failed_status_requires_manual_audit",))

    if not has_remote_or_active_proof:
        return _blocked(
            ("withdraw_requires_uploaded_resource",),
            recommended_action="skip_withdraw_not_uploaded",
        )

    withdrawn_at = _optional_text(request.requested_at) or _utc_now_iso()
    withdraw_batch_id = _optional_text(request.withdraw_batch_id) or upload_batch_id
    actions = _withdraw_actions(
        resource_id=resource_id,
        upload_batch_id=upload_batch_id,
        reason=reason,
        operator=operator,
        withdrawn_at=withdrawn_at,
        withdraw_batch_id=withdraw_batch_id,
        execution_kind=request.execution_kind,
    )
    return WithdrawLifecyclePlan(
        allowed=True,
        outcome="withdraw_allowed",
        actions=actions,
        recommended_action="proceed_withdraw",
    )


def _request_blocking_reasons(request: WithdrawRequest) -> tuple[str, ...]:
    reasons: list[str] = []
    if _optional_text(request.resource_id) is None:
        reasons.append("withdraw_resource_id_required")
    if _optional_text(request.reason) is None:
        reasons.append("withdraw_reason_required")
    if _optional_text(request.operator) is None:
        reasons.append("withdraw_operator_required")
    if request.execution_kind != "real":
        reasons.append("withdraw_execution_not_real")
    if not request.real_execution_confirmed:
        reasons.append("withdraw_real_execution_not_confirmed")
    return tuple(reasons)


def _identity_conflict(
    state: WithdrawLocalState,
    *,
    resource_id: str,
    upload_batch_id: str | None,
) -> bool:
    manifest_resource_id = _optional_text(state.manifest_remote_resource_id)
    if manifest_resource_id is not None and manifest_resource_id != resource_id:
        return True

    manifest_upload_batch_id = _optional_text(state.manifest_upload_batch_id)
    return (
        manifest_upload_batch_id is not None
        and upload_batch_id is not None
        and manifest_upload_batch_id != upload_batch_id
    )


def _already_withdrawn(state: WithdrawLocalState) -> bool:
    return WITHDRAWN_STATUS in {
        _normalized_status(state.metadata_sql_status),
        _normalized_status(state.manifest_resource_status),
        _normalized_status(state.postgres_resource_status),
    }


def _has_replaced_status(state: WithdrawLocalState) -> bool:
    return REPLACED_STATUS in {
        _normalized_status(state.metadata_sql_status),
        _normalized_status(state.manifest_resource_status),
        _normalized_status(state.postgres_resource_status),
    }


def _has_uploaded_or_active_proof(
    state: WithdrawLocalState,
    *,
    resource_id: str,
) -> bool:
    if _normalized_status(state.metadata_sql_status) == UPLOADED_STATUS:
        return True
    if _normalized_status(state.manifest_resource_status) == ACTIVE_STATUS:
        return True
    if _normalized_status(state.postgres_resource_status) == ACTIVE_STATUS:
        return True

    manifest_resource_id = _optional_text(state.manifest_remote_resource_id)
    return (
        state.manifest_published_to_postgres
        and manifest_resource_id is not None
        and manifest_resource_id == resource_id
    )


def _withdraw_actions(
    *,
    resource_id: str,
    upload_batch_id: str | None,
    reason: str,
    operator: str,
    withdrawn_at: str,
    withdraw_batch_id: str | None,
    execution_kind: str,
) -> tuple[WithdrawLifecycleAction, ...]:
    lifecycle_event = _lifecycle_event(
        resource_id=resource_id,
        upload_batch_id=upload_batch_id,
        reason=reason,
        operator=operator,
        withdrawn_at=withdrawn_at,
        withdraw_batch_id=withdraw_batch_id,
        execution_kind=execution_kind,
    )
    return (
        WithdrawLifecycleAction(
            target="postgres_resource",
            operation="update_fields",
            fields={
                "resource_id": resource_id,
                "resource_status": WITHDRAWN_STATUS,
                "withdrawn_at": withdrawn_at,
                "withdrawn_by": operator,
                "withdrawn_reason": reason,
            },
        ),
        WithdrawLifecycleAction(
            target="metadata",
            operation="merge_fields",
            fields={
                "sql_status": WITHDRAWN_STATUS,
                "sql_upload_record_id": resource_id,
                "sql_batch_id": upload_batch_id,
                "sql_audit_history": [lifecycle_event],
            },
        ),
        WithdrawLifecycleAction(
            target="package_manifest",
            operation="merge_fields",
            fields={
                "resource_status": WITHDRAWN_STATUS,
                "withdrawn_at": withdrawn_at,
                "withdraw_reason": reason,
                "withdraw_operator": operator,
                "withdraw_batch_id": withdraw_batch_id,
                "published_to_postgres": True,
                "remote_resource_id": resource_id,
                "upload_batch_id": upload_batch_id,
            },
        ),
        WithdrawLifecycleAction(
            target="upload_log",
            operation="append_event",
            fields={
                "event": "withdraw_success",
                "execution_kind": execution_kind,
                "resource_id": resource_id,
                "upload_batch_id": upload_batch_id,
                "withdraw_reason": reason,
                "withdraw_operator": operator,
                "withdraw_batch_id": withdraw_batch_id,
            },
        ),
    )


def _lifecycle_event(
    *,
    resource_id: str,
    upload_batch_id: str | None,
    reason: str,
    operator: str,
    withdrawn_at: str,
    withdraw_batch_id: str | None,
    execution_kind: str,
) -> dict[str, Any]:
    return {
        "action": "withdraw_success",
        "resource_id": resource_id,
        "upload_batch_id": upload_batch_id,
        "withdraw_reason": reason,
        "withdraw_operator": operator,
        "withdrawn_at": withdrawn_at,
        "withdraw_batch_id": withdraw_batch_id,
        "execution_kind": execution_kind,
    }


def _blocked(
    reasons: tuple[str, ...],
    *,
    recommended_action: str,
) -> WithdrawLifecyclePlan:
    return WithdrawLifecyclePlan(
        allowed=False,
        outcome="blocked",
        blocking_reasons=reasons,
        recommended_action=recommended_action,
    )


def _manual_audit(reasons: tuple[str, ...]) -> WithdrawLifecyclePlan:
    return WithdrawLifecyclePlan(
        allowed=False,
        outcome="manual_audit_required",
        blocking_reasons=reasons,
        recommended_action="manual_audit_required",
    )


def _normalized_status(value: str | None) -> str:
    normalized = _optional_text(value)
    if normalized is None:
        return NOT_UPLOADED_STATUS
    return normalized.lower()


def _required_text(value: str | None) -> str:
    normalized = _optional_text(value)
    if normalized is None:
        raise ValueError("required text missing after validation")
    return normalized


def _optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()
