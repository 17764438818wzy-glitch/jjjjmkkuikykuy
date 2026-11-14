"""Pure duplicate-upload guard for formal SQL upload decisions."""

from __future__ import annotations

from dataclasses import dataclass


UPLOAD_MODE = "upload"
RETRY_AFTER_FAILED_MODE = "retry_after_failed"
WRITEBACK_REPAIR_MODE = "writeback_repair"

_ALLOWED_OPERATION_MODES = {
    UPLOAD_MODE,
    RETRY_AFTER_FAILED_MODE,
    WRITEBACK_REPAIR_MODE,
}
_TERMINAL_SQL_STATUSES = {"withdrawn", "replaced"}
_NOT_UPLOADED_STATUSES = {None, "", "not_uploaded"}
_REQUIRED_IDENTITY_FIELDS = ("resource_id", "upload_batch_id")
_OPTIONAL_IDENTITY_FIELDS = ("confirmation_id", "field_snapshot_hash")


@dataclass(frozen=True)
class UploadIdentity:
    resource_id: str | None = None
    upload_batch_id: str | None = None
    confirmation_id: str | None = None
    field_snapshot_hash: str | None = None


@dataclass(frozen=True)
class UploadDuplicateGuardState:
    manifest_published_to_postgres: bool = False
    metadata_sql_status: str | None = "not_uploaded"
    manifest_identity: UploadIdentity | None = None
    metadata_identity: UploadIdentity | None = None
    upload_log_success_identity: UploadIdentity | None = None


@dataclass(frozen=True)
class UploadDuplicateGuardResult:
    allowed: bool
    outcome: str
    reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    recommended_action: str | None = None


@dataclass(frozen=True)
class _IdentityCheck:
    consistent: bool
    missing_required: bool = False


def check_duplicate_upload_guard(
    state: UploadDuplicateGuardState,
    candidate_identity: UploadIdentity | None = None,
    *,
    operation_mode: str = UPLOAD_MODE,
) -> UploadDuplicateGuardResult:
    if operation_mode not in _ALLOWED_OPERATION_MODES:
        return _blocked(
            "unknown_operation_mode",
            recommended_action="manual_audit_required",
        )

    sql_status = _normalized_sql_status(state.metadata_sql_status)
    if sql_status in _TERMINAL_SQL_STATUSES:
        return _manual_audit("terminal_sql_status_upload_forbidden")

    if (
        state.manifest_published_to_postgres
        and sql_status == "uploaded"
    ):
        return _published_and_uploaded_result(state, candidate_identity)

    if operation_mode == UPLOAD_MODE:
        return _upload_result(state, sql_status)
    if operation_mode == RETRY_AFTER_FAILED_MODE:
        return _retry_result(state, sql_status)
    return _repair_result(state, candidate_identity, sql_status)


def _upload_result(
    state: UploadDuplicateGuardState,
    sql_status: str,
) -> UploadDuplicateGuardResult:
    if not state.manifest_published_to_postgres:
        if sql_status == "not_uploaded":
            return UploadDuplicateGuardResult(
                allowed=True,
                outcome="upload_allowed",
                recommended_action="proceed_upload",
            )
        if sql_status == "failed":
            return _blocked(
                "retry_requires_explicit_mode",
                recommended_action="retry_after_failed",
            )
        if sql_status == "uploaded":
            return _blocked(
                "metadata_already_uploaded",
                recommended_action="manual_audit_or_repair_manifest",
            )
        return _manual_audit("metadata_sql_status_not_uploadable")

    if sql_status == "not_uploaded":
        return _blocked(
            "manifest_already_published",
            recommended_action="repair_metadata",
        )
    if sql_status == "failed":
        return _blocked(
            "manifest_already_published",
            recommended_action="repair_metadata_or_manual_audit",
        )
    return _blocked(
        "manifest_already_published",
        recommended_action="manual_audit_required",
    )


def _retry_result(
    state: UploadDuplicateGuardState,
    sql_status: str,
) -> UploadDuplicateGuardResult:
    if state.manifest_published_to_postgres:
        return _blocked(
            "manifest_already_published",
            recommended_action="repair_metadata_or_manual_audit",
        )
    if sql_status == "failed":
        return UploadDuplicateGuardResult(
            allowed=True,
            outcome="retry_allowed",
            recommended_action="proceed_retry_upload",
        )
    return _blocked(
        "retry_requires_failed_status",
        recommended_action="manual_audit_required",
    )


def _repair_result(
    state: UploadDuplicateGuardState,
    candidate_identity: UploadIdentity | None,
    sql_status: str,
) -> UploadDuplicateGuardResult:
    if not state.manifest_published_to_postgres:
        if sql_status == "uploaded":
            return _repair_manifest_result(state, candidate_identity)
        if sql_status == "failed":
            return _blocked(
                "retry_requires_explicit_mode",
                recommended_action="retry_after_failed",
            )
        return _blocked(
            "repair_requires_consistent_ids",
            recommended_action="proceed_upload",
        )

    if sql_status in {"not_uploaded", "failed"}:
        return _repair_metadata_result(state, candidate_identity)

    return _blocked(
        "manifest_already_published",
        recommended_action="manual_audit_required",
    )


def _published_and_uploaded_result(
    state: UploadDuplicateGuardState,
    candidate_identity: UploadIdentity | None,
) -> UploadDuplicateGuardResult:
    identity_check = _consistent_identities(
        candidate_identity,
        state.manifest_identity,
        state.metadata_identity,
    )
    if identity_check.consistent:
        return UploadDuplicateGuardResult(
            allowed=False,
            outcome="already_uploaded_noop",
            reasons=("already_uploaded_noop",),
            recommended_action="skip_upload_noop",
        )
    if identity_check.missing_required:
        return _manual_audit("identity_missing_for_consistency_check")
    return _blocked(
        "uploaded_conflict",
        recommended_action="manual_audit_required",
    )


def _repair_manifest_result(
    state: UploadDuplicateGuardState,
    candidate_identity: UploadIdentity | None,
) -> UploadDuplicateGuardResult:
    proof_identities = tuple(
        identity
        for identity in (
            state.metadata_identity,
            state.upload_log_success_identity,
        )
        if identity is not None
    )
    identity_check = _consistent_identities(candidate_identity, *proof_identities)
    if identity_check.consistent and proof_identities:
        return UploadDuplicateGuardResult(
            allowed=True,
            outcome="repair_allowed",
            recommended_action="repair_manifest",
        )
    return _manual_audit(
        "repair_requires_consistent_ids",
        warnings=_identity_warnings(identity_check),
    )


def _repair_metadata_result(
    state: UploadDuplicateGuardState,
    candidate_identity: UploadIdentity | None,
) -> UploadDuplicateGuardResult:
    identity_check = _consistent_identities(candidate_identity, state.manifest_identity)
    if identity_check.consistent:
        return UploadDuplicateGuardResult(
            allowed=True,
            outcome="repair_allowed",
            recommended_action="repair_metadata",
        )
    return _manual_audit(
        "repair_requires_consistent_ids",
        warnings=_identity_warnings(identity_check),
    )


def _consistent_identities(
    candidate_identity: UploadIdentity | None,
    *stored_identities: UploadIdentity | None,
) -> _IdentityCheck:
    identities = (candidate_identity, *stored_identities)
    if any(identity is None for identity in identities):
        return _IdentityCheck(consistent=False, missing_required=True)

    concrete_identities = tuple(
        identity for identity in identities if identity is not None
    )
    for field_name in _REQUIRED_IDENTITY_FIELDS:
        values = tuple(_identity_field(identity, field_name) for identity in concrete_identities)
        if any(value is None for value in values):
            return _IdentityCheck(consistent=False, missing_required=True)
        if len(set(values)) != 1:
            return _IdentityCheck(consistent=False)

    for field_name in _OPTIONAL_IDENTITY_FIELDS:
        values = tuple(
            value
            for identity in concrete_identities
            if (value := _identity_field(identity, field_name)) is not None
        )
        if len(set(values)) > 1:
            return _IdentityCheck(consistent=False)

    return _IdentityCheck(consistent=True)


def _identity_field(identity: UploadIdentity, field_name: str) -> str | None:
    value = getattr(identity, field_name)
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _identity_warnings(identity_check: _IdentityCheck) -> tuple[str, ...]:
    if identity_check.missing_required:
        return ("identity_missing_for_consistency_check",)
    return ()


def _normalized_sql_status(value: str | None) -> str:
    if value in _NOT_UPLOADED_STATUSES:
        return "not_uploaded"
    normalized = str(value).strip().lower()
    if normalized in _NOT_UPLOADED_STATUSES:
        return "not_uploaded"
    return normalized


def _blocked(
    reason: str,
    *,
    recommended_action: str,
    warnings: tuple[str, ...] = (),
) -> UploadDuplicateGuardResult:
    return UploadDuplicateGuardResult(
        allowed=False,
        outcome="blocked",
        reasons=(reason,),
        warnings=warnings,
        recommended_action=recommended_action,
    )


def _manual_audit(
    reason: str,
    *,
    warnings: tuple[str, ...] = (),
) -> UploadDuplicateGuardResult:
    return UploadDuplicateGuardResult(
        allowed=False,
        outcome="manual_audit_required",
        reasons=(reason,),
        warnings=warnings,
        recommended_action="manual_audit_required",
    )
