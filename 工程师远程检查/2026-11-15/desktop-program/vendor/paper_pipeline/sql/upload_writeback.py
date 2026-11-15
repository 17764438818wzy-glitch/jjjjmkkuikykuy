"""Build local upload writeback plans from formal upload outcomes.

The planner returns intended local actions only. Applying those actions belongs
to a later layer that can own file I/O, locking, and audit persistence.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from paper_pipeline.sql.formal_adapter import FormalUploadExecutionResult
from paper_pipeline.sql.formal_upload_plan import FormalUploadPlan


@dataclass(frozen=True)
class UploadWritebackContext:
    execution_kind: str
    sql_mode: str
    allow_write: bool
    real_execution_confirmed: bool
    confirmation_id: str | None = None
    field_snapshot_hash: str | None = None
    adapter_name: str | None = None
    operator_name: str | None = None


@dataclass(frozen=True)
class LocalUploadState:
    metadata_sql_status: str = "not_uploaded"
    manifest_published_to_postgres: bool = False
    manifest_remote_resource_id: str | None = None
    manifest_upload_batch_id: str | None = None
    manifest_confirmation_id: str | None = None
    manifest_field_snapshot_hash: str | None = None


@dataclass(frozen=True)
class UploadWritebackAction:
    target: str
    operation: str
    fields: Mapping[str, Any]


@dataclass(frozen=True)
class UploadWritebackPlan:
    allowed: bool
    outcome: str
    actions: tuple[UploadWritebackAction, ...]
    blocking_reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()


def build_upload_writeback_plan(
    upload_plan: FormalUploadPlan,
    result: FormalUploadExecutionResult,
    context: UploadWritebackContext,
    current_state: LocalUploadState | None = None,
) -> UploadWritebackPlan:
    state = current_state or LocalUploadState()

    proof_reasons = _writeback_proof_reasons(context)
    if proof_reasons:
        return _blocked(proof_reasons)

    if result.success:
        return _success_plan(upload_plan, result, context, state)

    return _failure_plan(upload_plan, result, context, state)


def _writeback_proof_reasons(context: UploadWritebackContext) -> tuple[str, ...]:
    reasons: list[str] = []
    if context.execution_kind != "real":
        reasons.append("writeback_execution_not_real")
    if context.sql_mode != "real":
        reasons.append("writeback_sql_mode_not_real")
    if not context.allow_write:
        reasons.append("writeback_write_not_allowed")
    if not context.real_execution_confirmed:
        reasons.append("writeback_real_execution_not_confirmed")
    return tuple(reasons)


def _success_plan(
    upload_plan: FormalUploadPlan,
    result: FormalUploadExecutionResult,
    context: UploadWritebackContext,
    state: LocalUploadState,
) -> UploadWritebackPlan:
    validation_reasons = _success_validation_reasons(upload_plan, result)
    if validation_reasons:
        return _blocked(validation_reasons)

    if state.manifest_published_to_postgres:
        if _same_manifest_writeback(upload_plan, context, state):
            return UploadWritebackPlan(
                allowed=True,
                outcome="noop",
                actions=(),
                warnings=("upload_writeback_already_applied",),
            )
        return _blocked(("upload_writeback_conflict_existing_upload",))

    warnings = _result_warnings(result)
    return UploadWritebackPlan(
        allowed=True,
        outcome="success",
        actions=(
            _package_manifest_action(upload_plan, result, context),
            _metadata_uploaded_action(upload_plan),
            _success_log_action(upload_plan, result, context),
        ),
        warnings=warnings,
    )


def _failure_plan(
    upload_plan: FormalUploadPlan,
    result: FormalUploadExecutionResult,
    context: UploadWritebackContext,
    state: LocalUploadState,
) -> UploadWritebackPlan:
    if state.metadata_sql_status == "uploaded" or state.manifest_published_to_postgres:
        return _blocked(("uploaded_to_failed_forbidden",))

    return UploadWritebackPlan(
        allowed=True,
        outcome="failure",
        actions=(
            _metadata_failed_action(upload_plan, result),
            _failure_log_action(upload_plan, result, context),
        ),
    )


def _success_validation_reasons(
    upload_plan: FormalUploadPlan,
    result: FormalUploadExecutionResult,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if result.resource_id != upload_plan.resource_id:
        reasons.append("writeback_resource_id_mismatch")
    if result.upload_batch_id != upload_plan.upload_batch_id:
        reasons.append("writeback_upload_batch_id_mismatch")
    if result.executed_count != len(upload_plan.records):
        reasons.append("writeback_executed_count_mismatch")
    if not _contains_all_plan_record_ids(upload_plan, result):
        reasons.append("writeback_record_ids_incomplete")
    if result.failed_record_id is not None:
        reasons.append("writeback_failed_record_present")
    return tuple(reasons)


def _contains_all_plan_record_ids(
    upload_plan: FormalUploadPlan,
    result: FormalUploadExecutionResult,
) -> bool:
    planned_record_ids = {record.record_id for record in upload_plan.records}
    return planned_record_ids.issubset(set(result.record_ids))


def _same_manifest_writeback(
    upload_plan: FormalUploadPlan,
    context: UploadWritebackContext,
    state: LocalUploadState,
) -> bool:
    return (
        state.manifest_remote_resource_id == upload_plan.resource_id
        and state.manifest_upload_batch_id == upload_plan.upload_batch_id
        and state.manifest_confirmation_id == context.confirmation_id
        and state.manifest_field_snapshot_hash == context.field_snapshot_hash
    )


def _package_manifest_action(
    upload_plan: FormalUploadPlan,
    result: FormalUploadExecutionResult,
    context: UploadWritebackContext,
) -> UploadWritebackAction:
    fields: dict[str, Any] = {
        "published_to_postgres": True,
        "remote_resource_id": upload_plan.resource_id,
        "upload_batch_id": upload_plan.upload_batch_id,
        "confirmation_id": context.confirmation_id,
        "field_snapshot_hash": context.field_snapshot_hash,
        "statement_count": result.executed_count,
        "record_ids": result.record_ids,
    }
    _include_optional_context_fields(fields, context)
    return UploadWritebackAction(
        target="package_manifest",
        operation="merge_fields",
        fields=fields,
    )


def _metadata_uploaded_action(upload_plan: FormalUploadPlan) -> UploadWritebackAction:
    return UploadWritebackAction(
        target="metadata",
        operation="merge_fields",
        fields={
            "sql_status": "uploaded",
            "sql_batch_id": upload_plan.upload_batch_id,
            "sql_upload_record_id": upload_plan.resource_id,
        },
    )


def _metadata_failed_action(
    upload_plan: FormalUploadPlan,
    result: FormalUploadExecutionResult,
) -> UploadWritebackAction:
    return UploadWritebackAction(
        target="metadata",
        operation="merge_fields",
        fields={
            "sql_status": "failed",
            "sql_batch_id": upload_plan.upload_batch_id,
            "sql_upload_error": result.error,
        },
    )


def _success_log_action(
    upload_plan: FormalUploadPlan,
    result: FormalUploadExecutionResult,
    context: UploadWritebackContext,
) -> UploadWritebackAction:
    fields: dict[str, Any] = {
        "event": "upload_success",
        "execution_kind": context.execution_kind,
        "resource_id": upload_plan.resource_id,
        "upload_batch_id": upload_plan.upload_batch_id,
        "executed_count": result.executed_count,
        "record_ids": result.record_ids,
        "confirmation_id": context.confirmation_id,
        "field_snapshot_hash": context.field_snapshot_hash,
    }
    _include_optional_context_fields(fields, context)
    if result.error is not None:
        fields["warning"] = result.error
    return UploadWritebackAction(
        target="upload_log",
        operation="append_event",
        fields=fields,
    )


def _failure_log_action(
    upload_plan: FormalUploadPlan,
    result: FormalUploadExecutionResult,
    context: UploadWritebackContext,
) -> UploadWritebackAction:
    fields: dict[str, Any] = {
        "event": "upload_failure",
        "execution_kind": context.execution_kind,
        "resource_id": upload_plan.resource_id,
        "upload_batch_id": upload_plan.upload_batch_id,
        "executed_count": result.executed_count,
        "record_ids": result.record_ids,
        "failed_record_id": result.failed_record_id,
        "error": result.error,
    }
    _include_optional_context_fields(fields, context)
    return UploadWritebackAction(
        target="upload_log",
        operation="append_event",
        fields=fields,
    )


def _include_optional_context_fields(
    fields: dict[str, Any],
    context: UploadWritebackContext,
) -> None:
    if context.adapter_name is not None:
        fields["adapter_name"] = context.adapter_name
    if context.operator_name is not None:
        fields["operator_name"] = context.operator_name


def _result_warnings(result: FormalUploadExecutionResult) -> tuple[str, ...]:
    if result.error is None:
        return ()
    return ("upload_writeback_execution_result_warning", result.error)


def _blocked(reasons: tuple[str, ...]) -> UploadWritebackPlan:
    return UploadWritebackPlan(
        allowed=False,
        outcome="blocked",
        actions=(),
        blocking_reasons=reasons,
    )
