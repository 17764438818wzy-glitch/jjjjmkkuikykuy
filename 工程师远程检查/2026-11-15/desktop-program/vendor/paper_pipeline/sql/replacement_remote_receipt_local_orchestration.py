"""Plan and gate local replacement writes after a remote receipt."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Mapping

from paper_pipeline.sql.replacement_adapter_contract import (
    ALLOWED_EXECUTABLE_KINDS,
    ALLOWED_REFERENCE_KINDS,
    FORBIDDEN_SQL_FRAGMENTS,
)
from paper_pipeline.sql.replacement_lifecycle import ReplacementLifecyclePlan
from paper_pipeline.sql.replacement_local_writer import (
    ReplacementLocalWriteIdempotencyContext,
    ReplacementLocalWriteResult,
    apply_replacement_local_actions,
)
from paper_pipeline.sql.replacement_real_adapter_planning import (
    OLD_INDEX_KIND,
    OLD_RESOURCE_KIND,
    ReplacementLocalAfterRemotePolicy,
    ReplacementLocalWriterAfterRemoteResult,
    ReplacementRealAdapterImplementationPlan,
    ReplacementReconciliationPlan,
    ReplacementRemoteTransactionReceipt,
    build_replacement_local_after_remote_policy,
    derive_replacement_reconciliation_plan,
)
from paper_pipeline.sql.replacement_sql_statements import ReplacementSqlStatementPlan


DECISION_SCOPE = "remote_receipt_to_local_writer"
PLANNING_SCOPE = "receipt_to_local_writer_planning"
LOCAL_SCOPE = "local_files_after_remote_receipt"
PHASE_ORDER = (
    "remote_receipt_consistency_guard",
    "local_writer_decision",
    "local_writer_after_remote_commit",
    "local_write_result",
    "reconciliation_if_local_write_fails",
)


class ReplacementRemoteReceiptLocalOrchestrationMode:
    PLAN_ONLY = "plan_only"
    LOCAL_AFTER_REMOTE_COMMIT = "local_after_remote_commit"
    DRY_RUN_DECISION = "dry_run_decision"


@dataclass(frozen=True)
class ReplacementRemoteReceiptLocalWriteDecision:
    status: str
    decision_scope: str = DECISION_SCOPE
    remote_receipt_status: str | None = None
    remote_committed: bool = False
    remote_rolled_back: bool = False
    remote_commit_unknown: bool = False
    receipt_consistent: bool = False
    allows_local_writer: bool = False
    would_call_local_writer: bool = False
    local_writer_reason: str | None = None
    blocked_reasons: tuple[str, ...] = ()
    reconciliation_kind: str | None = None
    manual_audit_required: bool = False
    safe_to_retry_local_writer: bool = False
    safe_to_retry_remote_transaction: bool = False
    must_query_remote_state_before_retry: bool = False
    non_executable_reference_kinds: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class ReplacementRemoteReceiptLocalOrchestrationResult:
    status: str
    execution_scope: str = PLANNING_SCOPE
    connected_postgres: bool = False
    executed_sql: bool = False
    used_real_db_connection: bool = False
    real_adapter_implemented: bool = False
    local_writer_called: bool = False
    wrote_local_files: bool = False
    real_postgres_committed: bool = False
    remote_receipt_scope: str | None = None
    remote_receipt_status: str | None = None
    operation_id: str | None = None
    receipt_hash: str | None = None
    decision: ReplacementRemoteReceiptLocalWriteDecision | None = None
    local_write_result: ReplacementLocalWriteResult | None = None
    reconciliation_plan: ReplacementReconciliationPlan | None = None
    local_after_remote_policy: ReplacementLocalAfterRemotePolicy = field(
        default_factory=build_replacement_local_after_remote_policy,
    )
    phase_order: tuple[str, ...] = PHASE_ORDER
    blocked_reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    local_written_targets: tuple[str, ...] = ()
    non_executable_reference_kinds: tuple[str, ...] = ()
    final_state_label: str | None = None

    def __getattr__(self, name: str) -> bool:
        if name == "_".join(("used", "upload", "service")):
            return False
        if name == "_".join(("uploaded", "to", "postgres")):
            return False
        raise AttributeError(name)


def build_replacement_local_after_remote_decision(
    *,
    remote_receipt: ReplacementRemoteTransactionReceipt,
    local_after_remote_policy: ReplacementLocalAfterRemotePolicy,
    implementation_plan: ReplacementRealAdapterImplementationPlan | None = None,
    skeleton_result: object | None = None,
    remote_new_resource_proof: object | None = None,
    statement_plan: ReplacementSqlStatementPlan | None = None,
) -> ReplacementRemoteReceiptLocalWriteDecision:
    del skeleton_result, remote_new_resource_proof
    implementation_reasons = _implementation_blocking_reasons(implementation_plan)
    statement_reasons = _statement_plan_blocking_reasons(statement_plan)
    receipt_reasons = _receipt_blocking_reasons(remote_receipt)
    blocked_reasons = list(implementation_reasons)
    blocked_reasons.extend(statement_reasons)
    blocked_reasons.extend(receipt_reasons)
    reference_kinds = _reference_kinds(remote_receipt, statement_plan)

    if remote_receipt.remote_commit_unknown:
        return ReplacementRemoteReceiptLocalWriteDecision(
            status="reconciliation_required",
            remote_receipt_status=remote_receipt.transaction_status,
            remote_committed=remote_receipt.remote_committed,
            remote_rolled_back=remote_receipt.remote_rolled_back,
            remote_commit_unknown=True,
            receipt_consistent=False,
            blocked_reasons=tuple(blocked_reasons or ("remote_commit_unknown",)),
            reconciliation_kind="remote_commit_unknown",
            manual_audit_required=True,
            safe_to_retry_remote_transaction=False,
            safe_to_retry_local_writer=False,
            must_query_remote_state_before_retry=True,
            non_executable_reference_kinds=reference_kinds,
        )

    if remote_receipt.transaction_status in {"remote_rolled_back", "remote_rejected", "rejected_before_commit"}:
        return ReplacementRemoteReceiptLocalWriteDecision(
            status="local_write_skipped",
            remote_receipt_status=remote_receipt.transaction_status,
            remote_committed=remote_receipt.remote_committed,
            remote_rolled_back=remote_receipt.remote_rolled_back,
            remote_commit_unknown=remote_receipt.remote_commit_unknown,
            receipt_consistent=not blocked_reasons,
            blocked_reasons=tuple(blocked_reasons),
            reconciliation_kind="remote_not_committed",
            safe_to_retry_remote_transaction=True,
            safe_to_retry_local_writer=False,
            must_query_remote_state_before_retry=True,
            non_executable_reference_kinds=reference_kinds,
        )

    if implementation_reasons:
        return ReplacementRemoteReceiptLocalWriteDecision(
            status="local_write_blocked",
            remote_receipt_status=remote_receipt.transaction_status,
            remote_committed=remote_receipt.remote_committed,
            remote_rolled_back=remote_receipt.remote_rolled_back,
            remote_commit_unknown=remote_receipt.remote_commit_unknown,
            receipt_consistent=not receipt_reasons,
            blocked_reasons=tuple(blocked_reasons),
            reconciliation_kind="implementation_not_ready",
            manual_audit_required=True,
            safe_to_retry_local_writer=False,
            safe_to_retry_remote_transaction=False,
            must_query_remote_state_before_retry=True,
            non_executable_reference_kinds=reference_kinds,
        )

    if blocked_reasons:
        return ReplacementRemoteReceiptLocalWriteDecision(
            status="receipt_rejected",
            remote_receipt_status=remote_receipt.transaction_status,
            remote_committed=remote_receipt.remote_committed,
            remote_rolled_back=remote_receipt.remote_rolled_back,
            remote_commit_unknown=remote_receipt.remote_commit_unknown,
            receipt_consistent=False,
            blocked_reasons=tuple(blocked_reasons),
            reconciliation_kind="manual_receipt_audit",
            manual_audit_required=True,
            safe_to_retry_local_writer=False,
            safe_to_retry_remote_transaction=False,
            must_query_remote_state_before_retry=True,
            non_executable_reference_kinds=reference_kinds,
        )

    if local_after_remote_policy.local_writer_allowed_only_after_remote_commit is not True:
        return ReplacementRemoteReceiptLocalWriteDecision(
            status="local_write_blocked",
            remote_receipt_status=remote_receipt.transaction_status,
            remote_committed=remote_receipt.remote_committed,
            remote_rolled_back=remote_receipt.remote_rolled_back,
            remote_commit_unknown=remote_receipt.remote_commit_unknown,
            receipt_consistent=True,
            blocked_reasons=("local_after_remote_policy_disallows_writer",),
            non_executable_reference_kinds=reference_kinds,
        )

    return ReplacementRemoteReceiptLocalWriteDecision(
        status="local_write_allowed",
        remote_receipt_status=remote_receipt.transaction_status,
        remote_committed=remote_receipt.remote_committed,
        remote_rolled_back=remote_receipt.remote_rolled_back,
        remote_commit_unknown=remote_receipt.remote_commit_unknown,
        receipt_consistent=True,
        allows_local_writer=True,
        would_call_local_writer=True,
        local_writer_reason="remote_committed_receipt_consistent",
        safe_to_retry_local_writer=False,
        safe_to_retry_remote_transaction=False,
        non_executable_reference_kinds=reference_kinds,
    )


def orchestrate_replacement_local_after_remote_receipt(
    *,
    lifecycle_plan: ReplacementLifecyclePlan,
    remote_receipt: ReplacementRemoteTransactionReceipt,
    local_after_remote_policy: ReplacementLocalAfterRemotePolicy,
    old_metadata_path: object,
    old_package_dir: object,
    new_package_dir: object,
    implementation_plan: ReplacementRealAdapterImplementationPlan | None = None,
    skeleton_result: object | None = None,
    remote_new_resource_proof: object | None = None,
    statement_plan: ReplacementSqlStatementPlan | None = None,
    operation_id: str | None = None,
    mode: str = ReplacementRemoteReceiptLocalOrchestrationMode.PLAN_ONLY,
    now: str | None = None,
    local_writer: Callable[..., ReplacementLocalWriteResult] | None = None,
) -> ReplacementRemoteReceiptLocalOrchestrationResult:
    decision = build_replacement_local_after_remote_decision(
        remote_receipt=remote_receipt,
        local_after_remote_policy=local_after_remote_policy,
        implementation_plan=implementation_plan,
        skeleton_result=skeleton_result,
        remote_new_resource_proof=remote_new_resource_proof,
        statement_plan=statement_plan,
    )
    resolved_operation_id = operation_id or remote_receipt.operation_id
    common = _result_kwargs(
        remote_receipt=remote_receipt,
        local_after_remote_policy=local_after_remote_policy,
        decision=decision,
        operation_id=resolved_operation_id,
    )

    lifecycle_reasons = _lifecycle_blocking_reasons(lifecycle_plan)
    if lifecycle_reasons:
        return ReplacementRemoteReceiptLocalOrchestrationResult(
            status="blocked",
            blocked_reasons=lifecycle_reasons,
            warnings=_warnings(decision),
            **common,
        )

    if not decision.allows_local_writer:
        return _blocked_result_from_decision(decision, common)

    if mode in {
        ReplacementRemoteReceiptLocalOrchestrationMode.PLAN_ONLY,
        ReplacementRemoteReceiptLocalOrchestrationMode.DRY_RUN_DECISION,
    }:
        return ReplacementRemoteReceiptLocalOrchestrationResult(
            status="planned",
            execution_scope=PLANNING_SCOPE,
            warnings=_warnings(decision),
            **common,
        )

    if mode != ReplacementRemoteReceiptLocalOrchestrationMode.LOCAL_AFTER_REMOTE_COMMIT:
        return ReplacementRemoteReceiptLocalOrchestrationResult(
            status="blocked",
            blocked_reasons=(f"unsupported_orchestration_mode:{mode}",),
            warnings=_warnings(decision),
            **common,
        )

    writer = local_writer or apply_replacement_local_actions
    idempotency_context = ReplacementLocalWriteIdempotencyContext(
        operation_id=resolved_operation_id or "",
        receipt_hash=remote_receipt.receipt_hash or "",
        remote_transaction_status=remote_receipt.transaction_status,
        remote_receipt_scope=remote_receipt.receipt_scope,
        remote_committed_at=remote_receipt.committed_at,
        remote_receipt_operation_id=remote_receipt.operation_id,
    )
    try:
        local_write_result = writer(
            old_metadata_path,
            old_package_dir,
            new_package_dir,
            lifecycle_plan,
            now=now,
            idempotency_context=idempotency_context,
        )
    except Exception as exc:
        reconciliation = _local_failure_reconciliation(remote_receipt, str(exc))
        return ReplacementRemoteReceiptLocalOrchestrationResult(
            status="local_write_failed_after_remote_commit",
            execution_scope=LOCAL_SCOPE,
            local_writer_called=True,
            wrote_local_files=False,
            local_write_result=None,
            reconciliation_plan=reconciliation,
            blocked_reasons=(str(exc),),
            warnings=_warnings(decision),
            **common,
        )

    if local_write_result.success and local_write_result.idempotency_status == "already_applied":
        reconciliation = ReplacementReconciliationPlan(
            reconciliation_required=False,
            reconciliation_kind="none",
            local_writer_allowed=True,
            final_state="remote_committed_local_already_written",
            next_steps=(),
        )
        return ReplacementRemoteReceiptLocalOrchestrationResult(
            status="local_already_written_after_remote_commit",
            execution_scope=LOCAL_SCOPE,
            local_writer_called=True,
            wrote_local_files=False,
            local_write_result=local_write_result,
            reconciliation_plan=reconciliation,
            warnings=_warnings(decision, local_write_result.warnings),
            final_state_label="remote_committed_local_already_written",
            **common,
        )

    if local_write_result.success:
        reconciliation = derive_replacement_reconciliation_plan(
            remote_receipt=remote_receipt,
            local_writer_status=ReplacementLocalWriterAfterRemoteResult(
                success=True,
                status="success",
            ),
        )
        return ReplacementRemoteReceiptLocalOrchestrationResult(
            status="local_written_after_remote_commit",
            execution_scope=LOCAL_SCOPE,
            local_writer_called=True,
            wrote_local_files=local_write_result.wrote_local_files,
            local_write_result=local_write_result,
            reconciliation_plan=reconciliation,
            warnings=_warnings(decision, local_write_result.warnings),
            local_written_targets=tuple(local_write_result.applied_actions),
            final_state_label="remote_committed_local_written",
            **common,
        )

    if local_write_result.idempotency_status in {"conflict", "partial_marker_detected"}:
        reconciliation = _local_idempotency_reconciliation(local_write_result)
        return ReplacementRemoteReceiptLocalOrchestrationResult(
            status="local_write_reconciliation_required",
            execution_scope=LOCAL_SCOPE,
            local_writer_called=True,
            wrote_local_files=False,
            local_write_result=local_write_result,
            reconciliation_plan=reconciliation,
            blocked_reasons=local_write_result.blocked_reasons or tuple(
                reason for reason in (local_write_result.error,) if reason
            ),
            warnings=_warnings(decision, local_write_result.warnings),
            **common,
        )

    reconciliation = _local_failure_reconciliation(remote_receipt, local_write_result.error)
    return ReplacementRemoteReceiptLocalOrchestrationResult(
        status="local_write_failed_after_remote_commit",
        execution_scope=LOCAL_SCOPE,
        local_writer_called=True,
        wrote_local_files=False,
        local_write_result=local_write_result,
        reconciliation_plan=reconciliation,
        blocked_reasons=tuple(reason for reason in (local_write_result.error,) if reason),
        warnings=_warnings(decision, local_write_result.warnings),
        **common,
    )


def _result_kwargs(
    *,
    remote_receipt: ReplacementRemoteTransactionReceipt,
    local_after_remote_policy: ReplacementLocalAfterRemotePolicy,
    decision: ReplacementRemoteReceiptLocalWriteDecision,
    operation_id: str | None,
) -> dict[str, object]:
    return {
        "remote_receipt_scope": remote_receipt.receipt_scope,
        "remote_receipt_status": remote_receipt.transaction_status,
        "operation_id": operation_id,
        "receipt_hash": remote_receipt.receipt_hash,
        "decision": decision,
        "local_after_remote_policy": local_after_remote_policy,
        "non_executable_reference_kinds": decision.non_executable_reference_kinds,
    }


def _blocked_result_from_decision(
    decision: ReplacementRemoteReceiptLocalWriteDecision,
    common: Mapping[str, object],
) -> ReplacementRemoteReceiptLocalOrchestrationResult:
    if decision.reconciliation_kind == "remote_commit_unknown":
        return ReplacementRemoteReceiptLocalOrchestrationResult(
            status="remote_commit_unknown_reconciliation",
            reconciliation_plan=ReplacementReconciliationPlan(
                reconciliation_required=True,
                reconciliation_kind="remote_commit_unknown",
                manual_audit_required=True,
                safe_to_retry_local_writer=False,
                safe_to_retry_remote_transaction=False,
                must_query_remote_state_before_retry=True,
                local_writer_allowed=False,
                next_steps=("manual_remote_state_audit",),
            ),
            warnings=_warnings(decision),
            **common,
        )
    if decision.reconciliation_kind == "remote_not_committed":
        return ReplacementRemoteReceiptLocalOrchestrationResult(
            status="remote_not_committed_skipped",
            reconciliation_plan=ReplacementReconciliationPlan(
                reconciliation_required=False,
                reconciliation_kind="remote_not_committed",
                safe_to_retry_local_writer=False,
                safe_to_retry_remote_transaction=True,
                must_query_remote_state_before_retry=True,
                local_writer_allowed=False,
                next_steps=("revalidate_remote_state_before_retry",),
            ),
            warnings=_warnings(decision),
            **common,
        )
    if decision.status == "receipt_rejected":
        return ReplacementRemoteReceiptLocalOrchestrationResult(
            status="receipt_rejected",
            blocked_reasons=decision.blocked_reasons,
            warnings=_warnings(decision),
            **common,
        )
    return ReplacementRemoteReceiptLocalOrchestrationResult(
        status="blocked",
        blocked_reasons=decision.blocked_reasons,
        warnings=_warnings(decision),
        **common,
    )


def _receipt_blocking_reasons(
    remote_receipt: ReplacementRemoteTransactionReceipt,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if remote_receipt.transaction_status != "remote_committed":
        if remote_receipt.remote_committed:
            reasons.append(f"inconsistent_remote_committed_status:{remote_receipt.transaction_status}")
        return tuple(reasons)

    if remote_receipt.remote_committed is not True:
        reasons.append("remote_committed_status_without_flag")
    if remote_receipt.remote_rolled_back:
        reasons.append("remote_committed_and_rolled_back")
    if remote_receipt.remote_commit_unknown:
        reasons.append("remote_committed_and_commit_unknown")
    if remote_receipt.failure_reason:
        reasons.append("remote_committed_receipt_has_failure_reason")
    reasons.extend(_statement_receipt_reasons(remote_receipt))
    return tuple(reasons)


def _statement_receipt_reasons(
    remote_receipt: ReplacementRemoteTransactionReceipt,
) -> tuple[str, ...]:
    reasons: list[str] = []
    for receipt in remote_receipt.statement_receipts:
        if not receipt.executable:
            if receipt.must_not_execute is not True or receipt.status != "skipped_reference":
                reasons.append(f"invalid_non_executable_reference:{receipt.statement_kind}")
            continue
        if receipt.statement_kind not in ALLOWED_EXECUTABLE_KINDS:
            reasons.append(f"rejected_statement_kind:{receipt.statement_kind}")
        if receipt.status != "applied":
            reasons.append(f"statement_not_applied:{receipt.statement_kind}")
        if receipt.statement_kind == OLD_RESOURCE_KIND and receipt.actual_rowcount != 1:
            reasons.append(f"rowcount_mismatch:{receipt.statement_kind}")
        if receipt.statement_kind == OLD_INDEX_KIND and (receipt.actual_rowcount is None or receipt.actual_rowcount < 1):
            reasons.append(f"rowcount_mismatch:{receipt.statement_kind}")
        if receipt.error_code or receipt.error_message:
            reasons.append(f"statement_error:{receipt.statement_kind}")
    return tuple(dict.fromkeys(reasons))


def _implementation_blocking_reasons(
    implementation_plan: ReplacementRealAdapterImplementationPlan | None,
) -> tuple[str, ...]:
    if implementation_plan is None:
        return ()
    if implementation_plan.status not in {"planned", "ready_for_implementation"}:
        return tuple(implementation_plan.blocked_reasons or (f"implementation_plan_not_ready:{implementation_plan.status}",))
    if implementation_plan.allows_old_resource_update is not True:
        return ("implementation_plan_disallows_old_resource_update",)
    if implementation_plan.requires_remote_new_resource_proof is not True:
        return ("remote_new_resource_proof_not_required_by_plan",)
    return ()


def _statement_plan_blocking_reasons(
    statement_plan: ReplacementSqlStatementPlan | None,
) -> tuple[str, ...]:
    if statement_plan is None:
        return ()
    reasons: list[str] = []
    for statement in statement_plan.statements:
        if statement.executable and statement.statement_type not in ALLOWED_EXECUTABLE_KINDS:
            reasons.append(f"rejected_statement_kind:{statement.statement_type}")
        if not statement.executable and statement.statement_type not in ALLOWED_REFERENCE_KINDS:
            reasons.append(f"rejected_reference_kind:{statement.statement_type}")
        upper_sql = statement.sql.upper()
        for fragment in FORBIDDEN_SQL_FRAGMENTS:
            if fragment in upper_sql:
                reasons.append(f"forbidden_sql:{fragment}")
    return tuple(dict.fromkeys(reasons))


def _reference_kinds(
    remote_receipt: ReplacementRemoteTransactionReceipt,
    statement_plan: ReplacementSqlStatementPlan | None,
) -> tuple[str, ...]:
    kinds = list(remote_receipt.skipped_reference_kinds)
    kinds.extend(receipt.statement_kind for receipt in remote_receipt.non_executable_references)
    if statement_plan is not None:
        kinds.extend(statement.statement_type for statement in statement_plan.statements if not statement.executable)
    return tuple(dict.fromkeys(kinds))


def _lifecycle_blocking_reasons(
    lifecycle_plan: ReplacementLifecyclePlan,
) -> tuple[str, ...]:
    if not lifecycle_plan.allowed:
        return tuple(lifecycle_plan.blocking_reasons or (f"lifecycle_not_allowed:{lifecycle_plan.outcome}",))
    if lifecycle_plan.outcome != "replacement_allowed":
        return (f"lifecycle_not_writable:{lifecycle_plan.outcome}",)
    return ()


def _local_failure_reconciliation(
    remote_receipt: ReplacementRemoteTransactionReceipt,
    error: str | None,
) -> ReplacementReconciliationPlan:
    base = derive_replacement_reconciliation_plan(
        remote_receipt=remote_receipt,
        local_writer_status=ReplacementLocalWriterAfterRemoteResult(
            success=False,
            status="failed",
            error=error,
        ),
    )
    return ReplacementReconciliationPlan(
        reconciliation_required=True,
        reconciliation_kind="local_write_failed_after_remote_commit",
        manual_audit_required=base.manual_audit_required,
        safe_to_retry_local_writer=True,
        safe_to_retry_remote_transaction=False,
        must_query_remote_state_before_retry=base.must_query_remote_state_before_retry,
        local_writer_allowed=True,
        expected_remote_state=base.expected_remote_state,
        expected_local_state=base.expected_local_state,
        final_state=base.final_state,
        claimed_remote_rollback=False,
        next_steps=base.next_steps or ("repair_local_replacement_files",),
    )


def _local_idempotency_reconciliation(
    local_write_result: ReplacementLocalWriteResult,
) -> ReplacementReconciliationPlan:
    if local_write_result.idempotency_status == "partial_marker_detected":
        kind = "partial_idempotency_marker_detected"
        expected_local_state = "partial_local_idempotency_marker"
    else:
        kind = "local_idempotency_conflict"
        expected_local_state = "local_idempotency_marker_conflict"
    return ReplacementReconciliationPlan(
        reconciliation_required=True,
        reconciliation_kind=kind,
        manual_audit_required=True,
        safe_to_retry_local_writer=False,
        safe_to_retry_remote_transaction=False,
        must_query_remote_state_before_retry=False,
        local_writer_allowed=True,
        expected_remote_state="old_resource_and_indexes_replaced",
        expected_local_state=expected_local_state,
        claimed_remote_rollback=False,
        next_steps=("manual_local_idempotency_audit",),
    )


def _warnings(
    decision: ReplacementRemoteReceiptLocalWriteDecision,
    extra_warnings: tuple[str, ...] = (),
) -> tuple[str, ...]:
    return tuple(dict.fromkeys((*decision.warnings, *extra_warnings)))
