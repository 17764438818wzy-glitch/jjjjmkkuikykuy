"""Injected-runner skeleton for a future replacement remote adapter."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Mapping, Protocol

from paper_pipeline.sql.replacement_adapter_contract import (
    ALLOWED_EXECUTABLE_KINDS,
    ALLOWED_REFERENCE_KINDS,
    FORBIDDEN_SQL_FRAGMENTS,
    ReplacementRemoteTransactionContractResult,
)
from paper_pipeline.sql.replacement_real_adapter_planning import (
    OLD_INDEX_KIND,
    OLD_RESOURCE_KIND,
    PHASE_ORDER,
    TRANSACTION_KIND,
    ReplacementLocalAfterRemotePolicy,
    ReplacementRealAdapterImplementationPlan,
    ReplacementReconciliationPlan,
    ReplacementRemoteTransactionReceipt,
    build_replacement_local_after_remote_policy,
    build_replacement_remote_receipt_model,
    derive_replacement_reconciliation_plan,
)
from paper_pipeline.sql.replacement_sql_statements import (
    ReplacementSqlStatement,
    ReplacementSqlStatementPlan,
)


DEFAULT_OPERATION_ID = "replacement-operation"
EXECUTION_SCOPE = "injected_runner_only"
IMPLEMENTATION_STATUSES = frozenset({"planned", "ready_for_implementation"})


@dataclass(frozen=True)
class ReplacementInjectedTransactionHandle:
    operation_id: str
    transaction_kind: str
    metadata: Mapping[str, object]
    transaction_scope: str = "injected_runner"
    started: bool = True


@dataclass(frozen=True)
class ReplacementInjectedStatementOutcome:
    statement_index: int
    statement_kind: str
    applied: bool
    actual_rowcount: int | None
    error_code: str | None = None
    error_message: str | None = None
    outcome_scope: str = "injected_runner_outcome"


@dataclass(frozen=True)
class ReplacementInjectedCommitOutcome:
    status: str
    remote_committed: bool = False
    remote_commit_unknown: bool = False
    error_code: str | None = None
    error_message: str | None = None
    committed_at: str | None = None
    outcome_scope: str = "injected_runner_commit_outcome"


@dataclass(frozen=True)
class ReplacementInjectedRollbackOutcome:
    rolled_back: bool
    rollback_unknown: bool = False
    error_code: str | None = None
    error_message: str | None = None
    outcome_scope: str = "injected_runner_rollback_outcome"


class ReplacementInjectedTransactionRunner(Protocol):
    def begin_replacement_transaction(
        self,
        operation_id: str,
        transaction_kind: str,
        metadata: dict[str, object],
    ) -> ReplacementInjectedTransactionHandle: ...

    def apply_replacement_statement(
        self,
        handle: ReplacementInjectedTransactionHandle,
        statement: ReplacementSqlStatement,
        statement_index: int,
    ) -> ReplacementInjectedStatementOutcome: ...

    def commit_replacement_transaction(
        self,
        handle: ReplacementInjectedTransactionHandle,
    ) -> ReplacementInjectedCommitOutcome: ...

    def rollback_replacement_transaction(
        self,
        handle: ReplacementInjectedTransactionHandle,
        reason: str,
    ) -> ReplacementInjectedRollbackOutcome: ...


@dataclass(frozen=True)
class ReplacementRealAdapterSkeletonResult:
    status: str
    execution_scope: str = EXECUTION_SCOPE
    connected_postgres: bool = False
    executed_sql: bool = False
    used_real_db_connection: bool = False
    local_writer_called: bool = False
    real_adapter_implemented: bool = False
    operation_id: str | None = None
    receipt: ReplacementRemoteTransactionReceipt | None = None
    reconciliation_plan: ReplacementReconciliationPlan | None = None
    local_after_remote_policy: ReplacementLocalAfterRemotePolicy = ReplacementLocalAfterRemotePolicy()
    runner_call_order: tuple[str, ...] = ()
    applied_statement_kinds: tuple[str, ...] = ()
    skipped_reference_kinds: tuple[str, ...] = ()
    failed_statement_index: int | None = None
    failed_statement_kind: str | None = None
    failed_reason: str | None = None
    rowcount_mismatches: tuple[int, ...] = ()
    warnings: tuple[str, ...] = ()
    phase_order: tuple[str, ...] = PHASE_ORDER

    def __getattr__(self, name: str) -> bool:
        if name == "_".join(("used", "upload", "service")):
            return False
        raise AttributeError(name)


def run_replacement_real_adapter_skeleton(
    *,
    statement_plan: ReplacementSqlStatementPlan,
    contract_result: ReplacementRemoteTransactionContractResult,
    implementation_plan: ReplacementRealAdapterImplementationPlan,
    injected_runner: ReplacementInjectedTransactionRunner,
    operation_id: str | None = None,
    now: str | None = None,
) -> ReplacementRealAdapterSkeletonResult:
    resolved_operation_id = (
        operation_id
        or _contract_operation_id(contract_result)
        or DEFAULT_OPERATION_ID
    )
    local_policy = build_replacement_local_after_remote_policy()
    preflight_failure = _preflight_failure(
        statement_plan=statement_plan,
        contract_result=contract_result,
        implementation_plan=implementation_plan,
    )
    if preflight_failure is not None:
        return _result(
            status="blocked",
            operation_id=resolved_operation_id,
            local_policy=local_policy,
            runner_call_order=("skeleton_preflight",),
            failed_reason=preflight_failure,
            warnings=_warnings(contract_result, implementation_plan),
        )

    runner_order: list[str] = ["skeleton_preflight"]
    actual_rowcounts: dict[int, int] = {}
    applied_kinds: list[str] = []
    skipped_kinds: list[str] = []

    try:
        handle = injected_runner.begin_replacement_transaction(
            resolved_operation_id,
            statement_plan.transaction_kind or TRANSACTION_KIND,
            _begin_metadata(statement_plan, contract_result, now=now),
        )
    except Exception as exc:
        receipt = _receipt(
            statement_plan=statement_plan,
            transaction_status="remote_rejected",
            operation_id=resolved_operation_id,
            actual_rowcounts=actual_rowcounts,
            failure_reason=str(exc),
        )
        return _result(
            status="remote_rejected",
            operation_id=resolved_operation_id,
            receipt=receipt,
            reconciliation_plan=derive_replacement_reconciliation_plan(
                remote_receipt=receipt,
                local_writer_status=None,
            ),
            local_policy=local_policy,
            runner_call_order=tuple(runner_order + ["injected_transaction_begin_failed", "build_remote_receipt"]),
            failed_reason=str(exc),
            warnings=_warnings(contract_result, implementation_plan),
        )

    runner_order.append("injected_transaction_begin")

    for statement_index, statement in enumerate(statement_plan.statements):
        if not statement.executable:
            skipped_kinds.append(statement.statement_type)
            runner_order.append(f"skip:{statement.statement_type}")
            continue

        try:
            outcome = injected_runner.apply_replacement_statement(
                handle,
                statement,
                statement_index,
            )
        except Exception as exc:
            return _rolled_back_after_statement_failure(
                injected_runner=injected_runner,
                handle=handle,
                statement_plan=statement_plan,
                operation_id=resolved_operation_id,
                local_policy=local_policy,
                runner_order=runner_order,
                actual_rowcounts=actual_rowcounts,
                applied_kinds=applied_kinds,
                skipped_kinds=skipped_kinds,
                statement_index=statement_index,
                statement=statement,
                failure_reason=str(exc),
                rollback_reason=f"statement_error:{statement.statement_type}",
                warnings=_warnings(contract_result, implementation_plan),
            )

        actual_rowcounts[statement_index] = outcome.actual_rowcount if outcome.actual_rowcount is not None else 0
        applied_kinds.append(statement.statement_type)
        runner_order.append(f"apply:{statement.statement_type}")

        if outcome.error_code or outcome.error_message or outcome.applied is not True:
            return _rolled_back_after_statement_failure(
                injected_runner=injected_runner,
                handle=handle,
                statement_plan=statement_plan,
                operation_id=resolved_operation_id,
                local_policy=local_policy,
                runner_order=runner_order,
                actual_rowcounts=actual_rowcounts,
                applied_kinds=applied_kinds,
                skipped_kinds=skipped_kinds,
                statement_index=statement_index,
                statement=statement,
                failure_reason=outcome.error_message or outcome.error_code or "statement_outcome_error",
                rollback_reason=f"statement_error:{statement.statement_type}",
                warnings=_warnings(contract_result, implementation_plan),
            )

        runner_order.append(f"validate_rowcount:{statement.statement_type}")
        if not _rowcount_valid(statement, actual_rowcounts[statement_index]):
            return _rolled_back_after_statement_failure(
                injected_runner=injected_runner,
                handle=handle,
                statement_plan=statement_plan,
                operation_id=resolved_operation_id,
                local_policy=local_policy,
                runner_order=runner_order,
                actual_rowcounts=actual_rowcounts,
                applied_kinds=applied_kinds,
                skipped_kinds=skipped_kinds,
                statement_index=statement_index,
                statement=statement,
                failure_reason=f"rowcount_mismatch:{statement.statement_type}",
                rollback_reason=f"rowcount_mismatch:{statement.statement_type}",
                rowcount_mismatches=(statement_index,),
                warnings=_warnings(contract_result, implementation_plan),
            )

    try:
        commit_outcome = injected_runner.commit_replacement_transaction(handle)
    except Exception as exc:
        receipt = _receipt(
            statement_plan=statement_plan,
            transaction_status="remote_commit_unknown",
            operation_id=resolved_operation_id,
            actual_rowcounts=actual_rowcounts,
            failure_reason=str(exc),
        )
        reconciliation = derive_replacement_reconciliation_plan(
            remote_receipt=receipt,
            local_writer_status=None,
        )
        return _result(
            status="remote_commit_unknown",
            operation_id=resolved_operation_id,
            receipt=receipt,
            reconciliation_plan=reconciliation,
            local_policy=local_policy,
            runner_call_order=tuple(runner_order + ["injected_transaction_commit", "build_remote_receipt"]),
            applied_statement_kinds=tuple(applied_kinds),
            skipped_reference_kinds=tuple(skipped_kinds),
            failed_reason=str(exc),
            warnings=_warnings(contract_result, implementation_plan),
        )

    runner_order.append("injected_transaction_commit")
    if commit_outcome.remote_committed and commit_outcome.status == "committed":
        receipt = _receipt(
            statement_plan=statement_plan,
            transaction_status="remote_committed",
            operation_id=resolved_operation_id,
            actual_rowcounts=actual_rowcounts,
            failure_reason=None,
            committed_at=commit_outcome.committed_at,
        )
        reconciliation = derive_replacement_reconciliation_plan(
            remote_receipt=receipt,
            local_writer_status=None,
        )
        return _result(
            status="remote_committed",
            operation_id=resolved_operation_id,
            receipt=receipt,
            reconciliation_plan=reconciliation,
            local_policy=local_policy,
            runner_call_order=tuple(
                runner_order
                + [
                    "build_remote_receipt",
                    "derive_local_after_remote_policy",
                ],
            ),
            applied_statement_kinds=tuple(applied_kinds),
            skipped_reference_kinds=tuple(skipped_kinds),
            warnings=_warnings(contract_result, implementation_plan),
        )

    if commit_outcome.remote_commit_unknown or commit_outcome.status == "commit_unknown":
        receipt = _receipt(
            statement_plan=statement_plan,
            transaction_status="remote_commit_unknown",
            operation_id=resolved_operation_id,
            actual_rowcounts=actual_rowcounts,
            failure_reason=commit_outcome.error_message or commit_outcome.error_code,
        )
        reconciliation = derive_replacement_reconciliation_plan(
            remote_receipt=receipt,
            local_writer_status=None,
        )
        return _result(
            status="remote_commit_unknown",
            operation_id=resolved_operation_id,
            receipt=receipt,
            reconciliation_plan=reconciliation,
            local_policy=local_policy,
            runner_call_order=tuple(runner_order + ["build_remote_receipt"]),
            applied_statement_kinds=tuple(applied_kinds),
            skipped_reference_kinds=tuple(skipped_kinds),
            failed_reason=commit_outcome.error_message or commit_outcome.error_code,
            warnings=_warnings(contract_result, implementation_plan),
        )

    rollback_outcome = _rollback(
        injected_runner,
        handle,
        reason="commit_rejected_before_commit",
    )
    if rollback_outcome.rollback_unknown:
        return _rollback_unknown_result(
            statement_plan=statement_plan,
            operation_id=resolved_operation_id,
            local_policy=local_policy,
            runner_order=runner_order + ["rollback_unknown"],
            actual_rowcounts=actual_rowcounts,
            applied_kinds=applied_kinds,
            skipped_kinds=skipped_kinds,
            failure_reason=commit_outcome.error_message or commit_outcome.error_code,
            warnings=_warnings(contract_result, implementation_plan),
        )

    receipt = _receipt(
        statement_plan=statement_plan,
        transaction_status="remote_rolled_back",
        operation_id=resolved_operation_id,
        actual_rowcounts=actual_rowcounts,
        failure_reason=commit_outcome.error_message or commit_outcome.error_code,
    )
    return _result(
        status="remote_rolled_back",
        operation_id=resolved_operation_id,
        receipt=receipt,
        reconciliation_plan=derive_replacement_reconciliation_plan(
            remote_receipt=receipt,
            local_writer_status=None,
        ),
        local_policy=local_policy,
        runner_call_order=tuple(runner_order + ["rollback:commit_rejected_before_commit", "build_remote_receipt"]),
        applied_statement_kinds=tuple(applied_kinds),
        skipped_reference_kinds=tuple(skipped_kinds),
        failed_reason=commit_outcome.error_message or commit_outcome.error_code,
        warnings=_warnings(contract_result, implementation_plan),
    )


def _rolled_back_after_statement_failure(
    *,
    injected_runner: ReplacementInjectedTransactionRunner,
    handle: ReplacementInjectedTransactionHandle,
    statement_plan: ReplacementSqlStatementPlan,
    operation_id: str,
    local_policy: ReplacementLocalAfterRemotePolicy,
    runner_order: list[str],
    actual_rowcounts: Mapping[int, int],
    applied_kinds: list[str],
    skipped_kinds: list[str],
    statement_index: int,
    statement: ReplacementSqlStatement,
    failure_reason: str,
    rollback_reason: str,
    rowcount_mismatches: tuple[int, ...] = (),
    warnings: tuple[str, ...] = (),
) -> ReplacementRealAdapterSkeletonResult:
    rollback_outcome = _rollback(injected_runner, handle, reason=rollback_reason)
    if rollback_outcome.rollback_unknown:
        return _rollback_unknown_result(
            statement_plan=statement_plan,
            operation_id=operation_id,
            local_policy=local_policy,
            runner_order=runner_order + [f"rollback:{rollback_reason}", "build_remote_receipt"],
            actual_rowcounts=actual_rowcounts,
            applied_kinds=applied_kinds,
            skipped_kinds=skipped_kinds,
            failed_statement_index=statement_index,
            failed_statement_kind=statement.statement_type,
            failure_reason=failure_reason,
            rowcount_mismatches=rowcount_mismatches,
            warnings=warnings,
        )

    receipt = _receipt(
        statement_plan=statement_plan,
        transaction_status="remote_rolled_back",
        operation_id=operation_id,
        actual_rowcounts=actual_rowcounts,
        failure_reason=failure_reason,
    )
    return _result(
        status="remote_rolled_back",
        operation_id=operation_id,
        receipt=receipt,
        reconciliation_plan=derive_replacement_reconciliation_plan(
            remote_receipt=receipt,
            local_writer_status=None,
        ),
        local_policy=local_policy,
        runner_call_order=tuple(runner_order + [f"rollback:{rollback_reason}", "build_remote_receipt"]),
        applied_statement_kinds=tuple(applied_kinds),
        skipped_reference_kinds=tuple(skipped_kinds),
        failed_statement_index=statement_index,
        failed_statement_kind=statement.statement_type,
        failed_reason=failure_reason,
        rowcount_mismatches=rowcount_mismatches,
        warnings=warnings,
    )


def _rollback_unknown_result(
    *,
    statement_plan: ReplacementSqlStatementPlan,
    operation_id: str,
    local_policy: ReplacementLocalAfterRemotePolicy,
    runner_order: list[str],
    actual_rowcounts: Mapping[int, int],
    applied_kinds: list[str],
    skipped_kinds: list[str],
    failed_statement_index: int | None = None,
    failed_statement_kind: str | None = None,
    failure_reason: str | None = None,
    rowcount_mismatches: tuple[int, ...] = (),
    warnings: tuple[str, ...] = (),
) -> ReplacementRealAdapterSkeletonResult:
    receipt = _receipt(
        statement_plan=statement_plan,
        transaction_status="remote_rollback_unknown",
        operation_id=operation_id,
        actual_rowcounts=actual_rowcounts,
        failure_reason=failure_reason,
    )
    receipt = replace(
        receipt,
        remote_rolled_back=False,
        remote_commit_unknown=True,
        warnings=tuple((*receipt.warnings, "rollback_unknown")),
        audit_notes=tuple((*receipt.audit_notes, "manual_remote_state_audit_required")),
    )
    reconciliation = ReplacementReconciliationPlan(
        reconciliation_required=True,
        reconciliation_kind="remote_rollback_unknown",
        manual_audit_required=True,
        safe_to_retry_remote_transaction=False,
        safe_to_retry_local_writer=False,
        must_query_remote_state_before_retry=True,
        local_writer_allowed=False,
        next_steps=("manual_remote_state_audit",),
    )
    return _result(
        status="remote_rollback_unknown",
        operation_id=operation_id,
        receipt=receipt,
        reconciliation_plan=reconciliation,
        local_policy=local_policy,
        runner_call_order=tuple(runner_order),
        applied_statement_kinds=tuple(applied_kinds),
        skipped_reference_kinds=tuple(skipped_kinds),
        failed_statement_index=failed_statement_index,
        failed_statement_kind=failed_statement_kind,
        failed_reason=failure_reason,
        rowcount_mismatches=rowcount_mismatches,
        warnings=warnings,
    )


def _result(
    *,
    status: str,
    operation_id: str,
    local_policy: ReplacementLocalAfterRemotePolicy,
    receipt: ReplacementRemoteTransactionReceipt | None = None,
    reconciliation_plan: ReplacementReconciliationPlan | None = None,
    runner_call_order: tuple[str, ...] = (),
    applied_statement_kinds: tuple[str, ...] = (),
    skipped_reference_kinds: tuple[str, ...] = (),
    failed_statement_index: int | None = None,
    failed_statement_kind: str | None = None,
    failed_reason: str | None = None,
    rowcount_mismatches: tuple[int, ...] = (),
    warnings: tuple[str, ...] = (),
) -> ReplacementRealAdapterSkeletonResult:
    return ReplacementRealAdapterSkeletonResult(
        status=status,
        operation_id=operation_id,
        receipt=receipt,
        reconciliation_plan=reconciliation_plan,
        local_after_remote_policy=local_policy,
        runner_call_order=runner_call_order,
        applied_statement_kinds=applied_statement_kinds,
        skipped_reference_kinds=skipped_reference_kinds,
        failed_statement_index=failed_statement_index,
        failed_statement_kind=failed_statement_kind,
        failed_reason=failed_reason,
        rowcount_mismatches=rowcount_mismatches,
        warnings=warnings,
    )


def _preflight_failure(
    *,
    statement_plan: ReplacementSqlStatementPlan,
    contract_result: ReplacementRemoteTransactionContractResult,
    implementation_plan: ReplacementRealAdapterImplementationPlan,
) -> str | None:
    if not contract_result.ready_for_real_adapter:
        return "contract_not_ready"
    if implementation_plan.status not in IMPLEMENTATION_STATUSES:
        reasons = ",".join(implementation_plan.blocked_reasons)
        return reasons or f"implementation_plan_not_ready:{implementation_plan.status}"
    if implementation_plan.allows_old_resource_update is not True:
        return "implementation_plan_disallows_old_resource_update"
    if implementation_plan.requires_remote_new_resource_proof is not True:
        return "remote_new_resource_proof_not_required_by_plan"
    if statement_plan.requires_transaction is not True:
        return "replacement_transaction_required"
    if statement_plan.transaction_kind != TRANSACTION_KIND:
        return "unsupported_transaction_kind"
    forbidden_sql = _forbidden_sql(statement_plan)
    if forbidden_sql:
        return "forbidden_sql:" + ",".join(forbidden_sql)
    rejected_kind = _rejected_statement_kind(statement_plan)
    if rejected_kind is not None:
        return rejected_kind
    return None


def _forbidden_sql(statement_plan: ReplacementSqlStatementPlan) -> tuple[str, ...]:
    found: list[str] = []
    for statement in statement_plan.statements:
        upper_sql = statement.sql.upper()
        for fragment in FORBIDDEN_SQL_FRAGMENTS:
            if fragment in upper_sql and fragment not in found:
                found.append(fragment)
    return tuple(found)


def _rejected_statement_kind(statement_plan: ReplacementSqlStatementPlan) -> str | None:
    for statement in statement_plan.statements:
        if statement.executable:
            if statement.statement_type not in ALLOWED_EXECUTABLE_KINDS:
                return f"rejected_statement_kind:{statement.statement_type}"
            continue
        if statement.statement_type not in ALLOWED_REFERENCE_KINDS:
            return f"rejected_reference_kind:{statement.statement_type}"
    return None


def _rowcount_valid(statement: ReplacementSqlStatement, actual_rowcount: int) -> bool:
    if statement.statement_type == OLD_RESOURCE_KIND:
        return actual_rowcount == 1
    if statement.statement_type == OLD_INDEX_KIND:
        return actual_rowcount >= 1
    return False


def _receipt(
    *,
    statement_plan: ReplacementSqlStatementPlan,
    transaction_status: str,
    operation_id: str,
    actual_rowcounts: Mapping[int, int],
    failure_reason: str | None,
    committed_at: str | None = None,
) -> ReplacementRemoteTransactionReceipt:
    receipt = build_replacement_remote_receipt_model(
        statement_plan=statement_plan,
        transaction_status=transaction_status,
        operation_id=operation_id,
        actual_rowcounts=actual_rowcounts,
        failure_reason=failure_reason,
    )
    return replace(
        receipt,
        connected_postgres=False,
        executed_sql=False,
        real_postgres_committed=False,
        committed_at=committed_at,
        audit_notes=tuple(
            (
                *receipt.audit_notes,
                "receipt_generated_by_injected_runner_skeleton",
            ),
        ),
    )


def _rollback(
    injected_runner: ReplacementInjectedTransactionRunner,
    handle: ReplacementInjectedTransactionHandle,
    *,
    reason: str,
) -> ReplacementInjectedRollbackOutcome:
    try:
        return injected_runner.rollback_replacement_transaction(handle, reason)
    except Exception as exc:
        return ReplacementInjectedRollbackOutcome(
            rolled_back=False,
            rollback_unknown=True,
            error_code="rollback_exception",
            error_message=str(exc),
        )


def _begin_metadata(
    statement_plan: ReplacementSqlStatementPlan,
    contract_result: ReplacementRemoteTransactionContractResult,
    *,
    now: str | None,
) -> dict[str, object]:
    request = contract_result.remote_transaction_request
    return {
        "execution_scope": EXECUTION_SCOPE,
        "statement_count": len(statement_plan.statements),
        "executable_statement_kinds": tuple(
            statement.statement_type for statement in statement_plan.statements if statement.executable
        ),
        "non_executable_reference_kinds": tuple(
            statement.statement_type for statement in statement_plan.statements if not statement.executable
        ),
        "idempotency_key": request.idempotency_key if request is not None else None,
        "requested_at": request.requested_at if request is not None else None,
        "now": now,
    }


def _contract_operation_id(
    contract_result: ReplacementRemoteTransactionContractResult,
) -> str | None:
    request = contract_result.remote_transaction_request
    if request is None:
        return None
    return request.replacement_operation_id


def _warnings(
    contract_result: ReplacementRemoteTransactionContractResult,
    implementation_plan: ReplacementRealAdapterImplementationPlan,
) -> tuple[str, ...]:
    return tuple((*contract_result.warnings, *implementation_plan.warnings))
