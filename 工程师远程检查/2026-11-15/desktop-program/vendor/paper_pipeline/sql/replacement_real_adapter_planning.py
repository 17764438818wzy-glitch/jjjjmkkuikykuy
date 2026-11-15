"""Planning-only models for a future replacement remote adapter."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Mapping

from paper_pipeline.sql.replacement_adapter_contract import (
    ReplacementRemoteTransactionContractResult,
)
from paper_pipeline.sql.replacement_sql_statements import (
    ReplacementSqlStatement,
    ReplacementSqlStatementPlan,
)


TRANSACTION_KIND = "single_resource_replacement"
OLD_RESOURCE_KIND = "replacement_old_resource_update"
OLD_INDEX_KIND = "replacement_old_index_status_update"
NEW_REFERENCE_KIND = "replacement_new_resource_upload_reference"
PLANNING_SCOPE = "planning_only"
PHASE_ORDER = (
    "lifecycle_validation",
    "sql_statement_plan",
    "adapter_contract_validation",
    "remote_new_resource_proof_validation",
    "remote_transaction_begin",
    "remote_executable_statements",
    "remote_rowcount_validation",
    "remote_transaction_commit",
    "remote_receipt",
    "local_success_writer_after_remote_commit",
    "reconciliation_if_local_write_fails",
)


@dataclass(frozen=True)
class ReplacementRemoteNewResourceProof:
    new_resource_id: str
    paper_id: str
    version: int
    resource_status: str = "active"
    sql_status: str = "uploaded"
    replaces_paper_id: str | None = None
    replaces_version: int | None = None
    replacement_reason: str | None = None
    proof_scope: str = "remote_postgres_resource_snapshot"
    proof_collected_at: str | None = None
    source: str = "test_fixture"
    verified: bool = True


@dataclass(frozen=True)
class ReplacementRemoteStatementReceipt:
    statement_index: int
    statement_kind: str
    executable: bool
    table_name: str
    status: str = "planned"
    expected_rowcount: int | None = None
    expected_min_rowcount: int | None = None
    actual_rowcount: int | None = None
    rowcount_policy: str = "not_applicable"
    must_not_execute: bool = False
    resource_id: str | None = None
    params_hash: str | None = None
    sql_text_hash: str | None = None
    error_code: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class ReplacementRemoteTransactionReceipt:
    receipt_scope: str = "real_remote_transaction_receipt_model"
    operation_id: str | None = None
    transaction_kind: str = TRANSACTION_KIND
    transaction_status: str = "remote_rolled_back"
    remote_committed: bool = False
    remote_rolled_back: bool = False
    remote_commit_unknown: bool = False
    connected_postgres: bool = False
    executed_sql: bool = False
    real_postgres_committed: bool = False
    statement_receipts: tuple[ReplacementRemoteStatementReceipt, ...] = ()
    non_executable_references: tuple[ReplacementRemoteStatementReceipt, ...] = ()
    executable_statement_kinds: tuple[str, ...] = ()
    skipped_reference_kinds: tuple[str, ...] = ()
    started_at: str | None = None
    finished_at: str | None = None
    committed_at: str | None = None
    rolled_back_at: str | None = None
    adapter_name: str | None = None
    database_session_id: str | None = None
    receipt_hash: str | None = None
    failure_reason: str | None = None
    warnings: tuple[str, ...] = ()
    audit_notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class ReplacementLocalAfterRemotePolicy:
    local_writer_allowed_only_after_remote_commit: bool = True
    remote_failure_prevents_local_success_writer: bool = True
    local_failure_after_remote_commit_policy: str = "reconciliation_required_after_remote_commit"
    local_success_writer: str = "S18-2 replacement_local_writer"
    local_failure_writer: str = "not_implemented"
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class ReplacementLocalWriterAfterRemoteResult:
    success: bool
    status: str
    error: str | None = None


@dataclass(frozen=True)
class ReplacementReconciliationPlan:
    reconciliation_required: bool
    reconciliation_kind: str
    manual_audit_required: bool = False
    safe_to_retry_local_writer: bool = False
    safe_to_retry_remote_transaction: bool = False
    must_query_remote_state_before_retry: bool = False
    local_writer_allowed: bool = False
    expected_remote_state: str | None = None
    expected_local_state: str | None = None
    final_state: str | None = None
    claimed_remote_rollback: bool = False
    next_steps: tuple[str, ...] = ()


@dataclass(frozen=True)
class ReplacementRealAdapterImplementationPlan:
    status: str
    implementation_scope: str = PLANNING_SCOPE
    adapter_kind: str = PLANNING_SCOPE
    connected_postgres: bool = False
    executed_sql: bool = False
    real_adapter_implemented: bool = False
    requires_existing_contract_ready: bool = True
    requires_remote_transaction: bool = True
    requires_remote_new_resource_proof: bool = True
    allows_old_resource_update: bool = False
    allows_local_writer_after_remote_commit: bool = False
    new_upload_reference_policy: str = "non_executable_reference_only"
    receipt_policy: str = "future_remote_transaction_receipt_required"
    reconciliation_policy: str = "reconciliation_required_after_remote_commit"
    blocked_reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    next_required_phase: str = "manual_audit_or_no_action"
    phase_order: tuple[str, ...] = PHASE_ORDER
    remote_receipt_model: ReplacementRemoteTransactionReceipt | None = None


def build_replacement_real_adapter_implementation_plan(
    *,
    contract_result: ReplacementRemoteTransactionContractResult,
    statement_plan: ReplacementSqlStatementPlan,
    remote_new_resource_proof: ReplacementRemoteNewResourceProof | None = None,
) -> ReplacementRealAdapterImplementationPlan:
    if not contract_result.ready_for_real_adapter:
        return _blocked_plan_from_contract(contract_result)

    proof_reasons = _proof_blocking_reasons(statement_plan, remote_new_resource_proof)
    if proof_reasons:
        return ReplacementRealAdapterImplementationPlan(
            status="blocked",
            blocked_reasons=proof_reasons,
            warnings=tuple(contract_result.warnings),
            next_required_phase="collect_remote_new_resource_active_proof",
        )

    return ReplacementRealAdapterImplementationPlan(
        status="planned",
        allows_old_resource_update=True,
        allows_local_writer_after_remote_commit=True,
        warnings=tuple(contract_result.warnings),
        next_required_phase="S18-7B_real_adapter_skeleton_with_injected_fake_connection",
    )


def build_replacement_remote_receipt_model(
    *,
    statement_plan: ReplacementSqlStatementPlan,
    transaction_status: str,
    operation_id: str | None = None,
    actual_rowcounts: Mapping[int, int] | None = None,
    failure_reason: str | None = None,
) -> ReplacementRemoteTransactionReceipt:
    rowcounts = dict(actual_rowcounts or {})
    statement_receipts = tuple(
        _statement_receipt(
            index=index,
            statement=statement,
            transaction_status=transaction_status,
            actual_rowcount=rowcounts.get(index),
            failure_reason=failure_reason,
        )
        for index, statement in enumerate(statement_plan.statements)
    )
    non_executable_references = tuple(receipt for receipt in statement_receipts if not receipt.executable)
    remote_committed = transaction_status == "remote_committed"
    remote_commit_unknown = transaction_status == "remote_commit_unknown"
    remote_rolled_back = transaction_status in {"remote_rolled_back", "remote_rejected"}
    return ReplacementRemoteTransactionReceipt(
        operation_id=operation_id,
        transaction_kind=statement_plan.transaction_kind or TRANSACTION_KIND,
        transaction_status=transaction_status,
        remote_committed=remote_committed,
        remote_rolled_back=remote_rolled_back,
        remote_commit_unknown=remote_commit_unknown,
        real_postgres_committed=remote_committed,
        statement_receipts=statement_receipts,
        non_executable_references=non_executable_references,
        executable_statement_kinds=tuple(receipt.statement_kind for receipt in statement_receipts if receipt.executable),
        skipped_reference_kinds=tuple(receipt.statement_kind for receipt in non_executable_references),
        failure_reason=failure_reason,
        receipt_hash=_receipt_hash(
            transaction_status=transaction_status,
            operation_id=operation_id,
            statement_receipts=statement_receipts,
        ),
    )


def derive_replacement_reconciliation_plan(
    *,
    remote_receipt: ReplacementRemoteTransactionReceipt,
    local_writer_status: ReplacementLocalWriterAfterRemoteResult | None,
) -> ReplacementReconciliationPlan:
    if remote_receipt.remote_commit_unknown:
        return ReplacementReconciliationPlan(
            reconciliation_required=True,
            reconciliation_kind="remote_commit_unknown",
            manual_audit_required=True,
            must_query_remote_state_before_retry=True,
            safe_to_retry_remote_transaction=False,
            local_writer_allowed=False,
            next_steps=("manual_remote_state_audit",),
        )

    if not remote_receipt.remote_committed:
        return ReplacementReconciliationPlan(
            reconciliation_required=False,
            reconciliation_kind="remote_not_committed",
            safe_to_retry_remote_transaction=True,
            safe_to_retry_local_writer=False,
            must_query_remote_state_before_retry=True,
            local_writer_allowed=False,
            next_steps=("revalidate_remote_state_before_retry",),
        )

    if local_writer_status is None:
        return ReplacementReconciliationPlan(
            reconciliation_required=False,
            reconciliation_kind="none",
            local_writer_allowed=True,
            next_steps=("run_local_success_writer",),
        )

    if local_writer_status.success:
        return ReplacementReconciliationPlan(
            reconciliation_required=False,
            reconciliation_kind="none",
            local_writer_allowed=True,
            final_state="remote_committed_local_written",
        )

    return ReplacementReconciliationPlan(
        reconciliation_required=True,
        reconciliation_kind="local_write_failed_after_remote_commit",
        safe_to_retry_local_writer=True,
        safe_to_retry_remote_transaction=False,
        must_query_remote_state_before_retry=False,
        local_writer_allowed=True,
        expected_remote_state="old_resource_and_indexes_replaced",
        expected_local_state="local_success_files_not_confirmed",
        next_steps=("repair_local_replacement_files",),
    )


def build_replacement_local_after_remote_policy() -> ReplacementLocalAfterRemotePolicy:
    return ReplacementLocalAfterRemotePolicy()


def _blocked_plan_from_contract(
    contract_result: ReplacementRemoteTransactionContractResult,
) -> ReplacementRealAdapterImplementationPlan:
    reasons = tuple(contract_result.blocked_reasons or contract_result.audit_reasons)
    return ReplacementRealAdapterImplementationPlan(
        status=contract_result.status,
        blocked_reasons=reasons,
        warnings=tuple(contract_result.warnings),
        next_required_phase=_next_phase_for_contract_status(contract_result.status),
    )


def _next_phase_for_contract_status(status: str) -> str:
    if status in {"manual_audit", "conflict"}:
        return "manual_audit"
    if status == "noop":
        return "no_action"
    return "fix_replacement_contract"


def _proof_blocking_reasons(
    statement_plan: ReplacementSqlStatementPlan,
    proof: ReplacementRemoteNewResourceProof | None,
) -> tuple[str, ...]:
    if proof is None:
        return ("new_resource_active_proof_missing",)

    context = _statement_context(statement_plan)
    checks = (
        (proof.verified is True, "new_resource_proof_not_verified"),
        (proof.resource_status == "active", "new_resource_proof_not_active"),
        (proof.sql_status == "uploaded", "new_resource_proof_not_uploaded"),
        (proof.new_resource_id == context.get("new_resource_id"), "new_resource_proof_resource_id_mismatch"),
        (proof.paper_id == context.get("new_paper_id"), "new_resource_proof_paper_id_mismatch"),
        (proof.version == context.get("new_version"), "new_resource_proof_version_mismatch"),
        (proof.replaces_paper_id == context.get("old_paper_id"), "new_resource_proof_replaces_paper_id_mismatch"),
        (proof.replaces_version == context.get("old_version"), "new_resource_proof_replaces_version_mismatch"),
    )
    return tuple(reason for ok, reason in checks if not ok)


def _statement_context(statement_plan: ReplacementSqlStatementPlan) -> dict[str, object]:
    context: dict[str, object] = {}
    for statement in statement_plan.statements:
        params = dict(statement.params)
        if statement.statement_type == OLD_RESOURCE_KIND:
            context["new_paper_id"] = params.get("replaced_by_paper_id")
            context["new_version"] = params.get("replaced_by_version")
            context["old_resource_id"] = params.get("old_resource_id")
        if statement.statement_type == NEW_REFERENCE_KIND:
            context["new_resource_id"] = params.get("new_resource_id")
            context["old_resource_id"] = params.get("replacement_of_resource_id")
            context["old_paper_id"] = params.get("replaces_paper_id")
            context["old_version"] = params.get("replaces_version")
            context["replacement_reason"] = params.get("replacement_reason")
    return context


def _statement_receipt(
    *,
    index: int,
    statement: ReplacementSqlStatement,
    transaction_status: str,
    actual_rowcount: int | None,
    failure_reason: str | None,
) -> ReplacementRemoteStatementReceipt:
    if not statement.executable:
        return ReplacementRemoteStatementReceipt(
            statement_index=index,
            statement_kind=statement.statement_type,
            executable=False,
            table_name=statement.table_name,
            status="skipped_reference",
            expected_rowcount=0,
            actual_rowcount=0,
            rowcount_policy="must_not_run",
            must_not_execute=True,
            resource_id=_resource_id(statement),
            params_hash=_params_hash(statement),
            sql_text_hash=_text_hash(statement.sql),
        )

    expected_rowcount = 1 if statement.statement_type == OLD_RESOURCE_KIND else None
    expected_min_rowcount = 1 if statement.statement_type == OLD_INDEX_KIND else None
    rowcount = _default_rowcount(
        transaction_status=transaction_status,
        expected_rowcount=expected_rowcount,
        expected_min_rowcount=expected_min_rowcount,
        actual_rowcount=actual_rowcount,
    )
    status = _statement_status(
        transaction_status=transaction_status,
        expected_rowcount=expected_rowcount,
        expected_min_rowcount=expected_min_rowcount,
        actual_rowcount=rowcount,
        failure_reason=failure_reason,
    )
    return ReplacementRemoteStatementReceipt(
        statement_index=index,
        statement_kind=statement.statement_type,
        executable=True,
        table_name=statement.table_name,
        status=status,
        expected_rowcount=expected_rowcount,
        expected_min_rowcount=expected_min_rowcount,
        actual_rowcount=rowcount,
        rowcount_policy="exactly_one" if expected_rowcount is not None else "at_least_one",
        resource_id=_resource_id(statement),
        params_hash=_params_hash(statement),
        sql_text_hash=_text_hash(statement.sql),
        error_message=failure_reason if status == "failed" else None,
    )


def _default_rowcount(
    *,
    transaction_status: str,
    expected_rowcount: int | None,
    expected_min_rowcount: int | None,
    actual_rowcount: int | None,
) -> int | None:
    if actual_rowcount is not None:
        return actual_rowcount
    if transaction_status == "remote_committed":
        return expected_rowcount if expected_rowcount is not None else expected_min_rowcount
    return actual_rowcount


def _statement_status(
    *,
    transaction_status: str,
    expected_rowcount: int | None,
    expected_min_rowcount: int | None,
    actual_rowcount: int | None,
    failure_reason: str | None,
) -> str:
    if failure_reason is not None and transaction_status == "remote_rejected":
        return "failed"
    if actual_rowcount is not None:
        if expected_rowcount is not None and actual_rowcount != expected_rowcount:
            return "rowcount_mismatch"
        if expected_min_rowcount is not None and actual_rowcount < expected_min_rowcount:
            return "rowcount_mismatch"
    if transaction_status == "remote_committed":
        return "applied"
    if transaction_status == "remote_commit_unknown":
        return "planned"
    return "planned"


def _resource_id(statement: ReplacementSqlStatement) -> str | None:
    params = dict(statement.params)
    for key in ("old_resource_id", "resource_id", "replacement_of_resource_id", "new_resource_id"):
        value = params.get(key)
        if value is not None:
            return str(value)
    return None


def _params_hash(statement: ReplacementSqlStatement) -> str:
    encoded = repr(sorted(dict(statement.params).items()))
    return _text_hash(encoded)


def _text_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _receipt_hash(
    *,
    transaction_status: str,
    operation_id: str | None,
    statement_receipts: tuple[ReplacementRemoteStatementReceipt, ...],
) -> str:
    payload = repr(
        (
            transaction_status,
            operation_id,
            tuple((item.statement_index, item.statement_kind, item.status) for item in statement_receipts),
        ),
    )
    return _text_hash(payload)
