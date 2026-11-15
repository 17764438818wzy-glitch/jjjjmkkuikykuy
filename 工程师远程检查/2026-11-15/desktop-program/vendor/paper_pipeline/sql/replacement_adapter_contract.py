"""Contract objects for a future replacement remote adapter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol

from paper_pipeline.sql.replacement_sql_statements import (
    ReplacementSqlStatement,
    ReplacementSqlStatementPlan,
)


READY_STATUS = "ready"
TRANSACTION_KIND = "single_resource_replacement"
OLD_RESOURCE_KIND = "replacement_old_resource_update"
OLD_INDEX_KIND = "replacement_old_index_status_update"
NEW_REFERENCE_KIND = "replacement_new_resource_upload_reference"
ALLOWED_EXECUTABLE_KINDS = (OLD_RESOURCE_KIND, OLD_INDEX_KIND)
ALLOWED_REFERENCE_KINDS = (NEW_REFERENCE_KIND,)
REMOTE_TRANSACTION_ORDER = (
    "begin_remote_transaction",
    "run_executable_statements",
    "skip_non_executable_references",
    "commit_remote_transaction",
    "allow_local_writer",
)
FORBIDDEN_SQL_FRAGMENTS = (
    "DELETE FROM",
    "DROP TABLE",
    "TRUNCATE",
    "ALTER TABLE",
    "CREATE TABLE",
)


@dataclass(frozen=True)
class ReplacementRemoteTransactionRequest:
    statement_plan: ReplacementSqlStatementPlan
    adapter_scope: str = "contract_only"
    transaction_scope: str = "future_remote_postgres"
    operator: str | None = None
    requested_at: str | None = None
    execution_kind: str = "real"
    real_execution_confirmed: bool = True
    requires_transaction: bool = True
    transaction_kind: str = TRANSACTION_KIND
    expected_executable_statement_kinds: tuple[str, ...] = ALLOWED_EXECUTABLE_KINDS
    expected_non_executable_reference_kinds: tuple[str, ...] = ALLOWED_REFERENCE_KINDS
    executable_statement_kinds: tuple[str, ...] = ()
    non_executable_reference_kinds: tuple[str, ...] = ()
    local_write_policy: str = "after_remote_commit"
    post_commit_local_write_required: bool = True
    rollback_policy: str = "remote_transaction_only"
    local_write_failure_policy: str = "reconciliation_required_after_remote_commit"
    replacement_operation_id: str | None = None
    idempotency_key: str | None = None
    remote_transaction_order: tuple[str, ...] = REMOTE_TRANSACTION_ORDER


@dataclass(frozen=True)
class ReplacementRemoteTransactionContractResult:
    status: str
    ready_for_real_adapter: bool
    execution_scope: str = "contract_only"
    commit_scope: str = "none"
    describes_contract_only: bool = True
    connected_postgres: bool = False
    executed_sql: bool = False
    executed_sql_text: bool = False
    real_postgres_committed: bool = False
    requires_transaction: bool = False
    transaction_kind: str | None = None
    statement_count: int = 0
    executable_statement_count: int = 0
    non_executable_reference_count: int = 0
    allowed_statement_kinds: tuple[str, ...] = ALLOWED_EXECUTABLE_KINDS
    expected_non_executable_reference_kinds: tuple[str, ...] = ALLOWED_REFERENCE_KINDS
    rejected_statement_kinds: tuple[str, ...] = ()
    forbidden_sql_detected: tuple[str, ...] = ()
    parameterized_sql: bool = True
    local_write_after_remote_commit: bool = False
    local_writer_required: bool = False
    reconciliation_required_possible: bool = False
    warnings: tuple[str, ...] = ()
    blocked_reasons: tuple[str, ...] = ()
    audit_reasons: tuple[str, ...] = ()
    remote_transaction_request: ReplacementRemoteTransactionRequest | None = None


@dataclass(frozen=True)
class ReplacementAdapterContractSummary:
    connected_postgres: bool = False
    executed_sql: bool = False
    real_adapter_implemented: bool = False
    non_executable_reference_policy: str = "record_reference_only"
    local_write_policy: str = "after_remote_commit"
    rollback_policy: str = "remote_transaction_only"
    local_write_failure_policy: str = "reconciliation_required_after_remote_commit"


class ReplacementRealAdapterContractError(ValueError):
    """Raised only for contract validation misuse."""


class ReplacementAdapterContract(Protocol):
    def validate_statement_plan(
        self,
        statement_plan: ReplacementSqlStatementPlan,
    ) -> ReplacementRemoteTransactionContractResult: ...

    def describe_required_transaction(self) -> Mapping[str, Any]: ...

    def build_execution_request(
        self,
        statement_plan: ReplacementSqlStatementPlan,
    ) -> ReplacementRemoteTransactionRequest: ...

    def classify_statement(self, statement: ReplacementSqlStatement) -> str: ...

    def summarize_contract_requirements(self) -> ReplacementAdapterContractSummary: ...


def validate_replacement_adapter_contract(
    statement_plan: ReplacementSqlStatementPlan,
    *,
    operator: str | None = None,
    requested_at: str | None = None,
    execution_kind: str = "real",
    real_execution_confirmed: bool = True,
    replacement_operation_id: str | None = None,
    idempotency_key: str | None = None,
    db_probe: object | None = None,
) -> ReplacementRemoteTransactionContractResult:
    counts = _statement_counts(statement_plan)
    if statement_plan.status != READY_STATUS:
        return _non_ready_result(statement_plan, counts)

    transaction_rejection = _transaction_header_rejection(statement_plan)
    if transaction_rejection is not None:
        return _blocked_result(
            statement_plan,
            counts,
            status="rejected",
            blocked_reasons=(transaction_rejection,),
        )

    forbidden_sql = _forbidden_sql_fragments(statement_plan)
    rejected_kinds = _rejected_statement_kinds(statement_plan)
    parameterized = _statements_are_parameterized(statement_plan)
    if forbidden_sql or rejected_kinds or not parameterized:
        return _blocked_result(
            statement_plan,
            counts,
            status="rejected",
            rejected_statement_kinds=rejected_kinds,
            forbidden_sql_detected=forbidden_sql,
            parameterized_sql=parameterized,
        )

    structure_rejection = _transaction_structure_rejection(statement_plan)
    if structure_rejection is not None:
        return _blocked_result(
            statement_plan,
            counts,
            status="rejected",
            blocked_reasons=(structure_rejection,),
        )

    execution_reasons = _execution_reasons(
        execution_kind=execution_kind,
        real_execution_confirmed=real_execution_confirmed,
    )
    if execution_reasons:
        return _blocked_result(
            statement_plan,
            counts,
            status="blocked",
            blocked_reasons=execution_reasons,
        )

    request = build_replacement_remote_transaction_request(
        statement_plan,
        operator=operator,
        requested_at=requested_at,
        execution_kind=execution_kind,
        real_execution_confirmed=real_execution_confirmed,
        replacement_operation_id=replacement_operation_id,
        idempotency_key=idempotency_key,
    )
    return ReplacementRemoteTransactionContractResult(
        status="ready",
        ready_for_real_adapter=True,
        requires_transaction=statement_plan.requires_transaction,
        transaction_kind=statement_plan.transaction_kind,
        statement_count=counts["statement_count"],
        executable_statement_count=counts["executable_statement_count"],
        non_executable_reference_count=counts["non_executable_reference_count"],
        parameterized_sql=True,
        local_write_after_remote_commit=True,
        local_writer_required=True,
        reconciliation_required_possible=True,
        warnings=tuple(statement_plan.warnings),
        remote_transaction_request=request,
    )


def validate_statement_plan(
    statement_plan: ReplacementSqlStatementPlan,
    **options: Any,
) -> ReplacementRemoteTransactionContractResult:
    return validate_replacement_adapter_contract(statement_plan, **options)


def describe_required_transaction() -> dict[str, Any]:
    return {
        "requires_transaction": True,
        "transaction_kind": TRANSACTION_KIND,
        "executable_statement_kinds": ALLOWED_EXECUTABLE_KINDS,
        "non_executable_reference_kinds": ALLOWED_REFERENCE_KINDS,
        "remote_transaction_order": REMOTE_TRANSACTION_ORDER,
        "rollback_policy": "remote_transaction_only",
        "local_write_failure_policy": "reconciliation_required_after_remote_commit",
    }


def build_execution_request(
    statement_plan: ReplacementSqlStatementPlan,
    **options: Any,
) -> ReplacementRemoteTransactionRequest:
    return build_replacement_remote_transaction_request(statement_plan, **options)


def build_replacement_remote_transaction_request(
    statement_plan: ReplacementSqlStatementPlan,
    *,
    operator: str | None = None,
    requested_at: str | None = None,
    execution_kind: str = "real",
    real_execution_confirmed: bool = True,
    replacement_operation_id: str | None = None,
    idempotency_key: str | None = None,
) -> ReplacementRemoteTransactionRequest:
    return ReplacementRemoteTransactionRequest(
        statement_plan=statement_plan,
        operator=operator,
        requested_at=requested_at,
        execution_kind=execution_kind,
        real_execution_confirmed=real_execution_confirmed,
        requires_transaction=statement_plan.requires_transaction,
        transaction_kind=statement_plan.transaction_kind or TRANSACTION_KIND,
        executable_statement_kinds=tuple(
            statement.statement_type for statement in statement_plan.statements if statement.executable
        ),
        non_executable_reference_kinds=tuple(
            statement.statement_type for statement in statement_plan.statements if not statement.executable
        ),
        replacement_operation_id=replacement_operation_id,
        idempotency_key=idempotency_key,
    )


def classify_replacement_statement(statement: ReplacementSqlStatement) -> str:
    if statement.executable and statement.statement_type in ALLOWED_EXECUTABLE_KINDS:
        return "executable"
    if not statement.executable and statement.statement_type in ALLOWED_REFERENCE_KINDS:
        return "non_executable_reference"
    return "rejected"


def classify_statement(statement: ReplacementSqlStatement) -> str:
    return classify_replacement_statement(statement)


def summarize_contract_requirements() -> ReplacementAdapterContractSummary:
    return ReplacementAdapterContractSummary()


def _non_ready_result(
    statement_plan: ReplacementSqlStatementPlan,
    counts: Mapping[str, int],
) -> ReplacementRemoteTransactionContractResult:
    blocked_reasons: tuple[str, ...] = ()
    audit_reasons: tuple[str, ...] = ()
    if statement_plan.status in {"manual_audit", "conflict"}:
        audit_reasons = tuple(statement_plan.reasons)
    elif statement_plan.status != "noop":
        blocked_reasons = tuple(statement_plan.reasons)
    return ReplacementRemoteTransactionContractResult(
        status=statement_plan.status,
        ready_for_real_adapter=False,
        requires_transaction=statement_plan.requires_transaction,
        transaction_kind=statement_plan.transaction_kind,
        statement_count=counts["statement_count"],
        executable_statement_count=counts["executable_statement_count"],
        non_executable_reference_count=counts["non_executable_reference_count"],
        warnings=tuple(statement_plan.warnings),
        blocked_reasons=blocked_reasons,
        audit_reasons=audit_reasons,
    )


def _blocked_result(
    statement_plan: ReplacementSqlStatementPlan,
    counts: Mapping[str, int],
    *,
    status: str,
    blocked_reasons: tuple[str, ...] = (),
    rejected_statement_kinds: tuple[str, ...] = (),
    forbidden_sql_detected: tuple[str, ...] = (),
    parameterized_sql: bool = True,
) -> ReplacementRemoteTransactionContractResult:
    return ReplacementRemoteTransactionContractResult(
        status=status,
        ready_for_real_adapter=False,
        requires_transaction=statement_plan.requires_transaction,
        transaction_kind=statement_plan.transaction_kind,
        statement_count=counts["statement_count"],
        executable_statement_count=counts["executable_statement_count"],
        non_executable_reference_count=counts["non_executable_reference_count"],
        warnings=tuple(statement_plan.warnings),
        blocked_reasons=blocked_reasons,
        rejected_statement_kinds=rejected_statement_kinds,
        forbidden_sql_detected=forbidden_sql_detected,
        parameterized_sql=parameterized_sql,
    )


def _statement_counts(statement_plan: ReplacementSqlStatementPlan) -> dict[str, int]:
    return {
        "statement_count": len(statement_plan.statements),
        "executable_statement_count": sum(1 for statement in statement_plan.statements if statement.executable),
        "non_executable_reference_count": sum(1 for statement in statement_plan.statements if not statement.executable),
    }


def _transaction_header_rejection(statement_plan: ReplacementSqlStatementPlan) -> str | None:
    if statement_plan.requires_transaction is not True:
        return "replacement_transaction_required"
    if statement_plan.transaction_kind != TRANSACTION_KIND:
        return "unsupported_transaction_kind"
    return None


def _transaction_structure_rejection(statement_plan: ReplacementSqlStatementPlan) -> str | None:
    if not any(
        statement.executable and statement.statement_type == OLD_RESOURCE_KIND
        for statement in statement_plan.statements
    ):
        return "old_resource_update_missing"
    return None


def _forbidden_sql_fragments(statement_plan: ReplacementSqlStatementPlan) -> tuple[str, ...]:
    found: list[str] = []
    for statement in statement_plan.statements:
        sql_text = statement.sql.upper()
        for fragment in FORBIDDEN_SQL_FRAGMENTS:
            if fragment in sql_text and fragment not in found:
                found.append(fragment)
    return tuple(found)


def _rejected_statement_kinds(statement_plan: ReplacementSqlStatementPlan) -> tuple[str, ...]:
    rejected: list[str] = []
    for statement in statement_plan.statements:
        if classify_replacement_statement(statement) == "rejected" and statement.statement_type not in rejected:
            rejected.append(statement.statement_type)
    return tuple(rejected)


def _statements_are_parameterized(statement_plan: ReplacementSqlStatementPlan) -> bool:
    for statement in statement_plan.statements:
        if not statement.executable:
            continue
        sql_text = statement.sql
        for value in statement.params.values():
            if _unsafe_param_value_is_in_sql(value, sql_text):
                return False
    return True


def _unsafe_param_value_is_in_sql(value: object, sql_text: str) -> bool:
    if value is None:
        return False
    if not isinstance(value, str):
        return False
    text = value.strip()
    if len(text) < 4:
        return False
    if not any(marker in text for marker in ("'", ";", "--")) and not _forbidden_text(text):
        return False
    return text in sql_text


def _forbidden_text(value: str) -> bool:
    upper_value = value.upper()
    return any(fragment in upper_value for fragment in FORBIDDEN_SQL_FRAGMENTS)


def _execution_reasons(
    *,
    execution_kind: str,
    real_execution_confirmed: bool,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if execution_kind != "real":
        reasons.append("replacement_execution_not_real")
    if real_execution_confirmed is not True:
        reasons.append("replacement_real_execution_not_confirmed")
    return tuple(reasons)
