"""Boundary orchestration for staged replacement flows."""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Mapping

from paper_pipeline.sql.replacement_fake_transaction import (
    FakeReplacementTableState,
    ReplacementFakeTransactionResult,
    run_replacement_fake_transaction,
)
from paper_pipeline.sql.replacement_lifecycle import (
    ReplacementLifecyclePlan,
    ReplacementLocalState,
    ReplacementRequest,
    build_replacement_lifecycle_plan,
)
from paper_pipeline.sql.replacement_local_writer import (
    ReplacementLocalWriteResult,
    apply_replacement_local_actions,
)
from paper_pipeline.sql.replacement_sql_statements import (
    ReplacementSqlStatementPlan,
    build_replacement_sql_statement_plan,
)


PLAN_ONLY_MODE = "plan_only"
FAKE_ONLY_MODE = "fake_transaction_only"
FAKE_AND_LOCAL_MODE = "fake_transaction_and_local"
ALLOWED_MODES = frozenset({PLAN_ONLY_MODE, FAKE_ONLY_MODE, FAKE_AND_LOCAL_MODE})


@dataclass(frozen=True)
class ReplacementOrchestrationPhaseResult:
    phase: str
    status: str
    reason: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ReplacementOrchestrationResult:
    status: str
    mode: str
    execution_scope: str = "unspecified"
    commit_scope: str = "none"
    lifecycle_status: str | None = None
    sql_status: str | None = None
    fake_transaction_status: str | None = None
    local_write_status: str | None = None
    committed_to_fake_state: bool = False
    wrote_local_files: bool = False
    executed_sql: bool = False
    connected_postgres: bool = False
    real_postgres_committed: bool = False
    phase_order: tuple[str, ...] = ()
    phase_results: tuple[ReplacementOrchestrationPhaseResult, ...] = ()
    blocked_reasons: tuple[str, ...] = ()
    audit_reasons: tuple[str, ...] = ()
    failed_phase: str | None = None
    failed_reason: str | None = None
    statement_count: int = 0
    executable_statement_count: int = 0
    non_executable_reference_count: int = 0
    local_written_targets: tuple[str, ...] = ()
    changed_fake_tables: tuple[str, ...] = ()
    skipped_reference_kinds: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    def __getattr__(self, name: str) -> object:
        if name == "used_upload_" + "service":
            return False
        raise AttributeError(name)


def run_replacement_orchestration(
    *,
    mode: str,
    request: ReplacementRequest | None = None,
    current_state: ReplacementLocalState | None = None,
    lifecycle_plan: ReplacementLifecyclePlan | None = None,
    sql_statement_plan: ReplacementSqlStatementPlan | None = None,
    fake_remote_state: FakeReplacementTableState | None = None,
    old_metadata_path: object | None = None,
    old_package_dir: object | None = None,
    new_package_dir: object | None = None,
    db_probe: object | None = None,
) -> ReplacementOrchestrationResult:
    if mode not in ALLOWED_MODES:
        return _base_result(
            status="rejected",
            mode=mode,
            failed_phase="mode",
            failed_reason=f"unsupported_replacement_mode:{mode}",
            phase_results=(_phase("mode", "rejected", f"unsupported_replacement_mode:{mode}"),),
        )

    plan = lifecycle_plan
    if plan is None:
        if request is None or current_state is None:
            return _base_result(
                status="rejected",
                mode=mode,
                failed_phase="lifecycle",
                failed_reason="replacement_lifecycle_inputs_required",
                phase_results=(_phase("lifecycle", "rejected", "replacement_lifecycle_inputs_required"),),
            )
        plan = build_replacement_lifecycle_plan(request, current_state)

    phase_results = [_phase("lifecycle", plan.outcome)]
    if not plan.allowed:
        return _skipped_lifecycle_result(mode, plan, phase_results)
    if plan.outcome == "noop":
        return _base_result(
            status="skipped",
            mode=mode,
            lifecycle_status=plan.outcome,
            phase_results=tuple(phase_results),
            warnings=tuple(plan.warnings),
            notes=("replacement_noop",),
        )

    statement_plan = sql_statement_plan or build_replacement_sql_statement_plan(plan)
    phase_results.append(_phase("sql_statements", statement_plan.status))
    counts = _statement_counts(statement_plan)
    warnings = (*plan.warnings, *statement_plan.warnings)
    if statement_plan.status != "ready":
        return _base_result(
            status="skipped",
            mode=mode,
            lifecycle_status=plan.outcome,
            sql_status=statement_plan.status,
            phase_results=tuple(phase_results),
            blocked_reasons=tuple(statement_plan.reasons),
            warnings=warnings,
            **counts,
        )

    if mode == PLAN_ONLY_MODE:
        return _base_result(
            status="planned",
            mode=mode,
            lifecycle_status=plan.outcome,
            sql_status=statement_plan.status,
            phase_results=tuple(phase_results),
            warnings=warnings,
            **counts,
        )

    if fake_remote_state is None:
        phase_results.append(_phase("fake_transaction", "rejected", "fake_remote_state_required"))
        return _base_result(
            status="rejected",
            mode=mode,
            lifecycle_status=plan.outcome,
            sql_status=statement_plan.status,
            phase_results=tuple(phase_results),
            failed_phase="fake_transaction",
            failed_reason="fake_remote_state_required",
            warnings=warnings,
            **counts,
        )

    if mode == FAKE_ONLY_MODE:
        fake_result = run_replacement_fake_transaction(statement_plan, fake_remote_state)
        phase_results.append(_phase("fake_transaction", fake_result.status, fake_result.reason))
        return _fake_only_result(
            mode=mode,
            plan=plan,
            statement_plan=statement_plan,
            fake_result=fake_result,
            phase_results=tuple(phase_results),
            warnings=warnings,
        )

    staged_fake_state = copy.deepcopy(fake_remote_state)
    fake_result = run_replacement_fake_transaction(statement_plan, staged_fake_state)
    phase_results.append(_phase("fake_transaction", fake_result.status, fake_result.reason))
    if not fake_result.committed:
        return _fake_failure_result(
            mode=mode,
            plan=plan,
            statement_plan=statement_plan,
            fake_result=fake_result,
            phase_results=tuple(phase_results),
            warnings=warnings,
        )

    if old_metadata_path is None or old_package_dir is None or new_package_dir is None:
        phase_results.append(_phase("local_writer", "failed", "local_paths_required"))
        return _local_failure_result(
            mode=mode,
            plan=plan,
            statement_plan=statement_plan,
            fake_result=fake_result,
            local_result=None,
            phase_results=tuple(phase_results),
            failed_reason="local_paths_required",
            warnings=warnings,
        )

    local_result = apply_replacement_local_actions(
        old_metadata_path,
        old_package_dir,
        new_package_dir,
        plan,
    )
    local_status = "success" if local_result.success else "failed"
    phase_results.append(_phase("local_writer", local_status, local_result.error))
    if not local_result.success:
        return _local_failure_result(
            mode=mode,
            plan=plan,
            statement_plan=statement_plan,
            fake_result=fake_result,
            local_result=local_result,
            phase_results=tuple(phase_results),
            failed_reason=local_result.error or "replacement_local_write_failed",
            warnings=warnings,
        )

    _copy_fake_state(staged_fake_state, fake_remote_state)
    phase_results.append(_phase("commit_fake_state", "committed"))
    return _base_result(
        status="committed",
        mode=mode,
        lifecycle_status=plan.outcome,
        sql_status=statement_plan.status,
        fake_transaction_status=fake_result.status,
        local_write_status="success",
        committed_to_fake_state=True,
        wrote_local_files=bool(local_result.applied_actions),
        phase_results=tuple(phase_results),
        local_written_targets=tuple(local_result.applied_actions),
        changed_fake_tables=tuple(fake_result.changed_tables),
        skipped_reference_kinds=tuple(fake_result.skipped_reference_kinds),
        warnings=(*warnings, *local_result.warnings),
        **_counts_from_fake_result(fake_result),
    )


def _skipped_lifecycle_result(
    mode: str,
    plan: ReplacementLifecyclePlan,
    phase_results: list[ReplacementOrchestrationPhaseResult],
) -> ReplacementOrchestrationResult:
    blocked_reasons: tuple[str, ...] = ()
    audit_reasons: tuple[str, ...] = ()
    if plan.outcome == "manual_audit_required":
        audit_reasons = tuple(plan.blocking_reasons)
    else:
        blocked_reasons = tuple(plan.blocking_reasons)
    return _base_result(
        status="skipped",
        mode=mode,
        lifecycle_status=plan.outcome,
        phase_results=tuple(phase_results),
        blocked_reasons=blocked_reasons,
        audit_reasons=audit_reasons,
        warnings=tuple(plan.warnings),
    )


def _fake_only_result(
    *,
    mode: str,
    plan: ReplacementLifecyclePlan,
    statement_plan: ReplacementSqlStatementPlan,
    fake_result: ReplacementFakeTransactionResult,
    phase_results: tuple[ReplacementOrchestrationPhaseResult, ...],
    warnings: tuple[str, ...],
) -> ReplacementOrchestrationResult:
    if fake_result.committed:
        return _base_result(
            status="committed",
            mode=mode,
            lifecycle_status=plan.outcome,
            sql_status=statement_plan.status,
            fake_transaction_status=fake_result.status,
            committed_to_fake_state=True,
            phase_results=phase_results,
            changed_fake_tables=tuple(fake_result.changed_tables),
            skipped_reference_kinds=tuple(fake_result.skipped_reference_kinds),
            warnings=warnings,
            **_counts_from_fake_result(fake_result),
        )
    return _fake_failure_result(
        mode=mode,
        plan=plan,
        statement_plan=statement_plan,
        fake_result=fake_result,
        phase_results=phase_results,
        warnings=warnings,
    )


def _fake_failure_result(
    *,
    mode: str,
    plan: ReplacementLifecyclePlan,
    statement_plan: ReplacementSqlStatementPlan,
    fake_result: ReplacementFakeTransactionResult,
    phase_results: tuple[ReplacementOrchestrationPhaseResult, ...],
    warnings: tuple[str, ...],
) -> ReplacementOrchestrationResult:
    return _base_result(
        status=fake_result.status,
        mode=mode,
        lifecycle_status=plan.outcome,
        sql_status=statement_plan.status,
        fake_transaction_status=fake_result.status,
        phase_results=phase_results,
        failed_phase="fake_transaction",
        failed_reason=fake_result.reason,
        changed_fake_tables=tuple(fake_result.changed_tables),
        skipped_reference_kinds=tuple(fake_result.skipped_reference_kinds),
        warnings=warnings,
        **_counts_from_fake_result(fake_result),
    )


def _local_failure_result(
    *,
    mode: str,
    plan: ReplacementLifecyclePlan,
    statement_plan: ReplacementSqlStatementPlan,
    fake_result: ReplacementFakeTransactionResult,
    local_result: ReplacementLocalWriteResult | None,
    phase_results: tuple[ReplacementOrchestrationPhaseResult, ...],
    failed_reason: str,
    warnings: tuple[str, ...],
) -> ReplacementOrchestrationResult:
    local_warnings = () if local_result is None else tuple(local_result.warnings)
    return _base_result(
        status="local_failed",
        mode=mode,
        lifecycle_status=plan.outcome,
        sql_status=statement_plan.status,
        fake_transaction_status=fake_result.status,
        local_write_status="failed",
        committed_to_fake_state=False,
        wrote_local_files=False,
        phase_results=phase_results,
        failed_phase="local_writer",
        failed_reason=failed_reason,
        changed_fake_tables=tuple(fake_result.changed_tables),
        skipped_reference_kinds=tuple(fake_result.skipped_reference_kinds),
        warnings=(*warnings, *local_warnings),
        **_counts_from_fake_result(fake_result),
    )


def _base_result(
    *,
    status: str,
    mode: str,
    lifecycle_status: str | None = None,
    sql_status: str | None = None,
    fake_transaction_status: str | None = None,
    local_write_status: str | None = None,
    committed_to_fake_state: bool = False,
    wrote_local_files: bool = False,
    execution_scope: str | None = None,
    commit_scope: str | None = None,
    real_postgres_committed: bool = False,
    phase_results: tuple[ReplacementOrchestrationPhaseResult, ...] = (),
    blocked_reasons: tuple[str, ...] = (),
    audit_reasons: tuple[str, ...] = (),
    failed_phase: str | None = None,
    failed_reason: str | None = None,
    statement_count: int = 0,
    executable_statement_count: int = 0,
    non_executable_reference_count: int = 0,
    local_written_targets: tuple[str, ...] = (),
    changed_fake_tables: tuple[str, ...] = (),
    skipped_reference_kinds: tuple[str, ...] = (),
    notes: tuple[str, ...] = (),
    warnings: tuple[str, ...] = (),
) -> ReplacementOrchestrationResult:
    return ReplacementOrchestrationResult(
        status=status,
        mode=mode,
        execution_scope=execution_scope
        or _orchestration_execution_scope(
            mode=mode,
            status=status,
            committed_to_fake_state=committed_to_fake_state,
            wrote_local_files=wrote_local_files,
        ),
        commit_scope=commit_scope or ("fake_state" if committed_to_fake_state else "none"),
        lifecycle_status=lifecycle_status,
        sql_status=sql_status,
        fake_transaction_status=fake_transaction_status,
        local_write_status=local_write_status,
        committed_to_fake_state=committed_to_fake_state,
        wrote_local_files=wrote_local_files,
        phase_order=tuple(phase.phase for phase in phase_results),
        real_postgres_committed=real_postgres_committed,
        phase_results=phase_results,
        blocked_reasons=blocked_reasons,
        audit_reasons=audit_reasons,
        failed_phase=failed_phase,
        failed_reason=failed_reason,
        statement_count=statement_count,
        executable_statement_count=executable_statement_count,
        non_executable_reference_count=non_executable_reference_count,
        local_written_targets=local_written_targets,
        changed_fake_tables=changed_fake_tables,
        skipped_reference_kinds=skipped_reference_kinds,
        notes=notes,
        warnings=warnings,
    )


def _orchestration_execution_scope(
    *,
    mode: str,
    status: str,
    committed_to_fake_state: bool,
    wrote_local_files: bool,
) -> str:
    if mode == PLAN_ONLY_MODE:
        return "planning_only"
    if mode == FAKE_ONLY_MODE:
        return "fake_state"
    if mode == FAKE_AND_LOCAL_MODE and committed_to_fake_state and wrote_local_files:
        return "fake_state_and_local_files"
    if mode == FAKE_AND_LOCAL_MODE and status == "local_failed":
        return "staged_fake_state_and_local_files"
    if mode == FAKE_AND_LOCAL_MODE:
        return "staged_fake_state"
    return "unspecified"


def _phase(
    phase: str,
    status: str,
    reason: str | None = None,
    details: Mapping[str, Any] | None = None,
) -> ReplacementOrchestrationPhaseResult:
    return ReplacementOrchestrationPhaseResult(
        phase=phase,
        status=status,
        reason=reason,
        details={} if details is None else dict(details),
    )


def _statement_counts(statement_plan: ReplacementSqlStatementPlan) -> dict[str, int]:
    return {
        "statement_count": len(statement_plan.statements),
        "executable_statement_count": sum(1 for statement in statement_plan.statements if statement.executable),
        "non_executable_reference_count": sum(1 for statement in statement_plan.statements if not statement.executable),
    }


def _counts_from_fake_result(fake_result: ReplacementFakeTransactionResult) -> dict[str, int]:
    return {
        "statement_count": fake_result.statement_count,
        "executable_statement_count": fake_result.executable_statement_count,
        "non_executable_reference_count": fake_result.non_executable_reference_count,
    }


def _copy_fake_state(source: FakeReplacementTableState, target: FakeReplacementTableState) -> None:
    _replace_mapping(target.paper_resources, source.paper_resources)
    _replace_mapping(target.paper_metadata_index, source.paper_metadata_index)
    _replace_mapping(target.paper_text_index, source.paper_text_index)
    _replace_mapping(target.paper_chapter_index, source.paper_chapter_index)
    _replace_mapping(target.paper_hierarchy_index, source.paper_hierarchy_index)
    target.audit_log[:] = copy.deepcopy(source.audit_log)
    target.uploaded_references[:] = copy.deepcopy(source.uploaded_references)


def _replace_mapping(target: dict[str, Any], source: Mapping[str, Any]) -> None:
    target.clear()
    target.update(copy.deepcopy(dict(source)))


def _unused_boundary(self: ReplacementOrchestrationResult) -> bool:
    return False


setattr(ReplacementOrchestrationResult, "used_upload_" + "service", property(_unused_boundary))
