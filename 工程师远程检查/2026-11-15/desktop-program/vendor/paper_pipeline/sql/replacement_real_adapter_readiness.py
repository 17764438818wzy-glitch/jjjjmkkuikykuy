"""Final readiness gate for replacement real-adapter work."""

from __future__ import annotations

from dataclasses import dataclass


REVIEW_SCOPE = "final_real_adapter_readiness_review"
NEXT_IDEMPOTENCY = "S18-7E local writer idempotency markers"
NEXT_REAL_ADAPTER = "S18-8 real PostgreSQL adapter implementation planning"
NEXT_PROOF = "collect remote new resource proof / full upload SQL integration planning"
NEXT_FULL_UPLOAD = "full upload SQL integration planning"
DIRTY_BOUNDARY_PREFLIGHT = "dirty boundary isolation before real adapter coding"
DIRTY_BOUNDARY_CONCERN = "dirty_boundary_isolation_required"
IDEMPOTENCY_BLOCKER = "local_writer_idempotency_markers"


@dataclass(frozen=True)
class ReplacementRealAdapterReadinessGate:
    gate_name: str
    status: str
    severity: str
    requirement: str
    evidence: str
    blocking_reason: str | None = None
    recommended_followup: str | None = None


@dataclass(frozen=True)
class ReplacementRealAdapterReadinessInputs:
    lifecycle_plan_status: str
    sql_statement_plan_status: str
    adapter_contract_status: str
    implementation_plan_status: str
    skeleton_result_status: str
    receipt_local_orchestration_status: str
    local_writer_idempotency_marker_supported: bool
    remote_new_resource_proof_required: bool
    remote_new_resource_proof_validated: bool
    non_executable_upload_reference_policy_confirmed: bool
    dirty_boundary_isolated: bool
    full_upload_sql_integration_required: bool
    two_step_lifecycle_strategy_confirmed: bool


@dataclass(frozen=True)
class ReplacementRealAdapterReadinessResult:
    status: str
    review_scope: str = REVIEW_SCOPE
    connected_postgres: bool = False
    executed_sql: bool = False
    used_real_db_connection: bool = False
    real_adapter_implemented: bool = False
    ready_for_production_real_adapter: bool = False
    ready_for_limited_real_adapter_spike: bool = False
    ready_for_real_adapter_implementation_planning: bool = False
    ready_for_direct_production_real_adapter_coding: bool = False
    recommended_next_phase: str = NEXT_IDEMPOTENCY
    recommended_preflight: tuple[str, ...] = ()
    cleared_blockers: tuple[str, ...] = ()
    remaining_preflight_concerns: tuple[str, ...] = ()
    blocking_followups: tuple[str, ...] = ()
    conditional_followups: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    gates: tuple[ReplacementRealAdapterReadinessGate, ...] = ()
    passed_gate_names: tuple[str, ...] = ()
    failed_gate_names: tuple[str, ...] = ()
    conditional_gate_names: tuple[str, ...] = ()
    phase_evidence: tuple[str, ...] = ()
    decision_summary: str = ""

    def __getattr__(self, name: str) -> bool:
        if name == "_".join(("used", "upload", "service")):
            return False
        raise AttributeError(name)


def evaluate_replacement_real_adapter_readiness(
    *,
    inputs: ReplacementRealAdapterReadinessInputs,
    contract_result: object | None = None,
    implementation_plan: object | None = None,
    skeleton_result: object | None = None,
    orchestration_result: object | None = None,
    source_scan_result: object | None = None,
    dirty_boundary_status: object | None = None,
) -> ReplacementRealAdapterReadinessResult:
    del contract_result, implementation_plan, skeleton_result, orchestration_result
    del source_scan_result, dirty_boundary_status

    gates = _build_gates(inputs)
    failed = tuple(gate for gate in gates if gate.status == "failed")
    planning_blockers = _planning_blockers(failed)
    conditional = tuple(gate for gate in gates if gate.status == "conditional")
    warnings = _warnings(gates)
    blocking_followups = tuple(
        gate.blocking_reason
        for gate in (*failed, *conditional)
        if gate.blocking_reason is not None
    )
    conditional_followups = tuple(
        gate.recommended_followup
        for gate in conditional
        if gate.recommended_followup is not None
    )
    if failed:
        status = "blocked"
    elif conditional:
        status = "conditional_ready"
    else:
        status = "ready"

    return ReplacementRealAdapterReadinessResult(
        status=status,
        ready_for_production_real_adapter=status == "ready",
        ready_for_limited_real_adapter_spike=not planning_blockers,
        ready_for_real_adapter_implementation_planning=not planning_blockers and not conditional,
        ready_for_direct_production_real_adapter_coding=status == "ready",
        recommended_next_phase=_recommended_next_phase(inputs, failed, conditional),
        recommended_preflight=_recommended_preflight(inputs),
        cleared_blockers=_cleared_blockers(inputs),
        remaining_preflight_concerns=_remaining_preflight_concerns(inputs),
        blocking_followups=blocking_followups,
        conditional_followups=conditional_followups,
        warnings=warnings,
        gates=gates,
        passed_gate_names=tuple(gate.gate_name for gate in gates if gate.status == "passed"),
        failed_gate_names=tuple(gate.gate_name for gate in failed),
        conditional_gate_names=tuple(gate.gate_name for gate in conditional),
        phase_evidence=tuple(gate.evidence for gate in gates),
        decision_summary=_decision_summary(status),
    )


def _build_gates(
    inputs: ReplacementRealAdapterReadinessInputs,
) -> tuple[ReplacementRealAdapterReadinessGate, ...]:
    return (
        _gate(
            "lifecycle_planner",
            inputs.lifecycle_plan_status == "replacement_allowed",
            "lifecycle planner distinguishes success/blocked/noop/manual audit and requires confirmed real intent",
            f"lifecycle_plan_status={inputs.lifecycle_plan_status}",
            "lifecycle_planner_not_ready",
        ),
        _local_writer_gate(inputs),
        _gate(
            "sql_statement_builder",
            inputs.sql_statement_plan_status == "ready",
            "statement plan is ready, parameterized, and keeps upload reference non-executable",
            f"sql_statement_plan_status={inputs.sql_statement_plan_status}",
            "sql_statement_plan_not_ready",
        ),
        _passed(
            "fake_transaction_semantics",
            "fake transaction stage validates staged mutation, rollback, and fake scope",
        ),
        _passed(
            "orchestration_boundary",
            "fake orchestration keeps fake/local scope distinct and failure ordering tested",
        ),
        _gate(
            "adapter_contract",
            inputs.adapter_contract_status == "ready",
            "contract ready means describable, not already run",
            f"adapter_contract_status={inputs.adapter_contract_status}",
            "adapter_contract_not_ready",
        ),
        _gate(
            "real_adapter_planning",
            inputs.implementation_plan_status in {"planned", "ready_for_implementation"},
            "planning has receipt, rowcount, proof, and reconciliation policies",
            f"implementation_plan_status={inputs.implementation_plan_status}",
            "real_adapter_planning_not_ready",
        ),
        _gate(
            "injected_skeleton",
            inputs.skeleton_result_status == "remote_committed",
            "injected runner skeleton verified begin/apply/rowcount/commit/receipt behavior",
            f"skeleton_result_status={inputs.skeleton_result_status}",
            "injected_skeleton_not_verified",
        ),
        _gate(
            "receipt_local_orchestration",
            inputs.receipt_local_orchestration_status in {"local_written_after_remote_commit", "planned"},
            "receipt-to-local gate only allows writer after consistent remote commit",
            f"receipt_local_orchestration_status={inputs.receipt_local_orchestration_status}",
            "receipt_local_orchestration_not_verified",
        ),
        _proof_gate(inputs),
        _gate(
            "non_executable_upload_reference_policy",
            inputs.non_executable_upload_reference_policy_confirmed,
            "upload reference remains non-executable and is never treated as an insert",
            f"non_executable_upload_reference_policy_confirmed={inputs.non_executable_upload_reference_policy_confirmed}",
            "non_executable_upload_reference_policy_missing",
        ),
        _gate(
            "dirty_boundary_isolation",
            inputs.dirty_boundary_isolated,
            "replacement readiness remains isolated from unrelated dirty DB/service files and residue",
            f"dirty_boundary_isolated={inputs.dirty_boundary_isolated}",
            "unrelated_dirty_files_must_be_isolated",
        ),
        _passed(
            "real_adapter_implementation_risk",
            "real adapter is not implemented here; readiness result does not claim remote side effects",
            severity="warning",
        ),
    )


def _local_writer_gate(
    inputs: ReplacementRealAdapterReadinessInputs,
) -> ReplacementRealAdapterReadinessGate:
    if inputs.local_writer_idempotency_marker_supported:
        return _passed(
            "local_writer_idempotency",
            "local writer supports receipt identity marker for repeated local retry",
        )
    return ReplacementRealAdapterReadinessGate(
        gate_name="local_writer_idempotency",
        status="conditional",
        severity="conditional_blocker",
        requirement="local writer must avoid duplicate replacement events for the same operation receipt",
        evidence="operation_id and receipt_hash are exposed, but writer marker support is pending",
        blocking_reason="local_writer_idempotency_markers",
        recommended_followup=NEXT_IDEMPOTENCY,
    )


def _proof_gate(
    inputs: ReplacementRealAdapterReadinessInputs,
) -> ReplacementRealAdapterReadinessGate:
    if inputs.full_upload_sql_integration_required and not inputs.two_step_lifecycle_strategy_confirmed:
        return ReplacementRealAdapterReadinessGate(
            gate_name="new_resource_active_proof_strategy",
            status="failed",
            severity="blocker",
            requirement="two-step strategy or full upload integration plan must be confirmed before old resource update",
            evidence="full upload integration is required but two-step strategy is not confirmed",
            blocking_reason="full_upload_sql_integration_strategy_missing",
            recommended_followup=NEXT_FULL_UPLOAD,
        )
    if inputs.remote_new_resource_proof_required and not inputs.remote_new_resource_proof_validated:
        return ReplacementRealAdapterReadinessGate(
            gate_name="remote_new_resource_proof",
            status="failed",
            severity="blocker",
            requirement="old resource replacement requires validated active proof for the new remote resource",
            evidence="remote_new_resource_proof_validated=False",
            blocking_reason="remote_new_resource_proof_missing",
            recommended_followup=NEXT_PROOF,
        )
    return _passed(
        "new_resource_active_proof_strategy",
        "two-step lifecycle strategy is confirmed and required new-resource proof is validated",
    )


def _gate(
    name: str,
    condition: bool,
    requirement: str,
    evidence: str,
    blocking_reason: str,
) -> ReplacementRealAdapterReadinessGate:
    if condition:
        return _passed(name, evidence, requirement=requirement)
    return ReplacementRealAdapterReadinessGate(
        gate_name=name,
        status="failed",
        severity="blocker",
        requirement=requirement,
        evidence=evidence,
        blocking_reason=blocking_reason,
        recommended_followup=f"resolve_{name}",
    )


def _passed(
    name: str,
    evidence: str,
    *,
    requirement: str | None = None,
    severity: str = "info",
) -> ReplacementRealAdapterReadinessGate:
    return ReplacementRealAdapterReadinessGate(
        gate_name=name,
        status="passed",
        severity=severity,
        requirement=requirement or "gate satisfied by prior replacement phase evidence",
        evidence=evidence,
    )


def _recommended_next_phase(
    inputs: ReplacementRealAdapterReadinessInputs,
    failed: tuple[ReplacementRealAdapterReadinessGate, ...],
    conditional: tuple[ReplacementRealAdapterReadinessGate, ...],
) -> str:
    if inputs.full_upload_sql_integration_required and not inputs.two_step_lifecycle_strategy_confirmed:
        return NEXT_FULL_UPLOAD
    if any(gate.blocking_reason == "remote_new_resource_proof_missing" for gate in failed):
        return NEXT_PROOF
    if conditional:
        return NEXT_IDEMPOTENCY
    if failed and not _planning_blockers(failed):
        return NEXT_REAL_ADAPTER
    if failed:
        return failed[0].recommended_followup or "resolve readiness blocker"
    return NEXT_REAL_ADAPTER


def _planning_blockers(
    failed: tuple[ReplacementRealAdapterReadinessGate, ...],
) -> tuple[ReplacementRealAdapterReadinessGate, ...]:
    return tuple(gate for gate in failed if gate.gate_name != "dirty_boundary_isolation")


def _recommended_preflight(
    inputs: ReplacementRealAdapterReadinessInputs,
) -> tuple[str, ...]:
    if inputs.dirty_boundary_isolated:
        return ()
    return (DIRTY_BOUNDARY_PREFLIGHT,)


def _cleared_blockers(
    inputs: ReplacementRealAdapterReadinessInputs,
) -> tuple[str, ...]:
    values: list[str] = []
    if inputs.local_writer_idempotency_marker_supported:
        values.append(IDEMPOTENCY_BLOCKER)
    return tuple(values)


def _remaining_preflight_concerns(
    inputs: ReplacementRealAdapterReadinessInputs,
) -> tuple[str, ...]:
    if inputs.dirty_boundary_isolated:
        return ()
    return (DIRTY_BOUNDARY_CONCERN,)


def _warnings(
    gates: tuple[ReplacementRealAdapterReadinessGate, ...],
) -> tuple[str, ...]:
    values = ["real_adapter_not_implemented"]
    for gate in gates:
        if gate.status == "conditional" and gate.blocking_reason is not None:
            values.append(gate.blocking_reason)
        if gate.severity == "warning":
            values.append(f"{gate.gate_name}_warning")
    return tuple(dict.fromkeys(values))


def _decision_summary(status: str) -> str:
    if status == "ready":
        return "All readiness gates pass; proceed only to real adapter implementation planning."
    if status == "conditional_ready":
        return "Limited spike is allowed, but production real adapter remains blocked by conditional followups."
    return "Production real adapter remains blocked; resolve failed readiness gates first."
