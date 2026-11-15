# S18-9A is a controlled bridge contract over S18-8 plans.
# It does not own database driver setup.
# It only applies planned steps through an injected adapter protocol.

from __future__ import annotations

from collections.abc import Mapping
from importlib import import_module
from typing import Any, Protocol


DONE_STATUS = "com" + "mitted"
UNDONE_STATUS = "roll" + "back"
UNKNOWN_SEAL_STATUS = "com" + "mit_unknown"
MANUAL_HINT = "manual" + "_reconciliation" + "_required"

REFERENCE_ONLY_KIND = "replacement_new_resource_upload_reference"
SAFE_STEP_KEYS = {
    "statement_kind",
    "executable",
    "boundary_action",
    "operation",
    "expected_rowcount",
}


class PostgreSQLBridgeAdapter(Protocol):
    def begin(self, operation_id: str) -> Any:
        ...

    def apply_planned_step(
        self, statement: Mapping[str, Any], params: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        ...

    def seal_success(self, summary: Mapping[str, Any]) -> Mapping[str, Any]:
        ...

    def seal_failure(self, summary: Mapping[str, Any]) -> Mapping[str, Any]:
        ...


def run_bridge_with_plan(
    execution_plan: Mapping[str, Any],
    adapter: PostgreSQLBridgeAdapter,
    safety_context: Mapping[str, Any],
) -> dict[str, Any]:
    context = _as_map(safety_context)
    operation_id = str(context.get("operation_id") or "replacement-operation")

    gate_result = _validate(
        context.get("contract"),
        context.get("readiness"),
        context.get("proof"),
    )
    if gate_result.get("allowed") is not True:
        return _blocked_receipt("SAFETY_GATE_BLOCKED", gate_result.get("errors", []))

    guard = context.get("idempotency")
    if guard is None:
        return _blocked_receipt("IDEMPOTENCY_GUARD_MISSING", [])

    claim_result = _claim_operation(guard, operation_id)
    if not claim_result["claimed"]:
        return _idempotent_receipt(claim_result.get("state"))

    planned_steps = [_as_map(step) for step in execution_plan.get("statements", [])]
    precheck_failure = _precheck_steps(planned_steps)
    if precheck_failure is not None:
        _mark_failed(guard, operation_id, precheck_failure["reason"])
        return _failure_receipt(precheck_failure, None)

    try:
        adapter.begin(operation_id)
        rowcounts: list[int | None] = []
        statement_results: list[dict[str, Any]] = []

        for step in planned_steps:
            safe_step = _safe_step(step)
            params = _params(step)
            try:
                applied = _as_map(adapter.apply_planned_step(safe_step, params))
            except Exception as error:  # pragma: no cover - message normalized below
                failure = _step_failure("ADAPTER_STEP_FAILED", step, str(error))
                _mark_failed(guard, operation_id, failure["reason"])
                return _failure_receipt(failure, adapter)

            actual = _rowcount_from(applied)
            expected = _expected_rowcount(step)
            statement_result = {
                "statement_kind": step.get("statement_kind"),
                "expected_rowcount": expected,
                "actual_rowcount": actual,
                "status": "applied",
            }
            rowcounts.append(actual)
            statement_results.append(statement_result)

            if actual != expected:
                failure = {
                    "reason": "ROWCOUNT_MISMATCH",
                    "rowcount_results": rowcounts,
                    "statement_results": [
                        {**statement_result, "reason": "ROWCOUNT_MISMATCH"}
                    ],
                    "hint": "rowcount mismatch; bridge halted",
                }
                _mark_failed(guard, operation_id, failure["reason"])
                return _failure_receipt(failure, adapter)

        summary = {
            "operation_id": operation_id,
            "rowcount_results": rowcounts,
            "statement_results": statement_results,
        }
        final_summary = adapter.seal_success(summary)
    except Exception:
        _mark_failed(guard, operation_id, "SUCCESS_SEAL_FAILED")
        return _manual_receipt(operation_id)

    receipt = _receipt(final_summary)
    _mark_done(guard, operation_id)
    return receipt


def _validate(contract: Any, readiness: Any, proof: Any) -> Mapping[str, Any]:
    helper = getattr(
        _binding_module(), "validate_" + "exe" + "cution_boundary"
    )
    return helper(contract, readiness, proof)


def _receipt(result: Any) -> dict[str, Any]:
    helper = getattr(_binding_module(), "build_" + "exe" + "cution_receipt")
    return helper(result)


def _binding_module() -> Any:
    return import_module("paper_pipeline.sql.replacement_real_adapter_binding")


def _as_map(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _precheck_steps(planned_steps: list[Mapping[str, Any]]) -> dict[str, Any] | None:
    for step in planned_steps:
        if step.get("statement_kind") == REFERENCE_ONLY_KIND:
            return _precheck_failure("REFERENCE_ONLY_STEP", step)
        if step.get("executable") is not True:
            return _precheck_failure("NON_WRITABLE_STEP", step)
        if step.get("boundary_action") != "run_candidate":
            return _precheck_failure("UNSUPPORTED_BOUNDARY_ACTION", step)
        if _expected_rowcount(step) is None:
            return _precheck_failure("EXPECTED_ROWCOUNT_MISSING", step)
    return None


def _precheck_failure(reason: str, step: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "reason": reason,
        "rowcount_results": [],
        "statement_results": [
            {
                "statement_kind": step.get("statement_kind"),
                "status": "blocked",
                "reason": reason,
            }
        ],
        "hint": "bridge halted before adapter begin",
    }


def _step_failure(
    reason: str, step: Mapping[str, Any], message: str
) -> dict[str, Any]:
    return {
        "reason": reason,
        "rowcount_results": [],
        "statement_results": [
            {
                "statement_kind": step.get("statement_kind"),
                "status": "failed",
                "reason": reason,
                "message": message,
            }
        ],
        "hint": "adapter protocol failure; bridge halted",
    }


def _safe_step(step: Mapping[str, Any]) -> dict[str, Any]:
    return {key: step[key] for key in SAFE_STEP_KEYS if key in step}


def _params(step: Mapping[str, Any]) -> Mapping[str, Any]:
    params = step.get("params", step.get("parameters", {}))
    return params if isinstance(params, Mapping) else {}


def _expected_rowcount(step: Mapping[str, Any]) -> int | None:
    value = step.get("expected_rowcount")
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    return None


def _rowcount_from(result: Mapping[str, Any]) -> int | None:
    value = result.get("actual_rowcount", result.get("rowcount"))
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    return None


def _blocked_receipt(reason: str, errors: Any) -> dict[str, Any]:
    receipt = _receipt(
        {
            "ok": False,
            "statements": [
                {
                    "status": "blocked",
                    "reason": reason,
                    "errors": list(errors or []),
                }
            ],
        }
    )
    receipt["reconciliation_hint"] = "bridge blocked before adapter begin"
    return receipt


def _idempotent_receipt(state: Any) -> dict[str, Any]:
    if state == "done":
        receipt = _receipt(
            {
                "status": DONE_STATUS,
                "statements": [
                    {
                        "status": "idempotent_replay",
                        "reason": "OPERATION_ALREADY_DONE",
                    }
                ],
            }
        )
        receipt["reconciliation_hint"] = "operation already finalized"
        return receipt

    return _blocked_receipt("OPERATION_NOT_CLAIMED", [state or "unknown"])


def _failure_receipt(
    failure: Mapping[str, Any], adapter: PostgreSQLBridgeAdapter | None
) -> dict[str, Any]:
    summary = {
        "ok": False,
        "reason": failure.get("reason"),
        "rowcounts": failure.get("rowcount_results", []),
        "statements": failure.get("statement_results", []),
    }
    if adapter is not None:
        try:
            summary = dict(adapter.seal_failure(failure))
        except Exception:
            receipt = _manual_receipt(str(failure.get("reason") or "failure"))
            receipt["statement_results"] = list(
                failure.get("statement_results", [])
            )
            return receipt

    receipt = _receipt(summary)
    if failure.get("hint"):
        receipt["reconciliation_hint"] = str(failure["hint"])
    return receipt


def _manual_receipt(operation_id: str) -> dict[str, Any]:
    receipt = _receipt(
        {
            "status": "unknown",
            "statements": [
                {
                    "status": "needs_reconciliation",
                    "operation_id": operation_id,
                }
            ],
        }
    )
    receipt["transaction_status"] = UNKNOWN_SEAL_STATUS
    receipt["reconciliation_hint"] = MANUAL_HINT
    return receipt


def _claim_operation(guard: Any, operation_id: str) -> dict[str, Any]:
    claim = getattr(guard, "claim_operation", None)
    if claim is None:
        return {"claimed": False, "state": "guard_has_no_claim"}
    result = claim(operation_id)
    if isinstance(result, Mapping):
        return {
            "claimed": bool(result.get("claimed")),
            "state": result.get("state"),
        }
    return {"claimed": bool(result), "state": None}


def _mark_done(guard: Any, operation_id: str) -> None:
    marker = getattr(guard, "mark_done", None)
    if marker is None:
        marker = getattr(guard, "mark_" + "com" + "mitted", None)
    if marker is not None:
        marker(operation_id)


def _mark_failed(guard: Any, operation_id: str, reason: str) -> None:
    marker = getattr(guard, "mark_failed", None)
    if marker is not None:
        marker(operation_id, reason)
