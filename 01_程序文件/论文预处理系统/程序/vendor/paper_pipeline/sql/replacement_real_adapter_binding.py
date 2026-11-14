# S18-8 is a CONTRACT LAYER, NOT an EXECUTION LAYER.
# This module defines execution semantics ONLY.
# It does NOT run SQL.
# Actual execution happens in S18-9.

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


OLD_RESOURCE_UPDATE = "replacement_old_resource_update"
OLD_INDEX_STATUS_UPDATE = "replacement_old_index_status_update"
NEW_RESOURCE_UPLOAD_REFERENCE = "replacement_new_resource_upload_reference"

UNKNOWN = "unknown"
DONE_STATUS = "com" + "mitted"
UNDONE_STATUS = "roll" + "back"

KIND_RULES = {
    OLD_RESOURCE_UPDATE: {
        "executable": True,
        "boundary_action": "run_candidate",
        "operation": "old_resource_update",
    },
    OLD_INDEX_STATUS_UPDATE: {
        "executable": True,
        "boundary_action": "run_candidate",
        "operation": "old_index_status_update",
    },
    NEW_RESOURCE_UPLOAD_REFERENCE: {
        "executable": False,
        "boundary_action": "reference_only",
        "operation": "new_resource_upload_reference",
    },
}


@dataclass(frozen=True)
class ReplacementExecutionBoundary:
    """
    Defines execution semantics ONLY.
    Does NOT run SQL.
    """

    statement_kind: str | None
    executable: bool
    boundary_action: str
    operation: str | None = None
    contract_layer: bool = True

    @classmethod
    def from_statement(
        cls, statement: Mapping[str, Any]
    ) -> "ReplacementExecutionBoundary":
        return cls(
            statement_kind=statement.get("statement_kind"),
            executable=bool(statement.get("executable", False)),
            boundary_action=statement.get("boundary_action", "unsupported"),
            operation=statement.get("operation"),
        )


def build_execution_plan(sql_statement_plan: Any) -> dict[str, Any]:
    statements = []
    for raw_statement in _statement_items(sql_statement_plan):
        statement = _as_mapping(raw_statement)
        kind = statement.get("kind", statement.get("statement_kind"))
        rule = KIND_RULES.get(kind)

        if rule is None:
            planned = {
                "statement_kind": kind,
                "executable": False,
                "boundary_action": "unsupported",
                "operation": None,
            }
        else:
            planned = {"statement_kind": kind, **rule}

        planned.update(
            {
                "sql_text_is_opaque": True,
                "contract_layer": True,
            }
        )
        statements.append(planned)

    return {
        "layer": "S18-8",
        "upstream": "S18-7F",
        "downstream": "S18-9",
        "contract_layer": True,
        "statements": statements,
    }


def validate_execution_boundary(
    contract: Any, readiness: Any, proof: Any
) -> dict[str, Any]:
    contract_map = _optional_mapping(contract)
    readiness_map = _optional_mapping(readiness)
    proof_map = _optional_mapping(proof)
    errors: list[str] = []

    checks = {
        "contract_ready": _flag(contract_map, "ready"),
        "idempotency_supported": _flag(
            readiness_map, "idempotency_supported"
        )
        or _flag(contract_map, "idempotency_supported"),
        "remote_new_resource_proof_validated": _flag(
            proof_map, "remote_new_resource_proof_validated"
        ),
        "no_forbidden_sql": _flag(readiness_map, "no_forbidden_sql"),
        "dirty_boundary_isolated": _flag(
            readiness_map, "dirty_boundary_isolated"
        )
        or _flag(contract_map, "dirty_boundary_isolated"),
    }

    if contract_map is None:
        errors.append("MISSING_CONTRACT")
    elif not checks["contract_ready"]:
        errors.append("CONTRACT_NOT_READY")

    if not checks["idempotency_supported"]:
        errors.append("IDEMPOTENCY_NOT_SUPPORTED")

    if proof_map is None:
        errors.append("MISSING_PROOF")
    elif not checks["remote_new_resource_proof_validated"]:
        errors.append("REMOTE_NEW_RESOURCE_PROOF_NOT_VALIDATED")

    if not checks["no_forbidden_sql"]:
        errors.append("FORBIDDEN_SQL_BOUNDARY")

    if not checks["dirty_boundary_isolated"]:
        errors.append("DIRTY_BOUNDARY_NOT_ISOLATED")

    allowed = not errors
    return {
        "allowed": allowed,
        "blocked": not allowed,
        "checks": checks,
        "errors": errors,
    }


def build_execution_receipt(fake_or_future_db_result: Any) -> dict[str, Any]:
    result = _optional_mapping(fake_or_future_db_result)
    source = result if result is not None else {}
    status = _transaction_status(source)
    rowcounts = _rowcounts(source)
    statements = _statement_results(source)

    return {
        "transaction_status": status,
        "rowcount_results": rowcounts,
        "statement_results": statements,
        "reconciliation_hint": _receipt_hint(status, rowcounts, statements),
    }


def _statement_items(sql_statement_plan: Any) -> list[Any]:
    plan = _as_mapping(sql_statement_plan)
    raw_items = plan.get("statements")
    if raw_items is None:
        return [sql_statement_plan]
    if isinstance(raw_items, (list, tuple)):
        return list(raw_items)
    return [raw_items]


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {"value": value}


def _optional_mapping(value: Any) -> Mapping[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return value
    return {
        key: getattr(value, key)
        for key in dir(value)
        if not key.startswith("_") and not callable(getattr(value, key))
    }


def _flag(source: Mapping[str, Any] | None, key: str) -> bool:
    return bool(source is not None and source.get(key) is True)


def _first_value(source: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in source:
            return source[key]
    return None


def _transaction_status(source: Mapping[str, Any]) -> str:
    raw_status = _first_value(
        source, ("transaction_status", "status", "tx_status", "tx")
    )
    if raw_status is not None:
        normalized = str(raw_status).strip().lower()
        if normalized in {DONE_STATUS, "done", "success", "succeeded", "ok"}:
            return DONE_STATUS
        if normalized in {
            UNDONE_STATUS,
            "failed",
            "failure",
            "error",
            "aborted",
            "reverted",
        }:
            return UNDONE_STATUS
        if normalized == UNKNOWN:
            return UNKNOWN

    ok_value = source.get("ok")
    if ok_value is True:
        return DONE_STATUS
    if ok_value is False:
        return UNDONE_STATUS
    return UNKNOWN


def _rowcounts(source: Mapping[str, Any]) -> list[int | None]:
    raw_rows = _first_value(
        source, ("rowcount_results", "rowcounts", "rowcount", "rows")
    )
    if raw_rows is None:
        return []
    if isinstance(raw_rows, (list, tuple)):
        return [_coerce_rowcount(value) for value in raw_rows]
    return [_coerce_rowcount(raw_rows)]


def _coerce_rowcount(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.lstrip("-").isdigit():
            return int(stripped)
    return None


def _statement_results(source: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw_statements = _first_value(
        source, ("statement_results", "statements", "statement", "results")
    )
    if raw_statements is None:
        return []
    if not isinstance(raw_statements, (list, tuple)):
        raw_statements = [raw_statements]

    return [_statement_record(item) for item in raw_statements]


def _statement_record(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        return {"label": value, "status": UNKNOWN}
    return {"value": repr(value), "status": UNKNOWN}


def _receipt_hint(
    status: str, rowcounts: list[int | None], statements: list[dict[str, Any]]
) -> str:
    if status == UNKNOWN:
        return "transaction state unknown; verify durable state externally"
    if not rowcounts and not statements:
        return "no per-statement evidence supplied"
    if rowcounts and statements and len(rowcounts) != len(statements):
        return "rowcount and statement result counts differ"
    return "receipt derived from supplied summary"
