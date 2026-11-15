"""Execute formal upload plans with an injected connection.

This module owns transaction semantics for formal upload statements, but it
does not create connections or load runtime configuration.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from paper_pipeline.sql.formal_statements import build_formal_insert_statements
from paper_pipeline.sql.formal_upload_plan import FormalUploadPlan


@dataclass(frozen=True)
class FormalUploadExecutionResult:
    success: bool
    resource_id: str
    upload_batch_id: str
    executed_count: int
    record_ids: tuple[str, ...]
    failed_record_id: str | None = None
    error: str | None = None


def execute_formal_upload_plan(
    plan: FormalUploadPlan,
    connection: Any,
) -> FormalUploadExecutionResult:
    executed_record_ids: list[str] = []
    failed_record_id: str | None = None
    should_rollback = False
    success = False
    error: str | None = None

    try:
        statements = build_formal_insert_statements(plan)
        with connection.cursor() as cursor:
            should_rollback = True
            for statement in statements:
                try:
                    cursor.execute(statement.sql, statement.params)
                except Exception as exc:  # noqa: BLE001 - adapter returns a result.
                    failed_record_id = statement.record_id
                    raise exc
                executed_record_ids.append(statement.record_id)

        try:
            connection.commit()
        except Exception as exc:  # noqa: BLE001 - adapter returns a result.
            rollback_error = _safe_rollback(connection)
            error = _merge_errors(str(exc), rollback_error)
        else:
            success = True
    except Exception as exc:  # noqa: BLE001 - adapter returns a result.
        rollback_error = _safe_rollback(connection) if should_rollback else None
        error = _merge_errors(str(exc), rollback_error)
    finally:
        close_error = _safe_close(connection)
        error = _merge_errors(error, close_error) if error is not None else close_error

    return _result(
        plan=plan,
        success=success,
        executed_record_ids=executed_record_ids,
        failed_record_id=failed_record_id,
        error=error,
    )


def _result(
    *,
    plan: FormalUploadPlan,
    success: bool,
    executed_record_ids: list[str],
    failed_record_id: str | None,
    error: str | None,
) -> FormalUploadExecutionResult:
    return FormalUploadExecutionResult(
        success=success,
        resource_id=plan.resource_id,
        upload_batch_id=plan.upload_batch_id,
        executed_count=len(executed_record_ids),
        record_ids=tuple(executed_record_ids),
        failed_record_id=failed_record_id,
        error=error,
    )


def _safe_rollback(connection: Any) -> str | None:
    try:
        connection.rollback()
    except Exception as exc:  # noqa: BLE001 - rollback is best effort.
        return f"rollback failed: {exc}"
    return None


def _safe_close(connection: Any) -> str | None:
    try:
        connection.close()
    except Exception as exc:  # noqa: BLE001 - close is best effort.
        return f"close failed: {exc}"
    return None


def _merge_errors(primary: str, secondary: str | None) -> str:
    if secondary is None:
        return primary
    return f"{primary}; {secondary}"
