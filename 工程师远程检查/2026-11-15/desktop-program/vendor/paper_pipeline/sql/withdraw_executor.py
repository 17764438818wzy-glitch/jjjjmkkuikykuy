"""Execute withdraw statements with an injected connection."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from paper_pipeline.sql.withdraw_lifecycle import WithdrawLifecyclePlan
from paper_pipeline.sql.withdraw_statements import (
    WithdrawSqlStatement,
    build_withdraw_sql_statements,
)


@dataclass(frozen=True)
class WithdrawExecutionResult:
    success: bool
    executed_count: int
    resource_id: str | None = None
    statement_types: tuple[str, ...] = ()
    failed_statement_type: str | None = None
    error: str | None = None
    warnings: tuple[str, ...] = ()


def execute_withdraw_plan(
    withdraw_plan: WithdrawLifecyclePlan,
    connection: Any,
) -> WithdrawExecutionResult:
    warnings: list[str] = []

    try:
        statements = build_withdraw_sql_statements(withdraw_plan)
    except Exception as exc:  # noqa: BLE001 - callers receive a result.
        _append_warning(warnings, _safe_close(connection))
        return WithdrawExecutionResult(
            success=False,
            executed_count=0,
            error=str(exc),
            warnings=tuple(warnings),
        )

    if not statements:
        warnings.append("withdraw_noop_no_sql")
        _append_warning(warnings, _safe_close(connection))
        return WithdrawExecutionResult(
            success=True,
            executed_count=0,
            warnings=tuple(warnings),
        )

    return _execute_statements(statements, connection, warnings)


def _execute_statements(
    statements: tuple[WithdrawSqlStatement, ...],
    connection: Any,
    warnings: list[str],
) -> WithdrawExecutionResult:
    executed_statement_types: list[str] = []
    failed_statement_type: str | None = None
    error: str | None = None
    success = False
    transaction_started = False
    cleanup_done = False

    try:
        with connection.cursor() as cursor:
            transaction_started = True
            for statement in statements:
                try:
                    cursor.execute(statement.sql, statement.params)
                except Exception as exc:  # noqa: BLE001 - callers receive a result.
                    failed_statement_type = statement.statement_type
                    error = str(exc)
                    cleanup_done = _rollback_once(connection, warnings)
                    break
                executed_statement_types.append(statement.statement_type)

        if error is None:
            try:
                connection.commit()
            except Exception as exc:  # noqa: BLE001 - callers receive a result.
                error = str(exc)
                cleanup_done = _rollback_once(connection, warnings)
            else:
                success = True
    except Exception as exc:  # noqa: BLE001 - callers receive a result.
        if error is None:
            error = str(exc)
        if transaction_started and not cleanup_done:
            cleanup_done = _rollback_once(connection, warnings)
    finally:
        _append_warning(warnings, _safe_close(connection))

    return WithdrawExecutionResult(
        success=success,
        resource_id=statements[0].resource_id,
        executed_count=len(executed_statement_types),
        statement_types=tuple(executed_statement_types),
        failed_statement_type=failed_statement_type,
        error=error,
        warnings=tuple(warnings),
    )


def _rollback_once(connection: Any, warnings: list[str]) -> bool:
    _append_warning(warnings, _safe_rollback(connection))
    return True


def _safe_rollback(connection: Any) -> str | None:
    try:
        connection.rollback()
    except Exception as exc:  # noqa: BLE001 - best effort cleanup.
        return f"rollback failed: {exc}"
    return None


def _safe_close(connection: Any) -> str | None:
    try:
        connection.close()
    except Exception as exc:  # noqa: BLE001 - best effort cleanup.
        return f"close failed: {exc}"
    return None


def _append_warning(warnings: list[str], warning: str | None) -> None:
    if warning is not None:
        warnings.append(warning)
