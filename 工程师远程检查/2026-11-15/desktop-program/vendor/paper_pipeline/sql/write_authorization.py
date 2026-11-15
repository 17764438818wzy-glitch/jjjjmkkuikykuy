"""Runtime authorization for SQL writes.

Read-only SQL checks can run independently. Mutating SQL operations must be
entered from the unified real_flow_50 pipeline path.
"""

from __future__ import annotations

import inspect
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from paper_pipeline.runtime.single_entry import REAL_FLOW_50_MODULE


@dataclass(frozen=True, slots=True)
class SqlWriteAuthorization:
    run_id: str
    operation: str
    source_module: str = REAL_FLOW_50_MODULE


_SQL_WRITE_AUTHORIZATION: ContextVar[SqlWriteAuthorization | None] = ContextVar(
    "paper_pipeline_sql_write_authorization",
    default=None,
)


@contextmanager
def authorize_sql_write_from_real_flow_50(
    *,
    run_id: str,
    operation: str,
) -> Iterator[SqlWriteAuthorization]:
    if not _stack_has_real_flow_50_or_test_caller():
        raise RuntimeError(
            authorization_required_message("authorize_sql_write_from_real_flow_50"),
        )
    authorization = SqlWriteAuthorization(
        run_id=str(run_id).strip(),
        operation=str(operation).strip(),
    )
    if not authorization.run_id:
        raise RuntimeError("SQL write authorization requires a run_id")
    token = _SQL_WRITE_AUTHORIZATION.set(authorization)
    try:
        yield authorization
    finally:
        _SQL_WRITE_AUTHORIZATION.reset(token)


def current_sql_write_authorization() -> SqlWriteAuthorization | None:
    authorization = _SQL_WRITE_AUTHORIZATION.get()
    if authorization is None:
        return None
    if authorization.source_module != REAL_FLOW_50_MODULE:
        return None
    if not authorization.run_id:
        return None
    return authorization


def has_sql_write_authorization() -> bool:
    return current_sql_write_authorization() is not None


def ensure_sql_write_authorized(operation: str) -> None:
    if has_sql_write_authorization():
        return
    raise RuntimeError(authorization_required_message(operation))


def authorization_required_message(operation: str) -> str:
    return (
        f"{operation} must run through {REAL_FLOW_50_MODULE} "
        "authorized SQL write path"
    )


def _stack_has_real_flow_50_or_test_caller() -> bool:
    frame = inspect.currentframe()
    try:
        candidate = frame.f_back if frame is not None else None
        while candidate is not None:
            module = str(candidate.f_globals.get("__name__", ""))
            if module == REAL_FLOW_50_MODULE:
                return True
            if module.startswith("tests.") or module.startswith("test_"):
                return True
            candidate = candidate.f_back
        return False
    finally:
        del frame
