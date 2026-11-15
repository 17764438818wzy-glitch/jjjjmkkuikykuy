"""Formal withdraw UPDATE statement builder."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping

from paper_pipeline.sql.withdraw_lifecycle import (
    WithdrawLifecycleAction,
    WithdrawLifecyclePlan,
)

TABLE_NAME = "paper_resources"
WHERE_FIELD = "resource_id"
RESOURCE_TABLE_ID_FIELD = "id"
WITHDRAWN_STATUS = "withdrawn"
STATEMENT_TYPE = "withdraw_resource"

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_LOCAL_ACTION_TARGETS = frozenset({"metadata", "package_manifest", "upload_log"})
_ALLOWED_TARGETS = frozenset({"postgres_resource", *_LOCAL_ACTION_TARGETS})
_ALLOWED_FIELDS = frozenset(
    {
        WHERE_FIELD,
        "resource_status",
        "withdrawn_at",
        "withdrawn_by",
        "withdrawn_reason",
    },
)
_UPDATE_FIELDS = (
    "resource_status",
    "withdrawn_at",
    "withdrawn_by",
    "withdrawn_reason",
)


@dataclass(frozen=True)
class WithdrawSqlStatement:
    table_name: str
    sql: str
    params: tuple[Any, ...]
    resource_id: str
    statement_type: str = STATEMENT_TYPE


def build_withdraw_sql_statements(
    withdraw_plan: WithdrawLifecyclePlan,
) -> tuple[WithdrawSqlStatement, ...]:
    if not withdraw_plan.allowed:
        raise ValueError("withdraw_plan_not_allowed")

    if withdraw_plan.outcome == "noop":
        return ()

    postgres_actions = _postgres_actions(tuple(withdraw_plan.actions))
    if not postgres_actions:
        raise ValueError("withdraw_postgres_resource_action_missing")

    return tuple(_build_statement(action) for action in postgres_actions)


def _postgres_actions(
    actions: tuple[WithdrawLifecycleAction, ...],
) -> tuple[WithdrawLifecycleAction, ...]:
    postgres_actions: list[WithdrawLifecycleAction] = []
    for action in actions:
        if action.target not in _ALLOWED_TARGETS:
            raise ValueError(f"unsupported_withdraw_sql_target:{action.target}")
        if action.target in _LOCAL_ACTION_TARGETS:
            continue
        if action.operation != "update_fields":
            raise ValueError(f"unsupported_withdraw_sql_operation:{action.target}")
        postgres_actions.append(action)
    return tuple(postgres_actions)


def _build_statement(action: WithdrawLifecycleAction) -> WithdrawSqlStatement:
    fields = dict(action.fields)
    _validate_fields(fields)
    resource_id = _required_text(fields.get(WHERE_FIELD), "withdraw_resource_id_required")

    resource_status = fields.get("resource_status")
    if resource_status != WITHDRAWN_STATUS:
        raise ValueError("withdraw_resource_status_must_be_withdrawn")

    params = tuple(fields.get(field_name) for field_name in _UPDATE_FIELDS)
    _validate_param_values(dict(zip(_UPDATE_FIELDS, params, strict=True)))

    sql = (
        "UPDATE paper_resources "
        "SET resource_status = %s, withdrawn_at = %s, "
        "withdrawn_by = %s, withdrawn_reason = %s "
        f"WHERE {RESOURCE_TABLE_ID_FIELD} = %s;"
    )
    return WithdrawSqlStatement(
        table_name=TABLE_NAME,
        sql=sql,
        params=(*params, resource_id),
        resource_id=resource_id,
    )


def _validate_fields(fields: Mapping[str, Any]) -> None:
    for field_name in fields:
        if not isinstance(field_name, str) or not _IDENTIFIER_RE.fullmatch(field_name):
            raise ValueError(f"invalid_withdraw_sql_field:{field_name}")
        if field_name not in _ALLOWED_FIELDS:
            raise ValueError(f"unsupported_withdraw_sql_field:{field_name}")


def _validate_param_values(fields: Mapping[str, Any]) -> None:
    for field_name, field_value in fields.items():
        if isinstance(field_value, (list, dict)):
            raise TypeError(f"withdraw_sql_field_structured_value:{field_name}")


def _required_text(value: Any, reason_code: str) -> str:
    normalized = _optional_text(value)
    if normalized is None:
        raise ValueError(reason_code)
    return normalized


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None
