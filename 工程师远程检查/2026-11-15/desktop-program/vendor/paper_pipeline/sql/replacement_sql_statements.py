"""Build replacement SQL statement plans without running them."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from paper_pipeline.sql.replacement_lifecycle import (
    ReplacementLifecycleAction,
    ReplacementLifecyclePlan,
)

REPLACED_STATUS = "replaced"
ACTIVE_STATUS = "active"
EXPECTED_OLD_RESOURCE_STATUS = "active"
TRANSACTION_KIND = "single_resource_replacement"

OLD_RESOURCE_TABLE = "paper_resources"
OLD_RESOURCE_STATEMENT_TYPE = "replacement_old_resource_update"
OLD_INDEX_STATEMENT_TYPE = "replacement_old_index_status_update"
NEW_UPLOAD_REFERENCE_TYPE = "replacement_new_resource_upload_reference"

OLD_INDEX_TABLES = (
    "paper_metadata_index",
    "paper_text_index",
    "paper_chapter_index",
    "paper_hierarchy_index",
)
LOCAL_ACTION_TARGETS = frozenset(
    {
        "old_metadata",
        "old_package_manifest",
        "old_upload_log",
        "new_package_manifest",
        "new_upload_log",
    },
)
REMOTE_ACTION_TARGETS = frozenset({"postgres_old_resource", "postgres_new_resource"})
SUPPORTED_ACTION_TARGETS = frozenset({*LOCAL_ACTION_TARGETS, *REMOTE_ACTION_TARGETS})


@dataclass(frozen=True)
class ReplacementSqlStatement:
    statement_type: str
    table_name: str
    sql: str
    params: Mapping[str, Any]
    executable: bool = True


@dataclass(frozen=True)
class ReplacementSqlStatementPlan:
    status: str
    statements: tuple[ReplacementSqlStatement, ...] = ()
    reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    requires_transaction: bool = False
    transaction_kind: str | None = None


def build_replacement_sql_statement_plan(
    replacement_plan: ReplacementLifecyclePlan,
) -> ReplacementSqlStatementPlan:
    warnings = tuple(replacement_plan.warnings)
    if not replacement_plan.allowed:
        return _non_executable_plan(
            _status_for_blocked_plan(replacement_plan),
            reasons=tuple(replacement_plan.blocking_reasons),
            warnings=warnings,
        )

    if replacement_plan.outcome == "noop":
        return _non_executable_plan("noop", warnings=warnings)

    actions = tuple(replacement_plan.actions)
    unsupported_target = _unsupported_target(actions)
    if unsupported_target is not None:
        return _non_executable_plan(
            "blocked",
            reasons=(f"unsupported_replacement_sql_target:{unsupported_target}",),
            warnings=warnings,
        )

    old_action = _single_action(actions, "postgres_old_resource")
    new_action = _single_action(actions, "postgres_new_resource")
    if old_action is None:
        return _non_executable_plan(
            "blocked",
            reasons=("replacement_old_resource_action_missing",),
            warnings=warnings,
        )
    if new_action is None:
        return _non_executable_plan(
            "blocked",
            reasons=("replacement_new_resource_action_missing",),
            warnings=warnings,
        )
    if old_action.operation != "update_fields":
        return _non_executable_plan(
            "blocked",
            reasons=("unsupported_replacement_sql_operation:postgres_old_resource",),
            warnings=warnings,
        )
    if new_action.operation != "upload_plan_reference":
        return _non_executable_plan(
            "blocked",
            reasons=("unsupported_replacement_sql_operation:postgres_new_resource",),
            warnings=warnings,
        )

    context = _context_from_actions(actions, old_action=old_action, new_action=new_action)
    statements = (
        _old_resource_update_statement(context),
        *(_old_index_update_statement(table_name, context) for table_name in OLD_INDEX_TABLES),
        _new_resource_upload_reference(context),
    )
    return ReplacementSqlStatementPlan(
        status="ready",
        statements=statements,
        warnings=warnings,
        requires_transaction=True,
        transaction_kind=TRANSACTION_KIND,
    )


def _status_for_blocked_plan(replacement_plan: ReplacementLifecyclePlan) -> str:
    if replacement_plan.outcome == "manual_audit_required":
        if any("conflict" in reason for reason in replacement_plan.blocking_reasons):
            return "conflict"
        return "manual_audit"
    return "blocked"


def _unsupported_target(actions: tuple[ReplacementLifecycleAction, ...]) -> str | None:
    for action in actions:
        if action.target not in SUPPORTED_ACTION_TARGETS:
            return action.target
    return None


def _context_from_actions(
    actions: tuple[ReplacementLifecycleAction, ...],
    *,
    old_action: ReplacementLifecycleAction,
    new_action: ReplacementLifecycleAction,
) -> dict[str, Any]:
    old_metadata = _single_action(actions, "old_metadata")
    old_log = _single_action(actions, "old_upload_log")
    new_log = _single_action(actions, "new_upload_log")
    return {
        "old_resource_id": _field(old_action, "resource_id"),
        "new_resource_id": _field(new_action, "resource_id"),
        "resource_status": REPLACED_STATUS,
        "expected_old_resource_status": EXPECTED_OLD_RESOURCE_STATUS,
        "new_resource_status": _field(new_action, "resource_status") or ACTIVE_STATUS,
        "replacement_of_resource_id": _field(new_action, "replacement_of_resource_id"),
        "replaced_by_paper_id": _field(old_action, "replaced_by_paper_id"),
        "replaced_by_version": _field(old_action, "replaced_by_version"),
        "replaces_paper_id": _field(new_action, "replaces_paper_id"),
        "replaces_version": _field(new_action, "replaces_version"),
        "replacement_reason": _first_value(
            old_action,
            new_action,
            old_metadata,
            old_log,
            new_log,
            field_names=("replacement_reason", "sql_replacement_reason"),
        ),
        "replaced_by": _first_value(
            old_metadata,
            old_log,
            new_log,
            field_names=("sql_replacement_operator", "replacement_operator"),
        ),
        "replacement_timestamp": _replacement_timestamp(old_metadata),
    }


def _old_resource_update_statement(context: Mapping[str, Any]) -> ReplacementSqlStatement:
    sql = (
        "UPDATE paper_resources "
        "SET resource_status = %(resource_status)s, "
        "replaced_by_paper_id = %(replaced_by_paper_id)s, "
        "replaced_by_version = %(replaced_by_version)s, "
        "replacement_reason = %(replacement_reason)s, "
        "updated_at = %(updated_at)s, "
        "replaced_at = %(replaced_at)s, "
        "replaced_by = %(replaced_by)s "
        "WHERE id = %(old_resource_id)s "
        "AND resource_status = %(expected_old_resource_status)s;"
    )
    return ReplacementSqlStatement(
        statement_type=OLD_RESOURCE_STATEMENT_TYPE,
        table_name=OLD_RESOURCE_TABLE,
        sql=sql,
        params={
            "old_resource_id": context["old_resource_id"],
            "resource_status": REPLACED_STATUS,
            "expected_old_resource_status": EXPECTED_OLD_RESOURCE_STATUS,
            "replaced_by_paper_id": context["replaced_by_paper_id"],
            "replaced_by_version": context["replaced_by_version"],
            "replacement_reason": context["replacement_reason"],
            "updated_at": context["replacement_timestamp"],
            "replaced_at": context["replacement_timestamp"],
            "replaced_by": context["replaced_by"],
        },
    )


def _old_index_update_statement(
    table_name: str,
    context: Mapping[str, Any],
) -> ReplacementSqlStatement:
    sql = (
        f"UPDATE {table_name} "
        "SET resource_status = %(resource_status)s, "
        "updated_at = %(updated_at)s "
        "WHERE resource_id = %(old_resource_id)s;"
    )
    return ReplacementSqlStatement(
        statement_type=OLD_INDEX_STATEMENT_TYPE,
        table_name=table_name,
        sql=sql,
        params={
            "old_resource_id": context["old_resource_id"],
            "resource_status": REPLACED_STATUS,
            "updated_at": context["replacement_timestamp"],
        },
    )


def _new_resource_upload_reference(context: Mapping[str, Any]) -> ReplacementSqlStatement:
    return ReplacementSqlStatement(
        statement_type=NEW_UPLOAD_REFERENCE_TYPE,
        table_name=OLD_RESOURCE_TABLE,
        sql="",
        params={
            "new_resource_id": context["new_resource_id"],
            "resource_status": context["new_resource_status"],
            "replacement_of_resource_id": context["replacement_of_resource_id"],
            "replaces_paper_id": context["replaces_paper_id"],
            "replaces_version": context["replaces_version"],
            "replacement_reason": context["replacement_reason"],
        },
        executable=False,
    )


def _non_executable_plan(
    status: str,
    *,
    reasons: tuple[str, ...] = (),
    warnings: tuple[str, ...] = (),
) -> ReplacementSqlStatementPlan:
    return ReplacementSqlStatementPlan(
        status=status,
        statements=(),
        reasons=reasons,
        warnings=warnings,
    )


def _single_action(
    actions: tuple[ReplacementLifecycleAction, ...],
    target: str,
) -> ReplacementLifecycleAction | None:
    for action in actions:
        if action.target == target:
            return action
    return None


def _field(action: ReplacementLifecycleAction, field_name: str) -> Any:
    return action.fields.get(field_name)


def _first_value(
    *actions: ReplacementLifecycleAction | None,
    field_names: tuple[str, ...],
) -> Any:
    for action in actions:
        if action is None:
            continue
        for field_name in field_names:
            value = action.fields.get(field_name)
            if value is not None:
                return value
    return None


def _replacement_timestamp(action: ReplacementLifecycleAction | None) -> Any:
    if action is None:
        return None
    raw_history = action.fields.get("sql_audit_history")
    if not isinstance(raw_history, list) or not raw_history:
        return None
    first_event = raw_history[0]
    if not isinstance(first_event, Mapping):
        return None
    return first_event.get("requested_at")
