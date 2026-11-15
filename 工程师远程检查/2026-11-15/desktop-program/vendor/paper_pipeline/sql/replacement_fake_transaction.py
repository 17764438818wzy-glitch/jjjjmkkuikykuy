"""Simulate replacement statement plans against in-memory table state."""

from __future__ import annotations

import copy
import hashlib
from dataclasses import dataclass, field
from json import dumps
from typing import Any, Mapping

from paper_pipeline.sql.replacement_sql_statements import (
    ReplacementSqlStatement,
    ReplacementSqlStatementPlan,
)

READY_STATUSES = frozenset({"ready", "success", "allowed"})
TRANSACTION_KIND = "single_resource_replacement"
OLD_RESOURCE_KIND = "replacement_old_resource_update"
OLD_INDEX_KIND = "replacement_old_index_status_update"
NEW_REFERENCE_KIND = "replacement_new_resource_upload_reference"
EXECUTABLE_KINDS = frozenset({OLD_RESOURCE_KIND, OLD_INDEX_KIND})
REFERENCE_KINDS = frozenset({NEW_REFERENCE_KIND})
FORBIDDEN_SQL_FRAGMENTS = (
    "DELETE FROM",
    "DROP TABLE",
    "TRUNCATE",
    "ALTER TABLE",
    "CREATE TABLE",
)
INDEX_TABLE_NAMES = frozenset(
    {
        "paper_metadata_index",
        "paper_text_index",
        "paper_chapter_index",
        "paper_hierarchy_index",
    },
)


@dataclass
class FakeReplacementTableState:
    paper_resources: dict[str, dict[str, Any]] = field(default_factory=dict)
    paper_metadata_index: dict[str, Any] = field(default_factory=dict)
    paper_text_index: dict[str, Any] = field(default_factory=dict)
    paper_chapter_index: dict[str, Any] = field(default_factory=dict)
    paper_hierarchy_index: dict[str, Any] = field(default_factory=dict)
    audit_log: list[dict[str, Any]] = field(default_factory=list)
    uploaded_references: list[dict[str, Any]] = field(default_factory=list)
    reference_record_scope: str = "fake_non_executable_reference"


@dataclass(frozen=True)
class ReplacementFakeTransactionEvent:
    event: str
    statement_kind: str
    table_name: str
    details: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ReplacementFakeTransactionResult:
    status: str
    committed: bool
    rolled_back: bool
    skipped: bool
    execution_scope: str = "fake_state"
    commit_scope: str = "none"
    committed_to_fake_state: bool = False
    rolled_back_fake_state: bool = False
    connected_postgres: bool = False
    executed_sql: bool = False
    real_postgres_committed: bool = False
    wrote_local_files: bool = False
    reference_record_scope: str = "fake_non_executable_reference"
    reason: str | None = None
    failed_statement_kind: str | None = None
    failed_statement_index: int | None = None
    applied_statement_kinds: tuple[str, ...] = ()
    skipped_reference_kinds: tuple[str, ...] = ()
    transaction_events: tuple[ReplacementFakeTransactionEvent, ...] = ()
    changed_tables: tuple[str, ...] = ()
    before_snapshot_hash: str | None = None
    after_snapshot_hash: str | None = None
    statement_count: int = 0
    executable_statement_count: int = 0
    non_executable_reference_count: int = 0


def run_replacement_fake_transaction(
    statement_plan: ReplacementSqlStatementPlan,
    fake_state: FakeReplacementTableState,
    *,
    now: str | None = None,
) -> ReplacementFakeTransactionResult:
    before_hash = _snapshot_hash(fake_state)
    statement_count = len(statement_plan.statements)
    executable_count = sum(1 for statement in statement_plan.statements if statement.executable)
    reference_count = sum(1 for statement in statement_plan.statements if not statement.executable)

    if statement_plan.status not in READY_STATUSES:
        return _result(
            status="skipped",
            committed=False,
            rolled_back=False,
            skipped=True,
            reason=_skip_reason(statement_plan),
            before_hash=before_hash,
            after_hash=_snapshot_hash(fake_state),
            statement_count=statement_count,
            executable_count=executable_count,
            reference_count=reference_count,
        )

    rejection = _preflight_rejection(statement_plan)
    if rejection is not None:
        failed_index, failed_kind, reason = rejection
        return _result(
            status="rejected",
            committed=False,
            rolled_back=False,
            skipped=False,
            reason=reason,
            failed_statement_kind=failed_kind,
            failed_statement_index=failed_index,
            before_hash=before_hash,
            after_hash=_snapshot_hash(fake_state),
            statement_count=statement_count,
            executable_count=executable_count,
            reference_count=reference_count,
        )

    staged_state = copy.deepcopy(fake_state)
    applied_kinds: list[str] = []
    skipped_references: list[str] = []
    events: list[ReplacementFakeTransactionEvent] = []
    changed_tables: list[str] = []
    reference_context: dict[str, Any] = {}

    for index, statement in enumerate(statement_plan.statements):
        if not statement.executable:
            skipped_references.append(statement.statement_type)
            reference_details = _reference_details(statement, reference_context)
            reference_context.update(reference_details)
            staged_state.uploaded_references.append(reference_details)
            events.append(
                ReplacementFakeTransactionEvent(
                    event="fake_reference_recorded",
                    statement_kind=statement.statement_type,
                    table_name=statement.table_name,
                    details=reference_details,
                ),
            )
            continue

        failure_reason = _apply_statement(
            staged_state,
            statement,
            changed_tables=changed_tables,
            events=events,
            reference_context=reference_context,
            now=now,
        )
        if failure_reason is not None:
            return _result(
                status="rolled_back",
                committed=False,
                rolled_back=True,
                skipped=False,
                reason=failure_reason,
                failed_statement_kind=statement.statement_type,
                failed_statement_index=index,
                applied_statement_kinds=tuple(applied_kinds),
                skipped_reference_kinds=tuple(skipped_references),
                transaction_events=tuple(events),
                changed_tables=tuple(changed_tables),
                before_hash=before_hash,
                after_hash=_snapshot_hash(fake_state),
                statement_count=statement_count,
                executable_count=executable_count,
                reference_count=reference_count,
            )
        applied_kinds.append(statement.statement_type)

    _commit_state(staged_state, fake_state)
    fake_state.audit_log.extend(_event_payload(event) for event in events)
    return _result(
        status="committed",
        committed=True,
        rolled_back=False,
        skipped=False,
        applied_statement_kinds=tuple(applied_kinds),
        skipped_reference_kinds=tuple(skipped_references),
        transaction_events=tuple(events),
        changed_tables=tuple(changed_tables),
        before_hash=before_hash,
        after_hash=_snapshot_hash(fake_state),
        statement_count=statement_count,
        executable_count=executable_count,
        reference_count=reference_count,
    )


def _preflight_rejection(
    statement_plan: ReplacementSqlStatementPlan,
) -> tuple[int | None, str | None, str] | None:
    if statement_plan.requires_transaction is not True:
        return None, None, "replacement_transaction_required"
    if statement_plan.transaction_kind != TRANSACTION_KIND:
        return None, None, "unsupported_transaction_kind"
    for index, statement in enumerate(statement_plan.statements):
        forbidden_fragment = _forbidden_sql_fragment(statement.sql)
        if forbidden_fragment is not None:
            return index, statement.statement_type, f"forbidden_sql_text:{forbidden_fragment}"
        if statement.executable:
            if statement.statement_type not in EXECUTABLE_KINDS:
                return index, statement.statement_type, f"forbidden_statement_kind:{statement.statement_type}"
        elif statement.statement_type not in REFERENCE_KINDS:
            return index, statement.statement_type, f"forbidden_reference_kind:{statement.statement_type}"
    if not any(statement.statement_type == OLD_RESOURCE_KIND and statement.executable for statement in statement_plan.statements):
        return None, None, "old_resource_update_missing"
    return None


def _apply_statement(
    staged_state: FakeReplacementTableState,
    statement: ReplacementSqlStatement,
    *,
    changed_tables: list[str],
    events: list[ReplacementFakeTransactionEvent],
    reference_context: dict[str, Any],
    now: str | None,
) -> str | None:
    if statement.statement_type == OLD_RESOURCE_KIND:
        return _apply_old_resource_update(
            staged_state,
            statement,
            changed_tables=changed_tables,
            events=events,
            reference_context=reference_context,
            now=now,
        )
    if statement.statement_type == OLD_INDEX_KIND:
        return _apply_index_update(
            staged_state,
            statement,
            changed_tables=changed_tables,
            events=events,
            now=now,
        )
    return f"forbidden_statement_kind:{statement.statement_type}"


def _apply_old_resource_update(
    staged_state: FakeReplacementTableState,
    statement: ReplacementSqlStatement,
    *,
    changed_tables: list[str],
    events: list[ReplacementFakeTransactionEvent],
    reference_context: dict[str, Any],
    now: str | None,
) -> str | None:
    params = dict(statement.params)
    old_resource_id = params.get("old_resource_id")
    resource = staged_state.paper_resources.get(str(old_resource_id))
    if resource is None:
        return "old_resource_missing"
    if resource.get("resource_status") != "active":
        return "old_resource_not_active"

    resource["resource_status"] = params.get("resource_status")
    resource["replaced_by_paper_id"] = params.get("replaced_by_paper_id")
    resource["replaced_by_version"] = params.get("replaced_by_version")
    resource["replacement_reason"] = params.get("replacement_reason")
    resource["updated_at"] = params.get("updated_at") or now
    if "replaced_at" in params:
        resource["replaced_at"] = params.get("replaced_at")
    if "replaced_by" in params:
        resource["replaced_by"] = params.get("replaced_by")
    if "replaced_by" in params:
        reference_context["operator"] = params.get("replaced_by")

    _append_once(changed_tables, "paper_resources")
    events.append(
        ReplacementFakeTransactionEvent(
            event="fake_old_resource_replaced",
            statement_kind=statement.statement_type,
            table_name=statement.table_name,
            details={
                "old_resource_id": old_resource_id,
                "new_resource_id": reference_context.get("new_resource_id"),
                "resource_status": resource.get("resource_status"),
            },
        ),
    )
    return None


def _apply_index_update(
    staged_state: FakeReplacementTableState,
    statement: ReplacementSqlStatement,
    *,
    changed_tables: list[str],
    events: list[ReplacementFakeTransactionEvent],
    now: str | None,
) -> str | None:
    table = _table_by_name(staged_state, statement.table_name)
    if table is None:
        return f"index_table_missing:{statement.table_name}"

    params = dict(statement.params)
    old_resource_id = params.get("old_resource_id") or params.get("resource_id")
    rows = _rows_for_resource(table, old_resource_id)
    if not rows:
        return f"index_resource_missing:{statement.table_name}"

    for row in rows:
        row["resource_status"] = params.get("resource_status")
        row["updated_at"] = params.get("updated_at") or now

    _append_once(changed_tables, statement.table_name)
    events.append(
        ReplacementFakeTransactionEvent(
            event="fake_index_status_replaced",
            statement_kind=statement.statement_type,
            table_name=statement.table_name,
            details={"old_resource_id": old_resource_id, "row_count": len(rows)},
        ),
    )
    return None


def _table_by_name(staged_state: FakeReplacementTableState, table_name: str) -> dict[str, Any] | None:
    if table_name not in INDEX_TABLE_NAMES:
        return None
    return getattr(staged_state, table_name)


def _rows_for_resource(table: Mapping[str, Any], resource_id: object) -> list[dict[str, Any]]:
    resource_key = str(resource_id)
    direct = table.get(resource_key)
    if isinstance(direct, list):
        return [row for row in direct if isinstance(row, dict)]
    if isinstance(direct, dict):
        return [direct]

    rows: list[dict[str, Any]] = []
    for value in table.values():
        if isinstance(value, dict) and value.get("resource_id") == resource_key:
            rows.append(value)
        elif isinstance(value, list):
            rows.extend(row for row in value if isinstance(row, dict) and row.get("resource_id") == resource_key)
    return rows


def _reference_details(
    statement: ReplacementSqlStatement,
    reference_context: Mapping[str, Any],
) -> dict[str, Any]:
    params = dict(statement.params)
    return {
        "old_resource_id": params.get("replacement_of_resource_id"),
        "new_resource_id": params.get("new_resource_id"),
        "replaces_paper_id": params.get("replaces_paper_id"),
        "replaces_version": params.get("replaces_version"),
        "replacement_reason": params.get("replacement_reason"),
        "operator": params.get("operator") or reference_context.get("operator"),
    }


def _commit_state(staged_state: FakeReplacementTableState, fake_state: FakeReplacementTableState) -> None:
    _replace_mapping(fake_state.paper_resources, staged_state.paper_resources)
    _replace_mapping(fake_state.paper_metadata_index, staged_state.paper_metadata_index)
    _replace_mapping(fake_state.paper_text_index, staged_state.paper_text_index)
    _replace_mapping(fake_state.paper_chapter_index, staged_state.paper_chapter_index)
    _replace_mapping(fake_state.paper_hierarchy_index, staged_state.paper_hierarchy_index)
    fake_state.audit_log[:] = list(staged_state.audit_log)
    fake_state.uploaded_references[:] = list(staged_state.uploaded_references)


def _replace_mapping(target: dict[str, Any], source: Mapping[str, Any]) -> None:
    target.clear()
    target.update(copy.deepcopy(dict(source)))


def _forbidden_sql_fragment(sql: str) -> str | None:
    upper_sql = sql.upper()
    for fragment in FORBIDDEN_SQL_FRAGMENTS:
        if fragment in upper_sql:
            return fragment
    return None


def _skip_reason(statement_plan: ReplacementSqlStatementPlan) -> str | None:
    if statement_plan.reasons:
        return statement_plan.reasons[0]
    if statement_plan.status == "noop":
        return "replacement_noop"
    return statement_plan.status


def _snapshot_hash(fake_state: FakeReplacementTableState) -> str:
    encoded = dumps(_state_payload(fake_state), sort_keys=True, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _state_payload(fake_state: FakeReplacementTableState) -> dict[str, Any]:
    return {
        "paper_resources": fake_state.paper_resources,
        "paper_metadata_index": fake_state.paper_metadata_index,
        "paper_text_index": fake_state.paper_text_index,
        "paper_chapter_index": fake_state.paper_chapter_index,
        "paper_hierarchy_index": fake_state.paper_hierarchy_index,
        "audit_log": fake_state.audit_log,
        "uploaded_references": fake_state.uploaded_references,
    }


def _event_payload(event: ReplacementFakeTransactionEvent) -> dict[str, Any]:
    return {
        "event": event.event,
        "statement_kind": event.statement_kind,
        "table_name": event.table_name,
        "details": dict(event.details),
    }


def _append_once(values: list[str], value: str) -> None:
    if value not in values:
        values.append(value)


def _result(
    *,
    status: str,
    committed: bool,
    rolled_back: bool,
    skipped: bool,
    reason: str | None = None,
    failed_statement_kind: str | None = None,
    failed_statement_index: int | None = None,
    applied_statement_kinds: tuple[str, ...] = (),
    skipped_reference_kinds: tuple[str, ...] = (),
    transaction_events: tuple[ReplacementFakeTransactionEvent, ...] = (),
    changed_tables: tuple[str, ...] = (),
    before_hash: str | None = None,
    after_hash: str | None = None,
    statement_count: int = 0,
    executable_count: int = 0,
    reference_count: int = 0,
) -> ReplacementFakeTransactionResult:
    return ReplacementFakeTransactionResult(
        status=status,
        committed=committed,
        rolled_back=rolled_back,
        skipped=skipped,
        execution_scope="fake_state",
        commit_scope="fake_state" if committed else "none",
        committed_to_fake_state=committed,
        rolled_back_fake_state=rolled_back,
        connected_postgres=False,
        executed_sql=False,
        real_postgres_committed=False,
        wrote_local_files=False,
        reference_record_scope="fake_non_executable_reference",
        reason=reason,
        failed_statement_kind=failed_statement_kind,
        failed_statement_index=failed_statement_index,
        applied_statement_kinds=applied_statement_kinds,
        skipped_reference_kinds=skipped_reference_kinds,
        transaction_events=transaction_events,
        changed_tables=changed_tables,
        before_snapshot_hash=before_hash,
        after_snapshot_hash=after_hash,
        statement_count=statement_count,
        executable_statement_count=executable_count,
        non_executable_reference_count=reference_count,
    )
