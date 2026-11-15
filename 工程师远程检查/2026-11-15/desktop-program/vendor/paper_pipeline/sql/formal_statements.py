"""Formal PostgreSQL INSERT statement builder.

This module converts a formal upload plan into parameterized SQL text. It only
builds statement objects; database I/O belongs to a later adapter phase.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from paper_pipeline.sql.formal_schema import FORMAL_TABLE_NAMES
from paper_pipeline.sql.formal_upload_plan import FormalUploadPlan, FormalUploadRecord

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

JSONB_FIELD_NAMES = frozenset(
    {
        "keywords",
        "research_methods",
        "warnings",
        "similar_to",
        "payload_json",
        "heading_path",
        "chapter_ids",
        "chapter_order",
        "relations",
        "methods",
    },
)


@dataclass(frozen=True)
class FormalSqlStatement:
    table_name: str
    sql: str
    params: tuple[Any, ...]
    record_id: str


def build_formal_insert_statements(plan: FormalUploadPlan) -> tuple[FormalSqlStatement, ...]:
    records_by_table: dict[str, list[FormalUploadRecord]] = {}
    write_order = tuple(plan.write_order)
    formal_tables = set(FORMAL_TABLE_NAMES)

    for record in plan.records:
        if record.table_name not in formal_tables:
            raise ValueError(f"unknown formal table: {record.table_name}")
        if record.table_name not in write_order:
            raise ValueError(f"record table not present in write_order: {record.table_name}")
        records_by_table.setdefault(record.table_name, []).append(record)

    statements: list[FormalSqlStatement] = []
    for table_name in write_order:
        if table_name not in formal_tables:
            raise ValueError(f"unknown formal table in write_order: {table_name}")
        for record in records_by_table.get(table_name, []):
            statements.append(_build_insert_statement(record))
    return tuple(statements)


def _build_insert_statement(record: FormalUploadRecord) -> FormalSqlStatement:
    columns = tuple(record.payload.keys())
    if not columns:
        raise ValueError(f"payload must contain at least one column: {record.table_name}")

    for column in columns:
        _validate_column_name(column)

    placeholders: list[str] = []
    params: list[Any] = []
    for column in columns:
        placeholder, param = _placeholder_and_param(column, record.payload[column])
        placeholders.append(placeholder)
        params.append(param)

    sql = (
        f"INSERT INTO {record.table_name} "
        f"({', '.join(columns)}) "
        f"VALUES ({', '.join(placeholders)});"
    )
    return FormalSqlStatement(
        table_name=record.table_name,
        sql=sql,
        params=tuple(params),
        record_id=record.record_id,
    )


def _validate_column_name(column: str) -> None:
    if not _IDENTIFIER_RE.fullmatch(column):
        raise ValueError(f"invalid column name: {column}")


def _placeholder_and_param(column: str, value: Any) -> tuple[str, Any]:
    if column in JSONB_FIELD_NAMES:
        if isinstance(value, (list, dict)):
            return "%s::jsonb", json.dumps(value, ensure_ascii=False, sort_keys=True)
        if value is None:
            return "%s::jsonb", None
        return "%s", value

    if isinstance(value, (list, dict)):
        raise TypeError(f"non-JSONB field received structured value: {column}")
    return "%s", value
