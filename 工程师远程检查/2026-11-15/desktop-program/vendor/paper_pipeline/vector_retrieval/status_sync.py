"""Safe status synchronization for vector-eligible formal resources.

The synchronizer only copies local metadata status back to existing
``paper_resources`` rows when identity and source hash checks match.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from paper_pipeline.vector_retrieval.postgres_adapter import (
    ConnectFactory,
    PostgresVectorConfig,
    _connect,
    _safe_close,
)

_TARGET_INDEX_STATUS = "indexed"
_ALLOWED_LOCAL_SQL_STATUSES = {"not_uploaded", "uploaded"}


@dataclass(frozen=True, slots=True)
class ResourceStatusSyncCandidate:
    resource_id: str
    paper_id: str
    version: int
    metadata_path: str
    source_hash: str
    current_index_status: str
    current_sql_status: str
    target_index_status: str
    target_sql_status: str
    changed_fields: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ResourceStatusSyncSkip:
    paper_id: str
    metadata_path: str
    reason: str


@dataclass(frozen=True, slots=True)
class ResourceStatusSyncPlan:
    candidates: tuple[ResourceStatusSyncCandidate, ...]
    skipped: tuple[ResourceStatusSyncSkip, ...]
    total_count: int

    @property
    def update_count(self) -> int:
        return len(self.candidates)

    @property
    def skip_count(self) -> int:
        return len(self.skipped)


@dataclass(frozen=True, slots=True)
class ResourceStatusSyncExecutionResult:
    requested_count: int
    updated_count: int


def build_resource_status_sync_plan(
    rows: Iterable[Mapping[str, Any]],
    *,
    trust_embedded_local_status: bool = False,
) -> ResourceStatusSyncPlan:
    candidates: list[ResourceStatusSyncCandidate] = []
    skipped: list[ResourceStatusSyncSkip] = []
    total_count = 0

    for row in rows:
        total_count += 1
        db = _db_snapshot(row)
        local_result = _local_snapshot(row, trust_embedded_local_status)
        if isinstance(local_result, ResourceStatusSyncSkip):
            skipped.append(
                ResourceStatusSyncSkip(
                    paper_id=db.paper_id,
                    metadata_path=db.metadata_path,
                    reason=local_result.reason,
                ),
            )
            continue
        local = local_result

        mismatch_reason = _identity_mismatch_reason(db, local)
        if mismatch_reason is not None:
            skipped.append(
                ResourceStatusSyncSkip(
                    paper_id=db.paper_id,
                    metadata_path=db.metadata_path,
                    reason=mismatch_reason,
                ),
            )
            continue

        if local.index_status != _TARGET_INDEX_STATUS:
            skipped.append(
                ResourceStatusSyncSkip(
                    paper_id=db.paper_id,
                    metadata_path=db.metadata_path,
                    reason="local_index_status_not_indexed",
                ),
            )
            continue
        if local.sql_status not in _ALLOWED_LOCAL_SQL_STATUSES:
            skipped.append(
                ResourceStatusSyncSkip(
                    paper_id=db.paper_id,
                    metadata_path=db.metadata_path,
                    reason="local_sql_status_not_syncable",
                ),
            )
            continue

        changed_fields = _changed_fields(
            current_index_status=db.index_status,
            current_sql_status=db.sql_status,
            target_index_status=local.index_status,
            target_sql_status=local.sql_status,
        )
        if not changed_fields:
            continue

        candidates.append(
            ResourceStatusSyncCandidate(
                resource_id=db.resource_id,
                paper_id=db.paper_id,
                version=db.version,
                metadata_path=db.metadata_path,
                source_hash=db.source_hash,
                current_index_status=db.index_status,
                current_sql_status=db.sql_status,
                target_index_status=local.index_status,
                target_sql_status=local.sql_status,
                changed_fields=changed_fields,
            ),
        )

    return ResourceStatusSyncPlan(
        candidates=tuple(candidates),
        skipped=tuple(skipped),
        total_count=total_count,
    )


@dataclass(slots=True)
class PostgresResourceStatusSyncRepository:
    config: PostgresVectorConfig
    environ: Mapping[str, str] | None = None
    connect_factory: ConnectFactory | None = None

    def build_plan(self) -> ResourceStatusSyncPlan:
        connection = _connect(self.config, self.environ, self.connect_factory)
        try:
            with connection.cursor() as cursor:
                cursor.execute(_select_resource_status_rows_sql())
                rows = cursor.fetchall()
            return build_resource_status_sync_plan(rows)
        finally:
            _safe_close(connection)

    def apply_plan(self, plan: ResourceStatusSyncPlan) -> ResourceStatusSyncExecutionResult:
        if not plan.candidates:
            return ResourceStatusSyncExecutionResult(requested_count=0, updated_count=0)

        updated_count = 0
        connection = _connect(self.config, self.environ, self.connect_factory)
        try:
            with connection.cursor() as cursor:
                for candidate in plan.candidates:
                    cursor.execute(_update_resource_status_sql(), _update_params(candidate))
                    updated_count += max(int(getattr(cursor, "rowcount", 0)), 0)
            connection.commit()
        finally:
            _safe_close(connection)

        return ResourceStatusSyncExecutionResult(
            requested_count=plan.update_count,
            updated_count=updated_count,
        )


@dataclass(frozen=True, slots=True)
class _DbStatusSnapshot:
    resource_id: str
    paper_id: str
    version: int
    metadata_path: str
    source_hash: str
    index_status: str
    sql_status: str


@dataclass(frozen=True, slots=True)
class _LocalStatusSnapshot:
    paper_id: str
    version: int
    source_hash: str
    index_status: str
    sql_status: str


def _db_snapshot(row: Mapping[str, Any]) -> _DbStatusSnapshot:
    return _DbStatusSnapshot(
        resource_id=_required_text(row, "resource_id"),
        paper_id=_required_text(row, "paper_id"),
        version=_required_int(row, "version"),
        metadata_path=_required_text(row, "metadata_path"),
        source_hash=_required_text(row, "source_hash"),
        index_status=_required_text(row, "index_status"),
        sql_status=_required_text(row, "sql_status"),
    )


def _local_snapshot(
    row: Mapping[str, Any],
    trust_embedded_local_status: bool,
) -> _LocalStatusSnapshot | ResourceStatusSyncSkip:
    if trust_embedded_local_status:
        return _LocalStatusSnapshot(
            paper_id=_required_text(row, "local_paper_id"),
            version=_required_int(row, "local_version"),
            source_hash=_required_text(row, "local_source_hash"),
            index_status=_required_text(row, "local_index_status"),
            sql_status=_required_text(row, "local_sql_status"),
        )

    metadata_path = Path(_required_text(row, "metadata_path"))
    if not metadata_path.exists():
        return ResourceStatusSyncSkip(paper_id="", metadata_path=str(metadata_path), reason="metadata_missing")
    try:
        decoded = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ResourceStatusSyncSkip(paper_id="", metadata_path=str(metadata_path), reason="metadata_invalid")
    if not isinstance(decoded, dict):
        return ResourceStatusSyncSkip(paper_id="", metadata_path=str(metadata_path), reason="metadata_invalid")

    return _LocalStatusSnapshot(
        paper_id=_required_text(decoded, "paper_id", fallback_key="id"),
        version=_required_int(decoded, "version"),
        source_hash=_required_text(decoded, "source_hash"),
        index_status=_required_text(decoded, "index_status"),
        sql_status=_required_text(decoded, "sql_status"),
    )


def _identity_mismatch_reason(
    db: _DbStatusSnapshot,
    local: _LocalStatusSnapshot,
) -> str | None:
    if local.paper_id != db.paper_id:
        return "paper_id_mismatch"
    if local.version != db.version:
        return "version_mismatch"
    if local.source_hash != db.source_hash:
        return "source_hash_mismatch"
    return None


def _changed_fields(
    *,
    current_index_status: str,
    current_sql_status: str,
    target_index_status: str,
    target_sql_status: str,
) -> tuple[str, ...]:
    changed: list[str] = []
    if current_index_status != target_index_status:
        changed.append("index_status")
    if current_sql_status != target_sql_status:
        changed.append("sql_status")
    return tuple(changed)


def _required_text(
    row: Mapping[str, Any],
    key: str,
    *,
    fallback_key: str | None = None,
) -> str:
    value = row.get(key)
    if value is None and fallback_key is not None:
        value = row.get(fallback_key)
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{key}_missing")
    return text


def _required_int(row: Mapping[str, Any], key: str) -> int:
    value = row.get(key)
    if value is None:
        raise ValueError(f"{key}_invalid")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key}_invalid") from exc


def _select_resource_status_rows_sql() -> str:
    return """
SELECT
    id::text AS resource_id,
    paper_id,
    version,
    metadata_path,
    source_hash,
    index_status,
    sql_status
FROM paper_resources
WHERE resource_status = 'active'
ORDER BY paper_id, version
""".strip()


def _update_resource_status_sql() -> str:
    return """
UPDATE paper_resources
SET
    index_status = %(target_index_status)s,
    sql_status = %(target_sql_status)s,
    updated_at = NOW()
WHERE id = %(resource_id)s::uuid
  AND paper_id = %(paper_id)s
  AND version = %(version)s
  AND metadata_path = %(metadata_path)s
  AND source_hash = %(source_hash)s
  AND index_status = %(current_index_status)s
  AND sql_status = %(current_sql_status)s
""".strip()


def _update_params(candidate: ResourceStatusSyncCandidate) -> dict[str, object]:
    return {
        "resource_id": candidate.resource_id,
        "paper_id": candidate.paper_id,
        "version": candidate.version,
        "metadata_path": candidate.metadata_path,
        "source_hash": candidate.source_hash,
        "current_index_status": candidate.current_index_status,
        "current_sql_status": candidate.current_sql_status,
        "target_index_status": candidate.target_index_status,
        "target_sql_status": candidate.target_sql_status,
    }
