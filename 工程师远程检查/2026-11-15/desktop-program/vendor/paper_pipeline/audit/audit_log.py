"""Append-only JSONL audit logging for pipeline events."""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any


class AuditEventType(StrEnum):
    PLAN_CONFIRMED = "plan_confirmed"
    BATCH_STARTED = "batch_started"
    FILE_PENDING = "file_pending"
    FILE_FAILED = "file_failed"
    STAGE0_REJECTED = "stage0_rejected"
    METADATA_WRITTEN = "metadata_written"
    TIER_MOVED = "tier_moved"
    INDEX_BUILT = "index_built"
    INDEX_FAILED = "index_failed"
    SQL_PREVIEW_GENERATED = "sql_preview_generated"
    SQL_UPLOAD_CONFIRMED = "sql_upload_confirmed"
    SQL_UPLOADED = "sql_uploaded"
    SQL_UPLOAD_FAILED = "sql_upload_failed"
    PENDING_REVIEW_REQUIRED = "pending_review_required"
    RUN_COMPLETED = "run_completed"
    RUN_FAILED = "run_failed"
    RUN_ABANDONED = "run_abandoned"
    REPORT_GENERATED = "report_generated"


@dataclass(slots=True)
class AuditEvent:
    run_id: str
    event_type: AuditEventType | str
    actor: str
    action: str
    target_type: str
    target_id: str
    status: str
    message: str = ""
    evidence: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    risk_flags: list[str] = field(default_factory=list)
    paper_id: str = ""
    operator: str = ""
    timestamp: str = ""
    before: dict[str, Any] = field(default_factory=dict)
    after: dict[str, Any] = field(default_factory=dict)
    reason: str = ""
    event_id: str = field(default_factory=lambda: f"audit-{uuid.uuid4().hex}")
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def __post_init__(self) -> None:
        if not self.run_id:
            raise ValueError("run_id is required for audit events")
        if isinstance(self.event_type, str):
            self.event_type = AuditEventType(self.event_type)
        if not self.operator:
            self.operator = self.actor
        if not self.paper_id and self.target_type == "paper":
            self.paper_id = self.target_id
        if not self.timestamp:
            self.timestamp = self.created_at

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        event_type = (
            self.event_type
            if isinstance(self.event_type, AuditEventType)
            else AuditEventType(self.event_type)
        )
        payload["event_type"] = event_type.value
        return payload


@dataclass(slots=True)
class AuditLogWriter:
    path: str | Path

    def append(self, event: AuditEvent) -> AuditEvent:
        destination = Path(self.path)
        _ensure_not_input_raw(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event.to_dict(), ensure_ascii=False, sort_keys=True))
            handle.write("\n")
        return event


def _ensure_not_input_raw(path: Path) -> None:
    normalized = str(path).lower().replace("/", "\\")
    if "input_raw" in path.parts or "\\input_raw\\" in normalized:
        raise ValueError("audit log must not be written under input_raw")
