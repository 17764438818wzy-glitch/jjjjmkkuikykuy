"""Stage0 rejected audit helpers."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from paper_pipeline.domain import Stage0Decision

STAGE0_REJECTED_AUDIT_FILENAME = "stage0_rejected.jsonl"


@dataclass(slots=True)
class Stage0RejectedRecord:
    run_id: str
    source_path: str
    source_hash: str
    decision: Stage0Decision
    reason: str
    confidence: float
    evidence: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


def append_stage0_rejected_record(
    record: Stage0RejectedRecord,
    audit_dir: str | Path,
) -> Path:
    audit_path = Path(audit_dir) / STAGE0_REJECTED_AUDIT_FILENAME
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    payload = asdict(record)
    payload["decision"] = record.decision.value
    payload["created_at"] = record.created_at.isoformat()
    with audit_path.open("a", encoding="utf-8") as file_obj:
        file_obj.write(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        file_obj.write("\n")
    return audit_path
