"""Hash ledger persistence for read-only input scans."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, cast

from paper_pipeline.config import DEFAULT_RUNS_DIR
from paper_pipeline.domain.models import SourceFileRecord
from paper_pipeline.domain.serialization import (
    source_file_record_from_dict,
    source_file_record_to_dict,
)
from paper_pipeline.storage.scanner import ScanSummary

LEDGER_FILE_NAME = "hash_ledger.json"


@dataclass(slots=True)
class HashLedger:
    records: list[SourceFileRecord]
    summary: ScanSummary
    path: Path


def save_hash_ledger(
    *,
    records: list[SourceFileRecord],
    runs_dir: str | Path = DEFAULT_RUNS_DIR,
    summary: ScanSummary,
) -> Path:
    ledger_dir = Path(runs_dir) / summary.run_id
    ledger_dir.mkdir(parents=True, exist_ok=True)
    ledger_path = ledger_dir / LEDGER_FILE_NAME
    summary.ledger_path = str(ledger_path)

    payload = {
        "schema_version": 1,
        "run_id": summary.run_id,
        "created_at": _now_utc().isoformat(),
        "summary": _summary_to_dict(summary),
        "records": [source_file_record_to_dict(record) for record in records],
    }
    ledger_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return ledger_path


def load_hash_ledger(path: str | Path) -> HashLedger:
    ledger_path = Path(path)
    payload = json.loads(ledger_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("Hash ledger JSON must decode to an object")

    records = [
        source_file_record_from_dict(record)
        for record in cast(list[Mapping[str, Any]], payload["records"])
    ]
    summary = _summary_from_dict(cast(Mapping[str, Any], payload["summary"]))
    return HashLedger(records=records, summary=summary, path=ledger_path)


def _summary_to_dict(summary: ScanSummary) -> dict[str, Any]:
    return cast(dict[str, Any], _to_jsonable(asdict(summary)))


def _summary_from_dict(data: Mapping[str, Any]) -> ScanSummary:
    return ScanSummary(
        run_id=str(data["run_id"]),
        input_root=str(data["input_root"]),
        total_files=int(data["total_files"]),
        unique_files=int(data["unique_files"]),
        duplicate_files=int(data["duplicate_files"]),
        duplicate_hashes=int(data["duplicate_hashes"]),
        total_bytes=int(data["total_bytes"]),
        started_at=_datetime_from(data["started_at"]),
        completed_at=_datetime_from(data["completed_at"])
        if data.get("completed_at") is not None
        else None,
        ledger_path=str(data.get("ledger_path", "")),
    )


def _to_jsonable(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    return value


def _datetime_from(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)


def _now_utc() -> datetime:
    return datetime.now(UTC)
