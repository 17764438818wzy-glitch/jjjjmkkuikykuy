"""Read-only recursive scanning for raw source files."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable

from paper_pipeline.config import DEFAULT_INPUT_RAW
from paper_pipeline.domain.enums import DuplicateStatus
from paper_pipeline.domain.models import SourceFileRecord

DUPLICATE_SKIP_REASON = "duplicate_skip"
_HASH_CHUNK_SIZE = 1024 * 1024


def _now_utc() -> datetime:
    return datetime.now(UTC)


@dataclass(slots=True)
class ScanSummary:
    run_id: str
    input_root: str
    total_files: int = 0
    unique_files: int = 0
    duplicate_files: int = 0
    duplicate_hashes: int = 0
    total_bytes: int = 0
    started_at: datetime = field(default_factory=_now_utc)
    completed_at: datetime | None = None
    ledger_path: str = ""


@dataclass(slots=True)
class ScanResult:
    records: list[SourceFileRecord]
    summary: ScanSummary


def scan_input_raw(
    input_dir: str | Path = DEFAULT_INPUT_RAW,
    *,
    run_id: str | None = None,
) -> ScanResult:
    """Recursively read input files and mark exact hash duplicates."""
    root = Path(input_dir)
    scan_run_id = run_id or _default_run_id()
    started_at = _now_utc()

    records = [_record_for_file(path) for path in _iter_files(root)]
    _mark_duplicates(records)

    duplicate_hashes = {
        record.source_hash
        for record in records
        if len(record.all_sources_for_hash) > 1
    }
    summary = ScanSummary(
        run_id=scan_run_id,
        input_root=str(root),
        total_files=len(records),
        unique_files=sum(1 for record in records if not record.is_duplicate),
        duplicate_files=sum(1 for record in records if record.is_duplicate),
        duplicate_hashes=len(duplicate_hashes),
        total_bytes=sum(record.source_size for record in records),
        started_at=started_at,
        completed_at=_now_utc(),
    )
    return ScanResult(records=records, summary=summary)


def calculate_file_hash(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(_HASH_CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def format_scan_summary(summary: ScanSummary) -> str:
    return (
        f"扫描摘要: run_id={summary.run_id}, total_files={summary.total_files}, "
        f"unique_files={summary.unique_files}, "
        f"duplicate_files={summary.duplicate_files}, "
        f"duplicate_hashes={summary.duplicate_hashes}, "
        f"total_bytes={summary.total_bytes}"
    )


def _iter_files(root: Path) -> Iterable[Path]:
    return sorted(
        (path for path in root.rglob("*") if path.is_file()),
        key=lambda path: path.relative_to(root).as_posix(),
    )


def _record_for_file(path: Path) -> SourceFileRecord:
    stat_result = path.stat()
    source_path = str(path)
    return SourceFileRecord(
        path=source_path,
        source_path=source_path,
        source_name=path.name,
        source_size=stat_result.st_size,
        size_bytes=stat_result.st_size,
        source_hash=calculate_file_hash(path),
        mtime=stat_result.st_mtime,
        first_seen_at=_now_utc(),
    )


def _mark_duplicates(records: list[SourceFileRecord]) -> None:
    records_by_hash: dict[str, list[SourceFileRecord]] = {}
    for record in records:
        records_by_hash.setdefault(record.source_hash, []).append(record)

    for hash_records in records_by_hash.values():
        all_sources = [record.source_path for record in hash_records]
        canonical = hash_records[0]
        for index, record in enumerate(hash_records):
            record.all_sources_for_hash = list(all_sources)
            if index == 0:
                continue
            record.is_duplicate = True
            record.duplicate_status = DuplicateStatus.EXACT_DUPLICATE
            record.duplicate_of = canonical.source_path
            record.skip_reason = DUPLICATE_SKIP_REASON


def _default_run_id() -> str:
    return f"scan-{_now_utc().strftime('%Y%m%dT%H%M%S%fZ')}"
