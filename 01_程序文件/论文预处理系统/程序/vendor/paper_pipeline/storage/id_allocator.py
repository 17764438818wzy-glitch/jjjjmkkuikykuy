"""Formal accession id and version allocation for archived papers."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from paper_pipeline.domain import PaperIdentity
from paper_pipeline.domain.serialization import metadata_from_dict

_ACCESSION_RE = re.compile(r"ACC_(\d{6})")
FORMAL_LIBRARY_TIERS = {"gold", "silver", "pending", "rejected"}


class VersionedIdentity(Protocol):
    id: str
    version: int


@dataclass(slots=True)
class FormalPaperIdAllocator:
    library_dir: str | Path

    def allocate(self, *, source_hash: str) -> PaperIdentity:
        if not source_hash:
            raise ValueError("source_hash is required to allocate a formal id")

        records = list(_iter_metadata_records(Path(self.library_dir)))
        same_source_versions = [
            record
            for record in records
            if record.source_hash == source_hash and record.id.startswith("ACC_")
        ]
        if same_source_versions:
            current = max(same_source_versions, key=lambda record: record.version)
            return PaperIdentity(
                id=current.id,
                version=current.version + 1,
                source_hash=source_hash,
            )

        next_number = _next_accession_number(records)
        return PaperIdentity(
            id=f"ACC_{next_number:06d}",
            version=1,
            source_hash=source_hash,
        )


def format_formal_markdown_name(identity: VersionedIdentity) -> str:
    return f"{identity.id}_v{identity.version}.md"


def format_formal_metadata_name(identity: VersionedIdentity) -> str:
    del identity
    return "metadata.json"


def formal_stem(identity: VersionedIdentity) -> str:
    return f"{identity.id}_v{identity.version}"


def _iter_metadata_records(library_dir: Path):
    if not library_dir.exists():
        return
    for path in library_dir.rglob("metadata.json"):
        if not _is_formal_metadata_path(path, library_dir):
            continue
        try:
            decoded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(decoded, dict):
                yield metadata_from_dict(decoded)
        except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError):
            continue


def _is_formal_metadata_path(path: Path, library_dir: Path) -> bool:
    try:
        relative = path.relative_to(library_dir)
    except ValueError:
        return False
    return (
        len(relative.parts) >= 2
        and relative.parts[0] in FORMAL_LIBRARY_TIERS
        and path.parent.name.startswith("ACC_")
    )


def _next_accession_number(records: list[VersionedIdentity]) -> int:
    numbers: list[int] = []
    for record in records:
        match = _ACCESSION_RE.fullmatch(record.id)
        if match is not None:
            numbers.append(int(match.group(1)))
    return max(numbers, default=0) + 1
