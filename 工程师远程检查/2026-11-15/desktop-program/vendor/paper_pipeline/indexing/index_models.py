"""Data models for local library index builds."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from paper_pipeline.domain import PaperMetadata


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True, slots=True)
class IndexablePaper:
    metadata: PaperMetadata
    metadata_path: Path
    md_path: Path

    @property
    def paper_key(self) -> str:
        return paper_key(self.metadata)


@dataclass(frozen=True, slots=True)
class IndexedPaper:
    paper_id: str
    accession_id: str
    version: int
    metadata_path: Path
    md_path: Path


@dataclass(frozen=True, slots=True)
class IndexSkippedPaper:
    paper_id: str
    reasons: list[str]
    metadata_path: Path | None = None
    md_path: Path | None = None


@dataclass(frozen=True, slots=True)
class IndexFailure:
    paper_id: str
    error: str
    metadata_path: Path | None = None
    md_path: Path | None = None


@dataclass(frozen=True, slots=True)
class Bm25IndexEntry:
    paper_id: str
    accession_id: str
    version: int
    title: str
    abstract: str
    keywords: list[str]
    category: str
    direction: str
    tier: str
    source_hash: str
    md_path: str
    metadata_path: str
    text_terms: list[str]
    updated_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "paper_id": self.paper_id,
            "accession_id": self.accession_id,
            "version": self.version,
            "title": self.title,
            "abstract": self.abstract,
            "keywords": list(self.keywords),
            "category": self.category,
            "direction": self.direction,
            "tier": self.tier,
            "source_hash": self.source_hash,
            "md_path": self.md_path,
            "metadata_path": self.metadata_path,
            "text_terms": list(self.text_terms),
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True, slots=True)
class VectorIndexEntry:
    paper_id: str
    accession_id: str
    version: int
    embedding_model: str
    embedding_dimension: int
    embedding_stub: bool
    text_hash: str
    updated_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "paper_id": self.paper_id,
            "accession_id": self.accession_id,
            "version": self.version,
            "embedding_model": self.embedding_model,
            "embedding_dimension": self.embedding_dimension,
            "embedding_stub": self.embedding_stub,
            "text_hash": self.text_hash,
            "updated_at": self.updated_at,
        }


@dataclass(slots=True)
class IndexBuildResult:
    run_id: str
    started_at: str
    completed_at: str = ""
    indexed: list[IndexedPaper] = field(default_factory=list)
    skipped: list[IndexSkippedPaper] = field(default_factory=list)
    failures: list[IndexFailure] = field(default_factory=list)

    @property
    def success_count(self) -> int:
        return len(self.indexed)

    @property
    def skipped_count(self) -> int:
        return len(self.skipped)

    @property
    def failure_count(self) -> int:
        return len(self.failures)

    @property
    def failed_count(self) -> int:
        return self.failure_count

    @property
    def ready_for_sql_preview(self) -> bool:
        return self.failure_count == 0 and self.success_count > 0

    def finish(self) -> None:
        self.completed_at = utc_now_iso()

    def manifest_record(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "skipped_count": self.skipped_count,
            "indexed": [
                {
                    "paper_id": item.paper_id,
                    "accession_id": item.accession_id,
                    "version": item.version,
                    "metadata_path": str(item.metadata_path),
                    "md_path": str(item.md_path),
                }
                for item in self.indexed
            ],
            "skipped": [
                {
                    "paper_id": item.paper_id,
                    "reasons": list(item.reasons),
                    "metadata_path": str(item.metadata_path) if item.metadata_path else "",
                    "md_path": str(item.md_path) if item.md_path else "",
                }
                for item in self.skipped
            ],
            "failures": [
                {
                    "paper_id": item.paper_id,
                    "error": item.error,
                    "metadata_path": str(item.metadata_path) if item.metadata_path else "",
                    "md_path": str(item.md_path) if item.md_path else "",
                }
                for item in self.failures
            ],
        }


def paper_key(metadata: PaperMetadata) -> str:
    return f"{metadata.id}_v{metadata.version}"
