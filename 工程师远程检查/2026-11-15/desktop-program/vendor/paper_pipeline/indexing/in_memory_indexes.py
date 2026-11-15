"""In-memory index implementations for tests and local orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field

from paper_pipeline.indexing.index_models import Bm25IndexEntry, VectorIndexEntry


@dataclass(slots=True)
class InMemoryMetadataIndex:
    records: dict[str, dict[str, object]] = field(default_factory=dict)

    def upsert_metadata(self, paper_id: str, payload: dict[str, object]) -> None:
        self.records[paper_id] = dict(payload)


@dataclass(slots=True)
class InMemoryBm25Index:
    records: dict[str, Bm25IndexEntry] = field(default_factory=dict)

    def upsert(self, entry: Bm25IndexEntry) -> None:
        self.records[entry.paper_id] = entry


@dataclass(slots=True)
class InMemoryPaperVectorIndex:
    records: dict[str, VectorIndexEntry] = field(default_factory=dict)

    def upsert(self, entry: VectorIndexEntry) -> None:
        self.records[entry.paper_id] = entry


@dataclass(slots=True)
class InMemoryChapterVectorIndex:
    records: dict[tuple[str, str], dict[str, object]] = field(default_factory=dict)

    def upsert_chapter(self, paper_id: str, chapter_id: str, payload: dict[str, object]) -> None:
        self.records[(paper_id, chapter_id)] = dict(payload)


@dataclass(slots=True)
class InMemoryHierarchyIndex:
    records: dict[str, dict[str, object]] = field(default_factory=dict)

    def upsert_hierarchy(self, paper_id: str, payload: dict[str, object]) -> None:
        self.records[paper_id] = dict(payload)
