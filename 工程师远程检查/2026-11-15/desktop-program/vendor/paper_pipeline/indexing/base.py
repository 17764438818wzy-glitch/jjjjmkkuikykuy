"""Index interface protocols for task 11."""

from __future__ import annotations

from typing import Protocol

from paper_pipeline.indexing.index_models import Bm25IndexEntry, VectorIndexEntry


class MetadataIndex(Protocol):
    def upsert_metadata(self, paper_id: str, payload: dict[str, object]) -> None:
        """Store metadata-level index content for one paper."""


class Bm25Index(Protocol):
    def upsert(self, entry: Bm25IndexEntry) -> None:
        """Store one BM25/full-text entry."""


class PaperVectorIndex(Protocol):
    def upsert(self, entry: VectorIndexEntry) -> None:
        """Store one paper-level vector entry."""


class ChapterVectorIndex(Protocol):
    def upsert_chapter(self, paper_id: str, chapter_id: str, payload: dict[str, object]) -> None:
        """Store one chapter-level vector payload."""


class HierarchyIndex(Protocol):
    def upsert_hierarchy(self, paper_id: str, payload: dict[str, object]) -> None:
        """Store hierarchy metadata for one paper."""
