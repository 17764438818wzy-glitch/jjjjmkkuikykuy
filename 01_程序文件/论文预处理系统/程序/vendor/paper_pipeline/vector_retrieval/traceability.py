"""Traceability helpers for material package entries."""

from __future__ import annotations

from paper_pipeline.vector_retrieval.models import MaterialItem, PaperChunk, TraceabilityInfo


class TraceabilityService:
    def build_traceability(self, chunks: list[PaperChunk]) -> TraceabilityInfo:
        return TraceabilityInfo(
            resource_ids=_unique([chunk.resource_id for chunk in chunks]),
            paper_ids=_unique([chunk.paper_id for chunk in chunks]),
            chunk_ids=_unique([chunk.chunk_id for chunk in chunks]),
        )

    def item_traceability(self, chunk: PaperChunk) -> dict[str, str]:
        return {
            "resource_id": chunk.resource_id,
            "paper_id": chunk.paper_id,
            "title": chunk.title,
            "chapter_title": chunk.chapter_title,
            "chapter_role": chunk.chapter_role,
            "chunk_id": chunk.chunk_id,
            "chunk_hash": chunk.chunk_hash,
            "text_hash": chunk.text_hash,
            "source_hash": chunk.source_hash,
        }

    def validate_items(self, items: list[MaterialItem]) -> bool:
        return all(item.traceability.get("chunk_id") for item in items)


def _unique(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value and value not in seen:
            result.append(value)
            seen.add(value)
    return result

