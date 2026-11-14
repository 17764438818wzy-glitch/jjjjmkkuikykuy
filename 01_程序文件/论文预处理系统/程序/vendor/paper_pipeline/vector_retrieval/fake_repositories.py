"""In-memory repositories for dry-run and tests."""

from __future__ import annotations

from math import sqrt

from paper_pipeline.vector_retrieval.models import (
    CandidateChunk,
    ChunkEmbedding,
    EmbeddingCostRecord,
    FormalResource,
    PaperChunk,
    RetrievalRecord,
    SqlFilter,
    VectorProcessRecord,
)


class InMemoryResourceRepository:
    def __init__(self, resources: list[FormalResource] | None = None) -> None:
        self.resources = list(resources or [])

    def list_vector_eligible_resources(self) -> list[FormalResource]:
        return [resource for resource in self.resources if resource.is_vector_eligible()]

    def get_resource(self, resource_id: str) -> FormalResource | None:
        for resource in self.resources:
            if resource.resource_id == resource_id:
                return resource
        return None


class InMemoryVectorRepository:
    def __init__(self) -> None:
        self.chunks: dict[str, PaperChunk] = {}
        self.embeddings: dict[str, ChunkEmbedding] = {}

    def save_chunks(self, chunks: list[PaperChunk]) -> None:
        for chunk in chunks:
            self.chunks[chunk.chunk_id] = chunk

    def save_embeddings(self, embeddings: list[ChunkEmbedding]) -> None:
        for embedding in embeddings:
            self.embeddings[embedding.chunk_id] = embedding

    def search_chunks(
        self,
        query_vector: list[float],
        filters: SqlFilter,
        *,
        top_k: int,
    ) -> list[CandidateChunk]:
        candidates: list[CandidateChunk] = []
        for chunk_id, chunk in self.chunks.items():
            embedding = self.embeddings.get(chunk_id)
            if embedding is None or not _matches_filter(chunk, filters):
                continue
            score = _cosine(query_vector, embedding.embedding_vector)
            candidates.append(CandidateChunk(chunk=chunk, similarity_score=score))
        candidates.sort(key=lambda item: item.similarity_score, reverse=True)
        return candidates[:top_k]


class InMemoryRecordRepository:
    def __init__(self) -> None:
        self.retrieval_records: list[RetrievalRecord] = []
        self.cost_records: list[EmbeddingCostRecord] = []
        self.process_records: list[VectorProcessRecord] = []

    def save_retrieval_record(self, record: RetrievalRecord) -> None:
        self.retrieval_records.append(record)

    def save_cost_record(self, record: EmbeddingCostRecord) -> None:
        self.cost_records.append(record)

    def save_process_record(self, record: VectorProcessRecord) -> None:
        self.process_records.append(record)


def _matches_filter(chunk: PaperChunk, filters: SqlFilter) -> bool:
    if filters.major and filters.major not in {chunk.major, chunk.discipline}:
        return False
    if filters.direction and filters.direction != chunk.direction:
        return False
    if filters.thesis_type and filters.thesis_type != chunk.thesis_type:
        return False
    if filters.tier_preference and filters.tier_preference != chunk.tier:
        return False
    if filters.research_method_preference:
        if filters.research_method_preference not in chunk.research_method_tags:
            return False
    if filters.content_roles and chunk.content_role not in filters.content_roles:
        return False
    if filters.needs_real_data is not None:
        return chunk.needs_real_data is filters.needs_real_data
    return True


def _cosine(left: list[float], right: list[float]) -> float:
    length = min(len(left), len(right))
    if length == 0:
        return 0.0
    dot = sum(left[index] * right[index] for index in range(length))
    left_norm = sqrt(sum(left[index] ** 2 for index in range(length)))
    right_norm = sqrt(sum(right[index] ** 2 for index in range(length)))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return dot / (left_norm * right_norm)

