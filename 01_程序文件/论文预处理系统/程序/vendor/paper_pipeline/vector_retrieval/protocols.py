"""Protocol interfaces for vector retrieval services."""

from __future__ import annotations

from typing import Protocol

from paper_pipeline.vector_retrieval.models import (
    CandidateChunk,
    ChunkEmbedding,
    EmbeddingCostRecord,
    FormalResource,
    PaperChunk,
    RetrievalRecord,
    ReviewRequest,
    ReviewedMaterial,
    SqlFilter,
    VectorProcessRecord,
)


class ResourceRepository(Protocol):
    def list_vector_eligible_resources(self) -> list[FormalResource]:
        ...

    def get_resource(self, resource_id: str) -> FormalResource | None:
        ...


class VectorRepository(Protocol):
    def save_chunks(self, chunks: list[PaperChunk]) -> None:
        ...

    def save_embeddings(self, embeddings: list[ChunkEmbedding]) -> None:
        ...

    def search_chunks(
        self,
        query_vector: list[float],
        filters: SqlFilter,
        *,
        top_k: int,
    ) -> list[CandidateChunk]:
        ...


class RecordRepository(Protocol):
    def save_retrieval_record(self, record: RetrievalRecord) -> None:
        ...

    def save_cost_record(self, record: EmbeddingCostRecord) -> None:
        ...

    def save_process_record(self, record: VectorProcessRecord) -> None:
        ...


class EmbeddingProvider(Protocol):
    provider_name: str
    model_name: str
    dimension: int

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        ...

    def estimate_tokens(self, texts: list[str]) -> int:
        ...

    def estimate_cost(self, token_count: int) -> float:
        ...


class LightweightReviewProvider(Protocol):
    def review(self, request: ReviewRequest) -> list[ReviewedMaterial]:
        ...

