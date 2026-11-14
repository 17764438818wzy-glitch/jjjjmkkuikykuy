"""Semantic vector recall."""

from __future__ import annotations

from paper_pipeline.vector_retrieval.models import CandidateChunk, RetrievalRequest, SqlFilter
from paper_pipeline.vector_retrieval.protocols import EmbeddingProvider, VectorRepository


class SemanticRecallService:
    def __init__(
        self,
        vector_repository: VectorRepository,
        embedding_provider: EmbeddingProvider,
    ) -> None:
        self.vector_repository = vector_repository
        self.embedding_provider = embedding_provider

    def recall(
        self,
        request: RetrievalRequest,
        filters: SqlFilter,
        *,
        top_k: int = 120,
    ) -> list[CandidateChunk]:
        query_vector = self.embedding_provider.embed_texts([request.query_text])[0]
        return self.vector_repository.search_chunks(query_vector, filters, top_k=top_k)

