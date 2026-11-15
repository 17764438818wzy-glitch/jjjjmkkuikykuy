"""Embedding generation orchestration."""

from __future__ import annotations

from hashlib import sha256

from paper_pipeline.vector_retrieval.models import ChunkEmbedding, PaperChunk
from paper_pipeline.vector_retrieval.protocols import EmbeddingProvider


class EmbeddingService:
    def __init__(self, provider: EmbeddingProvider) -> None:
        self.provider = provider

    def embed_chunks(self, chunks: list[PaperChunk]) -> list[ChunkEmbedding]:
        texts = [chunk.chunk_text for chunk in chunks]
        vectors = self.provider.embed_texts(texts)
        token_count = self.provider.estimate_tokens(texts)
        cost = self.provider.estimate_cost(token_count)
        per_chunk_tokens = token_count // len(chunks) if chunks else 0
        per_chunk_cost = cost / len(chunks) if chunks else 0.0
        return [
            ChunkEmbedding(
                embedding_id=f"{chunk.chunk_id}:{self.provider.model_name}",
                chunk_id=chunk.chunk_id,
                provider=self.provider.provider_name,
                model=self.provider.model_name,
                dimension=self.provider.dimension,
                embedding_vector=vector,
                input_text_hash=_hash_text(chunk.chunk_text),
                estimated_tokens=per_chunk_tokens,
                estimated_cost=per_chunk_cost,
            )
            for chunk, vector in zip(chunks, vectors, strict=True)
        ]


def _hash_text(text: str) -> str:
    return sha256(text.encode("utf-8")).hexdigest()

