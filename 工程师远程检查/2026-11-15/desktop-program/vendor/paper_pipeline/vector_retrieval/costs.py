"""Embedding cost record helpers."""

from __future__ import annotations

from paper_pipeline.vector_retrieval.models import EmbeddingCostRecord, now_utc
from paper_pipeline.vector_retrieval.protocols import EmbeddingProvider


class CostRecordService:
    def build_embedding_cost_record(
        self,
        *,
        batch_id: str,
        paper_count: int,
        chunk_count: int,
        input_char_count: int,
        provider: EmbeddingProvider,
        success_count: int,
        failed_count: int,
    ) -> EmbeddingCostRecord:
        started_at = now_utc()
        estimated_tokens = provider.estimate_tokens(["x" * input_char_count])
        estimated_cost = provider.estimate_cost(estimated_tokens)
        return EmbeddingCostRecord(
            cost_record_id=f"cost-{batch_id}",
            batch_id=batch_id,
            paper_count=paper_count,
            chunk_count=chunk_count,
            input_char_count=input_char_count,
            estimated_tokens=estimated_tokens,
            provider=provider.provider_name,
            model=provider.model_name,
            success_count=success_count,
            failed_count=failed_count,
            started_at=started_at,
            completed_at=now_utc(),
            estimated_cost=estimated_cost,
        )

