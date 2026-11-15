"""Codex console helpers for vector retrieval MVP dry-runs."""

from __future__ import annotations

from paper_pipeline.vector_retrieval.fake_providers import (
    DeterministicFakeEmbeddingProvider,
    FakeLightweightReviewProvider,
)
from paper_pipeline.vector_retrieval.fake_repositories import (
    InMemoryRecordRepository,
    InMemoryResourceRepository,
    InMemoryVectorRepository,
)
from paper_pipeline.vector_retrieval.models import FormalResource
from paper_pipeline.vector_retrieval.service import VectorRetrievalMvpService


def build_fake_service(
    resources: list[FormalResource] | None = None,
) -> VectorRetrievalMvpService:
    return VectorRetrievalMvpService(
        resources=InMemoryResourceRepository(resources),
        vectors=InMemoryVectorRepository(),
        records=InMemoryRecordRepository(),
        embedding_provider=DeterministicFakeEmbeddingProvider(),
        review_provider=FakeLightweightReviewProvider(),
    )


def dangerous_operation_preview(operation: str) -> dict[str, object]:
    return {
        "operation": operation,
        "dry_run": True,
        "requires_explicit_confirmation": True,
        "will_execute": False,
    }

