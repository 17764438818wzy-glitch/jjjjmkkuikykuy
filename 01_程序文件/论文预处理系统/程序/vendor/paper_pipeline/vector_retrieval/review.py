"""Lightweight review orchestration."""

from __future__ import annotations

from paper_pipeline.vector_retrieval.models import (
    CandidateChunk,
    RetrievalRequest,
    ReviewRequest,
    ReviewedMaterial,
)
from paper_pipeline.vector_retrieval.protocols import LightweightReviewProvider


class LightweightReviewService:
    def __init__(self, provider: LightweightReviewProvider) -> None:
        self.provider = provider

    def review(
        self,
        request: RetrievalRequest,
        candidates: list[CandidateChunk],
    ) -> list[ReviewedMaterial]:
        return self.provider.review(
            ReviewRequest(retrieval_request=request, candidates=candidates),
        )

