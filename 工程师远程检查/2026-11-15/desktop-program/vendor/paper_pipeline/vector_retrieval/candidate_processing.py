"""Candidate material processing before lightweight review."""

from __future__ import annotations

from paper_pipeline.vector_retrieval.candidate_relevance import GenericCandidateRelevanceScorer
from paper_pipeline.vector_retrieval.models import CandidateChunk, RetrievalRequest
from paper_pipeline.vector_retrieval.sports_specificity import SportsSpecificityScorer


TIER_WEIGHTS = {"gold": 1.15, "silver": 1.0}
ROLE_BONUS = {
    "background": 0.04,
    "theory": 0.03,
    "method": 0.06,
    "data_result": 0.08,
    "problem_analysis": 0.03,
    "strategy": 0.04,
    "conclusion": 0.02,
}


class CandidateProcessor:
    def __init__(
        self,
        *,
        generic_scorer: GenericCandidateRelevanceScorer | None = None,
        sports_scorer: SportsSpecificityScorer | None = None,
    ) -> None:
        self.generic_scorer = generic_scorer or GenericCandidateRelevanceScorer()
        self.sports_scorer = sports_scorer or SportsSpecificityScorer()

    def process(
        self,
        candidates: list[CandidateChunk],
        *,
        request: RetrievalRequest | None = None,
        limit: int = 60,
    ) -> list[CandidateChunk]:
        unique = _dedupe(candidates)
        source_counts: dict[str, int] = {}
        processed: list[CandidateChunk] = []
        for candidate in unique:
            source_count = source_counts.get(candidate.chunk.resource_id, 0)
            if source_count >= 8:
                continue
            source_counts[candidate.chunk.resource_id] = source_count + 1
            candidate.role_group = candidate.chunk.content_role
            candidate.source_rank = source_count + 1
            candidate.weighted_score = self._weighted_score(candidate, request)
            processed.append(candidate)
        processed.sort(key=lambda item: item.weighted_score, reverse=True)
        return processed[:limit]

    def _weighted_score(
        self,
        candidate: CandidateChunk,
        request: RetrievalRequest | None,
    ) -> float:
        score = _base_weighted_score(candidate)
        if request is None:
            return score
        return (
            score
            + self.generic_scorer.score(request, candidate)
            + self.sports_scorer.score(request, candidate)
        )


def _dedupe(candidates: list[CandidateChunk]) -> list[CandidateChunk]:
    seen: set[str] = set()
    result: list[CandidateChunk] = []
    for candidate in candidates:
        if candidate.chunk.chunk_id in seen:
            continue
        seen.add(candidate.chunk.chunk_id)
        result.append(candidate)
    return result


def _base_weighted_score(candidate: CandidateChunk) -> float:
    tier_weight = TIER_WEIGHTS.get(candidate.chunk.tier, 0.9)
    role_bonus = ROLE_BONUS.get(candidate.chunk.content_role, 0.0)
    return candidate.similarity_score * tier_weight + role_bonus
