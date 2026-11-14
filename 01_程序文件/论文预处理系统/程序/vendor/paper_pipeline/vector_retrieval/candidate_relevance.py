"""Domain-neutral candidate relevance scoring."""

from __future__ import annotations

import re

from paper_pipeline.vector_retrieval.models import CandidateChunk, RetrievalRequest


_ADMIN_PATTERNS = (
    r"\[number\]",
    r"\?+",
    r"材料包",
    r"姓\s*名.*$",
    r"学\s*号.*$",
)
_LOW_SIGNAL_TERMS = {
    "调查研究",
    "现状研究",
    "研究",
    "分析",
    "对策",
    "影响",
    "发展",
}


class GenericCandidateRelevanceScorer:
    """Scores topic/title closeness without domain-specific vocabulary."""

    def score(self, request: RetrievalRequest, candidate: CandidateChunk) -> float:
        query_title = _clean_text(request.query_title)
        candidate_title = _clean_text(candidate.chunk.title)
        candidate_text = _clean_text(candidate.chunk.chunk_text)
        title_similarity = _ngram_dice(query_title, candidate_title)
        text_similarity = _ngram_dice(query_title, candidate_text)
        score = title_similarity * 0.26 + text_similarity * 0.08
        if query_title and candidate_title and (
            query_title in candidate_title or candidate_title in query_title
        ):
            score += 0.08
        return min(score, 0.38)


def _clean_text(value: str) -> str:
    cleaned = value.strip().lower()
    for pattern in _ADMIN_PATTERNS:
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)
    for term in _LOW_SIGNAL_TERMS:
        cleaned = cleaned.replace(term, "")
    return re.sub(r"\s+", "", cleaned)


def _ngrams(value: str, *, size: int = 2) -> set[str]:
    if len(value) < size:
        return {value} if value else set()
    return {value[index : index + size] for index in range(len(value) - size + 1)}


def _ngram_dice(left: str, right: str) -> float:
    left_grams = _ngrams(left)
    right_grams = _ngrams(right)
    if not left_grams or not right_grams:
        return 0.0
    overlap = len(left_grams & right_grams)
    return (2.0 * overlap) / (len(left_grams) + len(right_grams))
