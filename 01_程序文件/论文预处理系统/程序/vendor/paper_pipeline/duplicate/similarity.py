"""Lightweight deterministic similarity scoring for suspected duplicates."""

from __future__ import annotations

import re
from dataclasses import dataclass
from difflib import SequenceMatcher

from paper_pipeline.domain import PaperMetadata


@dataclass(frozen=True, slots=True)
class SimilarityScore:
    title_similarity: float = 0.0
    abstract_similarity: float = 0.0
    keyword_overlap: float = 0.0
    body_similarity: float = 0.0
    chapter_overlap: float = 0.0
    combined_score: float = 0.0


def calculate_similarity(
    left: PaperMetadata,
    right: PaperMetadata,
    *,
    left_body: str = "",
    right_body: str = "",
) -> SimilarityScore:
    """Score metadata and body similarity without external vector models."""

    title_similarity = _text_similarity(left.title, right.title)
    abstract_similarity = _text_similarity(left.abstract, right.abstract)
    keyword_overlap = _set_overlap(left.keywords, right.keywords)
    body_similarity = _text_similarity(left_body, right_body)
    chapter_overlap = _set_overlap(left.chapter_types, right.chapter_types)

    combined_score = (
        title_similarity * 0.25
        + abstract_similarity * 0.25
        + keyword_overlap * 0.20
        + body_similarity * 0.20
        + chapter_overlap * 0.10
    )
    return SimilarityScore(
        title_similarity=round(title_similarity, 4),
        abstract_similarity=round(abstract_similarity, 4),
        keyword_overlap=round(keyword_overlap, 4),
        body_similarity=round(body_similarity, 4),
        chapter_overlap=round(chapter_overlap, 4),
        combined_score=round(combined_score, 4),
    )


def _text_similarity(left: str, right: str) -> float:
    left_normalized = _normalize_text(left)
    right_normalized = _normalize_text(right)
    if not left_normalized and not right_normalized:
        return 0.0
    if not left_normalized or not right_normalized:
        return 0.0
    sequence_ratio = SequenceMatcher(None, left_normalized, right_normalized).ratio()
    gram_ratio = _jaccard(_char_grams(left_normalized), _char_grams(right_normalized))
    return max(sequence_ratio, gram_ratio)


def _set_overlap(left: list[str], right: list[str]) -> float:
    left_values = {_normalize_text(item) for item in left if _normalize_text(item)}
    right_values = {_normalize_text(item) for item in right if _normalize_text(item)}
    if not left_values and not right_values:
        return 0.0
    if not left_values or not right_values:
        return 0.0
    exact = _jaccard(left_values, right_values)
    fuzzy = _text_similarity(" ".join(sorted(left_values)), " ".join(sorted(right_values)))
    return max(exact, fuzzy)


def _char_grams(value: str) -> set[str]:
    if len(value) <= 1:
        return {value} if value else set()
    return {value[index : index + 2] for index in range(len(value) - 1)}


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 0.0
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def _normalize_text(value: str) -> str:
    return re.sub(r"[\W_]+", "", value.lower(), flags=re.UNICODE)
