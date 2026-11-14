"""Duplicate and suspected-duplicate decisions for task 10."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

from paper_pipeline.domain import DuplicateStatus, PaperMetadata, Tier
from paper_pipeline.duplicate.similarity import SimilarityScore, calculate_similarity

DEFAULT_SUSPECTED_DUPLICATE_THRESHOLD = 0.85


@dataclass(frozen=True, slots=True)
class DuplicateCandidate:
    metadata: PaperMetadata
    body: str = ""


@dataclass(frozen=True, slots=True)
class DuplicateDetectionResult:
    duplicate_status: DuplicateStatus
    similar_to: list[str] = field(default_factory=list)
    reason: str = ""
    evidence: list[str] = field(default_factory=list)
    score: SimilarityScore | None = None
    skip_processing: bool = False
    should_run_dity: bool = True
    should_create_formal_markdown: bool = True
    requires_pending_review: bool = False
    auto_merge: bool = False
    will_write_sql: bool = False
    will_build_index: bool = False


def detect_duplicate(
    incoming: DuplicateCandidate,
    existing_candidates: Sequence[DuplicateCandidate],
    *,
    suspected_threshold: float = DEFAULT_SUSPECTED_DUPLICATE_THRESHOLD,
) -> DuplicateDetectionResult:
    eligible_candidates = [
        candidate
        for candidate in existing_candidates
        if _eligible_for_duplicate_detection(candidate.metadata)
    ]

    exact_duplicate = _find_exact_duplicate(incoming.metadata, eligible_candidates)
    if exact_duplicate is not None:
        return DuplicateDetectionResult(
            duplicate_status=DuplicateStatus.EXACT_DUPLICATE,
            similar_to=[_metadata_reference(exact_duplicate.metadata)],
            reason="source_hash 完全相同，判定为 exact_duplicate 并自动跳过后续处理",
            evidence=["source_hash"],
            skip_processing=True,
            should_run_dity=False,
            should_create_formal_markdown=False,
            requires_pending_review=False,
            auto_merge=False,
            will_write_sql=False,
            will_build_index=False,
        )

    best_candidate: DuplicateCandidate | None = None
    best_score: SimilarityScore | None = None
    for candidate in eligible_candidates:
        if _same_source_hash(incoming.metadata, candidate.metadata):
            continue
        score = calculate_similarity(
            incoming.metadata,
            candidate.metadata,
            left_body=incoming.body,
            right_body=candidate.body,
        )
        if best_score is None or score.combined_score > best_score.combined_score:
            best_candidate = candidate
            best_score = score

    if best_candidate is not None and best_score is not None:
        if best_score.combined_score >= suspected_threshold:
            return DuplicateDetectionResult(
                duplicate_status=DuplicateStatus.SUSPECTED_DUPLICATE,
                similar_to=[_metadata_reference(best_candidate.metadata)],
                reason="source_hash 不同但元数据和正文高度相似，需进入 pending 复审",
                evidence=[
                    "title",
                    "abstract",
                    "keywords",
                    "chapter_types",
                    "body",
                ],
                score=best_score,
                skip_processing=False,
                should_run_dity=True,
                should_create_formal_markdown=True,
                requires_pending_review=True,
                auto_merge=False,
                will_write_sql=False,
                will_build_index=False,
            )
        return DuplicateDetectionResult(
            duplicate_status=DuplicateStatus.UNIQUE,
            reason="source_hash 不同且相似度低于阈值，判定为 unique",
            evidence=["similarity_score"],
            score=best_score,
            skip_processing=False,
            should_run_dity=True,
            should_create_formal_markdown=True,
            requires_pending_review=False,
            auto_merge=False,
            will_write_sql=False,
            will_build_index=False,
        )

    return DuplicateDetectionResult(
        duplicate_status=DuplicateStatus.UNIQUE,
        reason="未找到可参与重复判断的候选记录，判定为 unique",
        evidence=[],
        skip_processing=False,
        should_run_dity=True,
        should_create_formal_markdown=True,
        requires_pending_review=False,
        auto_merge=False,
        will_write_sql=False,
        will_build_index=False,
    )


def apply_duplicate_detection_to_metadata(
    metadata: PaperMetadata,
    result: DuplicateDetectionResult,
) -> PaperMetadata:
    metadata.duplicate_status = result.duplicate_status
    metadata.similar_to = list(result.similar_to)
    if result.duplicate_status is DuplicateStatus.SUSPECTED_DUPLICATE:
        metadata.tier = Tier.PENDING
    return metadata


def _find_exact_duplicate(
    incoming: PaperMetadata,
    candidates: Sequence[DuplicateCandidate],
) -> DuplicateCandidate | None:
    for candidate in candidates:
        if _same_source_hash(incoming, candidate.metadata):
            return candidate
    return None


def _same_source_hash(left: PaperMetadata, right: PaperMetadata) -> bool:
    return bool(left.source_hash) and left.source_hash == right.source_hash


def _eligible_for_duplicate_detection(metadata: PaperMetadata) -> bool:
    tier = metadata.tier if isinstance(metadata.tier, Tier) else Tier(metadata.tier)
    return tier is not Tier.STAGE0_REJECTED


def _metadata_reference(metadata: PaperMetadata) -> str:
    if metadata.id:
        return metadata.id
    return f"{metadata.source_hash}:v{metadata.version}"
