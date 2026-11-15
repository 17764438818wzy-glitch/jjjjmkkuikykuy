"""Service orchestration for optional AI review of pending papers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from paper_pipeline.domain import DuplicateStatus, PaperMetadata, ReviewHistoryItem, Tier
from paper_pipeline.domain.serialization import metadata_from_json, metadata_to_dict
from paper_pipeline.review.ai_review_client import (
    AIReviewConfig,
    AIReviewInput,
    AIReviewResult,
    AIReviewSchemaError,
    parse_ai_review_result,
)
from paper_pipeline.review.review_service import (
    ReviewApplicationResult,
)
from paper_pipeline.storage.metadata_writer import MetadataWriter
from paper_pipeline.storage.tier_mover import TierMover


class AIReviewClient(Protocol):
    @property
    def is_configured(self) -> bool: ...

    def review(self, request: AIReviewInput) -> str: ...


@dataclass(frozen=True, slots=True)
class AIReviewRunResult:
    status: str
    metadata_path: Path
    markdown_path: Path
    suggestion: AIReviewResult | None = None
    applied: bool = False
    review_result: ReviewApplicationResult | None = None
    error: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "metadata_path": str(self.metadata_path),
            "markdown_path": str(self.markdown_path),
            "suggestion": (
                self.suggestion.to_dict() if self.suggestion is not None else None
            ),
            "applied": self.applied,
            "review_result": (
                {
                    "metadata_path": str(self.review_result.metadata_path),
                    "old_metadata_path": str(self.review_result.old_metadata_path),
                    "old_tier": self.review_result.old_tier.value,
                    "new_tier": self.review_result.new_tier.value,
                    "sql_upload_triggered": self.review_result.sql_upload_triggered,
                    "index_build_triggered": self.review_result.index_build_triggered,
                    "index_update_required": self.review_result.index_update_required,
                    "notes": list(self.review_result.notes),
                }
                if self.review_result is not None
                else None
            ),
            "error": self.error,
        }


@dataclass(slots=True)
class AIReviewService:
    library_dir: str | Path
    client: AIReviewClient
    config: AIReviewConfig
    min_confidence: float = 0.7

    def review_one(
        self,
        metadata_path: str | Path,
        markdown_path: str | Path,
        *,
        confirmed: bool = False,
    ) -> AIReviewRunResult:
        metadata_file = Path(metadata_path)
        markdown_file = Path(markdown_path)

        if not self.config.enabled:
            return AIReviewRunResult("disabled", metadata_file, markdown_file)
        metadata = metadata_from_json(metadata_file.read_text(encoding="utf-8"))
        if not self.client.is_configured:
            routed_metadata_path, routed_markdown_path = _route_pending_initial_review(
                library_dir=self.library_dir,
                metadata_path=metadata_file,
                markdown_path=markdown_file,
                metadata=metadata,
                status="deepseek_call_failed",
                reason="deepseek_call_failed: AI review client is not configured",
            )
            return AIReviewRunResult(
                "deepseek_call_failed",
                routed_metadata_path,
                routed_markdown_path,
                applied=True,
                error="AI review client is not configured",
            )

        request = AIReviewInput(
            paper_id=metadata.id,
            run_id=metadata.run_id,
            metadata=metadata_to_dict(metadata),
            markdown=markdown_file.read_text(encoding="utf-8"),
        )

        try:
            raw = self.client.review(request)
            suggestion = parse_ai_review_result(raw, fallback_model=self.config.model)
        except AIReviewSchemaError as exc:
            routed_metadata_path, routed_markdown_path = _route_pending_initial_review(
                library_dir=self.library_dir,
                metadata_path=metadata_file,
                markdown_path=markdown_file,
                metadata=metadata,
                status="deepseek_invalid_response",
                reason=f"deepseek_invalid_response: {exc}",
            )
            return AIReviewRunResult(
                "deepseek_invalid_response",
                routed_metadata_path,
                routed_markdown_path,
                applied=True,
                error=str(exc),
            )
        except Exception as exc:  # noqa: BLE001 - service reports client failures.
            routed_metadata_path, routed_markdown_path = _route_pending_initial_review(
                library_dir=self.library_dir,
                metadata_path=metadata_file,
                markdown_path=markdown_file,
                metadata=metadata,
                status="deepseek_call_failed",
                reason=f"deepseek_call_failed: {exc}",
            )
            return AIReviewRunResult(
                "deepseek_call_failed",
                routed_metadata_path,
                routed_markdown_path,
                applied=True,
                error=str(exc),
            )

        if self.config.dry_run:
            return AIReviewRunResult(
                "suggested",
                metadata_file,
                markdown_file,
                suggestion=suggestion,
            )
        if _needs_human_review(suggestion, min_confidence=self.min_confidence):
            routed_metadata_path, routed_markdown_path = _route_pending_initial_review(
                library_dir=self.library_dir,
                metadata_path=metadata_file,
                markdown_path=markdown_file,
                metadata=metadata,
                status="deepseek_uncertain",
                reason=f"deepseek_uncertain: {suggestion.reason}",
                suggestion=suggestion,
            )
            return AIReviewRunResult(
                "deepseek_uncertain",
                routed_metadata_path,
                routed_markdown_path,
                suggestion=suggestion,
                applied=True,
            )

        routed_metadata_path, routed_markdown_path = _route_initial_review(
            library_dir=self.library_dir,
            metadata_path=metadata_file,
            markdown_path=markdown_file,
            metadata=metadata,
            suggestion=suggestion,
        )
        return AIReviewRunResult(
            "routed",
            routed_metadata_path,
            routed_markdown_path,
            suggestion=suggestion,
            applied=True,
        )


def _needs_human_review(
    suggestion: AIReviewResult,
    *,
    min_confidence: float,
) -> bool:
    return (
        suggestion.blocking
        or bool(suggestion.risk_flags)
        or suggestion.confidence < min_confidence
    )


def _route_initial_review(
    *,
    library_dir: str | Path,
    metadata_path: Path,
    markdown_path: Path,
    metadata: PaperMetadata,
    suggestion: AIReviewResult,
) -> tuple[Path, Path]:
    target_tier = Tier(suggestion.target_tier)
    if target_tier is Tier.GOLD:
        return _write_initial_review_route(
            library_dir=library_dir,
            metadata_path=metadata_path,
            markdown_path=markdown_path,
            metadata=metadata,
            new_tier=Tier.PENDING,
            quality_tier_suggestion=Tier.GOLD.value,
            gold_candidate=True,
            needs_codex_review=True,
            reason=suggestion.reason,
            decision=suggestion.decision,
            evidence=suggestion.evidence,
            reviewer=suggestion.reviewer,
            model=suggestion.model,
        )
    if target_tier is Tier.SILVER:
        return _write_initial_review_route(
            library_dir=library_dir,
            metadata_path=metadata_path,
            markdown_path=markdown_path,
            metadata=metadata,
            new_tier=Tier.PENDING,
            quality_tier_suggestion=Tier.SILVER.value,
            gold_candidate=False,
            needs_codex_review=True,
            reason=suggestion.reason,
            decision=suggestion.decision,
            evidence=suggestion.evidence,
            reviewer=suggestion.reviewer,
            model=suggestion.model,
        )
    if target_tier is Tier.REJECTED:
        return _write_initial_review_route(
            library_dir=library_dir,
            metadata_path=metadata_path,
            markdown_path=markdown_path,
            metadata=metadata,
            new_tier=Tier.REJECTED,
            quality_tier_suggestion=Tier.REJECTED.value,
            gold_candidate=False,
            needs_codex_review=False,
            reason=suggestion.reason,
            decision=suggestion.decision,
            evidence=suggestion.evidence,
            reviewer=suggestion.reviewer,
            model=suggestion.model,
        )
    return _route_pending_initial_review(
        library_dir=library_dir,
        metadata_path=metadata_path,
        markdown_path=markdown_path,
        metadata=metadata,
        status="deepseek_uncertain",
        reason=f"deepseek_uncertain: {suggestion.reason}",
        suggestion=suggestion,
    )


def _route_pending_initial_review(
    *,
    library_dir: str | Path,
    metadata_path: Path,
    markdown_path: Path,
    metadata: PaperMetadata,
    status: str,
    reason: str,
    suggestion: AIReviewResult | None = None,
) -> tuple[Path, Path]:
    return _write_initial_review_route(
        library_dir=library_dir,
        metadata_path=metadata_path,
        markdown_path=markdown_path,
        metadata=metadata,
        new_tier=Tier.PENDING,
        quality_tier_suggestion=Tier.PENDING.value,
        gold_candidate=False,
        needs_codex_review=True,
        reason=reason,
        decision=status,
        evidence=[] if suggestion is None else suggestion.evidence,
        reviewer="deepseek" if suggestion is None else suggestion.reviewer,
        model="" if suggestion is None else suggestion.model,
        decision_source=status,
    )


def _write_initial_review_route(
    *,
    library_dir: str | Path,
    metadata_path: Path,
    markdown_path: Path,
    metadata: PaperMetadata,
    new_tier: Tier,
    quality_tier_suggestion: str,
    gold_candidate: bool,
    needs_codex_review: bool,
    reason: str,
    decision: str,
    evidence: list[str],
    reviewer: str,
    model: str,
    decision_source: str = "deepseek_initial_review",
) -> tuple[Path, Path]:
    review_reason = reason.strip()
    if new_tier is Tier.REJECTED and not review_reason:
        raise ValueError("reason is required when DeepSeek initial review rejects a paper")

    old_tier = _coerce_tier(metadata.tier)
    destination_metadata_path = metadata_path
    destination_markdown_path = markdown_path
    if new_tier != old_tier:
        moved = TierMover(library_dir).move_current_version(
            paper_id=metadata.id,
            version=metadata.version,
            new_tier=new_tier,
            category=metadata.category,
            direction=metadata.direction,
            reviewer=reviewer,
            reason=review_reason,
        )
        destination_metadata_path = moved.metadata_path
        destination_markdown_path = moved.markdown_path
        metadata = metadata_from_json(destination_metadata_path.read_text(encoding="utf-8"))

    metadata.tier = new_tier
    metadata.quality_tier_suggestion = quality_tier_suggestion
    metadata.gold_candidate = gold_candidate
    metadata.needs_codex_review = needs_codex_review
    metadata.tier_decision_source = decision_source
    metadata.quality_reason = review_reason
    metadata.add_review_history(
        ReviewHistoryItem(
            reviewer=reviewer,
            action="deepseek_initial_review_routed",
            comment=f"{old_tier.value} -> {new_tier.value}: {review_reason}",
            old_tier=old_tier.value,
            new_tier=new_tier.value,
            reason=review_reason,
            evidence=list(evidence),
            agent_name="AIReviewClient",
            decision_source=decision_source,
            model=model,
            decision=decision,
            duplicate_status=_coerce_duplicate_status(metadata.duplicate_status).value,
            similar_to=list(metadata.similar_to),
        ),
    )
    MetadataWriter().write_metadata(metadata, destination_metadata_path)
    return destination_metadata_path, destination_markdown_path


def _coerce_tier(tier: Tier | str) -> Tier:
    if isinstance(tier, Tier):
        return tier
    return Tier(tier)


def _coerce_duplicate_status(status: DuplicateStatus | str) -> DuplicateStatus:
    if isinstance(status, DuplicateStatus):
        return status
    return DuplicateStatus(status)
