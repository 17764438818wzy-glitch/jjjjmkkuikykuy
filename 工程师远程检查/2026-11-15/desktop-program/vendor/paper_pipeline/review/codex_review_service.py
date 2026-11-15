"""Codex review orchestration for pending initial-review candidates."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Protocol

from paper_pipeline.agents.pending_review_agent import PendingReviewAgent
from paper_pipeline.domain import (
    AgentResult,
    DuplicateStatus,
    PaperMetadata,
    ReviewHistoryItem,
    SqlStatus,
    Tier,
)
from paper_pipeline.domain.serialization import metadata_from_json
from paper_pipeline.review.review_service import ReviewApplicationResult, ReviewService
from paper_pipeline.storage.metadata_writer import MetadataWriter


class CodexReviewer(Protocol):
    agent_name: str

    def review(
        self,
        *,
        task_id: str,
        pending_metadata: PaperMetadata,
        pending_body: str = "",
    ) -> AgentResult: ...


CodexReviewStatus = Literal[
    "applied",
    "skipped",
    "codex_call_failed",
]


@dataclass(frozen=True, slots=True)
class CodexReviewRunResult:
    status: CodexReviewStatus
    metadata_path: Path
    markdown_path: Path
    applied: bool = False
    agent_result: AgentResult | None = None
    review_result: ReviewApplicationResult | None = None
    error: str = ""
    reason: str = ""


@dataclass(slots=True)
class CodexReviewService:
    library_dir: str | Path
    reviewer: CodexReviewer = field(default_factory=PendingReviewAgent)
    review_service: ReviewService | None = None
    metadata_writer: MetadataWriter = field(default_factory=MetadataWriter)
    min_confidence: float = 0.7

    def review_one(
        self,
        metadata_path: str | Path,
        markdown_path: str | Path,
        *,
        task_id: str = "",
        confirmed_by: str = "codex",
    ) -> CodexReviewRunResult:
        metadata_file = Path(metadata_path)
        markdown_file = Path(markdown_path)
        metadata = metadata_from_json(metadata_file.read_text(encoding="utf-8"))

        eligible, skip_reason = _eligible_for_codex_review(metadata)
        if not eligible:
            return CodexReviewRunResult(
                "skipped",
                metadata_file,
                markdown_file,
                reason=skip_reason,
            )

        pending_body = markdown_file.read_text(encoding="utf-8")
        review_task_id = task_id or f"codex-review-{metadata.id}_v{metadata.version}"
        try:
            agent_result = self.reviewer.review(
                task_id=review_task_id,
                pending_metadata=metadata,
                pending_body=pending_body,
            )
        except Exception as exc:  # noqa: BLE001 - failures are recorded as review evidence.
            reason = f"codex_call_failed: {exc}"
            _record_codex_call_failed(
                metadata=metadata,
                metadata_path=metadata_file,
                writer=self.metadata_writer,
                reviewer=confirmed_by,
                agent_name=_reviewer_name(self.reviewer),
                reason=reason,
            )
            return CodexReviewRunResult(
                "codex_call_failed",
                metadata_file,
                markdown_file,
                applied=False,
                error=str(exc),
                reason=reason,
            )

        target_tier, reason = _target_tier_for_codex_result(
            agent_result,
            metadata=metadata,
            min_confidence=self.min_confidence,
        )
        application_result = _application_agent_result(
            agent_result,
            metadata=metadata,
            target_tier=target_tier,
            reason=reason,
        )
        review_service = self.review_service or ReviewService(
            library_dir=self.library_dir,
            metadata_writer=self.metadata_writer,
        )
        review_result = review_service.apply_review_decision(
            metadata_file,
            application_result,
            confirmed_by=confirmed_by,
            confirmed=True,
            reason=reason,
        )
        return CodexReviewRunResult(
            "applied",
            review_result.metadata_path,
            _markdown_path_for_metadata(review_result.metadata_path),
            applied=True,
            agent_result=agent_result,
            review_result=review_result,
            reason=reason,
        )


def _eligible_for_codex_review(metadata: PaperMetadata) -> tuple[bool, str]:
    tier = _coerce_tier(metadata.tier)
    if tier is Tier.MANUAL_REVIEW_REQUIRED:
        return False, "manual_review_required is reserved for user review"
    if not metadata.needs_codex_review:
        return False, "needs_codex_review is false"
    if tier is Tier.PENDING or metadata.gold_candidate:
        return True, ""
    return False, f"tier={tier.value} is not a Codex review candidate"


def _target_tier_for_codex_result(
    agent_result: AgentResult,
    *,
    metadata: PaperMetadata,
    min_confidence: float,
) -> tuple[Tier, str]:
    reason = _result_reason(agent_result)
    if _is_uncertain_or_conflicted(agent_result, min_confidence=min_confidence):
        return (
            Tier.MANUAL_REVIEW_REQUIRED,
            reason or "codex_uncertain: Codex could not determine a safe final tier",
        )

    target_tier = _suggested_tier(agent_result)
    if target_tier is Tier.REJECTED and not reason.strip():
        return (
            Tier.MANUAL_REVIEW_REQUIRED,
            "codex_reject_missing_reason: rejected review requires a reason",
        )
    if target_tier in {Tier.GOLD, Tier.SILVER, Tier.REJECTED}:
        return target_tier, reason

    return (
        Tier.MANUAL_REVIEW_REQUIRED,
        reason
        or f"codex_uncertain: unsupported Codex target for {metadata.id}_v{metadata.version}",
    )


def _application_agent_result(
    agent_result: AgentResult,
    *,
    metadata: PaperMetadata,
    target_tier: Tier,
    reason: str,
) -> AgentResult:
    decision = dict(agent_result.decision)
    evidence = _evidence(agent_result)
    decision["new_tier_suggestion"] = target_tier.value
    decision.setdefault("category_suggestion", metadata.category)
    decision.setdefault("direction_suggestion", metadata.direction)
    if metadata.research_method_primary:
        decision.setdefault("research_method_suggestion", metadata.research_method_primary)
    decision.setdefault("duplicate_decision", _coerce_duplicate_status(metadata).value)
    decision.setdefault("similar_to", list(metadata.similar_to))
    decision.setdefault("reason", reason)
    decision.setdefault("evidence", evidence)
    decision.setdefault("ai_decision", _ai_decision_for(target_tier))
    decision.setdefault("decision_source", "codex_review")
    return AgentResult(
        agent_name=agent_result.agent_name,
        status="success",
        decision=decision,
        confidence=agent_result.confidence,
        reason=reason,
        evidence=evidence,
        risk_flags=list(agent_result.risk_flags),
    )


def _record_codex_call_failed(
    *,
    metadata: PaperMetadata,
    metadata_path: Path,
    writer: MetadataWriter,
    reviewer: str,
    agent_name: str,
    reason: str,
) -> None:
    tier = _coerce_tier(metadata.tier)
    metadata.quality_reason = reason
    metadata.sql_status = _coerce_sql_status(metadata.sql_status)
    metadata.add_review_history(
        ReviewHistoryItem(
            reviewer=reviewer,
            action="codex_review_failed",
            comment=f"{tier.value} -> {tier.value}: {reason}",
            old_tier=tier.value,
            new_tier=tier.value,
            reason=reason,
            evidence=["metadata", "processed_markdown"],
            agent_name=agent_name,
            decision_source="codex_review_failed",
            decision="codex_call_failed",
            duplicate_status=_coerce_duplicate_status(metadata).value,
            similar_to=list(metadata.similar_to),
        ),
    )
    writer.write_metadata(metadata, metadata_path)


def _is_uncertain_or_conflicted(
    agent_result: AgentResult,
    *,
    min_confidence: float,
) -> bool:
    if agent_result.status != "success":
        return True
    if agent_result.confidence < min_confidence:
        return True
    if agent_result.needs_review or agent_result.blocking:
        return True
    if agent_result.risk_flags:
        return True
    decision = agent_result.decision
    conflict_keys = {
        "conflict",
        "classification_conflict",
        "direction_conflict",
        "quality_conflict",
        "duplicate_conflict",
        "desensitization_risk",
    }
    if any(bool(decision.get(key)) for key in conflict_keys):
        return True
    risk_flags = decision.get("risk_flags")
    return isinstance(risk_flags, list) and bool(risk_flags)


def _suggested_tier(agent_result: AgentResult) -> Tier:
    value = agent_result.decision.get("new_tier_suggestion")
    if value:
        return Tier(str(value))
    decision = str(
        agent_result.decision.get("ai_decision")
        or agent_result.decision.get("decision")
        or "",
    )
    if decision == "approve_gold":
        return Tier.GOLD
    if decision == "approve_silver":
        return Tier.SILVER
    if decision == "reject":
        return Tier.REJECTED
    return Tier.MANUAL_REVIEW_REQUIRED


def _result_reason(agent_result: AgentResult) -> str:
    decision_reason = agent_result.decision.get("reason", "")
    return str(decision_reason).strip() or agent_result.reason.strip()


def _evidence(agent_result: AgentResult) -> list[str]:
    decision_evidence = agent_result.decision.get("evidence", [])
    if isinstance(decision_evidence, list):
        values = [str(item) for item in decision_evidence]
    else:
        values = [str(item) for item in agent_result.evidence]
    if "metadata" not in values:
        values.append("metadata")
    return values


def _ai_decision_for(tier: Tier) -> str:
    if tier is Tier.GOLD:
        return "approve_gold"
    if tier is Tier.SILVER:
        return "approve_silver"
    if tier is Tier.REJECTED:
        return "reject"
    return "manual_review_required"


def _markdown_path_for_metadata(metadata_path: Path) -> Path:
    if metadata_path.name.endswith(".metadata.json"):
        return metadata_path.with_name(metadata_path.name.replace(".metadata.json", ".md"))
    if metadata_path.name == "metadata.json" and metadata_path.parent.name.startswith("ACC_"):
        return metadata_path.parent / f"{metadata_path.parent.name}.md"
    return metadata_path.with_suffix(".md")


def _reviewer_name(reviewer: CodexReviewer) -> str:
    return getattr(reviewer, "agent_name", reviewer.__class__.__name__)


def _coerce_tier(tier: Tier | str) -> Tier:
    if isinstance(tier, Tier):
        return tier
    return Tier(tier)


def _coerce_duplicate_status(metadata: PaperMetadata) -> DuplicateStatus:
    status = metadata.duplicate_status
    if isinstance(status, DuplicateStatus):
        return status
    return DuplicateStatus(status)


def _coerce_sql_status(status: SqlStatus | str) -> SqlStatus:
    if isinstance(status, SqlStatus):
        return status
    return SqlStatus(status)
