"""Apply confirmed pending-review decisions through the service layer."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from paper_pipeline.domain import (
    AgentResult,
    DuplicateStatus,
    IndexStatus,
    PaperMetadata,
    ReviewHistoryItem,
    SqlStatus,
    Tier,
)
from paper_pipeline.domain.serialization import metadata_from_json
from paper_pipeline.storage.metadata_writer import MetadataWriter
from paper_pipeline.storage.tier_mover import TierMover, can_change_tier


@dataclass(frozen=True, slots=True)
class ReviewApplicationResult:
    metadata_path: Path
    old_metadata_path: Path
    old_tier: Tier
    new_tier: Tier
    sql_upload_triggered: bool = False
    index_build_triggered: bool = False
    index_update_required: bool = False
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ReviewService:
    library_dir: str | Path | None = None
    metadata_writer: MetadataWriter = field(default_factory=MetadataWriter)

    def apply_review_decision(
        self,
        metadata_path: str | Path,
        agent_result: AgentResult,
        *,
        confirmed_by: str,
        confirmed: bool,
        reason: str = "",
    ) -> ReviewApplicationResult:
        if not confirmed:
            raise ValueError("explicit confirmation is required before applying review")

        old_metadata_path = Path(metadata_path)
        metadata = metadata_from_json(old_metadata_path.read_text(encoding="utf-8"))
        old_tier = _coerce_tier(metadata.tier)
        new_tier = _new_tier_from(agent_result)
        review_reason = _review_reason(agent_result, reason)
        if new_tier is Tier.REJECTED and not review_reason.strip():
            raise ValueError("reason is required when applying rejected review")
        if new_tier != old_tier and not can_change_tier(metadata):
            raise ValueError("uploaded SQL records cannot change tier in review service")

        _apply_decision_fields(metadata, agent_result, new_tier)
        destination = old_metadata_path
        notes: list[str] = []

        if new_tier != old_tier and self.library_dir is not None:
            self.metadata_writer.write_metadata(metadata, old_metadata_path)
            moved = TierMover(
                self.library_dir,
                metadata_writer=self.metadata_writer,
            ).move_current_version(
                paper_id=metadata.id,
                version=metadata.version,
                new_tier=new_tier,
                category=metadata.category,
                direction=metadata.direction,
                reviewer=confirmed_by,
                reason=review_reason,
            )
            destination = moved.metadata_path
            metadata = metadata_from_json(destination.read_text(encoding="utf-8"))
            _apply_decision_fields(metadata, agent_result, new_tier)
            notes.append("library tier move applied through TierMover")
        else:
            metadata.tier = new_tier

        index_update_required = new_tier in {Tier.GOLD, Tier.SILVER}
        if index_update_required:
            metadata.index_status = IndexStatus.NEEDS_REBUILD

        metadata.add_review_history(
            _review_history_item(
                metadata=metadata,
                old_tier=old_tier,
                new_tier=new_tier,
                agent_result=agent_result,
                confirmed_by=confirmed_by,
                reason=review_reason,
            ),
        )
        self.metadata_writer.write_metadata(metadata, destination)
        return ReviewApplicationResult(
            metadata_path=destination,
            old_metadata_path=old_metadata_path,
            old_tier=old_tier,
            new_tier=new_tier,
            sql_upload_triggered=False,
            index_build_triggered=False,
            index_update_required=index_update_required,
            notes=notes,
        )


def _apply_decision_fields(
    metadata: PaperMetadata,
    agent_result: AgentResult,
    new_tier: Tier,
) -> None:
    decision = agent_result.decision
    metadata.tier = new_tier
    _apply_codex_review_state(metadata, new_tier)

    category = str(decision.get("category_suggestion", "")).strip()
    direction = str(decision.get("direction_suggestion", "")).strip()
    research_method = str(decision.get("research_method_suggestion", "")).strip()
    if category:
        metadata.category = category
    if direction:
        metadata.direction = direction
    if research_method:
        metadata.research_method_primary = research_method

    duplicate_decision = decision.get("duplicate_decision")
    if duplicate_decision:
        metadata.duplicate_status = DuplicateStatus(str(duplicate_decision))
    similar_to = decision.get("similar_to")
    if isinstance(similar_to, list):
        metadata.similar_to = [str(item) for item in similar_to]

    metadata.sql_status = _coerce_sql_status(metadata.sql_status)


def _apply_codex_review_state(metadata: PaperMetadata, new_tier: Tier) -> None:
    if new_tier is Tier.GOLD:
        metadata.gold_candidate = False
        metadata.needs_codex_review = False
        metadata.tier_decision_source = "reviewed_gold"
        return

    if new_tier is Tier.SILVER:
        metadata.gold_candidate = False
        metadata.needs_codex_review = False
        metadata.tier_decision_source = "reviewed_silver"
        return

    if new_tier is Tier.MANUAL_REVIEW_REQUIRED:
        metadata.gold_candidate = False
        metadata.needs_codex_review = False
        metadata.tier_decision_source = "manual_review_required"
        return

    if not metadata.gold_candidate and not metadata.needs_codex_review:
        return

    metadata.gold_candidate = False
    metadata.needs_codex_review = False
    metadata.tier_decision_source = "codex_review_decision"


def _review_history_item(
    *,
    metadata: PaperMetadata,
    old_tier: Tier,
    new_tier: Tier,
    agent_result: AgentResult,
    confirmed_by: str,
    reason: str,
) -> ReviewHistoryItem:
    evidence = _decision_evidence(agent_result)
    from_auto_gold_suggestion = (
        new_tier is Tier.GOLD and metadata.quality_tier_suggestion == Tier.GOLD.value
    )
    if from_auto_gold_suggestion and "quality_tier_suggestion=gold" not in evidence:
        evidence.append("quality_tier_suggestion=gold")
    explicit_decision_source = str(agent_result.decision.get("decision_source", "")).strip()
    decision_source = (
        "codex_review_confirmed_from_auto_quality_suggestion"
        if from_auto_gold_suggestion
        else explicit_decision_source or "confirmed_orchestrator"
    )
    return ReviewHistoryItem(
        reviewer=confirmed_by,
        action="pending_review_applied",
        comment=f"{old_tier.value} -> {new_tier.value}: {reason}",
        operator=confirmed_by,
        old_tier=old_tier.value,
        new_tier=new_tier.value,
        reason=reason,
        evidence=evidence,
        agent_name=agent_result.agent_name,
        model=str(agent_result.decision.get("model", "")),
        decision=str(agent_result.decision.get("ai_decision", "")),
        decision_source=decision_source,
        duplicate_status=DuplicateStatus(metadata.duplicate_status).value,
        similar_to=list(metadata.similar_to),
    )


def _new_tier_from(agent_result: AgentResult) -> Tier:
    value = agent_result.decision.get("new_tier_suggestion")
    if not value:
        raise ValueError("new_tier_suggestion is required")
    return Tier(str(value))


def _review_reason(agent_result: AgentResult, explicit_reason: str) -> str:
    decision_reason = agent_result.decision.get("reason", "")
    return explicit_reason.strip() or str(decision_reason).strip() or agent_result.reason


def _decision_evidence(agent_result: AgentResult) -> list[str]:
    decision_evidence = agent_result.decision.get("evidence", [])
    if isinstance(decision_evidence, list):
        return [str(item) for item in decision_evidence]
    return [str(item) for item in agent_result.evidence]


def _coerce_tier(tier: Tier | str) -> Tier:
    if isinstance(tier, Tier):
        return tier
    return Tier(tier)


def _coerce_sql_status(status: SqlStatus | str) -> SqlStatus:
    if isinstance(status, SqlStatus):
        return status
    return SqlStatus(status)


def _unused(value: Any) -> Any:
    return value
