"""Advisory pending-review agent for task 10."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

from paper_pipeline.agents.base import BaseSubAgent
from paper_pipeline.domain import (
    AgentResult,
    DuplicateStatus,
    PaperMetadata,
    Tier,
)
from paper_pipeline.duplicate.detector import DuplicateDetectionResult
from paper_pipeline.taxonomy.categories import validate_category
from paper_pipeline.taxonomy.directions import validate_direction
from paper_pipeline.taxonomy.methods import validate_research_method_primary


class PendingReviewAgent(BaseSubAgent):
    agent_name = "PendingReviewAgent"

    def run(self, task_id: str, metadata: PaperMetadata) -> AgentResult:
        return self.review(
            task_id=task_id,
            pending_metadata=metadata,
            pending_body="",
            similar_candidates=[],
        )

    def review(
        self,
        *,
        task_id: str,
        pending_metadata: PaperMetadata,
        pending_body: str = "",
        similar_candidates: Sequence[PaperMetadata] = (),
        upstream_result: AgentResult | None = None,
        duplicate_detection_result: DuplicateDetectionResult | None = None,
    ) -> AgentResult:
        duplicate_status = _duplicate_status_from(
            pending_metadata,
            duplicate_detection_result,
        )
        category = _valid_category_or_empty(pending_metadata.category)
        direction = _valid_direction_or_empty(category, pending_metadata.direction)
        research_method = _valid_method_or_empty(
            pending_metadata.research_method_primary or "",
        )
        similar_to = _similar_to_values(
            pending_metadata,
            similar_candidates,
            duplicate_detection_result,
        )
        evidence = _evidence(
            pending_body=pending_body,
            similar_candidates=similar_candidates,
            upstream_result=upstream_result,
            duplicate_detection_result=duplicate_detection_result,
        )

        new_tier, status, confidence, needs_review, reason = _suggest_tier(
            pending_metadata=pending_metadata,
            pending_body=pending_body,
            duplicate_status=duplicate_status,
            category=category,
            direction=direction,
            research_method=research_method,
        )
        review_history_item: dict[str, object] = {
            "reviewer": self.agent_name,
            "operator": self.agent_name,
            "action": "pending_review_suggested",
            "old_tier": Tier(pending_metadata.tier).value,
            "new_tier": new_tier.value,
            "reason": reason,
            "evidence": evidence,
            "agent_name": self.agent_name,
            "decision_source": "agent_suggestion",
            "duplicate_status": duplicate_status.value,
            "similar_to": similar_to,
        }
        decision: dict[str, object] = {
            "old_tier": Tier(pending_metadata.tier).value,
            "new_tier_suggestion": new_tier.value,
            "category_suggestion": category,
            "direction_suggestion": direction,
            "research_method_suggestion": research_method,
            "duplicate_decision": duplicate_status.value,
            "similar_to": similar_to,
            "reason": reason,
            "evidence": evidence,
            "review_history_item": review_history_item,
        }
        return AgentResult(
            agent_name=self.agent_name,
            task_id=task_id,
            paper_id=pending_metadata.id,
            run_id=pending_metadata.run_id,
            status=status,
            decision=decision,
            confidence=confidence,
            reason=reason,
            evidence=evidence,
            risk_flags=list(pending_metadata.risk_flags),
            needs_review=needs_review,
            suggested_next_action=(
                "仅提交总智能体确认；PendingReviewAgent 不写 metadata、不移动文件、"
                "不上传 SQL、不构建索引"
            ),
        )


def _duplicate_status_from(
    metadata: PaperMetadata,
    result: DuplicateDetectionResult | None,
) -> DuplicateStatus:
    if result is not None:
        return result.duplicate_status
    if isinstance(metadata.duplicate_status, DuplicateStatus):
        return metadata.duplicate_status
    return DuplicateStatus(metadata.duplicate_status)


def _similar_to_values(
    metadata: PaperMetadata,
    similar_candidates: Sequence[PaperMetadata],
    result: DuplicateDetectionResult | None,
) -> list[str]:
    values = list(metadata.similar_to)
    if result is not None:
        values.extend(result.similar_to)
    values.extend(candidate.id for candidate in similar_candidates if candidate.id)
    return _deduplicate(values)


def _evidence(
    *,
    pending_body: str,
    similar_candidates: Sequence[PaperMetadata],
    upstream_result: AgentResult | None,
    duplicate_detection_result: DuplicateDetectionResult | None,
) -> list[str]:
    evidence = ["pending_metadata"]
    if pending_body.strip():
        evidence.append("pending_body")
    if similar_candidates:
        evidence.append("similar_to_metadata")
    if upstream_result is not None:
        evidence.append("upstream_result")
    if duplicate_detection_result is not None:
        evidence.append("duplicate_detection_result")
    return evidence


def _suggest_tier(
    *,
    pending_metadata: PaperMetadata,
    pending_body: str,
    duplicate_status: DuplicateStatus,
    category: str,
    direction: str,
    research_method: str,
) -> tuple[Tier, Literal["success", "failed", "uncertain"], float, bool, str]:
    rejection_reason = _rejection_reason(pending_metadata, pending_body)
    if rejection_reason:
        return Tier.REJECTED, "success", 0.78, False, rejection_reason

    if duplicate_status is not DuplicateStatus.UNIQUE:
        return (
            Tier.PENDING,
            "uncertain",
            0.4,
            True,
            "存在重复风险，疑似重复默认继续 pending，等待总智能体或人工确认",
        )

    if pending_metadata.risk_flags:
        return Tier.PENDING, "uncertain", 0.42, True, "存在高风险标记，继续 pending"

    missing = []
    if not category:
        missing.append("category")
    if not direction:
        missing.append("direction")
    if not research_method or research_method == "未明确":
        missing.append("research_method")
    if missing:
        return (
            Tier.PENDING,
            "uncertain",
            0.35,
            True,
            f"{'、'.join(missing)} 不明确，继续 pending",
        )

    score = pending_metadata.score if pending_metadata.score is not None else 70.0
    if score >= 90:
        return Tier.GOLD, "success", 0.84, False, "复审证据完整且质量分达到 gold 建议阈值"
    if score >= 60:
        return Tier.SILVER, "success", 0.76, False, "复审证据完整，建议 silver"
    return Tier.PENDING, "uncertain", 0.45, True, "质量分不足且缺少明确拒绝证据，继续 pending"


def _rejection_reason(metadata: PaperMetadata, body: str) -> str:
    haystack = " ".join(
        [
            metadata.title,
            metadata.quality_reason,
            metadata.abstract,
            body,
        ],
    )
    for marker in ("非论文", "广告", "严重缺失", "无法识别", "空白文档"):
        if marker in haystack:
            return f"复审发现{marker}，建议 rejected"
    return ""


def _valid_category_or_empty(category: str) -> str:
    result = validate_category(category)
    return result.value if result.is_valid else ""


def _valid_direction_or_empty(category: str, direction: str) -> str:
    result = validate_direction(category, direction)
    return result.value if result.is_valid else ""


def _valid_method_or_empty(method: str) -> str:
    result = validate_research_method_primary(method)
    return result.value if result.is_valid else "未明确"


def _deduplicate(values: list[str]) -> list[str]:
    seen: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.append(value)
    return seen

