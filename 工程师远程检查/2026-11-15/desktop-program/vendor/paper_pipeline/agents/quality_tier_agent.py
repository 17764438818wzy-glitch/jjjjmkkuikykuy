"""Advisory quality-tier agent for task 08."""

from __future__ import annotations

from paper_pipeline.agents.base import BaseSubAgent
from paper_pipeline.domain import (
    AgentResult,
    DuplicateStatus,
    PaperMetadata,
    Tier,
)
from paper_pipeline.taxonomy.categories import validate_category
from paper_pipeline.taxonomy.directions import validate_direction
from paper_pipeline.taxonomy.methods import validate_research_method_primary


class QualityTierAgent(BaseSubAgent):
    agent_name = "QualityTierAgent"

    def run(self, task_id: str, metadata: PaperMetadata) -> AgentResult:
        issues = _quality_issues(metadata)
        risk_codes = _risk_codes(metadata)
        if issues or risk_codes:
            reason = "；".join([*issues, *risk_codes]) or "质量存在不确定风险"
            return AgentResult(
                agent_name=self.agent_name,
                task_id=task_id,
                paper_id=metadata.id,
                run_id=metadata.run_id,
                status="uncertain",
                decision={
                    "tier": Tier.PENDING.value,
                    "score": min(_quality_score(metadata), 60),
                    "quality_reason": reason,
                    "risk_flags": risk_codes,
                    "needs_review": True,
                },
                confidence=0.45,
                reason=reason,
                evidence=["metadata", "risk_flags"],
                risk_flags=list(metadata.risk_flags),
                needs_review=True,
                suggested_next_action="保持 pending，由总智能体或人工复审后再决定正式等级",
            )

        score = _quality_score(metadata)
        tier = Tier.GOLD if score >= 90 else Tier.SILVER
        quality_reason = "结构完整、分类方向明确、研究方法明确，未发现阻断风险"
        return AgentResult(
            agent_name=self.agent_name,
            task_id=task_id,
            paper_id=metadata.id,
            run_id=metadata.run_id,
            status="success",
            decision={
                "tier": tier.value,
                "score": score,
                "quality_reason": quality_reason,
                "risk_flags": [],
                "needs_review": False,
            },
            confidence=0.82 if tier is Tier.SILVER else 0.9,
            reason=quality_reason,
            evidence=["metadata.category", "metadata.direction", "metadata.structure"],
            risk_flags=[],
            suggested_next_action="仅作为等级建议提交总智能体，不写入正式 metadata",
        )


def _quality_issues(metadata: PaperMetadata) -> list[str]:
    issues: list[str] = []
    category_result = validate_category(metadata.category)
    if not category_result.is_valid:
        issues.append("分类不明确")
        return issues

    direction_result = validate_direction(metadata.category, metadata.direction)
    if not direction_result.is_valid:
        issues.append("方向不明确或与分类不匹配")

    method_result = validate_research_method_primary(metadata.research_method_primary or "")
    if not method_result.is_valid or method_result.value == "未明确":
        issues.append("研究方法不明确")

    return issues


def _risk_codes(metadata: PaperMetadata) -> list[str]:
    risk_codes = [flag.code for flag in metadata.risk_flags]
    if metadata.duplicate_status is not DuplicateStatus.UNIQUE:
        risk_codes.append(metadata.duplicate_status.value)
    return _deduplicate(risk_codes)


def _quality_score(metadata: PaperMetadata) -> int:
    score = 50
    if validate_category(metadata.category).is_valid:
        score += 15
    if validate_direction(metadata.category, metadata.direction).is_valid:
        score += 15
    method_result = validate_research_method_primary(metadata.research_method_primary or "")
    if method_result.is_valid and method_result.value != "未明确":
        score += 10
    if len(metadata.abstract.strip()) >= 20:
        score += 5
    if metadata.keywords:
        score += 5
    if len(metadata.chapter_types) >= 5:
        score += 5
    if metadata.sensitive_clean:
        score += 5
    return min(score, 95)


def _deduplicate(values: list[str]) -> list[str]:
    seen: list[str] = []
    for value in values:
        if value not in seen:
            seen.append(value)
    return seen
