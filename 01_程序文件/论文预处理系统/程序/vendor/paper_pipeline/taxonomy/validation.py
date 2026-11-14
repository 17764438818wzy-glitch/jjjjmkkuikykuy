"""Shared validation helpers for task 08 taxonomy decisions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


AgentStatus = Literal["success", "uncertain"]


@dataclass(frozen=True, slots=True)
class TaxonomyValidationResult:
    is_valid: bool
    value: str = ""
    reason: str = ""
    needs_review: bool = False
    suggested_status: AgentStatus = "success"


def invalid_taxonomy_result(value: str, reason: str) -> TaxonomyValidationResult:
    return TaxonomyValidationResult(
        is_valid=False,
        value=value,
        reason=reason,
        needs_review=True,
        suggested_status="uncertain",
    )


def valid_taxonomy_result(value: str, reason: str = "") -> TaxonomyValidationResult:
    return TaxonomyValidationResult(
        is_valid=True,
        value=value,
        reason=reason,
        needs_review=False,
        suggested_status="success",
    )


def validate_tier_candidate(
    *,
    tier: str,
    category: str,
    direction: str,
) -> TaxonomyValidationResult:
    from paper_pipeline.taxonomy.categories import validate_category
    from paper_pipeline.taxonomy.directions import validate_direction

    normalized_tier = tier.strip()
    if normalized_tier not in {"gold", "silver", "pending", "rejected"}:
        return invalid_taxonomy_result(normalized_tier, "质量等级建议不在允许名单中")

    if normalized_tier not in {"gold", "silver"}:
        return valid_taxonomy_result(normalized_tier)

    category_result = validate_category(category)
    if not category_result.is_valid:
        return invalid_taxonomy_result(normalized_tier, "gold/silver 候选缺少明确固定大类")

    direction_result = validate_direction(category, direction)
    if not direction_result.is_valid:
        return invalid_taxonomy_result(normalized_tier, "gold/silver 候选缺少匹配方向")

    return valid_taxonomy_result(normalized_tier)


def can_run_classification_agents(
    *,
    stage0_decision: object,
    pipeline_status: object = "",
    has_classifiable_content: bool = False,
) -> bool:
    normalized_stage0 = _enum_value(stage0_decision)
    normalized_pipeline_status = _enum_value(pipeline_status)
    if normalized_stage0 != "accept":
        return False
    return normalized_pipeline_status == "success" or has_classifiable_content


def _enum_value(value: object) -> str:
    raw_value = getattr(value, "value", value)
    return str(raw_value).strip()
