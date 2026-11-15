"""Conservative stage0 admission rules."""

from __future__ import annotations

from dataclasses import dataclass, field

from paper_pipeline.domain import Stage0Decision


@dataclass(slots=True)
class Stage0RuleDecision:
    decision: Stage0Decision
    reason: str
    confidence: float
    evidence: list[str] = field(default_factory=list)


def evaluate_stage0_text(*, source_name: str, text: str) -> Stage0RuleDecision:
    normalized = _normalize(text)
    name = source_name.lower()
    if not normalized:
        return _reject("空白文档", 0.99, ["blank"])

    if _contains_any(normalized, ["开题报告"]) or "opening" in name:
        return _reject("明确是开题报告", 0.98, ["开题报告"])

    if _contains_any(normalized, ["任务书"]) or "task" in name:
        return _reject("明确是任务书", 0.98, ["任务书"])

    if _looks_like_template(normalized, name):
        return _reject("明显模板", 0.95, ["模板"])

    if _looks_like_journal_article(normalized):
        return _reject("明确是期刊文章", 0.92, ["期刊文章"])

    if _looks_like_full_paper(normalized):
        evidence = ["毕业论文", "摘要", "关键词", "参考文献"]
        if "文献综述" in normalized:
            evidence.append("文献综述")
        return Stage0RuleDecision(
            decision=Stage0Decision.ACCEPT,
            reason="符合正式论文基本结构",
            confidence=0.82,
            evidence=evidence,
        )

    if _looks_like_independent_literature_review(normalized, name):
        return _reject("明确是独立文献综述文件", 0.86, ["文献综述"])

    return Stage0RuleDecision(
        decision=Stage0Decision.PENDING,
        reason="准入判断不确定",
        confidence=0.0,
        evidence=["uncertain"],
    )


def _reject(reason: str, confidence: float, evidence: list[str]) -> Stage0RuleDecision:
    return Stage0RuleDecision(
        decision=Stage0Decision.STAGE0_REJECT,
        reason=reason,
        confidence=confidence,
        evidence=evidence,
    )


def _normalize(text: str) -> str:
    return " ".join(text.replace("\ufeff", "").split())


def _contains_any(text: str, needles: list[str]) -> bool:
    compact = "".join(text.split())
    return any(needle in text or needle in compact for needle in needles)


def _looks_like_template(text: str, source_name: str) -> bool:
    return (
        _contains_any(text, ["模板"])
        and _contains_any(text, ["请在此处", "填写", "论文题目", "学院", "专业"])
    ) or ("template" in source_name and "毕业论文" in text)


def _looks_like_journal_article(text: str) -> bool:
    return _contains_any(
        text,
        [
            "期刊论文",
            "收稿日期",
            "doi:",
            "doi：",
            "中图分类号",
            "基金项目",
            "投稿",
        ],
    )


def _looks_like_full_paper(text: str) -> bool:
    has_paper_marker = _contains_any(text, ["毕业论文", "学位论文"])
    structure_score = sum(
        _contains_any(text, [marker])
        for marker in ["摘要", "关键词", "绪论", "结论", "参考文献"]
    )
    return has_paper_marker and structure_score >= 4


def _looks_like_independent_literature_review(text: str, source_name: str) -> bool:
    has_review_marker = _contains_any(text, ["文献综述"]) or "literature" in source_name
    has_full_paper_shape = _looks_like_full_paper(text)
    return has_review_marker and not has_full_paper_shape
