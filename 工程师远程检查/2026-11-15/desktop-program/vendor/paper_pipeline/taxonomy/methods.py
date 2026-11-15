"""Fixed research-method taxonomy for task 08."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from paper_pipeline.taxonomy.validation import (
    TaxonomyValidationResult,
    invalid_taxonomy_result,
    valid_taxonomy_result,
)

PRIMARY_RESEARCH_METHODS: tuple[str, ...] = (
    "理论分析",
    "文献综述型",
    "案例研究",
    "问卷调查",
    "访谈研究",
    "实证研究",
    "简单数据分析",
    "系统设计与实现",
    "工程设计",
    "实验研究",
    "仿真研究",
    "比较研究",
    "混合方法",
    "未明确",
)

RESEARCH_METHOD_TAGS: tuple[str, ...] = (
    "问卷调查",
    "访谈",
    "案例企业",
    "案例地区",
    "案例平台",
    "描述性统计",
    "相关性分析",
    "回归分析",
    "显著性检验",
    "变量假设",
    "信度分析",
    "效度分析",
    "SPSS",
    "AMOS",
    "结构方程模型",
    "因子分析",
    "层次分析法",
    "熵权法",
    "TOPSIS",
    "灰色关联分析",
    "Python爬虫",
    "数据可视化",
    "机器学习",
    "深度学习",
    "图像识别",
    "自然语言处理",
    "系统需求分析",
    "系统设计",
    "数据库设计",
    "前后端分离",
    "Spring Boot",
    "SSM",
    "Vue",
    "微信小程序",
    "STM32",
    "51单片机",
    "PLC",
    "LabVIEW",
    "仿真建模",
    "实验对照",
    "性能测试",
    "有限元分析",
    "CAD建模",
    "BIM",
    "施工组织设计",
    "政策分析",
    "文本分析",
    "比较分析",
)

_PRIMARY_KEYWORDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("系统设计与实现", ("系统", "平台", "spring boot", "vue", "数据库", "前后端", "开发")),
    ("问卷调查", ("问卷", "调查问卷")),
    ("访谈研究", ("访谈", "访谈法")),
    ("案例研究", ("案例", "个案")),
    ("实验研究", ("实验", "对照")),
    ("仿真研究", ("仿真", "模拟")),
    ("简单数据分析", ("描述性统计", "数据分析", "spss", "回归")),
    ("比较研究", ("比较", "对比")),
    ("文献综述型", ("文献综述", "综述")),
    ("工程设计", ("工程设计", "施工组织", "有限元", "cad")),
    ("理论分析", ("理论", "机制分析")),
)

_TAG_KEYWORDS: dict[str, tuple[str, ...]] = {
    tag: (tag.lower(),) for tag in RESEARCH_METHOD_TAGS
}


@dataclass(frozen=True, slots=True)
class NormalizedResearchMethods:
    research_method_primary: str
    research_method_tags: list[str]
    suggested_tags: list[str]
    needs_review: bool = False
    reason: str = ""


def validate_research_method_primary(value: object) -> TaxonomyValidationResult:
    if _is_multiple(value):
        return invalid_taxonomy_result("", "每篇论文只能有一个 research_method_primary")

    normalized = _normalize(str(value)) if value is not None else ""
    if normalized in PRIMARY_RESEARCH_METHODS:
        return valid_taxonomy_result(normalized)

    reason = "研究方法不明确" if not normalized else "主研究方法不在固定名单中"
    return invalid_taxonomy_result(normalized, reason)


def normalize_research_methods(
    *,
    primary: object,
    tags: Iterable[str] | None,
    text: str = "",
) -> NormalizedResearchMethods:
    primary_result = validate_research_method_primary(primary)
    inferred_primary = infer_research_method_primary(text)

    needs_review = False
    reason = ""
    if primary_result.is_valid and primary_result.value != "未明确":
        normalized_primary = primary_result.value
    elif inferred_primary:
        normalized_primary = inferred_primary
    else:
        normalized_primary = "未明确"
        needs_review = True
        reason = primary_result.reason or "研究方法不明确"

    normalized_tags, suggested_tags = normalize_research_method_tags(tags or (), text=text)
    return NormalizedResearchMethods(
        research_method_primary=normalized_primary,
        research_method_tags=normalized_tags,
        suggested_tags=suggested_tags,
        needs_review=needs_review,
        reason=reason,
    )


def normalize_research_method_tags(
    tags: Iterable[str],
    *,
    text: str = "",
) -> tuple[list[str], list[str]]:
    official: list[str] = []
    suggested: list[str] = []

    for tag in tags:
        normalized = _normalize(tag)
        if not normalized:
            continue
        if normalized in RESEARCH_METHOD_TAGS:
            _append_unique(official, normalized)
        else:
            _append_unique(suggested, normalized)

    search_text = text.lower()
    for tag, keywords in _TAG_KEYWORDS.items():
        if any(keyword in search_text for keyword in keywords):
            _append_unique(official, tag)

    return official, suggested


def infer_research_method_primary(text: str) -> str:
    search_text = text.lower()
    for method, keywords in _PRIMARY_KEYWORDS:
        if any(keyword in search_text for keyword in keywords):
            return method
    return ""


def _is_multiple(value: object) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, str)


def _normalize(value: str) -> str:
    return value.strip()


def _append_unique(values: list[str], value: str) -> None:
    if value not in values:
        values.append(value)
