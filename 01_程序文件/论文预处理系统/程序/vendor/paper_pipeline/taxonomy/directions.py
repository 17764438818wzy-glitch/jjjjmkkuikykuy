"""Closed direction taxonomy for task 08 direction suggestions."""

from __future__ import annotations

from collections.abc import Iterable

from paper_pipeline.taxonomy.categories import is_valid_category
from paper_pipeline.taxonomy.validation import (
    TaxonomyValidationResult,
    invalid_taxonomy_result,
    valid_taxonomy_result,
)

# Seed v1 mapping. Production can replace this with the legacy pipeline taxonomy,
# but callers still validate against this closed interface.
DIRECTION_TAXONOMY: dict[str, tuple[str, ...]] = {
    "经管财会类": ("会计审计", "财务管理", "市场营销", "电子商务", "企业管理"),
    "教育教学类": ("课程与教学", "学前教育", "职业教育", "教育管理"),
    "医护健康类": ("护理管理", "临床护理", "康复健康", "公共卫生"),
    "法学社会公共类": ("法学实务", "社会治理", "公共管理", "思想政治教育"),
    "文学外语传媒类": ("英语语言文学", "翻译实践", "新闻传播", "汉语言文学"),
    "艺术设计类": ("视觉传达设计", "环境艺术设计", "数字媒体艺术", "产品设计"),
    "体育健康与休闲运动类": (
        "校园体育与体育教学",
        "体育培训与青少年运动",
        "大众健身与休闲体育",
        "体育产业与场馆运营",
    ),
    "计算机软件与数据类": (
        "软件工程与系统开发",
        "数据分析与挖掘",
        "人工智能应用",
        "网络与信息安全",
        "数字媒体技术",
    ),
    "智能硬件与控制类": ("单片机与嵌入式", "PLC与工业控制", "物联网硬件", "自动控制"),
    "机械制造与机电类": ("机械设计制造", "机电一体化", "模具夹具设计", "数控加工"),
    "土木建筑与交通类": ("建筑工程管理", "土木结构设计", "交通道路工程", "BIM与施工组织"),
    "电气电子与通信能源类": ("电气自动化", "电子信息技术", "通信工程", "新能源与电力"),
    "化工材料环境安全类": ("化工工艺", "材料应用", "环境治理", "安全工程"),
    "农林生物资源类": ("农业技术", "林业园林", "生物资源", "畜牧养殖"),
    "理学统计心理类": ("数学建模", "统计分析", "心理研究", "基础理学"),
}

_DIRECTION_KEYWORDS: dict[str, dict[str, tuple[str, ...]]] = {
    "计算机软件与数据类": {
        "软件工程与系统开发": (
            "spring boot",
            "vue",
            "系统",
            "平台",
            "前后端",
            "数据库",
            "软件",
            "网站",
            "小程序",
            "开发",
        ),
        "数据分析与挖掘": ("数据分析", "爬虫", "可视化", "回归", "分类模型"),
        "人工智能应用": ("机器学习", "深度学习", "图像识别", "自然语言处理"),
        "网络与信息安全": ("网络安全", "信息安全", "漏洞", "加密"),
        "数字媒体技术": ("数字媒体", "游戏", "交互", "三维"),
    },
    "医护健康类": {
        "护理管理": ("护理管理", "护理干预", "护士", "患者满意度"),
        "临床护理": ("临床", "护理", "患者", "术后"),
        "康复健康": ("康复", "健康", "慢病"),
        "公共卫生": ("公共卫生", "流行病", "健康管理"),
    },
    "体育健康与休闲运动类": {
        "校园体育与体育教学": ("校园体育", "体育教学", "体育课", "教学", "学生"),
        "体育培训与青少年运动": ("体育培训", "青少年运动", "训练", "儿童", "青少年"),
        "大众健身与休闲体育": ("大众健身", "休闲体育", "健身", "运动参与"),
        "体育产业与场馆运营": ("体育产业", "场馆", "运营", "体育消费"),
    },
}


def is_valid_direction(category: str, direction: str) -> bool:
    normalized_direction = _normalize(direction)
    return normalized_direction in DIRECTION_TAXONOMY.get(_normalize(category), ())


def validate_direction(category: str, direction: str) -> TaxonomyValidationResult:
    normalized_category = _normalize(category)
    normalized_direction = _normalize(direction)

    if not is_valid_category(normalized_category):
        return invalid_taxonomy_result(normalized_direction, "方向校验缺少有效固定大类")
    if normalized_direction in DIRECTION_TAXONOMY.get(normalized_category, ()):
        return valid_taxonomy_result(normalized_direction)

    reason = "方向为空，需进入 pending 复审" if not normalized_direction else "方向不属于对应大类固定名单"
    return invalid_taxonomy_result(normalized_direction, reason)


def infer_direction(category: str, text_parts: Iterable[str]) -> tuple[str, float, list[str]]:
    normalized_category = _normalize(category)
    if not is_valid_category(normalized_category):
        return "", 0.0, []

    text = " ".join(part for part in text_parts if part).lower()
    keyword_map = _DIRECTION_KEYWORDS.get(normalized_category, {})
    scores = {
        direction: sum(1 for keyword in keywords if keyword in text)
        for direction, keywords in keyword_map.items()
    }
    if not scores:
        return "", 0.0, []

    best_direction = max(scores, key=lambda direction: scores[direction])
    best_score = scores[best_direction]
    if best_score <= 0:
        return "", 0.0, []

    alternatives = [
        direction
        for direction, score in sorted(scores.items(), key=lambda item: item[1], reverse=True)
        if direction != best_direction and score > 0
    ]
    confidence = min(0.55 + best_score * 0.1, 0.9)
    return best_direction, confidence, alternatives[:3]


def _normalize(value: str | None) -> str:
    return (value or "").strip()
