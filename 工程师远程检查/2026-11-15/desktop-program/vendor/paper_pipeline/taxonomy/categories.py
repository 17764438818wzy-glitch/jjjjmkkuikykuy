"""Fixed category taxonomy for task 08 classification suggestions."""

from __future__ import annotations

from collections.abc import Iterable

from paper_pipeline.taxonomy.validation import (
    TaxonomyValidationResult,
    invalid_taxonomy_result,
    valid_taxonomy_result,
)

CATEGORY_NAMES: tuple[str, ...] = (
    "经管财会类",
    "教育教学类",
    "医护健康类",
    "法学社会公共类",
    "文学外语传媒类",
    "艺术设计类",
    "体育健康与休闲运动类",
    "计算机软件与数据类",
    "智能硬件与控制类",
    "机械制造与机电类",
    "土木建筑与交通类",
    "电气电子与通信能源类",
    "化工材料环境安全类",
    "农林生物资源类",
    "理学统计心理类",
)

_CATEGORY_KEYWORDS: dict[str, tuple[str, ...]] = {
    "经管财会类": (
        "会计",
        "财务",
        "审计",
        "企业管理",
        "市场营销",
        "电子商务",
        "供应链",
        "金融",
        "经济",
    ),
    "教育教学类": (
        "教学",
        "课程",
        "课堂",
        "学习",
        "学生",
        "教育",
        "教师",
        "幼儿园",
    ),
    "医护健康类": (
        "护理",
        "医院",
        "患者",
        "健康",
        "康复",
        "临床",
        "医学",
    ),
    "法学社会公共类": (
        "法律",
        "法治",
        "公共管理",
        "社会治理",
        "社区",
        "政策",
    ),
    "文学外语传媒类": (
        "翻译",
        "英语",
        "文学",
        "新闻",
        "传播",
        "传媒",
    ),
    "艺术设计类": (
        "视觉传达",
        "艺术",
        "设计",
        "包装",
        "品牌",
        "动画",
        "室内设计",
    ),
    "体育健康与休闲运动类": (
        "体育",
        "运动",
        "健身",
        "体能",
        "训练",
        "教学",
        "青少年运动",
        "休闲体育",
    ),
    "计算机软件与数据类": (
        "软件",
        "系统",
        "平台",
        "数据库",
        "前后端",
        "spring boot",
        "vue",
        "python",
        "算法",
        "数据分析",
        "机器学习",
        "网站",
        "小程序",
    ),
    "智能硬件与控制类": (
        "单片机",
        "stm32",
        "plc",
        "控制系统",
        "传感器",
        "嵌入式",
        "物联网",
    ),
    "机械制造与机电类": (
        "机械",
        "机电",
        "数控",
        "夹具",
        "模具",
        "机械设计",
    ),
    "土木建筑与交通类": (
        "建筑",
        "施工",
        "土木",
        "道路",
        "桥梁",
        "交通",
        "bim",
    ),
    "电气电子与通信能源类": (
        "电气",
        "电子",
        "通信",
        "电力",
        "新能源",
        "光伏",
    ),
    "化工材料环境安全类": (
        "化工",
        "材料",
        "环境",
        "安全",
        "污水",
        "污染",
    ),
    "农林生物资源类": (
        "农业",
        "林业",
        "植物",
        "养殖",
        "生物",
        "园林",
    ),
    "理学统计心理类": (
        "数学",
        "统计",
        "心理",
        "物理",
        "化学",
        "模型",
    ),
}


def is_valid_category(category: str) -> bool:
    return _normalize(category) in CATEGORY_NAMES


def validate_category(category: str) -> TaxonomyValidationResult:
    normalized = _normalize(category)
    if normalized in CATEGORY_NAMES:
        return valid_taxonomy_result(normalized)
    reason = "分类为空，需进入 pending 复审" if not normalized else "分类不在固定大类名单中"
    return invalid_taxonomy_result(normalized, reason)


def infer_category(text_parts: Iterable[str]) -> tuple[str, float, list[str]]:
    text = _search_text(text_parts)
    scores = {
        category: sum(1 for keyword in keywords if keyword in text)
        for category, keywords in _CATEGORY_KEYWORDS.items()
    }
    best_category = max(scores, key=lambda category: scores[category])
    best_score = scores[best_category]
    if best_score <= 0:
        return "", 0.0, []

    alternatives = [
        category
        for category, score in sorted(scores.items(), key=lambda item: item[1], reverse=True)
        if category != best_category and score > 0
    ]
    confidence = min(0.55 + best_score * 0.1, 0.92)
    return best_category, confidence, alternatives[:3]


def _normalize(value: str | None) -> str:
    return (value or "").strip()


def _search_text(text_parts: Iterable[str]) -> str:
    return " ".join(part for part in text_parts if part).lower()
