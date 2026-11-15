"""Missing real data reminder rules."""

from __future__ import annotations

import re

from paper_pipeline.vector_retrieval.models import MaterialItem, RetrievalRequest


SPORTS_REAL_DATA_FIELDS = [
    "研究对象年龄",
    "人数",
    "性别",
    "班级 / 队伍",
    "实验组 / 对照组",
    "是否随机分组",
    "干预周期",
    "每周训练次数",
    "每次训练时长",
    "训练动作与组数",
    "测试指标",
    "前测 / 后测数据",
    "统计方法",
    "场地与器材",
    "安全与伦理说明",
]


class MissingRealDataService:
    def missing_items(
        self,
        request: RetrievalRequest,
        materials: list[MaterialItem],
        *,
        package_type: str,
    ) -> list[str]:
        if package_type != "sports_training_intervention":
            return ["真实数据支撑"] if _needs_data(materials) else []
        found_text = "\n".join(item.original_chunk for item in materials)
        missing = [
            field
            for field in SPORTS_REAL_DATA_FIELDS
            if not _field_covered(field, found_text)
        ]
        if _looks_like_sports_experiment(request):
            return missing
        return missing[:5] if _needs_data(materials) else []


def _needs_data(materials: list[MaterialItem]) -> bool:
    return any(item.needs_real_data_check for item in materials)


def _looks_like_sports_experiment(request: RetrievalRequest) -> bool:
    text = f"{request.query_title} {request.direction} {request.thesis_type}"
    return any(term in text for term in ("体育", "训练", "干预", "实验", "身体素质"))


def _field_covered(field: str, text: str) -> bool:
    if field == "前测 / 后测数据":
        return "前测" in text and "后测" in text and bool(re.search(r"\d", text))
    aliases = {
        "实验组 / 对照组": ("实验组", "对照组"),
        "每周训练次数": ("每周", "训练"),
        "每次训练时长": ("每次", "时长"),
        "训练动作与组数": ("训练动作", "组数"),
        "安全与伦理说明": ("安全", "伦理"),
    }
    terms = aliases.get(field, (field.split(" / ")[0],))
    return all(term in text for term in terms)
