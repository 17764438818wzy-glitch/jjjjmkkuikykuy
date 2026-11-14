"""Sports experiment / training intervention material package adapter."""

from __future__ import annotations


ROLE_TO_SPORTS_SECTION = {
    "background": "研究背景与意义",
    "theory": "理论基础 / 概念界定",
    "method": "研究对象与方法",
    "data_result": "实验结果 / 数据分析材料",
    "problem_analysis": "现状问题 / 影响因素",
    "strategy": "对策建议 / 训练优化建议",
    "conclusion": "结论参考",
    "other": "辅助材料",
}

TRAINING_TERMS = ("训练方案", "教学干预", "干预设计", "每周训练", "训练动作")
TEST_TERMS = ("测试指标", "评价方法", "前测", "后测")


class SportsTrainingAdapter:
    package_type = "sports_training_intervention"

    def applies_to(self, major: str, direction: str, title: str) -> bool:
        text = f"{major} {direction} {title}"
        return any(term in text for term in ("体育", "训练", "干预", "身体素质"))

    def section_for(self, content_role: str, text: str) -> str:
        if any(term in text for term in TRAINING_TERMS):
            return "训练方案 / 教学干预设计"
        if any(term in text for term in TEST_TERMS):
            return "测试指标与评价方法"
        return ROLE_TO_SPORTS_SECTION.get(content_role, "辅助材料")

