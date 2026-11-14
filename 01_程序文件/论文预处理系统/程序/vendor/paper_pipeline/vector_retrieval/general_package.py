"""General material package section mapping."""

from __future__ import annotations


ROLE_TO_GENERAL_SECTION = {
    "background": "研究背景材料",
    "theory": "理论基础材料",
    "method": "研究方法材料",
    "data_result": "数据 / 结果材料",
    "problem_analysis": "问题与原因分析材料",
    "strategy": "对策建议材料",
    "conclusion": "结论参考材料",
    "other": "辅助材料",
}


class GeneralPackageAdapter:
    def section_for_role(self, content_role: str) -> str:
        return ROLE_TO_GENERAL_SECTION.get(content_role, "辅助材料")

