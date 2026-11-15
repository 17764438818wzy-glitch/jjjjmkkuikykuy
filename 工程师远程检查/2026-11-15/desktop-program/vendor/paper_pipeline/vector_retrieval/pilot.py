"""Pilot evaluation helpers."""

from __future__ import annotations

from paper_pipeline.vector_retrieval.models import FormalResource, PilotEvaluation


class PilotEvaluationService:
    def select_pilot_resources(
        self,
        resources: list[FormalResource],
        *,
        limit: int = 50,
    ) -> list[FormalResource]:
        return [
            resource for resource in resources if resource.is_sports_pilot_candidate()
        ][:limit]

    def default_topics(self) -> list[str]:
        return [
            "花样跳绳训练对 6-7 岁儿童身体素质影响的研究",
            "核心力量训练对竞技健美操运动员空中跳跃类动作的影响研究",
            "男子板鞋竞速 100m 冠军途中跑技术特征分析",
        ]

    def is_passed(self, evaluation: PilotEvaluation) -> bool:
        usable_count = sum(
            1 for item in evaluation.topic_results if item.get("usable") is True
        )
        return usable_count >= 2

