"""Advisory direction agent for task 08."""

from __future__ import annotations

from paper_pipeline.agents.base import BaseSubAgent
from paper_pipeline.domain import AgentResult, PaperMetadata
from paper_pipeline.taxonomy.categories import validate_category
from paper_pipeline.taxonomy.directions import infer_direction, validate_direction


class DirectionAgent(BaseSubAgent):
    agent_name = "DirectionAgent"

    def run(self, task_id: str, metadata: PaperMetadata) -> AgentResult:
        category_result = validate_category(metadata.category)
        if not category_result.is_valid:
            return self._uncertain(
                task_id=task_id,
                metadata=metadata,
                category=metadata.category,
                reason="方向判断需要先有明确固定大类",
                evidence=["metadata.category"],
            )

        if metadata.direction:
            direction_result = validate_direction(metadata.category, metadata.direction)
            if direction_result.is_valid:
                return AgentResult(
                    agent_name=self.agent_name,
                    task_id=task_id,
                    paper_id=metadata.id,
                    run_id=metadata.run_id,
                    status="success",
                    decision={
                        "category": category_result.value,
                        "direction": direction_result.value,
                        "alternative_directions": [],
                        "needs_review": False,
                    },
                    confidence=0.98,
                    reason="已有方向属于对应大类固定方向名单",
                    evidence=["metadata.category", "metadata.direction"],
                    suggested_next_action="提交总智能体汇总，等待最终 metadata 写入",
                )

            return self._uncertain(
                task_id=task_id,
                metadata=metadata,
                category=category_result.value,
                reason=direction_result.reason,
                evidence=["metadata.direction"],
            )

        direction, confidence, alternatives = infer_direction(
            category_result.value,
            _metadata_text_parts(metadata),
        )
        if not direction:
            return self._uncertain(
                task_id=task_id,
                metadata=metadata,
                category=category_result.value,
                reason="方向证据不足或 v1 seed 方向库未覆盖，不能自由创造方向",
                evidence=[],
            )

        return AgentResult(
            agent_name=self.agent_name,
            task_id=task_id,
            paper_id=metadata.id,
            run_id=metadata.run_id,
            status="success",
            decision={
                "category": category_result.value,
                "direction": direction,
                "alternative_directions": alternatives,
                "needs_review": False,
            },
            confidence=confidence,
            reason="根据题名、摘要和关键词命中固定方向规则",
            evidence=["title", "abstract", "keywords"],
            suggested_next_action="提交总智能体汇总，等待最终 metadata 写入",
        )

    def _uncertain(
        self,
        *,
        task_id: str,
        metadata: PaperMetadata,
        category: str,
        reason: str,
        evidence: list[str],
    ) -> AgentResult:
        return AgentResult(
            agent_name=self.agent_name,
            task_id=task_id,
            paper_id=metadata.id,
            run_id=metadata.run_id,
            status="uncertain",
            decision={
                "category": category,
                "direction": "",
                "alternative_directions": [],
                "needs_review": True,
            },
            confidence=0.0,
            reason=reason,
            evidence=evidence,
            needs_review=True,
            suggested_next_action="转入 pending 复审，由总智能体或人工确认方向",
        )


def _metadata_text_parts(metadata: PaperMetadata) -> list[str]:
    return [
        metadata.title,
        metadata.major,
        metadata.abstract,
        " ".join(metadata.keywords),
        " ".join(metadata.chapter_types),
    ]
