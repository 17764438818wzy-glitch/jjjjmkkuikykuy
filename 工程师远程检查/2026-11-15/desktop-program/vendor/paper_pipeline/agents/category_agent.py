"""Advisory category agent for task 08."""

from __future__ import annotations

from paper_pipeline.agents.base import BaseSubAgent
from paper_pipeline.domain import AgentResult, PaperMetadata
from paper_pipeline.taxonomy.categories import infer_category, validate_category


class CategoryAgent(BaseSubAgent):
    agent_name = "CategoryAgent"

    def run(self, task_id: str, metadata: PaperMetadata) -> AgentResult:
        if metadata.category:
            validation = validate_category(metadata.category)
            if validation.is_valid:
                return AgentResult(
                    agent_name=self.agent_name,
                    task_id=task_id,
                    paper_id=metadata.id,
                    run_id=metadata.run_id,
                    status="success",
                    decision={
                        "category": validation.value,
                        "alternative_categories": [],
                        "needs_review": False,
                    },
                    confidence=0.98,
                    reason="已有分类属于固定大类名单",
                    evidence=["metadata.category"],
                    suggested_next_action="提交总智能体汇总，等待最终 metadata 写入",
                )

            return self._uncertain(
                task_id=task_id,
                metadata=metadata,
                reason=validation.reason,
                evidence=["metadata.category"],
            )

        category, confidence, alternatives = infer_category(_metadata_text_parts(metadata))
        if not category:
            return self._uncertain(
                task_id=task_id,
                metadata=metadata,
                reason="分类证据不足，不能硬猜固定大类",
                evidence=[],
            )

        return AgentResult(
            agent_name=self.agent_name,
            task_id=task_id,
            paper_id=metadata.id,
            run_id=metadata.run_id,
            status="success",
            decision={
                "category": category,
                "alternative_categories": alternatives,
                "needs_review": False,
            },
            confidence=confidence,
            reason="根据题名、专业、摘要和关键词命中固定大类规则",
            evidence=["title", "major", "abstract", "keywords"],
            suggested_next_action="提交总智能体汇总，等待最终 metadata 写入",
        )

    def _uncertain(
        self,
        *,
        task_id: str,
        metadata: PaperMetadata,
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
                "category": "",
                "alternative_categories": [],
                "needs_review": True,
            },
            confidence=0.0,
            reason=reason,
            evidence=evidence,
            needs_review=True,
            suggested_next_action="转入 pending 复审，由总智能体或人工确认分类",
        )


def _metadata_text_parts(metadata: PaperMetadata) -> list[str]:
    return [
        metadata.title,
        metadata.major,
        metadata.abstract,
        " ".join(metadata.keywords),
        " ".join(metadata.chapter_types),
    ]
