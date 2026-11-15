"""Advisory research-method agent for task 08."""

from __future__ import annotations

from typing import Literal

from paper_pipeline.agents.base import BaseSubAgent
from paper_pipeline.domain import AgentResult, PaperMetadata
from paper_pipeline.taxonomy.methods import normalize_research_methods


class ResearchMethodAgent(BaseSubAgent):
    agent_name = "ResearchMethodAgent"

    def run(self, task_id: str, metadata: PaperMetadata) -> AgentResult:
        normalized = normalize_research_methods(
            primary=metadata.research_method_primary or "",
            tags=metadata.research_method_tags,
            text=_metadata_text(metadata),
        )
        status: Literal["success", "uncertain"] = (
            "uncertain" if normalized.needs_review else "success"
        )
        confidence = 0.35 if normalized.needs_review else 0.85
        reason = normalized.reason or "研究方法和辅助标签均来自固定名单校验"

        return AgentResult(
            agent_name=self.agent_name,
            task_id=task_id,
            paper_id=metadata.id,
            run_id=metadata.run_id,
            status=status,
            decision={
                "research_method_primary": normalized.research_method_primary,
                "research_method_tags": normalized.research_method_tags,
                "suggested_tags": normalized.suggested_tags,
                "needs_review": normalized.needs_review,
            },
            confidence=confidence,
            reason=reason,
            evidence=["metadata.research_method_primary", "metadata.research_method_tags"],
            needs_review=normalized.needs_review,
            suggested_next_action=(
                "转入 pending 复审，确认研究方法"
                if normalized.needs_review
                else "提交总智能体汇总，等待最终 metadata 写入"
            ),
        )


def _metadata_text(metadata: PaperMetadata) -> str:
    return " ".join(
        [
            metadata.title,
            metadata.major,
            metadata.abstract,
            " ".join(metadata.keywords),
            " ".join(metadata.chapter_types),
        ],
    )
