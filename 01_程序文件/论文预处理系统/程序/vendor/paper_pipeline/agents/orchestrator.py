"""Top-level agent that coordinates sub-agents and summarizes their results."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Iterable, Sequence

from paper_pipeline.agents.base import BaseSubAgent
from paper_pipeline.domain import AgentResult, PaperMetadata, Tier


def _now_utc() -> datetime:
    return datetime.now(UTC)


@dataclass(slots=True)
class OrchestratorRunResult:
    task_id: str
    paper_id: str
    run_id: str
    results: list[AgentResult]
    suggested_tier: Tier
    needs_review: bool
    blocking: bool
    sql_blocked: bool
    summary: str
    created_at: datetime = field(default_factory=_now_utc)


class PipelineOrchestratorAgent:
    """Coordinates sub-agents; only this layer faces callers."""

    def __init__(self, subagents: Iterable[BaseSubAgent] | None = None) -> None:
        self._subagents: dict[str, BaseSubAgent] = {}
        self.result_history: list[AgentResult] = []
        for subagent in subagents or ():
            self.register_subagent(subagent)

    def register_subagent(self, subagent: BaseSubAgent) -> None:
        self._subagents[subagent.agent_name] = subagent

    def run_subagent(
        self,
        agent_name: str,
        task_id: str,
        metadata: PaperMetadata,
    ) -> AgentResult:
        subagent = self._subagents[agent_name]
        result = subagent.run(task_id=task_id, metadata=metadata)
        self.result_history.append(result)
        return result

    def run(
        self,
        task_id: str,
        metadata: PaperMetadata,
        agent_names: Sequence[str] | None = None,
    ) -> OrchestratorRunResult:
        selected_names = list(agent_names) if agent_names is not None else list(self._subagents)
        results: list[AgentResult] = []
        blocking = False

        for agent_name in selected_names:
            subagent = self._subagents[agent_name]
            if blocking and subagent.high_risk_action:
                continue

            result = self.run_subagent(
                agent_name=agent_name,
                task_id=task_id,
                metadata=metadata,
            )
            results.append(result)
            blocking = blocking or result.blocking

        has_uncertain = any(result.status == "uncertain" for result in results)
        needs_review = has_uncertain or any(result.needs_review for result in results)
        suggested_tier = Tier.PENDING if has_uncertain else Tier.SILVER
        sql_blocked = blocking
        summary = self._build_summary(
            task_id=task_id,
            metadata=metadata,
            results=results,
            suggested_tier=suggested_tier,
            needs_review=needs_review,
            blocking=blocking,
            sql_blocked=sql_blocked,
        )

        return OrchestratorRunResult(
            task_id=task_id,
            paper_id=metadata.id,
            run_id=metadata.run_id,
            results=results,
            suggested_tier=suggested_tier,
            needs_review=needs_review,
            blocking=blocking,
            sql_blocked=sql_blocked,
            summary=summary,
        )

    def _build_summary(
        self,
        task_id: str,
        metadata: PaperMetadata,
        results: Sequence[AgentResult],
        suggested_tier: Tier,
        needs_review: bool,
        blocking: bool,
        sql_blocked: bool,
    ) -> str:
        statuses = "、".join(
            f"{result.agent_name}={result.status}" for result in results
        )
        review_text = "建议待复审" if needs_review else "暂不需要人工复审"
        blocking_text = "已阻断 SQL 入库等高风险动作" if sql_blocked else "未触发 SQL 阻断"
        return (
            "中文汇总："
            f"任务 {task_id} 已完成论文 {metadata.id} 的小智能体调度；"
            f"共记录 {len(results)} 个结果：{statuses}。"
            f"{review_text}；建议等级为 {suggested_tier.value}。"
            f"{blocking_text}。"
            f"blocking={blocking}。"
        )
