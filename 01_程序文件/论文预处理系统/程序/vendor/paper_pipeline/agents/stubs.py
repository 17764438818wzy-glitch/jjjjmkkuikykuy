"""Stub sub-agents used by the orchestrator skeleton."""

from __future__ import annotations

from typing import ClassVar

from paper_pipeline.agents.base import BaseSubAgent
from paper_pipeline.domain import AgentResult, PaperMetadata, Tier


class _SuccessfulStubAgent(BaseSubAgent):
    decision: ClassVar[dict[str, object]] = {}
    confidence: ClassVar[float] = 0.6
    reason: ClassVar[str] = "stub 小智能体返回占位判断"
    suggested_next_action: ClassVar[str] = "交由总智能体汇总"

    def run(self, task_id: str, metadata: PaperMetadata) -> AgentResult:
        return AgentResult(
            agent_name=self.agent_name,
            task_id=task_id,
            paper_id=metadata.id,
            run_id=metadata.run_id,
            status="success",
            decision=dict(self.decision),
            confidence=self.confidence,
            reason=self.reason,
            evidence=["stub_result"],
            suggested_next_action=self.suggested_next_action,
        )


class Stage0AdmissionAgentStub(_SuccessfulStubAgent):
    agent_name = "Stage0AdmissionAgentStub"
    decision = {"admitted": True}
    reason = "stage0 stub 默认准入"


class CategoryAgentStub(_SuccessfulStubAgent):
    agent_name = "CategoryAgentStub"
    decision = {"category": "uncategorized_stub"}
    reason = "分类 stub 未做真实分类"


class DirectionAgentStub(_SuccessfulStubAgent):
    agent_name = "DirectionAgentStub"
    decision = {"direction": "undetermined_stub"}
    reason = "方向 stub 未做真实方向判断"


class ResearchMethodAgentStub(_SuccessfulStubAgent):
    agent_name = "ResearchMethodAgentStub"
    decision = {"research_method_primary": "undetermined_stub"}
    reason = "研究方法 stub 未做真实方法识别"


class QualityTierAgentStub(_SuccessfulStubAgent):
    agent_name = "QualityTierAgentStub"
    decision = {"suggested_tier": Tier.SILVER.value}
    reason = "等级 stub 只给总智能体参考建议，不写入最终等级"


class DuplicateVersionAgentStub(_SuccessfulStubAgent):
    agent_name = "DuplicateVersionAgentStub"
    decision = {"duplicate_status": "unique_stub"}
    reason = "重复版本 stub 未做真实查重"


class PendingReviewAgentStub(_SuccessfulStubAgent):
    agent_name = "PendingReviewAgentStub"
    decision = {"needs_review": False}
    reason = "复审 stub 默认不要求人工复审"


class SqlEligibilityAgentStub(_SuccessfulStubAgent):
    agent_name = "SqlEligibilityAgentStub"
    high_risk_action = True
    decision = {"sql_eligible": True}
    reason = "SQL 资格 stub 只返回资格判断，不上传 SQL"
