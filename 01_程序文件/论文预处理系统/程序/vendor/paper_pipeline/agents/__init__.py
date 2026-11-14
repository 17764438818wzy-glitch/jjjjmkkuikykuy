"""Agent orchestration primitives for the local paper pipeline."""

from paper_pipeline.agents.base import BaseSubAgent
from paper_pipeline.agents.category_agent import CategoryAgent
from paper_pipeline.agents.direction_agent import DirectionAgent
from paper_pipeline.agents.orchestrator import (
    OrchestratorRunResult,
    PipelineOrchestratorAgent,
)
from paper_pipeline.agents.pending_review_agent import PendingReviewAgent
from paper_pipeline.agents.quality_tier_agent import QualityTierAgent
from paper_pipeline.agents.research_method_agent import ResearchMethodAgent
from paper_pipeline.agents.stubs import (
    CategoryAgentStub,
    DirectionAgentStub,
    DuplicateVersionAgentStub,
    PendingReviewAgentStub,
    QualityTierAgentStub,
    ResearchMethodAgentStub,
    SqlEligibilityAgentStub,
    Stage0AdmissionAgentStub,
)

__all__ = [
    "BaseSubAgent",
    "CategoryAgent",
    "CategoryAgentStub",
    "DirectionAgent",
    "DirectionAgentStub",
    "DuplicateVersionAgentStub",
    "OrchestratorRunResult",
    "PendingReviewAgent",
    "PendingReviewAgentStub",
    "PipelineOrchestratorAgent",
    "QualityTierAgent",
    "QualityTierAgentStub",
    "ResearchMethodAgent",
    "ResearchMethodAgentStub",
    "SqlEligibilityAgentStub",
    "Stage0AdmissionAgentStub",
]
