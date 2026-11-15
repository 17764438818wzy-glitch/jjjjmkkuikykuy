"""Base interface for sub-agents managed by the pipeline orchestrator."""

from __future__ import annotations

from abc import ABC, abstractmethod

from paper_pipeline.domain import AgentResult, PaperMetadata


class BaseSubAgent(ABC):
    """Small agents report only to the orchestrator through AgentResult."""

    agent_name: str
    high_risk_action: bool = False

    @abstractmethod
    def run(self, task_id: str, metadata: PaperMetadata) -> AgentResult:
        """Return a unified result without mutating final pipeline decisions."""
