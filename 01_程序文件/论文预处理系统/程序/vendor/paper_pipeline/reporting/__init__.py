"""Run reporting, evidence, and manifest generation."""

from paper_pipeline.reporting.evidence import EvidenceCollector, RunEvidenceBundle
from paper_pipeline.reporting.report_generator import (
    ReportGenerationContext,
    ReportGenerationResult,
    generate_run_report,
)

__all__ = [
    "EvidenceCollector",
    "ReportGenerationContext",
    "ReportGenerationResult",
    "RunEvidenceBundle",
    "generate_run_report",
]
