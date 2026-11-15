"""Structured stage models for the local Dity pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class PipelineStageName(StrEnum):
    NORMALIZE_CONVERT = "normalize_convert"
    DOCUMENT_PARSE = "stage2_document_parse"
    BLOCK_GENERATION = "block_generation"
    DESENSITIZE_CLEAN = "desensitize_clean"
    BASIC_QUALITY_FILTER = "basic_quality_filter"
    LLM_QUALITY_FILTER = "llm_quality_filter"
    CHAPTER_STRUCTURE = "chapter_structure"
    FINAL_QUALITY_CHECK = "final_quality_check"
    MARKDOWN_OUTPUT = "markdown_output"


class PipelineStageStatus(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"
    PENDING = "pending"
    SKIPPED = "skipped"
    TIMEOUT = "timeout"


DEFAULT_PIPELINE_STAGE_ORDER: tuple[PipelineStageName, ...] = (
    PipelineStageName.NORMALIZE_CONVERT,
    PipelineStageName.DOCUMENT_PARSE,
    PipelineStageName.BLOCK_GENERATION,
    PipelineStageName.DESENSITIZE_CLEAN,
    PipelineStageName.BASIC_QUALITY_FILTER,
    PipelineStageName.LLM_QUALITY_FILTER,
    PipelineStageName.CHAPTER_STRUCTURE,
    PipelineStageName.FINAL_QUALITY_CHECK,
    PipelineStageName.MARKDOWN_OUTPUT,
)


@dataclass(slots=True)
class PipelineStageResult:
    stage_name: PipelineStageName
    status: PipelineStageStatus
    input_path: str
    output_path: str
    logs: list[str] = field(default_factory=list)
    error: str = ""
    duration_seconds: float = 0.0
    risk_flags: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not isinstance(self.stage_name, PipelineStageName):
            self.stage_name = PipelineStageName(self.stage_name)
        if not isinstance(self.status, PipelineStageStatus):
            self.status = PipelineStageStatus(self.status)
