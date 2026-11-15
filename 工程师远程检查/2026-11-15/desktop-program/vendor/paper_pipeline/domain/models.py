"""Domain data structures shared across pipeline stages."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal

from paper_pipeline.domain.enums import (
    DuplicateStatus,
    FileRuntimeStatus,
    IndexStatus,
    ProcessingPlanStatus,
    RunStatus,
    SqlStatus,
    Tier,
    VersionStatus,
)


def _now_utc() -> datetime:
    return datetime.now(UTC)


@dataclass(slots=True)
class RiskFlag:
    code: str
    message: str
    severity: str = "warning"


@dataclass(slots=True)
class ReviewHistoryItem:
    reviewer: str
    action: str
    comment: str = ""
    reviewed_at: datetime = field(default_factory=_now_utc)
    operator: str = ""
    created_at: datetime | None = None
    old_tier: str = ""
    new_tier: str = ""
    reason: str = ""
    evidence: list[str] = field(default_factory=list)
    agent_name: str = ""
    decision_source: str = ""
    model: str = ""
    decision: str = ""
    duplicate_status: str = ""
    similar_to: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.operator:
            self.operator = self.reviewer
        if self.created_at is None:
            self.created_at = self.reviewed_at


@dataclass(slots=True)
class PaperIdentity:
    id: str
    version: int = 1
    source_hash: str = ""


@dataclass(slots=True)
class SourceFileRecord:
    path: str = ""
    source_hash: str = ""
    size_bytes: int = 0
    discovered_at: datetime = field(default_factory=_now_utc)
    source_path: str = ""
    source_name: str = ""
    source_size: int = 0
    mtime: float = 0.0
    first_seen_at: datetime | None = None
    is_duplicate: bool = False
    duplicate_status: DuplicateStatus = DuplicateStatus.UNIQUE
    duplicate_of: str = ""
    skip_reason: str = ""
    all_sources_for_hash: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.source_path and self.path:
            self.source_path = self.path
        if not self.path and self.source_path:
            self.path = self.source_path
        if not self.source_size and self.size_bytes:
            self.source_size = self.size_bytes
        if not self.size_bytes and self.source_size:
            self.size_bytes = self.source_size
        if not self.source_name and self.source_path:
            self.source_name = self.source_path.rsplit("\\", maxsplit=1)[-1].rsplit(
                "/",
                maxsplit=1,
            )[-1]
        if self.first_seen_at is None:
            self.first_seen_at = self.discovered_at


@dataclass(slots=True)
class ProcessingPlan:
    run_id: str
    paper_id: str = ""
    steps: list[str] = field(default_factory=list)
    source_path: str = ""
    created_at: datetime = field(default_factory=_now_utc)
    input_dir: str = ""
    library_dir: str = ""
    output_cleaned_dir: str = ""
    total_scanned_count: int = 0
    new_file_count: int = 0
    duplicate_skip_count: int = 0
    estimated_process_count: int = 0
    batch_size: int = 50
    batch_count: int = 0
    will_write_sql: bool = False
    requires_user_sql_confirmation: bool = True
    status: ProcessingPlanStatus = ProcessingPlanStatus.DRAFT
    batches: list[BatchPlan] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not isinstance(self.status, ProcessingPlanStatus):
            self.status = ProcessingPlanStatus(self.status)
        if self.batch_size <= 0:
            raise ValueError("batch_size must be greater than 0")
        self.will_write_sql = False
        self.requires_user_sql_confirmation = True
        if self.new_file_count and not self.estimated_process_count:
            self.estimated_process_count = self.new_file_count
        if self.batches and not self.batch_count:
            self.batch_count = len(self.batches)
        self._ensure_required_notes()

    def _ensure_required_notes(self) -> None:
        required_notes = [
            "本阶段不会写入 SQL。",
            "SQL 入库需要用户人工确认。",
        ]
        for note in required_notes:
            if note not in self.notes:
                self.notes.append(note)


@dataclass(slots=True)
class BatchPlan:
    run_id: str
    plans: list[ProcessingPlan] = field(default_factory=list)
    created_at: datetime = field(default_factory=_now_utc)
    batch_index: int = 0
    batch_size: int = 50
    file_count: int = 0
    source_paths: list[str] = field(default_factory=list)


@dataclass(slots=True)
class AgentResult:
    agent_name: str
    success: bool = True
    message: str = ""
    data: dict[str, object] = field(default_factory=dict)
    task_id: str = ""
    paper_id: str = ""
    run_id: str = ""
    status: Literal["success", "failed", "uncertain"] = "success"
    decision: dict[str, object] = field(default_factory=dict)
    confidence: float = 0.0
    reason: str = ""
    evidence: list[str] = field(default_factory=list)
    risk_flags: list[RiskFlag] = field(default_factory=list)
    needs_review: bool = False
    blocking: bool = False
    suggested_next_action: str = ""
    created_at: datetime = field(default_factory=_now_utc)

    def __post_init__(self) -> None:
        if self.status == "success" and not self.success:
            self.status = "failed"
        self.success = self.status == "success"
        if not self.reason and self.message:
            self.reason = self.message
        if not self.decision and self.data:
            self.decision = dict(self.data)


@dataclass(slots=True)
class FileRunState:
    source_path: str
    batch_index: int = 0
    status: FileRuntimeStatus = FileRuntimeStatus.NOT_STARTED
    reason: str = ""
    attempts: int = 0
    started_at: datetime | None = None
    completed_at: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.status, FileRuntimeStatus):
            self.status = FileRuntimeStatus(self.status)


@dataclass(slots=True)
class BatchState:
    run_id: str
    batch_index: int
    file_count: int = 0
    status: RunStatus = RunStatus.CREATED
    completed_items: int = 0
    pending_items: int = 0
    failed_items: int = 0
    started_at: datetime | None = None
    completed_at: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.status, RunStatus):
            self.status = RunStatus(self.status)


@dataclass(slots=True)
class RuntimeArtifact:
    source_path: str
    artifact_uri: str
    temporary: bool = True
    formal_tier: Tier | None = None
    created_at: datetime = field(default_factory=_now_utc)

    def __post_init__(self) -> None:
        if self.formal_tier is not None and not isinstance(self.formal_tier, Tier):
            self.formal_tier = Tier(self.formal_tier)


@dataclass(slots=True)
class RunState:
    run_id: str
    status: RunStatus = RunStatus.CREATED
    total_items: int = 0
    completed_items: int = 0
    pending_items: int = 0
    failed_items: int = 0
    started_at: datetime = field(default_factory=_now_utc)
    completed_at: datetime | None = None
    updated_at: datetime = field(default_factory=_now_utc)
    current_batch_index: int = 0
    current_source_path: str = ""
    batch_states: list[BatchState] = field(default_factory=list)
    file_states: list[FileRunState] = field(default_factory=list)
    temporary_artifacts: list[RuntimeArtifact] = field(default_factory=list)
    completed_artifacts: list[RuntimeArtifact] = field(default_factory=list)
    sql_status: SqlStatus = SqlStatus.NOT_UPLOADED
    sql_upload_triggered: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.status, RunStatus):
            self.status = RunStatus(self.status)
        if not isinstance(self.sql_status, SqlStatus):
            self.sql_status = SqlStatus(self.sql_status)


@dataclass(slots=True)
class PaperMetadata:
    id: str
    version: int = 1
    title: str = ""
    year: int | None = None
    school: str = ""
    major: str = ""
    category: str = ""
    direction: str = ""
    research_method_primary: str | None = None
    research_method_tags: list[str] = field(default_factory=list)
    suggested_tags: list[str] = field(default_factory=list)
    tier: Tier = Tier.PENDING
    quality_tier_suggestion: str = ""
    gold_candidate: bool = False
    needs_codex_review: bool = False
    tier_decision_source: str = ""
    score: float | None = None
    quality_reason: str = ""
    chapter_types: list[str] = field(default_factory=list)
    abstract: str = ""
    abstract_source: str | None = None
    keywords: list[str] = field(default_factory=list)
    keywords_source: str | None = None
    sensitive_clean: bool = False
    risk_flags: list[RiskFlag] = field(default_factory=list)
    sql_status: SqlStatus = SqlStatus.NOT_UPLOADED
    version_status: VersionStatus = VersionStatus.CURRENT
    index_status: IndexStatus = IndexStatus.NOT_INDEXED
    source_hash: str = ""
    source_path: str = ""
    run_id: str = ""
    duplicate_status: DuplicateStatus = DuplicateStatus.UNIQUE
    similar_to: list[str] = field(default_factory=list)
    sql_uploaded_at: str = ""
    sql_batch_id: str = ""
    sql_preview_id: str = ""
    sql_uploaded_by: str = ""
    sql_upload_adapter: str = ""
    sql_upload_record_id: str = ""
    sql_upload_error: str = ""
    sql_audit_history: list[dict[str, object]] = field(default_factory=list)
    review_history: list[ReviewHistoryItem] = field(default_factory=list)
    created_at: datetime = field(default_factory=_now_utc)
    updated_at: datetime = field(default_factory=_now_utc)

    def add_review_history(self, item: ReviewHistoryItem) -> None:
        self.review_history.append(item)
        self.updated_at = _now_utc()
