"""DTOs used by the Windows workbench and tray entrypoints."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True)
class WorkbenchFileItem:
    source_path: str
    file_name: str
    status: str
    reason: str = ""
    final_markdown_path: str = ""
    metadata_path: str = ""
    selected: bool = True
    full_path: str = ""
    file_type: str = ""
    size_bytes: int = 0
    supported: bool = True
    route: str = ""
    stage: str = ""

    def __post_init__(self) -> None:
        if not self.full_path:
            self.full_path = self.source_path
        if not self.file_type and "." in self.file_name:
            self.file_type = self.file_name.rsplit(".", maxsplit=1)[-1].lower()
        if not self.stage:
            self.stage = self.status

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class WorkbenchConfigItem:
    name: str
    status: str
    detail: str = ""
    key: str = ""
    severity: str = "info"
    source: str = ""
    values: dict[str, str] = field(default_factory=dict)
    missing_keys: list[str] = field(default_factory=list)
    action_hint: str = ""

    def __post_init__(self) -> None:
        if not self.key:
            self.key = self.name.lower().replace(" ", "_").replace("/", "_")
        if not self.severity:
            self.severity = "info"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class WorkbenchOverviewAgentMessage:
    role: str
    content: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class WorkbenchStatusSummary:
    pending_count: int = 0
    failed_count: int = 0
    sql_blocked_count: int = 0
    stage0_rejected_count: int = 0
    indexed_count: int = 0
    index_failed_count: int = 0
    sql_eligible_count: int = 0
    sql_uploaded_count: int = 0
    gold_count: int = 0
    silver_count: int = 0
    pending_tier_count: int = 0
    rejected_count: int = 0
    current_stage: str = "未启动"
    output_dir: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class WorkbenchState:
    current_run_id: str = ""
    run_status: str = "not_started"
    plan_status: str = "missing"
    can_start: bool = False
    can_pause: bool = False
    can_stop: bool = False
    can_continue: bool = False
    can_abandon: bool = False
    scanned_count: int = 0
    new_file_count: int = 0
    duplicate_count: int = 0
    total_batches: int = 0
    completed_batches: int = 0
    completed_count: int = 0
    pending_count: int = 0
    failed_count: int = 0
    stage0_rejected_count: int = 0
    indexed_count: int = 0
    index_failed_count: int = 0
    sql_eligible_count: int = 0
    sql_blocked_count: int = 0
    sql_uploaded_count: int = 0
    latest_report_path: str = ""
    latest_evidence_bundle_path: str = ""
    latest_notification_path: str = ""
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    suggested_next_action: str = "请先显式扫描并生成处理计划。"
    gold_count: int = 0
    silver_count: int = 0
    pending_tier_count: int = 0
    rejected_count: int = 0
    current_stage: str = "未启动"
    output_dir: str = ""
    input_dir: str = ""
    library_dir: str = ""
    requested_file_count: int = 5
    config_items: list[WorkbenchConfigItem] = field(default_factory=list)
    file_items: list[WorkbenchFileItem] = field(default_factory=list)
    service_url: str = "http://127.0.0.1:7861/local"
    summary: WorkbenchStatusSummary = field(default_factory=WorkbenchStatusSummary)
    run_progress: Any | None = None
    review_items: list[Any] = field(default_factory=list)
    index_state: Any | None = None
    sql_upload_state: Any | None = None
    history_items: list[Any] = field(default_factory=list)
    selected_config_key: str = ""
    config_panel: Any | None = None
    active_section: str = "overview"
    overview_agent_available: bool = False
    overview_agent_question: str = ""
    overview_agent_answer: str = ""
    overview_agent_message: str = ""
    overview_agent_messages: list[WorkbenchOverviewAgentMessage] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.summary == WorkbenchStatusSummary():
            self.summary = WorkbenchStatusSummary(
                pending_count=self.pending_count,
                failed_count=self.failed_count,
                sql_blocked_count=self.sql_blocked_count,
                stage0_rejected_count=self.stage0_rejected_count,
                indexed_count=self.indexed_count,
                index_failed_count=self.index_failed_count,
                sql_eligible_count=self.sql_eligible_count,
                sql_uploaded_count=self.sql_uploaded_count,
                gold_count=self.gold_count,
                silver_count=self.silver_count,
                pending_tier_count=self.pending_tier_count,
                rejected_count=self.rejected_count,
                current_stage=self.current_stage,
                output_dir=self.output_dir,
            )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["summary"] = self.summary.to_dict()
        payload["config_items"] = [item.to_dict() for item in self.config_items]
        payload["file_items"] = [item.to_dict() for item in self.file_items]
        payload["overview_agent_messages"] = [
            item.to_dict() for item in self.overview_agent_messages
        ]
        return payload
