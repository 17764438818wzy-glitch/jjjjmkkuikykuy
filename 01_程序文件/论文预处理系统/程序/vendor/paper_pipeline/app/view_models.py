"""Pure view models for the Tk workbench."""

from __future__ import annotations

from dataclasses import dataclass, field

from paper_pipeline.app.app_state import WorkbenchState


@dataclass(frozen=True, slots=True)
class StatusBadge:
    label: str
    severity: str


@dataclass(frozen=True, slots=True)
class WorkbenchSummaryViewModel:
    title: str
    current_run_id: str
    run_status: StatusBadge
    current_stage: str
    flow_entry: str
    input_dir: str


@dataclass(frozen=True, slots=True)
class WorkbenchSectionViewModel:
    key: str
    title: str
    rows: list[object] = field(default_factory=list)
    empty_text: str = ""


@dataclass(frozen=True, slots=True)
class WorkbenchNavItem:
    key: str
    title: str
    badge: str = ""
    severity: str = "muted"
    active: bool = False


@dataclass(frozen=True, slots=True)
class WorkbenchStatusRailSection:
    title: str
    lines: list[str] = field(default_factory=list)
    severity: str = "muted"


@dataclass(frozen=True, slots=True)
class WorkbenchOperationAction:
    key: str
    label: str
    callback_key: str
    enabled: bool = True
    severity: str = "secondary"


@dataclass(frozen=True, slots=True)
class WorkbenchOperationConsole:
    title: str
    detail: str
    lines: list[str] = field(default_factory=list)
    actions: list[WorkbenchOperationAction] = field(default_factory=list)
    severity: str = "muted"


@dataclass(frozen=True, slots=True)
class WorkbenchPageViewModel:
    summary: WorkbenchSummaryViewModel
    sections: list[WorkbenchSectionViewModel]
    can_start: bool
    start_disabled_reason: str
    operation_console: WorkbenchOperationConsole = field(
        default_factory=lambda: WorkbenchOperationConsole(
            title="选择文件开始",
            detail="从 NAS 目录扫描或选择单个文件。",
        ),
    )
    selected_config_key: str = ""
    config_panel: object | None = None
    active_section: str = "overview"
    nav_items: list[WorkbenchNavItem] = field(default_factory=list)
    active_section_model: WorkbenchSectionViewModel = field(
        default_factory=lambda: WorkbenchSectionViewModel(
            key="overview",
            title="总览",
            empty_text="暂无状态。",
        ),
    )
    run_progress: object | None = None
    status_rail_sections: list[WorkbenchStatusRailSection] = field(default_factory=list)
    status_rail_collapsed: bool = False
    overview_agent_available: bool = False
    overview_agent_question: str = ""
    overview_agent_answer: str = ""
    overview_agent_message: str = ""
    overview_agent_messages: list[object] = field(default_factory=list)


def status_badge(status: str) -> StatusBadge:
    normalized = status.strip() or "not_started"
    severity_by_status = {
        "completed": "success",
        "uploaded": "success",
        "approved": "success",
        "running": "info",
        "plan_ready": "info",
        "queued": "info",
        "pending": "warning",
        "review_required": "warning",
        "sql_blocked": "warning",
        "failed": "danger",
        "rejected": "danger",
        "not_started": "muted",
    }
    return StatusBadge(
        label=display_status(normalized),
        severity=severity_by_status.get(normalized, "info"),
    )


def display_status(status: str) -> str:
    normalized = status.strip() or "not_started"
    labels = {
        "not_started": "未开始",
        "plan_ready": "待开始",
        "ready": "待开始",
        "queued": "待开始",
        "running": "运行中",
        "pausing": "暂停中",
        "stopping": "停止中",
        "interrupted": "已中断",
        "completed": "已完成",
        "uploaded": "已上传",
        "processed": "已处理",
        "approved": "已通过",
        "pending": "待处理",
        "review_required": "待人工处理",
        "sql_blocked": "SQL 阻断",
        "failed": "失败",
        "rejected": "已拒绝",
        "unsupported": "不支持",
    }
    return labels.get(normalized, normalized)


def display_stage(stage: str) -> str:
    normalized = stage.strip() or "not_started"
    labels = {
        "not_started": "未开始",
        "ready": "待开始",
        "plan_ready": "待开始",
        "running": "运行中",
        "interrupted": "已中断",
        "completed": "已完成",
        "parse": "解析中",
        "extract": "提取中",
    }
    return labels.get(normalized, normalized)


def build_page_view_model(
    state: WorkbenchState,
    *,
    status_rail_collapsed: bool = False,
) -> WorkbenchPageViewModel:
    selected_count = sum(1 for item in state.file_items if item.selected)
    can_start = bool(state.can_start) and (not state.file_items or selected_count > 0)
    start_disabled_reason = ""
    if not state.can_start:
        start_disabled_reason = "当前运行状态不允许开始。"
    elif state.file_items and selected_count == 0:
        start_disabled_reason = "请至少勾选一个文件。"

    sections = _sections_from_state(state)
    active_section = _normalize_active_section(state.active_section)
    active_section_model = (
        _overview_section(state)
        if active_section == "overview"
        else next(section for section in sections if section.key == active_section)
    )

    return WorkbenchPageViewModel(
        summary=WorkbenchSummaryViewModel(
            title="论文处理审核工作台",
            current_run_id=state.current_run_id or "未创建",
            run_status=status_badge(state.run_status),
            current_stage=display_stage(state.current_stage),
            flow_entry="real_flow_50",
            input_dir=state.input_dir,
        ),
        sections=sections,
        can_start=can_start,
        start_disabled_reason=start_disabled_reason,
        operation_console=_operation_console(
            state,
            selected_count=selected_count,
            file_count=len(state.file_items),
            can_start=can_start,
            start_disabled_reason=start_disabled_reason,
        ),
        selected_config_key=state.selected_config_key,
        config_panel=state.config_panel,
        active_section=active_section,
        nav_items=_nav_items(state, active_section),
        active_section_model=active_section_model,
        run_progress=state.run_progress,
        status_rail_sections=_status_rail_sections(
            state,
            selected_count=selected_count,
            file_count=len(state.file_items),
            start_disabled_reason=start_disabled_reason,
        ),
        status_rail_collapsed=status_rail_collapsed,
        overview_agent_available=state.overview_agent_available,
        overview_agent_question=state.overview_agent_question,
        overview_agent_answer=state.overview_agent_answer,
        overview_agent_message=state.overview_agent_message,
        overview_agent_messages=list(state.overview_agent_messages),
    )


def _sections_from_state(state: WorkbenchState) -> list[WorkbenchSectionViewModel]:
    return [
        WorkbenchSectionViewModel(
            key="config",
            title="配置",
            rows=list(state.config_items),
            empty_text="尚未读取到配置状态。",
        ),
        WorkbenchSectionViewModel(
            key="files",
            title="文件",
            rows=list(state.file_items),
            empty_text="请选择文件夹或单个文件。",
        ),
        WorkbenchSectionViewModel(
            key="processing",
            title="运行",
            rows=[state.run_progress or state.summary],
        ),
        WorkbenchSectionViewModel(
            key="review",
            title="审核",
            rows=list(state.review_items),
            empty_text="还没有可审核的处理结果。",
        ),
        WorkbenchSectionViewModel(
            key="index",
            title="索引",
            rows=[state.index_state] if state.index_state is not None else [],
            empty_text="索引功能预留：当前只展示状态，不执行重建。",
        ),
        WorkbenchSectionViewModel(
            key="sql",
            title="SQL",
            rows=[state.sql_upload_state] if state.sql_upload_state is not None else [],
            empty_text="审核通过并生成上传预览后显示。",
        ),
        WorkbenchSectionViewModel(
            key="history",
            title="历史",
            rows=list(state.history_items),
            empty_text="暂无历史运行记录。",
        ),
    ]


def _overview_section(state: WorkbenchState) -> WorkbenchSectionViewModel:
    return WorkbenchSectionViewModel(
        key="overview",
        title="总览",
        rows=[state.summary],
        empty_text="暂无状态。",
    )


def _nav_items(state: WorkbenchState, active_section: str) -> list[WorkbenchNavItem]:
    warning_count = sum(
        1 for item in state.config_items if item.severity in {"warning", "error"}
    )
    file_count = len(state.file_items)
    selected_count = sum(1 for item in state.file_items if item.selected)
    review_count = len(state.review_items)
    index_state = state.index_state
    index_badge = ""
    index_severity = "muted"
    if index_state is not None:
        needs_rebuild_count = int(getattr(index_state, "needs_rebuild_count", 0))
        failed_count = int(getattr(index_state, "failed_count", 0))
        if needs_rebuild_count:
            index_badge = str(needs_rebuild_count)
            index_severity = "warning"
        elif failed_count:
            index_badge = str(failed_count)
            index_severity = "danger"
    sql_badge = ""
    if state.sql_eligible_count or state.sql_blocked_count:
        sql_badge = f"{state.sql_eligible_count}/{state.sql_blocked_count}"
    nav_specs = [
        ("overview", "总览", "", "muted"),
        (
            "config",
            "配置",
            str(warning_count) if warning_count else "",
            "warning" if warning_count else "muted",
        ),
        (
            "files",
            "文件",
            f"{selected_count}/{file_count}" if file_count else "",
            "info" if file_count else "muted",
        ),
        (
            "processing",
            "运行",
            status_badge(state.run_status).label,
            status_badge(state.run_status).severity,
        ),
        (
            "review",
            "审核",
            str(review_count or state.pending_count) if review_count or state.pending_count else "",
            "warning" if review_count or state.pending_count else "muted",
        ),
        (
            "index",
            "索引",
            index_badge,
            index_severity,
        ),
        (
            "sql",
            "SQL",
            sql_badge,
            "warning" if state.sql_blocked_count else "muted",
        ),
        (
            "history",
            "历史",
            str(len(state.history_items)) if state.history_items else "",
            "muted",
        ),
    ]
    return [
        WorkbenchNavItem(
            key=key,
            title=title,
            badge=badge,
            severity=severity,
            active=key == active_section,
        )
        for key, title, badge, severity in nav_specs
    ]


def _status_rail_sections(
    state: WorkbenchState,
    *,
    selected_count: int,
    file_count: int,
    start_disabled_reason: str,
) -> list[WorkbenchStatusRailSection]:
    warning_lines = [
        f"{item.name}: {item.status}"
        for item in state.config_items
        if item.severity in {"warning", "error"}
    ]
    if start_disabled_reason:
        warning_lines.insert(0, start_disabled_reason)
    output_lines = [
        f"输出：{state.output_dir or '暂无输出目录'}",
        f"报告：{state.latest_report_path or '暂无报告'}",
        f"证据包：{state.latest_evidence_bundle_path or '暂无证据包'}",
    ]
    return [
        WorkbenchStatusRailSection(
            title="运行状态",
            lines=[
                f"状态：{status_badge(state.run_status).label}",
                f"阶段：{display_stage(state.current_stage)}",
                f"已选择：{selected_count} / {file_count}",
                f"待处理：{state.pending_count}",
                f"失败：{state.failed_count}",
            ],
            severity=status_badge(state.run_status).severity,
        ),
        WorkbenchStatusRailSection(
            title="下一步",
            lines=[state.suggested_next_action or "暂无建议"],
            severity="info",
        ),
        WorkbenchStatusRailSection(
            title="配置警告",
            lines=warning_lines or ["无"],
            severity="warning" if warning_lines else "muted",
        ),
        WorkbenchStatusRailSection(
            title="输出",
            lines=output_lines,
            severity="muted",
        ),
    ]


def _operation_console(
    state: WorkbenchState,
    *,
    selected_count: int,
    file_count: int,
    can_start: bool,
    start_disabled_reason: str,
) -> WorkbenchOperationConsole:
    if state.run_status == "running":
        return WorkbenchOperationConsole(
            title="运行中",
            detail=(
                f"运行编号：{state.current_run_id or '未创建'} | "
                f"阶段：{display_stage(state.current_stage)}"
            ),
            lines=[
                f"待处理：{state.pending_count}",
                f"失败：{state.failed_count}",
                f"输出：{state.output_dir or '暂无输出目录'}",
            ],
            actions=[
                WorkbenchOperationAction("refresh", "刷新状态", "refresh", True),
                WorkbenchOperationAction("pause", "暂停", "pause", state.can_pause),
                WorkbenchOperationAction("stop", "停止", "stop", state.can_stop, "danger"),
                WorkbenchOperationAction(
                    "open_output",
                    "打开输出目录",
                    "open_output",
                    bool(state.output_dir),
                ),
            ],
            severity="info",
        )

    if state.run_status == "completed":
        detail = "已完成运行。"
        actions = [
            WorkbenchOperationAction(
                "review_sql",
                "查看 SQL 预览",
                "select_sql",
                state.sql_eligible_count > 0 or state.sql_blocked_count > 0,
            ),
            WorkbenchOperationAction(
                "open_report",
                "打开报告",
                "open_report",
                bool(state.latest_report_path),
            ),
            WorkbenchOperationAction("refresh", "刷新状态", "refresh", True),
        ]
        if state.sql_eligible_count > state.sql_uploaded_count:
            detail = "已生成 SQL 预览，等待你确认上传。"
        elif state.sql_uploaded_count:
            detail = "SQL 上传已完成，文件会在 NAS 状态标记里锁定。"
        return WorkbenchOperationConsole(
            title="运行完成",
            detail=detail,
            lines=[
                f"SQL 可上传：{state.sql_eligible_count}",
                f"SQL 已上传：{state.sql_uploaded_count}",
            ],
            actions=actions,
            severity="success",
        )

    if selected_count > 0:
        return WorkbenchOperationConsole(
            title=f"已选择 {selected_count} 个文件",
            detail="点击开始运行后才会处理文件；不会自动上传 SQL。",
            lines=[
                f"已选择：{selected_count}/{file_count}",
                start_disabled_reason or "处理计划已生成，等待开始运行。",
            ],
            actions=[
                WorkbenchOperationAction(
                    "start",
                    "开始运行",
                    "start",
                    can_start,
                    "primary",
                ),
                WorkbenchOperationAction("scan", "重新扫描", "scan", True),
                WorkbenchOperationAction("clear", "清空选择", "clear_selection", True),
            ],
            severity="info",
        )

    return WorkbenchOperationConsole(
        title="选择文件开始",
        detail=start_disabled_reason or "从 NAS 目录扫描或选择单个文件。",
        lines=[
            f"已选择：{selected_count}/{file_count}",
            "已处理或已上传的文件会自动锁定，避免重复上传。",
        ],
        actions=[
            WorkbenchOperationAction("choose_dir", "选择目录", "choose_dir", True),
            WorkbenchOperationAction("choose_file", "选择文件", "choose_file", True),
            WorkbenchOperationAction("scan", "扫描候选", "scan", True),
            WorkbenchOperationAction("start", "开始运行", "start", False, "primary"),
        ],
        severity="muted",
    )


def _normalize_active_section(active_section: str) -> str:
    valid_sections = {
        "overview",
        "config",
        "files",
        "processing",
        "review",
        "index",
        "sql",
        "history",
    }
    return active_section if active_section in valid_sections else "overview"
