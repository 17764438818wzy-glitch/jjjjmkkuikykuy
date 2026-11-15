"""Tk section renderers for the workbench."""

from __future__ import annotations

import tkinter.ttk as ttk
from typing import Any

from paper_pipeline.app.app_state import (
    WorkbenchConfigItem,
    WorkbenchFileItem,
    WorkbenchStatusSummary,
)
from paper_pipeline.app.history import WorkbenchRunHistoryItem
from paper_pipeline.app.index_view import WorkbenchIndexState
from paper_pipeline.app.review_view import WorkbenchReviewItem
from paper_pipeline.app.run_status import RunProgressViewModel
from paper_pipeline.app.sql_view import WorkbenchSqlUploadState
from paper_pipeline.app.view_models import (
    WorkbenchOperationAction,
    WorkbenchNavItem,
    WorkbenchPageViewModel,
    WorkbenchSectionViewModel,
    display_status,
)

SURFACE = "#f6f7f8"
PANEL = "#fbfbfa"
PANEL_ALT = "#eef0f2"
BORDER = "#d9dde1"
TEXT = "#1f2328"
MUTED = "#68707a"
ACCENT = "#2f6f73"
SUCCESS = "#2f7d55"
WARNING = "#9a6700"
DANGER = "#b42318"
INFO = "#3567a8"

TableColumn = tuple[str, str, int]
TableRow = tuple[str, tuple[str, ...]]


def render_workbench_sections(
    *,
    parent: Any,
    tk_module: Any,
    view_model: WorkbenchPageViewModel,
    callbacks: dict[str, Any],
) -> None:
    _render_top_bar(parent, tk_module, view_model, callbacks)
    _render_tab_bar(parent, tk_module, view_model.nav_items, callbacks)
    _render_operation_console(parent, tk_module, view_model, callbacks)

    body = tk_module.Frame(parent, bg=SURFACE)
    body.pack(fill=tk_module.BOTH, expand=True)

    content = tk_module.Frame(body, bg=SURFACE)
    content.pack(side=tk_module.LEFT, fill=tk_module.BOTH, expand=True)

    _render_section(
        content,
        tk_module,
        view_model.active_section_model,
        view_model,
        callbacks,
    )
    _render_status_rail(body, tk_module, view_model, callbacks)


def _render_top_bar(
    parent: Any,
    tk: Any,
    vm: WorkbenchPageViewModel,
    callbacks: dict[str, Any],
) -> None:
    frame = tk.Frame(parent, bg=SURFACE)
    frame.pack(fill=tk.X, pady=(0, 10))
    tk.Label(
        frame,
        text="Paper Pipeline",
        font=("Microsoft YaHei UI", 14, "bold"),
        bg=SURFACE,
        fg=TEXT,
    ).pack(side=tk.LEFT, padx=(0, 14))
    tk.Label(
        frame,
        text=(
            f"运行：{vm.summary.current_run_id}   "
            f"状态：{vm.summary.run_status.label}   "
            f"阶段：{vm.summary.current_stage}"
        ),
        bg=SURFACE,
        fg=MUTED,
        anchor="w",
    ).pack(side=tk.LEFT, fill=tk.X, expand=True)
    tk.Button(
        frame,
        text="刷新",
        command=callbacks.get("refresh"),
    ).pack(side=tk.LEFT, padx=(8, 0))


def _render_tab_bar(
    parent: Any,
    tk: Any,
    nav_items: list[WorkbenchNavItem],
    callbacks: dict[str, Any],
) -> None:
    frame = tk.Frame(parent, bg=SURFACE)
    frame.pack(fill=tk.X, pady=(0, 8))
    for item in nav_items:
        label = f"{item.title} {item.badge}" if item.badge else item.title
        tk.Button(
            frame,
            text=label,
            command=lambda key=item.key: _select_section(callbacks, key),
            fg=ACCENT if item.active else TEXT,
        ).pack(side=tk.LEFT, padx=(0, 6))


def _render_status_rail(
    parent: Any,
    tk: Any,
    vm: WorkbenchPageViewModel,
    callbacks: dict[str, Any],
) -> None:
    rail = tk.Frame(parent, bg=PANEL_ALT)
    rail.pack(side=tk.RIGHT, fill=tk.Y, padx=(12, 0))
    if vm.status_rail_collapsed:
        tk.Button(
            rail,
            text="状态 >",
            command=callbacks.get("toggle_status_rail"),
        ).pack(fill=tk.X, padx=8, pady=8)
        return

    header = tk.Frame(rail, bg=PANEL_ALT)
    header.pack(fill=tk.X, padx=10, pady=(10, 4))
    tk.Label(
        header,
        text="运行状态",
        font=("Microsoft YaHei UI", 11, "bold"),
        bg=PANEL_ALT,
        fg=TEXT,
        anchor="w",
    ).pack(side=tk.LEFT, fill=tk.X, expand=True)
    tk.Button(
        header,
        text="<",
        command=callbacks.get("toggle_status_rail"),
    ).pack(side=tk.RIGHT)

    for section in vm.status_rail_sections:
        if section.title == "运行状态":
            lines = section.lines
        else:
            tk.Label(
                rail,
                text=section.title,
                font=("Microsoft YaHei UI", 10, "bold"),
                bg=PANEL_ALT,
                fg=TEXT,
                anchor="w",
            ).pack(fill=tk.X, padx=10, pady=(10, 2))
            lines = section.lines
        for line in lines:
            tk.Label(
                rail,
                text=line,
                bg=PANEL_ALT,
                fg=_severity_color(section.severity),
                anchor="w",
            ).pack(fill=tk.X, padx=10, pady=1)


def _render_section(
    parent: Any,
    tk: Any,
    section: WorkbenchSectionViewModel,
    page_vm: WorkbenchPageViewModel,
    callbacks: dict[str, Any],
) -> None:
    frame = tk.Frame(parent, bg=PANEL)
    frame.pack(fill=tk.BOTH, expand=True)
    tk.Label(
        frame,
        text=section.title,
        font=("Microsoft YaHei UI", 13, "bold"),
        bg=PANEL,
        fg=TEXT,
        anchor="w",
    ).pack(fill=tk.X, padx=12, pady=(10, 6))
    if section.key == "overview":
        _render_overview(frame, tk, page_vm, callbacks)
    elif section.key == "config":
        _render_config_rows(frame, tk, section.rows, callbacks)
    elif section.key == "files":
        _render_file_rows(frame, tk, section.rows, callbacks)
    elif section.key == "processing":
        _render_processing(frame, tk, page_vm, callbacks)
    elif section.key == "review":
        _render_review_rows(frame, tk, section.rows, callbacks)
    elif section.key == "index":
        _render_index_rows(frame, tk, section.rows)
    elif section.key == "sql":
        _render_sql_rows(frame, tk, section.rows)
    elif section.key == "history":
        _render_history_rows(frame, tk, section.rows)


def _render_operation_console(
    parent: Any,
    tk: Any,
    vm: WorkbenchPageViewModel,
    callbacks: dict[str, Any],
) -> None:
    console = vm.operation_console
    frame = tk.Frame(parent, bg=PANEL_ALT)
    frame.pack(side=getattr(tk, "BOTTOM", "bottom"), fill=tk.X, pady=(8, 0))

    header = tk.Frame(frame, bg=PANEL_ALT)
    header.pack(fill=tk.X, padx=12, pady=(10, 2))
    tk.Label(
        header,
        text="运行操作台",
        font=("Microsoft YaHei UI", 11, "bold"),
        bg=PANEL_ALT,
        fg=TEXT,
        anchor="w",
    ).pack(side=tk.LEFT, padx=(0, 12))
    tk.Label(
        header,
        text=console.title,
        bg=PANEL_ALT,
        fg=_severity_color(console.severity),
        anchor="w",
    ).pack(side=tk.LEFT, fill=tk.X, expand=True)

    tk.Label(
        frame,
        text=console.detail,
        bg=PANEL_ALT,
        fg=MUTED,
        anchor="w",
    ).pack(fill=tk.X, padx=12, pady=(0, 6))

    if console.lines:
        tk.Label(
            frame,
            text="  |  ".join(console.lines),
            bg=PANEL_ALT,
            fg=MUTED,
            anchor="w",
        ).pack(fill=tk.X, padx=12, pady=(0, 8))

    actions = tk.Frame(frame, bg=PANEL_ALT)
    actions.pack(fill=tk.X, padx=12, pady=(0, 10))
    for action in console.actions:
        _render_operation_action(actions, tk, action, callbacks)


def _render_operation_action(
    parent: Any,
    tk: Any,
    action: WorkbenchOperationAction,
    callbacks: dict[str, Any],
) -> None:
    callback = callbacks.get(action.callback_key)
    enabled = action.enabled and callable(callback)
    tk.Button(
        parent,
        text=action.label,
        command=callback,
        state=tk.NORMAL if enabled else tk.DISABLED,
        fg=_severity_color(action.severity),
    ).pack(side=tk.LEFT, padx=(0, 6))


def _render_overview(
    parent: Any,
    tk: Any,
    vm: WorkbenchPageViewModel,
    callbacks: dict[str, Any],
) -> None:
    summary = _first_summary(vm.active_section_model.rows)
    lines = [
        f"流程入口: {vm.summary.flow_entry}",
        f"输入目录: {vm.summary.input_dir or '未设置'}",
        f"当前阶段: {vm.summary.current_stage}",
    ]
    if summary is not None:
        lines.extend(
            [
                (
                    "待处理概况: "
                    f"pending={summary.pending_count}, failed={summary.failed_count}"
                ),
                f"SQL: eligible={summary.sql_eligible_count}, blocked={summary.sql_blocked_count}",
            ],
        )
    for line in lines:
        tk.Label(parent, text=line, bg=PANEL, fg=TEXT, anchor="w").pack(
            fill=tk.X,
            padx=12,
            pady=2,
        )
    agent_panel = tk.Frame(parent, bg=PANEL)
    agent_panel.pack(fill=tk.X, padx=12, pady=(12, 4))
    tk.Label(
        agent_panel,
        text="DeepSeek 总 Agent",
        font=("Microsoft YaHei UI", 11, "bold"),
        bg=PANEL,
        fg=TEXT,
        anchor="w",
    ).pack(fill=tk.X, pady=(0, 4))
    status_text = (
        "已接入，可用中文询问当前上传、审核和 SQL 状态。"
        if vm.overview_agent_available
        else "未配置 DEEPSEEK_API_KEY，暂不能发起中文问答。"
    )
    tk.Label(agent_panel, text=status_text, bg=PANEL, fg=MUTED, anchor="w").pack(
        fill=tk.X,
        pady=(0, 4),
    )
    history = list(vm.overview_agent_messages)
    if not history and vm.overview_agent_answer:
        history = [
            type("_Message", (), {"role": "assistant", "content": vm.overview_agent_answer})(),
        ]
    for message in history:
        role = str(getattr(message, "role", "assistant"))
        content = str(getattr(message, "content", "")).strip()
        if not content:
            continue
        prefix = "你：" if role == "user" else "DeepSeek："
        tk.Label(
            agent_panel,
            text=f"{prefix}{content}",
            bg=PANEL,
            fg=TEXT if role == "user" else INFO,
            anchor="w",
            justify=tk.LEFT,
            wraplength=760,
        ).pack(fill=tk.X, pady=(2, 2))
    question_var = callbacks.get("overview_agent_question_var")
    if question_var is None:
        question_var = _text_variable(tk, vm.overview_agent_question)
    question_row = tk.Frame(agent_panel, bg=PANEL)
    question_row.pack(fill=tk.X, pady=(0, 4))
    tk.Label(question_row, text="问题", bg=PANEL, fg=TEXT, anchor="w").pack(
        side=tk.LEFT,
        padx=(0, 8),
    )
    if hasattr(tk, "Entry"):
        tk.Entry(question_row, textvariable=question_var).pack(
            side=tk.LEFT,
            fill=tk.X,
            expand=True,
            padx=(0, 8),
        )
    else:
        tk.Label(
            question_row,
            text=getattr(question_var, "get", lambda: "")(),
            bg=PANEL,
            fg=MUTED,
            anchor="w",
        ).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 8))
    callback = callbacks.get("ask_overview_agent")
    tk.Button(
        question_row,
        text="询问总 Agent",
        command=callback,
        state=tk.NORMAL
        if callable(callback) and vm.overview_agent_available
        else tk.DISABLED,
    ).pack(side=tk.LEFT)
    if vm.overview_agent_message:
        tk.Label(
            agent_panel,
            text=vm.overview_agent_message,
            bg=PANEL,
            fg=INFO if vm.overview_agent_answer else WARNING,
            anchor="w",
        ).pack(fill=tk.X, pady=(2, 2))
    if vm.overview_agent_answer and not history:
        tk.Label(
            agent_panel,
            text=vm.overview_agent_answer,
            bg=PANEL,
            fg=TEXT,
            anchor="w",
            justify=tk.LEFT,
            wraplength=760,
        ).pack(fill=tk.X, pady=(2, 4))


def _render_config_rows(
    parent: Any,
    tk: Any,
    rows: list[object],
    callbacks: dict[str, Any],
) -> None:
    if not rows:
        _render_empty(parent, tk, "尚未读取到配置状态。")
        return
    table_rows: list[TableRow] = []
    for row in rows:
        item = row if isinstance(row, WorkbenchConfigItem) else None
        if item is None:
            continue
        table_rows.append(
            (
                item.key,
                (
                    item.name,
                    item.status,
                    item.source,
                    item.detail,
                    ", ".join(item.missing_keys) if item.missing_keys else "无",
                    "配置",
                ),
            ),
        )
    rendered = _render_table(
        parent,
        tk,
        columns=(
            ("name", "名称", 150),
            ("status", "状态", 90),
            ("source", "来源", 100),
            ("detail", "当前值摘要", 260),
            ("missing", "缺失项", 160),
            ("action", "操作", 70),
        ),
        rows=table_rows,
    )
    if rendered:
        for row in rows:
            item = row if isinstance(row, WorkbenchConfigItem) else None
            if item is None:
                continue
            tk.Button(
                parent,
                text=f"配置 {item.name}",
                command=lambda key=item.key: _select_config(callbacks, key),
            ).pack(anchor="w", padx=12, pady=1)
        return
    for row in rows:
        item = row if isinstance(row, WorkbenchConfigItem) else None
        if item is None:
            continue
        text = f"{item.name} | {item.status} | {item.detail}"
        if item.missing_keys:
            text = f"{text} | 缺失: {', '.join(item.missing_keys)}"
        row_frame = tk.Frame(parent, bg=PANEL)
        row_frame.pack(fill=tk.X, padx=12, pady=2)
        tk.Label(
            row_frame,
            text=text,
            bg=PANEL,
            fg=_severity_color(item.severity),
            anchor="w",
        ).pack(side=tk.LEFT, fill=tk.X, expand=True)
        tk.Button(
            row_frame,
            text="配置",
            command=lambda key=item.key: _select_config(callbacks, key),
        ).pack(side=tk.LEFT, padx=(8, 0))


def _render_table(
    parent: Any,
    tk: Any,
    *,
    columns: tuple[TableColumn, ...],
    rows: list[TableRow],
) -> bool:
    ttk_module = getattr(tk, "ttk", None)
    if ttk_module is None:
        ttk_module = ttk
    treeview_factory = getattr(ttk_module, "Treeview", None)
    if treeview_factory is None:
        return False
    column_keys = tuple(column[0] for column in columns)
    tree = treeview_factory(parent, columns=column_keys, show="headings")
    tree.pack(fill=tk.BOTH, expand=True, padx=12, pady=(0, 8))
    for key, heading, width in columns:
        tree.heading(key, text=heading)
        tree.column(key, width=width, anchor="w")
    for item_id, values in rows:
        tree.insert("", "end", iid=item_id, values=values)
    return True


def render_config_dialog(
    *,
    parent: Any,
    tk_module: Any,
    config_panel: object,
    callbacks: dict[str, Any],
) -> bool:
    if not hasattr(tk_module, "Toplevel"):
        return False

    title = str(getattr(config_panel, "title", ""))
    dialog = tk_module.Toplevel(parent)
    dialog.title(f"{title} 配置" if title else "配置")
    geometry = getattr(dialog, "geometry", None)
    if geometry is not None:
        geometry("560x620")
    _render_config_editor(
        dialog,
        tk_module,
        config_panel,
        callbacks,
        close_after_action=getattr(dialog, "destroy", None),
    )
    _make_modal(parent, dialog)
    return True


def _render_config_editor(
    parent: Any,
    tk: Any,
    panel: object,
    callbacks: dict[str, Any],
    *,
    close_after_action: Any | None = None,
) -> None:
    fields = tuple(getattr(panel, "fields", ()))
    title = str(getattr(panel, "title", ""))
    description = str(getattr(panel, "description", ""))
    supports_save = bool(getattr(panel, "supports_save", False))
    action_hint = str(getattr(panel, "action_hint", ""))
    key = str(getattr(panel, "key", ""))
    editor = tk.Frame(parent, bg=SURFACE)
    editor.pack(fill=tk.BOTH, expand=True, padx=12, pady=(10, 12))
    tk.Label(
        editor,
        text=f"{title} 配置",
        font=("Microsoft YaHei UI", 11, "bold"),
        bg=SURFACE,
        fg=TEXT,
        anchor="w",
    ).pack(fill=tk.X, padx=8, pady=(8, 2))
    if description:
        tk.Label(editor, text=description, bg=SURFACE, fg=MUTED, anchor="w").pack(
            fill=tk.X,
            padx=8,
            pady=(0, 6),
        )
    if not fields:
        _render_empty(editor, tk, action_hint or "该能力不需要在工作台保存配置。")
        return
    field_vars: dict[str, Any] = {}
    for field in fields:
        env_key = str(getattr(field, "env_key", ""))
        source = str(getattr(field, "source", ""))
        has_saved_value = bool(getattr(field, "has_saved_value", False))
        secret = bool(getattr(field, "secret", False))
        value = str(getattr(field, "value", ""))
        label = env_key if not source else f"{env_key} | source={source}"
        if secret and has_saved_value:
            label = f"{label} | saved"
        tk.Label(editor, text=label, bg=SURFACE, fg=TEXT, anchor="w").pack(
            fill=tk.X,
            padx=8,
            pady=(3, 1),
        )
        variable = _text_variable(tk, value)
        field_vars[env_key] = variable
        if hasattr(tk, "Entry"):
            entry_kwargs: dict[str, Any] = {"textvariable": variable}
            if secret:
                entry_kwargs["show"] = "*"
            tk.Entry(editor, **entry_kwargs).pack(fill=tk.X, padx=8, pady=(0, 2))
        else:
            display_value = str(getattr(field, "display_value", value))
            tk.Label(editor, text=display_value, bg=SURFACE, fg=MUTED, anchor="w").pack(
                fill=tk.X,
                padx=8,
                pady=(0, 2),
            )
    buttons = tk.Frame(editor, bg=SURFACE)
    buttons.pack(fill=tk.X, padx=8, pady=(8, 8))
    tk.Button(
        buttons,
        text="保存配置",
        state=tk.NORMAL if supports_save else tk.DISABLED,
        command=lambda: _save_config_and_close(
            callbacks,
            key,
            field_vars,
            close_after_action,
        ),
    ).pack(side=tk.LEFT, padx=(0, 6))
    tk.Button(
        buttons,
        text="清空本项",
        state=tk.NORMAL if supports_save else tk.DISABLED,
        command=lambda: _clear_config_and_close(callbacks, key, close_after_action),
    ).pack(side=tk.LEFT, padx=(0, 6))
    tk.Button(
        buttons,
        text="取消",
        command=lambda: _close_dialog(close_after_action),
    ).pack(side=tk.LEFT, padx=(0, 6))


def _render_file_rows(parent: Any, tk: Any, rows: list[object], callbacks: dict[str, Any]) -> None:
    toolbar = tk.Frame(parent, bg=PANEL)
    toolbar.pack(fill=tk.X, padx=12, pady=(0, 8))
    for key, label in [
        ("choose_dir", "选择目录"),
        ("choose_file", "选择文件"),
        ("scan", "扫描候选"),
        ("select_all", "全选"),
        ("select_supported", "仅支持项"),
    ]:
        tk.Button(toolbar, text=label, command=callbacks.get(key)).pack(side=tk.LEFT, padx=(0, 6))
    if not rows:
        _render_empty(parent, tk, "请选择文件夹或单个文件。")
        return
    table_rows: list[TableRow] = []
    for row in rows:
        item = row if isinstance(row, WorkbenchFileItem) else None
        if item is None:
            continue
        table_rows.append(
            (
                item.full_path,
                (
                    "[x]" if item.selected else "[ ]",
                    item.file_name,
                    item.file_type,
                    item.route,
                    display_status(item.status),
                    item.reason,
                ),
            ),
        )
    rendered = _render_table(
        parent,
        tk,
        columns=(
            ("selected", "选择", 60),
            ("file_name", "文件名", 180),
            ("file_type", "类型", 80),
            ("route", "路由", 180),
            ("status", "状态", 100),
            ("reason", "原因", 260),
        ),
        rows=table_rows,
    )
    if rendered:
        return
    for row in rows:
        item = row if isinstance(row, WorkbenchFileItem) else None
        if item is None:
            continue
        mark = "[x]" if item.selected else "[ ]"
        supported = "支持" if item.supported else "不支持"
        text = f"{mark} {item.file_name} | {supported} | {item.file_type} | {item.route} | {item.status}"
        tk.Label(parent, text=text, bg=PANEL, fg=TEXT, anchor="w").pack(
            fill=tk.X,
            padx=12,
            pady=2,
        )


def _render_processing(
    parent: Any,
    tk: Any,
    vm: WorkbenchPageViewModel,
    callbacks: dict[str, Any],
) -> None:
    toolbar = tk.Frame(parent, bg=PANEL)
    toolbar.pack(fill=tk.X, padx=12, pady=(0, 8))
    tk.Button(toolbar, text="刷新状态", command=callbacks.get("refresh")).pack(
        side=tk.LEFT,
        padx=(0, 6),
    )
    if vm.start_disabled_reason:
        tk.Label(parent, text=vm.start_disabled_reason, bg=PANEL, fg=WARNING, anchor="w").pack(
            fill=tk.X,
            padx=12,
            pady=(0, 4),
        )
    progress = vm.run_progress if isinstance(vm.run_progress, RunProgressViewModel) else None
    if progress is not None and progress.run_id:
        for line in [
            f"运行编号: {progress.run_id}",
            (
                f"总数: {progress.total_count} | 完成: {progress.completed_count} | "
                f"待处理: {progress.pending_count} | 失败: {progress.failed_count}"
            ),
            f"输出目录: {progress.output_dir}",
        ]:
            tk.Label(parent, text=line, bg=PANEL, fg=TEXT, anchor="w").pack(
                fill=tk.X,
                padx=12,
                pady=2,
            )
        stage_rows: list[TableRow] = [
            (
                f"{index}",
                (
                    row.file_name,
                    row.stage,
                    row.status,
                    row.tool,
                    row.reason,
                    row.artifact_path,
                ),
            )
            for index, row in enumerate(progress.file_stage_rows, start=1)
        ]
        if stage_rows:
            _render_table(
                parent,
                tk,
                columns=(
                    ("file", "文件", 180),
                    ("stage", "阶段", 150),
                    ("status", "状态", 90),
                    ("tool", "工具", 140),
                    ("reason", "原因", 220),
                    ("artifact", "产物", 220),
                ),
                rows=stage_rows,
            )
    tk.Label(
        parent,
        text="开始、暂停、停止和 SQL 确认集中在底部运行操作台。",
        bg=PANEL,
        fg=MUTED,
        anchor="w",
    ).pack(fill=tk.X, padx=12, pady=(0, 6))


def _render_review_empty(parent: Any, tk: Any, section: WorkbenchSectionViewModel) -> None:
    _render_empty(parent, tk, section.empty_text)
    tk.Label(
        parent,
        text="审核写入路径尚未接入时，gold、silver、rejected、pending 动作保持禁用。",
        bg=PANEL,
        fg=MUTED,
        anchor="w",
    ).pack(fill=tk.X, padx=12, pady=(0, 6))


def _render_review_rows(
    parent: Any,
    tk: Any,
    rows: list[object],
    callbacks: dict[str, Any],
) -> None:
    review_items = [row for row in rows if isinstance(row, WorkbenchReviewItem)]
    if not review_items:
        _render_empty(parent, tk, "还没有可审核的处理结果。")
        return
    toolbar = tk.Frame(parent, bg=PANEL)
    toolbar.pack(fill=tk.X, padx=12, pady=(0, 8))
    tk.Button(toolbar, text="全选", command=callbacks.get("select_all_review")).pack(
        side=tk.LEFT,
        padx=(0, 6),
    )
    tk.Button(
        toolbar,
        text="取消全选",
        command=callbacks.get("clear_review_selection"),
    ).pack(side=tk.LEFT, padx=(0, 6))
    start_callback = callbacks.get("start_review")
    tk.Button(
        toolbar,
        text="开始复审",
        command=start_callback,
        state=tk.NORMAL
        if callable(start_callback) and any(item.selected for item in review_items)
        else tk.DISABLED,
    ).pack(side=tk.LEFT, padx=(0, 6))
    _render_table(
        parent,
        tk,
        columns=(
            ("selected", "选择", 60),
            ("paper_id", "论文ID", 140),
            ("title", "标题", 180),
            ("tier", "等级", 80),
            ("category", "分类", 120),
            ("direction", "方向", 140),
            ("sql", "SQL", 80),
            ("markdown", "正文", 240),
            ("metadata", "元数据", 240),
        ),
        rows=[
            (
                item.metadata_path or item.markdown_path or item.paper_id,
                (
                    "[x]" if item.selected else "[ ]",
                    item.paper_id,
                    item.title or item.file_name,
                    item.tier,
                    item.category,
                    item.direction,
                    item.sql_eligibility_label,
                    item.markdown_path,
                    item.metadata_path,
                ),
            )
            for item in review_items
        ],
    )
    first = review_items[0]
    preview = first.markdown_preview.replace("\n", " ")[:240]
    for line in [
        f"当前结果: {first.markdown_path}",
        f"元数据: {first.metadata_path}",
        f"预览: {preview}",
    ]:
        tk.Label(parent, text=line, bg=PANEL, fg=MUTED, anchor="w").pack(
            fill=tk.X,
            padx=12,
            pady=2,
        )


def _render_index_rows(parent: Any, tk: Any, rows: list[object]) -> None:
    index_state = next((row for row in rows if isinstance(row, WorkbenchIndexState)), None)
    if index_state is None:
        _render_empty(parent, tk, "索引功能预留：当前只展示状态，不执行重建。")
        return
    for line in [
        index_state.message,
        (
            f"总数: {index_state.total_count} | 已索引: {index_state.indexed_count} | "
            f"待重建: {index_state.needs_rebuild_count} | 失败: {index_state.failed_count}"
        ),
    ]:
        tk.Label(parent, text=line, bg=PANEL, fg=TEXT, anchor="w").pack(
            fill=tk.X,
            padx=12,
            pady=2,
        )
    toolbar = tk.Frame(parent, bg=PANEL)
    toolbar.pack(fill=tk.X, padx=12, pady=(4, 8))
    tk.Button(
        toolbar,
        text="重建索引",
        state=tk.NORMAL if index_state.rebuild_enabled else tk.DISABLED,
        command=None,
    ).pack(side=tk.LEFT, padx=(0, 6))
    if not index_state.rows:
        _render_empty(parent, tk, "暂无待索引项目。")
        return
    _render_table(
        parent,
        tk,
        columns=(
            ("paper_id", "论文ID", 140),
            ("title", "标题", 180),
            ("tier", "等级", 80),
            ("status", "索引状态", 110),
            ("reason", "原因", 280),
            ("metadata", "元数据", 260),
        ),
        rows=[
            (
                row.metadata_path or row.paper_id,
                (
                    row.paper_id,
                    row.title,
                    row.tier,
                    row.index_status,
                    row.reason,
                    row.metadata_path,
                ),
            )
            for row in index_state.rows
        ],
    )


def _render_sql_rows(parent: Any, tk: Any, rows: list[object]) -> None:
    sql_state = next(
        (row for row in rows if isinstance(row, WorkbenchSqlUploadState)),
        None,
    )
    if sql_state is None:
        _render_empty(parent, tk, "还没有 SQL 预览。")
        return
    for line in [
        f"预览ID: {sql_state.preview_id or '暂无'}",
        (
            f"SQL模式: {sql_state.sql_mode} | 连接: {sql_state.connection_status} | "
            f"目标表: {sql_state.target_table or '未设置'}"
        ),
        f"可上传: {sql_state.eligible_count} | 阻断: {sql_state.blocked_count}",
        sql_state.confirm_disabled_reason,
    ]:
        if line:
            tk.Label(parent, text=line, bg=PANEL, fg=TEXT, anchor="w").pack(
                fill=tk.X,
                padx=12,
                pady=2,
            )
    if sql_state.preview_rows:
        _render_table(
            parent,
            tk,
            columns=(
                ("paper_id", "论文ID", 140),
                ("title", "标题", 180),
                ("tier", "等级", 80),
                ("category", "分类", 120),
                ("direction", "方向", 140),
                ("status", "SQL状态", 100),
            ),
            rows=[
                (
                    row.paper_id,
                    (
                        row.paper_id,
                        row.title,
                        row.tier,
                        row.category,
                        row.direction,
                        row.sql_status,
                    ),
                )
                for row in sql_state.preview_rows
            ],
        )
    if sql_state.blocked_reasons:
        _render_table(
            parent,
            tk,
            columns=(
                ("paper_id", "论文ID", 140),
                ("fields", "阻断字段", 180),
                ("reasons", "原因", 320),
                ("next", "下一步", 240),
                ("metadata", "元数据", 240),
            ),
            rows=[
                (
                    row.paper_id,
                    (
                        row.paper_id,
                        ", ".join(row.blocking_fields),
                        " | ".join(row.reasons),
                        row.suggested_next_action,
                        row.metadata_path,
                    ),
                )
                for row in sql_state.blocked_reasons
            ],
        )


def _render_history_rows(parent: Any, tk: Any, rows: list[object]) -> None:
    history_items = [row for row in rows if isinstance(row, WorkbenchRunHistoryItem)]
    if not history_items:
        _render_empty(parent, tk, "暂无历史运行记录。")
        return
    _render_table(
        parent,
        tk,
        columns=(
            ("run_id", "运行编号", 190),
            ("started", "开始时间", 180),
            ("total", "总数", 70),
            ("completed", "完成", 70),
            ("failed", "失败", 70),
            ("report", "报告", 260),
            ("state", "状态文件", 260),
        ),
        rows=[
            (
                item.run_id,
                (
                    item.run_id,
                    item.started_at,
                    str(item.total_count),
                    str(item.completed_count),
                    str(item.failed_count),
                    item.report_path,
                    item.run_state_path,
                ),
            )
            for item in history_items
        ],
    )


def _render_empty(parent: Any, tk: Any, text: str) -> None:
    tk.Label(parent, text=text, bg=PANEL, fg=MUTED, anchor="w").pack(
        fill=tk.X,
        padx=12,
        pady=(0, 8),
    )


def _select_section(callbacks: dict[str, Any], key: str) -> None:
    callback = callbacks.get("select_section")
    if callable(callback):
        callback(key)


def _select_config(callbacks: dict[str, Any], key: str) -> None:
    callback = callbacks.get("select_config")
    if callable(callback):
        callback(key)


def _save_config(callbacks: dict[str, Any], key: str, values: dict[str, Any]) -> None:
    callback = callbacks.get("save_config")
    if callable(callback):
        callback(key, {field_key: variable.get() for field_key, variable in values.items()})


def _clear_config(callbacks: dict[str, Any], key: str) -> None:
    callback = callbacks.get("clear_config")
    if callable(callback):
        callback(key)


def _save_config_and_close(
    callbacks: dict[str, Any],
    key: str,
    values: dict[str, Any],
    close_after_action: Any | None,
) -> None:
    _save_config(callbacks, key, values)
    _close_dialog(close_after_action)


def _clear_config_and_close(
    callbacks: dict[str, Any],
    key: str,
    close_after_action: Any | None,
) -> None:
    _clear_config(callbacks, key)
    _close_dialog(close_after_action)


def _close_dialog(close_after_action: Any | None) -> None:
    if callable(close_after_action):
        close_after_action()


def _make_modal(parent: Any, dialog: Any) -> None:
    transient = getattr(dialog, "transient", None)
    grab_set = getattr(dialog, "grab_set", None)
    if transient is not None:
        transient(parent)
    if grab_set is not None:
        grab_set()


def _text_variable(tk: Any, value: str) -> Any:
    if hasattr(tk, "StringVar"):
        return tk.StringVar(value=value)
    return _SimpleTextVariable(value)


def _first_summary(rows: list[object]) -> WorkbenchStatusSummary | None:
    for row in rows:
        if isinstance(row, WorkbenchStatusSummary):
            return row
    return None


def _severity_color(severity: str) -> str:
    if severity in {"ok", "success"}:
        return SUCCESS
    if severity == "info":
        return INFO
    if severity == "warning":
        return WARNING
    if severity in {"error", "danger"}:
        return DANGER
    return MUTED


class _SimpleTextVariable:
    def __init__(self, value: str) -> None:
        self._value = value

    def get(self) -> str:
        return self._value

    def set(self, value: str) -> None:
        self._value = value
