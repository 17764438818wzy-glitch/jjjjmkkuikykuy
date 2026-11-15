"""Windows desktop workbench entrypoint."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from paper_pipeline.app.app_state import WorkbenchState
from paper_pipeline.app.controller import WorkbenchController
from paper_pipeline.app.openers import SystemPathOpener
from paper_pipeline.app.tk_sections import render_config_dialog, render_workbench_sections
from paper_pipeline.app.view_models import build_page_view_model
from paper_pipeline.app.tray import WindowsTrayIconAdapter
from paper_pipeline.app.workbench import build_default_controller
from paper_pipeline.notifications import NotificationService, WindowsNotificationAdapter

APP_TITLE = "Paper Pipeline Workbench"
SURFACE = "#f4f2ea"
PANEL = "#e9e6dc"
TEXT = "#242620"
MUTED = "#62675d"
ACCENT = "#295f5a"


def build_windows_controller(base_dir: str | Path | None = None) -> WorkbenchController:
    controller = build_default_controller(base_dir)
    controller.opener = SystemPathOpener()
    controller.notification_service = NotificationService(WindowsNotificationAdapter())
    return controller


def launch_windows_workbench(
    *,
    controller: WorkbenchController | None = None,
    base_dir: str | Path | None = None,
    tk_module: Any | None = None,
    tray_adapter: Any | None = None,
) -> int:
    tk = tk_module or _load_tkinter()
    active_controller = controller or build_windows_controller(base_dir)
    root = tk.Tk()
    root.title(APP_TITLE)
    root.geometry("1120x720")

    state = active_controller.load_status()
    app = WorkbenchWindow(
        root=root,
        tk_module=tk,
        controller=active_controller,
        tray_adapter=tray_adapter,
    )
    app.build(state)
    app.start_tray(state)
    root.protocol("WM_DELETE_WINDOW", app.handle_close)
    root.mainloop()
    return 0


class WorkbenchWindow:
    def __init__(
        self,
        *,
        root: Any,
        tk_module: Any,
        controller: WorkbenchController,
        tray_adapter: Any | None = None,
    ) -> None:
        self.root = root
        self.tk = tk_module
        self.controller = controller
        self.tray_adapter = tray_adapter or WindowsTrayIconAdapter(
            on_open=self.show_window,
            on_show_status=self.show_window,
            on_exit=self.exit_app,
        )
        self.state = controller.load_status()
        self.file_limit_var = self._make_text_var(str(self.state.requested_file_count))
        self.overview_agent_question_var = self._make_text_var(
            self.state.overview_agent_question,
        )
        self.status_rail_collapsed = False

    def build(self, state: WorkbenchState | None = None) -> None:
        self.state = state or self.controller.load_status()
        self.file_limit_var.set(str(self.state.requested_file_count))
        if (
            self.state.overview_agent_question
            and not self.state.overview_agent_messages
            and not self.overview_agent_question_var.get()
        ):
            self.overview_agent_question_var.set(self.state.overview_agent_question)
        _clear_window(self.root)
        container = self.tk.Frame(self.root, bg=SURFACE)
        container.pack(fill=self.tk.BOTH, expand=True, padx=14, pady=14)
        render_workbench_sections(
            parent=container,
            tk_module=self.tk,
            view_model=build_page_view_model(
                self.state,
                status_rail_collapsed=self.status_rail_collapsed,
            ),
            callbacks={
                "refresh": self.refresh_status,
                "choose_dir": self.choose_input_dir,
                "choose_file": self.choose_input_file,
                "scan": self.scan_candidates,
                "select_all": self.select_all_files,
                "select_supported": self.select_supported_files,
                "clear_selection": self.clear_selected_files,
                "start": self.start_run,
                "pause": self.request_pause,
                "stop": self.request_stop,
                "open_output": self.open_output_dir,
                "open_report": self.open_latest_report,
                "select_sql": lambda: self.select_section("sql"),
                "select_section": self.select_section,
                "select_config": self.select_config,
                "select_all_review": self.select_all_review,
                "clear_review_selection": self.clear_review_selection,
                "start_review": self.start_review,
                "save_config": self.save_config,
                "clear_config": self.clear_config,
                "toggle_status_rail": self.toggle_status_rail,
                "ask_overview_agent": self.ask_overview_agent,
                "overview_agent_question_var": self.overview_agent_question_var,
            },
        )

        for warning in self.state.warnings:
            self.tk.Label(container, text=f"Warning: {warning}", anchor="w").pack(
                fill=self.tk.X,
                pady=2,
            )

    def start_tray(self, state: WorkbenchState | None = None) -> bool:
        run_detached = getattr(self.tray_adapter, "run_detached", None)
        if run_detached is None:
            return False
        return bool(run_detached(state or self.state))

    def handle_close(self) -> None:
        self.state = self.controller.load_status()
        if self.state.run_status != "running":
            self.exit_app()
            return

        choice = _ask_running_close_choice(self.root, self.tk)
        if choice == "background":
            self.root.withdraw()

    def show_window(self) -> None:
        deiconify = getattr(self.root, "deiconify", None)
        lift = getattr(self.root, "lift", None)
        if deiconify is not None:
            deiconify()
        if lift is not None:
            lift()

    def start_run(self) -> None:
        if not self._apply_file_limit():
            return
        self.controller.start_run()
        if hasattr(self.tk, "Frame"):
            self.build(self.controller.load_status())

    def request_pause(self) -> None:
        self._show_result(self.controller.request_pause())
        if hasattr(self.tk, "Frame"):
            self.build(self.controller.load_status())

    def request_stop(self) -> None:
        self._show_result(self.controller.request_stop())
        if hasattr(self.tk, "Frame"):
            self.build(self.controller.load_status())

    def open_output_dir(self) -> None:
        output_dir = self.state.output_dir
        if not output_dir:
            return
        self._show_result(self.controller.open_directory(output_dir))

    def open_latest_report(self) -> None:
        report_path = self.state.latest_report_path
        if not report_path:
            return
        self._show_result(self.controller.open_report(report_path))

    def refresh_status(self) -> None:
        self.build(self.controller.load_status())

    def ask_overview_agent(self) -> None:
        result = self.controller.ask_overview_agent(self.overview_agent_question_var.get())
        if not result.accepted:
            self._show_result(result)
        else:
            self.overview_agent_question_var.set("")
        if hasattr(self.tk, "Frame"):
            self.build(result.state or self.controller.load_status())

    def scan_candidates(self) -> None:
        self.build(self.controller.scan_candidates([self.state.input_dir]))

    def select_section(self, section_key: str) -> None:
        self.build(self.controller.set_active_section(section_key))

    def select_config(self, item_key: str) -> None:
        state = self.controller.select_config_item(item_key)
        self.build(state)
        if state.config_panel is None:
            return
        opened = render_config_dialog(
            parent=self.root,
            tk_module=self.tk,
            config_panel=state.config_panel,
            callbacks={
                "save_config": self.save_config,
                "clear_config": self.clear_config,
            },
        )
        if not opened:
            self._show_message("当前环境不支持配置弹窗。")

    def save_config(self, item_key: str, values: dict[str, object]) -> None:
        self._show_result(self.controller.save_config_values(item_key, values))
        if hasattr(self.tk, "Frame"):
            self.build(self.controller.load_status())

    def clear_config(self, item_key: str) -> None:
        self._show_result(self.controller.clear_config_values(item_key))
        if hasattr(self.tk, "Frame"):
            self.build(self.controller.load_status())

    def toggle_status_rail(self) -> None:
        self.status_rail_collapsed = not self.status_rail_collapsed
        if hasattr(self.tk, "Frame"):
            self.build(self.state)

    def choose_input_dir(self) -> None:
        filedialog = getattr(self.tk, "filedialog", None)
        if filedialog is None or not hasattr(filedialog, "askdirectory"):
            self._show_message("当前环境不支持目录选择。")
            return
        selected = filedialog.askdirectory(title="选择 NAS 或输入目录")
        if not selected:
            return
        self.build(self.controller.set_input_dir(selected))

    def choose_input_file(self) -> None:
        filedialog = getattr(self.tk, "filedialog", None)
        if filedialog is None or not hasattr(filedialog, "askopenfilename"):
            self._show_message("当前环境不支持文件选择。")
            return
        selected = filedialog.askopenfilename(title="选择文件")
        if not selected:
            return
        self.build(self.controller.scan_candidates([selected]))

    def select_all_files(self) -> None:
        next_state = self.state
        for item in self.state.file_items:
            next_state = self.controller.set_file_selected(item.full_path, True)
        self.build(next_state)

    def select_supported_files(self) -> None:
        next_state = self.state
        for item in self.state.file_items:
            next_state = self.controller.set_file_selected(item.full_path, item.supported)
        self.build(next_state)

    def clear_selected_files(self) -> None:
        next_state = self.state
        for item in self.state.file_items:
            next_state = self.controller.set_file_selected(item.full_path, False)
        self.build(next_state)

    def select_all_review(self) -> None:
        self.build(self.controller.set_all_review_selected(True))

    def clear_review_selection(self) -> None:
        self.build(self.controller.set_all_review_selected(False))

    def start_review(self) -> None:
        result = self.controller.start_selected_reviews()
        self._show_result(result)
        if hasattr(self.tk, "Frame"):
            self.build(result.state or self.controller.load_status())

    def _apply_file_limit(self) -> bool:
        try:
            self.controller.set_file_limit(self.file_limit_var.get())
        except ValueError:
            self._show_message("本次处理数量请输入 1 到 50 之间的整数。")
            return False
        return True

    def exit_app(self) -> None:
        stop_tray = getattr(self.tray_adapter, "stop", None)
        if stop_tray is not None:
            stop_tray()
        self.root.destroy()

    def _button(
        self,
        parent: Any,
        text: str,
        command: Any,
        *,
        enabled: bool,
    ) -> None:
        state = self.tk.NORMAL if enabled else self.tk.DISABLED
        self.tk.Button(parent, text=text, command=command, state=state).pack(
            side=self.tk.LEFT,
            padx=(0, 8),
        )

    def _make_text_var(self, value: str) -> Any:
        if hasattr(self.tk, "StringVar"):
            return self.tk.StringVar(value=value)
        return _SimpleTextVar(value)

    def _show_result(self, result: Any) -> None:
        self._show_message(getattr(result, "message", ""))

    def _show_message(self, message: str) -> None:
        messagebox = getattr(self.tk, "messagebox", None)
        if messagebox is not None and hasattr(messagebox, "showinfo"):
            messagebox.showinfo(title=APP_TITLE, message=message)

    def _build_directory_panel(self, container: Any) -> None:
        panel = self.tk.Frame(container, bg=PANEL)
        panel.pack(fill=self.tk.X, pady=(0, 10))
        self.tk.Label(
            panel,
            text=f"输入目录: {self.state.input_dir or '未设置'}",
            anchor="w",
            bg=PANEL,
            fg=TEXT,
        ).pack(fill=self.tk.X, padx=12, pady=(10, 3))
        self.tk.Label(
            panel,
            text=f"本地输出: {self.state.output_dir or '未生成'}",
            anchor="w",
            bg=PANEL,
            fg=MUTED,
        ).pack(fill=self.tk.X, padx=12, pady=(0, 6))
        limit_row = self.tk.Frame(panel, bg=PANEL)
        limit_row.pack(fill=self.tk.X, padx=12, pady=(0, 10))
        self.tk.Label(
            limit_row,
            text="本次处理数量",
            anchor="w",
            bg=PANEL,
            fg=TEXT,
        ).pack(side=self.tk.LEFT, padx=(0, 8))
        if hasattr(self.tk, "Spinbox"):
            self.tk.Spinbox(
                limit_row,
                from_=1,
                to=50,
                width=6,
                textvariable=self.file_limit_var,
            ).pack(side=self.tk.LEFT, padx=(0, 10))
        elif hasattr(self.tk, "Entry"):
            self.tk.Entry(
                limit_row,
                width=8,
                textvariable=self.file_limit_var,
            ).pack(side=self.tk.LEFT, padx=(0, 10))
        else:
            self.tk.Label(
                limit_row,
                text=str(self.state.requested_file_count),
                bg=PANEL,
                fg=TEXT,
            ).pack(side=self.tk.LEFT, padx=(0, 10))
        self.tk.Label(
            limit_row,
            text="建议先跑 3 到 5 个文件；真实 SQL 上传仍由最终确认控制。",
            anchor="w",
            bg=PANEL,
            fg=MUTED,
        ).pack(side=self.tk.LEFT)

    def _build_config_panel(self, container: Any) -> None:
        self.tk.Label(
            container,
            text="配置状态",
            font=("Microsoft YaHei UI", 12, "bold"),
            anchor="w",
            bg=SURFACE,
            fg=TEXT,
        ).pack(fill=self.tk.X, pady=(2, 4))
        panel = self.tk.Frame(container, bg=SURFACE)
        panel.pack(fill=self.tk.X, pady=(0, 8))
        for item in self.state.config_items:
            label = f"{item.name}: {item.status}"
            if item.detail:
                label = f"{label} ({item.detail})"
            self.tk.Label(
                panel,
                text=label,
                anchor="w",
                bg=SURFACE,
                fg=_status_color(item.status),
            ).pack(fill=self.tk.X, pady=1)

    def _build_status_panel(self, container: Any) -> None:
        panel = self.tk.Frame(container, bg=SURFACE)
        panel.pack(fill=self.tk.X, pady=(0, 4))
        for line in _state_lines(self.state):
            self.tk.Label(
                panel,
                text=line,
                anchor="w",
                bg=SURFACE,
                fg=TEXT,
            ).pack(fill=self.tk.X, pady=2)

    def _build_review_list(self, container: Any) -> None:
        self.tk.Label(
            container,
            text="文件审核列表",
            font=("Microsoft YaHei UI", 12, "bold"),
            anchor="w",
            bg=SURFACE,
            fg=TEXT,
        ).pack(fill=self.tk.X, pady=(10, 4))
        rows = _file_item_lines(self.state)
        if not rows:
            self.tk.Label(
                container,
                text="还没有处理结果。选择输入目录后启动 dry-run。",
                anchor="w",
                bg=SURFACE,
                fg=MUTED,
            ).pack(fill=self.tk.X, pady=(0, 8))
            return

        if hasattr(self.tk, "Listbox"):
            listbox = self.tk.Listbox(container, height=min(max(len(rows), 5), 14))
            listbox.pack(fill=self.tk.BOTH, expand=True, pady=(0, 8))
            for row in rows:
                listbox.insert(self.tk.END, row)
            return

        for row in rows:
            self.tk.Label(container, text=row, anchor="w").pack(fill=self.tk.X, pady=1)


def _state_lines(state: WorkbenchState) -> list[str]:
    return [
        f"Current run: {state.current_run_id or 'not_started'}",
        f"Run status: {state.run_status}",
        "Flow entry: real_flow_50",
        f"Requested files: {state.requested_file_count}",
        f"Current stage: {state.current_stage}",
        (
            "Status counts: "
            f"completed={state.completed_count}, total={state.new_file_count}, "
            f"pending={state.pending_count}, failed={state.failed_count}"
        ),
        f"Next: {state.suggested_next_action}",
    ]


def _file_item_lines(state: WorkbenchState) -> list[str]:
    lines: list[str] = []
    for item in state.file_items:
        reason = f" | reason={item.reason}" if item.reason else ""
        final_md = f" | final={item.final_markdown_path}" if item.final_markdown_path else ""
        lines.append(f"{item.status} | {item.file_name}{reason}{final_md}")
    return lines


def _status_color(status: str) -> str:
    if status in {"已配置", "环境存在"}:
        return ACCENT
    return MUTED


class _SimpleTextVar:
    def __init__(self, value: str) -> None:
        self._value = value

    def get(self) -> str:
        return self._value

    def set(self, value: str) -> None:
        self._value = value


def _clear_window(root: Any) -> None:
    children = getattr(root, "winfo_children", None)
    if children is None:
        return
    for child in children():
        destroy = getattr(child, "destroy", None)
        if destroy is not None:
            destroy()


def _ask_running_close_choice(root: Any, tk: Any) -> str:
    if hasattr(tk, "Toplevel"):
        return _ask_running_close_choice_dialog(root, tk)

    messagebox = getattr(tk, "messagebox", None)
    if messagebox is not None and hasattr(messagebox, "askyesno"):
        answer = messagebox.askyesno(
            title="Close workbench",
            message="real_flow_50 is running. Keep the workbench in the background?",
        )
        if answer is True:
            return "background"
    return "cancel"


def _ask_running_close_choice_dialog(root: Any, tk: Any) -> str:
    choice = {"value": "cancel"}
    dialog = tk.Toplevel(root)
    dialog.title("Close workbench")
    tk.Label(
        dialog,
        text="real_flow_50 is running. Keep the workbench in the background?",
    ).pack(padx=20, pady=(18, 10))
    buttons = tk.Frame(dialog)
    buttons.pack(padx=20, pady=(0, 18))

    def choose(value: str) -> None:
        choice["value"] = value
        dialog.destroy()

    tk.Button(buttons, text="Run in background", command=lambda: choose("background")).pack(
        side=tk.LEFT,
        padx=4,
    )
    tk.Button(buttons, text="Cancel close", command=lambda: choose("cancel")).pack(
        side=tk.LEFT,
        padx=4,
    )
    transient = getattr(dialog, "transient", None)
    grab_set = getattr(dialog, "grab_set", None)
    wait_window = getattr(root, "wait_window", None)
    if transient is not None:
        transient(root)
    if grab_set is not None:
        grab_set()
    if wait_window is not None:
        wait_window(dialog)
    return choice["value"]


def _load_tkinter() -> Any:
    import tkinter as tk
    from tkinter import filedialog, messagebox

    tk.filedialog = filedialog
    tk.messagebox = messagebox
    return tk


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="paper-pipeline-workbench-gui")
    parser.add_argument(
        "--base-dir",
        default="",
        help="Safe local workbench directory; defaults to .paper_pipeline_workbench.",
    )
    args = parser.parse_args(argv)
    return launch_windows_workbench(base_dir=args.base_dir or None)


if __name__ == "__main__":
    raise SystemExit(main())
