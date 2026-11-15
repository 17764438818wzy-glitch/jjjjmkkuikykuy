"""Trigger-only controller for the local paper pipeline workbench."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from paper_pipeline.app.actions import WorkbenchActionResult
from paper_pipeline.app.app_state import (
    WorkbenchConfigItem,
    WorkbenchFileItem,
    WorkbenchOverviewAgentMessage,
    WorkbenchState,
    WorkbenchStatusSummary,
)
from paper_pipeline.app.candidate_selection import (
    CandidateSelection,
    discover_candidates,
    prepare_selected_input_dir,
)
from paper_pipeline.app.config_store import WorkbenchConfigStore, patched_environ
from paper_pipeline.app.config_status import build_config_items
from paper_pipeline.app.history import load_run_history
from paper_pipeline.app.overview_agent import OverviewAgentConfig, OverviewAgentService
from paper_pipeline.app.index_view import build_index_state
from paper_pipeline.app.review_view import (
    CodexReviewActionGateway,
    build_library_review_items,
    build_review_item,
)
from paper_pipeline.app.run_status import build_run_progress
from paper_pipeline.app.sql_view import (
    DisabledSqlUploadGateway,
    build_sql_upload_state,
    load_latest_sql_preview,
)
from paper_pipeline.domain import RunState, RunStatus
from paper_pipeline.e2e import real_flow_50
from paper_pipeline.notifications import (
    FakeNotificationAdapter,
    NotificationEvent,
    NotificationLevel,
    NotificationService,
)
from paper_pipeline.runtime import load_run_state

RealFlowRunner = Callable[[real_flow_50.RealFlow50Config], Any]

_DELEGATED_ACTION_MESSAGE = (
    "This UI layer is a trigger only; execution is owned by "
    "paper_pipeline.e2e.real_flow_50."
)


@dataclass(slots=True)
class WorkbenchController:
    input_dir: str | Path
    library_dir: str | Path
    output_cleaned_dir: str | Path
    runs_dir: str | Path
    requested_file_count: int = 5
    current_run_state: RunState | None = None
    status_summary: WorkbenchStatusSummary = field(
        default_factory=WorkbenchStatusSummary,
    )
    notification_service: NotificationService = field(
        default_factory=lambda: NotificationService(FakeNotificationAdapter()),
    )
    real_flow_runner: RealFlowRunner | None = None
    opener: Any | None = None
    latest_report_path: str | Path = ""
    latest_evidence_bundle_path: str | Path = ""
    latest_notification_path: str | Path = ""
    candidate_selection: CandidateSelection | None = None
    workbench_dir: str | Path | None = None
    review_gateway: Any | None = None
    sql_upload_gateway: Any | None = None
    overview_agent_service: Any | None = None
    selected_config_key: str = ""
    config_store: WorkbenchConfigStore | None = None
    active_section: str = "overview"
    overview_agent_question: str = ""
    overview_agent_answer: str = ""
    overview_agent_message: str = ""
    overview_agent_messages: list[WorkbenchOverviewAgentMessage] = field(
        default_factory=list,
    )
    review_selection: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        self.input_dir = Path(self.input_dir)
        self.library_dir = Path(self.library_dir)
        self.output_cleaned_dir = Path(self.output_cleaned_dir)
        self.runs_dir = Path(self.runs_dir)
        self.workbench_dir = (
            Path(".paper_pipeline_workbench")
            if self.workbench_dir is None
            else Path(self.workbench_dir)
        )
        if self.review_gateway is None:
            self.review_gateway = CodexReviewActionGateway(self.library_dir)
        if self.sql_upload_gateway is None:
            self.sql_upload_gateway = DisabledSqlUploadGateway()
        if self.overview_agent_service is None:
            self.overview_agent_service = OverviewAgentService()
        if self.config_store is None:
            self.config_store = WorkbenchConfigStore.from_workbench_dir(self.workbench_dir)
        self._apply_saved_input_dir()
        self.requested_file_count = _normalize_file_limit(self.requested_file_count)

    def load_status(self) -> WorkbenchState:
        candidate_items = (
            list(self.candidate_selection.items)
            if self.candidate_selection is not None
            else []
        )
        has_candidate_plan = bool(candidate_items)
        run_state = None if has_candidate_plan else self.current_run_state or self._load_latest_run_state()
        config_env = self._config_env()
        run_status = (
            "plan_ready"
            if has_candidate_plan
            else _status_value(run_state.status) if run_state else "not_started"
        )
        report_path, evidence_bundle_path = _report_paths_for_run(
            self.library_dir,
            run_state,
        )
        run_output_dir = _output_dir_for_run(self.output_cleaned_dir, run_state)
        review_items = (
            []
            if has_candidate_plan
            else _review_items_for_library_or_run(
                self.library_dir,
                run_state,
                output_cleaned_dir=Path(self.output_cleaned_dir),
                selected_metadata_paths=self.review_selection,
            )
        )
        self.review_selection.intersection_update({item.metadata_path for item in review_items})
        latest_preview = None if has_candidate_plan else load_latest_sql_preview(
            self.library_dir,
            run_state,
        )
        sql_mode = _sql_mode(config_env)
        sql_upload_state = build_sql_upload_state(
            preview=latest_preview,
            sql_mode=sql_mode,
            connection_status=_sql_connection_status(sql_mode, config_env),
            target_table=config_env.get("POSTGRES_TABLE", ""),
        )
        summary = self.status_summary
        overview_agent_config = OverviewAgentConfig.from_environ(config_env)
        batch_states = list(getattr(run_state, "batch_states", []) if run_state else [])
        completed_batches = sum(
            _status_value(batch.status) == RunStatus.COMPLETED.value
            for batch in batch_states
        )

        pending_count = 0 if has_candidate_plan else run_state.pending_items if run_state else summary.pending_count
        failed_count = 0 if has_candidate_plan else run_state.failed_items if run_state else summary.failed_count
        warnings = []
        if run_status == RunStatus.INTERRUPTED.value:
            warnings.append(
                "Interrupted run detected; use real_flow_50/recovery entrypoints.",
            )

        state = WorkbenchState(
            current_run_id=run_state.run_id if run_state else "",
            run_status=run_status,
            plan_status="plan_ready" if has_candidate_plan else "delegated_to_real_flow_50",
            can_start=run_status
            not in {
                RunStatus.RUNNING.value,
                RunStatus.PAUSING.value,
                RunStatus.STOPPING.value,
            },
            can_pause=False,
            can_stop=False,
            can_continue=False,
            can_abandon=False,
            scanned_count=len(candidate_items) if has_candidate_plan else 0,
            new_file_count=(
                len(candidate_items)
                if has_candidate_plan
                else run_state.total_items if run_state else 0
            ),
            duplicate_count=0,
            total_batches=len(batch_states),
            completed_batches=completed_batches,
            completed_count=0 if has_candidate_plan else run_state.completed_items if run_state else 0,
            pending_count=pending_count,
            failed_count=failed_count,
            stage0_rejected_count=0,
            indexed_count=0,
            index_failed_count=0,
            sql_eligible_count=sql_upload_state.eligible_count,
            sql_blocked_count=sql_upload_state.blocked_count,
            sql_uploaded_count=0,
            latest_report_path=report_path,
            latest_evidence_bundle_path=evidence_bundle_path,
            latest_notification_path="",
            warnings=warnings,
            suggested_next_action=_suggest_next_action(run_status),
            gold_count=0,
            silver_count=0,
            pending_tier_count=0,
            rejected_count=0,
            current_stage=_current_stage(run_status),
            output_dir=run_output_dir,
            input_dir=str(self.input_dir),
            library_dir=str(self.library_dir),
            requested_file_count=self.requested_file_count,
            config_items=build_config_items(env=config_env),
            file_items=(
                candidate_items
                if has_candidate_plan
                else _file_items_from_run_state(
                    run_state,
                    output_cleaned_dir=Path(self.output_cleaned_dir),
                )
            ),
            run_progress=build_run_progress(
                run_state,
                output_cleaned_dir=self.output_cleaned_dir,
            ),
            review_items=review_items,
            index_state=build_index_state(self.library_dir),
            sql_upload_state=sql_upload_state,
            history_items=load_run_history(self.runs_dir),
            selected_config_key=self.selected_config_key,
            config_panel=self._selected_config_panel(config_env),
            active_section=self.active_section,
            overview_agent_available=overview_agent_config.configured,
            overview_agent_question=self.overview_agent_question,
            overview_agent_answer=self.overview_agent_answer,
            overview_agent_message=self.overview_agent_message,
            overview_agent_messages=list(self.overview_agent_messages),
        )
        state.summary = WorkbenchStatusSummary(
            pending_count=state.pending_count,
            failed_count=state.failed_count,
            sql_blocked_count=state.sql_blocked_count,
            stage0_rejected_count=state.stage0_rejected_count,
            indexed_count=state.indexed_count,
            index_failed_count=state.index_failed_count,
            sql_eligible_count=state.sql_eligible_count,
            sql_uploaded_count=state.sql_uploaded_count,
            gold_count=state.gold_count,
            silver_count=state.silver_count,
            pending_tier_count=state.pending_tier_count,
            rejected_count=state.rejected_count,
            current_stage=state.current_stage,
            output_dir=state.output_dir,
        )
        return state

    def set_input_dir(self, input_dir: str | Path) -> WorkbenchState:
        self.input_dir = Path(input_dir)
        return self.load_status()

    def set_file_limit(self, file_count: int | str) -> WorkbenchState:
        self.requested_file_count = _normalize_file_limit(file_count)
        return self.load_status()

    def scan_candidates(self, paths: list[str | Path] | None = None) -> WorkbenchState:
        scan_paths = paths or [self.input_dir]
        self.candidate_selection = discover_candidates(list(scan_paths))
        return self.load_status()

    def set_file_selected(self, full_path: str, selected: bool) -> WorkbenchState:
        if self.candidate_selection is None:
            self.candidate_selection = CandidateSelection([])
        self.candidate_selection = self.candidate_selection.set_selected(full_path, selected)
        return self.load_status()

    def select_config_item(self, item_key: str) -> WorkbenchState:
        self.selected_config_key = item_key
        self.active_section = "config"
        return self.load_status()

    def set_active_section(self, section_key: str) -> WorkbenchState:
        self.active_section = section_key
        return self.load_status()

    def save_config_values(
        self,
        item_key: str,
        values: dict[str, object],
    ) -> WorkbenchActionResult:
        assert self.config_store is not None
        self.selected_config_key = item_key
        saved_keys = self.config_store.save_item(item_key, values)
        if item_key == "nas_input_dir":
            self._apply_saved_input_dir()
        return self._result(
            "save_config_values",
            True,
            "Configuration saved.",
            data={
                "item_key": item_key,
                "saved_keys": saved_keys,
                "config_path": str(self.config_store.path),
            },
        )

    def clear_config_values(self, item_key: str) -> WorkbenchActionResult:
        assert self.config_store is not None
        self.selected_config_key = item_key
        removed_keys = self.config_store.clear_item(item_key)
        return self._result(
            "clear_config_values",
            True,
            "Configuration cleared.",
            data={
                "item_key": item_key,
                "removed_keys": removed_keys,
                "config_path": str(self.config_store.path),
            },
        )

    def ask_overview_agent(self, question: str) -> WorkbenchActionResult:
        normalized_question = question.strip()
        self.overview_agent_question = normalized_question
        self.overview_agent_answer = ""
        self.overview_agent_message = ""
        state = self.load_status()
        config = OverviewAgentConfig.from_environ(self._config_env())
        assert self.overview_agent_service is not None
        result = self.overview_agent_service.answer(
            question=normalized_question,
            state=state,
            config=config,
        )
        self.overview_agent_question = result.question or normalized_question
        self.overview_agent_answer = result.answer
        self.overview_agent_message = result.message
        if result.accepted:
            self.overview_agent_messages = _trim_overview_agent_messages(
                [
                    *self.overview_agent_messages,
                    WorkbenchOverviewAgentMessage(
                        role="user",
                        content=self.overview_agent_question,
                    ),
                    WorkbenchOverviewAgentMessage(
                        role="assistant",
                        content=result.answer,
                    ),
                ],
            )
        return self._result(
            "ask_overview_agent",
            result.accepted,
            result.message,
            data={
                "question": self.overview_agent_question,
                "answer": result.answer,
            },
            errors=[] if result.accepted else [result.message],
            level=NotificationLevel.INFO if result.accepted else NotificationLevel.WARNING,
        )

    def start_run(self) -> WorkbenchActionResult:
        result = self._run_real_flow(created_by="windows-workbench")
        self.current_run_state = self._load_real_flow_run_state(result)
        self.candidate_selection = None
        return self._result(
            "start_run",
            True,
            "Submitted to real_flow_50.",
            data={"run_id": result.run_id},
        )

    def request_pause(self) -> WorkbenchActionResult:
        return self._delegated_result("request_pause")

    def request_stop(self) -> WorkbenchActionResult:
        return self._delegated_result("request_stop")

    def continue_run(self) -> WorkbenchActionResult:
        return self._delegated_result("continue_run")

    def abandon_run(self) -> WorkbenchActionResult:
        return self._delegated_result("abandon_run")

    def open_report(self, _path: str | Path | None = None) -> WorkbenchActionResult:
        if self.opener is not None and _path is not None:
            return self._open_safe_path(_path, action="open_report")
        return self._delegated_result("open_report")

    def open_directory(self, _path: str | Path) -> WorkbenchActionResult:
        if self.opener is not None:
            return self._open_safe_path(_path, action="open_directory")
        return self._delegated_result("open_directory")

    def apply_review_decision(
        self,
        *,
        metadata_path: str,
        new_tier: str,
        reason: str,
        confirmed_by: str = "workbench-user",
    ) -> Any:
        assert self.review_gateway is not None
        return self.review_gateway.apply_decision(
            metadata_path=metadata_path,
            new_tier=new_tier,
            reason=reason,
            confirmed_by=confirmed_by,
        )

    def set_review_selected(self, metadata_path: str, selected: bool) -> WorkbenchState:
        normalized_path = str(Path(metadata_path))
        if selected:
            self.review_selection.add(normalized_path)
        else:
            self.review_selection.discard(normalized_path)
        self.active_section = "review"
        return self.load_status()

    def set_all_review_selected(self, selected: bool) -> WorkbenchState:
        current_items = _review_items_for_library_or_run(
            self.library_dir,
            self.current_run_state or self._load_latest_run_state(),
            output_cleaned_dir=Path(self.output_cleaned_dir),
            selected_metadata_paths=self.review_selection,
        )
        if selected:
            self.review_selection = {item.metadata_path for item in current_items}
        else:
            self.review_selection.clear()
        self.active_section = "review"
        return self.load_status()

    def start_selected_reviews(self) -> WorkbenchActionResult:
        state = self.load_status()
        selected_paths = [
            item.metadata_path
            for item in state.review_items
            if getattr(item, "selected", False) and item.metadata_path
        ]
        if not selected_paths:
            return self._result(
                "start_review",
                False,
                "请先选择待复审论文。",
                errors=["no review candidates selected"],
                level=NotificationLevel.WARNING,
            )
        assert self.review_gateway is not None
        with patched_environ(self._config_overrides()):
            payload = self.review_gateway.review_selected(
                metadata_paths=selected_paths,
                confirmed_by="workbench-user",
            )
        reviewed_count = int(payload.get("reviewed_count", 0))
        failed_count = int(payload.get("failed_count", 0))
        if failed_count:
            message = f"复审完成：{reviewed_count} 篇已处理，{failed_count} 篇失败或仍需处理。"
            level = NotificationLevel.WARNING
        else:
            message = f"复审完成：已处理 {reviewed_count} 篇。"
            level = NotificationLevel.INFO
        self.review_selection.clear()
        self.active_section = "review"
        return self._result(
            "start_review",
            reviewed_count > 0,
            message,
            data=dict(payload),
            warnings=[message] if failed_count else [],
            level=level,
        )

    def submit_sql_upload_confirmation(
        self,
        *,
        preview_id: str,
        confirmation_text: str,
        confirmed_by: str = "workbench-user",
    ) -> Any:
        assert self.sql_upload_gateway is not None
        return self.sql_upload_gateway.confirm_upload(
            preview_id=preview_id,
            confirmation_text=confirmation_text,
            confirmed_by=confirmed_by,
        )

    def _delegated_result(self, action: str) -> WorkbenchActionResult:
        return self._result(
            action,
            False,
            _DELEGATED_ACTION_MESSAGE,
            errors=[_DELEGATED_ACTION_MESSAGE],
            level=NotificationLevel.WARNING,
        )

    def _open_safe_path(self, path: str | Path, *, action: str) -> WorkbenchActionResult:
        from paper_pipeline.app.safe_open import SafeOpener, SafeOpenPolicy

        assert self.opener is not None
        safe_opener = SafeOpener(
            SafeOpenPolicy(
                [
                    Path(self.input_dir),
                    Path(self.library_dir),
                    Path(self.output_cleaned_dir),
                    Path(self.workbench_dir or ".paper_pipeline_workbench"),
                ],
            ),
            open_adapter=self.opener.open,
        )
        result = safe_opener.open(path)
        return self._result(
            action,
            result.accepted,
            result.message,
            data={"path": result.path},
            errors=[] if result.accepted else [result.message],
        )

    def _run_real_flow(self, *, created_by: str) -> Any:
        runner = self.real_flow_runner or real_flow_50.run_real_flow_50
        input_dir = self.input_dir
        if self.candidate_selection is not None and self.candidate_selection.items:
            prepared = prepare_selected_input_dir(
                self.candidate_selection,
                workbench_dir=Path(self.workbench_dir or ".paper_pipeline_workbench"),
                run_token=self._active_run_id() or "next",
            )
            input_dir = prepared.input_dir
        with patched_environ(self._config_overrides()):
            return runner(
                real_flow_50.RealFlow50Config(
                    input_dir=input_dir,
                    library_dir=self.library_dir,
                    output_cleaned_dir=self.output_cleaned_dir,
                    run_id="",
                    expected_file_count=self.requested_file_count,
                    max_file_count=self.requested_file_count,
                    allow_less_than_50=True,
                    confirm_upload=False,
                    created_by=created_by,
                ),
            )

    def _config_env(self) -> dict[str, str]:
        assert self.config_store is not None
        return self.config_store.build_environ(os.environ)

    def _config_overrides(self) -> dict[str, str]:
        assert self.config_store is not None
        return self.config_store.build_runtime_overrides()

    def _selected_config_panel(self, environ: dict[str, str]) -> Any | None:
        if not self.selected_config_key:
            return None
        assert self.config_store is not None
        return self.config_store.build_panel(self.selected_config_key, environ=environ)

    def _apply_saved_input_dir(self) -> None:
        configured = self._config_overrides().get("PAPER_PIPELINE_INPUT_DIR", "").strip()
        if configured:
            self.input_dir = Path(configured)

    def _load_real_flow_run_state(self, result: Any) -> RunState:
        state_path = Path(result.library_dir) / "runs" / result.run_id / "run_state.json"
        if state_path.exists():
            return load_run_state(state_path)
        return RunState(run_id=result.run_id, status=RunStatus.COMPLETED)

    def _load_latest_run_state(self) -> RunState | None:
        runs_dir = Path(self.runs_dir)
        if not runs_dir.exists():
            return None
        state_paths = sorted(
            runs_dir.glob("*/run_state.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        for state_path in state_paths:
            try:
                state = load_run_state(state_path)
            except (OSError, ValueError, TypeError):
                continue
            self.current_run_state = state
            return state
        return None

    def _active_run_id(self) -> str:
        if self.current_run_state is None:
            return ""
        return self.current_run_state.run_id

    def _result(
        self,
        action: str,
        accepted: bool,
        message: str,
        *,
        data: dict[str, Any] | None = None,
        warnings: list[str] | None = None,
        errors: list[str] | None = None,
        requires_confirmation: bool = False,
        level: NotificationLevel = NotificationLevel.INFO,
    ) -> WorkbenchActionResult:
        notification = self._notify(
            action=action,
            accepted=accepted,
            message=message,
            level=level,
            evidence=data or {},
        )
        payload = dict(data or {})
        if notification is not None:
            payload["notification_event_id"] = notification.event_id
        return WorkbenchActionResult(
            action=action,
            accepted=accepted,
            message=message,
            state=self.load_status(),
            data=payload,
            warnings=list(warnings or []),
            errors=list(errors or []),
            requires_confirmation=requires_confirmation,
        )

    def _notify(
        self,
        *,
        action: str,
        accepted: bool,
        message: str,
        level: NotificationLevel,
        evidence: dict[str, Any],
    ) -> NotificationEvent | None:
        run_id = self._active_run_id() or "WORKBENCH"
        state = self.load_status()
        safe_evidence = {
            "accepted": accepted,
            "pending_count": state.pending_count,
            "failed_count": state.failed_count,
            "suggested_next_action": state.suggested_next_action,
            **evidence,
        }
        try:
            return self.notification_service.notify(
                run_id=run_id,
                scenario=f"workbench_{action}",
                level=level,
                title=f"Paper pipeline workbench: {action}",
                message=message,
                evidence=safe_evidence,
            )
        except Exception:
            return None


def _status_value(value: object) -> str:
    return getattr(value, "value", str(value))


def _trim_overview_agent_messages(
    messages: list[WorkbenchOverviewAgentMessage],
) -> list[WorkbenchOverviewAgentMessage]:
    return [
        item
        for item in messages[-12:]
        if item.role in {"user", "assistant"} and item.content.strip()
    ]


def _current_stage(run_status: str) -> str:
    if run_status == RunStatus.RUNNING.value:
        return "running"
    if run_status == RunStatus.INTERRUPTED.value:
        return "interrupted"
    if run_status == RunStatus.COMPLETED.value:
        return "completed"
    if run_status == "plan_ready":
        return "plan_ready"
    return "ready"


def _suggest_next_action(run_status: str) -> str:
    if run_status == "plan_ready":
        return "处理计划已生成，点击开始运行。"
    if run_status == RunStatus.RUNNING.value:
        return "正在运行，请等待统一流程完成。"
    if run_status == RunStatus.INTERRUPTED.value:
        return "运行已中断，请使用 real_flow_50 恢复入口继续。"
    if run_status == RunStatus.COMPLETED.value:
        return "运行已完成，请查看生成结果。"
    return "请选择文件或目录，生成处理计划。"


def _normalize_file_limit(value: int | str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise ValueError("file limit must be an integer") from None
    if parsed < 1 or parsed > 50:
        raise ValueError("file limit must be between 1 and 50")
    return parsed


def _config_items_from_environment() -> list[WorkbenchConfigItem]:
    env = os.environ
    openai_key = bool(env.get("OPENAI_API_KEY", "").strip())
    codex_key = bool(env.get("CODEX_REVIEW_API_KEY", "").strip())
    dity_endpoint = bool(env.get("DITY_LLM_ENDPOINT", "").strip())
    postgres_ready = bool(
        (
            env.get("POSTGRES_DSN", "").strip()
            or env.get("DATABASE_URL", "").strip()
            or (
                env.get("POSTGRES_HOST", "").strip()
                and env.get("POSTGRES_DB", "").strip()
                and env.get("POSTGRES_USER", "").strip()
            )
        )
        and env.get(env.get("POSTGRES_PASSWORD_ENV", "POSTGRES_PASSWORD"), "").strip()
    )
    mineru_ready = bool(
        env.get("MINERU_PATH", "").strip()
        or env.get("DITY_MINERU_PATH", "").strip()
    )
    libreoffice_ready = bool(
        env.get("LIBREOFFICE_PATH", "").strip()
        or env.get("DITY_LIBREOFFICE_PATH", "").strip()
    )

    llm_status = "已配置" if openai_key or dity_endpoint else "未配置"
    llm_detail = "OPENAI_API_KEY" if openai_key else ""
    if dity_endpoint:
        llm_detail = "DITY_LLM_ENDPOINT"
    if not llm_detail:
        llm_detail = "使用本地 fallback"

    return [
        WorkbenchConfigItem("LLM", llm_status, llm_detail),
        WorkbenchConfigItem(
            "Codex Review",
            "已配置" if codex_key or openai_key else "未配置",
            "CODEX_REVIEW_API_KEY/OPENAI_API_KEY" if codex_key or openai_key else "不会自动复审",
        ),
        WorkbenchConfigItem(
            "PostgreSQL",
            "已配置" if postgres_ready else "未配置",
            "真实 SQL 仍需二次确认" if postgres_ready else "当前保持 dry-run/fake",
        ),
        WorkbenchConfigItem(
            "MinerU",
            "已配置" if mineru_ready else "未配置",
            "未配置时使用可用 fallback",
        ),
        WorkbenchConfigItem(
            "LibreOffice",
            "已配置" if libreoffice_ready else "未配置",
            "未配置时 docx_xml 可兜底处理 docx",
        ),
    ]


def _output_dir_for_run(output_cleaned_dir: str | Path, run_state: RunState | None) -> str:
    root = Path(output_cleaned_dir)
    if run_state is None or not run_state.run_id:
        return str(root)
    return str(root / "runs" / run_state.run_id)


def _report_paths_for_run(
    library_dir: str | Path,
    run_state: RunState | None,
) -> tuple[str, str]:
    if run_state is None or not run_state.run_id:
        return "", ""
    reports_dir = Path(library_dir) / "runs" / run_state.run_id / "reports"
    manifest_path = reports_dir / "report_manifest.json"
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            manifest = {}
        if isinstance(manifest, dict):
            report_path = str(manifest.get("human_readable_report_path", "")).strip()
            evidence_path = str(manifest.get("evidence_bundle_path", "")).strip()
            if report_path or evidence_path:
                return report_path, evidence_path
    return (
        _path_text(_first_existing(reports_dir, ["human_readable_report.md", "run_report.json"])),
        _path_text(_first_existing(reports_dir, ["evidence_bundle.json"])),
    )


def _review_items_for_run(
    run_state: RunState | None,
    *,
    output_cleaned_dir: Path,
) -> list[Any]:
    if run_state is None or not run_state.run_id:
        return []
    final_paths: list[Path] = []
    seen: set[Path] = set()
    for artifact in run_state.completed_artifacts:
        artifact_uri = str(getattr(artifact, "artifact_uri", ""))
        if not artifact_uri or "://" in artifact_uri:
            continue
        final_path = Path(artifact_uri)
        if final_path.name != "final.md":
            continue
        resolved = final_path.resolve()
        if resolved not in seen:
            seen.add(resolved)
            final_paths.append(final_path)
    run_output_dir = output_cleaned_dir / "runs" / run_state.run_id
    if run_output_dir.exists():
        for final_path in sorted(run_output_dir.glob("*/final.md")):
            resolved = final_path.resolve()
            if resolved not in seen:
                seen.add(resolved)
                final_paths.append(final_path)
    return [
        build_review_item(
            markdown_path=final_path,
            metadata_path=final_path.with_name("metadata.json"),
        )
        for final_path in final_paths
    ]


def _review_items_for_library_or_run(
    library_dir: str | Path,
    run_state: RunState | None,
    *,
    output_cleaned_dir: Path,
    selected_metadata_paths: set[str],
) -> list[Any]:
    library_items = build_library_review_items(
        library_dir,
        selected_metadata_paths=selected_metadata_paths,
    )
    if library_items:
        return library_items
    return _review_items_for_run(
        run_state,
        output_cleaned_dir=output_cleaned_dir,
    )


def _sql_mode(environ: dict[str, str]) -> str:
    return (
        environ.get("PAPER_PIPELINE_SQL_MODE", "").strip()
        or environ.get("SQL_UPLOAD_MODE", "").strip()
        or "dry-run"
    )


def _sql_connection_status(sql_mode: str, environ: dict[str, str]) -> str:
    if sql_mode != "real":
        return "not-tested"
    if (
        environ.get("POSTGRES_DSN", "").strip()
        or environ.get("DATABASE_URL", "").strip()
        or (
            environ.get("POSTGRES_HOST", "").strip()
            and environ.get("POSTGRES_DB", "").strip()
            and environ.get("POSTGRES_USER", "").strip()
        )
    ):
        return "ok"
    return "missing-config"


def _first_existing(root: Path, names: list[str]) -> Path | None:
    for name in names:
        candidate = root / name
        if candidate.exists():
            return candidate
    return None


def _path_text(path: Path | None) -> str:
    return "" if path is None else str(path)


def _file_items_from_run_state(
    run_state: RunState | None,
    *,
    output_cleaned_dir: Path,
) -> list[WorkbenchFileItem]:
    if run_state is None:
        return []

    artifact_by_source = {
        artifact.source_path: artifact.artifact_uri
        for artifact in run_state.completed_artifacts
        if artifact.artifact_uri
    }
    return [
        _file_item_from_state(
            file_state,
            artifact_uri=artifact_by_source.get(file_state.source_path, ""),
            output_cleaned_dir=output_cleaned_dir,
            run_id=run_state.run_id,
        )
        for file_state in run_state.file_states
    ]


def _file_item_from_state(
    file_state: Any,
    *,
    artifact_uri: str,
    output_cleaned_dir: Path,
    run_id: str,
) -> WorkbenchFileItem:
    source_path = str(file_state.source_path)
    final_markdown_path = _displayable_artifact_path(artifact_uri)
    metadata_path = _metadata_path_for(final_markdown_path)
    if not final_markdown_path:
        final_markdown_path = _fallback_final_markdown_path(
            output_cleaned_dir=output_cleaned_dir,
            run_id=run_id,
            source_path=source_path,
        )
        metadata_path = _metadata_path_for(final_markdown_path)

    return WorkbenchFileItem(
        source_path=source_path,
        file_name=Path(source_path).name,
        status=_status_value(file_state.status),
        reason=str(getattr(file_state, "reason", "")),
        final_markdown_path=final_markdown_path,
        metadata_path=metadata_path,
    )


def _displayable_artifact_path(artifact_uri: str) -> str:
    if not artifact_uri:
        return ""
    if "://" in artifact_uri:
        return ""
    return artifact_uri


def _metadata_path_for(final_markdown_path: str) -> str:
    if not final_markdown_path:
        return ""
    final_path = Path(final_markdown_path)
    if final_path.name != "final.md":
        return ""
    metadata_path = final_path.with_name("metadata.json")
    return str(metadata_path) if metadata_path.exists() else str(metadata_path)


def _fallback_final_markdown_path(
    *,
    output_cleaned_dir: Path,
    run_id: str,
    source_path: str,
) -> str:
    run_output_dir = output_cleaned_dir / "runs" / run_id
    if not run_output_dir.exists():
        return ""
    source_name = Path(source_path).stem.lower()
    for final_path in sorted(run_output_dir.glob("*/final.md")):
        if source_name and source_name in final_path.parent.name.lower():
            return str(final_path)
    return ""
