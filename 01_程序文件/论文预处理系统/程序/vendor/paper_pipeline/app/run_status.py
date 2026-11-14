"""Read-only run progress view models."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from paper_pipeline.domain import RunState


@dataclass(frozen=True, slots=True)
class FileStageRow:
    file_name: str
    source_path: str
    stage: str
    status: str
    tool: str
    reason: str = ""
    artifact_path: str = ""


@dataclass(frozen=True, slots=True)
class RunProgressViewModel:
    run_id: str
    run_status: str
    current_stage: str
    current_file: str
    total_count: int
    completed_count: int
    pending_count: int
    failed_count: int
    output_dir: str
    file_stage_rows: list[FileStageRow] = field(default_factory=list)


def build_run_progress(
    run_state: RunState | None,
    *,
    output_cleaned_dir: str | Path,
) -> RunProgressViewModel:
    if run_state is None:
        return RunProgressViewModel(
            run_id="",
            run_status="not_started",
            current_stage="ready",
            current_file="",
            total_count=0,
            completed_count=0,
            pending_count=0,
            failed_count=0,
            output_dir=str(output_cleaned_dir),
        )

    output_dir = Path(output_cleaned_dir)
    log_rows = _rows_from_pipeline_logs(run_state, output_dir)
    rows = log_rows or [
        FileStageRow(
            file_name=Path(file_state.source_path).name,
            source_path=file_state.source_path,
            stage="runtime",
            status=getattr(file_state.status, "value", str(file_state.status)),
            tool="real_flow_50",
            reason=file_state.reason,
        )
        for file_state in run_state.file_states
    ]
    return RunProgressViewModel(
        run_id=run_state.run_id,
        run_status=getattr(run_state.status, "value", str(run_state.status)),
        current_stage=_current_stage_from_rows(rows),
        current_file=run_state.current_source_path,
        total_count=run_state.total_items,
        completed_count=run_state.completed_items,
        pending_count=run_state.pending_items,
        failed_count=run_state.failed_items,
        output_dir=str(output_dir),
        file_stage_rows=rows,
    )


def _rows_from_pipeline_logs(run_state: RunState, output_dir: Path) -> list[FileStageRow]:
    rows: list[FileStageRow] = []
    run_dir = output_dir / "runs" / run_state.run_id
    if not run_dir.exists():
        return rows
    for log_path in sorted(run_dir.glob("*/logs/pipeline.json")):
        try:
            decoded = json.loads(log_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        source_path = str(decoded.get("source_path", ""))
        stages = decoded.get("stages", [])
        if not isinstance(stages, list):
            continue
        for stage in stages:
            if not isinstance(stage, dict):
                continue
            stage_name = str(stage.get("stage", ""))
            rows.append(
                FileStageRow(
                    file_name=Path(source_path).name,
                    source_path=source_path,
                    stage=stage_name,
                    status=str(stage.get("status", "")),
                    tool=stage_name,
                    reason=str(stage.get("reason", "")),
                    artifact_path=_first_log_path(stage),
                ),
            )
    return rows


def _first_log_path(stage: dict[str, Any]) -> str:
    logs = stage.get("logs", [])
    if isinstance(logs, list) and logs:
        return str(logs[0])
    return ""


def _current_stage_from_rows(rows: list[FileStageRow]) -> str:
    for row in rows:
        if row.status in {"running", "pending", "failed"}:
            return row.stage
    if rows:
        return rows[-1].stage
    return "ready"
