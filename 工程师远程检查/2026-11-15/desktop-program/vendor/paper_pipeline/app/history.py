"""Read-only run history for the workbench."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from paper_pipeline.runtime import load_run_state


@dataclass(frozen=True, slots=True)
class WorkbenchRunHistoryItem:
    run_id: str
    started_at: str
    input_dir: str
    total_count: int
    completed_count: int
    pending_count: int
    failed_count: int
    sql_uploaded_count: int
    report_path: str
    run_state_path: str
    output_dir: str
    review_record_path: str


def load_run_history(runs_dir: str | Path) -> list[WorkbenchRunHistoryItem]:
    root = Path(runs_dir)
    if not root.exists():
        return []
    state_paths = sorted(
        root.glob("*/run_state.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    items: list[WorkbenchRunHistoryItem] = []
    for state_path in state_paths:
        try:
            state = load_run_state(state_path)
        except (OSError, ValueError, TypeError):
            continue
        run_dir = state_path.parent
        items.append(
            WorkbenchRunHistoryItem(
                run_id=state.run_id,
                started_at=state.started_at.isoformat(),
                input_dir="",
                total_count=state.total_items,
                completed_count=state.completed_items,
                pending_count=state.pending_items,
                failed_count=state.failed_items,
                sql_uploaded_count=1 if state.sql_upload_triggered else 0,
                report_path=_path_text(
                    _first_existing(
                        run_dir,
                        [
                            "reports/human_readable_report.md",
                            "reports/run_report.json",
                            "reports/report_manifest.json",
                            "report.md",
                            "run_report.md",
                        ],
                    ),
                ),
                run_state_path=str(state_path),
                output_dir=str(run_dir),
                review_record_path=_path_text(
                    _first_existing(run_dir, ["review_history.json", "audit/audit.jsonl"]),
                ),
            ),
        )
    return items


def _first_existing(root: Path, names: list[str]) -> Path | None:
    for name in names:
        candidate = root / name
        if candidate.exists():
            return candidate
    return None


def _path_text(path: Path | None) -> str:
    return "" if path is None else str(path)
