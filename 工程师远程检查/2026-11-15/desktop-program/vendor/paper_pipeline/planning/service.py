"""Processing plan generation and start-confirmation guards."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from paper_pipeline.domain import (
    BatchPlan,
    ProcessingPlan,
    ProcessingPlanStatus,
    SourceFileRecord,
)

DEFAULT_BATCH_SIZE = 50


class ProcessingPlanExecutionBlocked(RuntimeError):
    """Raised when a plan is not allowed to start processing."""


def generate_processing_plan(
    *,
    library_dir: str | Path,
    output_cleaned_dir: str | Path,
    scan_result: object | None = None,
    records: Sequence[SourceFileRecord] | None = None,
    summary: Mapping[str, Any] | object | None = None,
    run_id: str | None = None,
    input_dir: str | Path | None = None,
    runs_dir: str | Path | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> ProcessingPlan:
    """Build a draft processing plan from task 03 scan output.

    This function intentionally does not call scan_input_raw. Callers must pass
    either a ScanResult-like object or an already prepared summary/record list.
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than 0")

    resolved_summary = summary
    resolved_records = list(records or [])
    if scan_result is not None:
        if resolved_summary is None:
            resolved_summary = getattr(scan_result, "summary")
        if not resolved_records:
            resolved_records = list(getattr(scan_result, "records"))

    if resolved_summary is None and not resolved_records:
        raise ValueError("scan_result, summary, or records must be provided")

    total_scanned_count = _int_summary_value(
        resolved_summary,
        "total_files",
        len(resolved_records),
    )
    duplicate_skip_count = _int_summary_value(
        resolved_summary,
        "duplicate_files",
        sum(1 for record in resolved_records if record.is_duplicate),
    )
    new_file_count = _int_summary_value(
        resolved_summary,
        "unique_files",
        total_scanned_count - duplicate_skip_count,
    )
    estimated_process_count = new_file_count
    plan_run_id = run_id or str(_summary_value(resolved_summary, "run_id", ""))
    if not plan_run_id:
        plan_run_id = _default_run_id()

    plan_input_dir = str(
        input_dir
        if input_dir is not None
        else _summary_value(resolved_summary, "input_root", ""),
    )
    if runs_dir is not None and plan_input_dir:
        ensure_runs_dir_outside_input_dir(
            input_dir=plan_input_dir,
            runs_dir=runs_dir,
        )
    processable_records = [
        record for record in resolved_records if not record.is_duplicate
    ]
    batches = _build_batches(
        run_id=plan_run_id,
        records=processable_records,
        estimated_process_count=estimated_process_count,
        batch_size=batch_size,
    )

    return ProcessingPlan(
        run_id=plan_run_id,
        created_at=_now_utc(),
        input_dir=plan_input_dir,
        library_dir=str(library_dir),
        output_cleaned_dir=str(output_cleaned_dir),
        total_scanned_count=total_scanned_count,
        new_file_count=new_file_count,
        duplicate_skip_count=duplicate_skip_count,
        estimated_process_count=estimated_process_count,
        batch_size=batch_size,
        batch_count=len(batches),
        will_write_sql=False,
        requires_user_sql_confirmation=True,
        status=ProcessingPlanStatus.DRAFT,
        batches=batches,
    )


def confirm_processing_plan(plan: ProcessingPlan) -> ProcessingPlan:
    if plan.status == ProcessingPlanStatus.CANCELLED:
        raise ProcessingPlanExecutionBlocked("处理计划已取消，不能确认。")
    plan.status = ProcessingPlanStatus.CONFIRMED
    plan.will_write_sql = False
    plan.requires_user_sql_confirmation = True
    return plan


def cancel_processing_plan(plan: ProcessingPlan) -> ProcessingPlan:
    plan.status = ProcessingPlanStatus.CANCELLED
    plan.will_write_sql = False
    plan.requires_user_sql_confirmation = True
    return plan


def ensure_processing_plan_can_execute(plan: ProcessingPlan) -> None:
    if plan.status == ProcessingPlanStatus.DRAFT:
        raise ProcessingPlanExecutionBlocked(
            "处理计划尚未确认，不能执行处理。请先由用户人工确认。",
        )
    if plan.status == ProcessingPlanStatus.CANCELLED:
        raise ProcessingPlanExecutionBlocked("处理计划已取消，不能执行处理。")
    if plan.status != ProcessingPlanStatus.CONFIRMED:
        raise ProcessingPlanExecutionBlocked(
            f"处理计划状态为 {plan.status.value}，不能执行处理。",
        )


def ensure_runs_dir_outside_input_dir(
    *,
    input_dir: str | Path,
    runs_dir: str | Path,
) -> None:
    input_path = Path(input_dir).resolve(strict=False)
    runs_path = Path(runs_dir).resolve(strict=False)
    if runs_path == input_path or runs_path.is_relative_to(input_path):
        raise ValueError("runs_dir 不能位于 input_dir 内部。")


def _build_batches(
    *,
    run_id: str,
    records: Sequence[SourceFileRecord],
    estimated_process_count: int,
    batch_size: int,
) -> list[BatchPlan]:
    batch_counts = _batch_counts(estimated_process_count, batch_size)
    batches: list[BatchPlan] = []
    offset = 0
    for batch_index, file_count in enumerate(batch_counts, start=1):
        batch_records = records[offset : offset + file_count]
        batches.append(
            BatchPlan(
                run_id=run_id,
                batch_index=batch_index,
                batch_size=batch_size,
                file_count=file_count,
                source_paths=[record.source_path for record in batch_records],
            ),
        )
        offset += file_count
    return batches


def _batch_counts(total_count: int, batch_size: int) -> list[int]:
    counts: list[int] = []
    remaining = total_count
    while remaining > 0:
        count = min(batch_size, remaining)
        counts.append(count)
        remaining -= count
    return counts


def _summary_value(
    summary: Mapping[str, Any] | object | None,
    key: str,
    default: object,
) -> object:
    if summary is None:
        return default
    if isinstance(summary, Mapping):
        return summary.get(key, default)
    return getattr(summary, key, default)


def _int_summary_value(
    summary: Mapping[str, Any] | object | None,
    key: str,
    default: int,
) -> int:
    return int(cast(Any, _summary_value(summary, key, default)))


def _default_run_id() -> str:
    return f"plan-{_now_utc().strftime('%Y%m%dT%H%M%S%fZ')}"


def _now_utc() -> datetime:
    return datetime.now(UTC)
