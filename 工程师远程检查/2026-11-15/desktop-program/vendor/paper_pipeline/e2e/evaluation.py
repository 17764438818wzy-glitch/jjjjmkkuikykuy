"""Evaluation report writers for task 16."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from paper_pipeline.e2e.checklist import EvaluationIssue, issues_to_dicts


@dataclass(frozen=True, slots=True)
class EvaluationOutputPaths:
    evaluation_dir: Path
    evaluation_report_md_path: Path
    evaluation_report_json_path: Path
    e2e_run_manifest_path: Path


def default_metrics() -> dict[str, Any]:
    return {
        "total_input_files": 0,
        "scanned_files": 0,
        "unreadable_files": 0,
        "exact_duplicate_files": 0,
        "suspected_duplicate_files": 0,
        "unique_files": 0,
        "plan_generated": False,
        "plan_confirmed": False,
        "batch_count": 0,
        "batch_sizes": [],
        "run_status": "",
        "completed_files": 0,
        "pending_files": 0,
        "failed_files": 0,
        "interrupted_files": 0,
        "stage0_accept_count": 0,
        "stage0_reject_count": 0,
        "stage0_pending_count": 0,
        "stage0_rejected_audit_count": 0,
        "stage0_false_reject_suspected_count": 0,
        "pipeline_success_count": 0,
        "pipeline_pending_count": 0,
        "pipeline_failed_count": 0,
        "stage2_document_parse_success_count": 0,
        "stage2_document_parse_failed_count": 0,
        "markdown_generated_count": 0,
        "final_md_missing_count": 0,
        "category_success_count": 0,
        "direction_success_count": 0,
        "research_method_success_count": 0,
        "quality_tier_suggested_count": 0,
        "needs_review_count": 0,
        "risk_flags_count": 0,
        "library_gold_count": 0,
        "library_silver_count": 0,
        "library_pending_count": 0,
        "library_rejected_count": 0,
        "output_cleaned_bundle_count": 0,
        "metadata_written_count": 0,
        "metadata_validation_failed_count": 0,
        "exact_duplicate_skipped_count": 0,
        "suspected_duplicate_pending_count": 0,
        "pending_review_suggested_count": 0,
        "pending_review_applied_count": 0,
        "review_history_count": 0,
        "indexed_count": 0,
        "index_failed_count": 0,
        "needs_rebuild_count": 0,
        "index_skipped_count": 0,
        "bm25_index_entries": 0,
        "vector_stub_entries": 0,
        "sql_eligible_count": 0,
        "sql_blocked_count": 0,
        "preview_generated": False,
        "preview_checksum_valid": False,
        "confirmation_required": True,
        "second_confirmation_performed": False,
        "unconfirmed_upload_blocked": False,
        "real_database_upload_requested": False,
        "sql_dry_run_covered": False,
        "sql_upload_mode": "fake/dry_run",
        "sql_upload_success_count": 0,
        "sql_upload_failed_count": 0,
        "sql_status_uploaded_count": 0,
        "sql_status_failed_count": 0,
        "sql_status_not_uploaded_count": 0,
        "touched_input_raw": False,
        "deleted_original_files": False,
        "moved_original_files": False,
        "wrote_real_sql": False,
        "connected_real_database": False,
        "leaked_secret_detected": False,
        "output_cleaned_deleted": False,
        "old_versions_deleted": False,
        "unconfirmed_upload_triggered": False,
        "failed_items_written_uploaded": False,
        "blocked_items_uploaded": False,
        "pending_reason_count": 0,
        "real_windows_notification_sent": False,
        "sql_remote_response_detail_count": 0,
    }


def write_evaluation_outputs(
    *,
    run_id: str,
    library_dir: str | Path,
    input_dir: str | Path,
    output_cleaned_dir: str | Path,
    metrics: dict[str, Any],
    p0_issues: list[EvaluationIssue],
    p1_issues: list[EvaluationIssue],
    p2_issues: list[EvaluationIssue],
    overall_conclusion: str,
    manifest: dict[str, Any],
) -> EvaluationOutputPaths:
    evaluation_dir = Path(library_dir) / "runs" / run_id / "evaluation"
    paths = EvaluationOutputPaths(
        evaluation_dir=evaluation_dir,
        evaluation_report_md_path=evaluation_dir / "evaluation_report.md",
        evaluation_report_json_path=evaluation_dir / "evaluation_report.json",
        e2e_run_manifest_path=evaluation_dir / "e2e_run_manifest.json",
    )
    evaluation_dir.mkdir(parents=True, exist_ok=True)

    report = {
        "run_id": run_id,
        "input_dir": str(input_dir),
        "library_dir": str(library_dir),
        "output_cleaned_dir": str(output_cleaned_dir),
        "overall_conclusion": overall_conclusion,
        "metrics": metrics,
        "p0_issues": issues_to_dicts(p0_issues),
        "p1_issues": issues_to_dicts(p1_issues),
        "p2_issues": issues_to_dicts(p2_issues),
    }
    paths.evaluation_report_json_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    paths.evaluation_report_md_path.write_text(
        _render_markdown_report(report),
        encoding="utf-8",
    )
    paths.e2e_run_manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return paths


def _render_markdown_report(report: dict[str, Any]) -> str:
    metrics = report["metrics"]
    lines = [
        "# Task 16 Evaluation Report",
        "",
        f"- run_id: {report['run_id']}",
        f"- conclusion: {report['overall_conclusion']}",
        f"- input files: {metrics['scanned_files']}/{metrics['total_input_files']}",
        f"- completed / pending / failed: {metrics['completed_files']} / "
        f"{metrics['pending_files']} / {metrics['failed_files']}",
        f"- stage0 accept / reject / pending: {metrics['stage0_accept_count']} / "
        f"{metrics['stage0_reject_count']} / {metrics['stage0_pending_count']}",
        f"- pipeline success: {metrics['pipeline_success_count']}",
        f"- indexed: {metrics['indexed_count']}",
        f"- SQL uploaded / failed / not_uploaded: "
        f"{metrics['sql_status_uploaded_count']} / "
        f"{metrics['sql_status_failed_count']} / "
        f"{metrics['sql_status_not_uploaded_count']}",
        "",
        "## P0 Issues",
        "",
        *_issue_lines(report["p0_issues"]),
        "",
        "## P1 Issues",
        "",
        *_issue_lines(report["p1_issues"]),
        "",
        "## P2 Improvements",
        "",
        *_issue_lines(report["p2_issues"]),
        "",
    ]
    return "\n".join(lines)


def _issue_lines(issues: list[dict[str, str]]) -> list[str]:
    if not issues:
        return ["- 无"]
    return [f"- {issue['code']}: {issue['message']}" for issue in issues]
