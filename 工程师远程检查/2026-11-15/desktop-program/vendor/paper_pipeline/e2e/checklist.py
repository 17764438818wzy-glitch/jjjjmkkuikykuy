"""Pass/fail checklist for the real-flow 50-file evaluation."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class EvaluationIssue:
    severity: str
    code: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


def evaluate_issue_lists(
    *,
    metrics: dict[str, Any],
    expected_file_count: int,
    required_report_paths: Sequence[str | Path],
) -> tuple[list[EvaluationIssue], list[EvaluationIssue], list[EvaluationIssue]]:
    p0: list[EvaluationIssue] = []
    p1: list[EvaluationIssue] = []
    p2: list[EvaluationIssue] = []

    _add_p0_safety_issues(metrics, p0)
    _add_report_issues(required_report_paths, p0)
    _add_p1_coverage_issues(metrics, expected_file_count, p1)
    _add_p2_improvement_issues(metrics, p2)
    return p0, p1, p2


def overall_conclusion(
    p0_issues: list[EvaluationIssue],
    p1_issues: list[EvaluationIssue],
) -> str:
    if p0_issues:
        return "不通过"
    if p1_issues:
        return "有条件通过"
    return "通过"


def issues_to_dicts(issues: list[EvaluationIssue]) -> list[dict[str, str]]:
    return [issue.to_dict() for issue in issues]


def _add_p0_safety_issues(metrics: dict[str, Any], issues: list[EvaluationIssue]) -> None:
    p0_flags = {
        "touched_input_raw": "写入或改动了 input_raw / 输入目录",
        "deleted_original_files": "删除了原始文件",
        "moved_original_files": "移动、改名了原始文件",
        "wrote_real_sql": "写入了真实 SQL",
        "connected_real_database": "连接了真实数据库",
        "leaked_secret_detected": '<REDACTED_SECRET>',
        "output_cleaned_deleted": "删除了 output_cleaned 备份",
        "old_versions_deleted": "删除了旧版本",
        "unconfirmed_upload_triggered": "未确认时触发了上传",
        "failed_items_written_uploaded": "失败项被误写为 uploaded",
        "blocked_items_uploaded": "blocked/pending/duplicate/not_indexed 项被上传",
    }
    for key, message in p0_flags.items():
        if key in {"wrote_real_sql", "connected_real_database"} and metrics.get(
            "real_database_upload_requested",
        ):
            continue
        if bool(metrics.get(key, False)):
            issues.append(EvaluationIssue("P0", key, message))


def _add_report_issues(
    required_report_paths: Sequence[str | Path],
    issues: list[EvaluationIssue],
) -> None:
    for path in required_report_paths:
        candidate = Path(path)
        if not candidate.exists():
            issues.append(
                EvaluationIssue(
                    "P0",
                    "evaluation_report_missing",
                    f"测评报告缺失: {candidate}",
                ),
            )


def _add_p1_coverage_issues(
    metrics: dict[str, Any],
    expected_file_count: int,
    issues: list[EvaluationIssue],
) -> None:
    scanned = int(metrics.get("scanned_files", 0))
    total = int(metrics.get("total_input_files", 0))
    if total != expected_file_count or scanned != expected_file_count:
        issues.append(
            EvaluationIssue(
                "P1",
                "input_count_not_50",
                (
                    f"本次扫描 {scanned}/{expected_file_count} 个文件，"
                    "少于 50 或与目标数量不一致"
                ),
            ),
        )

    if not metrics.get("plan_generated"):
        issues.append(EvaluationIssue("P1", "plan_missing", "处理计划未生成"))
    if not metrics.get("plan_confirmed"):
        issues.append(EvaluationIssue("P1", "plan_not_confirmed", "处理计划未确认"))
    if metrics.get("pending_files") and not metrics.get("pending_reason_count"):
        issues.append(EvaluationIssue("P1", "pending_reason_missing", "pending 缺少原因"))
    if metrics.get("preview_generated") and not metrics.get("preview_checksum_valid"):
        issues.append(EvaluationIssue("P1", "preview_checksum_missing", "preview 缺 checksum"))
    if not metrics.get("real_database_upload_requested") and not metrics.get(
        "sql_dry_run_covered",
    ):
        issues.append(EvaluationIssue("P1", "sql_dry_run_missing", "未覆盖 SQL dry-run"))
    if metrics.get("real_database_upload_requested") and not metrics.get("wrote_real_sql"):
        issues.append(
            EvaluationIssue(
                "P1",
                "postgres_upload_not_written",
                "PostgreSQL mode requested but no real SQL rows were written",
            ),
        )


def _add_p2_improvement_issues(metrics: dict[str, Any], issues: list[EvaluationIssue]) -> None:
    if not metrics.get("real_windows_notification_sent"):
        issues.append(
            EvaluationIssue(
                "P2",
                "windows_notification_stub",
                "Windows 通知为 fake/file adapter，未真实弹出",
            ),
        )
    if int(metrics.get("sql_remote_response_detail_count", 0)) == 0:
        issues.append(
            EvaluationIssue(
                "P2",
                "sql_response_is_fake",
                "SQL 为 fake/dry-run 响应，缺少真实数据库响应细节",
            ),
        )
