from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from paper_pipeline.indexing.package_validation import (
    PackageValidationIssue,
    validate_local_index_package,
)


@dataclass(frozen=True)
class PreuploadPackageGateResult:
    package_dir: Path
    allowed: bool
    paper_id: str | None
    version: int | None
    blocking_reasons: list[str]
    warnings: list[str]
    blocking_fields: list[str]
    suggested_next_action: str


def check_package_preupload_gate(
    package_dir: Path,
    *,
    current_schema_version: str = "1.0",
) -> PreuploadPackageGateResult:
    validation = validate_local_index_package(
        Path(package_dir),
        current_schema_version=current_schema_version,
    )
    blocking_reasons = [
        _namespaced_issue_code(issue)
        for issue in validation.issues
    ]
    warnings = [
        _namespaced_warning_code(warning)
        for warning in validation.warnings
    ]

    return PreuploadPackageGateResult(
        package_dir=validation.package_dir,
        allowed=validation.valid,
        paper_id=validation.paper_id,
        version=validation.version,
        blocking_reasons=blocking_reasons,
        warnings=warnings,
        blocking_fields=_blocking_fields(validation.issues),
        suggested_next_action=(
            "ready_for_sql_preview"
            if validation.valid
            else "rebuild_local_index_package"
        ),
    )


def check_package_preupload_gates(
    package_dirs: Iterable[Path],
    *,
    current_schema_version: str = "1.0",
) -> list[PreuploadPackageGateResult]:
    return [
        check_package_preupload_gate(
            package_dir,
            current_schema_version=current_schema_version,
        )
        for package_dir in package_dirs
    ]


def _namespaced_issue_code(issue: PackageValidationIssue) -> str:
    reason = f"package_validation:{issue.code}"
    if issue.code != "duplicate_upload_blocked":
        return reason

    guard_reason = _duplicate_guard_reason(issue.message)
    if guard_reason is None:
        return reason
    return f"{reason}:{guard_reason}"


def _namespaced_warning_code(warning: PackageValidationIssue) -> str:
    return f"package_validation_warning:{warning.code}"


def _duplicate_guard_reason(message: str) -> str | None:
    marker = "reasons="
    marker_index = message.find(marker)
    if marker_index < 0:
        return None

    reason_text = message[marker_index + len(marker):].split(";", 1)[0]
    primary_reason = reason_text.split(",", 1)[0].strip()
    if not primary_reason or primary_reason == "none":
        return None
    return primary_reason


def _blocking_fields(issues: list[PackageValidationIssue]) -> list[str]:
    fields: list[str] = []
    seen: set[str] = set()
    has_published_manifest_issue = any(
        issue.code == "already_published_to_postgres"
        for issue in issues
    )
    for issue in issues:
        if (
            has_published_manifest_issue
            and issue.code == "duplicate_upload_blocked"
            and issue.field == "duplicate_guard"
        ):
            continue
        field = issue.field or issue.file or "package_dir"
        if field in seen:
            continue
        seen.add(field)
        fields.append(field)
    return fields
