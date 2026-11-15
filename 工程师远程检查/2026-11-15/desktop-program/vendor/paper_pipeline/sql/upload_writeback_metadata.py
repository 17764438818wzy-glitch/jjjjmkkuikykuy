"""Apply upload writeback actions to a local metadata file."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from paper_pipeline.sql.upload_writeback import UploadWritebackAction, UploadWritebackPlan

_DEFAULT_SQL_STATUS = "not_uploaded"
_TERMINAL_SQL_STATUSES = {"withdrawn", "replaced"}
_ALLOWED_METADATA_FIELDS = {
    "sql_status",
    "sql_batch_id",
    "sql_upload_record_id",
    "sql_upload_error",
    "sql_uploaded_at",
    "sql_failed_at",
    "sql_confirmation_id",
    "sql_field_snapshot_hash",
    "sql_preview_id",
    "sql_uploaded_by",
    "sql_upload_adapter",
    "sql_audit_history",
}


@dataclass(frozen=True)
class MetadataUploadWritebackResult:
    success: bool
    metadata_path: Path
    metadata_updated: bool = False
    applied_actions: tuple[str, ...] = ()
    skipped_actions: tuple[str, ...] = ()
    error: str | None = None
    warnings: tuple[str, ...] = ()


def apply_metadata_upload_writeback(
    metadata_path: Path | str,
    writeback_plan: UploadWritebackPlan,
) -> MetadataUploadWritebackResult:
    path = Path(metadata_path)

    if not writeback_plan.allowed:
        return _result(
            path,
            success=False,
            error="writeback_plan_not_allowed",
            warnings=writeback_plan.warnings,
        )

    if writeback_plan.outcome == "noop":
        return _result(path, success=True, warnings=writeback_plan.warnings)

    action_error = _unsupported_action_error(writeback_plan.actions)
    if action_error is not None:
        return _result(
            path,
            success=False,
            error=action_error,
            warnings=writeback_plan.warnings,
        )

    metadata_actions = tuple(
        action for action in writeback_plan.actions if action.target == "metadata"
    )
    skipped_actions = tuple(
        action.target
        for action in writeback_plan.actions
        if action.target in {"package_manifest", "upload_log"}
    )

    field_error = _forbidden_field_error(metadata_actions)
    if field_error is not None:
        return _result(
            path,
            success=False,
            skipped_actions=skipped_actions,
            error=field_error,
            warnings=writeback_plan.warnings,
        )

    try:
        metadata = _read_json_object(path)
    except FileNotFoundError:
        return _result(
            path,
            success=False,
            skipped_actions=skipped_actions,
            error="metadata_missing",
            warnings=writeback_plan.warnings,
        )
    except (OSError, TypeError, ValueError) as exc:
        return _result(
            path,
            success=False,
            skipped_actions=skipped_actions,
            error=f"metadata_read_failed:{exc}",
            warnings=writeback_plan.warnings,
        )

    merged_fields = _merged_metadata_fields(metadata_actions)
    transition_error = _transition_error(
        current_status=str(metadata.get("sql_status", _DEFAULT_SQL_STATUS) or _DEFAULT_SQL_STATUS),
        next_status=merged_fields.get("sql_status"),
    )
    if transition_error is not None:
        return _result(
            path,
            success=False,
            skipped_actions=skipped_actions,
            error=transition_error,
            warnings=writeback_plan.warnings,
        )

    if not merged_fields:
        return _result(
            path,
            success=True,
            skipped_actions=skipped_actions,
            warnings=writeback_plan.warnings,
        )

    updated = dict(metadata)
    updated.update(merged_fields)

    try:
        _atomic_write_json(path, updated)
    except OSError as exc:
        return _result(
            path,
            success=False,
            skipped_actions=skipped_actions,
            error=f"metadata_write_failed:{exc}",
            warnings=writeback_plan.warnings,
        )

    return _result(
        path,
        success=True,
        metadata_updated=True,
        applied_actions=("metadata",),
        skipped_actions=skipped_actions,
        warnings=writeback_plan.warnings,
    )


def _unsupported_action_error(actions: tuple[UploadWritebackAction, ...]) -> str | None:
    for action in actions:
        if action.target in {"package_manifest", "upload_log"}:
            continue
        if action.target == "metadata":
            if action.operation != "merge_fields":
                return "unsupported_writeback_operation:metadata"
            continue
        return f"unsupported_writeback_target:{action.target}"
    return None


def _forbidden_field_error(
    actions: tuple[UploadWritebackAction, ...],
) -> str | None:
    for action in actions:
        for field_name in action.fields:
            if field_name not in _ALLOWED_METADATA_FIELDS:
                return f"forbidden_metadata_writeback_field:{field_name}"
    return None


def _merged_metadata_fields(
    actions: tuple[UploadWritebackAction, ...],
) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for action in actions:
        merged.update(dict(action.fields))
    return merged


def _transition_error(current_status: str, next_status: Any) -> str | None:
    if next_status is None:
        return None

    next_status_value = str(next_status)
    if current_status == "uploaded" and next_status_value == "failed":
        return "uploaded_to_failed_forbidden"
    if current_status in _TERMINAL_SQL_STATUSES and next_status_value == "uploaded":
        return "terminal_sql_status_writeback_forbidden"
    return None


def _read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("metadata_not_object")
    return payload


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    tmp_path = path.with_name(f".{path.name}.tmp")
    encoded = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    tmp_path.write_text(f"{encoded}\n", encoding="utf-8")
    tmp_path.replace(path)


def _result(
    metadata_path: Path,
    *,
    success: bool,
    metadata_updated: bool = False,
    applied_actions: tuple[str, ...] = (),
    skipped_actions: tuple[str, ...] = (),
    error: str | None = None,
    warnings: tuple[str, ...] = (),
) -> MetadataUploadWritebackResult:
    return MetadataUploadWritebackResult(
        success=success,
        metadata_path=metadata_path,
        metadata_updated=metadata_updated,
        applied_actions=applied_actions,
        skipped_actions=skipped_actions,
        error=error,
        warnings=warnings,
    )
