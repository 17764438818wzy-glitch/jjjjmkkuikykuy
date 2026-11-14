"""Apply withdraw lifecycle actions to local JSON files."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from paper_pipeline.sql.withdraw_lifecycle import (
    WithdrawLifecycleAction,
    WithdrawLifecyclePlan,
)

MANIFEST_FILENAME = "manifest.json"
UPLOAD_LOG_FILENAME = "upload_log.json"
UPLOAD_LOG_SCHEMA_VERSION = 1
UPLOAD_LOG_TYPE = "package_upload_log"

_ALLOWED_METADATA_FIELDS = {
    "sql_status",
    "sql_upload_record_id",
    "sql_batch_id",
    "sql_audit_history",
    "sql_withdrawn_at",
    "sql_withdrawn_by",
    "sql_withdraw_reason",
    "sql_withdraw_batch_id",
}
_ALLOWED_MANIFEST_FIELDS = {
    "resource_status",
    "withdrawn_at",
    "withdraw_reason",
    "withdraw_operator",
    "withdraw_batch_id",
    "published_to_postgres",
    "remote_resource_id",
    "upload_batch_id",
}


@dataclass(frozen=True)
class WithdrawLocalWritebackResult:
    success: bool
    metadata_path: Path
    package_dir: Path
    manifest_path: Path
    upload_log_path: Path
    metadata_updated: bool = False
    manifest_updated: bool = False
    upload_log_updated: bool = False
    applied_actions: tuple[str, ...] = ()
    skipped_actions: tuple[str, ...] = ()
    error: str | None = None
    warnings: tuple[str, ...] = ()


def apply_local_withdraw_writeback(
    metadata_path: Path | str,
    package_dir: Path | str,
    withdraw_plan: WithdrawLifecyclePlan,
) -> WithdrawLocalWritebackResult:
    path = Path(metadata_path)
    package_path = Path(package_dir)
    manifest_path = package_path / MANIFEST_FILENAME
    upload_log_path = package_path / UPLOAD_LOG_FILENAME
    warnings = tuple(withdraw_plan.warnings)

    if not withdraw_plan.allowed:
        return _result(
            path,
            package_path,
            manifest_path,
            upload_log_path,
            success=False,
            error="withdraw_plan_not_allowed",
            warnings=warnings,
        )

    if withdraw_plan.outcome == "noop":
        return _result(
            path,
            package_path,
            manifest_path,
            upload_log_path,
            success=True,
            warnings=warnings,
        )

    actions = tuple(withdraw_plan.actions)
    skipped_actions = tuple(action.target for action in actions if action.target == "postgres_resource")

    action_error = _unsupported_action_error(actions)
    if action_error is not None:
        return _result(
            path,
            package_path,
            manifest_path,
            upload_log_path,
            success=False,
            skipped_actions=skipped_actions,
            error=action_error,
            warnings=warnings,
        )

    metadata_actions = tuple(action for action in actions if action.target == "metadata")
    manifest_actions = tuple(action for action in actions if action.target == "package_manifest")
    upload_log_actions = tuple(action for action in actions if action.target == "upload_log")

    field_error = _forbidden_metadata_field_error(metadata_actions)
    if field_error is None:
        field_error = _forbidden_manifest_field_error(manifest_actions)
    if field_error is not None:
        return _result(
            path,
            package_path,
            manifest_path,
            upload_log_path,
            success=False,
            skipped_actions=skipped_actions,
            error=field_error,
            warnings=warnings,
        )

    if not metadata_actions and not manifest_actions and not upload_log_actions:
        return _result(
            path,
            package_path,
            manifest_path,
            upload_log_path,
            success=True,
            skipped_actions=skipped_actions,
            warnings=warnings,
        )

    try:
        metadata = _read_json_object(path, "metadata")
    except FileNotFoundError:
        return _result(
            path,
            package_path,
            manifest_path,
            upload_log_path,
            success=False,
            skipped_actions=skipped_actions,
            error="metadata_missing",
            warnings=warnings,
        )
    except (OSError, TypeError, ValueError) as exc:
        return _result(
            path,
            package_path,
            manifest_path,
            upload_log_path,
            success=False,
            skipped_actions=skipped_actions,
            error=f"metadata_read_failed:{exc}",
            warnings=warnings,
        )

    try:
        manifest = _read_json_object(manifest_path, "manifest")
    except FileNotFoundError:
        return _result(
            path,
            package_path,
            manifest_path,
            upload_log_path,
            success=False,
            skipped_actions=skipped_actions,
            error="manifest_missing",
            warnings=warnings,
        )
    except (OSError, TypeError, ValueError) as exc:
        return _result(
            path,
            package_path,
            manifest_path,
            upload_log_path,
            success=False,
            skipped_actions=skipped_actions,
            error=f"manifest_read_failed:{exc}",
            warnings=warnings,
        )

    try:
        upload_log_payload = _upload_log_payload(upload_log_path, upload_log_actions)
    except (OSError, TypeError, ValueError) as exc:
        return _result(
            path,
            package_path,
            manifest_path,
            upload_log_path,
            success=False,
            skipped_actions=skipped_actions,
            error=f"upload_log_read_failed:{exc}",
            warnings=warnings,
        )

    updated_manifest = _apply_merge_actions(manifest, manifest_actions)
    updated_metadata = _apply_merge_actions(metadata, metadata_actions)

    applied_actions: list[str] = []
    upload_log_updated = False
    manifest_updated = False
    metadata_updated = False

    if upload_log_payload is not None:
        try:
            _atomic_write_json(upload_log_path, upload_log_payload)
        except OSError as exc:
            return _result(
                path,
                package_path,
                manifest_path,
                upload_log_path,
                success=False,
                skipped_actions=skipped_actions,
                error=f"upload_log_write_failed:{exc}",
                warnings=warnings,
            )
        upload_log_updated = True
        applied_actions.append("upload_log")

    if updated_manifest is not None:
        try:
            _atomic_write_json(manifest_path, updated_manifest)
        except OSError as exc:
            return _partial_result(
                path,
                package_path,
                manifest_path,
                upload_log_path,
                skipped_actions=skipped_actions,
                applied_actions=tuple(applied_actions),
                upload_log_updated=upload_log_updated,
                manifest_updated=manifest_updated,
                metadata_updated=metadata_updated,
                error=f"manifest_write_failed:{exc}",
                warnings=warnings,
            )
        manifest_updated = True
        applied_actions.append("package_manifest")

    if updated_metadata is not None:
        try:
            _atomic_write_json(path, updated_metadata)
        except OSError as exc:
            return _partial_result(
                path,
                package_path,
                manifest_path,
                upload_log_path,
                skipped_actions=skipped_actions,
                applied_actions=tuple(applied_actions),
                upload_log_updated=upload_log_updated,
                manifest_updated=manifest_updated,
                metadata_updated=metadata_updated,
                error=f"metadata_write_failed:{exc}",
                warnings=warnings,
            )
        metadata_updated = True
        applied_actions.append("metadata")

    return _result(
        path,
        package_path,
        manifest_path,
        upload_log_path,
        success=True,
        metadata_updated=metadata_updated,
        manifest_updated=manifest_updated,
        upload_log_updated=upload_log_updated,
        applied_actions=tuple(applied_actions),
        skipped_actions=skipped_actions,
        warnings=warnings,
    )


def _unsupported_action_error(actions: tuple[WithdrawLifecycleAction, ...]) -> str | None:
    for action in actions:
        if action.target == "postgres_resource":
            if action.operation != "update_fields":
                return "unsupported_withdraw_writeback_operation:postgres_resource"
            continue
        if action.target == "metadata":
            if action.operation != "merge_fields":
                return "unsupported_withdraw_writeback_operation:metadata"
            continue
        if action.target == "package_manifest":
            if action.operation != "merge_fields":
                return "unsupported_withdraw_writeback_operation:package_manifest"
            continue
        if action.target == "upload_log":
            if action.operation != "append_event":
                return "unsupported_withdraw_writeback_operation:upload_log"
            continue
        return f"unsupported_withdraw_writeback_target:{action.target}"
    return None


def _forbidden_metadata_field_error(
    actions: tuple[WithdrawLifecycleAction, ...],
) -> str | None:
    for action in actions:
        for field_name in action.fields:
            if field_name not in _ALLOWED_METADATA_FIELDS:
                return f"forbidden_withdraw_metadata_field:{field_name}"
    return None


def _forbidden_manifest_field_error(
    actions: tuple[WithdrawLifecycleAction, ...],
) -> str | None:
    for action in actions:
        for field_name, field_value in action.fields.items():
            if field_name not in _ALLOWED_MANIFEST_FIELDS:
                return f"forbidden_withdraw_manifest_field:{field_name}"
            if field_name == "published_to_postgres" and field_value is False:
                return "withdraw_manifest_cannot_unpublish"
    return None


def _apply_merge_actions(
    payload: Mapping[str, Any],
    actions: tuple[WithdrawLifecycleAction, ...],
) -> dict[str, Any] | None:
    if not actions:
        return None
    updated = dict(payload)
    for action in actions:
        updated.update(dict(action.fields))
    return updated


def _upload_log_payload(
    upload_log_path: Path,
    actions: tuple[WithdrawLifecycleAction, ...],
) -> dict[str, Any] | None:
    if not actions:
        return None

    if upload_log_path.exists():
        payload = _read_json_object(upload_log_path, "upload_log")
        raw_events = payload.get("events")
        if not isinstance(raw_events, list):
            raise ValueError("upload_log_events_not_list")
        events = list(raw_events)
        payload = dict(payload)
        payload["events"] = events
    else:
        payload = {
            "schema_version": UPLOAD_LOG_SCHEMA_VERSION,
            "log_type": UPLOAD_LOG_TYPE,
            "events": [],
        }
        events = payload["events"]

    for action in actions:
        events.append(dict(action.fields))
    return payload


def _read_json_object(path: Path, label: str) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{label}_not_object")
    return payload


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    tmp_path = path.with_name(f".{path.name}.tmp")
    encoded = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    tmp_path.write_text(f"{encoded}\n", encoding="utf-8")
    tmp_path.replace(path)


def _partial_result(
    metadata_path: Path,
    package_dir: Path,
    manifest_path: Path,
    upload_log_path: Path,
    *,
    skipped_actions: tuple[str, ...],
    applied_actions: tuple[str, ...],
    upload_log_updated: bool,
    manifest_updated: bool,
    metadata_updated: bool,
    error: str,
    warnings: tuple[str, ...],
) -> WithdrawLocalWritebackResult:
    if applied_actions:
        warnings = (*warnings, "partial_withdraw_local_writeback")
    return _result(
        metadata_path,
        package_dir,
        manifest_path,
        upload_log_path,
        success=False,
        metadata_updated=metadata_updated,
        manifest_updated=manifest_updated,
        upload_log_updated=upload_log_updated,
        applied_actions=applied_actions,
        skipped_actions=skipped_actions,
        error=error,
        warnings=warnings,
    )


def _result(
    metadata_path: Path,
    package_dir: Path,
    manifest_path: Path,
    upload_log_path: Path,
    *,
    success: bool,
    metadata_updated: bool = False,
    manifest_updated: bool = False,
    upload_log_updated: bool = False,
    applied_actions: tuple[str, ...] = (),
    skipped_actions: tuple[str, ...] = (),
    error: str | None = None,
    warnings: tuple[str, ...] = (),
) -> WithdrawLocalWritebackResult:
    return WithdrawLocalWritebackResult(
        success=success,
        metadata_path=metadata_path,
        package_dir=package_dir,
        manifest_path=manifest_path,
        upload_log_path=upload_log_path,
        metadata_updated=metadata_updated,
        manifest_updated=manifest_updated,
        upload_log_updated=upload_log_updated,
        applied_actions=applied_actions,
        skipped_actions=skipped_actions,
        error=error,
        warnings=warnings,
    )
