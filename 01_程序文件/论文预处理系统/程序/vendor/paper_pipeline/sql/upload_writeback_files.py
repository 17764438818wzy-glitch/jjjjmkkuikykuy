"""Apply package-scoped upload writeback actions to local JSON files."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from paper_pipeline.sql.upload_writeback import UploadWritebackAction, UploadWritebackPlan

MANIFEST_FILENAME = "manifest.json"
UPLOAD_LOG_FILENAME = "upload_log.json"
UPLOAD_LOG_SCHEMA_VERSION = 1
UPLOAD_LOG_TYPE = "package_upload_log"


@dataclass(frozen=True)
class PackageUploadWritebackResult:
    success: bool
    package_dir: Path
    manifest_path: Path
    upload_log_path: Path
    manifest_updated: bool = False
    upload_log_updated: bool = False
    applied_actions: tuple[str, ...] = ()
    skipped_actions: tuple[str, ...] = ()
    error: str | None = None
    warnings: tuple[str, ...] = ()


def apply_package_upload_writeback(
    package_dir: Path | str,
    writeback_plan: UploadWritebackPlan,
) -> PackageUploadWritebackResult:
    package_path = Path(package_dir)
    manifest_path = package_path / MANIFEST_FILENAME
    upload_log_path = package_path / UPLOAD_LOG_FILENAME

    if not writeback_plan.allowed:
        return _result(
            package_path,
            manifest_path,
            upload_log_path,
            success=False,
            error="writeback_plan_not_allowed",
            warnings=writeback_plan.warnings,
        )

    if writeback_plan.outcome == "noop":
        return _result(
            package_path,
            manifest_path,
            upload_log_path,
            success=True,
            warnings=writeback_plan.warnings,
        )

    action_error = _unsupported_action_error(writeback_plan.actions)
    if action_error is not None:
        return _result(
            package_path,
            manifest_path,
            upload_log_path,
            success=False,
            error=action_error,
            warnings=writeback_plan.warnings,
        )

    skipped_actions = tuple(
        action.target for action in writeback_plan.actions if action.target == "metadata"
    )
    package_manifest_actions = tuple(
        action for action in writeback_plan.actions if action.target == "package_manifest"
    )
    upload_log_actions = tuple(
        action for action in writeback_plan.actions if action.target == "upload_log"
    )

    try:
        manifest = _read_json_object(manifest_path, "manifest")
    except FileNotFoundError:
        return _result(
            package_path,
            manifest_path,
            upload_log_path,
            success=False,
            skipped_actions=skipped_actions,
            error="manifest_missing",
            warnings=writeback_plan.warnings,
        )
    except (OSError, TypeError, ValueError) as exc:
        return _result(
            package_path,
            manifest_path,
            upload_log_path,
            success=False,
            skipped_actions=skipped_actions,
            error=f"manifest_read_failed:{exc}",
            warnings=writeback_plan.warnings,
        )

    updated_manifest = _apply_manifest_actions(manifest, package_manifest_actions)
    upload_log_payload = _upload_log_payload(upload_log_path, upload_log_actions)

    applied_actions: list[str] = []
    upload_log_updated = False
    manifest_updated = False

    if upload_log_payload is not None:
        try:
            _atomic_write_json(upload_log_path, upload_log_payload)
        except OSError as exc:
            return _result(
                package_path,
                manifest_path,
                upload_log_path,
                success=False,
                skipped_actions=skipped_actions,
                error=f"upload_log_write_failed:{exc}",
                warnings=writeback_plan.warnings,
            )
        upload_log_updated = True
        applied_actions.append("upload_log")

    if updated_manifest is not None:
        try:
            _atomic_write_json(manifest_path, updated_manifest)
        except OSError as exc:
            warnings = writeback_plan.warnings
            if upload_log_updated:
                warnings = (*warnings, "partial_local_writeback")
            return _result(
                package_path,
                manifest_path,
                upload_log_path,
                success=False,
                upload_log_updated=upload_log_updated,
                applied_actions=tuple(applied_actions),
                skipped_actions=skipped_actions,
                error=f"manifest_write_failed:{exc}",
                warnings=warnings,
            )
        manifest_updated = True
        applied_actions.append("package_manifest")

    return _result(
        package_path,
        manifest_path,
        upload_log_path,
        success=True,
        manifest_updated=manifest_updated,
        upload_log_updated=upload_log_updated,
        applied_actions=tuple(applied_actions),
        skipped_actions=skipped_actions,
        warnings=writeback_plan.warnings,
    )


def _unsupported_action_error(actions: tuple[UploadWritebackAction, ...]) -> str | None:
    for action in actions:
        if action.target == "metadata":
            continue
        if action.target == "package_manifest":
            if action.operation != "merge_fields":
                return "unsupported_writeback_operation:package_manifest"
            continue
        if action.target == "upload_log":
            if action.operation != "append_event":
                return "unsupported_writeback_operation:upload_log"
            continue
        return f"unsupported_writeback_target:{action.target}"
    return None


def _apply_manifest_actions(
    manifest: Mapping[str, Any],
    actions: tuple[UploadWritebackAction, ...],
) -> dict[str, Any] | None:
    if not actions:
        return None
    updated = dict(manifest)
    for action in actions:
        updated.update(dict(action.fields))
    return updated


def _upload_log_payload(
    upload_log_path: Path,
    actions: tuple[UploadWritebackAction, ...],
) -> dict[str, Any] | None:
    if not actions:
        return None

    if upload_log_path.exists():
        payload = _read_json_object(upload_log_path, "upload_log")
        raw_events = payload.get("events")
        events = list(raw_events) if isinstance(raw_events, list) else []
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


def _result(
    package_dir: Path,
    manifest_path: Path,
    upload_log_path: Path,
    *,
    success: bool,
    manifest_updated: bool = False,
    upload_log_updated: bool = False,
    applied_actions: tuple[str, ...] = (),
    skipped_actions: tuple[str, ...] = (),
    error: str | None = None,
    warnings: tuple[str, ...] = (),
) -> PackageUploadWritebackResult:
    return PackageUploadWritebackResult(
        success=success,
        package_dir=package_dir,
        manifest_path=manifest_path,
        upload_log_path=upload_log_path,
        manifest_updated=manifest_updated,
        upload_log_updated=upload_log_updated,
        applied_actions=applied_actions,
        skipped_actions=skipped_actions,
        error=error,
        warnings=warnings,
    )
