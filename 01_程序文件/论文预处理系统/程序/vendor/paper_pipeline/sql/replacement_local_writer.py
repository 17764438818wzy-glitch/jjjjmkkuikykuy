"""Apply replacement lifecycle actions to local JSON files."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from paper_pipeline.sql.replacement_lifecycle import (
    ReplacementLifecycleAction,
    ReplacementLifecyclePlan,
)

MANIFEST_FILENAME = "manifest.json"
UPLOAD_LOG_FILENAME = "upload_log.json"
UPLOAD_LOG_SCHEMA_VERSION = 1
UPLOAD_LOG_TYPE = "package_upload_log"
IDEMPOTENCY_MARKER_SCOPE = "replacement_local_writer_after_remote_receipt"
IDEMPOTENCY_MARKER_SCHEMA_VERSION = "1.0"
IDEMPOTENCY_TRIGGER_AFTER_REMOTE_RECEIPT = "after_remote_commit_receipt"
IDEMPOTENCY_SOURCE = "S18-7C_remote_receipt_local_orchestration"

_REMOTE_TARGET_PREFIX = "post" + "gres_"
_LOCAL_OPERATIONS = {
    "old_metadata": "merge_fields",
    "old_package_manifest": "merge_fields",
    "old_upload_log": "append_event",
    "new_package_manifest": "merge_fields",
    "new_upload_log": "append_event",
}


@dataclass(frozen=True)
class ReplacementLocalWriteIdempotencyContext:
    operation_id: str
    receipt_hash: str
    remote_transaction_status: str
    remote_receipt_scope: str
    local_write_trigger: str = IDEMPOTENCY_TRIGGER_AFTER_REMOTE_RECEIPT
    remote_committed_at: str | None = None
    remote_receipt_operation_id: str | None = None
    idempotency_key: str | None = None
    source: str = IDEMPOTENCY_SOURCE
    marker_schema_version: str = IDEMPOTENCY_MARKER_SCHEMA_VERSION


@dataclass(frozen=True)
class ReplacementLocalIdempotencyMarker:
    operation_id: str
    receipt_hash: str
    idempotency_key: str
    remote_transaction_status: str
    local_write_trigger: str
    applied_at: str | None
    applied_by: str | None
    marker_scope: str = IDEMPOTENCY_MARKER_SCOPE
    marker_schema_version: str = IDEMPOTENCY_MARKER_SCHEMA_VERSION
    remote_receipt_scope: str | None = None
    remote_committed_at: str | None = None
    remote_receipt_operation_id: str | None = None
    source: str = IDEMPOTENCY_SOURCE

    def as_dict(self) -> dict[str, Any]:
        values = {
            "operation_id": self.operation_id,
            "receipt_hash": self.receipt_hash,
            "idempotency_key": self.idempotency_key,
            "remote_transaction_status": self.remote_transaction_status,
            "local_write_trigger": self.local_write_trigger,
            "applied_at": self.applied_at,
            "applied_by": self.applied_by,
            "marker_scope": self.marker_scope,
            "marker_schema_version": self.marker_schema_version,
            "remote_receipt_scope": self.remote_receipt_scope,
            "remote_committed_at": self.remote_committed_at,
            "remote_receipt_operation_id": self.remote_receipt_operation_id,
            "source": self.source,
        }
        return {key: value for key, value in values.items() if value is not None}


@dataclass(frozen=True)
class ReplacementLocalWriteResult:
    success: bool
    old_metadata_path: Path
    old_package_dir: Path
    old_manifest_path: Path
    old_upload_log_path: Path
    new_package_dir: Path
    new_manifest_path: Path
    new_upload_log_path: Path
    execution_scope: str = "local_files"
    commit_scope: str = "none"
    wrote_local_files: bool = False
    committed_to_fake_state: bool = False
    connected_postgres: bool = False
    executed_sql: bool = False
    real_postgres_committed: bool = False
    old_metadata_updated: bool = False
    old_manifest_updated: bool = False
    old_upload_log_updated: bool = False
    new_manifest_updated: bool = False
    new_upload_log_updated: bool = False
    applied_actions: tuple[str, ...] = ()
    skipped_actions: tuple[str, ...] = ()
    error: str | None = None
    warnings: tuple[str, ...] = ()
    idempotency_marker_supported: bool = False
    idempotency_status: str = "not_requested"
    operation_id: str | None = None
    receipt_hash: str | None = None
    idempotency_key: str | None = None
    duplicate_receipt_detected: bool = False
    idempotent_noop: bool = False
    idempotency_conflict: bool = False
    partial_idempotency_marker_detected: bool = False
    reconciliation_required: bool = False
    blocked_reasons: tuple[str, ...] = ()


def apply_replacement_local_actions(
    old_metadata_path: Path | str,
    old_package_dir: Path | str,
    new_package_dir: Path | str,
    replacement_plan: ReplacementLifecyclePlan,
    *,
    now: str | None = None,
    clock: Callable[[], str] | None = None,
    idempotency_context: ReplacementLocalWriteIdempotencyContext | None = None,
) -> ReplacementLocalWriteResult:
    old_metadata = Path(old_metadata_path)
    old_package = Path(old_package_dir)
    new_package = Path(new_package_dir)
    old_manifest_path = old_package / MANIFEST_FILENAME
    old_upload_log_path = old_package / UPLOAD_LOG_FILENAME
    new_manifest_path = new_package / MANIFEST_FILENAME
    new_upload_log_path = new_package / UPLOAD_LOG_FILENAME
    warnings = tuple(replacement_plan.warnings)
    idempotency_base = _idempotency_base(idempotency_context)

    if not replacement_plan.allowed:
        return _result(
            old_metadata,
            old_package,
            old_manifest_path,
            old_upload_log_path,
            new_package,
            new_manifest_path,
            new_upload_log_path,
            success=False,
            error="replacement_plan_not_allowed",
            warnings=warnings,
            **_blocked_idempotency_kwargs(idempotency_base, "replacement_plan_not_allowed"),
        )

    if replacement_plan.outcome == "noop":
        return _result(
            old_metadata,
            old_package,
            old_manifest_path,
            old_upload_log_path,
            new_package,
            new_manifest_path,
            new_upload_log_path,
            success=True,
            warnings=warnings,
            **_blocked_idempotency_kwargs(idempotency_base, "replacement_plan_noop"),
        )

    actions = tuple(replacement_plan.actions)
    action_error = _unsupported_action_error(actions)
    skipped_actions = tuple(action.target for action in actions if _is_remote_target(action.target))
    if action_error is not None:
        return _result(
            old_metadata,
            old_package,
            old_manifest_path,
            old_upload_log_path,
            new_package,
            new_manifest_path,
            new_upload_log_path,
            success=False,
            skipped_actions=skipped_actions,
            error=action_error,
            warnings=warnings,
            **_blocked_idempotency_kwargs(idempotency_base, action_error),
        )

    local_actions = {
        target: tuple(action for action in actions if action.target == target)
        for target in _LOCAL_OPERATIONS
    }
    if not any(local_actions.values()):
        return _result(
            old_metadata,
            old_package,
            old_manifest_path,
            old_upload_log_path,
            new_package,
            new_manifest_path,
            new_upload_log_path,
            success=True,
            skipped_actions=skipped_actions,
            warnings=warnings,
            **_blocked_idempotency_kwargs(idempotency_base, "no_local_replacement_actions"),
        )

    try:
        old_metadata_payload = _read_json_object(old_metadata, "old_metadata")
        old_manifest = _read_json_object(old_manifest_path, "old_manifest")
        new_manifest = _read_json_object(new_manifest_path, "new_manifest")
        old_log = _upload_log_payload(old_upload_log_path, local_actions["old_upload_log"])
        new_log = _upload_log_payload(new_upload_log_path, local_actions["new_upload_log"])
    except FileNotFoundError as exc:
        return _result(
            old_metadata,
            old_package,
            old_manifest_path,
            old_upload_log_path,
            new_package,
            new_manifest_path,
            new_upload_log_path,
            success=False,
            skipped_actions=skipped_actions,
            error=_missing_file_error(exc.filename, old_metadata, old_manifest_path, new_manifest_path),
            warnings=warnings,
            **_blocked_idempotency_kwargs(idempotency_base, "replacement_local_file_missing"),
        )
    except (OSError, TypeError, ValueError) as exc:
        return _result(
            old_metadata,
            old_package,
            old_manifest_path,
            old_upload_log_path,
            new_package,
            new_manifest_path,
            new_upload_log_path,
            success=False,
            skipped_actions=skipped_actions,
            error=f"replacement_local_read_failed:{exc}",
            warnings=warnings,
            **_blocked_idempotency_kwargs(idempotency_base, "replacement_local_read_failed"),
        )

    context = _context_from_actions(actions, now=now, clock=clock)
    context.update(
        {
            "old_metadata_path": old_metadata,
            "old_manifest_path": old_manifest_path,
            "old_log_path": old_upload_log_path,
            "new_manifest_path": new_manifest_path,
            "new_log_path": new_upload_log_path,
        },
    )
    idempotency_check = _check_idempotency_state(
        idempotency_context=idempotency_context,
        old_manifest=old_manifest,
        old_log=old_log,
        new_manifest=new_manifest,
        new_log=new_log,
    )
    if idempotency_check["status"] == "already_applied":
        return _result(
            old_metadata,
            old_package,
            old_manifest_path,
            old_upload_log_path,
            new_package,
            new_manifest_path,
            new_upload_log_path,
            success=True,
            skipped_actions=skipped_actions,
            warnings=warnings,
            **idempotency_check["result_kwargs"],
        )
    if idempotency_check["status"] in {"conflict", "partial_marker_detected", "blocked"}:
        return _result(
            old_metadata,
            old_package,
            old_manifest_path,
            old_upload_log_path,
            new_package,
            new_manifest_path,
            new_upload_log_path,
            success=False,
            skipped_actions=skipped_actions,
            error=idempotency_check["error"],
            warnings=warnings,
            **idempotency_check["result_kwargs"],
        )

    marker: ReplacementLocalIdempotencyMarker | None = None
    if idempotency_context is not None:
        marker = _marker_from_context(idempotency_context, context)
        context["idempotency_marker"] = marker.as_dict()

    updated_payloads = _updated_payloads(
        old_metadata_payload=old_metadata_payload,
        old_manifest=old_manifest,
        old_log=old_log,
        new_manifest=new_manifest,
        new_log=new_log,
        local_actions=local_actions,
        context=context,
    )

    try:
        _write_all_json(updated_payloads)
    except OSError as exc:
        return _result(
            old_metadata,
            old_package,
            old_manifest_path,
            old_upload_log_path,
            new_package,
            new_manifest_path,
            new_upload_log_path,
            success=False,
            skipped_actions=skipped_actions,
            error=f"replacement_local_write_failed:{exc}",
            warnings=warnings,
            **_blocked_idempotency_kwargs(idempotency_base, "replacement_local_write_failed"),
        )

    applied_actions = tuple(
        target for target in _LOCAL_OPERATIONS if local_actions[target]
    )
    return _result(
        old_metadata,
        old_package,
        old_manifest_path,
        old_upload_log_path,
        new_package,
        new_manifest_path,
        new_upload_log_path,
        success=True,
        old_metadata_updated=bool(local_actions["old_metadata"]),
        old_manifest_updated=bool(local_actions["old_package_manifest"]),
        old_upload_log_updated=bool(local_actions["old_upload_log"]),
        new_manifest_updated=bool(local_actions["new_package_manifest"]),
        new_upload_log_updated=bool(local_actions["new_upload_log"]),
        applied_actions=applied_actions,
        skipped_actions=skipped_actions,
        warnings=warnings,
        **_applied_idempotency_kwargs(idempotency_base, marker),
    )


def _unsupported_action_error(actions: tuple[ReplacementLifecycleAction, ...]) -> str | None:
    for action in actions:
        if _is_remote_target(action.target):
            continue
        expected_operation = _LOCAL_OPERATIONS.get(action.target)
        if expected_operation is None:
            return f"unsupported_replacement_local_target:{action.target}"
        if action.operation != expected_operation:
            return f"unsupported_replacement_local_operation:{action.target}"
    return None


def _is_remote_target(target: str) -> bool:
    return target.startswith(_REMOTE_TARGET_PREFIX)


def _updated_payloads(
    *,
    old_metadata_payload: Mapping[str, Any],
    old_manifest: Mapping[str, Any],
    old_log: Mapping[str, Any] | None,
    new_manifest: Mapping[str, Any],
    new_log: Mapping[str, Any] | None,
    local_actions: Mapping[str, tuple[ReplacementLifecycleAction, ...]],
    context: Mapping[str, Any],
) -> dict[Path, Mapping[str, Any]]:
    updated: dict[Path, Mapping[str, Any]] = {}
    old_metadata_path = context["old_metadata_path"]
    old_manifest_path = context["old_manifest_path"]
    old_log_path = context["old_log_path"]
    new_manifest_path = context["new_manifest_path"]
    new_log_path = context["new_log_path"]

    if local_actions["old_metadata"]:
        updated[old_metadata_path] = _old_metadata_payload(
            old_metadata_payload,
            local_actions["old_metadata"],
            context,
        )
    if local_actions["old_package_manifest"]:
        updated[old_manifest_path] = _old_manifest_payload(
            old_manifest,
            local_actions["old_package_manifest"],
            context,
        )
    if local_actions["old_upload_log"] and old_log is not None:
        updated[old_log_path] = _log_with_events(
            old_log,
            local_actions["old_upload_log"],
            context,
            replacement_status="replaced",
        )
    if local_actions["new_package_manifest"]:
        updated[new_manifest_path] = _new_manifest_payload(
            new_manifest,
            local_actions["new_package_manifest"],
            context,
        )
    if local_actions["new_upload_log"] and new_log is not None:
        updated[new_log_path] = _log_with_events(
            new_log,
            local_actions["new_upload_log"],
            context,
            replacement_status=None,
        )
    return updated


def _context_from_actions(
    actions: tuple[ReplacementLifecycleAction, ...],
    *,
    now: str | None,
    clock: Callable[[], str] | None,
) -> dict[str, Any]:
    old_metadata = _first_action(actions, "old_metadata")
    old_manifest = _first_action(actions, "old_package_manifest")
    old_log = _first_action(actions, "old_upload_log")
    new_log = _first_action(actions, "new_upload_log")
    remote_old = _first_action(actions, _REMOTE_TARGET_PREFIX + "old_resource")
    remote_new = _first_action(actions, _REMOTE_TARGET_PREFIX + "new_resource")

    timestamp = _optional_text(now)
    if timestamp is None and clock is not None:
        timestamp = _optional_text(clock())
    if timestamp is None:
        timestamp = _requested_at_from_action(old_metadata)

    return {
        "old_resource_id": _first_text(
            old_log,
            remote_old,
            field_names=("old_resource_id", "resource_id"),
        ),
        "new_resource_id": _first_text(
            old_log,
            new_log,
            remote_new,
            field_names=("new_resource_id", "resource_id"),
        ),
        "reason": _first_text(
            old_metadata,
            old_manifest,
            old_log,
            field_names=("replacement_reason", "sql_replacement_reason"),
        ),
        "operator": _first_text(
            old_metadata,
            old_manifest,
            old_log,
            field_names=("replacement_operator", "sql_replacement_operator"),
        ),
        "timestamp": timestamp,
        "replaced_by_paper_id": _field_value(remote_old, "replaced_by_paper_id"),
        "replaced_by_version": _field_value(remote_old, "replaced_by_version"),
        "replaces_paper_id": _field_value(remote_new, "replaces_paper_id"),
        "replaces_version": _field_value(remote_new, "replaces_version"),
    }


def _old_metadata_payload(
    payload: Mapping[str, Any],
    actions: tuple[ReplacementLifecycleAction, ...],
    context: Mapping[str, Any],
) -> dict[str, Any]:
    updated = dict(payload)
    for action in actions:
        fields = dict(action.fields)
        for field_name in (
            "sql_status",
            "sql_upload_record_id",
            "sql_replacement_record_id",
            "sql_replacement_reason",
            "sql_replacement_operator",
            "sql_replacement_batch_id",
            "sql_audit_history",
        ):
            if field_name in fields:
                updated[field_name] = fields[field_name]
    updated["replaced_at"] = context["timestamp"]
    updated["replaced_by"] = context["operator"]
    updated["replacement_reason"] = context["reason"]
    if context.get("replaced_by_paper_id") is not None and context.get("replaced_by_version") is not None:
        updated["replaced_by_paper_id"] = context["replaced_by_paper_id"]
        updated["replaced_by_version"] = context["replaced_by_version"]
    return updated


def _old_manifest_payload(
    payload: Mapping[str, Any],
    actions: tuple[ReplacementLifecycleAction, ...],
    context: Mapping[str, Any],
) -> dict[str, Any]:
    updated = dict(payload)
    for action in actions:
        fields = dict(action.fields)
        for field_name in (
            "published_to_postgres_scope",
            "remote_resource_id_scope",
        ):
            if field_name in fields:
                updated[field_name] = fields[field_name]
    replacement = dict(updated.get("replacement") or {})
    replacement.update(
        {
            "status": "replaced",
            "replacement_reason": context["reason"],
            "replaced_at": context["timestamp"],
            "replaced_by": context["operator"],
        },
    )
    if context.get("replaced_by_paper_id") is not None and context.get("replaced_by_version") is not None:
        replacement["replaced_by_paper_id"] = context["replaced_by_paper_id"]
        replacement["replaced_by_version"] = context["replaced_by_version"]
    marker = context.get("idempotency_marker")
    if isinstance(marker, Mapping):
        replacement["local_idempotency_marker"] = dict(marker)
    updated["replacement"] = replacement
    return updated


def _new_manifest_payload(
    payload: Mapping[str, Any],
    actions: tuple[ReplacementLifecycleAction, ...],
    context: Mapping[str, Any],
) -> dict[str, Any]:
    updated = dict(payload)
    for action in actions:
        fields = dict(action.fields)
        for field_name in ("replacement_of_resource_id", "replacement_batch_id"):
            if field_name in fields:
                updated[field_name] = fields[field_name]
    if context.get("replaces_paper_id") is not None and context.get("replaces_version") is not None:
        updated["replaces_paper_id"] = context["replaces_paper_id"]
        updated["replaces_version"] = context["replaces_version"]
    updated["replacement_reason"] = context["reason"]
    updated["replacement_created_at"] = context["timestamp"]
    updated["replacement_created_by"] = context["operator"]
    marker = context.get("idempotency_marker")
    if isinstance(marker, Mapping):
        replacement = dict(updated.get("replacement") or {})
        replacement["local_idempotency_marker"] = dict(marker)
        updated["replacement"] = replacement
    return updated


def _upload_log_payload(
    upload_log_path: Path,
    actions: tuple[ReplacementLifecycleAction, ...],
) -> dict[str, Any] | None:
    if not actions:
        return None

    if upload_log_path.exists():
        payload = _read_json_object(upload_log_path, "upload_log")
        raw_events = payload.get("events")
        if not isinstance(raw_events, list):
            raise ValueError("upload_log_events_not_list")
        payload = dict(payload)
        payload["events"] = list(raw_events)
        return payload

    return {
        "schema_version": UPLOAD_LOG_SCHEMA_VERSION,
        "log_type": UPLOAD_LOG_TYPE,
        "events": [],
    }


def _log_with_events(
    payload: Mapping[str, Any],
    actions: tuple[ReplacementLifecycleAction, ...],
    context: Mapping[str, Any],
    *,
    replacement_status: str | None,
) -> dict[str, Any]:
    updated = dict(payload)
    events = list(updated.get("events") or [])
    for action in actions:
        event = _normalized_log_event(action.fields, context)
        events.append(event)
    updated["events"] = events
    if replacement_status is not None:
        updated["last_status"] = replacement_status
    else:
        updated["last_status"] = _new_log_status(updated.get("last_status"))
    return updated


def _normalized_log_event(
    fields: Mapping[str, Any],
    context: Mapping[str, Any],
) -> dict[str, Any]:
    event = dict(fields)
    if "replacement_operator" in event:
        event["operator"] = event["replacement_operator"]
    if "replacement_reason" in event:
        event["reason"] = event["replacement_reason"]
    event.setdefault("old_resource_id", context.get("old_resource_id"))
    event.setdefault("new_resource_id", context.get("new_resource_id"))
    event.setdefault("operator", context.get("operator"))
    event.setdefault("reason", context.get("reason"))
    event.setdefault("created_at", context.get("timestamp"))
    event.setdefault("updated_at", context.get("timestamp"))
    marker = context.get("idempotency_marker")
    if isinstance(marker, Mapping):
        for field_name in (
            "operation_id",
            "receipt_hash",
            "idempotency_key",
            "remote_transaction_status",
            "local_write_trigger",
            "marker_scope",
            "marker_schema_version",
        ):
            if field_name in marker:
                event.setdefault(field_name, marker[field_name])
    return event


def _new_log_status(current_status: object) -> str:
    text = _optional_text(current_status)
    if text in {None, "not_uploaded"}:
        return "not_uploaded"
    if text == "uploaded":
        return "replacement_created"
    return text


def _write_all_json(payloads: Mapping[Path, Mapping[str, Any]]) -> None:
    encoded_payloads = {
        path: f"{json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)}\n"
        for path, payload in payloads.items()
    }
    for path, encoded in encoded_payloads.items():
        tmp_path = path.with_name(f".{path.name}.tmp")
        tmp_path.write_text(encoded, encoding="utf-8")
    for path in encoded_payloads:
        path.with_name(f".{path.name}.tmp").replace(path)


def _read_json_object(path: Path, label: str) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{label}_not_object")
    return payload


def _missing_file_error(
    filename: str | None,
    old_metadata_path: Path,
    old_manifest_path: Path,
    new_manifest_path: Path,
) -> str:
    if filename is None:
        return "replacement_local_file_missing"
    missing_path = Path(filename)
    if missing_path == old_metadata_path:
        return "old_metadata_missing"
    if missing_path == old_manifest_path:
        return "old_manifest_missing"
    if missing_path == new_manifest_path:
        return "new_manifest_missing"
    return "replacement_local_file_missing"


def _idempotency_base(
    idempotency_context: ReplacementLocalWriteIdempotencyContext | None,
) -> dict[str, Any] | None:
    if idempotency_context is None:
        return None
    operation_id = _optional_text(idempotency_context.operation_id)
    receipt_hash = _optional_text(idempotency_context.receipt_hash)
    return {
        "idempotency_marker_supported": True,
        "operation_id": operation_id,
        "receipt_hash": receipt_hash,
        "idempotency_key": _idempotency_key(operation_id, receipt_hash, idempotency_context.idempotency_key),
    }


def _blocked_idempotency_kwargs(
    base: Mapping[str, Any] | None,
    reason: str | None,
) -> dict[str, Any]:
    if base is None:
        return {}
    return {
        **base,
        "idempotency_status": "blocked",
        "reconciliation_required": True,
        "blocked_reasons": tuple(reason for reason in (reason,) if reason),
    }


def _applied_idempotency_kwargs(
    base: Mapping[str, Any] | None,
    marker: ReplacementLocalIdempotencyMarker | None,
) -> dict[str, Any]:
    if base is None or marker is None:
        return {}
    return {
        **base,
        "operation_id": marker.operation_id,
        "receipt_hash": marker.receipt_hash,
        "idempotency_key": marker.idempotency_key,
        "idempotency_status": "applied",
    }


def _check_idempotency_state(
    *,
    idempotency_context: ReplacementLocalWriteIdempotencyContext | None,
    old_manifest: Mapping[str, Any],
    old_log: Mapping[str, Any] | None,
    new_manifest: Mapping[str, Any],
    new_log: Mapping[str, Any] | None,
) -> dict[str, Any]:
    base = _idempotency_base(idempotency_context)
    if idempotency_context is None or base is None:
        return {"status": "not_requested"}

    operation_id = base.get("operation_id")
    receipt_hash = base.get("receipt_hash")
    if operation_id is None or receipt_hash is None:
        reason = "idempotency_context_missing_identity"
        return {
            "status": "blocked",
            "error": "replacement_local_idempotency_blocked",
            "result_kwargs": _blocked_idempotency_kwargs(base, reason),
        }

    markers = (
        _manifest_marker(old_manifest),
        _log_marker(old_log),
        _manifest_marker(new_manifest),
        _log_marker(new_log),
    )
    present_markers = tuple(marker for marker in markers if marker is not None)
    if not present_markers:
        return {"status": "not_applied"}

    identities = tuple(_marker_identity(marker) for marker in present_markers)
    if any(identity is None for identity in identities):
        return _partial_idempotency_result(base)
    if len(present_markers) != len(markers):
        return _partial_idempotency_result(base)
    if len(set(identities)) != 1:
        return _partial_idempotency_result(base)

    existing_operation_id, existing_receipt_hash = identities[0]
    if existing_operation_id == operation_id and existing_receipt_hash == receipt_hash:
        return {
            "status": "already_applied",
            "result_kwargs": {
                **base,
                "idempotency_status": "already_applied",
                "duplicate_receipt_detected": True,
                "idempotent_noop": True,
            },
        }
    if existing_operation_id == operation_id:
        return _conflict_idempotency_result(base, "same_operation_id_different_receipt_hash")
    if existing_receipt_hash == receipt_hash:
        return _conflict_idempotency_result(base, "same_receipt_hash_different_operation_id")
    return _conflict_idempotency_result(base, "different_idempotency_marker")


def _conflict_idempotency_result(
    base: Mapping[str, Any],
    reason: str,
) -> dict[str, Any]:
    return {
        "status": "conflict",
        "error": "replacement_local_idempotency_conflict",
        "result_kwargs": {
            **base,
            "idempotency_status": "conflict",
            "idempotency_conflict": True,
            "reconciliation_required": True,
            "blocked_reasons": (reason,),
        },
    }


def _partial_idempotency_result(base: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status": "partial_marker_detected",
        "error": "replacement_local_partial_idempotency_marker_detected",
        "result_kwargs": {
            **base,
            "idempotency_status": "partial_marker_detected",
            "partial_idempotency_marker_detected": True,
            "reconciliation_required": True,
            "blocked_reasons": ("partial_idempotency_marker_detected",),
        },
    }


def _manifest_marker(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    replacement = payload.get("replacement")
    if not isinstance(replacement, Mapping):
        return None
    marker = replacement.get("local_idempotency_marker")
    if not isinstance(marker, Mapping):
        return None
    return dict(marker)


def _log_marker(payload: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    events = payload.get("events")
    if not isinstance(events, list):
        return None
    for event in reversed(events):
        if not isinstance(event, Mapping):
            continue
        marker = _event_marker(event)
        if marker is not None:
            return marker
    return None


def _event_marker(event: Mapping[str, Any]) -> dict[str, Any] | None:
    marker_fields = (
        "operation_id",
        "receipt_hash",
        "idempotency_key",
        "remote_transaction_status",
        "local_write_trigger",
        "marker_scope",
        "marker_schema_version",
    )
    if not any(field_name in event for field_name in marker_fields):
        return None
    return {
        field_name: event[field_name]
        for field_name in marker_fields
        if field_name in event
    }


def _marker_identity(marker: Mapping[str, Any]) -> tuple[str, str] | None:
    operation_id = _optional_text(marker.get("operation_id"))
    receipt_hash = _optional_text(marker.get("receipt_hash"))
    idempotency_key = _optional_text(marker.get("idempotency_key"))
    marker_scope = _optional_text(marker.get("marker_scope"))
    if operation_id is None or receipt_hash is None:
        return None
    if idempotency_key != _idempotency_key(operation_id, receipt_hash, None):
        return None
    if marker_scope is not None and marker_scope != IDEMPOTENCY_MARKER_SCOPE:
        return None
    return operation_id, receipt_hash


def _marker_from_context(
    idempotency_context: ReplacementLocalWriteIdempotencyContext,
    context: Mapping[str, Any],
) -> ReplacementLocalIdempotencyMarker:
    operation_id = _optional_text(idempotency_context.operation_id) or ""
    receipt_hash = _optional_text(idempotency_context.receipt_hash) or ""
    return ReplacementLocalIdempotencyMarker(
        operation_id=operation_id,
        receipt_hash=receipt_hash,
        idempotency_key=_idempotency_key(operation_id, receipt_hash, idempotency_context.idempotency_key),
        remote_transaction_status=idempotency_context.remote_transaction_status,
        local_write_trigger=idempotency_context.local_write_trigger,
        applied_at=_optional_text(context.get("timestamp")),
        applied_by=_optional_text(context.get("operator")),
        marker_schema_version=idempotency_context.marker_schema_version,
        remote_receipt_scope=idempotency_context.remote_receipt_scope,
        remote_committed_at=idempotency_context.remote_committed_at,
        remote_receipt_operation_id=idempotency_context.remote_receipt_operation_id,
        source=idempotency_context.source,
    )


def _idempotency_key(
    operation_id: str | None,
    receipt_hash: str | None,
    explicit_key: str | None,
) -> str | None:
    key = _optional_text(explicit_key)
    if key is not None:
        return key
    if operation_id is None or receipt_hash is None:
        return None
    return f"{operation_id}:{receipt_hash}"


def _first_action(
    actions: tuple[ReplacementLifecycleAction, ...],
    target: str,
) -> ReplacementLifecycleAction | None:
    for action in actions:
        if action.target == target:
            return action
    return None


def _first_text(
    *actions: ReplacementLifecycleAction | None,
    field_names: tuple[str, ...],
) -> str | None:
    for action in actions:
        if action is None:
            continue
        for field_name in field_names:
            text = _optional_text(action.fields.get(field_name))
            if text is not None:
                return text
    return None


def _field_value(action: ReplacementLifecycleAction | None, field_name: str) -> Any:
    if action is None:
        return None
    return action.fields.get(field_name)


def _requested_at_from_action(action: ReplacementLifecycleAction | None) -> str | None:
    if action is None:
        return None
    raw_history = action.fields.get("sql_audit_history")
    if not isinstance(raw_history, list) or not raw_history:
        return None
    first_event = raw_history[0]
    if not isinstance(first_event, Mapping):
        return None
    return _optional_text(first_event.get("requested_at"))


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _result(
    old_metadata_path: Path,
    old_package_dir: Path,
    old_manifest_path: Path,
    old_upload_log_path: Path,
    new_package_dir: Path,
    new_manifest_path: Path,
    new_upload_log_path: Path,
    *,
    success: bool,
    old_metadata_updated: bool = False,
    old_manifest_updated: bool = False,
    old_upload_log_updated: bool = False,
    new_manifest_updated: bool = False,
    new_upload_log_updated: bool = False,
    applied_actions: tuple[str, ...] = (),
    skipped_actions: tuple[str, ...] = (),
    error: str | None = None,
    warnings: tuple[str, ...] = (),
    idempotency_marker_supported: bool = False,
    idempotency_status: str = "not_requested",
    operation_id: str | None = None,
    receipt_hash: str | None = None,
    idempotency_key: str | None = None,
    duplicate_receipt_detected: bool = False,
    idempotent_noop: bool = False,
    idempotency_conflict: bool = False,
    partial_idempotency_marker_detected: bool = False,
    reconciliation_required: bool = False,
    blocked_reasons: tuple[str, ...] = (),
) -> ReplacementLocalWriteResult:
    return ReplacementLocalWriteResult(
        success=success,
        old_metadata_path=old_metadata_path,
        old_package_dir=old_package_dir,
        old_manifest_path=old_manifest_path,
        old_upload_log_path=old_upload_log_path,
        new_package_dir=new_package_dir,
        new_manifest_path=new_manifest_path,
        new_upload_log_path=new_upload_log_path,
        execution_scope="local_files",
        commit_scope="none",
        wrote_local_files=success and bool(applied_actions),
        committed_to_fake_state=False,
        connected_postgres=False,
        executed_sql=False,
        real_postgres_committed=False,
        old_metadata_updated=old_metadata_updated,
        old_manifest_updated=old_manifest_updated,
        old_upload_log_updated=old_upload_log_updated,
        new_manifest_updated=new_manifest_updated,
        new_upload_log_updated=new_upload_log_updated,
        applied_actions=applied_actions,
        skipped_actions=skipped_actions,
        error=error,
        warnings=warnings,
        idempotency_marker_supported=idempotency_marker_supported,
        idempotency_status=idempotency_status,
        operation_id=operation_id,
        receipt_hash=receipt_hash,
        idempotency_key=idempotency_key,
        duplicate_receipt_detected=duplicate_receipt_detected,
        idempotent_noop=idempotent_noop,
        idempotency_conflict=idempotency_conflict,
        partial_idempotency_marker_detected=partial_idempotency_marker_detected,
        reconciliation_required=reconciliation_required,
        blocked_reasons=blocked_reasons,
    )
