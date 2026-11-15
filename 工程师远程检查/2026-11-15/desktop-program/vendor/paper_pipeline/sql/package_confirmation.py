from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from paper_pipeline.sql.package_preview_snapshot import calculate_field_snapshot_hash


SCHEMA_VERSION = "1.0"
SNAPSHOT_TYPE = "package_sql_preview_confirmation"
CONFIRMATION_STATUS = "confirmed_for_sql_upload"
_TMP_CONFIRMATION_FILENAME = ".confirmation.tmp"


@dataclass(frozen=True)
class PackageConfirmationResult:
    confirmation_id: str
    confirmation_path: Path
    created: bool
    preview_id: str | None
    confirmed_by: str
    operator_name: str
    confirmed_package_dirs: list[str]
    confirmed_paper_ids: list[str]
    field_snapshot_hash: str
    confirmation_status: str
    warnings: list[str]


def build_package_confirmation_snapshot(
    preview_snapshot_path: Path,
    confirmation_path: Path,
    *,
    confirmed_by: str,
    operator_name: str = "local_admin",
    preview_was_shown: bool,
    operator_confirmed_upload: bool,
    confirmation_id: str | None = None,
    allow_blocked: bool = False,
) -> PackageConfirmationResult:
    preview_file = Path(preview_snapshot_path)
    confirmation_file = Path(confirmation_path)
    preview_payload = _read_preview_snapshot(preview_file)
    field_snapshot_hash = _validated_field_snapshot_hash(preview_payload)

    if preview_was_shown is not True:
        raise ValueError("preview_was_not_shown")
    if operator_confirmed_upload is not True:
        raise ValueError("operator_upload_not_confirmed")
    resolved_confirmed_by = _required_text(
        confirmed_by,
        reason_code="confirmed_by_missing",
    )
    resolved_operator_name = _required_text(
        operator_name,
        reason_code="operator_name_missing",
    )

    candidate_count = _int_value(preview_payload.get("candidate_count"))
    allowed_count = _int_value(preview_payload.get("allowed_count"))
    blocked_count = _int_value(preview_payload.get("blocked_count"))
    if blocked_count > 0 and not allow_blocked:
        raise ValueError("blocked_preview_cannot_confirm")
    if allowed_count == 0:
        raise ValueError("no_allowed_preview_candidates")

    allowed_previews = _dict_list(preview_payload.get("allowed_previews"))
    confirmed_package_dirs = _strings_from_records(allowed_previews, "package_dir")
    confirmed_paper_ids = _string_list(preview_payload.get("candidate_paper_ids"))
    if not confirmed_paper_ids:
        confirmed_paper_ids = _strings_from_records(allowed_previews, "paper_id")
    warnings = _string_list(preview_payload.get("warnings"))
    resolved_confirmation_id = confirmation_id or f"confirmation_{uuid.uuid4().hex}"
    preview_id = _optional_text(preview_payload.get("preview_id"))

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "snapshot_type": SNAPSHOT_TYPE,
        "confirmation_id": resolved_confirmation_id,
        "preview_id": preview_id,
        "preview_snapshot_path": str(preview_file),
        "confirmed_at": datetime.now(UTC).isoformat(),
        "confirmed_by": resolved_confirmed_by,
        "operator_name": resolved_operator_name,
        "preview_was_shown": True,
        "operator_confirmed_upload": True,
        "field_snapshot_hash": field_snapshot_hash,
        "candidate_count": candidate_count,
        "allowed_count": allowed_count,
        "blocked_count": blocked_count,
        "confirmed_package_dirs": confirmed_package_dirs,
        "confirmed_paper_ids": confirmed_paper_ids,
        "warnings": warnings,
        "confirmation_status": CONFIRMATION_STATUS,
    }
    _atomic_write_json(confirmation_file, payload)

    return PackageConfirmationResult(
        confirmation_id=resolved_confirmation_id,
        confirmation_path=confirmation_file,
        created=True,
        preview_id=preview_id,
        confirmed_by=resolved_confirmed_by,
        operator_name=resolved_operator_name,
        confirmed_package_dirs=confirmed_package_dirs,
        confirmed_paper_ids=confirmed_paper_ids,
        field_snapshot_hash=field_snapshot_hash,
        confirmation_status=CONFIRMATION_STATUS,
        warnings=warnings,
    )


def _read_preview_snapshot(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ValueError("preview_snapshot_missing")
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError("preview_snapshot_invalid_json") from exc
    except OSError as exc:
        raise ValueError("preview_snapshot_missing") from exc
    if not isinstance(decoded, dict):
        raise ValueError("preview_snapshot_invalid_json")
    return decoded


def _validated_field_snapshot_hash(snapshot_payload: dict[str, Any]) -> str:
    field_snapshot_hash = snapshot_payload.get("field_snapshot_hash")
    if not isinstance(field_snapshot_hash, str) or not field_snapshot_hash.strip():
        raise ValueError("field_snapshot_hash_missing")
    calculated_hash = calculate_field_snapshot_hash(snapshot_payload)
    if calculated_hash != field_snapshot_hash:
        raise ValueError("field_snapshot_hash_mismatch")
    return field_snapshot_hash


def _required_text(value: str, *, reason_code: str) -> str:
    if not isinstance(value, str):
        raise ValueError(reason_code)
    normalized = value.strip()
    if not normalized:
        raise ValueError(reason_code)
    return normalized


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _int_value(value: object) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    return 0


def _dict_list(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _strings_from_records(records: list[dict[str, Any]], field_name: str) -> list[str]:
    return [
        normalized
        for record in records
        if (normalized := _optional_text(record.get(field_name))) is not None
    ]


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [
        normalized
        for item in value
        if (normalized := _optional_text(item)) is not None
    ]


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    tmp_path = path.with_name(_TMP_CONFIRMATION_FILENAME)
    encoded = json.dumps(payload, ensure_ascii=False, indent=2)
    tmp_path.write_text(f"{encoded}\n", encoding="utf-8")
    tmp_path.replace(path)
