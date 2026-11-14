"""Source-file status markers used to avoid duplicate NAS processing."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

STATUS_DIR_NAME = "_paper_pipeline_status"
LOCKED_SOURCE_STATUSES = {"processing", "processed", "uploaded"}


def write_source_status_marker(
    source_path: str | Path,
    *,
    status: str,
    run_id: str = "",
    paper_id: str = "",
    target_table: str = "",
    extra: dict[str, Any] | None = None,
) -> Path:
    source = Path(source_path)
    marker = _status_marker_path(source)
    marker.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "source_path": str(source),
        "source_hash": _quick_source_fingerprint(source),
        "status": str(status).strip() or "processed",
        "run_id": str(run_id).strip(),
        "paper_id": str(paper_id).strip(),
        "target_table": str(target_table).strip(),
        "updated_at": datetime.now(UTC).isoformat(),
    }
    if extra:
        payload.update(extra)
    marker.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return marker


def read_source_status_marker(source_path: str | Path) -> dict[str, Any] | None:
    marker = _status_marker_path(Path(source_path))
    if not marker.exists():
        return None
    try:
        decoded = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(decoded, dict):
        return None
    return decoded


def resolve_workbench_original_source(source_path: str | Path) -> Path | None:
    source = Path(source_path)
    manifest = _workbench_selection_manifest_for(source)
    if manifest is None:
        return None
    try:
        payload = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    files = payload.get("files") if isinstance(payload, dict) else None
    if not isinstance(files, list):
        return None
    source_resolved = source.resolve(strict=False)
    for item in files:
        if not isinstance(item, dict):
            continue
        prepared = Path(str(item.get("prepared_path", "")))
        if prepared.resolve(strict=False) != source_resolved:
            continue
        original = Path(str(item.get("original_path", "")))
        if original.exists() and original.is_file():
            return original
    return None


def _status_marker_path(source: Path) -> Path:
    return source.parent / STATUS_DIR_NAME / f"{_safe_marker_name(source.name)}.json"


def _workbench_selection_manifest_for(source: Path) -> Path | None:
    candidates = [
        source.parent / "selection_manifest.json",
        source.parent.parent / "selection_manifest.json",
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _safe_marker_name(name: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in {".", "-", "_"} else "_" for ch in name)
    return safe or "source"


def _quick_source_fingerprint(source: Path) -> str:
    try:
        stat = source.stat()
    except OSError:
        return ""
    return f"{stat.st_size}:{int(stat.st_mtime_ns)}"
