from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from paper_pipeline.sql.package_preview import (
    PackageSqlPreview,
    PackageSqlPreviewBlockedItem,
    build_package_sql_preview,
)


SCHEMA_VERSION = "1.0"
SNAPSHOT_TYPE = "package_sql_preview"
_TMP_SNAPSHOT_FILENAME = ".snapshot.tmp"


@dataclass(frozen=True)
class PackagePreviewSnapshotResult:
    preview_id: str
    snapshot_path: Path
    created: bool
    candidate_count: int
    allowed_count: int
    blocked_count: int
    field_snapshot_hash: str
    warnings: list[str]


def build_package_preview_snapshot(
    package_dirs: Iterable[Path],
    snapshot_path: Path,
    *,
    preview_id: str | None = None,
) -> PackagePreviewSnapshotResult:
    package_previews = [
        build_package_sql_preview(Path(package_dir))
        for package_dir in package_dirs
    ]
    resolved_preview_id = preview_id or f"preview_{uuid.uuid4().hex}"
    snapshot_file = Path(snapshot_path)
    allowed_previews = [
        _allowed_preview_to_snapshot(preview)
        for preview in package_previews
        if preview.allowed
    ]
    blocked_previews = [
        _blocked_preview_to_snapshot(preview.blocked_item)
        for preview in package_previews
        if not preview.allowed and preview.blocked_item is not None
    ]
    warnings = _merge_unique(
        [
            warning
            for preview in package_previews
            for warning in preview.warnings
        ],
    )
    candidate_paper_ids = [
        paper_id
        for preview in package_previews
        if (paper_id := preview.paper_id) is not None
    ]

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "snapshot_type": SNAPSHOT_TYPE,
        "preview_id": resolved_preview_id,
        "preview_created_at": datetime.now(UTC).isoformat(),
        "candidate_count": len(package_previews),
        "allowed_count": len(allowed_previews),
        "blocked_count": len(blocked_previews),
        "candidate_paper_ids": candidate_paper_ids,
        "allowed_previews": allowed_previews,
        "blocked_previews": blocked_previews,
        "warnings": warnings,
        "field_snapshot_hash": "",
    }
    field_snapshot_hash = calculate_field_snapshot_hash(payload)
    payload["field_snapshot_hash"] = field_snapshot_hash
    _atomic_write_json(snapshot_file, payload)

    return PackagePreviewSnapshotResult(
        preview_id=resolved_preview_id,
        snapshot_path=snapshot_file,
        created=True,
        candidate_count=len(package_previews),
        allowed_count=len(allowed_previews),
        blocked_count=len(blocked_previews),
        field_snapshot_hash=field_snapshot_hash,
        warnings=warnings,
    )


def calculate_field_snapshot_hash(snapshot_payload: dict[str, Any]) -> str:
    hash_payload = dict(snapshot_payload)
    hash_payload.pop("field_snapshot_hash", None)
    encoded = json.dumps(
        hash_payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _allowed_preview_to_snapshot(preview: PackageSqlPreview) -> dict[str, Any]:
    return {
        "package_dir": str(preview.package_dir),
        "paper_id": preview.paper_id,
        "version": preview.version,
        "title": preview.title,
        "category": preview.category,
        "direction": preview.direction,
        "tier": preview.tier,
        "version_status": preview.version_status,
        "duplicate_status": preview.duplicate_status,
        "sensitive_clean": preview.sensitive_clean,
        "index_status": preview.index_status,
        "sql_status": preview.sql_status,
        "abstract": preview.abstract,
        "keywords": list(preview.keywords),
        "chapter_titles": list(preview.chapter_titles),
        "chapter_roles": list(preview.chapter_roles),
        "source_hash": preview.source_hash,
        "markdown_hash": preview.markdown_hash,
        "text_hash": preview.text_hash,
        "markdown_path": preview.markdown_path,
        "metadata_path": preview.metadata_path,
        "markdown_size_bytes": preview.markdown_size_bytes,
        "markdown_char_count": preview.markdown_char_count,
        "will_upload_markdown_content": preview.will_upload_markdown_content,
        "warnings": list(preview.warnings),
        "blocks_warnings": list(preview.blocks_warnings),
        "abstract_source": preview.abstract_source,
        "keywords_source": preview.keywords_source,
        "chapter_role_source": list(preview.chapter_role_source),
        "metadata_enriched": preview.metadata_enriched,
        "llm_model": list(preview.llm_model),
    }


def _blocked_preview_to_snapshot(
    blocked_item: PackageSqlPreviewBlockedItem,
) -> dict[str, Any]:
    return {
        "package_dir": str(blocked_item.package_dir),
        "paper_id": blocked_item.paper_id,
        "version": blocked_item.version,
        "reasons": list(blocked_item.reasons),
        "blocking_fields": list(blocked_item.blocking_fields),
        "suggested_next_action": blocked_item.suggested_next_action,
        "warnings": list(blocked_item.warnings),
    }


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    tmp_path = path.with_name(_TMP_SNAPSHOT_FILENAME)
    encoded = json.dumps(payload, ensure_ascii=False, indent=2)
    tmp_path.write_text(f"{encoded}\n", encoding="utf-8")
    tmp_path.replace(path)


def _merge_unique(items: list[str]) -> list[str]:
    merged: list[str] = []
    for item in items:
        if item and item not in merged:
            merged.append(item)
    return merged
