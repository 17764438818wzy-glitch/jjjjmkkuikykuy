"""Data models for SQL preview and confirmation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

SQL_CONFIRMATION_TEXT = "确认上传SQL"


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def utc_expires_iso(*, hours: int = 24) -> str:
    return (datetime.now(UTC) + timedelta(hours=hours)).isoformat()


@dataclass(frozen=True, slots=True)
class SqlUploadPreviewItem:
    paper_id: str
    accession_id: str
    version: int
    title: str
    tier: str
    category: str
    direction: str
    research_method_primary: str
    research_method_tags: list[str]
    source_hash: str
    source_path: str
    md_path: str
    metadata_path: str
    index_status: str
    sql_status: str
    version_status: str
    duplicate_status: str
    abstract: str
    keywords: list[str]
    similar_to: list[str] = field(default_factory=list)
    metadata_fingerprint: str = ""
    md_fingerprint: str = ""


@dataclass(frozen=True, slots=True)
class SqlUploadBlockedItem:
    paper_id: str
    reasons: list[str]
    blocking_fields: list[str]
    suggested_next_action: str
    metadata_path: str = ""
    md_path: str = ""


@dataclass(frozen=True, slots=True)
class SqlUploadPreview:
    preview_id: str
    run_id: str
    created_at: str
    created_by: str
    candidate_count: int
    eligible_count: int
    blocked_count: int
    items: list[SqlUploadPreviewItem]
    blocked_items: list[SqlUploadBlockedItem]
    preview_checksum: str
    expires_at: str
    ready_for_confirmation: bool
    will_upload_sql: bool = False


@dataclass(frozen=True, slots=True)
class SqlUploadConfirmationContext:
    preview_id: str
    preview_checksum: str
    eligible_count: int
    upload_mode: str
    target_summary: str
    confirmation_text: str


@dataclass(frozen=True, slots=True)
class SqlUploadConfirmation:
    preview_id: str
    preview_checksum: str
    confirmed_by: str
    confirmation_text: str
    preview_was_shown: bool = False
    operator_confirmed_upload: bool = False
    eligible_count: int = -1
    upload_mode: str = ""
    target_summary: str = ""
    confirmed_at: str = field(default_factory=utc_now_iso)


def preview_to_dict(preview: SqlUploadPreview) -> dict[str, Any]:
    return cast(dict[str, Any], asdict(preview))


def preview_from_dict(data: dict[str, Any]) -> SqlUploadPreview:
    return SqlUploadPreview(
        preview_id=str(data["preview_id"]),
        run_id=str(data["run_id"]),
        created_at=str(data["created_at"]),
        created_by=str(data["created_by"]),
        candidate_count=int(data["candidate_count"]),
        eligible_count=int(data["eligible_count"]),
        blocked_count=int(data["blocked_count"]),
        items=[
            SqlUploadPreviewItem(
                paper_id=str(item["paper_id"]),
                accession_id=str(item["accession_id"]),
                version=int(item["version"]),
                title=str(item["title"]),
                tier=str(item["tier"]),
                category=str(item["category"]),
                direction=str(item["direction"]),
                research_method_primary=str(item.get("research_method_primary", "")),
                research_method_tags=[str(tag) for tag in item.get("research_method_tags", [])],
                source_hash=str(item["source_hash"]),
                source_path=str(item["source_path"]),
                md_path=str(item["md_path"]),
                metadata_path=str(item["metadata_path"]),
                index_status=str(item["index_status"]),
                sql_status=str(item["sql_status"]),
                version_status=str(item["version_status"]),
                duplicate_status=str(item["duplicate_status"]),
                abstract=str(item["abstract"]),
                keywords=[str(keyword) for keyword in item.get("keywords", [])],
                similar_to=[str(value) for value in item.get("similar_to", [])],
                metadata_fingerprint=str(item.get("metadata_fingerprint", "")),
                md_fingerprint=str(item.get("md_fingerprint", "")),
            )
            for item in data.get("items", [])
        ],
        blocked_items=[
            SqlUploadBlockedItem(
                paper_id=str(item["paper_id"]),
                reasons=[str(reason) for reason in item.get("reasons", [])],
                blocking_fields=[str(field) for field in item.get("blocking_fields", [])],
                suggested_next_action=str(item.get("suggested_next_action", "")),
                metadata_path=str(item.get("metadata_path", "")),
                md_path=str(item.get("md_path", "")),
            )
            for item in data.get("blocked_items", [])
        ],
        preview_checksum=str(data.get("preview_checksum", "")),
        expires_at=str(data["expires_at"]),
        ready_for_confirmation=bool(data["ready_for_confirmation"]),
        will_upload_sql=bool(data.get("will_upload_sql", False)),
    )


def calculate_preview_checksum(preview: SqlUploadPreview) -> str:
    payload = preview_to_dict(preview)
    payload["preview_checksum"] = ""
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def build_sql_upload_confirmation_context(
    preview: SqlUploadPreview,
    *,
    upload_mode: str,
    target_summary: str,
) -> SqlUploadConfirmationContext:
    normalized_mode = str(upload_mode).strip() or "not-configured"
    safe_target_summary = str(target_summary).strip() or "target=unknown"
    return SqlUploadConfirmationContext(
        preview_id=preview.preview_id,
        preview_checksum=preview.preview_checksum,
        eligible_count=preview.eligible_count,
        upload_mode=normalized_mode,
        target_summary=safe_target_summary,
        confirmation_text=build_sql_upload_confirmation_text(
            preview_id=preview.preview_id,
            preview_checksum=preview.preview_checksum,
            eligible_count=preview.eligible_count,
            upload_mode=normalized_mode,
            target_summary=safe_target_summary,
        ),
    )


def build_sql_upload_confirmation_text(
    *,
    preview_id: str,
    preview_checksum: str,
    eligible_count: int,
    upload_mode: str,
    target_summary: str,
) -> str:
    payload = {
        "preview_id": preview_id,
        "preview_checksum": preview_checksum,
        "eligible_count": eligible_count,
        "upload_mode": upload_mode,
        "target_summary": target_summary,
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:12].upper()
    return f"CONFIRM SQL UPLOAD {preview_id} {eligible_count} {upload_mode} {digest}"


def file_fingerprint(path: str | Path) -> str:
    candidate = Path(path)
    if not candidate.exists() or not candidate.is_file():
        return ""
    digest = hashlib.sha256()
    with candidate.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_expired(expires_at: str, *, now: datetime | None = None) -> bool:
    current = now or datetime.now(UTC)
    expires = datetime.fromisoformat(expires_at)
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=UTC)
    return expires <= current
