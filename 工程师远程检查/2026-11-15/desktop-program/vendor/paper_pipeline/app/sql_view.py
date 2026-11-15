"""Read-only SQL upload view models and disabled gateway defaults."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from paper_pipeline.sql.sql_models import preview_from_dict


@dataclass(frozen=True, slots=True)
class SqlPreviewRow:
    paper_id: str
    title: str
    tier: str
    category: str
    direction: str
    duplicate_status: str
    index_status: str
    sql_status: str
    version_status: str
    expected_result: str


@dataclass(frozen=True, slots=True)
class SqlBlockedRow:
    paper_id: str
    blocking_fields: list[str]
    reasons: list[str]
    suggested_next_action: str
    metadata_path: str
    md_path: str


@dataclass(frozen=True, slots=True)
class WorkbenchSqlUploadState:
    sql_mode: str
    adapter_mode: str
    connection_status: str
    target_table: str
    eligible_count: int
    blocked_count: int
    blocked_reasons: list[SqlBlockedRow]
    preview_id: str
    preview_checksum: str
    preview_rows: list[SqlPreviewRow]
    upload_result_rows: list[str] = field(default_factory=list)
    confirm_enabled: bool = False
    confirm_disabled_reason: str = ""


@dataclass(frozen=True, slots=True)
class SqlUploadActionResult:
    accepted: bool
    message: str
    preview_id: str = ""


class SqlUploadGateway(Protocol):
    def confirm_upload(
        self,
        *,
        preview_id: str,
        confirmation_text: str,
        confirmed_by: str,
    ) -> SqlUploadActionResult:
        raise NotImplementedError


class DisabledSqlUploadGateway:
    def confirm_upload(
        self,
        *,
        preview_id: str,
        confirmation_text: str,
        confirmed_by: str,
    ) -> SqlUploadActionResult:
        return SqlUploadActionResult(
            accepted=False,
            message="SQL 上传授权路径尚未接入，当前只展示预览。",
            preview_id=preview_id,
        )


def load_latest_sql_preview(library_dir: str | Path, run_state: Any | None) -> Any | None:
    if run_state is None or not getattr(run_state, "run_id", ""):
        return None
    preview_dir = Path(library_dir) / "runs" / str(run_state.run_id) / "sql_previews"
    if not preview_dir.exists():
        return None
    preview_paths = sorted(
        preview_dir.glob("*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for preview_path in preview_paths:
        try:
            decoded = json.loads(preview_path.read_text(encoding="utf-8"))
            if isinstance(decoded, dict):
                return preview_from_dict(decoded)
        except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
            continue
    return None


def build_sql_upload_state(
    *,
    preview: Any | None,
    sql_mode: str,
    connection_status: str,
    target_table: str,
) -> WorkbenchSqlUploadState:
    if preview is None:
        return WorkbenchSqlUploadState(
            sql_mode=sql_mode,
            adapter_mode=sql_mode,
            connection_status=connection_status,
            target_table=target_table,
            eligible_count=0,
            blocked_count=0,
            blocked_reasons=[],
            preview_id="",
            preview_checksum="",
            preview_rows=[],
            confirm_enabled=False,
            confirm_disabled_reason=_disabled_reason(sql_mode, preview),
        )
    confirm_enabled = _can_confirm(sql_mode, connection_status, preview)
    return WorkbenchSqlUploadState(
        sql_mode=sql_mode,
        adapter_mode=sql_mode,
        connection_status=connection_status,
        target_table=target_table,
        eligible_count=int(getattr(preview, "eligible_count", 0)),
        blocked_count=int(getattr(preview, "blocked_count", 0)),
        blocked_reasons=[
            SqlBlockedRow(
                paper_id=str(getattr(item, "paper_id", "")),
                blocking_fields=list(getattr(item, "blocking_fields", [])),
                reasons=list(getattr(item, "reasons", [])),
                suggested_next_action=str(getattr(item, "suggested_next_action", "")),
                metadata_path=str(getattr(item, "metadata_path", "")),
                md_path=str(getattr(item, "md_path", "")),
            )
            for item in getattr(preview, "blocked_items", [])
        ],
        preview_id=str(getattr(preview, "preview_id", "")),
        preview_checksum=str(getattr(preview, "preview_checksum", "")),
        preview_rows=[
            SqlPreviewRow(
                paper_id=str(getattr(item, "paper_id", "")),
                title=str(getattr(item, "title", "")),
                tier=str(getattr(item, "tier", "")),
                category=str(getattr(item, "category", "")),
                direction=str(getattr(item, "direction", "")),
                duplicate_status=str(getattr(item, "duplicate_status", "")),
                index_status=str(getattr(item, "index_status", "")),
                sql_status=str(getattr(item, "sql_status", "")),
                version_status=str(getattr(item, "version_status", "")),
                expected_result="更新或插入",
            )
            for item in getattr(preview, "items", [])
        ],
        confirm_enabled=confirm_enabled,
        confirm_disabled_reason="" if confirm_enabled else _disabled_reason(sql_mode, preview),
    )


def _can_confirm(sql_mode: str, connection_status: str, preview: Any) -> bool:
    return (
        sql_mode == "real"
        and connection_status == "ok"
        and bool(getattr(preview, "ready_for_confirmation", False))
        and int(getattr(preview, "eligible_count", 0)) > 0
        and not _is_expired(str(getattr(preview, "expires_at", "")))
    )


def _disabled_reason(sql_mode: str, preview: Any | None) -> str:
    if sql_mode != "real":
        return f"当前 SQL 模式是 {sql_mode}，不会真实写库。"
    if preview is None:
        return "尚无上传预览。"
    if int(getattr(preview, "eligible_count", 0)) <= 0:
        return "没有符合上传条件的文件。"
    if _is_expired(str(getattr(preview, "expires_at", ""))):
        return "上传预览已过期，请重新生成。"
    return "连接状态或授权路径尚未满足真实上传条件。"


def _is_expired(expires_at: str) -> bool:
    if not expires_at:
        return True
    expires = datetime.fromisoformat(expires_at)
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=UTC)
    return expires <= datetime.now(UTC)
