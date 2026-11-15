from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from paper_pipeline.sql.package_preupload import check_package_preupload_gate


_MANIFEST_FILENAME = "manifest.json"
_METADATA_INDEX_FILENAME = "metadata_index.json"
_PAPER_TEXT_INDEX_FILENAME = "paper_text_index.json"
_CHAPTER_INDEX_FILENAME = "chapter_index.json"
_HIERARCHY_INDEX_FILENAME = "hierarchy_index.json"


@dataclass(frozen=True)
class PackageSqlPreviewBlockedItem:
    package_dir: Path
    paper_id: str | None
    version: int | None
    reasons: list[str]
    blocking_fields: list[str]
    suggested_next_action: str
    warnings: list[str]


@dataclass(frozen=True)
class PackageSqlPreview:
    package_dir: Path
    allowed: bool
    paper_id: str | None
    version: int | None
    title: str | None
    category: str | None
    direction: str | None
    tier: str | None
    version_status: str | None
    duplicate_status: str | None
    sensitive_clean: bool | None
    index_status: str | None
    sql_status: str | None
    abstract: str | None
    keywords: list[str]
    chapter_titles: list[str]
    chapter_roles: list[str]
    source_hash: str | None
    markdown_hash: str | None
    text_hash: str | None
    markdown_path: str | None
    metadata_path: str | None
    markdown_size_bytes: int | None
    markdown_char_count: int | None
    will_upload_markdown_content: bool
    warnings: list[str]
    blocks_warnings: list[str]
    abstract_source: str | None
    keywords_source: str | None
    chapter_role_source: list[str]
    metadata_enriched: bool | None
    llm_model: list[str]
    blocked_item: PackageSqlPreviewBlockedItem | None


class _PreviewBuildError(Exception):
    def __init__(self, reason: str, blocking_field: str) -> None:
        super().__init__(reason)
        self.reason = reason
        self.blocking_field = blocking_field


def build_package_sql_preview(
    package_dir: Path,
    *,
    current_schema_version: str = "1.0",
) -> PackageSqlPreview:
    package_path = Path(package_dir)
    gate = check_package_preupload_gate(
        package_path,
        current_schema_version=current_schema_version,
    )

    if not gate.allowed:
        return _blocked_preview(
            package_dir=gate.package_dir,
            paper_id=gate.paper_id,
            version=gate.version,
            reasons=gate.blocking_reasons,
            blocking_fields=gate.blocking_fields,
            suggested_next_action=gate.suggested_next_action,
            warnings=gate.warnings,
        )

    try:
        manifest = _read_required_json(
            package_path / _MANIFEST_FILENAME,
            missing_reason="package_preview_manifest_missing",
            blocking_field=_MANIFEST_FILENAME,
        )
        metadata_index = _read_required_json(
            package_path / _METADATA_INDEX_FILENAME,
            missing_reason="package_preview_metadata_index_missing",
            blocking_field=_METADATA_INDEX_FILENAME,
        )
        paper_text_index = _read_required_json(
            package_path / _PAPER_TEXT_INDEX_FILENAME,
            missing_reason="package_preview_paper_text_index_missing",
            blocking_field=_PAPER_TEXT_INDEX_FILENAME,
        )
        chapter_index = _read_required_json(
            package_path / _CHAPTER_INDEX_FILENAME,
            missing_reason="package_preview_chapter_index_missing",
            blocking_field=_CHAPTER_INDEX_FILENAME,
        )
        hierarchy_index = _read_required_json(
            package_path / _HIERARCHY_INDEX_FILENAME,
            missing_reason="package_preview_hierarchy_index_missing",
            blocking_field=_HIERARCHY_INDEX_FILENAME,
        )

        markdown_path = _first_str(
            manifest.get("markdown_path"),
            metadata_index.get("markdown_path"),
            paper_text_index.get("markdown_path"),
        )
        if markdown_path is None:
            raise _PreviewBuildError(
                "package_preview_missing_required_field",
                "markdown_path",
            )
        markdown_hash = _first_str(
            manifest.get("markdown_hash"),
            metadata_index.get("markdown_hash"),
            paper_text_index.get("markdown_hash"),
        )
        if markdown_hash is None:
            raise _PreviewBuildError(
                "package_preview_missing_required_field",
                "markdown_hash",
            )

        markdown_file = _resolve_recorded_path(markdown_path, package_path)
        markdown_bytes = _read_markdown_bytes(markdown_file)
        actual_markdown_hash = hashlib.sha256(markdown_bytes).hexdigest()
        if actual_markdown_hash != markdown_hash:
            raise _PreviewBuildError(
                "package_preview_markdown_hash_mismatch",
                "markdown_hash",
            )
        markdown_text = markdown_bytes.decode("utf-8")
    except (OSError, UnicodeDecodeError, _PreviewBuildError) as exc:
        if isinstance(exc, _PreviewBuildError):
            reason = exc.reason
            blocking_field = exc.blocking_field
        else:
            reason = "package_preview_markdown_missing"
            blocking_field = "markdown_path"
        return _blocked_preview(
            package_dir=gate.package_dir,
            paper_id=gate.paper_id,
            version=gate.version,
            reasons=[reason],
            blocking_fields=[blocking_field],
            suggested_next_action="rebuild_local_index_package",
            warnings=gate.warnings,
        )

    chapters = _chapter_records(chapter_index)
    warnings = _merge_unique(
        [
            *gate.warnings,
            *_warnings_from(manifest),
            *_warnings_from(metadata_index),
            *_warnings_from(paper_text_index),
            *_warnings_from(chapter_index),
            *_warnings_from(hierarchy_index),
        ],
    )

    return PackageSqlPreview(
        package_dir=gate.package_dir,
        allowed=True,
        paper_id=_first_str(metadata_index.get("paper_id"), manifest.get("paper_id"), gate.paper_id),
        version=_first_int(metadata_index.get("version"), manifest.get("version"), gate.version),
        title=_first_str(metadata_index.get("title"), paper_text_index.get("title")),
        category=_first_str(metadata_index.get("category"), paper_text_index.get("category")),
        direction=_first_str(metadata_index.get("direction"), paper_text_index.get("direction")),
        tier=_first_str(metadata_index.get("tier"), paper_text_index.get("tier")),
        version_status=_first_str(metadata_index.get("version_status")),
        duplicate_status=_first_str(metadata_index.get("duplicate_status")),
        sensitive_clean=_bool_or_none(metadata_index.get("sensitive_clean")),
        index_status=_first_str(metadata_index.get("index_status"), manifest.get("index_status")),
        sql_status=_first_str(metadata_index.get("sql_status")),
        abstract=_first_str(metadata_index.get("abstract"), paper_text_index.get("abstract")),
        keywords=_string_list(metadata_index.get("keywords") or paper_text_index.get("keywords")),
        chapter_titles=[
            title
            for chapter in chapters
            if (title := _first_str(chapter.get("chapter_title"))) is not None
        ],
        chapter_roles=[
            role
            for chapter in chapters
            if (role := _first_str(chapter.get("chapter_role"))) is not None
        ],
        source_hash=_first_str(metadata_index.get("source_hash"), manifest.get("source_hash")),
        markdown_hash=markdown_hash,
        text_hash=_first_str(paper_text_index.get("text_hash"), manifest.get("text_hash")),
        markdown_path=markdown_path,
        metadata_path=_first_str(metadata_index.get("metadata_path"), manifest.get("metadata_path")),
        markdown_size_bytes=len(markdown_bytes),
        markdown_char_count=len(markdown_text),
        will_upload_markdown_content=True,
        warnings=warnings,
        blocks_warnings=[warning for warning in warnings if "blocks" in warning],
        abstract_source=_first_str(metadata_index.get("abstract_source")),
        keywords_source=_first_str(metadata_index.get("keywords_source")),
        chapter_role_source=[
            source
            for chapter in chapters
            if (source := _first_str(chapter.get("chapter_role_source"))) is not None
        ],
        metadata_enriched=_bool_or_none(metadata_index.get("metadata_enriched")),
        llm_model=_unique_strings(
            chapter.get("llm_model")
            for chapter in chapters
            if _first_str(chapter.get("llm_model")) is not None
        ),
        blocked_item=None,
    )


def _blocked_preview(
    *,
    package_dir: Path,
    paper_id: str | None,
    version: int | None,
    reasons: list[str],
    blocking_fields: list[str],
    suggested_next_action: str,
    warnings: list[str],
) -> PackageSqlPreview:
    blocked_item = PackageSqlPreviewBlockedItem(
        package_dir=package_dir,
        paper_id=paper_id,
        version=version,
        reasons=list(reasons),
        blocking_fields=list(blocking_fields),
        suggested_next_action=suggested_next_action,
        warnings=list(warnings),
    )
    return PackageSqlPreview(
        package_dir=package_dir,
        allowed=False,
        paper_id=paper_id,
        version=version,
        title=None,
        category=None,
        direction=None,
        tier=None,
        version_status=None,
        duplicate_status=None,
        sensitive_clean=None,
        index_status=None,
        sql_status=None,
        abstract=None,
        keywords=[],
        chapter_titles=[],
        chapter_roles=[],
        source_hash=None,
        markdown_hash=None,
        text_hash=None,
        markdown_path=None,
        metadata_path=None,
        markdown_size_bytes=None,
        markdown_char_count=None,
        will_upload_markdown_content=False,
        warnings=list(warnings),
        blocks_warnings=[warning for warning in warnings if "blocks" in warning],
        abstract_source=None,
        keywords_source=None,
        chapter_role_source=[],
        metadata_enriched=None,
        llm_model=[],
        blocked_item=blocked_item,
    )


def _read_required_json(
    path: Path,
    *,
    missing_reason: str,
    blocking_field: str,
) -> dict[str, Any]:
    if not path.is_file():
        raise _PreviewBuildError(missing_reason, blocking_field)
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise _PreviewBuildError(
            "package_preview_json_invalid",
            blocking_field,
        ) from exc
    if not isinstance(decoded, dict):
        raise _PreviewBuildError("package_preview_json_invalid", blocking_field)
    return decoded


def _read_markdown_bytes(path: Path) -> bytes:
    if not path.is_file():
        raise _PreviewBuildError("package_preview_markdown_missing", "markdown_path")
    return path.read_bytes()


def _resolve_recorded_path(value: str, package_dir: Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return package_dir / path


def _chapter_records(chapter_index: dict[str, Any]) -> list[dict[str, Any]]:
    chapters = chapter_index.get("chapters")
    if not isinstance(chapters, list):
        return []
    return [chapter for chapter in chapters if isinstance(chapter, dict)]


def _warnings_from(payload: dict[str, Any]) -> list[str]:
    warnings = payload.get("warnings")
    if not isinstance(warnings, list):
        return []
    return [
        normalized
        for warning in warnings
        if (normalized := _first_str(warning)) is not None
    ]


def _first_str(*values: object) -> str | None:
    for value in values:
        if value is None:
            continue
        normalized = str(value).strip()
        if normalized:
            return normalized
    return None


def _first_int(*values: object) -> int | None:
    for value in values:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    return None


def _bool_or_none(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    return None


def _string_list(values: object) -> list[str]:
    if isinstance(values, str):
        return [normalized] if (normalized := _first_str(values)) is not None else []
    if not isinstance(values, list):
        return []
    return [
        normalized
        for value in values
        if (normalized := _first_str(value)) is not None
    ]


def _unique_strings(values: object) -> list[str]:
    unique_values: list[str] = []
    for value in values:
        normalized = _first_str(value)
        if normalized is None or normalized in unique_values:
            continue
        unique_values.append(normalized)
    return unique_values


def _merge_unique(items: list[str]) -> list[str]:
    merged: list[str] = []
    for item in items:
        if item and item not in merged:
            merged.append(item)
    return merged
