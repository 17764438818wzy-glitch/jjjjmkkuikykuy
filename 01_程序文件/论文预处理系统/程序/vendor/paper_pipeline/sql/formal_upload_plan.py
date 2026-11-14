"""Read-only formal PostgreSQL upload plan builder.

This module assembles typed upload records from a validated local index package
and a human confirmation snapshot. It does not perform database I/O or mutate
the package files it reads.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from paper_pipeline.indexing.package_validation import validate_local_index_package
from paper_pipeline.sql.formal_schema import FORMAL_SCHEMA_VERSION


WRITE_ORDER = (
    "paper_upload_batches",
    "paper_resources",
    "paper_metadata_index",
    "paper_text_index",
    "paper_chapter_index",
    "paper_hierarchy_index",
    "paper_index_manifest",
)
_BATCH_STATUS_PLACEHOLDER_WARNING = "batch_status_placeholder_warning"
_CONFIRMATION_STATUS = "confirmed_for_sql_upload"


@dataclass(frozen=True)
class FormalUploadRecord:
    table_name: str
    record_id: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class FormalUploadPlan:
    package_dir: Path
    confirmation_snapshot_path: Path
    resource_id: str
    upload_batch_id: str
    records: tuple[FormalUploadRecord, ...]
    write_order: tuple[str, ...]
    markdown_content_char_count: int
    warnings: tuple[str, ...]


def build_formal_upload_plan(
    package_dir: Path,
    confirmation_snapshot_path: Path,
    *,
    resource_id: str | None = None,
    upload_batch_id: str | None = None,
    operator_name: str | None = None,
) -> FormalUploadPlan:
    package_path = Path(package_dir)
    confirmation_path = Path(confirmation_snapshot_path)

    validation = validate_local_index_package(package_path)
    if not validation.valid:
        raise ValueError(_package_validation_error(validation.issues))

    confirmation = _read_json_object(confirmation_path, "confirmation_snapshot")
    _validate_confirmation_snapshot(confirmation, package_path)

    manifest = _read_json_object(package_path / "manifest.json", "manifest")
    metadata_index = _read_json_object(package_path / "metadata_index.json", "metadata_index")
    paper_text_index = _read_json_object(package_path / "paper_text_index.json", "paper_text_index")
    chapter_index = _read_json_object(package_path / "chapter_index.json", "chapter_index")
    hierarchy_index = _read_json_object(package_path / "hierarchy_index.json", "hierarchy_index")

    markdown_path = _resolve_recorded_path(
        _required_text(manifest.get("markdown_path"), "markdown_path_missing"),
        package_path,
    )
    try:
        markdown_content = markdown_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError("markdown_file_missing") from exc

    resolved_resource_id = _optional_text(resource_id) or _new_uuid()
    resolved_upload_batch_id = _optional_text(upload_batch_id) or _new_uuid()
    resolved_operator = _resolve_operator_name(operator_name, confirmation)
    now = _utc_now_iso()
    warnings = _merge_unique(
        [
            *[warning.code for warning in validation.warnings],
            *_string_list(confirmation.get("warnings")),
            *_string_list(manifest.get("warnings")),
            *_string_list(metadata_index.get("warnings")),
            *_string_list(paper_text_index.get("warnings")),
            *_string_list(chapter_index.get("warnings")),
            *_string_list(hierarchy_index.get("warnings")),
            _BATCH_STATUS_PLACEHOLDER_WARNING,
        ],
    )

    metadata_record_id = _new_uuid()
    text_record_id = _new_uuid()
    hierarchy_record_id = _new_uuid()
    manifest_record_id = _new_uuid()
    chapter_records = _chapter_records(
        chapter_index=chapter_index,
        resource_id=resolved_resource_id,
        now=now,
    )
    records = (
        FormalUploadRecord(
            table_name="paper_upload_batches",
            record_id=resolved_upload_batch_id,
            payload=_batch_payload(
                upload_batch_id=resolved_upload_batch_id,
                confirmation=confirmation,
                operator_name=resolved_operator,
                now=now,
            ),
        ),
        FormalUploadRecord(
            table_name="paper_resources",
            record_id=resolved_resource_id,
            payload=_resource_payload(
                resource_id=resolved_resource_id,
                manifest=manifest,
                metadata_index=metadata_index,
                paper_text_index=paper_text_index,
                markdown_content=markdown_content,
                operator_name=resolved_operator,
                now=now,
            ),
        ),
        FormalUploadRecord(
            table_name="paper_metadata_index",
            record_id=metadata_record_id,
            payload=_metadata_payload(
                record_id=metadata_record_id,
                metadata_index=metadata_index,
                resource_id=resolved_resource_id,
                now=now,
            ),
        ),
        FormalUploadRecord(
            table_name="paper_text_index",
            record_id=text_record_id,
            payload=_text_payload(
                record_id=text_record_id,
                paper_text_index=paper_text_index,
                metadata_index=metadata_index,
                resource_id=resolved_resource_id,
                now=now,
            ),
        ),
        *chapter_records,
        FormalUploadRecord(
            table_name="paper_hierarchy_index",
            record_id=hierarchy_record_id,
            payload=_hierarchy_payload(
                record_id=hierarchy_record_id,
                hierarchy_index=hierarchy_index,
                resource_id=resolved_resource_id,
                now=now,
            ),
        ),
        FormalUploadRecord(
            table_name="paper_index_manifest",
            record_id=manifest_record_id,
            payload=_manifest_payload(
                record_id=manifest_record_id,
                manifest=manifest,
                metadata_index=metadata_index,
                confirmation=confirmation,
                resource_id=resolved_resource_id,
                upload_batch_id=resolved_upload_batch_id,
                package_dir=package_path,
                warnings=warnings,
                operator_name=resolved_operator,
                now=now,
            ),
        ),
    )

    return FormalUploadPlan(
        package_dir=package_path,
        confirmation_snapshot_path=confirmation_path,
        resource_id=resolved_resource_id,
        upload_batch_id=resolved_upload_batch_id,
        records=records,
        write_order=WRITE_ORDER,
        markdown_content_char_count=len(markdown_content),
        warnings=tuple(warnings),
    )


def _package_validation_error(issues: list[Any]) -> str:
    issue_codes = ",".join(str(issue.code) for issue in issues)
    issue_details = ";".join(
        f"{issue.code}:{message}"
        for issue in issues
        if (message := _optional_text(getattr(issue, "message", None))) is not None
    )
    if not issue_details:
        return f"package_validation_failed:{issue_codes}"
    return f"package_validation_failed:{issue_codes};details={issue_details}"


def _batch_payload(
    *,
    upload_batch_id: str,
    confirmation: dict[str, Any],
    operator_name: str | None,
    now: str,
) -> dict[str, Any]:
    return {
        "id": upload_batch_id,
        "schema_version": FORMAL_SCHEMA_VERSION,
        "status": "completed",
        "started_at": None,
        "completed_at": None,
        "total_count": 1,
        "success_count": 0,
        "failed_count": 0,
        "skipped_count": 0,
        "confirmed_by": _optional_text(confirmation.get("confirmed_by")),
        "confirmed_at": _optional_text(confirmation.get("confirmed_at")),
        "uploaded_by": operator_name,
        "notes": _BATCH_STATUS_PLACEHOLDER_WARNING,
        "created_at": now,
        "updated_at": now,
    }


def _resource_payload(
    *,
    resource_id: str,
    manifest: dict[str, Any],
    metadata_index: dict[str, Any],
    paper_text_index: dict[str, Any],
    markdown_content: str,
    operator_name: str | None,
    now: str,
) -> dict[str, Any]:
    return {
        "id": resource_id,
        "schema_version": _first_text(metadata_index, manifest, "schema_version")
        or FORMAL_SCHEMA_VERSION,
        "paper_id": _first_text(metadata_index, manifest, "paper_id"),
        "version": _first_value(metadata_index, manifest, "version"),
        "title": _first_value(metadata_index, paper_text_index, "title"),
        "sanitized_title": metadata_index.get("sanitized_title"),
        "abstract": _first_value(metadata_index, paper_text_index, "abstract"),
        "abstract_source": metadata_index.get("abstract_source"),
        "keywords": _first_value(metadata_index, paper_text_index, "keywords"),
        "keywords_source": metadata_index.get("keywords_source"),
        "category": _first_value(metadata_index, paper_text_index, "category"),
        "direction": _first_value(metadata_index, paper_text_index, "direction"),
        "major": metadata_index.get("major"),
        "discipline": metadata_index.get("discipline"),
        "thesis_type": metadata_index.get("thesis_type"),
        "research_methods": metadata_index.get("research_methods"),
        "tier": _first_value(metadata_index, paper_text_index, "tier"),
        "quality_tier": metadata_index.get("quality_tier"),
        "quality_reason": metadata_index.get("quality_reason"),
        "sensitive_clean": metadata_index.get("sensitive_clean"),
        "version_status": metadata_index.get("version_status"),
        "duplicate_status": metadata_index.get("duplicate_status"),
        "index_status": _first_value(metadata_index, manifest, "index_status"),
        "sql_status": metadata_index.get("sql_status"),
        "resource_status": "active",
        "source_path": _first_value(metadata_index, manifest, "source_path"),
        "markdown_path": _first_value(metadata_index, manifest, "markdown_path"),
        "metadata_path": _first_value(metadata_index, manifest, "metadata_path"),
        "storage_key": metadata_index.get("storage_key"),
        "object_url": metadata_index.get("object_url"),
        "source_hash": _first_value(metadata_index, manifest, "source_hash"),
        "markdown_hash": _first_value(metadata_index, manifest, paper_text_index, "markdown_hash"),
        "text_hash": _first_value(paper_text_index, manifest, metadata_index, "text_hash"),
        "blocks_path": metadata_index.get("blocks_path"),
        "blocks_hash": _first_value(metadata_index, manifest, "blocks_hash"),
        "block_count": metadata_index.get("block_count"),
        "parser_selected": metadata_index.get("parser_selected"),
        "blocks_schema_version": metadata_index.get("blocks_schema_version"),
        "desensitized_at": metadata_index.get("desensitized_at"),
        "markdown_content": markdown_content,
        "metadata_enriched": metadata_index.get("metadata_enriched"),
        "metadata_enriched_at": metadata_index.get("metadata_enriched_at"),
        "metadata_enriched_by": metadata_index.get("metadata_enriched_by"),
        "llm_model": metadata_index.get("llm_model"),
        "uploaded_at": None,
        "uploaded_by": operator_name,
        "withdrawn_at": None,
        "withdrawn_by": None,
        "withdrawn_reason": None,
        "replaced_by_paper_id": metadata_index.get("replaced_by_paper_id"),
        "replaced_by_version": metadata_index.get("replaced_by_version"),
        "replaces_paper_id": metadata_index.get("replaces_paper_id"),
        "replaces_version": metadata_index.get("replaces_version"),
        "replacement_reason": metadata_index.get("replacement_reason"),
        "created_at": now,
        "updated_at": now,
    }


def _metadata_payload(
    *,
    record_id: str,
    metadata_index: dict[str, Any],
    resource_id: str,
    now: str,
) -> dict[str, Any]:
    return {
        "id": record_id,
        "resource_id": resource_id,
        "schema_version": metadata_index.get("schema_version"),
        "paper_id": metadata_index.get("paper_id"),
        "version": metadata_index.get("version"),
        "title": metadata_index.get("title"),
        "sanitized_title": metadata_index.get("sanitized_title"),
        "category": metadata_index.get("category"),
        "direction": metadata_index.get("direction"),
        "major": metadata_index.get("major"),
        "discipline": metadata_index.get("discipline"),
        "thesis_type": metadata_index.get("thesis_type"),
        "research_methods": metadata_index.get("research_methods"),
        "keywords": metadata_index.get("keywords"),
        "abstract": metadata_index.get("abstract"),
        "tier": metadata_index.get("tier"),
        "quality_tier": metadata_index.get("quality_tier"),
        "quality_reason": metadata_index.get("quality_reason"),
        "sensitive_clean": metadata_index.get("sensitive_clean"),
        "version_status": metadata_index.get("version_status"),
        "duplicate_status": metadata_index.get("duplicate_status"),
        "index_status": metadata_index.get("index_status"),
        "sql_status": metadata_index.get("sql_status"),
        "resource_status": "active",
        "source_hash": metadata_index.get("source_hash"),
        "markdown_hash": metadata_index.get("markdown_hash"),
        "text_hash": metadata_index.get("text_hash"),
        "markdown_path": metadata_index.get("markdown_path"),
        "metadata_path": metadata_index.get("metadata_path"),
        "index_scope": metadata_index.get("index_scope"),
        "warnings": _string_list(metadata_index.get("warnings")),
        "created_at": now,
        "updated_at": now,
    }


def _text_payload(
    *,
    record_id: str,
    paper_text_index: dict[str, Any],
    metadata_index: dict[str, Any],
    resource_id: str,
    now: str,
) -> dict[str, Any]:
    return {
        "id": record_id,
        "resource_id": resource_id,
        "schema_version": paper_text_index.get("schema_version"),
        "paper_id": paper_text_index.get("paper_id"),
        "version": paper_text_index.get("version"),
        "title": _first_value(paper_text_index, metadata_index, "title"),
        "abstract": _first_value(paper_text_index, metadata_index, "abstract"),
        "keywords": _first_value(paper_text_index, metadata_index, "keywords"),
        "category": _first_value(paper_text_index, metadata_index, "category"),
        "direction": _first_value(paper_text_index, metadata_index, "direction"),
        "tier": _first_value(paper_text_index, metadata_index, "tier"),
        "retrieval_text": paper_text_index.get("retrieval_text"),
        "text_hash": paper_text_index.get("text_hash"),
        "search_vector": None,
        "resource_status": "active",
        "created_at": now,
        "updated_at": now,
    }


def _chapter_records(
    *,
    chapter_index: dict[str, Any],
    resource_id: str,
    now: str,
) -> tuple[FormalUploadRecord, ...]:
    raw_chapters = chapter_index.get("chapters")
    if not isinstance(raw_chapters, list):
        raise ValueError("chapter_index_chapters_invalid")

    records: list[FormalUploadRecord] = []
    for offset, chapter in enumerate(raw_chapters, start=1):
        if not isinstance(chapter, dict):
            raise ValueError("chapter_index_chapters_invalid")
        record_id = _new_uuid()
        chapter_id = _optional_text(chapter.get("chapter_id")) or f"ch_{offset:03d}"
        records.append(
            FormalUploadRecord(
                table_name="paper_chapter_index",
                record_id=record_id,
                payload={
                    "id": record_id,
                    "resource_id": resource_id,
                    "schema_version": _first_value(chapter, chapter_index, "schema_version"),
                    "paper_id": _first_value(chapter, chapter_index, "paper_id"),
                    "version": _first_value(chapter, chapter_index, "version"),
                    "chapter_id": chapter_id,
                    "chapter_order": chapter.get("chapter_order"),
                    "chapter_title": chapter.get("chapter_title"),
                    "chapter_role": chapter.get("chapter_role"),
                    "heading_path": chapter.get("heading_path"),
                    "text_hash": _first_value(chapter, chapter_index, "text_hash"),
                    "chapter_role_source": chapter.get("chapter_role_source"),
                    "llm_model": chapter.get("llm_model"),
                    "index_scope": _first_value(chapter, chapter_index, "index_scope"),
                    "resource_status": "active",
                    "warnings": _string_list(chapter.get("warnings")),
                    "created_at": now,
                    "updated_at": now,
                },
            ),
        )
    return tuple(records)


def _hierarchy_payload(
    *,
    record_id: str,
    hierarchy_index: dict[str, Any],
    resource_id: str,
    now: str,
) -> dict[str, Any]:
    return {
        "id": record_id,
        "resource_id": resource_id,
        "schema_version": hierarchy_index.get("schema_version"),
        "paper_id": hierarchy_index.get("paper_id"),
        "version": hierarchy_index.get("version"),
        "hierarchy_type": hierarchy_index.get("hierarchy_type"),
        "chapter_ids": hierarchy_index.get("chapter_ids"),
        "chapter_order": hierarchy_index.get("chapter_order"),
        "relations": hierarchy_index.get("relations"),
        "resource_status": "active",
        "created_at": now,
        "updated_at": now,
    }


def _manifest_payload(
    *,
    record_id: str,
    manifest: dict[str, Any],
    metadata_index: dict[str, Any],
    confirmation: dict[str, Any],
    resource_id: str,
    upload_batch_id: str,
    package_dir: Path,
    warnings: list[str],
    operator_name: str | None,
    now: str,
) -> dict[str, Any]:
    return {
        "id": record_id,
        "resource_id": resource_id,
        "upload_batch_id": upload_batch_id,
        "schema_version": manifest.get("schema_version") or FORMAL_SCHEMA_VERSION,
        "paper_id": manifest.get("paper_id"),
        "version": manifest.get("version"),
        "local_index_run_id": _first_value(
            manifest,
            confirmation,
            "local_index_run_id",
            "index_run_id",
            "run_id",
            "preview_id",
        ),
        "local_index_package_path": str(package_dir),
        "source_hash": _first_value(manifest, metadata_index, "source_hash"),
        "markdown_hash": _first_value(manifest, metadata_index, "markdown_hash"),
        "text_hash": _first_value(manifest, metadata_index, "text_hash"),
        "blocks_hash": _first_value(manifest, metadata_index, "blocks_hash"),
        "index_built_at": _first_value(manifest, "completed_at", "built_at", "updated_at"),
        "published_at": None,
        "published_by": operator_name,
        "warnings": list(warnings),
        "created_at": now,
        "updated_at": now,
    }


def _validate_confirmation_snapshot(
    confirmation: dict[str, Any],
    package_dir: Path,
) -> None:
    if confirmation.get("confirmation_status") != _CONFIRMATION_STATUS:
        raise ValueError("confirmation_not_confirmed")
    if confirmation.get("operator_confirmed_upload") is not True:
        raise ValueError("operator_upload_not_confirmed")

    confirmed_package_dirs = _string_list(confirmation.get("confirmed_package_dirs"))
    if not any(_same_path(package_dir, value) for value in confirmed_package_dirs):
        raise ValueError("package_not_confirmed")


def _read_json_object(path: Path, label: str) -> dict[str, Any]:
    if not path.is_file():
        raise ValueError(f"{label}_missing")
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label}_invalid_json") from exc
    except OSError as exc:
        raise ValueError(f"{label}_missing") from exc
    if not isinstance(decoded, dict):
        raise ValueError(f"{label}_invalid_json")
    return decoded


def _resolve_recorded_path(value: str, package_dir: Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return package_dir / path


def _same_path(left: Path, right: str) -> bool:
    candidate = Path(right)
    if candidate == left:
        return True
    try:
        return candidate.resolve() == left.resolve()
    except OSError:
        return False


def _resolve_operator_name(
    operator_name: str | None,
    confirmation: dict[str, Any],
) -> str | None:
    return (
        _optional_text(operator_name)
        or _optional_text(confirmation.get("operator_name"))
        or _optional_text(confirmation.get("confirmed_by"))
    )


def _first_value(*sources_and_keys: object) -> Any:
    sources, keys = _split_sources_and_keys(sources_and_keys)
    for key in keys:
        for source in sources:
            if key in source and source[key] is not None:
                return source[key]
    return None


def _first_text(*sources_and_keys: object) -> str | None:
    value = _first_value(*sources_and_keys)
    return _optional_text(value)


def _split_sources_and_keys(values: tuple[object, ...]) -> tuple[list[dict[str, Any]], list[str]]:
    sources: list[dict[str, Any]] = []
    keys: list[str] = []
    for value in values:
        if isinstance(value, dict):
            sources.append(value)
        elif isinstance(value, str):
            keys.append(value)
        else:
            raise TypeError("expected source dictionaries followed by keys")
    return sources, keys


def _required_text(value: object, reason_code: str) -> str:
    normalized = _optional_text(value)
    if normalized is None:
        raise ValueError(reason_code)
    return normalized


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [
        normalized
        for item in value
        if (normalized := _optional_text(item)) is not None
    ]


def _merge_unique(items: list[str]) -> list[str]:
    merged: list[str] = []
    for item in items:
        if item and item not in merged:
            merged.append(item)
    return merged


def _new_uuid() -> str:
    return str(uuid.uuid4())


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()
