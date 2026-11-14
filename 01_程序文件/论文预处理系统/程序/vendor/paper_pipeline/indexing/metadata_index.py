"""S5-1 metadata index writer for local index packages."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

from paper_pipeline.domain import PaperMetadata
from paper_pipeline.eligibility import EligibilityScope, check_formal_candidate
from paper_pipeline.indexing.package_skeleton import (
    MANIFEST_FILENAME,
    MANIFEST_TYPE,
    SCHEMA_VERSION,
    _sha256_file,
    build_package_dir_name,
)

INDEX_TYPE = "metadata_index"
INDEX_SCOPE = "formal_candidate"
METADATA_INDEX_FILENAME = "metadata_index.json"
_TMP_METADATA_INDEX_FILENAME = ".metadata_index.json.tmp"


@dataclass(frozen=True)
class MetadataIndexBuildResult:
    paper_id: str
    version: int
    package_dir: Path
    metadata_index_path: Path
    created: bool
    skipped: bool
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def build_metadata_index(
    metadata: PaperMetadata,
    metadata_path: Path,
    markdown_path: Path,
    package_dir: Path,
    *,
    overwrite: bool = False,
) -> MetadataIndexBuildResult:
    metadata_file = Path(metadata_path)
    markdown_file = Path(markdown_path)
    package_path = Path(package_dir)

    paper_id = str(metadata.id)
    version = metadata.version
    build_package_dir_name(paper_id, version)
    metadata_index_path = package_path / METADATA_INDEX_FILENAME

    eligibility = check_formal_candidate(
        metadata,
        metadata_path=metadata_file,
        markdown_path=markdown_file,
    )
    warnings = _merge_unique(eligibility.warnings)

    if not (
        eligibility.eligible
        and eligibility.scope is EligibilityScope.FORMAL_CANDIDATE
    ):
        reasons = _merge_unique(["not_formal_candidate", *eligibility.blocking_reasons])
        return MetadataIndexBuildResult(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            metadata_index_path=metadata_index_path,
            created=False,
            skipped=True,
            reasons=reasons,
            warnings=warnings,
        )

    if not package_path.exists() or not package_path.is_dir():
        return MetadataIndexBuildResult(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            metadata_index_path=metadata_index_path,
            created=False,
            skipped=True,
            reasons=["package_dir_missing"],
            warnings=warnings,
        )

    manifest_path = package_path / MANIFEST_FILENAME
    if not manifest_path.exists():
        return MetadataIndexBuildResult(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            metadata_index_path=metadata_index_path,
            created=False,
            skipped=True,
            reasons=["package_manifest_missing"],
            warnings=warnings,
        )

    markdown_hash = _sha256_file(markdown_file)
    manifest = _read_manifest(manifest_path)
    if manifest is None:
        return MetadataIndexBuildResult(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            metadata_index_path=metadata_index_path,
            created=False,
            skipped=True,
            reasons=["package_manifest_invalid"],
            warnings=warnings,
        )
    if not _manifest_matches_skeleton(
        manifest=manifest,
        metadata=metadata,
        metadata_path=metadata_file,
        markdown_path=markdown_file,
        package_dir=package_path,
        source_hash=eligibility.source_hash,
    ):
        return MetadataIndexBuildResult(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            metadata_index_path=metadata_index_path,
            created=False,
            skipped=True,
            reasons=["package_manifest_invalid"],
            warnings=warnings,
        )
    if manifest.get("markdown_hash") != markdown_hash:
        return MetadataIndexBuildResult(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            metadata_index_path=metadata_index_path,
            created=False,
            skipped=True,
            reasons=["manifest_markdown_hash_mismatch"],
            warnings=warnings,
        )

    if metadata_index_path.exists() and not overwrite:
        return MetadataIndexBuildResult(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            metadata_index_path=metadata_index_path,
            created=False,
            skipped=True,
            reasons=["metadata_index_exists"],
            warnings=warnings,
        )

    payload, payload_warnings = _build_metadata_index_payload(
        metadata=metadata,
        metadata_path=metadata_file,
        markdown_path=markdown_file,
        markdown_hash=markdown_hash,
        source_hash=eligibility.source_hash,
        eligibility_warnings=warnings,
    )
    _atomic_write_json(metadata_index_path, payload)

    return MetadataIndexBuildResult(
        paper_id=paper_id,
        version=version,
        package_dir=package_path,
        metadata_index_path=metadata_index_path,
        created=True,
        skipped=False,
        reasons=[],
        warnings=payload_warnings,
    )


def _manifest_matches_skeleton(
    *,
    manifest: dict[str, Any],
    metadata: PaperMetadata,
    metadata_path: Path,
    markdown_path: Path,
    package_dir: Path,
    source_hash: str | None,
) -> bool:
    expected = {
        "schema_version": SCHEMA_VERSION,
        "manifest_type": MANIFEST_TYPE,
        "paper_id": metadata.id,
        "version": metadata.version,
        "index_scope": INDEX_SCOPE,
        "source_hash": source_hash or _blank_to_none(metadata.source_hash),
        "metadata_path": str(metadata_path),
        "markdown_path": str(markdown_path),
        "package_path": str(package_dir),
    }
    return all(manifest.get(key) == value for key, value in expected.items())


def _build_metadata_index_payload(
    *,
    metadata: PaperMetadata,
    metadata_path: Path,
    markdown_path: Path,
    markdown_hash: str,
    source_hash: str | None,
    eligibility_warnings: list[str],
) -> tuple[dict[str, Any], list[str]]:
    now = datetime.now(UTC).isoformat()
    warnings = _merge_unique([*eligibility_warnings, *_optional_warnings(metadata)])

    payload = {
        "schema_version": SCHEMA_VERSION,
        "index_type": INDEX_TYPE,
        "paper_id": metadata.id,
        "version": metadata.version,
        "title": _blank_to_none(metadata.title),
        "sanitized_title": _blank_to_none(getattr(metadata, "sanitized_title", None)),
        "category": _blank_to_none(metadata.category),
        "direction": _blank_to_none(metadata.direction),
        "major": _blank_to_none(metadata.major),
        "discipline": _blank_to_none(getattr(metadata, "discipline", None)),
        "thesis_type": _blank_to_none(getattr(metadata, "thesis_type", None)),
        "research_methods": _research_methods(metadata),
        "keywords": _string_list(metadata.keywords),
        "abstract": _blank_to_none(metadata.abstract),
        "abstract_source": _blank_to_none(metadata.abstract_source),
        "keywords_source": _blank_to_none(metadata.keywords_source),
        "tier": _json_value(metadata.tier),
        "quality_tier": _blank_to_none(getattr(metadata, "quality_tier", None)),
        "quality_tier_suggestion": _blank_to_none(metadata.quality_tier_suggestion),
        "quality_reason": _blank_to_none(metadata.quality_reason),
        "sensitive_clean": bool(metadata.sensitive_clean),
        "version_status": _json_value(metadata.version_status),
        "duplicate_status": _json_value(metadata.duplicate_status),
        "index_status": _json_value(metadata.index_status),
        "sql_status": _json_value(metadata.sql_status),
        "source_hash": source_hash or _blank_to_none(metadata.source_hash),
        "markdown_hash": markdown_hash,
        "text_hash": None,
        "blocks_hash": _blank_to_none(getattr(metadata, "blocks_hash", None)),
        "markdown_path": str(markdown_path),
        "metadata_path": str(metadata_path),
        "blocks_path": _blank_to_none(getattr(metadata, "blocks_path", None)),
        "block_count": getattr(metadata, "block_count", None),
        "parser_selected": _blank_to_none(getattr(metadata, "parser_selected", None)),
        "blocks_schema_version": _blank_to_none(
            getattr(metadata, "blocks_schema_version", None),
        ),
        "desensitized_at": _json_value(getattr(metadata, "desensitized_at", None)),
        "index_scope": INDEX_SCOPE,
        "warnings": warnings,
        "created_at": now,
        "updated_at": now,
    }
    return payload, warnings


def _optional_warnings(metadata: PaperMetadata) -> list[str]:
    warnings: list[str] = []
    if _blank_to_none(getattr(metadata, "quality_tier", None)) is None and (
        _blank_to_none(metadata.quality_tier_suggestion) is None
    ):
        warnings.append("quality_tier_missing_warning")
    if _blank_to_none(metadata.abstract) is None:
        warnings.append("abstract_missing_warning")
    if not _string_list(metadata.keywords):
        warnings.append("keywords_missing_warning")
    if not _research_methods(metadata):
        warnings.append("research_methods_missing_warning")
    if (
        _blank_to_none(getattr(metadata, "blocks_hash", None)) is None
        and _blank_to_none(getattr(metadata, "blocks_path", None)) is None
    ):
        warnings.append("blocks_missing_warning")
    return warnings


def _research_methods(metadata: PaperMetadata) -> list[str]:
    methods: list[str] = []
    primary = _blank_to_none(metadata.research_method_primary)
    if primary is not None:
        methods.append(primary)
    for tag in metadata.research_method_tags:
        normalized = _blank_to_none(tag)
        if normalized is not None and normalized not in methods:
            methods.append(normalized)
    return methods


def _string_list(values: list[str]) -> list[str]:
    normalized_values: list[str] = []
    for value in values:
        normalized = _blank_to_none(value)
        if normalized is not None:
            normalized_values.append(normalized)
    return normalized_values


def _read_manifest(path: Path) -> dict[str, Any] | None:
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(decoded, dict):
        return None
    return decoded


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    tmp_path = path.with_name(_TMP_METADATA_INDEX_FILENAME)
    encoded = json.dumps(payload, ensure_ascii=False, indent=2)
    tmp_path.write_text(f"{encoded}\n", encoding="utf-8")
    tmp_path.replace(path)


def _blank_to_none(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _merge_unique(items: list[str]) -> list[str]:
    merged: list[str] = []
    for item in items:
        if item and item not in merged:
            merged.append(item)
    return merged


def _json_value(value: object) -> object:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    return value
