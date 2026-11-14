"""S8-1 hierarchy index writer for local index packages."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from paper_pipeline.domain import PaperMetadata
from paper_pipeline.eligibility import EligibilityScope, check_formal_candidate
from paper_pipeline.indexing.chapter_index import CHAPTER_INDEX_FILENAME
from paper_pipeline.indexing.metadata_index import METADATA_INDEX_FILENAME
from paper_pipeline.indexing.package_skeleton import (
    MANIFEST_FILENAME,
    MANIFEST_TYPE,
    SCHEMA_VERSION,
    _sha256_file,
    build_package_dir_name,
)
from paper_pipeline.indexing.paper_text_index import PAPER_TEXT_INDEX_FILENAME

INDEX_TYPE = "hierarchy_index"
INDEX_SCOPE = "formal_candidate"
HIERARCHY_TYPE = "paper_chapter"
HIERARCHY_INDEX_FILENAME = "hierarchy_index.json"
_TMP_HIERARCHY_INDEX_FILENAME = ".hierarchy_index.json.tmp"
_METADATA_INDEX_TYPE = "metadata_index"
_PAPER_TEXT_INDEX_TYPE = "paper_text_index"
_CHAPTER_INDEX_TYPE = "chapter_index"
_CHAPTER_ID_MISSING_WARNING = "chapter_id_missing_warning"
_CHAPTER_ORDER_NORMALIZED_WARNING = "chapter_order_normalized_warning"


@dataclass(frozen=True)
class HierarchyIndexBuildResult:
    paper_id: str
    version: int
    package_dir: Path
    hierarchy_index_path: Path
    created: bool
    skipped: bool
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class _ChapterRelationSource:
    chapter_id: str
    chapter_order: int
    source_position: int


def build_hierarchy_index(
    metadata: PaperMetadata,
    metadata_path: Path,
    markdown_path: Path,
    package_dir: Path,
    *,
    overwrite: bool = False,
) -> HierarchyIndexBuildResult:
    metadata_file = Path(metadata_path)
    markdown_file = Path(markdown_path)
    package_path = Path(package_dir)

    paper_id = str(metadata.id)
    version = metadata.version
    build_package_dir_name(paper_id, version)
    hierarchy_index_path = package_path / HIERARCHY_INDEX_FILENAME

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
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            hierarchy_index_path=hierarchy_index_path,
            reasons=reasons,
            warnings=warnings,
        )

    if not package_path.exists() or not package_path.is_dir():
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            hierarchy_index_path=hierarchy_index_path,
            reasons=["package_dir_missing"],
            warnings=warnings,
        )

    manifest_path = package_path / MANIFEST_FILENAME
    if not manifest_path.exists():
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            hierarchy_index_path=hierarchy_index_path,
            reasons=["package_manifest_missing"],
            warnings=warnings,
        )

    metadata_index_path = package_path / METADATA_INDEX_FILENAME
    if not metadata_index_path.exists():
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            hierarchy_index_path=hierarchy_index_path,
            reasons=["metadata_index_missing"],
            warnings=warnings,
        )

    paper_text_index_path = package_path / PAPER_TEXT_INDEX_FILENAME
    if not paper_text_index_path.exists():
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            hierarchy_index_path=hierarchy_index_path,
            reasons=["paper_text_index_missing"],
            warnings=warnings,
        )

    chapter_index_path = package_path / CHAPTER_INDEX_FILENAME
    if not chapter_index_path.exists():
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            hierarchy_index_path=hierarchy_index_path,
            reasons=["chapter_index_missing"],
            warnings=warnings,
        )

    markdown_hash = _sha256_file(markdown_file)
    source_hash = eligibility.source_hash or _blank_to_none(metadata.source_hash)

    manifest = _read_json_object(manifest_path)
    if manifest is None:
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            hierarchy_index_path=hierarchy_index_path,
            reasons=["package_manifest_invalid"],
            warnings=warnings,
        )
    if not _manifest_matches_skeleton(
        manifest=manifest,
        metadata=metadata,
        metadata_path=metadata_file,
        markdown_path=markdown_file,
        package_dir=package_path,
        source_hash=source_hash,
    ):
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            hierarchy_index_path=hierarchy_index_path,
            reasons=["package_manifest_invalid"],
            warnings=warnings,
        )
    if manifest.get("markdown_hash") != markdown_hash:
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            hierarchy_index_path=hierarchy_index_path,
            reasons=["manifest_markdown_hash_mismatch"],
            warnings=warnings,
        )

    metadata_index = _read_json_object(metadata_index_path)
    if metadata_index is None:
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            hierarchy_index_path=hierarchy_index_path,
            reasons=["metadata_index_invalid"],
            warnings=warnings,
        )
    if not _metadata_index_matches(
        metadata_index=metadata_index,
        metadata=metadata,
        metadata_path=metadata_file,
        markdown_path=markdown_file,
        source_hash=source_hash,
    ):
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            hierarchy_index_path=hierarchy_index_path,
            reasons=["metadata_index_invalid"],
            warnings=warnings,
        )
    if metadata_index.get("markdown_hash") != markdown_hash:
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            hierarchy_index_path=hierarchy_index_path,
            reasons=["metadata_index_markdown_hash_mismatch"],
            warnings=warnings,
        )

    paper_text_index = _read_json_object(paper_text_index_path)
    if paper_text_index is None:
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            hierarchy_index_path=hierarchy_index_path,
            reasons=["paper_text_index_invalid"],
            warnings=warnings,
        )
    if not _paper_text_index_matches(
        paper_text_index=paper_text_index,
        metadata=metadata,
        metadata_path=metadata_file,
        markdown_path=markdown_file,
        markdown_hash=markdown_hash,
    ):
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            hierarchy_index_path=hierarchy_index_path,
            reasons=["paper_text_index_invalid"],
            warnings=warnings,
        )
    text_hash = _blank_to_none(paper_text_index.get("text_hash"))
    if text_hash is None:
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            hierarchy_index_path=hierarchy_index_path,
            reasons=["paper_text_index_text_hash_missing"],
            warnings=warnings,
        )

    chapter_index = _read_json_object(chapter_index_path)
    if chapter_index is None:
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            hierarchy_index_path=hierarchy_index_path,
            reasons=["chapter_index_invalid"],
            warnings=warnings,
        )
    if not _chapter_index_matches(
        chapter_index=chapter_index,
        metadata=metadata,
        metadata_path=metadata_file,
        markdown_path=markdown_file,
        markdown_hash=markdown_hash,
    ):
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            hierarchy_index_path=hierarchy_index_path,
            reasons=["chapter_index_invalid"],
            warnings=warnings,
        )
    if chapter_index.get("text_hash") != text_hash:
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            hierarchy_index_path=hierarchy_index_path,
            reasons=["chapter_index_text_hash_mismatch"],
            warnings=warnings,
        )

    chapters = chapter_index.get("chapters")
    if not isinstance(chapters, list):
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            hierarchy_index_path=hierarchy_index_path,
            reasons=["chapter_index_invalid"],
            warnings=warnings,
        )
    if not chapters:
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            hierarchy_index_path=hierarchy_index_path,
            reasons=["chapter_index_empty"],
            warnings=warnings,
        )

    if hierarchy_index_path.exists() and not overwrite:
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            hierarchy_index_path=hierarchy_index_path,
            reasons=["hierarchy_index_exists"],
            warnings=warnings,
        )

    relation_sources, relation_warnings = _extract_relation_sources(chapters)
    payload_warnings = _merge_unique([*warnings, *relation_warnings])
    if not relation_sources:
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            hierarchy_index_path=hierarchy_index_path,
            reasons=["no_valid_chapters"],
            warnings=payload_warnings,
        )

    relation_sources = sorted(
        relation_sources,
        key=lambda item: (item.chapter_order, item.source_position),
    )
    chapter_ids = [chapter.chapter_id for chapter in relation_sources]
    chapter_order = [chapter.chapter_order for chapter in relation_sources]
    relations = [
        {
            "relation_type": "contains",
            "parent_type": "paper",
            "parent_id": paper_id,
            "child_type": "chapter",
            "child_id": chapter.chapter_id,
            "child_order": chapter.chapter_order,
        }
        for chapter in relation_sources
    ]

    payload = _build_payload(
        metadata=metadata,
        metadata_path=metadata_file,
        markdown_path=markdown_file,
        markdown_hash=markdown_hash,
        text_hash=text_hash,
        chapter_ids=chapter_ids,
        chapter_order=chapter_order,
        relations=relations,
        warnings=payload_warnings,
    )
    _atomic_write_json(hierarchy_index_path, payload)

    return HierarchyIndexBuildResult(
        paper_id=paper_id,
        version=version,
        package_dir=package_path,
        hierarchy_index_path=hierarchy_index_path,
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
        "source_hash": source_hash,
        "metadata_path": str(metadata_path),
        "markdown_path": str(markdown_path),
        "package_path": str(package_dir),
    }
    return all(manifest.get(key) == value for key, value in expected.items())


def _metadata_index_matches(
    *,
    metadata_index: dict[str, Any],
    metadata: PaperMetadata,
    metadata_path: Path,
    markdown_path: Path,
    source_hash: str | None,
) -> bool:
    expected = {
        "schema_version": SCHEMA_VERSION,
        "index_type": _METADATA_INDEX_TYPE,
        "paper_id": metadata.id,
        "version": metadata.version,
        "source_hash": source_hash,
        "metadata_path": str(metadata_path),
        "markdown_path": str(markdown_path),
    }
    return all(metadata_index.get(key) == value for key, value in expected.items())


def _paper_text_index_matches(
    *,
    paper_text_index: dict[str, Any],
    metadata: PaperMetadata,
    metadata_path: Path,
    markdown_path: Path,
    markdown_hash: str,
) -> bool:
    expected = {
        "schema_version": SCHEMA_VERSION,
        "index_type": _PAPER_TEXT_INDEX_TYPE,
        "paper_id": metadata.id,
        "version": metadata.version,
        "metadata_path": str(metadata_path),
        "markdown_path": str(markdown_path),
        "markdown_hash": markdown_hash,
    }
    return all(paper_text_index.get(key) == value for key, value in expected.items())


def _chapter_index_matches(
    *,
    chapter_index: dict[str, Any],
    metadata: PaperMetadata,
    metadata_path: Path,
    markdown_path: Path,
    markdown_hash: str,
) -> bool:
    expected = {
        "schema_version": SCHEMA_VERSION,
        "index_type": _CHAPTER_INDEX_TYPE,
        "paper_id": metadata.id,
        "version": metadata.version,
        "index_scope": INDEX_SCOPE,
        "metadata_path": str(metadata_path),
        "markdown_path": str(markdown_path),
        "markdown_hash": markdown_hash,
    }
    return all(chapter_index.get(key) == value for key, value in expected.items())


def _extract_relation_sources(chapters: list[object]) -> tuple[list[_ChapterRelationSource], list[str]]:
    raw_sources: list[tuple[str, object, int]] = []
    warnings: list[str] = []
    order_needs_normalization = False

    for source_position, chapter in enumerate(chapters, start=1):
        if not isinstance(chapter, dict):
            warnings = _merge_unique([*warnings, _CHAPTER_ID_MISSING_WARNING])
            continue
        chapter_id = _blank_to_none(chapter.get("chapter_id"))
        if chapter_id is None:
            warnings = _merge_unique([*warnings, _CHAPTER_ID_MISSING_WARNING])
            continue
        raw_order = chapter.get("chapter_order")
        if not _is_int(raw_order):
            order_needs_normalization = True
        raw_sources.append((chapter_id, raw_order, source_position))

    if order_needs_normalization:
        warnings = _merge_unique([*warnings, _CHAPTER_ORDER_NORMALIZED_WARNING])
        return (
            [
                _ChapterRelationSource(
                    chapter_id=chapter_id,
                    chapter_order=normalized_order,
                    source_position=source_position,
                )
                for normalized_order, (chapter_id, _raw_order, source_position) in enumerate(
                    raw_sources,
                    start=1,
                )
            ],
            warnings,
        )

    return (
        [
            _ChapterRelationSource(
                chapter_id=chapter_id,
                chapter_order=int(raw_order),
                source_position=source_position,
            )
            for chapter_id, raw_order, source_position in raw_sources
        ],
        warnings,
    )


def _build_payload(
    *,
    metadata: PaperMetadata,
    metadata_path: Path,
    markdown_path: Path,
    markdown_hash: str,
    text_hash: str,
    chapter_ids: list[str],
    chapter_order: list[int],
    relations: list[dict[str, object]],
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "index_type": INDEX_TYPE,
        "paper_id": metadata.id,
        "version": metadata.version,
        "index_scope": INDEX_SCOPE,
        "hierarchy_type": HIERARCHY_TYPE,
        "markdown_path": str(markdown_path),
        "metadata_path": str(metadata_path),
        "markdown_hash": markdown_hash,
        "text_hash": text_hash,
        "chapter_ids": chapter_ids,
        "chapter_order": chapter_order,
        "relations": relations,
        "warnings": warnings,
        "updated_at": datetime.now(UTC).isoformat(),
    }


def _read_json_object(path: Path) -> dict[str, Any] | None:
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(decoded, dict):
        return None
    return decoded


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    tmp_path = path.with_name(_TMP_HIERARCHY_INDEX_FILENAME)
    encoded = json.dumps(payload, ensure_ascii=False, indent=2)
    tmp_path.write_text(f"{encoded}\n", encoding="utf-8")
    tmp_path.replace(path)


def _result(
    *,
    paper_id: str,
    version: int,
    package_dir: Path,
    hierarchy_index_path: Path,
    reasons: list[str],
    warnings: list[str],
) -> HierarchyIndexBuildResult:
    return HierarchyIndexBuildResult(
        paper_id=paper_id,
        version=version,
        package_dir=package_dir,
        hierarchy_index_path=hierarchy_index_path,
        created=False,
        skipped=True,
        reasons=reasons,
        warnings=warnings,
    )


def _blank_to_none(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _merge_unique(items: list[str]) -> list[str]:
    merged: list[str] = []
    for item in items:
        if item and item not in merged:
            merged.append(item)
    return merged
