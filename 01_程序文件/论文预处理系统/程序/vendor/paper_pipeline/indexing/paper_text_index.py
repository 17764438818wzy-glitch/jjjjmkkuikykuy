"""S6-1 paper text index writer for local index packages."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

from paper_pipeline.domain import PaperMetadata
from paper_pipeline.eligibility import EligibilityScope, check_formal_candidate
from paper_pipeline.indexing.metadata_index import METADATA_INDEX_FILENAME
from paper_pipeline.indexing.package_skeleton import (
    MANIFEST_FILENAME,
    MANIFEST_TYPE,
    SCHEMA_VERSION,
    _sha256_file,
    build_package_dir_name,
)

INDEX_TYPE = "paper_text_index"
INDEX_SCOPE = "formal_candidate"
PAPER_TEXT_INDEX_FILENAME = "paper_text_index.json"
_TMP_PAPER_TEXT_INDEX_FILENAME = ".paper_text_index.json.tmp"
_METADATA_INDEX_TYPE = "metadata_index"


@dataclass(frozen=True)
class PaperTextIndexBuildResult:
    paper_id: str
    version: int
    package_dir: Path
    paper_text_index_path: Path
    created: bool
    skipped: bool
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def build_paper_text_index(
    metadata: PaperMetadata,
    metadata_path: Path,
    markdown_path: Path,
    package_dir: Path,
    *,
    overwrite: bool = False,
) -> PaperTextIndexBuildResult:
    metadata_file = Path(metadata_path)
    markdown_file = Path(markdown_path)
    package_path = Path(package_dir)

    paper_id = str(metadata.id)
    version = metadata.version
    build_package_dir_name(paper_id, version)
    paper_text_index_path = package_path / PAPER_TEXT_INDEX_FILENAME

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
            paper_text_index_path=paper_text_index_path,
            reasons=reasons,
            warnings=warnings,
        )

    if not package_path.exists() or not package_path.is_dir():
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            paper_text_index_path=paper_text_index_path,
            reasons=["package_dir_missing"],
            warnings=warnings,
        )

    manifest_path = package_path / MANIFEST_FILENAME
    if not manifest_path.exists():
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            paper_text_index_path=paper_text_index_path,
            reasons=["package_manifest_missing"],
            warnings=warnings,
        )

    metadata_index_path = package_path / METADATA_INDEX_FILENAME
    if not metadata_index_path.exists():
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            paper_text_index_path=paper_text_index_path,
            reasons=["metadata_index_missing"],
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
            paper_text_index_path=paper_text_index_path,
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
            paper_text_index_path=paper_text_index_path,
            reasons=["package_manifest_invalid"],
            warnings=warnings,
        )
    if manifest.get("markdown_hash") != markdown_hash:
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            paper_text_index_path=paper_text_index_path,
            reasons=["manifest_markdown_hash_mismatch"],
            warnings=warnings,
        )

    metadata_index = _read_json_object(metadata_index_path)
    if metadata_index is None:
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            paper_text_index_path=paper_text_index_path,
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
            paper_text_index_path=paper_text_index_path,
            reasons=["metadata_index_invalid"],
            warnings=warnings,
        )
    if metadata_index.get("markdown_hash") != markdown_hash:
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            paper_text_index_path=paper_text_index_path,
            reasons=["metadata_index_markdown_hash_mismatch"],
            warnings=warnings,
        )

    if paper_text_index_path.exists() and not overwrite:
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            paper_text_index_path=paper_text_index_path,
            reasons=["paper_text_index_exists"],
            warnings=warnings,
        )

    markdown_text = markdown_file.read_text(encoding="utf-8")
    retrieval_text = build_retrieval_text(metadata, markdown_text)
    payload_warnings = warnings
    if not retrieval_text:
        payload_warnings = _merge_unique([*warnings, "retrieval_text_empty_warning"])
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            paper_text_index_path=paper_text_index_path,
            reasons=["retrieval_text_empty"],
            warnings=payload_warnings,
        )

    payload = _build_payload(
        metadata=metadata,
        metadata_path=metadata_file,
        markdown_path=markdown_file,
        markdown_hash=markdown_hash,
        retrieval_text=retrieval_text,
        warnings=payload_warnings,
    )
    _atomic_write_json(paper_text_index_path, payload)

    return PaperTextIndexBuildResult(
        paper_id=paper_id,
        version=version,
        package_dir=package_path,
        paper_text_index_path=paper_text_index_path,
        created=True,
        skipped=False,
        reasons=[],
        warnings=payload_warnings,
    )


def markdown_to_plain_text(markdown: str) -> str:
    text = str(markdown or "")
    text = text.replace("\ufeff", "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    text = re.sub(r"!\[([^\]]*)\]\([^)]+\)", r"\1", text)
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    text = re.sub(r"(?m)^\s{0,3}```[^\n]*\n?", "", text)
    text = re.sub(r"(?m)^\s{0,3}```\s*$", "", text)
    text = re.sub(r"`([^`]*)`", r"\1", text)
    text = re.sub(r"(?m)^\s{0,3}#{1,6}\s*", "", text)
    text = re.sub(r"(?m)^\s{0,3}>\s?", "", text)
    text = re.sub(r"(?m)^\s*(?:[-+*]|\d+[.)])\s+", "", text)
    text = re.sub(r"(?m)^\s*:?-{3,}:?(?:\s+:?-{3,}:?)*\s*$", " ", text)
    text = text.replace("|", " ")
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[*_~]+", "", text)
    return _normalize_whitespace(text)


def build_retrieval_text(metadata: PaperMetadata, markdown_text: str) -> str:
    parts = [
        _blank_to_none(getattr(metadata, "title", None)),
        _blank_to_none(getattr(metadata, "abstract", None)),
        _keywords_text(getattr(metadata, "keywords", [])),
        markdown_to_plain_text(markdown_text),
    ]
    return _normalize_whitespace(" ".join(part for part in parts if part))


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


def _build_payload(
    *,
    metadata: PaperMetadata,
    metadata_path: Path,
    markdown_path: Path,
    markdown_hash: str,
    retrieval_text: str,
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "index_type": INDEX_TYPE,
        "paper_id": metadata.id,
        "version": metadata.version,
        "title": _blank_to_none(metadata.title),
        "abstract": _blank_to_none(metadata.abstract),
        "keywords": _keywords_list(getattr(metadata, "keywords", [])),
        "category": _blank_to_none(metadata.category),
        "direction": _blank_to_none(metadata.direction),
        "tier": _json_value(metadata.tier),
        "markdown_path": str(markdown_path),
        "metadata_path": str(metadata_path),
        "markdown_hash": markdown_hash,
        "text_hash": hashlib.sha256(retrieval_text.encode("utf-8")).hexdigest(),
        "retrieval_text": retrieval_text,
        "search_vector_ready": False,
        "updated_at": datetime.now(UTC).isoformat(),
        "warnings": warnings,
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
    tmp_path = path.with_name(_TMP_PAPER_TEXT_INDEX_FILENAME)
    encoded = json.dumps(payload, ensure_ascii=False, indent=2)
    tmp_path.write_text(f"{encoded}\n", encoding="utf-8")
    tmp_path.replace(path)


def _result(
    *,
    paper_id: str,
    version: int,
    package_dir: Path,
    paper_text_index_path: Path,
    reasons: list[str],
    warnings: list[str],
) -> PaperTextIndexBuildResult:
    return PaperTextIndexBuildResult(
        paper_id=paper_id,
        version=version,
        package_dir=package_dir,
        paper_text_index_path=paper_text_index_path,
        created=False,
        skipped=True,
        reasons=reasons,
        warnings=warnings,
    )


def _keywords_text(values: object) -> str | None:
    if isinstance(values, str):
        return _blank_to_none(values)
    return _blank_to_none(" ".join(_keywords_list(values)))


def _keywords_list(values: object) -> list[str]:
    if isinstance(values, str):
        return [normalized] if (normalized := _blank_to_none(values)) else []
    if not isinstance(values, (list, tuple, set)):
        return []
    normalized_values: list[str] = []
    for value in values:
        normalized = _blank_to_none(value)
        if normalized is not None:
            normalized_values.append(normalized)
    return normalized_values


def _blank_to_none(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


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
