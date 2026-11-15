"""Local index package skeleton writer for S4-1."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

from paper_pipeline.domain import PaperMetadata
from paper_pipeline.eligibility import EligibilityScope, check_formal_candidate

SCHEMA_VERSION = "1.0"
MANIFEST_TYPE = "local_index_package_skeleton"
INDEX_SCOPE = "formal_candidate"
MANIFEST_FILENAME = "manifest.json"
_TMP_MANIFEST_FILENAME = ".manifest.json.tmp"
_PENDING_WARNINGS = (
    "metadata_index_pending_s5",
    "paper_text_index_pending_s6",
    "chapter_index_pending_s7",
    "hierarchy_index_pending_s8",
)


@dataclass(frozen=True)
class LocalIndexPackageSkeletonResult:
    paper_id: str
    version: int
    package_dir: Path
    manifest_path: Path
    created: bool
    skipped: bool
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def build_package_dir_name(paper_id: str, version: int) -> str:
    normalized_paper_id = str(paper_id or "").strip()
    if (
        not normalized_paper_id
        or "/" in normalized_paper_id
        or "\\" in normalized_paper_id
        or ".." in normalized_paper_id
    ):
        raise ValueError("invalid_paper_id")
    if not isinstance(version, int) or isinstance(version, bool) or version <= 0:
        raise ValueError("invalid_version")
    return f"{normalized_paper_id}_v{version}"


def build_local_index_package_skeleton(
    metadata: PaperMetadata,
    metadata_path: Path,
    markdown_path: Path,
    indexes_root: Path,
    *,
    overwrite: bool = False,
) -> LocalIndexPackageSkeletonResult:
    metadata_file = Path(metadata_path)
    markdown_file = Path(markdown_path)
    indexes_root_path = Path(indexes_root)

    paper_id = str(metadata.id)
    version = metadata.version
    package_dir_name = build_package_dir_name(paper_id, version)
    package_dir = indexes_root_path / package_dir_name
    _ensure_under_root(package_dir, indexes_root_path)
    manifest_path = package_dir / MANIFEST_FILENAME

    eligibility = check_formal_candidate(
        metadata,
        metadata_path=metadata_file,
        markdown_path=markdown_file,
    )
    warnings = _merge_unique([*_PENDING_WARNINGS, *eligibility.warnings])

    if not (
        eligibility.eligible
        and eligibility.scope is EligibilityScope.FORMAL_CANDIDATE
    ):
        reasons = _merge_unique(["not_formal_candidate", *eligibility.blocking_reasons])
        return LocalIndexPackageSkeletonResult(
            paper_id=paper_id,
            version=version,
            package_dir=package_dir,
            manifest_path=manifest_path,
            created=False,
            skipped=True,
            reasons=reasons,
            warnings=warnings,
        )

    if manifest_path.exists() and not overwrite:
        return LocalIndexPackageSkeletonResult(
            paper_id=paper_id,
            version=version,
            package_dir=package_dir,
            manifest_path=manifest_path,
            created=False,
            skipped=True,
            reasons=["package_manifest_exists"],
            warnings=warnings,
        )

    package_dir.mkdir(parents=True, exist_ok=True)
    manifest = _build_manifest(
        metadata=metadata,
        metadata_path=metadata_file,
        markdown_path=markdown_file,
        package_dir=package_dir,
        warnings=warnings,
        source_hash=eligibility.source_hash,
    )
    _atomic_write_json(manifest_path, manifest)

    return LocalIndexPackageSkeletonResult(
        paper_id=paper_id,
        version=version,
        package_dir=package_dir,
        manifest_path=manifest_path,
        created=True,
        skipped=False,
        reasons=[],
        warnings=warnings,
    )


def _build_manifest(
    *,
    metadata: PaperMetadata,
    metadata_path: Path,
    markdown_path: Path,
    package_dir: Path,
    warnings: list[str],
    source_hash: str | None,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "manifest_type": MANIFEST_TYPE,
        "paper_id": metadata.id,
        "version": metadata.version,
        "index_scope": INDEX_SCOPE,
        "index_status": _json_value(metadata.index_status),
        "built_at": datetime.now(UTC).isoformat(),
        "source_hash": source_hash or metadata.source_hash,
        "markdown_hash": _sha256_file(markdown_path),
        "text_hash": None,
        "blocks_hash": getattr(metadata, "blocks_hash", None),
        "warnings": warnings,
        "published_to_postgres": False,
        "published_at": None,
        "remote_resource_id": None,
        "upload_batch_id": None,
        "postgres_manifest_id": None,
        "metadata_path": str(metadata_path),
        "markdown_path": str(markdown_path),
        "package_path": str(package_dir),
    }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    tmp_path = path.with_name(_TMP_MANIFEST_FILENAME)
    encoded = json.dumps(payload, ensure_ascii=False, indent=2)
    tmp_path.write_text(f"{encoded}\n", encoding="utf-8")
    tmp_path.replace(path)


def _ensure_under_root(path: Path, root: Path) -> None:
    resolved_path = path.resolve(strict=False)
    resolved_root = root.resolve(strict=False)
    try:
        resolved_path.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError("package_path_outside_indexes_root") from exc


def _merge_unique(items: list[str]) -> list[str]:
    merged: list[str] = []
    for item in items:
        if item and item not in merged:
            merged.append(item)
    return merged


def _json_value(value: object) -> object:
    if isinstance(value, Enum):
        return value.value
    return value
