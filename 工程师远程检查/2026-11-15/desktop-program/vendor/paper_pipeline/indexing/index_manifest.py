"""S9-1 local index package manifest finalization."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from paper_pipeline.indexing.package_skeleton import (
    MANIFEST_FILENAME,
    MANIFEST_TYPE as SKELETON_MANIFEST_TYPE,
    SCHEMA_VERSION,
)

LOCAL_INDEX_PACKAGE_MANIFEST_TYPE = "local_index_package"
GLOBAL_INDEX_MANIFEST_TYPE = "global_index_manifest"
INDEX_MANIFEST_FILENAME = "index_manifest.json"
INDEX_SCOPE = "formal_candidate"

METADATA_INDEX_FILENAME = "metadata_index.json"
PAPER_TEXT_INDEX_FILENAME = "paper_text_index.json"
CHAPTER_INDEX_FILENAME = "chapter_index.json"
HIERARCHY_INDEX_FILENAME = "hierarchy_index.json"

_TMP_MANIFEST_FILENAME = ".manifest.json.tmp"
_TMP_INDEX_MANIFEST_FILENAME = ".index_manifest.json.tmp"

_REQUIRED_FILES = [
    MANIFEST_FILENAME,
    METADATA_INDEX_FILENAME,
    PAPER_TEXT_INDEX_FILENAME,
    CHAPTER_INDEX_FILENAME,
    HIERARCHY_INDEX_FILENAME,
]

_INDEX_TYPES = {
    METADATA_INDEX_FILENAME: "metadata_index",
    PAPER_TEXT_INDEX_FILENAME: "paper_text_index",
    CHAPTER_INDEX_FILENAME: "chapter_index",
    HIERARCHY_INDEX_FILENAME: "hierarchy_index",
}

_MISSING_REASONS = {
    MANIFEST_FILENAME: "package_manifest_missing",
    METADATA_INDEX_FILENAME: "metadata_index_missing",
    PAPER_TEXT_INDEX_FILENAME: "paper_text_index_missing",
    CHAPTER_INDEX_FILENAME: "chapter_index_missing",
    HIERARCHY_INDEX_FILENAME: "hierarchy_index_missing",
}

_INVALID_REASONS = {
    MANIFEST_FILENAME: "package_manifest_invalid",
    METADATA_INDEX_FILENAME: "metadata_index_invalid",
    PAPER_TEXT_INDEX_FILENAME: "paper_text_index_invalid",
    CHAPTER_INDEX_FILENAME: "chapter_index_invalid",
    HIERARCHY_INDEX_FILENAME: "hierarchy_index_invalid",
}


@dataclass(frozen=True)
class PackageManifestFinalizeResult:
    paper_id: str | None
    version: int | None
    package_dir: Path
    manifest_path: Path
    completed: bool
    skipped: bool
    failed: bool
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class IndexManifestBuildResult:
    indexes_root: Path
    index_manifest_path: Path
    run_id: str
    total_count: int
    success_count: int
    skipped_count: int
    failed_count: int
    created: bool
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class _PackageValidation:
    paper_id: str | None
    version: int | None
    manifest: dict[str, Any] | None
    reasons: list[str]
    warnings: list[str]
    source_hash: str | None
    markdown_hash: str | None
    text_hash: str | None


def finalize_package_manifest(
    package_dir: Path,
    *,
    overwrite: bool = False,
) -> PackageManifestFinalizeResult:
    package_path = Path(package_dir)
    manifest_path = package_path / MANIFEST_FILENAME
    validation = _validate_package(package_path)

    if validation.reasons:
        return PackageManifestFinalizeResult(
            paper_id=validation.paper_id,
            version=validation.version,
            package_dir=package_path,
            manifest_path=manifest_path,
            completed=False,
            skipped=False,
            failed=True,
            reasons=validation.reasons,
            warnings=validation.warnings,
        )

    if (
        validation.manifest is not None
        and validation.manifest.get("package_complete") is True
        and not overwrite
    ):
        if _finalized_manifest_incomplete(
            package_dir=package_path,
            manifest=validation.manifest,
            text_hash=validation.text_hash,
        ):
            return PackageManifestFinalizeResult(
                paper_id=validation.paper_id,
                version=validation.version,
                package_dir=package_path,
                manifest_path=manifest_path,
                completed=False,
                skipped=False,
                failed=True,
                reasons=["package_manifest_incomplete"],
                warnings=validation.warnings,
            )
        return PackageManifestFinalizeResult(
            paper_id=validation.paper_id,
            version=validation.version,
            package_dir=package_path,
            manifest_path=manifest_path,
            completed=False,
            skipped=True,
            failed=False,
            reasons=["package_manifest_already_complete"],
            warnings=validation.warnings,
        )

    finalized = _finalized_manifest(
        package_dir=package_path,
        manifest=validation.manifest or {},
        completed_at=_utc_now_iso(),
        warnings=validation.warnings,
        text_hash=validation.text_hash,
    )
    _atomic_write_json(manifest_path, finalized, tmp_filename=_TMP_MANIFEST_FILENAME)

    return PackageManifestFinalizeResult(
        paper_id=validation.paper_id,
        version=validation.version,
        package_dir=package_path,
        manifest_path=manifest_path,
        completed=True,
        skipped=False,
        failed=False,
        reasons=[],
        warnings=validation.warnings,
    )


def build_global_index_manifest(
    indexes_root: Path,
    *,
    overwrite: bool = False,
    run_id: str | None = None,
) -> IndexManifestBuildResult:
    indexes_root_path = Path(indexes_root)
    index_manifest_path = indexes_root_path / INDEX_MANIFEST_FILENAME
    effective_run_id = run_id or f"index-manifest-{_utc_now_iso()}"

    if index_manifest_path.exists() and not overwrite:
        return IndexManifestBuildResult(
            indexes_root=indexes_root_path,
            index_manifest_path=index_manifest_path,
            run_id=effective_run_id,
            total_count=0,
            success_count=0,
            skipped_count=0,
            failed_count=0,
            created=False,
            warnings=["global_index_manifest_exists"],
        )

    started_at = _utc_now_iso()
    package_dirs = _package_dirs(indexes_root_path)
    indexed_packages: list[dict[str, Any]] = []
    skipped_items: list[dict[str, Any]] = []
    failed_items: list[dict[str, Any]] = []
    warnings: list[str] = []

    for package_path in package_dirs:
        validation = _validate_package(package_path)
        if validation.reasons:
            failed_items.append(
                _item_record(
                    package_path=package_path,
                    reasons=validation.reasons,
                    warnings=validation.warnings,
                ),
            )
            continue

        if validation.manifest is None:
            failed_items.append(
                _item_record(
                    package_path=package_path,
                    reasons=["package_manifest_invalid"],
                    warnings=validation.warnings,
                ),
            )
            continue

        if validation.manifest.get("package_complete") is not True:
            finalize_result = finalize_package_manifest(package_path)
            if finalize_result.failed:
                failed_items.append(
                    _item_record(
                        package_path=package_path,
                        reasons=finalize_result.reasons,
                        warnings=finalize_result.warnings,
                    ),
                )
                continue
            if finalize_result.skipped and not finalize_result.completed:
                skipped_items.append(
                    _item_record(
                        package_path=package_path,
                        reasons=finalize_result.reasons,
                        warnings=finalize_result.warnings,
                    ),
                )
                continue
            manifest = _read_json_object(package_path / MANIFEST_FILENAME) or {}
        else:
            if _finalized_manifest_incomplete(
                package_dir=package_path,
                manifest=validation.manifest,
                text_hash=validation.text_hash,
            ):
                failed_items.append(
                    _item_record(
                        package_path=package_path,
                        reasons=["package_manifest_incomplete"],
                        warnings=validation.warnings,
                    ),
                )
                continue
            manifest = validation.manifest

        indexed_packages.append(_indexed_package_record(package_path, manifest))

    completed_at = _utc_now_iso()
    payload = {
        "schema_version": SCHEMA_VERSION,
        "manifest_type": GLOBAL_INDEX_MANIFEST_TYPE,
        "run_id": effective_run_id,
        "started_at": started_at,
        "completed_at": completed_at,
        "total_count": len(package_dirs),
        "success_count": len(indexed_packages),
        "skipped_count": len(skipped_items),
        "failed_count": len(failed_items),
        "indexed_packages": indexed_packages,
        "skipped_items": skipped_items,
        "failed_items": failed_items,
        "warnings": warnings,
    }
    indexes_root_path.mkdir(parents=True, exist_ok=True)
    _atomic_write_json(
        index_manifest_path,
        payload,
        tmp_filename=_TMP_INDEX_MANIFEST_FILENAME,
    )

    return IndexManifestBuildResult(
        indexes_root=indexes_root_path,
        index_manifest_path=index_manifest_path,
        run_id=effective_run_id,
        total_count=len(package_dirs),
        success_count=len(indexed_packages),
        skipped_count=len(skipped_items),
        failed_count=len(failed_items),
        created=True,
        warnings=warnings,
    )


def _validate_package(package_dir: Path) -> _PackageValidation:
    manifest_path = package_dir / MANIFEST_FILENAME
    manifest = _read_json_object(manifest_path)
    reasons: list[str] = []
    warnings: list[str] = []

    if not manifest_path.exists():
        return _PackageValidation(
            paper_id=None,
            version=None,
            manifest=None,
            reasons=[_MISSING_REASONS[MANIFEST_FILENAME]],
            warnings=[],
            source_hash=None,
            markdown_hash=None,
            text_hash=None,
        )
    if manifest is None:
        return _PackageValidation(
            paper_id=None,
            version=None,
            manifest=None,
            reasons=[_INVALID_REASONS[MANIFEST_FILENAME]],
            warnings=[],
            source_hash=None,
            markdown_hash=None,
            text_hash=None,
        )

    paper_id = _blank_to_none(manifest.get("paper_id"))
    version = manifest.get("version") if _is_int(manifest.get("version")) else None
    source_hash = _blank_to_none(manifest.get("source_hash"))
    markdown_hash = _blank_to_none(manifest.get("markdown_hash"))
    index_scope = _blank_to_none(manifest.get("index_scope"))
    warnings = _merge_unique([*_warnings_from(manifest)])

    if manifest.get("schema_version") != SCHEMA_VERSION:
        reasons.append("schema_version_mismatch")
    if manifest.get("manifest_type") not in {
        SKELETON_MANIFEST_TYPE,
        LOCAL_INDEX_PACKAGE_MANIFEST_TYPE,
    }:
        reasons.append("manifest_type_mismatch")
    if paper_id is None:
        reasons.append("paper_id_mismatch")
    if version is None:
        reasons.append("version_mismatch")
    if index_scope != INDEX_SCOPE:
        reasons.append("index_scope_mismatch")
    if source_hash is None:
        reasons.append("source_hash_mismatch")
    if markdown_hash is None:
        reasons.append("markdown_hash_mismatch")

    indexes: dict[str, dict[str, Any]] = {}
    for filename in _REQUIRED_FILES[1:]:
        path = package_dir / filename
        if not path.exists():
            reasons.append(_MISSING_REASONS[filename])
            continue
        payload = _read_json_object(path)
        if payload is None:
            reasons.append(_INVALID_REASONS[filename])
            continue
        indexes[filename] = payload
        warnings = _merge_unique([*warnings, *_warnings_from(payload)])

    if any(reason.endswith("_missing") or reason.endswith("_invalid") for reason in reasons):
        return _PackageValidation(
            paper_id=paper_id,
            version=version,
            manifest=manifest,
            reasons=_merge_unique(reasons),
            warnings=warnings,
            source_hash=source_hash,
            markdown_hash=markdown_hash,
            text_hash=None,
        )

    for filename, payload in indexes.items():
        if payload.get("schema_version") != SCHEMA_VERSION:
            reasons.append("schema_version_mismatch")
        if payload.get("index_type") != _INDEX_TYPES[filename]:
            reasons.append("index_type_mismatch")
        if _blank_to_none(payload.get("paper_id")) != paper_id:
            reasons.append("paper_id_mismatch")
        if payload.get("version") != version:
            reasons.append("version_mismatch")
        payload_scope = _blank_to_none(payload.get("index_scope"))
        if filename in {
            METADATA_INDEX_FILENAME,
            CHAPTER_INDEX_FILENAME,
            HIERARCHY_INDEX_FILENAME,
        }:
            if payload_scope != index_scope:
                reasons.append("index_scope_mismatch")
        elif payload_scope is not None and payload_scope != index_scope:
            reasons.append("index_scope_mismatch")

        payload_source_hash = _blank_to_none(payload.get("source_hash"))
        if filename == METADATA_INDEX_FILENAME:
            if payload_source_hash != source_hash:
                reasons.append("source_hash_mismatch")
        elif payload_source_hash is not None and payload_source_hash != source_hash:
            reasons.append("source_hash_mismatch")
        if _blank_to_none(payload.get("markdown_hash")) != markdown_hash:
            reasons.append("markdown_hash_mismatch")

    text_hash = _consistent_text_hash(indexes, manifest, reasons)

    return _PackageValidation(
        paper_id=paper_id,
        version=version,
        manifest=manifest,
        reasons=_merge_unique(reasons),
        warnings=warnings,
        source_hash=source_hash,
        markdown_hash=markdown_hash,
        text_hash=text_hash,
    )


def _consistent_text_hash(
    indexes: dict[str, dict[str, Any]],
    manifest: dict[str, Any],
    reasons: list[str],
) -> str | None:
    text_hashes: list[str] = []
    for filename in (
        PAPER_TEXT_INDEX_FILENAME,
        CHAPTER_INDEX_FILENAME,
        HIERARCHY_INDEX_FILENAME,
    ):
        text_hash = _blank_to_none(indexes[filename].get("text_hash"))
        if text_hash is None:
            reasons.append("text_hash_mismatch")
            continue
        text_hashes.append(text_hash)

    unique_hashes = _merge_unique(text_hashes)
    if len(unique_hashes) != 1:
        reasons.append("text_hash_mismatch")
        return unique_hashes[0] if unique_hashes else None

    manifest_text_hash = _blank_to_none(manifest.get("text_hash"))
    if manifest_text_hash is not None and manifest_text_hash != unique_hashes[0]:
        reasons.append("text_hash_mismatch")
    return unique_hashes[0]


def _finalized_manifest(
    *,
    package_dir: Path,
    manifest: dict[str, Any],
    completed_at: str,
    warnings: list[str],
    text_hash: str | None,
) -> dict[str, Any]:
    finalized = dict(manifest)
    finalized.update(
        {
            "manifest_type": LOCAL_INDEX_PACKAGE_MANIFEST_TYPE,
            "index_status": "indexed",
            "completed_at": completed_at,
            "package_complete": True,
            "text_hash": text_hash,
            "required_files": list(_REQUIRED_FILES),
            "metadata_index_path": str(package_dir / METADATA_INDEX_FILENAME),
            "paper_text_index_path": str(package_dir / PAPER_TEXT_INDEX_FILENAME),
            "chapter_index_path": str(package_dir / CHAPTER_INDEX_FILENAME),
            "hierarchy_index_path": str(package_dir / HIERARCHY_INDEX_FILENAME),
            "warnings": warnings,
            "published_to_postgres": False,
            "published_at": None,
            "remote_resource_id": None,
            "upload_batch_id": None,
            "postgres_manifest_id": None,
        },
    )
    return finalized


def _finalized_manifest_incomplete(
    *,
    package_dir: Path,
    manifest: dict[str, Any],
    text_hash: str | None,
) -> bool:
    expected = {
        "manifest_type": LOCAL_INDEX_PACKAGE_MANIFEST_TYPE,
        "index_status": "indexed",
        "package_complete": True,
        "text_hash": text_hash,
        "required_files": list(_REQUIRED_FILES),
        "metadata_index_path": str(package_dir / METADATA_INDEX_FILENAME),
        "paper_text_index_path": str(package_dir / PAPER_TEXT_INDEX_FILENAME),
        "chapter_index_path": str(package_dir / CHAPTER_INDEX_FILENAME),
        "hierarchy_index_path": str(package_dir / HIERARCHY_INDEX_FILENAME),
        "published_to_postgres": False,
        "published_at": None,
        "remote_resource_id": None,
        "upload_batch_id": None,
        "postgres_manifest_id": None,
    }
    if any(manifest.get(key) != value for key, value in expected.items()):
        return True
    completed_at = _blank_to_none(manifest.get("completed_at"))
    if completed_at is None:
        return True
    try:
        datetime.fromisoformat(completed_at)
    except ValueError:
        return True
    return False


def _package_dirs(indexes_root: Path) -> list[Path]:
    if not indexes_root.exists():
        return []
    return sorted(
        (
            path
            for path in indexes_root.iterdir()
            if path.is_dir() and not path.name.startswith(".")
        ),
        key=lambda path: path.name,
    )


def _indexed_package_record(package_dir: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "paper_id": manifest.get("paper_id"),
        "version": manifest.get("version"),
        "package_path": str(package_dir),
        "manifest_path": str(package_dir / MANIFEST_FILENAME),
        "metadata_index_path": str(package_dir / METADATA_INDEX_FILENAME),
        "paper_text_index_path": str(package_dir / PAPER_TEXT_INDEX_FILENAME),
        "chapter_index_path": str(package_dir / CHAPTER_INDEX_FILENAME),
        "hierarchy_index_path": str(package_dir / HIERARCHY_INDEX_FILENAME),
        "source_hash": manifest.get("source_hash"),
        "markdown_hash": manifest.get("markdown_hash"),
        "text_hash": manifest.get("text_hash"),
        "warnings": list(manifest.get("warnings") or []),
    }


def _item_record(
    *,
    package_path: Path,
    reasons: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "package_path": str(package_path),
        "reasons": list(reasons),
        "warnings": list(warnings),
    }


def _read_json_object(path: Path) -> dict[str, Any] | None:
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(decoded, dict):
        return None
    return decoded


def _atomic_write_json(path: Path, payload: dict[str, Any], *, tmp_filename: str) -> None:
    tmp_path = path.with_name(tmp_filename)
    encoded = json.dumps(payload, ensure_ascii=False, indent=2)
    tmp_path.write_text(f"{encoded}\n", encoding="utf-8")
    tmp_path.replace(path)


def _warnings_from(payload: dict[str, Any]) -> list[str]:
    warnings = payload.get("warnings")
    if not isinstance(warnings, list):
        return []
    return [str(warning) for warning in warnings if str(warning)]


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


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()
