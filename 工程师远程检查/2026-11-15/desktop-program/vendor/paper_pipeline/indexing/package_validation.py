from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from paper_pipeline.sql.upload_duplicate_guard import (
    UPLOAD_MODE,
    UploadDuplicateGuardResult,
    UploadDuplicateGuardState,
    UploadIdentity,
    check_duplicate_upload_guard,
)


REQUIRED_PACKAGE_FILES = (
    "manifest.json",
    "metadata_index.json",
    "paper_text_index.json",
    "chapter_index.json",
    "hierarchy_index.json",
)

_INDEX_SCOPE = "formal_candidate"
_INDEX_TYPES = {
    "metadata_index.json": "metadata_index",
    "paper_text_index.json": "paper_text_index",
    "chapter_index.json": "chapter_index",
    "hierarchy_index.json": "hierarchy_index",
}
_SCOPE_REQUIRED_FILES = {
    "manifest.json",
    "metadata_index.json",
    "chapter_index.json",
    "hierarchy_index.json",
}
_TEXT_HASH_PEERS = (
    "chapter_index.json",
    "hierarchy_index.json",
    "manifest.json",
)
_VECTOR_FIELD_ALLOWLIST = {"search_vector_ready"}


@dataclass(frozen=True)
class PackageValidationIssue:
    code: str
    message: str
    file: str | None = None
    field: str | None = None


@dataclass(frozen=True)
class PackageValidationResult:
    package_dir: Path
    valid: bool
    paper_id: str | None
    version: int | None
    issues: list[PackageValidationIssue]
    warnings: list[PackageValidationIssue]
    manifest_path: Path | None = None


def validate_local_index_package(
    package_dir: Path,
    *,
    current_schema_version: str = "1.0",
) -> PackageValidationResult:
    package_path = Path(package_dir)
    issues: list[PackageValidationIssue] = []
    warnings: list[PackageValidationIssue] = []
    manifest_path = package_path / "manifest.json"

    if not package_path.is_dir():
        _add_issue(
            issues,
            "package_dir_missing",
            "Package directory does not exist.",
        )
        return _result(package_path, None, None, issues, warnings, manifest_path)

    missing_files = [
        filename
        for filename in REQUIRED_PACKAGE_FILES
        if not (package_path / filename).is_file()
    ]
    if missing_files:
        for filename in missing_files:
            _add_issue(
                issues,
                "required_file_missing",
                f"Required package file is missing: {filename}.",
                file=filename,
            )
        return _result(package_path, None, None, issues, warnings, manifest_path)

    payloads: dict[str, dict[str, Any]] = {}
    for filename in REQUIRED_PACKAGE_FILES:
        path = package_path / filename
        try:
            decoded = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            _add_issue(
                issues,
                "json_invalid",
                f"JSON could not be parsed: {exc}.",
                file=filename,
            )
            continue
        if not isinstance(decoded, dict):
            _add_issue(
                issues,
                "json_invalid",
                "JSON payload must be an object.",
                file=filename,
            )
            continue
        payloads[filename] = decoded

    if issues:
        return _result(package_path, None, None, issues, warnings, manifest_path)

    manifest = payloads["manifest.json"]
    metadata_index = payloads["metadata_index.json"]
    paper_id = _blank_to_none(manifest.get("paper_id"))
    version = _int_or_none(manifest.get("version"))

    _validate_schema(payloads, current_schema_version, issues)
    _validate_identity(payloads, paper_id, version, issues)
    _validate_status(manifest, issues)
    _validate_duplicate_upload_guard(manifest, metadata_index, issues)
    _validate_forbidden_fields(payloads, issues)
    _validate_source_hashes(payloads, issues)
    _validate_markdown_hashes(payloads, package_path, issues)
    _validate_text_hashes(payloads, issues)
    _validate_blocks_hash(metadata_index, package_path, issues, warnings)

    return _result(package_path, paper_id, version, issues, warnings, manifest_path)


def _validate_schema(
    payloads: dict[str, dict[str, Any]],
    current_schema_version: str,
    issues: list[PackageValidationIssue],
) -> None:
    for filename, payload in payloads.items():
        if payload.get("schema_version") != current_schema_version:
            _add_issue(
                issues,
                "schema_version_mismatch",
                "schema_version does not match the current validator version.",
                file=filename,
                field="schema_version",
            )


def _validate_identity(
    payloads: dict[str, dict[str, Any]],
    paper_id: str | None,
    version: int | None,
    issues: list[PackageValidationIssue],
) -> None:
    for filename, payload in payloads.items():
        if _blank_to_none(payload.get("paper_id")) != paper_id:
            _add_issue(
                issues,
                "paper_id_mismatch",
                "paper_id does not match the package manifest.",
                file=filename,
                field="paper_id",
            )
        if payload.get("version") != version:
            _add_issue(
                issues,
                "version_mismatch",
                "version does not match the package manifest.",
                file=filename,
                field="version",
            )

        index_scope = _blank_to_none(payload.get("index_scope"))
        if filename in _SCOPE_REQUIRED_FILES and index_scope != _INDEX_SCOPE:
            _add_issue(
                issues,
                "index_scope_mismatch",
                "index_scope must be formal_candidate.",
                file=filename,
                field="index_scope",
            )
        if filename not in _SCOPE_REQUIRED_FILES and index_scope is not None:
            if index_scope != _INDEX_SCOPE:
                _add_issue(
                    issues,
                    "index_scope_mismatch",
                    "index_scope must be formal_candidate when present.",
                    file=filename,
                    field="index_scope",
                )

        expected_index_type = _INDEX_TYPES.get(filename)
        if expected_index_type is not None and payload.get("index_type") != expected_index_type:
            _add_issue(
                issues,
                "index_type_mismatch",
                "index_type does not match the package file.",
                file=filename,
                field="index_type",
            )


def _validate_status(
    manifest: dict[str, Any],
    issues: list[PackageValidationIssue],
) -> None:
    if manifest.get("package_complete") is not True:
        _add_issue(
            issues,
            "package_not_complete",
            "manifest.package_complete must be true.",
            file="manifest.json",
            field="package_complete",
        )
    if manifest.get("index_status") != "indexed":
        _add_issue(
            issues,
            "index_status_not_indexed",
            "manifest.index_status must be indexed.",
            file="manifest.json",
            field="index_status",
        )
    if manifest.get("published_to_postgres") is True:
        _add_issue(
            issues,
            "already_published_to_postgres",
            "Package has already been marked as published to PostgreSQL.",
            file="manifest.json",
            field="published_to_postgres",
        )


def _validate_duplicate_upload_guard(
    manifest: dict[str, Any],
    metadata_index: dict[str, Any],
    issues: list[PackageValidationIssue],
) -> None:
    guard_result = check_duplicate_upload_guard(
        UploadDuplicateGuardState(
            manifest_published_to_postgres=(
                manifest.get("published_to_postgres") is True
            ),
            metadata_sql_status=(
                _blank_to_none(metadata_index.get("sql_status")) or "not_uploaded"
            ),
            manifest_identity=_manifest_upload_identity(manifest),
            metadata_identity=_metadata_upload_identity(metadata_index),
        ),
        candidate_identity=None,
        operation_mode=UPLOAD_MODE,
    )
    if guard_result.allowed:
        return

    _add_issue(
        issues,
        "duplicate_upload_blocked",
        _duplicate_guard_message(guard_result),
        file="metadata_index.json",
        field="duplicate_guard",
    )


def _manifest_upload_identity(payload: dict[str, Any]) -> UploadIdentity | None:
    return _upload_identity(
        resource_id=_first_text(
            payload,
            "remote_resource_id",
            "resource_id",
            "sql_upload_record_id",
        ),
        upload_batch_id=_first_text(
            payload,
            "upload_batch_id",
            "sql_batch_id",
        ),
        confirmation_id=_first_text(
            payload,
            "confirmation_id",
            "sql_confirmation_id",
        ),
        field_snapshot_hash=_first_text(
            payload,
            "field_snapshot_hash",
            "sql_field_snapshot_hash",
        ),
    )


def _metadata_upload_identity(payload: dict[str, Any]) -> UploadIdentity | None:
    return _upload_identity(
        resource_id=_first_text(
            payload,
            "sql_upload_record_id",
            "remote_resource_id",
            "resource_id",
        ),
        upload_batch_id=_first_text(
            payload,
            "sql_batch_id",
            "upload_batch_id",
        ),
        confirmation_id=_first_text(
            payload,
            "sql_confirmation_id",
            "confirmation_id",
        ),
        field_snapshot_hash=_first_text(
            payload,
            "sql_field_snapshot_hash",
            "field_snapshot_hash",
        ),
    )


def _upload_identity(
    *,
    resource_id: str | None,
    upload_batch_id: str | None,
    confirmation_id: str | None,
    field_snapshot_hash: str | None,
) -> UploadIdentity | None:
    if not any((resource_id, upload_batch_id, confirmation_id, field_snapshot_hash)):
        return None
    return UploadIdentity(
        resource_id=resource_id,
        upload_batch_id=upload_batch_id,
        confirmation_id=confirmation_id,
        field_snapshot_hash=field_snapshot_hash,
    )


def _first_text(payload: dict[str, Any], *field_names: str) -> str | None:
    for field_name in field_names:
        value = _blank_to_none(payload.get(field_name))
        if value is not None:
            return value
    return None


def _duplicate_guard_message(guard_result: UploadDuplicateGuardResult) -> str:
    reasons = ",".join(guard_result.reasons) or "none"
    warnings = ",".join(guard_result.warnings) or "none"
    recommended_action = guard_result.recommended_action or "none"
    return (
        "Duplicate upload guard blocked package upload: "
        f"outcome={guard_result.outcome}; "
        f"reasons={reasons}; "
        f"warnings={warnings}; "
        f"recommended_action={recommended_action}."
    )


def _validate_source_hashes(
    payloads: dict[str, dict[str, Any]],
    issues: list[PackageValidationIssue],
) -> None:
    manifest_hash = _blank_to_none(payloads["manifest.json"].get("source_hash"))
    metadata_hash = _blank_to_none(payloads["metadata_index.json"].get("source_hash"))

    if manifest_hash is None:
        _add_issue(
            issues,
            "source_hash_missing",
            "manifest.source_hash is required.",
            file="manifest.json",
            field="source_hash",
        )
    if metadata_hash is None:
        _add_issue(
            issues,
            "source_hash_missing",
            "metadata_index.source_hash is required.",
            file="metadata_index.json",
            field="source_hash",
        )
    if manifest_hash is None or metadata_hash is None:
        return

    if metadata_hash != manifest_hash:
        _add_issue(
            issues,
            "source_hash_mismatch",
            "metadata_index.source_hash does not match manifest.source_hash.",
            file="metadata_index.json",
            field="source_hash",
        )

    for filename, payload in payloads.items():
        if filename in {"manifest.json", "metadata_index.json"}:
            continue
        source_hash = _blank_to_none(payload.get("source_hash"))
        if source_hash is not None and source_hash != manifest_hash:
            _add_issue(
                issues,
                "source_hash_mismatch",
                "source_hash does not match manifest.source_hash.",
                file=filename,
                field="source_hash",
            )


def _validate_markdown_hashes(
    payloads: dict[str, dict[str, Any]],
    package_dir: Path,
    issues: list[PackageValidationIssue],
) -> None:
    hash_by_file: dict[str, str] = {}
    for filename, payload in payloads.items():
        markdown_hash = _blank_to_none(payload.get("markdown_hash"))
        if markdown_hash is None:
            _add_issue(
                issues,
                "markdown_hash_missing",
                "markdown_hash is required.",
                file=filename,
                field="markdown_hash",
            )
            continue
        hash_by_file[filename] = markdown_hash

    unique_hashes = set(hash_by_file.values())
    if len(unique_hashes) > 1:
        _add_issue(
            issues,
            "markdown_hash_mismatch",
            "markdown_hash values are not consistent across package JSON files.",
            field="markdown_hash",
        )

    manifest = payloads["manifest.json"]
    markdown_path_value = _blank_to_none(manifest.get("markdown_path"))
    if markdown_path_value is None:
        _add_issue(
            issues,
            "markdown_path_missing",
            "manifest.markdown_path is required.",
            file="manifest.json",
            field="markdown_path",
        )
        return

    markdown_path = _resolve_recorded_path(markdown_path_value, package_dir)
    if not markdown_path.is_file():
        _add_issue(
            issues,
            "markdown_file_missing",
            "Recorded markdown_path does not exist.",
            file="manifest.json",
            field="markdown_path",
        )
        return

    manifest_hash = _blank_to_none(manifest.get("markdown_hash"))
    if manifest_hash is not None and _sha256_file(markdown_path) != manifest_hash:
        _add_issue(
            issues,
            "markdown_hash_mismatch",
            "manifest.markdown_hash does not match the markdown file.",
            file="manifest.json",
            field="markdown_hash",
        )


def _validate_text_hashes(
    payloads: dict[str, dict[str, Any]],
    issues: list[PackageValidationIssue],
) -> None:
    paper_text = payloads["paper_text_index.json"]
    paper_text_hash = _blank_to_none(paper_text.get("text_hash"))
    retrieval_text = paper_text.get("retrieval_text")

    if paper_text_hash is None:
        _add_issue(
            issues,
            "text_hash_missing",
            "paper_text_index.text_hash is required.",
            file="paper_text_index.json",
            field="text_hash",
        )
    if not isinstance(retrieval_text, str):
        _add_issue(
            issues,
            "retrieval_text_missing",
            "paper_text_index.retrieval_text is required.",
            file="paper_text_index.json",
            field="retrieval_text",
        )

    if paper_text_hash is None or not isinstance(retrieval_text, str):
        return

    calculated_text_hash = hashlib.sha256(retrieval_text.encode("utf-8")).hexdigest()
    if calculated_text_hash != paper_text_hash:
        _add_issue(
            issues,
            "text_hash_mismatch",
            "paper_text_index.text_hash does not match retrieval_text.",
            file="paper_text_index.json",
            field="text_hash",
        )

    for filename in _TEXT_HASH_PEERS:
        text_hash = _blank_to_none(payloads[filename].get("text_hash"))
        if text_hash is not None and text_hash != paper_text_hash:
            _add_issue(
                issues,
                "text_hash_mismatch",
                "text_hash does not match paper_text_index.text_hash.",
                file=filename,
                field="text_hash",
            )


def _validate_blocks_hash(
    metadata_index: dict[str, Any],
    package_dir: Path,
    issues: list[PackageValidationIssue],
    warnings: list[PackageValidationIssue],
) -> None:
    blocks_path_value = _blank_to_none(metadata_index.get("blocks_path"))
    blocks_hash = _blank_to_none(metadata_index.get("blocks_hash"))

    if blocks_path_value is None and blocks_hash is None:
        _add_warning(
            warnings,
            "blocks_missing_warning",
            "blocks_path and blocks_hash are both missing.",
            file="metadata_index.json",
        )
        return

    if blocks_path_value is None or blocks_hash is None:
        _add_warning(
            warnings,
            "blocks_hash_not_checked_warning",
            "blocks_hash could not be checked because blocks metadata is incomplete.",
            file="metadata_index.json",
        )
        return

    blocks_path = _resolve_recorded_path(blocks_path_value, package_dir)
    if not blocks_path.is_file():
        _add_warning(
            warnings,
            "blocks_hash_not_checked_warning",
            "blocks_hash could not be checked because blocks_path does not exist.",
            file="metadata_index.json",
            field="blocks_path",
        )
        return

    if _sha256_file(blocks_path) != blocks_hash:
        _add_issue(
            issues,
            "blocks_hash_mismatch",
            "metadata_index.blocks_hash does not match blocks_path.",
            file="metadata_index.json",
            field="blocks_hash",
        )


def _validate_forbidden_fields(
    payloads: dict[str, dict[str, Any]],
    issues: list[PackageValidationIssue],
) -> None:
    for filename, payload in payloads.items():
        if "markdown_content" in payload:
            _add_issue(
                issues,
                "forbidden_markdown_content",
                "Index package JSON must not contain markdown_content.",
                file=filename,
                field="markdown_content",
            )
        field_names = list(_iter_field_names(payload))
        if filename == "chapter_index.json" and "chapter_text" in field_names:
            _add_issue(
                issues,
                "forbidden_chapter_text",
                "chapter_index JSON must not contain chapter_text.",
                file=filename,
                field="chapter_text",
            )

        for field in field_names:
            lower_field = field.lower()
            if "embedding" in lower_field:
                _add_issue(
                    issues,
                    "forbidden_embedding",
                    "Index package JSON must not contain embedding fields.",
                    file=filename,
                    field=field,
                )
            if _is_forbidden_vector_field(lower_field):
                _add_issue(
                    issues,
                    "forbidden_vector",
                    "Index package JSON must not contain vector fields.",
                    file=filename,
                    field=field,
                )
            if _is_forbidden_chunk_field(lower_field):
                _add_issue(
                    issues,
                    "forbidden_chunk",
                    "Index package JSON must not contain chunk fields.",
                    file=filename,
                    field=field,
                )


def _is_forbidden_vector_field(field: str) -> bool:
    if field in _VECTOR_FIELD_ALLOWLIST:
        return False
    return (
        field in {"vector", "vectors", "vector_hash", "vector_index", "vector_path"}
        or field.startswith("vector_")
        or field.endswith("_vector")
    )


def _is_forbidden_chunk_field(field: str) -> bool:
    return (
        field in {"chunk", "chunks", "chunk_index"}
        or field.startswith("chunk_")
        or field.endswith("_chunk")
    )


def _iter_field_names(value: object) -> list[str]:
    fields: list[str] = []
    if isinstance(value, dict):
        for key, nested_value in value.items():
            if isinstance(key, str):
                fields.append(key)
            fields.extend(_iter_field_names(nested_value))
    elif isinstance(value, list):
        for item in value:
            fields.extend(_iter_field_names(item))
    return fields


def _result(
    package_dir: Path,
    paper_id: str | None,
    version: int | None,
    issues: list[PackageValidationIssue],
    warnings: list[PackageValidationIssue],
    manifest_path: Path | None,
) -> PackageValidationResult:
    return PackageValidationResult(
        package_dir=package_dir,
        valid=not issues,
        paper_id=paper_id,
        version=version,
        issues=issues,
        warnings=warnings,
        manifest_path=manifest_path,
    )


def _add_issue(
    issues: list[PackageValidationIssue],
    code: str,
    message: str,
    *,
    file: str | None = None,
    field: str | None = None,
) -> None:
    issue = PackageValidationIssue(code=code, message=message, file=file, field=field)
    if issue not in issues:
        issues.append(issue)


def _add_warning(
    warnings: list[PackageValidationIssue],
    code: str,
    message: str,
    *,
    file: str | None = None,
    field: str | None = None,
) -> None:
    warning = PackageValidationIssue(code=code, message=message, file=file, field=field)
    if warning not in warnings:
        warnings.append(warning)


def _blank_to_none(value: object) -> str | None:
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return None


def _int_or_none(value: object) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def _resolve_recorded_path(value: str, package_dir: Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return package_dir / path


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
