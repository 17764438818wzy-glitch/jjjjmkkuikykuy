"""Eligibility checks for formal library candidates.

This module is intentionally standalone. It does not call the index builder,
SQL preview service, upload service, PostgreSQL adapter, or any package writer.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path
from typing import Any, TypeVar

from paper_pipeline.domain import (
    DuplicateStatus,
    IndexStatus,
    PaperMetadata,
    SqlStatus,
    Tier,
    VersionStatus,
)
from paper_pipeline.domain.serialization import metadata_from_json
from paper_pipeline.taxonomy.categories import validate_category
from paper_pipeline.taxonomy.directions import validate_direction

from paper_pipeline.eligibility.models import EligibilityResult, EligibilityScope

_EnumT = TypeVar("_EnumT", bound=StrEnum)

_CRITICAL_RAW_FIELDS = (
    "paper_id",
    "version",
    "title",
    "category",
    "direction",
    "tier",
    "sensitive_clean",
    "version_status",
    "duplicate_status",
    "source_hash",
)

_DEFAULTABLE_STATUS_FIELDS = ("index_status", "sql_status")
_FORMAL_TIERS = {Tier.GOLD, Tier.SILVER}
_ALLOWED_INDEX_STATUSES = {
    IndexStatus.NOT_INDEXED,
    IndexStatus.NEEDS_REBUILD,
    IndexStatus.INDEXED,
}
_ALLOWED_SQL_STATUSES = {SqlStatus.NOT_UPLOADED, SqlStatus.FAILED}


def load_raw_metadata_dict(metadata_path: Path) -> dict[str, Any]:
    """Load metadata JSON without applying PaperMetadata defaults."""

    try:
        decoded = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("metadata_invalid") from exc
    if not isinstance(decoded, dict):
        raise ValueError("metadata_invalid")
    return decoded


def metadata_has_explicit_field(raw: Mapping[str, Any], field: str) -> bool:
    """Return whether a raw metadata object explicitly carries a field.

    Current persisted metadata uses ``id`` for ``PaperMetadata.id``. The new
    eligibility vocabulary calls the result field ``paper_id``, so both raw
    shapes count as explicit paper identity.
    """

    if field == "paper_id":
        return "paper_id" in raw or "id" in raw
    return field in raw


def check_formal_candidate(
    metadata: PaperMetadata | None,
    metadata_path: Path | str | None = None,
    markdown_path: Path | str | None = None,
    raw_metadata: Mapping[str, Any] | None = None,
    allow_uploaded_sql: bool = False,
) -> EligibilityResult:
    metadata_file = Path(metadata_path) if metadata_path is not None else None
    markdown_file = Path(markdown_path) if markdown_path is not None else None

    blocking_reasons: list[str] = []
    warnings: list[str] = []
    raw = raw_metadata
    raw_stage0_rejected = False

    if raw is None and metadata_file is None:
        warnings.append("metadata_raw_check_skipped")
    elif raw is None and metadata_file is not None:
        if metadata_file.exists():
            try:
                raw = load_raw_metadata_dict(metadata_file)
            except ValueError:
                _add_once(blocking_reasons, "metadata_invalid")
        else:
            _add_once(blocking_reasons, "metadata_missing")

    if metadata is None:
        metadata = _load_metadata(metadata_file, blocking_reasons)

    if raw is not None:
        raw_stage0_rejected = _raw_indicates_stage0_rejected(raw)
        _check_raw_metadata_fields(raw, blocking_reasons, warnings)

    if metadata is None:
        _add_once(blocking_reasons, "metadata_invalid")
        return EligibilityResult(
            paper_id=None,
            version=None,
            scope=EligibilityScope.BLOCKED,
            eligible=False,
            blocking_reasons=blocking_reasons,
            warnings=warnings,
            metadata_path=metadata_file,
            markdown_path=markdown_file,
            disposition="blocked",
        )

    tier = _coerce_enum(Tier, metadata.tier)
    version_status = _coerce_enum(VersionStatus, metadata.version_status)
    duplicate_status = _coerce_enum(DuplicateStatus, metadata.duplicate_status)
    index_status = _coerce_enum(IndexStatus, metadata.index_status)
    sql_status = _coerce_enum(SqlStatus, metadata.sql_status)

    _check_markdown(markdown_file, blocking_reasons)
    _check_metadata_values(
        metadata=metadata,
        tier=tier,
        version_status=version_status,
        duplicate_status=duplicate_status,
        index_status=index_status,
        sql_status=sql_status,
        allow_uploaded_sql=allow_uploaded_sql,
        blocking_reasons=blocking_reasons,
    )

    eligible = not blocking_reasons
    scope = _scope_for(
        eligible=eligible,
        tier=tier,
        version_status=version_status,
        duplicate_status=duplicate_status,
        stage0_rejected=raw_stage0_rejected,
    )
    disposition = _disposition_for(
        eligible=eligible,
        tier=tier,
        version_status=version_status,
        duplicate_status=duplicate_status,
        stage0_rejected=raw_stage0_rejected,
    )

    return EligibilityResult(
        paper_id=metadata.id,
        version=metadata.version,
        scope=scope,
        eligible=eligible,
        blocking_reasons=blocking_reasons,
        warnings=warnings,
        metadata_path=metadata_file,
        markdown_path=markdown_file,
        source_hash=_blank_to_none(metadata.source_hash),
        tier=_enum_value(tier, metadata.tier),
        version_status=_enum_value(version_status, metadata.version_status),
        duplicate_status=_enum_value(duplicate_status, metadata.duplicate_status),
        sensitive_clean=bool(metadata.sensitive_clean),
        index_status=_enum_value(index_status, metadata.index_status),
        sql_status=_enum_value(sql_status, metadata.sql_status),
        disposition=disposition,
    )


def _load_metadata(
    metadata_file: Path | None,
    blocking_reasons: list[str],
) -> PaperMetadata | None:
    if metadata_file is None or not metadata_file.exists():
        return None
    try:
        return metadata_from_json(metadata_file.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, KeyError):
        _add_once(blocking_reasons, "metadata_invalid")
        return None


def _check_raw_metadata_fields(
    raw: Mapping[str, Any],
    blocking_reasons: list[str],
    warnings: list[str],
) -> None:
    for field in _CRITICAL_RAW_FIELDS:
        if not metadata_has_explicit_field(raw, field):
            _add_once(blocking_reasons, f"metadata_field_defaulted:{field}")

    # These two statuses are defaulted by PaperMetadata, but formal_candidate
    # is a pre-upload gate. Missing raw status fields are surfaced for audit and
    # should be hardened later by SQL upload eligibility, not by this core gate.
    for field in _DEFAULTABLE_STATUS_FIELDS:
        if not metadata_has_explicit_field(raw, field):
            _add_once(warnings, f"metadata_field_defaulted:{field}")


def _check_markdown(
    markdown_file: Path | None,
    blocking_reasons: list[str],
) -> None:
    if markdown_file is None or not markdown_file.exists():
        _add_once(blocking_reasons, "markdown_missing")
        return
    if not markdown_file.is_file():
        _add_once(blocking_reasons, "markdown_unreadable")
        return
    try:
        markdown_file.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        _add_once(blocking_reasons, "markdown_unreadable")


def _check_metadata_values(
    *,
    metadata: PaperMetadata,
    tier: Tier | None,
    version_status: VersionStatus | None,
    duplicate_status: DuplicateStatus | None,
    index_status: IndexStatus | None,
    sql_status: SqlStatus | None,
    allow_uploaded_sql: bool,
    blocking_reasons: list[str],
) -> None:
    if tier not in _FORMAL_TIERS:
        _add_once(blocking_reasons, "tier_not_formal")
    if version_status is not VersionStatus.CURRENT:
        _add_once(blocking_reasons, "version_not_current")
    if duplicate_status is not DuplicateStatus.UNIQUE:
        _add_once(blocking_reasons, "duplicate_not_unique")
    if metadata.sensitive_clean is not True:
        _add_once(blocking_reasons, "sensitive_not_clean")
    if not str(metadata.source_hash or "").strip():
        _add_once(blocking_reasons, "source_hash_missing")
    if index_status not in _ALLOWED_INDEX_STATUSES:
        _add_once(blocking_reasons, "index_status_not_allowed")
    allowed_sql_statuses = set(_ALLOWED_SQL_STATUSES)
    if allow_uploaded_sql:
        allowed_sql_statuses.add(SqlStatus.UPLOADED)
    if sql_status not in allowed_sql_statuses:
        _add_once(blocking_reasons, "sql_status_not_allowed")

    category_result = validate_category(metadata.category)
    if not category_result.is_valid:
        _add_once(blocking_reasons, "category_invalid")
    direction_result = validate_direction(metadata.category, metadata.direction)
    if not direction_result.is_valid:
        _add_once(blocking_reasons, "direction_invalid")


def _scope_for(
    *,
    eligible: bool,
    tier: Tier | None,
    version_status: VersionStatus | None,
    duplicate_status: DuplicateStatus | None,
    stage0_rejected: bool = False,
) -> EligibilityScope:
    if eligible:
        return EligibilityScope.FORMAL_CANDIDATE
    if stage0_rejected or tier is Tier.STAGE0_REJECTED:
        return EligibilityScope.STAGE0_AUDIT_ONLY
    if duplicate_status is not None and duplicate_status is not DuplicateStatus.UNIQUE:
        return EligibilityScope.AUDIT_ONLY
    if version_status in {
        VersionStatus.REPLACED,
        VersionStatus.WITHDRAWN,
        VersionStatus.ABANDONED,
    }:
        return EligibilityScope.AUDIT_ONLY
    if tier is Tier.REJECTED:
        return EligibilityScope.AUDIT_ONLY
    if tier in {Tier.PENDING, Tier.MANUAL_REVIEW_REQUIRED}:
        return EligibilityScope.REVIEW_ONLY
    return EligibilityScope.BLOCKED


def _disposition_for(
    *,
    eligible: bool,
    tier: Tier | None,
    version_status: VersionStatus | None,
    duplicate_status: DuplicateStatus | None,
    stage0_rejected: bool = False,
) -> str:
    if eligible:
        return EligibilityScope.FORMAL_CANDIDATE.value
    if stage0_rejected or tier is Tier.STAGE0_REJECTED:
        return "stage0_rejected"
    if duplicate_status is not None and duplicate_status is not DuplicateStatus.UNIQUE:
        return "duplicate"
    if version_status is VersionStatus.REPLACED:
        return "version_replaced"
    if version_status is VersionStatus.WITHDRAWN:
        return "version_withdrawn"
    if version_status is VersionStatus.ABANDONED:
        return "version_abandoned"
    if tier is Tier.REJECTED:
        return "rejected"
    if tier in {Tier.PENDING, Tier.MANUAL_REVIEW_REQUIRED}:
        return "pending"
    return "blocked"


def _raw_indicates_stage0_rejected(raw: Mapping[str, Any]) -> bool:
    if raw.get("stage0_rejected") is True:
        return True
    values = (
        raw.get("tier"),
        raw.get("scope"),
        raw.get("disposition"),
        raw.get("stage0_decision"),
        raw.get("decision"),
    )
    return any(str(value).strip().lower() in {"stage0_rejected", "stage0_reject"} for value in values)


def _coerce_enum(enum_type: type[_EnumT], value: object) -> _EnumT | None:
    if isinstance(value, enum_type):
        return value
    if value is None:
        return None
    try:
        return enum_type(str(value))
    except (TypeError, ValueError):
        return None


def _enum_value(coerced: StrEnum | None, original: object) -> str | None:
    if coerced is not None:
        return coerced.value
    if original is None:
        return None
    return str(original)


def _blank_to_none(value: str | None) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _add_once(items: list[str], value: str) -> None:
    if value not in items:
        items.append(value)
