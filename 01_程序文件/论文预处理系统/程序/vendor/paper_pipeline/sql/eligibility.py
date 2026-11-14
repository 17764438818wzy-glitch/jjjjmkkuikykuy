"""SQL preview eligibility checks for task 11.

This module deliberately does not generate SQL, connect to databases, upload
records, or mutate sql_status. It only reports whether a paper can proceed to a
later SQL preview task.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from paper_pipeline.domain import (
    IndexStatus,
    PaperMetadata,
    SqlStatus,
    Tier,
)
from paper_pipeline.domain.serialization import metadata_from_json
from paper_pipeline.eligibility import EligibilityResult, check_formal_candidate

_REVIEWED_TIER_SOURCES = {
    Tier.GOLD: "reviewed_gold",
    Tier.SILVER: "reviewed_silver",
}


@dataclass(frozen=True, slots=True)
class SqlEligibilityCheck:
    metadata: PaperMetadata | None = None
    metadata_path: str | Path | None = None
    md_path: str | Path | None = None


@dataclass(frozen=True, slots=True)
class SqlEligibilityResult:
    eligible: bool
    paper_id: str
    reasons: list[str] = field(default_factory=list)
    blocking_fields: list[str] = field(default_factory=list)
    suggested_next_action: str = ""
    ready_for_sql_preview: bool = False
    warnings: list[str] = field(default_factory=list)


def check_sql_eligibility(check: SqlEligibilityCheck) -> SqlEligibilityResult:
    metadata_path = Path(check.metadata_path) if check.metadata_path is not None else None
    md_path = Path(check.md_path) if check.md_path is not None else None
    metadata = check.metadata or _load_metadata(metadata_path)

    reasons: list[str] = []
    blocking_fields: list[str] = []
    formal_result = check_formal_candidate(
        metadata,
        metadata_path=metadata_path,
        markdown_path=md_path,
    )
    warnings = list(formal_result.warnings)

    if metadata_path is None or not metadata_path.exists():
        _block(
            reasons,
            blocking_fields,
            "metadata_path",
            "metadata file is missing",
        )
    if md_path is None or not md_path.exists():
        _block(reasons, blocking_fields, "md_path", "markdown file is missing")
    _merge_formal_result(formal_result, reasons, blocking_fields)
    if metadata is None:
        _block(reasons, blocking_fields, "metadata", "metadata is missing or invalid")
        return SqlEligibilityResult(
            eligible=False,
            paper_id="",
            reasons=reasons,
            blocking_fields=blocking_fields,
            suggested_next_action=_suggest_next_action(blocking_fields),
            ready_for_sql_preview=False,
            warnings=warnings,
        )

    paper_id = f"{metadata.id}_v{metadata.version}"
    _check_sql_upload_fields(metadata, reasons, blocking_fields)
    eligible = not blocking_fields
    return SqlEligibilityResult(
        eligible=eligible,
        paper_id=paper_id,
        reasons=reasons,
        blocking_fields=blocking_fields,
        suggested_next_action=_suggest_next_action(blocking_fields),
        ready_for_sql_preview=eligible,
        warnings=warnings,
    )


def _check_sql_upload_fields(
    metadata: PaperMetadata,
    reasons: list[str],
    blocking_fields: list[str],
) -> None:
    tier = _coerce_tier(metadata.tier)
    index_status = _coerce_index_status(metadata.index_status)
    sql_status = _coerce_sql_status(metadata.sql_status)

    if tier is not None and _unreviewed_gold_candidate(metadata):
        _block(
            reasons,
            blocking_fields,
            "gold_candidate",
            "gold_candidate requires Codex review before SQL eligibility",
        )
    if (
        tier is not None
        and tier in _REVIEWED_TIER_SOURCES
        and not _reviewed_formal_tier(metadata, tier)
    ):
        _block(
            reasons,
            blocking_fields,
            "review_status",
            f"tier={tier.value} requires Codex/AI review before SQL eligibility",
        )

    if index_status is not IndexStatus.INDEXED:
        _block(
            reasons,
            blocking_fields,
            "index_status",
            "sql_index_status_not_indexed",
        )

    # Preserve the existing SQL retry policy: only not_uploaded may proceed.
    # failed remains blocked until a dedicated retry workflow is designed.
    if sql_status is not SqlStatus.NOT_UPLOADED:
        _block(
            reasons,
            blocking_fields,
            "sql_status",
            "sql_status_not_uploadable",
        )


def _load_metadata(path: Path | None) -> PaperMetadata | None:
    if path is None or not path.exists():
        return None
    try:
        return metadata_from_json(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, KeyError):
        return None


def _merge_formal_result(
    formal_result: EligibilityResult,
    reasons: list[str],
    blocking_fields: list[str],
) -> None:
    for reason in formal_result.blocking_reasons:
        _block(
            reasons,
            blocking_fields,
            _blocking_field_for_formal_reason(reason),
            reason,
        )


def _blocking_field_for_formal_reason(reason: str) -> str:
    if reason.startswith("metadata_field_defaulted:"):
        return reason.split(":", maxsplit=1)[1]
    return {
        "metadata_missing": "metadata_path",
        "metadata_invalid": "metadata",
        "markdown_missing": "md_path",
        "markdown_unreadable": "md_path",
        "sensitive_not_clean": "sensitive_clean",
        "source_hash_missing": "source_hash",
        "tier_not_formal": "tier",
        "version_not_current": "version_status",
        "duplicate_not_unique": "duplicate_status",
        "category_invalid": "category",
        "direction_invalid": "direction",
        "index_status_not_allowed": "index_status",
        "sql_status_not_allowed": "sql_status",
    }.get(reason, reason)


def _block(
    reasons: list[str],
    blocking_fields: list[str],
    field: str,
    reason: str,
) -> None:
    if field not in blocking_fields:
        blocking_fields.append(field)
    if reason not in reasons:
        reasons.append(reason)


def _suggest_next_action(blocking_fields: list[str]) -> str:
    if not blocking_fields:
        return "ready for SQL preview"
    if "gold_candidate" in blocking_fields:
        return "confirm gold candidate through Codex review before SQL preview"
    if "review_status" in blocking_fields:
        return "apply Codex/AI review decision before SQL preview"
    if "index_status" in blocking_fields:
        return "run index rebuild before SQL preview"
    if "metadata_path" in blocking_fields or "metadata" in blocking_fields:
        return "restore or regenerate metadata before SQL preview"
    if "md_path" in blocking_fields:
        return "restore formal markdown before SQL preview"
    if "sensitive_clean" in blocking_fields:
        return "finish sensitive content cleanup before SQL preview"
    if "source_hash" in blocking_fields:
        return "restore source hash before SQL preview"
    return "resolve blocking fields before SQL preview"


def _coerce_tier(value: Tier | str) -> Tier | None:
    if isinstance(value, Tier):
        return value
    try:
        return Tier(value)
    except ValueError:
        return None


def _unreviewed_gold_candidate(metadata: PaperMetadata) -> bool:
    return bool(metadata.gold_candidate) and (
        metadata.needs_codex_review
        or metadata.tier_decision_source == "auto_quality_suggestion"
    )


def _reviewed_formal_tier(metadata: PaperMetadata, tier: Tier) -> bool:
    return metadata.tier_decision_source == _REVIEWED_TIER_SOURCES[tier]


def _coerce_index_status(value: IndexStatus | str) -> IndexStatus | None:
    if isinstance(value, IndexStatus):
        return value
    try:
        return IndexStatus(value)
    except ValueError:
        return None


def _coerce_sql_status(value: SqlStatus | str) -> SqlStatus | None:
    if isinstance(value, SqlStatus):
        return value
    try:
        return SqlStatus(value)
    except ValueError:
        return None
