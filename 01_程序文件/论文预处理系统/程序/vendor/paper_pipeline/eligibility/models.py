"""Shared eligibility result models."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path


class EligibilityScope(str, Enum):
    FORMAL_CANDIDATE = "formal_candidate"
    REVIEW_ONLY = "review_only"
    AUDIT_ONLY = "audit_only"
    STAGE0_AUDIT_ONLY = "stage0_audit_only"
    BLOCKED = "blocked"


@dataclass(frozen=True, slots=True)
class EligibilityResult:
    paper_id: str | None
    version: int | None
    scope: EligibilityScope
    eligible: bool
    blocking_reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metadata_path: Path | None = None
    markdown_path: Path | None = None
    source_hash: str | None = None
    tier: str | None = None
    version_status: str | None = None
    duplicate_status: str | None = None
    sensitive_clean: bool | None = None
    index_status: str | None = None
    sql_status: str | None = None
    disposition: str | None = None
