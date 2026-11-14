"""Status enums shared by pipeline modules."""

from __future__ import annotations

from enum import StrEnum


class Tier(StrEnum):
    GOLD = "gold"
    SILVER = "silver"
    PENDING = "pending"
    MANUAL_REVIEW_REQUIRED = "manual_review_required"
    REJECTED = "rejected"
    STAGE0_REJECTED = "stage0_rejected"


class SqlStatus(StrEnum):
    NOT_UPLOADED = "not_uploaded"
    UPLOADED = "uploaded"
    FAILED = "failed"
    WITHDRAWN = "withdrawn"
    REPLACED = "replaced"


class IndexStatus(StrEnum):
    NOT_INDEXED = "not_indexed"
    INDEXED = "indexed"
    FAILED = "failed"
    NEEDS_REBUILD = "needs_rebuild"


class VersionStatus(StrEnum):
    CURRENT = "current"
    REPLACED = "replaced"
    WITHDRAWN = "withdrawn"
    ABANDONED = "abandoned"


class DuplicateStatus(StrEnum):
    UNIQUE = "unique"
    EXACT_DUPLICATE = "exact_duplicate"
    SUSPECTED_DUPLICATE = "suspected_duplicate"
    MERGED_AS_VERSION = "merged_as_version"


class ProcessingPlanStatus(StrEnum):
    DRAFT = "draft"
    CONFIRMED = "confirmed"
    CANCELLED = "cancelled"


class RunStatus(StrEnum):
    CREATED = "created"
    PLANNED = "planned"
    CONFIRMED = "confirmed"
    RUNNING = "running"
    PAUSING = "pausing"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    INTERRUPTED = "interrupted"
    ABANDONED = "abandoned"
    COMPLETED = "completed"
    FAILED = "failed"


class FileRuntimeStatus(StrEnum):
    NOT_STARTED = "not_started"
    RUNNING = "running"
    COMPLETED = "completed"
    PENDING = "pending"
    FAILED = "failed"


class Stage0Decision(StrEnum):
    ACCEPT = "accept"
    STAGE0_REJECT = "stage0_reject"
    PENDING = "pending"
