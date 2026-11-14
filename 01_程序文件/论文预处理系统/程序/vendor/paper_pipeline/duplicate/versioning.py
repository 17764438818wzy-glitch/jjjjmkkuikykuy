"""Version relationship helpers layered on task 09 identity semantics."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

from paper_pipeline.domain import PaperMetadata, Tier, VersionStatus


@dataclass(frozen=True, slots=True)
class VersionRelationship:
    is_new_version: bool
    paper_id: str = ""
    next_version: int = 1
    new_version_status: VersionStatus = VersionStatus.CURRENT
    old_versions_to_keep: list[str] = field(default_factory=list)
    reason: str = ""


def determine_version_relationship(
    source_hash: str,
    existing_metadata: Sequence[PaperMetadata],
) -> VersionRelationship:
    if not source_hash:
        raise ValueError("source_hash is required")

    same_hash_records = [
        metadata
        for metadata in existing_metadata
        if metadata.source_hash == source_hash and _eligible_for_versioning(metadata)
    ]
    if not same_hash_records:
        return VersionRelationship(
            is_new_version=False,
            next_version=1,
            new_version_status=VersionStatus.CURRENT,
            reason="未发现同 source_hash 既有正式记录，按新论文处理",
        )

    latest = max(same_hash_records, key=lambda metadata: metadata.version)
    return VersionRelationship(
        is_new_version=True,
        paper_id=latest.id,
        next_version=latest.version + 1,
        new_version_status=VersionStatus.CURRENT,
        old_versions_to_keep=[
            f"{metadata.id}_v{metadata.version}"
            for metadata in sorted(
                same_hash_records,
                key=lambda item: (item.id, item.version),
            )
        ],
        reason="同一 source_hash 重新处理，复用 accession id 并递增版本",
    )


def _eligible_for_versioning(metadata: PaperMetadata) -> bool:
    tier = metadata.tier if isinstance(metadata.tier, Tier) else Tier(metadata.tier)
    return tier is not Tier.STAGE0_REJECTED
