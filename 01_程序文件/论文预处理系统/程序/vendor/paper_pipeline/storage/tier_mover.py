"""Move current formal library files when the final tier changes."""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path

from paper_pipeline.domain import (
    PaperMetadata,
    ReviewHistoryItem,
    SqlStatus,
    Tier,
)
from paper_pipeline.domain.serialization import metadata_from_json
from paper_pipeline.storage.id_allocator import formal_stem
from paper_pipeline.storage.library_repository import LibraryArchiveResult, LibraryRepository
from paper_pipeline.storage.metadata_writer import MetadataWriter


@dataclass(slots=True)
class TierMover:
    library_dir: str | Path
    metadata_writer: MetadataWriter = field(default_factory=MetadataWriter)

    def move_current_version(
        self,
        *,
        paper_id: str,
        version: int,
        new_tier: Tier,
        category: str = "",
        direction: str = "",
        reviewer: str = "orchestrator",
        reason: str = "",
    ) -> LibraryArchiveResult:
        current = self._find_current_pair(paper_id=paper_id, version=version)
        metadata = metadata_from_json(current.metadata_path.read_text(encoding="utf-8"))
        if not can_change_tier(metadata):
            raise ValueError("uploaded SQL records cannot change tier in task 09")

        old_tier = metadata.tier
        resolved_tier = new_tier if isinstance(new_tier, Tier) else Tier(new_tier)
        if old_tier in {Tier.GOLD, Tier.SILVER} and resolved_tier is Tier.REJECTED:
            if not reason.strip():
                raise ValueError("reason is required when rejecting gold or silver")

        metadata.tier = resolved_tier
        if category:
            metadata.category = category
        if direction:
            metadata.direction = direction
        metadata.add_review_history(
            ReviewHistoryItem(
                reviewer=reviewer,
                action="tier_changed",
                comment=(
                    f"{old_tier.value} -> {resolved_tier.value}: {reason.strip()}"
                    if reason.strip()
                    else f"{old_tier.value} -> {resolved_tier.value}"
                ),
            ),
        )

        repository = LibraryRepository(
            self.library_dir,
            metadata_writer=self.metadata_writer,
        )
        destination = repository.paths_for(metadata)
        if current.markdown_path.resolve() != destination.markdown_path.resolve():
            if destination.markdown_path.parent.exists():
                shutil.rmtree(destination.markdown_path.parent)
            destination.markdown_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(current.markdown_path), destination.markdown_path)
        else:
            destination.markdown_path.parent.mkdir(parents=True, exist_ok=True)
        self.metadata_writer.write_metadata(metadata, destination.metadata_path)
        if current.metadata_path.resolve() != destination.metadata_path.resolve():
            current.metadata_path.unlink(missing_ok=True)
        _remove_empty_entry_dir(current.metadata_path.parent)
        repository.remove_current_duplicates(
            stem=formal_stem(metadata),
            keep_paths={destination.markdown_path, destination.metadata_path},
        )
        return destination

    def _find_current_pair(self, *, paper_id: str, version: int) -> LibraryArchiveResult:
        stem = f"{paper_id}_v{version}"
        root = Path(self.library_dir)
        metadata_paths = [
            path
            for path in root.rglob("metadata.json")
            if path.parent.name == stem and not _is_under_non_formal_root(path, root)
        ]
        if not metadata_paths:
            raise FileNotFoundError(f"{stem}/metadata.json")
        if len(metadata_paths) > 1:
            raise ValueError(f"multiple current metadata files found for {stem}")

        metadata_path = metadata_paths[0]
        markdown_path = metadata_path.parent / f"{stem}.md"
        if not markdown_path.exists():
            raise FileNotFoundError(markdown_path)
        return LibraryArchiveResult(
            markdown_path=markdown_path,
            metadata_path=metadata_path,
        )


def can_change_tier(metadata: PaperMetadata) -> bool:
    sql_status = (
        metadata.sql_status
        if isinstance(metadata.sql_status, SqlStatus)
        else SqlStatus(metadata.sql_status)
    )
    return sql_status is not SqlStatus.UPLOADED


def _is_under_non_formal_root(path: Path, root: Path) -> bool:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return True
    return not relative.parts or relative.parts[0] not in {
        Tier.GOLD.value,
        Tier.SILVER.value,
        Tier.PENDING.value,
        Tier.REJECTED.value,
    }


def _remove_empty_entry_dir(path: Path) -> None:
    try:
        path.rmdir()
    except OSError:
        return
