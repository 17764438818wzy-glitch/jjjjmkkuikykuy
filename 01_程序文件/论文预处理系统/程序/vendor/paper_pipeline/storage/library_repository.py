"""Formal library archive repository for Markdown and metadata pairs."""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from paper_pipeline.domain import PaperMetadata, Tier
from paper_pipeline.source_status_markers import (
    resolve_workbench_original_source,
    write_source_status_marker,
)
from paper_pipeline.storage.id_allocator import (
    formal_stem,
    format_formal_markdown_name,
    format_formal_metadata_name,
)
from paper_pipeline.storage.metadata_writer import MetadataWriter, validate_formal_metadata
from paper_pipeline.taxonomy.categories import validate_category
from paper_pipeline.taxonomy.directions import validate_direction

if TYPE_CHECKING:
    from paper_pipeline.stage0.audit import Stage0RejectedRecord


FORMAL_LIBRARY_TIERS = {Tier.GOLD, Tier.SILVER, Tier.PENDING, Tier.REJECTED}


@dataclass(frozen=True, slots=True)
class LibraryArchiveResult:
    markdown_path: Path
    metadata_path: Path


@dataclass(slots=True)
class LibraryRepository:
    library_dir: str | Path
    metadata_writer: MetadataWriter = field(default_factory=MetadataWriter)

    def archive_markdown(
        self,
        markdown_source: str | Path,
        metadata: PaperMetadata,
    ) -> LibraryArchiveResult:
        validate_formal_metadata(metadata)
        source = Path(markdown_source)
        if not source.exists():
            raise FileNotFoundError(source)

        result = self.paths_for(metadata)
        result.markdown_path.parent.mkdir(parents=True, exist_ok=True)
        self.remove_current_duplicates(
            stem=formal_stem(metadata),
            keep_paths={result.markdown_path, result.metadata_path},
        )
        if source.resolve() != result.markdown_path.resolve():
            shutil.copy2(source, result.markdown_path)
        self.metadata_writer.write_metadata(metadata, result.metadata_path)
        _write_processed_source_marker(metadata)
        return result

    def paths_for(self, metadata: PaperMetadata) -> LibraryArchiveResult:
        stem = formal_stem(metadata)
        directory = self.directory_for(metadata) / stem
        return LibraryArchiveResult(
            markdown_path=directory / format_formal_markdown_name(metadata),
            metadata_path=directory / format_formal_metadata_name(metadata),
        )

    def directory_for(self, metadata: PaperMetadata) -> Path:
        tier = _formal_storage_tier(metadata.tier)
        if tier is Tier.STAGE0_REJECTED:
            raise ValueError("stage0_rejected cannot enter formal library tiers")

        if tier is Tier.PENDING:
            category = metadata.category.strip()
            direction = metadata.direction.strip()
            if (
                category
                and direction
                and validate_category(category).is_valid
                and validate_direction(category, direction).is_valid
            ):
                return Path(self.library_dir) / tier.value / category / direction
            return Path(self.library_dir) / tier.value / "unknown" / "unknown"

        return Path(self.library_dir) / tier.value / metadata.category / metadata.direction

    def record_stage0_rejected(self, record: Stage0RejectedRecord) -> Path:
        del record
        raise ValueError("stage0_rejected audit records are not final library archives")

    def remove_current_duplicates(
        self,
        *,
        stem: str,
        keep_paths: set[Path] | None = None,
    ) -> None:
        keep_resolved = {path.resolve() for path in keep_paths or set()}
        keep_parent_resolved = {path.parent.resolve() for path in keep_paths or set()}
        root = Path(self.library_dir)
        if not root.exists():
            return
        for path in root.rglob(stem):
            if not path.is_dir():
                continue
            if _is_under_non_formal_root(path, root):
                continue
            if path.resolve() in keep_resolved or path.resolve() in keep_parent_resolved:
                continue
            shutil.rmtree(path)

        for path in root.rglob(f"{stem}*"):
            if _is_under_non_formal_root(path, root):
                continue
            if path.suffix not in {".md", ".json"}:
                continue
            if path.resolve() in keep_resolved:
                continue
            if path.name in {f"{stem}.md", f"{stem}.metadata.json"}:
                path.unlink()


def _formal_storage_tier(tier: Tier | str) -> Tier:
    resolved = tier if isinstance(tier, Tier) else Tier(tier)
    if resolved is Tier.MANUAL_REVIEW_REQUIRED:
        return Tier.PENDING
    return resolved


def _is_under_non_formal_root(path: Path, root: Path) -> bool:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return True
    if not relative.parts:
        return True
    return relative.parts[0] not in {tier.value for tier in FORMAL_LIBRARY_TIERS}


def _write_processed_source_marker(metadata: PaperMetadata) -> None:
    source = resolve_workbench_original_source(metadata.source_path)
    if source is None:
        return
    if not source.exists() or not source.is_file():
        return
    write_source_status_marker(
        source,
        status="processed",
        run_id=metadata.run_id,
        paper_id=f"{metadata.id}_v{metadata.version}",
        extra={
            "tier": str(metadata.tier),
            "category": metadata.category,
            "direction": metadata.direction,
        },
    )
