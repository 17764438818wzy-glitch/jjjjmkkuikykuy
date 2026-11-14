"""Read-only index status view models for the workbench."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from paper_pipeline.domain import IndexStatus, PaperMetadata, Tier
from paper_pipeline.domain.serialization import metadata_from_json


@dataclass(frozen=True, slots=True)
class IndexStatusRow:
    paper_id: str
    title: str
    tier: str
    index_status: str
    reason: str
    metadata_path: str
    markdown_path: str


@dataclass(frozen=True, slots=True)
class WorkbenchIndexState:
    total_count: int = 0
    needs_rebuild_count: int = 0
    indexed_count: int = 0
    failed_count: int = 0
    rebuild_enabled: bool = False
    rows: list[IndexStatusRow] = field(default_factory=list)
    message: str = "索引功能预留：当前只展示状态，不执行重建。"


def build_index_state(library_dir: str | Path) -> WorkbenchIndexState:
    root = Path(library_dir)
    if not root.exists():
        return WorkbenchIndexState()

    rows: list[IndexStatusRow] = []
    total_count = 0
    indexed_count = 0
    needs_rebuild_count = 0
    failed_count = 0

    for metadata_path in _iter_metadata_paths(root):
        metadata = _load_metadata(metadata_path)
        if metadata is None:
            continue
        total_count += 1
        status = _coerce_index_status(metadata.index_status)
        if status is IndexStatus.INDEXED:
            indexed_count += 1
            continue
        if status is IndexStatus.NEEDS_REBUILD:
            needs_rebuild_count += 1
        if status is IndexStatus.FAILED:
            failed_count += 1
        rows.append(
            IndexStatusRow(
                paper_id=f"{metadata.id}_v{metadata.version}",
                title=metadata.title,
                tier=_coerce_tier(metadata.tier).value,
                index_status=status.value,
                reason=_index_reason(metadata, status),
                metadata_path=str(metadata_path),
                markdown_path=str(_markdown_path_for_metadata(metadata_path)),
            ),
        )

    return WorkbenchIndexState(
        total_count=total_count,
        needs_rebuild_count=needs_rebuild_count,
        indexed_count=indexed_count,
        failed_count=failed_count,
        rebuild_enabled=False,
        rows=rows,
    )


def _iter_metadata_paths(root: Path):
    seen: set[Path] = set()
    for pattern in ("metadata.json", "*.metadata.json"):
        for path in sorted(root.rglob(pattern)):
            if path in seen or _is_under(path, root, "runs") or _is_under(path, root, "indexes"):
                continue
            seen.add(path)
            yield path


def _load_metadata(path: Path) -> PaperMetadata | None:
    try:
        return metadata_from_json(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError):
        return None


def _markdown_path_for_metadata(metadata_path: Path) -> Path:
    if metadata_path.name == "metadata.json":
        return metadata_path.parent / f"{metadata_path.parent.name}.md"
    return metadata_path.with_name(metadata_path.name.removesuffix(".metadata.json") + ".md")


def _index_reason(metadata: PaperMetadata, status: IndexStatus) -> str:
    tier = _coerce_tier(metadata.tier)
    if tier not in {Tier.GOLD, Tier.SILVER}:
        return "等待复审通过后再进入索引。"
    if status is IndexStatus.NEEDS_REBUILD:
        return "复审通过后需要重建索引。"
    if status is IndexStatus.FAILED:
        return "上次索引失败，等待索引页实现重试。"
    return "尚未建立索引。"


def _coerce_index_status(value: IndexStatus | str) -> IndexStatus:
    if isinstance(value, IndexStatus):
        return value
    return IndexStatus(value)


def _coerce_tier(value: Tier | str) -> Tier:
    if isinstance(value, Tier):
        return value
    return Tier(value)


def _is_under(path: Path, root: Path, directory_name: str) -> bool:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return False
    return bool(relative.parts) and relative.parts[0] == directory_name
