"""Read-only file candidate selection for the workbench."""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from paper_pipeline.app.app_state import WorkbenchFileItem
from paper_pipeline.source_status_markers import (
    LOCKED_SOURCE_STATUSES,
    read_source_status_marker,
    write_source_status_marker,
)

SUPPORTED_SUFFIXES = {".pdf", ".docx", ".doc", ".txt", ".md", ".zip", ".rar", ".7z"}
DEFAULT_MAX_SCAN_ENTRIES = 2_000

__all__ = [
    "CandidateSelection",
    "PreparedSelectedInput",
    "discover_candidates",
    "is_selectable_candidate",
    "prepare_selected_input_dir",
    "route_for_path",
    "write_source_status_marker",
]


@dataclass(frozen=True, slots=True)
class CandidateSelection:
    items: list[WorkbenchFileItem]

    @classmethod
    def from_paths(cls, paths: list[str | Path]) -> CandidateSelection:
        return discover_candidates(paths)

    def selected_items(self) -> list[WorkbenchFileItem]:
        return [item for item in self.items if item.selected and is_selectable_candidate(item)]

    def set_selected(self, full_path: str, selected: bool) -> CandidateSelection:
        updated = []
        for item in self.items:
            if item.full_path == full_path:
                next_selected = selected and is_selectable_candidate(item)
                updated.append(
                    WorkbenchFileItem(
                        source_path=item.source_path,
                        file_name=item.file_name,
                        status=item.status,
                        reason=item.reason,
                        final_markdown_path=item.final_markdown_path,
                        metadata_path=item.metadata_path,
                        selected=next_selected,
                        full_path=item.full_path,
                        file_type=item.file_type,
                        size_bytes=item.size_bytes,
                        supported=item.supported,
                        route=item.route,
                        stage=item.stage,
                    ),
                )
            else:
                updated.append(item)
        return CandidateSelection(updated)


@dataclass(frozen=True, slots=True)
class PreparedSelectedInput:
    input_dir: Path
    manifest_path: Path
    selected_count: int


def discover_candidates(
    paths: list[str | Path],
    *,
    max_entries: int = DEFAULT_MAX_SCAN_ENTRIES,
) -> CandidateSelection:
    discovered: dict[Path, WorkbenchFileItem] = {}
    visited_entries = 0
    for raw_path in paths:
        path = Path(raw_path)
        candidates = path.rglob("*") if path.is_dir() else (path,)
        for candidate in candidates:
            visited_entries += 1
            if visited_entries > max_entries:
                return CandidateSelection(list(discovered.values()))
            try:
                if not candidate.is_file():
                    continue
                resolved = candidate.resolve()
                size_bytes = candidate.stat().st_size
            except OSError:
                continue
            if resolved in discovered:
                continue
            suffix = candidate.suffix.lower()
            supported = suffix in SUPPORTED_SUFFIXES
            status_marker = read_source_status_marker(candidate)
            status = _candidate_status(supported, status_marker)
            selectable = supported and status not in LOCKED_SOURCE_STATUSES
            discovered[resolved] = WorkbenchFileItem(
                source_path=str(candidate),
                full_path=str(candidate),
                file_name=candidate.name,
                file_type=suffix.removeprefix("."),
                size_bytes=size_bytes,
                supported=supported,
                selected=selectable,
                route=route_for_path(candidate),
                status=status,
                stage="not_started",
                reason=_candidate_reason(supported, status_marker),
            )
    return CandidateSelection(list(discovered.values()))


def is_selectable_candidate(item: WorkbenchFileItem) -> bool:
    return bool(item.supported and item.status not in LOCKED_SOURCE_STATUSES)


def route_for_path(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        return "PDF：MinerU 主解析，Docling 备用"
    if suffix == ".docx":
        return "DOCX：Docling 主解析，LibreOffice 转 PDF 后 MinerU 备用，docx_xml 兜底"
    if suffix == ".doc":
        return "DOC：LibreOffice 转 DOCX 后 Docling 主解析，或转 PDF 后 MinerU 备用"
    if suffix in {".txt", ".md"}:
        return "文本：本地文本解析"
    if suffix == ".zip":
        return "ZIP：内置解压后逐项准入"
    if suffix in {".rar", ".7z"}:
        return "RAR/7Z：需要 7z、7zz、7za 或 unrar"
    return "不支持：进入待处理或不支持状态"


def _candidate_status(supported: bool, marker: dict[str, Any] | None) -> str:
    if not supported:
        return "pending"
    if marker is None:
        return "queued"
    status = str(marker.get("status", "")).strip().lower()
    if status in LOCKED_SOURCE_STATUSES:
        return status
    return "queued"


def _candidate_reason(supported: bool, marker: dict[str, Any] | None) -> str:
    if not supported:
        return "不支持的文件类型"
    if marker is None:
        return ""
    status = str(marker.get("status", "")).strip().lower()
    run_id = str(marker.get("run_id", "")).strip()
    target_table = str(marker.get("target_table", "")).strip()
    if status == "uploaded":
        return _join_reason("已上传", run_id, target_table)
    if status == "processed":
        return _join_reason("已处理", run_id, target_table)
    if status == "processing":
        return _join_reason("其他运行正在处理", run_id, target_table)
    return ""


def _join_reason(*parts: str) -> str:
    return " | ".join(part for part in parts if part)


def prepare_selected_input_dir(
    selection: CandidateSelection,
    *,
    workbench_dir: str | Path,
    run_token: str,
) -> PreparedSelectedInput:
    selected = selection.selected_items()
    root = Path(workbench_dir) / "selected_input" / run_token
    input_dir = root / "files"
    if root.exists():
        shutil.rmtree(root)
    input_dir.mkdir(parents=True, exist_ok=True)
    files = []
    used_names: set[str] = set()
    for item in selected:
        source = Path(item.full_path)
        safe_name = _unique_name(source.name, used_names)
        destination = input_dir / safe_name
        shutil.copy2(source, destination)
        files.append(
            {
                "original_path": str(source),
                "prepared_path": str(destination),
                "file_name": source.name,
                "size_bytes": source.stat().st_size,
            },
        )
    manifest_path = root / "selection_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {"run_token": run_token, "selected_count": len(files), "files": files},
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return PreparedSelectedInput(input_dir, manifest_path, len(files))


def _unique_name(name: str, used_names: set[str]) -> str:
    if name not in used_names:
        used_names.add(name)
        return name
    stem = Path(name).stem
    suffix = Path(name).suffix
    index = 2
    while True:
        candidate = f"{stem}_{index}{suffix}"
        if candidate not in used_names:
            used_names.add(candidate)
            return candidate
        index += 1
