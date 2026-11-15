"""Archive extraction entry points for task 06."""

from __future__ import annotations

import shutil
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from zipfile import BadZipFile, ZipFile

from paper_pipeline.storage.scanner import calculate_file_hash


@dataclass(slots=True)
class ArchiveMemberRecord:
    source_path: str
    member_name: str
    source_hash: str = ""
    source_size: int = 0


@dataclass(slots=True)
class ArchiveExtractionResult:
    archive_path: str
    work_dir: str
    run_id: str = ""
    success: bool = False
    members: list[ArchiveMemberRecord] = field(default_factory=list)
    error_reason: str = ""


ArchiveAdapter = Callable[[Path, Path], list[Path]]
SUPPORTED_ARCHIVE_SUFFIXES = {".zip", ".rar", ".7z"}
_DEPENDENCY_REQUIRED_MESSAGE = "需要 7z/7zz/7za/unrar 依赖"


def is_supported_archive(path: str | Path) -> bool:
    return Path(path).suffix.lower() in SUPPORTED_ARCHIVE_SUFFIXES


def extract_archive(
    archive_path: str | Path,
    *,
    work_dir: str | Path,
    run_id: str = "",
    adapters: dict[str, ArchiveAdapter] | None = None,
) -> ArchiveExtractionResult:
    archive = Path(archive_path)
    destination_root = _destination_for_archive(archive, Path(work_dir), run_id)
    suffix = archive.suffix.lower()
    if not is_supported_archive(archive):
        return _failure(
            archive=archive,
            work_dir=destination_root,
            run_id=run_id,
            reason=f"不支持的压缩包类型: {suffix or '<none>'}",
        )

    try:
        _ensure_safe_work_dir(destination_root)
        if suffix == ".zip":
            extracted_paths = _extract_zip(archive, destination_root)
        else:
            extracted_paths = _extract_external_archive(
                archive,
                destination_root,
                suffix=suffix,
                adapters=adapters or {},
            )
    except Exception as exc:
        return _failure(
            archive=archive,
            work_dir=destination_root,
            run_id=run_id,
            reason=f"解压失败: {exc}",
        )

    return ArchiveExtractionResult(
        archive_path=str(archive),
        work_dir=str(destination_root),
        run_id=run_id,
        success=True,
        members=[
            ArchiveMemberRecord(
                source_path=str(path),
                member_name=path.relative_to(destination_root).as_posix(),
                source_hash=calculate_file_hash(path),
                source_size=path.stat().st_size,
            )
            for path in sorted(extracted_paths, key=lambda item: item.as_posix())
            if path.is_file()
        ],
        error_reason="",
    )


def _extract_zip(archive: Path, destination_root: Path) -> list[Path]:
    extracted_paths: list[Path] = []
    try:
        with ZipFile(archive) as zip_file:
            for member in zip_file.infolist():
                if member.is_dir():
                    continue
                target_path = _safe_member_target(destination_root, member.filename)
                target_path.parent.mkdir(parents=True, exist_ok=True)
                with zip_file.open(member) as source, target_path.open("wb") as target:
                    shutil.copyfileobj(source, target)
                extracted_paths.append(target_path)
    except BadZipFile as exc:
        raise ValueError("zip 文件损坏或格式不正确") from exc
    return extracted_paths


def _extract_external_archive(
    archive: Path,
    destination_root: Path,
    *,
    suffix: str,
    adapters: dict[str, ArchiveAdapter],
) -> list[Path]:
    adapter = adapters.get(suffix)
    if adapter is None:
        raise RuntimeError(
            f"{suffix} 解压{_DEPENDENCY_REQUIRED_MESSAGE}，当前仅提供适配器接口",
        )
    extracted_paths = adapter(archive, destination_root)
    return [_safe_existing_output_path(destination_root, path) for path in extracted_paths]


def _safe_member_target(destination_root: Path, member_name: str) -> Path:
    normalized_name = member_name.replace("\\", "/")
    target_path = destination_root / normalized_name
    return _safe_output_path(destination_root, target_path)


def _safe_existing_output_path(destination_root: Path, path: Path) -> Path:
    target_path = Path(path)
    if not target_path.is_absolute():
        target_path = destination_root / target_path
    return _safe_output_path(destination_root, target_path)


def _safe_output_path(destination_root: Path, target_path: Path) -> Path:
    destination = destination_root.resolve(strict=False)
    resolved_target = target_path.resolve(strict=False)
    if not resolved_target.is_relative_to(destination):
        raise ValueError("压缩包成员路径越界，已拒绝解压")
    return resolved_target


def _ensure_safe_work_dir(work_dir: Path) -> None:
    if any(part.lower() == "input_raw" for part in work_dir.resolve(strict=False).parts):
        raise ValueError("解压工作目录不能位于 input_raw 内")


def _destination_for_archive(archive: Path, work_dir: Path, run_id: str) -> Path:
    run_part = run_id or "run"
    return work_dir / run_part / f"{archive.stem}_extracted"


def _failure(
    *,
    archive: Path,
    work_dir: Path,
    run_id: str,
    reason: str,
) -> ArchiveExtractionResult:
    return ArchiveExtractionResult(
        archive_path=str(archive),
        work_dir=str(work_dir),
        run_id=run_id,
        success=False,
        members=[],
        error_reason=reason,
    )
