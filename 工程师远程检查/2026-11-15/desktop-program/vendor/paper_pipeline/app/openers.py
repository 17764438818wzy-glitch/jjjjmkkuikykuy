"""Safe wrappers for opening reports and directories from the workbench."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol


class PathOpener(Protocol):
    def open(self, path: Path) -> None:
        """Open a file or directory path."""


@dataclass(slots=True)
class OpenPathResult:
    opened: bool
    path: Path
    error: str = ""


@dataclass(slots=True)
class FakePathOpener:
    opened_paths: list[Path] = field(default_factory=list)

    def open(self, path: Path) -> None:
        self.opened_paths.append(path)


@dataclass(slots=True)
class SystemPathOpener:
    def open(self, path: Path) -> None:
        startfile = getattr(os, "startfile", None)
        if startfile is None:
            raise RuntimeError("当前系统不支持 os.startfile")
        startfile(str(path))


def open_report_path(
    path: str | Path,
    *,
    allowed_roots: list[str | Path],
    opener: PathOpener | None = None,
) -> OpenPathResult:
    candidate = Path(path)
    validation_error = _validate_open_path(candidate, allowed_roots=allowed_roots)
    if validation_error:
        return OpenPathResult(opened=False, path=candidate, error=validation_error)
    if not candidate.is_file():
        return OpenPathResult(
            opened=False,
            path=candidate,
            error="报告路径不是文件。",
        )
    return _open(candidate, opener)


def open_directory_path(
    path: str | Path,
    *,
    allowed_roots: list[str | Path],
    opener: PathOpener | None = None,
) -> OpenPathResult:
    candidate = Path(path)
    validation_error = _validate_open_path(candidate, allowed_roots=allowed_roots)
    if validation_error:
        return OpenPathResult(opened=False, path=candidate, error=validation_error)
    if not candidate.is_dir():
        return OpenPathResult(
            opened=False,
            path=candidate,
            error="目录路径不是文件夹。",
        )
    return _open(candidate, opener)


def _open(path: Path, opener: PathOpener | None) -> OpenPathResult:
    try:
        (opener or SystemPathOpener()).open(path)
    except OSError as exc:
        return OpenPathResult(opened=False, path=path, error=str(exc))
    return OpenPathResult(opened=True, path=path)


def _validate_open_path(
    path: Path,
    *,
    allowed_roots: list[str | Path],
) -> str:
    if _contains_input_raw(path):
        return "不能打开 input_raw 相关路径。"
    if not _is_under_allowed_root(path, allowed_roots):
        return "路径不在允许打开的 library / output_cleaned / reports / evidence 范围内。"
    if not path.exists():
        return "路径不存在。"
    return ""


def _contains_input_raw(path: Path) -> bool:
    return any(part.lower() == "input_raw" for part in path.parts)


def _is_under_allowed_root(path: Path, allowed_roots: list[str | Path]) -> bool:
    resolved = path.resolve(strict=False)
    for root in allowed_roots:
        root_path = Path(root).resolve(strict=False)
        if resolved == root_path or resolved.is_relative_to(root_path):
            return True
    return False
