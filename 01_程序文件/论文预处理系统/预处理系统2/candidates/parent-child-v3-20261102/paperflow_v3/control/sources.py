"""Discover only explicitly selected local folders using first-step rules."""

import os
from pathlib import Path, PurePosixPath

from .classification import first_step_modules


def discover_sources(source_paths, first_step_root, *, forbidden_roots=()):
    discover_groups = first_step_modules(first_step_root)["control"].discover_groups
    sources = []
    for raw in source_paths:
        source = Path(raw).expanduser()
        if not source.is_absolute() or not source.is_dir() or not source.name:
            raise ValueError("请选择存在的本机资料文件夹")
        if source.is_symlink() or source.is_junction():
            raise ValueError("资料文件夹不能是链接")
        sources.append(source.resolve())
    for index, source in enumerate(sources):
        if any(source == other or source.is_relative_to(other) or other.is_relative_to(source)
               for other in sources[:index]):
            raise ValueError("选择的资料文件夹存在重叠")
        for raw in forbidden_roots:
            root = Path(raw).resolve()
            if source == root or source.is_relative_to(root) or root.is_relative_to(source):
                raise ValueError("资料文件夹不能与工作目录重叠")

    groups, ungrouped = [], []
    for source in sources:
        relative_paths = []

        def fail(error):
            raise ValueError("资料文件夹无法完整读取") from error

        for current, dirs, files in os.walk(source, followlinks=False, onerror=fail):
            current = Path(current)
            for name in [*dirs, *files]:
                path = current / name
                if path.is_symlink() or path.is_junction():
                    raise ValueError("资料文件夹包含链接，无法确认归属")
            for name in files:
                relative_paths.append((PurePosixPath(source.name) /
                    (current / name).relative_to(source).as_posix()).as_posix())
        found, loose = discover_groups(relative_paths, source.name)
        library = source.parent if source.name in found else source
        groups.extend({"group_path": str(source if group == source.name else
                      source.joinpath(*PurePosixPath(group).parts[1:])), "library_root": str(library)}
                      for group in found)
        ungrouped.extend(str(source.joinpath(*PurePosixPath(path).parts[1:])) for path in loose)
    if len(groups) > 30000:
        raise ValueError("候选资料组超过 30000 组")
    return {"groups": groups, "ungrouped": ungrouped}
