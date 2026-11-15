"""Install the Windows desktop shortcut for the workbench."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

APP_TITLE = "论文流水线工作台"

ShortcutRunner = Callable[[list[str]], object]


@dataclass(frozen=True, slots=True)
class DesktopShortcutSpec:
    shortcut_path: Path
    target_path: Path
    arguments: str
    working_directory: Path
    launcher_path: Path | None = None
    icon_path: Path | None = None


def build_desktop_shortcut_spec(
    *,
    desktop_dir: str | Path | None = None,
    pythonw_path: str | Path | None = None,
    launcher_path: str | Path | None = None,
    base_dir: str | Path | None = None,
) -> DesktopShortcutSpec:
    desktop = Path(desktop_dir) if desktop_dir is not None else _default_desktop_dir()
    target = Path(pythonw_path) if pythonw_path is not None else _default_pythonw_path()
    launcher = (
        Path(launcher_path)
        if launcher_path is not None
        else _default_launcher_path()
    )
    workbench_base = (
        Path(base_dir)
        if base_dir is not None
        else Path.home() / ".paper_pipeline_workbench"
    )
    return DesktopShortcutSpec(
        shortcut_path=desktop / f"{APP_TITLE}.lnk",
        target_path=target,
        arguments=_shortcut_arguments(launcher, workbench_base),
        working_directory=Path.cwd(),
        launcher_path=launcher,
    )


def install_desktop_shortcut(
    *,
    desktop_dir: str | Path | None = None,
    pythonw_path: str | Path | None = None,
    launcher_path: str | Path | None = None,
    base_dir: str | Path | None = None,
    runner: ShortcutRunner | None = None,
) -> Path:
    spec = build_desktop_shortcut_spec(
        desktop_dir=desktop_dir,
        pythonw_path=pythonw_path,
        launcher_path=launcher_path,
        base_dir=base_dir,
    )
    spec.shortcut_path.parent.mkdir(parents=True, exist_ok=True)
    command = [
        "powershell",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-Command",
        _shortcut_script(spec),
    ]
    if runner is None:
        subprocess.run(command, check=True)
    else:
        runner(command)
    return spec.shortcut_path


def _shortcut_script(spec: DesktopShortcutSpec) -> str:
    lines = [
        "$shell = New-Object -ComObject WScript.Shell",
        f"$shortcut = $shell.CreateShortcut({_ps_quote(str(spec.shortcut_path))})",
        f"$shortcut.TargetPath = {_ps_quote(str(spec.target_path))}",
        f"$shortcut.Arguments = {_ps_quote(spec.arguments)}",
        f"$shortcut.WorkingDirectory = {_ps_quote(str(spec.working_directory))}",
        f"$shortcut.Description = {_ps_quote(APP_TITLE)}",
    ]
    if spec.icon_path is not None:
        lines.append(f"$shortcut.IconLocation = {_ps_quote(str(spec.icon_path))}")
    lines.append("$shortcut.Save()")
    return "; ".join(lines)


def _shortcut_arguments(launcher: Path | None, base_dir: Path) -> str:
    if launcher is not None:
        return subprocess.list2cmdline(
            [str(launcher), "--base-dir", str(base_dir)],
        )
    return subprocess.list2cmdline(
        ["-m", "paper_pipeline.app.windows_app", "--base-dir", str(base_dir)],
    )


def _ps_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _default_desktop_dir() -> Path:
    userprofile = os.environ.get("USERPROFILE")
    if userprofile:
        return Path(userprofile) / "Desktop"
    return Path.home() / "Desktop"


def _default_pythonw_path() -> Path:
    project_pythonw = Path.cwd() / ".venv" / "Scripts" / "pythonw.exe"
    if project_pythonw.exists():
        return project_pythonw
    executable = Path(sys.executable)
    if executable.name.lower() == "python.exe":
        pythonw = executable.with_name("pythonw.exe")
        if pythonw.exists():
            return pythonw
    return executable


def _default_launcher_path() -> Path | None:
    repo_root = Path(__file__).resolve().parents[3]
    launcher = repo_root / "launch_paper_pipeline_workbench.pyw"
    if launcher.exists():
        return launcher
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="paper-pipeline-install-shortcut")
    parser.add_argument("--desktop-dir", default="", help="桌面目录；默认使用当前用户桌面")
    parser.add_argument("--pythonw", default="", help="pythonw.exe 路径；默认自动检测")
    parser.add_argument(
        "--launcher",
        default="",
        help="工作台 .pyw 启动器路径；默认使用仓库根目录 launch_paper_pipeline_workbench.pyw",
    )
    parser.add_argument(
        "--base-dir",
        default="",
        help="工作台安全本地目录；默认使用用户目录下 .paper_pipeline_workbench",
    )
    args = parser.parse_args(argv)
    path = install_desktop_shortcut(
        desktop_dir=args.desktop_dir or None,
        pythonw_path=args.pythonw or None,
        launcher_path=args.launcher or None,
        base_dir=args.base_dir or None,
    )
    print(f"已创建桌面快捷方式：{path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
