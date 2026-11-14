"""LibreOffice executable discovery and minimal conversion checks."""

from __future__ import annotations

import re
import shutil
import subprocess
import tempfile
import zipfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

LibreOfficeResolveStatus = Literal[
    "not-configured",
    "found",
    "executable-missing",
]
LibreOfficeConversionStatus = Literal["conversion-ok", "conversion-failed"]
PathExists = Callable[[Path], bool]
Which = Callable[[str], str | None]


@dataclass(frozen=True, slots=True)
class LibreOfficePathResolution:
    status: LibreOfficeResolveStatus
    path: Path | None = None
    source: str = ""
    message: str = ""


@dataclass(frozen=True, slots=True)
class LibreOfficeCommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


class LibreOfficeCommandRunner(Protocol):
    def run(
        self,
        command: list[str],
        *,
        cwd: Path | None = None,
        timeout_seconds: int,
    ) -> LibreOfficeCommandResult:
        """Run a LibreOffice command without invoking a shell."""


@dataclass(frozen=True, slots=True)
class LibreOfficeConversionCheck:
    status: LibreOfficeConversionStatus
    message: str
    output_path: Path | None = None
    command: list[str] | None = None


class SubprocessLibreOfficeRunner:
    def run(
        self,
        command: list[str],
        *,
        cwd: Path | None = None,
        timeout_seconds: int,
    ) -> LibreOfficeCommandResult:
        completed = subprocess.run(  # noqa: S603 - command is an argument list.
            command,
            cwd=cwd,
            timeout=timeout_seconds,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        return LibreOfficeCommandResult(
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )


def resolve_libreoffice_path(
    environ: Mapping[str, str] | None = None,
    *,
    which: Which | None = None,
    path_exists: PathExists | None = None,
) -> LibreOfficePathResolution:
    source = environ or {}
    lookup = which or shutil.which
    exists = path_exists or _default_path_exists

    for key in ("LIBREOFFICE_PATH", "DITY_LIBREOFFICE_PATH"):
        raw_value = str(source.get(key, "")).strip()
        if not raw_value:
            continue
        path = Path(raw_value)
        if exists(path):
            return LibreOfficePathResolution(
                status="found",
                path=path,
                source=key,
                message=f"LibreOffice executable found via {key}: {path}",
            )
        return LibreOfficePathResolution(
            status="executable-missing",
            path=path,
            source=key,
            message=f"{key} is configured but executable is missing: {path}",
        )

    for executable_name in ("soffice", "libreoffice", "soffice.exe", "libreoffice.exe"):
        discovered = lookup(executable_name)
        if discovered:
            path = Path(discovered)
            return LibreOfficePathResolution(
                status="found",
                path=path,
                source=f"PATH:{executable_name}",
                message=(
                    f"LibreOffice executable found on PATH via "
                    f"{executable_name}: {path}"
                ),
            )

    return LibreOfficePathResolution(
        status="not-configured",
        message=(
            "LibreOffice executable was not configured and was not found on PATH "
            "(searched LIBREOFFICE_PATH, DITY_LIBREOFFICE_PATH, soffice, libreoffice)."
        ),
    )


def build_libreoffice_conversion_command(
    executable_path: str | Path,
    source_path: str | Path,
    output_dir: str | Path,
    *,
    target_format: str = "pdf",
) -> list[str]:
    return [
        str(executable_path),
        "--headless",
        "--convert-to",
        target_format,
        "--outdir",
        str(output_dir),
        str(source_path),
    ]


def run_minimal_conversion_check(
    executable_path: str | Path,
    *,
    source_path: str | Path | None = None,
    target_format: str = "pdf",
    runner: LibreOfficeCommandRunner | None = None,
    timeout_seconds: int = 60,
    temp_dir: str | Path | None = None,
) -> LibreOfficeConversionCheck:
    command_runner = runner or SubprocessLibreOfficeRunner()
    if temp_dir is not None:
        destination = Path(temp_dir)
        destination.mkdir(parents=True, exist_ok=True)
        return _run_conversion_check(
            executable_path=Path(executable_path),
            source_path=Path(source_path) if source_path is not None else None,
            target_format=target_format,
            runner=command_runner,
            timeout_seconds=timeout_seconds,
            temp_dir=destination,
        )

    with tempfile.TemporaryDirectory(prefix="paper-pipeline-libreoffice-") as raw_dir:
        return _run_conversion_check(
            executable_path=Path(executable_path),
            source_path=Path(source_path) if source_path is not None else None,
            target_format=target_format,
            runner=command_runner,
            timeout_seconds=timeout_seconds,
            temp_dir=Path(raw_dir),
        )


def _run_conversion_check(
    *,
    executable_path: Path,
    source_path: Path | None,
    target_format: str,
    runner: LibreOfficeCommandRunner,
    timeout_seconds: int,
    temp_dir: Path,
) -> LibreOfficeConversionCheck:
    source = source_path or _write_minimal_odt(temp_dir / "minimal-libreoffice-check.odt")
    if not source.exists():
        return LibreOfficeConversionCheck(
            status="conversion-failed",
            message="LibreOffice conversion failed: sample office file is missing.",
        )

    command = build_libreoffice_conversion_command(
        executable_path,
        source,
        temp_dir,
        target_format=target_format,
    )
    try:
        result = runner.run(command, timeout_seconds=timeout_seconds)
    except Exception as exc:  # noqa: BLE001 - doctor reports capability failures.
        return LibreOfficeConversionCheck(
            status="conversion-failed",
            message=f"LibreOffice conversion failed: {_safe_error(str(exc))}",
            command=command,
        )

    if result.returncode != 0:
        return LibreOfficeConversionCheck(
            status="conversion-failed",
            message=(
                f"LibreOffice conversion failed with exit code {result.returncode}: "
                f"{_safe_command_output(result)}"
            ),
            command=command,
        )

    output_path = temp_dir / f"{source.stem}.{_target_suffix(target_format)}"
    if not output_path.exists():
        return LibreOfficeConversionCheck(
            status="conversion-failed",
            message=(
                "LibreOffice conversion failed: expected output was not created "
                f"({output_path.name})."
            ),
            command=command,
        )

    return LibreOfficeConversionCheck(
        status="conversion-ok",
        message=f"LibreOffice conversion-ok: created {output_path.name}.",
        output_path=output_path,
        command=command,
    )


def _write_minimal_odt(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "mimetype",
            "application/vnd.oasis.opendocument.text",
            compress_type=zipfile.ZIP_STORED,
        )
        archive.writestr(
            "META-INF/manifest.xml",
            """<?xml version="1.0" encoding="UTF-8"?>
<manifest:manifest xmlns:manifest="urn:oasis:names:tc:opendocument:xmlns:manifest:1.0" manifest:version="1.2">
  <manifest:file-entry manifest:full-path="/" manifest:media-type="application/vnd.oasis.opendocument.text"/>
  <manifest:file-entry manifest:full-path="content.xml" manifest:media-type="text/xml"/>
</manifest:manifest>
""",
        )
        archive.writestr(
            "content.xml",
            """<?xml version="1.0" encoding="UTF-8"?>
<office:document-content xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0" xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0" office:version="1.2">
  <office:body>
    <office:text>
      <text:p>LibreOffice conversion capability check.</text:p>
    </office:text>
  </office:body>
</office:document-content>
""",
        )
    return path


def _target_suffix(target_format: str) -> str:
    return target_format.split(":", maxsplit=1)[0].strip().lstrip(".") or "pdf"


def _safe_command_output(result: LibreOfficeCommandResult) -> str:
    raw = "\n".join(part for part in (result.stderr, result.stdout) if part.strip())
    return _safe_error(raw) if raw.strip() else "no stdout/stderr"


def _safe_error(value: str) -> str:
    cleaned = re.sub(r"[&|;`$<>]", " ", value)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:500] if cleaned else "no details"


def _default_path_exists(path: Path) -> bool:
    try:
        return path.exists()
    except (OSError, PermissionError):
        return False
