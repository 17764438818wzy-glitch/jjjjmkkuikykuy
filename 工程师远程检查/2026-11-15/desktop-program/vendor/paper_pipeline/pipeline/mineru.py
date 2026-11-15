"""MinerU capability detection and minimal parse checks."""

from __future__ import annotations

import os
import json
import re
import shutil
import subprocess
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Literal

MineruPathStatus = Literal["not-configured", "found", "executable-missing"]
MineruParseStatus = Literal["parse-ok", "parse-failed", "not-configured", "skipped"]
MineruWhich = Callable[[str], str | None]
MineruPathExists = Callable[[Path], bool]
_SENSITIVE_NAME_PARTS = ("KEY", "TOKEN", "PASSWORD", "SECRET")


@dataclass(frozen=True, slots=True)
class MineruPathResolution:
    status: MineruPathStatus
    path: Path | None
    source: str
    message: str


@dataclass(frozen=True, slots=True)
class MineruCommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


@dataclass(frozen=True, slots=True)
class MineruParseCheckResult:
    status: MineruParseStatus
    message: str
    output_path: Path | None = None


class MineruCommandRunner(Protocol):
    def run(
        self,
        command: list[str],
        *,
        cwd: Path | None = None,
        timeout_seconds: int,
    ) -> MineruCommandResult:
        """Run MinerU without going through a shell."""


class SubprocessMineruRunner:
    def run(
        self,
        command: list[str],
        *,
        cwd: Path | None = None,
        timeout_seconds: int,
    ) -> MineruCommandResult:
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
        return MineruCommandResult(
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )


def resolve_mineru_path(
    environ: Mapping[str, str] | None = None,
    *,
    which: MineruWhich | None = None,
    path_exists: MineruPathExists | None = None,
) -> MineruPathResolution:
    source = environ if environ is not None else os.environ
    exists = path_exists or _default_path_exists
    for key in ("MINERU_PATH", "DITY_MINERU_PATH"):
        raw = str(source.get(key, "")).strip()
        if not raw:
            continue
        path = Path(raw)
        if exists(path):
            return MineruPathResolution(
                status="found",
                path=path,
                source=key,
                message=f"MinerU executable found from {key}.",
            )
        return MineruPathResolution(
            status="executable-missing",
            path=path,
            source=key,
            message=f"MinerU executable configured by {key} is missing.",
        )

    finder = which or shutil.which
    for executable_name in ("mineru", "mineru.exe", "mineru.cmd", "mineru.bat"):
        discovered = finder(executable_name)
        if discovered:
            return MineruPathResolution(
                status="found",
                path=Path(discovered),
                source=f"PATH:{executable_name}",
                message="MinerU executable found on PATH.",
            )
    return MineruPathResolution(
        status="not-configured",
        path=None,
        source="",
        message="MinerU executable is not configured and was not found on PATH.",
    )


def build_mineru_parse_command(
    mineru_path: str | Path,
    input_path: str | Path,
    output_dir: str | Path,
) -> list[str]:
    return [
        str(mineru_path),
        "--path",
        str(input_path),
        "--output",
        str(output_dir),
    ]


def materialize_mineru_json(
    *,
    output_dir: str | Path,
    output_path: str | Path,
    stdout: str = "",
) -> bool:
    destination = Path(output_path)
    if _is_readable_mineru_artifact(destination):
        return True

    stdout_payload = _json_object_from_text(stdout)
    if stdout_payload is not None:
        _write_json(destination, stdout_payload)
        if _is_readable_mineru_artifact(destination):
            return True

    payload = _payload_from_mineru_output_dir(Path(output_dir))
    if payload is None:
        return False
    _write_json(destination, payload)
    return True


def run_minimal_parse_check(
    mineru_path: str | Path | None,
    *,
    source_path: str | Path | None = None,
    runner: MineruCommandRunner | None = None,
    temp_dir: str | Path | None = None,
    timeout_seconds: int = 60,
    environ: Mapping[str, str] | None = None,
) -> MineruParseCheckResult:
    if mineru_path is None:
        return MineruParseCheckResult(
            status="not-configured",
            message="MinerU executable is not configured.",
        )
    if source_path is None:
        return MineruParseCheckResult(
            status="skipped",
            message="MinerU minimal parse check skipped because no sample PDF is configured.",
        )

    source = Path(source_path)
    if not source.exists():
        return MineruParseCheckResult(
            status="skipped",
            message="MinerU minimal parse check skipped because the sample PDF is missing.",
        )

    if temp_dir is None:
        with tempfile.TemporaryDirectory(prefix="paper-pipeline-mineru-") as created:
            return _run_parse_check_in_dir(
                Path(mineru_path),
                source,
                output_dir=Path(created),
                runner=runner or SubprocessMineruRunner(),
                timeout_seconds=timeout_seconds,
                environ=environ,
                keep_output_path=False,
            )

    return _run_parse_check_in_dir(
        Path(mineru_path),
        source,
        output_dir=Path(temp_dir),
        runner=runner or SubprocessMineruRunner(),
        timeout_seconds=timeout_seconds,
        environ=environ,
        keep_output_path=True,
    )


def _default_path_exists(path: Path) -> bool:
    try:
        return path.exists()
    except OSError:
        return False


def _run_parse_check_in_dir(
    mineru_path: Path,
    source_path: Path,
    *,
    output_dir: Path,
    runner: MineruCommandRunner,
    timeout_seconds: int,
    environ: Mapping[str, str] | None,
    keep_output_path: bool,
) -> MineruParseCheckResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "mineru.json"
    command = build_mineru_parse_command(mineru_path, source_path, output_dir)
    redacted_values = (str(mineru_path), str(source_path), str(output_dir), str(output_path))
    try:
        command_result = runner.run(command, timeout_seconds=timeout_seconds)
    except Exception as exc:  # noqa: BLE001 - capability checks report safe failures.
        return MineruParseCheckResult(
            status="parse-failed",
            message=_safe_error(
                "MinerU parse failed",
                f"{type(exc).__name__}: {exc}",
                environ,
                redacted_values,
            ),
        )

    if command_result.returncode != 0:
        return MineruParseCheckResult(
            status="parse-failed",
            message=_safe_error(
                "MinerU parse failed",
                command_result.stderr or command_result.stdout or f"exit {command_result.returncode}",
                environ,
                redacted_values,
            ),
        )

    materialize_mineru_json(
        output_dir=output_dir,
        output_path=output_path,
        stdout=command_result.stdout,
    )

    if not _is_readable_mineru_artifact(output_path):
        return MineruParseCheckResult(
            status="parse-failed",
            message="MinerU parse failed: readable JSON artifact was not produced.",
        )

    return MineruParseCheckResult(
        status="parse-ok",
        message="MinerU minimal parse check produced a readable JSON artifact.",
        output_path=output_path if keep_output_path else None,
    )


def _json_object_from_text(value: str) -> dict[str, object] | None:
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return None
    return decoded if isinstance(decoded, dict) else None


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _payload_from_mineru_output_dir(output_dir: Path) -> dict[str, object] | None:
    if not output_dir.exists():
        return None

    for candidate in _mineru_json_candidates(output_dir):
        payload = _payload_from_json_file(candidate)
        if payload is not None:
            return payload

    for candidate in sorted(output_dir.rglob("*.md")):
        try:
            text = candidate.read_text(encoding="utf-8").strip()
        except (OSError, UnicodeDecodeError):
            continue
        if text:
            return _payload_from_text(text)
    return None


def _mineru_json_candidates(output_dir: Path) -> list[Path]:
    patterns = (
        "*_content_list.json",
        "*_content_list_v2.json",
        "mineru.json",
        "*_middle.json",
        "*.json",
    )
    candidates: list[Path] = []
    seen: set[Path] = set()
    for pattern in patterns:
        for candidate in sorted(output_dir.rglob(pattern)):
            if candidate in seen:
                continue
            seen.add(candidate)
            candidates.append(candidate)
    return candidates


def _payload_from_json_file(path: Path) -> dict[str, object] | None:
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None

    if isinstance(decoded, dict):
        if _is_readable_mineru_payload(decoded):
            return decoded
        return _payload_from_text(json.dumps(decoded, ensure_ascii=False))
    if isinstance(decoded, list):
        blocks = _blocks_from_content_list(decoded)
        if blocks:
            return {
                "blocks": blocks,
                "content": "\n\n".join(str(block["text"]) for block in blocks),
            }
    return None


def _blocks_from_content_list(items: list[object]) -> list[dict[str, object]]:
    blocks: list[dict[str, object]] = []
    for item in items:
        if isinstance(item, Mapping):
            text = str(item.get("text") or item.get("content") or "").strip()
            if text:
                blocks.append(
                    {
                        "type": str(item.get("type") or "paragraph"),
                        "text": text,
                    },
                )
        else:
            text = str(item).strip()
            if text:
                blocks.append({"type": "paragraph", "text": text})
    return blocks


def _payload_from_text(text: str) -> dict[str, object]:
    blocks = [
        {"type": "paragraph", "text": part.strip()}
        for part in re.split(r"\n{2,}", text)
        if part.strip()
    ]
    if not blocks:
        blocks = [{"type": "paragraph", "text": text}]
    return {
        "text": text,
        "blocks": blocks,
    }


def _is_readable_mineru_artifact(path: Path) -> bool:
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return False
    return _is_readable_mineru_payload(decoded)


def _is_readable_mineru_payload(decoded: object) -> bool:
    if not isinstance(decoded, dict) or not decoded:
        return False
    return any(key in decoded for key in ("blocks", "text", "content", "pages"))


def _safe_error(
    prefix: str,
    detail: str,
    environ: Mapping[str, str] | None,
    redacted_values: tuple[str, ...],
) -> str:
    sanitized = str(detail)
    for value in redacted_values:
        if value:
            sanitized = sanitized.replace(value, "<redacted:path>")
    for key, value in (environ or {}).items():
        if not _is_sensitive_key(key):
            continue
        raw = str(value)
        if raw:
            sanitized = sanitized.replace(raw, f"<redacted:{key}>")
    sanitized = re.sub(r"://([^:/@\s]+):([^/@\s]+)@", r"://\1:<redacted>@", sanitized)
    sanitized = re.sub(
        r"(?i)\b(password|token|api[_-]?key|secret)=([^;\s&|]+)",
        r"\1=<redacted>",
        sanitized,
    )
    sanitized = re.sub(r"[&;|`<>]+", " ", sanitized)
    sanitized = re.sub(r"\s+", " ", sanitized).strip()
    if len(sanitized) > 300:
        sanitized = sanitized[:297].rstrip() + "..."
    return f"{prefix}: {sanitized or 'no stderr or stdout'}"


def _is_sensitive_key(key: str) -> bool:
    upper = key.upper()
    return any(part in upper for part in _SENSITIVE_NAME_PARTS)
