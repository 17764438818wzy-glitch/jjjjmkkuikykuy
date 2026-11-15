"""External capability health checks for the local paper pipeline."""

from __future__ import annotations

import argparse
import json
import os
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

from paper_pipeline.config import DEFAULT_INPUT_RAW
from paper_pipeline.pipeline.docling import ImportAvailable, resolve_docling_availability
from paper_pipeline.pipeline.libreoffice import (
    LibreOfficeCommandRunner,
    LibreOfficePathResolution,
    Which,
    resolve_libreoffice_path,
    run_minimal_conversion_check,
)
from paper_pipeline.pipeline.local_dity_adapter import DityToolConfig
from paper_pipeline.pipeline.llm import LLMProvider, run_llm_healthcheck
from paper_pipeline.pipeline.mineru import (
    MineruCommandRunner,
    MineruPathResolution,
    resolve_mineru_path,
    run_minimal_parse_check,
)
from paper_pipeline.sql.adapter_factory import select_sql_upload_adapter
from paper_pipeline.sql.healthcheck import (
    SqlSelect1ConnectFactory,
    SqlTestUploadConnectFactory,
    run_sql_select1_check,
    run_sql_test_upload_check,
)
from paper_pipeline.sql.postgres_adapter import PostgresUploadConfig

DoctorStatus = Literal["pass", "warn", "fail", "skipped"]
DoctorMode = Literal[
    "real",
    "fake",
    "dry_run",
    "dry-run",
    "not-configured",
    "uploaded",
    "found",
    "executable-missing",
    "conversion-ok",
    "conversion-failed",
    "parse-ok",
    "parse-failed",
    "skipped",
    "config-error",
    "failed",
]
PathExists = Callable[[Path], bool]
PostgresSelectOne = SqlSelect1ConnectFactory
PostgresTestUpload = SqlTestUploadConnectFactory

_LOCAL_TOOL_CONFIG_KEYS = (
    "LIBREOFFICE_PATH",
    "MINERU_PATH",
    "LIBREOFFICE_TIMEOUT_SECONDS",
    "MINERU_TIMEOUT_SECONDS",
    "LLM_TIMEOUT_SECONDS",
)
_LEGACY_DITY_LOCAL_TOOL_CONFIG_KEYS = (
    "DITY_LIBREOFFICE_PATH",
    "DITY_MINERU_PATH",
    "DITY_LIBREOFFICE_TIMEOUT_SECONDS",
    "DITY_MINERU_TIMEOUT_SECONDS",
    "DITY_LLM_TIMEOUT_SECONDS",
)
_SENSITIVE_NAME_PARTS = ("KEY", "TOKEN", "PASSWORD", "SECRET")


@dataclass(frozen=True, slots=True)
class DoctorCheck:
    name: str
    status: DoctorStatus
    mode: DoctorMode
    message: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


def run_doctor(
    *,
    environ: Mapping[str, str] | None = None,
    postgres_select_one: PostgresSelectOne | None = None,
    postgres_test_upload: PostgresTestUpload | None = None,
    path_exists: PathExists | None = None,
    which: Which | None = None,
    libreoffice_conversion_runner: LibreOfficeCommandRunner | None = None,
    libreoffice_conversion_source: str | Path | None = None,
    mineru_parse_runner: MineruCommandRunner | None = None,
    mineru_parse_source: str | Path | None = None,
    llm_provider: LLMProvider | None = None,
    docling_import_available: ImportAvailable | None = None,
) -> list[DoctorCheck]:
    source = environ if environ is not None else os.environ
    exists = path_exists or _default_path_exists
    find_executable = which
    sql_selection = select_sql_upload_adapter(source)
    postgres_config = sql_selection.config
    postgres_errors = postgres_config.validate(sql_selection.environ)
    libreoffice_resolution = resolve_libreoffice_path(
        source,
        which=find_executable,
        path_exists=exists,
    )
    mineru_resolution = resolve_mineru_path(
        source,
        which=find_executable,
        path_exists=exists,
    )
    dity_config = DityToolConfig.from_environ(
        source,
        which=find_executable,
        path_exists=exists,
    )

    checks = [
        _check_nas_access(source, exists),
        _check_postgres_configured(
            postgres_config,
            postgres_errors,
            sql_selection.environ,
        ),
        _check_postgres_select_one(
            source,
            postgres_select_one,
        ),
        _check_postgres_test_upload(
            source,
            postgres_test_upload,
        ),
        _check_sql_adapter_mode(sql_selection),
        _check_llm_provider(source, llm_provider),
        _check_mineru_path(mineru_resolution),
        _check_mineru_parse(
            mineru_resolution,
            dity_config,
            runner=mineru_parse_runner,
            source_path=mineru_parse_source or _mineru_parse_source_from_env(source),
            environ=source,
        ),
        _check_docling_import(docling_import_available),
        _check_libreoffice_path(libreoffice_resolution),
        _check_libreoffice_conversion(
            libreoffice_resolution,
            dity_config,
            runner=libreoffice_conversion_runner,
            source_path=libreoffice_conversion_source,
        ),
        _check_dity_env_present(source),
        _check_dity_config_read_by_code(source),
        _check_codex_mode(source),
    ]
    return [_redacted_check(check, source) for check in checks]


def format_doctor_json(checks: Sequence[DoctorCheck]) -> str:
    return json.dumps(
        [check.to_dict() for check in checks],
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )


def format_doctor_text(checks: Sequence[DoctorCheck]) -> str:
    lines = ["name\tstatus\tmode\tmessage"]
    lines.extend(
        f"{check.name}\t{check.status}\t{check.mode}\t{check.message}"
        for check in checks
    )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="paper-pipeline-doctor")
    parser.add_argument(
        "--text",
        action="store_true",
        help="Print a tab-separated table instead of JSON.",
    )
    args = parser.parse_args(argv)
    checks = run_doctor()
    print(format_doctor_text(checks) if args.text else format_doctor_json(checks))
    return 0


def _check_nas_access(
    environ: Mapping[str, str],
    path_exists: PathExists,
) -> DoctorCheck:
    raw_path = (
        environ.get("PAPER_PIPELINE_NAS_PATH")
        or environ.get("PAPER_PIPELINE_INPUT_RAW")
        or str(DEFAULT_INPUT_RAW)
    )
    path = Path(raw_path)
    if not path_exists(path):
        return DoctorCheck(
            name="nas_accessible",
            status="fail",
            mode="real",
            message=f"NAS path is not accessible: {path}",
        )
    return DoctorCheck(
        name="nas_accessible",
        status="pass",
        mode="real",
        message=f"NAS path is accessible: {path}",
    )


def _check_postgres_configured(
    config: PostgresUploadConfig,
    errors: Sequence[str],
    environ: Mapping[str, str],
) -> DoctorCheck:
    if errors:
        return DoctorCheck(
            name="postgres_configured",
            status="warn",
            mode="not-configured",
            message="PostgreSQL config is incomplete: " + "; ".join(errors),
        )
    summary = config.safe_summary(environ)
    message = (
        "PostgreSQL config present: "
        f"dsn={summary['dsn']}, host={summary['host'] or 'UNSET'}, "
        f"port={summary['port']}, database={summary['database'] or 'UNSET'}, "
        f"user={summary['user'] or 'UNSET'}, password={summary['password']}, "
        f"table={summary['table_name']}, sslmode={summary['sslmode']}"
    )
    return DoctorCheck(
        name="postgres_configured",
        status="pass",
        mode="real",
        message=message,
    )


def _check_postgres_select_one(
    environ: Mapping[str, str],
    select_one: PostgresSelectOne | None,
) -> DoctorCheck:
    result = run_sql_select1_check(
        environ=environ,
        connect_factory=select_one,
    )
    return DoctorCheck(
        name=result.name,
        status=result.status,
        mode=result.mode,
        message=result.message,
    )


def _check_postgres_test_upload(
    environ: Mapping[str, str],
    connect_factory: PostgresTestUpload | None,
) -> DoctorCheck:
    result = run_sql_test_upload_check(
        environ=environ,
        connect_factory=connect_factory,
    )
    return DoctorCheck(
        name=result.name,
        status=result.status,
        mode=result.mode,
        message=result.message,
    )


def _check_sql_adapter_mode(selection) -> DoctorCheck:
    safe = selection.to_safe_dict()
    adapter_name = str(safe["adapter_name"])
    if selection.mode == "real":
        return DoctorCheck(
            name="sql_adapter_mode",
            status="pass",
            mode="real",
            message=f"SQL adapter is {adapter_name}; real PostgreSQL mode is enabled.",
        )
    if selection.mode == "fake":
        return DoctorCheck(
            name="sql_adapter_mode",
            status="warn",
            mode="fake",
            message=(
                f"SQL adapter is {adapter_name}; fake mode is enabled and never "
                "writes a real database."
            ),
        )
    return DoctorCheck(
        name="sql_adapter_mode",
        status="warn",
        mode="dry-run",
        message=(
            f"SQL adapter is {adapter_name}; dry-run mode is enabled with a fake adapter. "
            f"{safe['message']}"
        ),
    )


def _check_llm_provider(
    environ: Mapping[str, str],
    provider: LLMProvider | None,
) -> DoctorCheck:
    result = run_llm_healthcheck(environ=environ, provider=provider)
    message = (
        f"provider={result.provider}; mode={result.mode}; model={result.model}; "
        f"key={result.key_status}; invoked={result.llm_invoked}; {result.message}"
    )
    if result.error:
        message = f"{message}; error={result.error}"
    return DoctorCheck(
        name="llm_provider_mode",
        status=result.status,
        mode=result.mode,
        message=message,
    )


def _check_mineru_path(resolution: MineruPathResolution) -> DoctorCheck:
    if resolution.status == "found":
        return DoctorCheck(
            name="mineru_path",
            status="warn",
            mode="found",
            message=(
                f"MinerU resolve status: found; source={resolution.source}; "
                f"path={resolution.path}. This only means the executable was found "
                "and does not prove parse-ok."
            ),
        )
    if resolution.status == "executable-missing":
        return DoctorCheck(
            name="mineru_path",
            status="fail",
            mode="executable-missing",
            message=(
                f"MinerU resolve status: executable-missing; "
                f"source={resolution.source}; {resolution.message}"
            ),
        )
    return DoctorCheck(
        name="mineru_path",
        status="warn",
        mode="not-configured",
        message=f"MinerU resolve status: not-configured; {resolution.message}",
    )


def _check_mineru_parse(
    resolution: MineruPathResolution,
    config: DityToolConfig,
    *,
    runner: MineruCommandRunner | None,
    source_path: str | Path | None,
    environ: Mapping[str, str],
) -> DoctorCheck:
    if resolution.status != "found" or resolution.path is None:
        return DoctorCheck(
            name="mineru_parse_check",
            status="skipped",
            mode=resolution.status,
            message=(
                "MinerU parse check skipped because resolve status is "
                f"{resolution.status}."
            ),
        )

    result = run_minimal_parse_check(
        resolution.path,
        source_path=source_path,
        runner=runner,
        timeout_seconds=config.mineru_timeout_seconds,
        environ=environ,
    )
    if result.status == "parse-ok":
        status: DoctorStatus = "pass"
    elif result.status == "parse-failed":
        status = "fail"
    else:
        status = "skipped"
    return DoctorCheck(
        name="mineru_parse_check",
        status=status,
        mode=result.status,
        message=result.message,
    )


def _check_docling_import(
    import_available: ImportAvailable | None,
) -> DoctorCheck:
    availability = resolve_docling_availability(import_available=import_available)
    if availability.status == "found":
        return DoctorCheck(
            name="docling_import_available",
            status="pass",
            mode="found",
            message=(
                f"{availability.message} Stage2 .docx primary parser and .pdf "
                "fallback parser are available."
            ),
        )
    return DoctorCheck(
        name="docling_import_available",
        status="warn",
        mode="not-configured",
        message=(
            f"{availability.message} Stage2 .docx primary parser and .pdf "
            "fallback parser will use pending_parser_unavailable when selected."
        ),
    )


def _check_libreoffice_path(resolution: LibreOfficePathResolution) -> DoctorCheck:
    if resolution.status == "found":
        return DoctorCheck(
            name="libreoffice_path",
            status="pass",
            mode="found",
            message=(
                f"LibreOffice executable found via {resolution.source}: "
                f"{resolution.path}"
            ),
        )
    if resolution.status == "executable-missing":
        return DoctorCheck(
            name="libreoffice_path",
            status="fail",
            mode="executable-missing",
            message=resolution.message,
        )
    return DoctorCheck(
        name="libreoffice_path",
        status="warn",
        mode="not-configured",
        message=resolution.message,
    )


def _check_libreoffice_conversion(
    resolution: LibreOfficePathResolution,
    config: DityToolConfig,
    *,
    runner: LibreOfficeCommandRunner | None,
    source_path: str | Path | None,
) -> DoctorCheck:
    if resolution.status != "found" or resolution.path is None:
        return DoctorCheck(
            name="libreoffice_conversion",
            status="skipped",
            mode=resolution.status,
            message="Skipped because LibreOffice executable was not found.",
        )

    result = run_minimal_conversion_check(
        resolution.path,
        source_path=source_path,
        runner=runner,
        timeout_seconds=config.libreoffice_timeout_seconds,
    )
    return DoctorCheck(
        name="libreoffice_conversion",
        status="pass" if result.status == "conversion-ok" else "fail",
        mode=result.status,
        message=result.message,
    )


def _check_dity_env_present(environ: Mapping[str, str]) -> DoctorCheck:
    dity_keys = _present_keys(environ, "DITY_")
    dify_keys = _present_keys(environ, "DIFY_")
    if dity_keys:
        extra = (
            f"; DIFY_* also present but not read: {', '.join(dify_keys)}"
            if dify_keys
            else ""
        )
        return DoctorCheck(
            name="dify_dity_env_present",
            status="warn",
            mode="real",
            message=f"Legacy DITY_* env keys present: {', '.join(dity_keys)}{extra}",
        )
    if dify_keys:
        return DoctorCheck(
            name="dify_dity_env_present",
            status="warn",
            mode="not-configured",
            message=(
                "DIFY_* env keys are present but are not read for local tools or uploads: "
                + ", ".join(dify_keys)
            ),
        )
    return DoctorCheck(
        name="dify_dity_env_present",
        status="warn",
        mode="not-configured",
        message="No DITY_* or DIFY_* environment keys are present.",
    )


def _check_dity_config_read_by_code(environ: Mapping[str, str]) -> DoctorCheck:
    standard_keys = [
        key for key in _LOCAL_TOOL_CONFIG_KEYS if str(environ.get(key, "")).strip()
    ]
    legacy_keys = [
        key
        for key in _LEGACY_DITY_LOCAL_TOOL_CONFIG_KEYS
        if str(environ.get(key, "")).strip()
    ]
    dify_keys = _present_keys(environ, "DIFY_")
    if standard_keys:
        return DoctorCheck(
            name="dify_dity_config_read",
            status="pass",
            mode="real",
            message=(
                "standard local tool config keys present; "
                "DityToolConfig.from_environ reads these keys first: "
                + ", ".join(standard_keys)
            ),
        )
    if legacy_keys:
        return DoctorCheck(
            name="dify_dity_config_read",
            status="warn",
            mode="real",
            message=(
                "DityToolConfig.from_environ reads these legacy fallback local tool "
                "config keys: "
                + ", ".join(legacy_keys)
            ),
        )
    if dify_keys:
        return DoctorCheck(
            name="dify_dity_config_read",
            status="warn",
            mode="not-configured",
            message=(
                "DIFY_* env exists but DityToolConfig.from_environ does not read "
                "DIFY_* for local tools or uploads."
            ),
        )
    return DoctorCheck(
        name="dify_dity_config_read",
        status="skipped",
        mode="not-configured",
        message="Skipped because no local tool configuration is present.",
    )


def _check_codex_mode(environ: Mapping[str, str]) -> DoctorCheck:
    codex_keys = _present_keys(environ, "CODEX_")
    if codex_keys:
        return DoctorCheck(
            name="codex_integration_mode",
            status="warn",
            mode="not-configured",
            message=(
                "CODEX_* env keys are present but no Codex client is read by the code; "
                "Codex review remains a manually confirmed workflow."
            ),
        )
    return DoctorCheck(
        name="codex_integration_mode",
        status="skipped",
        mode="not-configured",
        message=(
            "No Codex client or env-backed adapter exists; review_service applies "
            "manually confirmed AgentResult decisions only."
        ),
    )


def _mineru_parse_source_from_env(environ: Mapping[str, str]) -> Path | None:
    raw = str(
        environ.get("DITY_MINERU_TEST_PDF")
        or environ.get("MINERU_TEST_PDF")
        or "",
    ).strip()
    return Path(raw) if raw else None


def _default_path_exists(path: Path) -> bool:
    try:
        if not path.exists():
            return False
        if path.is_dir():
            next(path.iterdir(), None)
        return True
    except (OSError, PermissionError):
        return False


def _present_keys(environ: Mapping[str, str], prefix: str) -> list[str]:
    return sorted(
        key
        for key, value in environ.items()
        if key.upper().startswith(prefix) and str(value).strip()
    )


def _redacted_check(check: DoctorCheck, environ: Mapping[str, str]) -> DoctorCheck:
    return DoctorCheck(
        name=check.name,
        status=check.status,
        mode=check.mode,
        message=redact_sensitive_text(check.message, environ),
    )


def redact_sensitive_text(text: str, environ: Mapping[str, str]) -> str:
    redacted = text
    for key, value in environ.items():
        if not _is_sensitive_key(key):
            continue
        raw = str(value)
        if raw:
            redacted = redacted.replace(raw, f"<redacted:{key}>")
    redacted = re.sub(r"://([^:/@\s]+):([^/@\s]+)@", r"://\1:<redacted>@", redacted)
    redacted = re.sub(
        r"(?i)\b(password|token|api_key|secret)=([^;,\s]+)",
        r"\1=<redacted>",
        redacted,
    )
    return redacted


def _is_sensitive_key(key: str) -> bool:
    upper = key.upper()
    return any(part in upper for part in _SENSITIVE_NAME_PARTS)


if __name__ == "__main__":
    raise SystemExit(main())
