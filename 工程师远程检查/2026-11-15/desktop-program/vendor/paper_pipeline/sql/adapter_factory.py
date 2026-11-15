"""SQL upload adapter mode selection and safe configuration loading."""

from __future__ import annotations

import json
import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from paper_pipeline.sql.postgres_adapter import (
    ConnectFactory,
    PostgresSqlUploadAdapter,
    PostgresUploadConfig,
    redact_postgres_secrets,
)
from paper_pipeline.sql.upload_adapter import FakeSqlUploadAdapter, SqlUploadAdapter

SqlAdapterMode = Literal["fake", "dry_run", "real"]

_CONFIG_FILE_ENV = "PAPER_PIPELINE_SQL_CONFIG_FILE"
_MODE_KEYS = (
    "PAPER_PIPELINE_SQL_MODE",
    "PAPER_PIPELINE_SQL_ADAPTER_MODE",
    "SQL_UPLOAD_MODE",
)


@dataclass(frozen=True, slots=True)
class SqlUploadAdapterSelection:
    mode: SqlAdapterMode
    adapter: SqlUploadAdapter
    config: PostgresUploadConfig
    environ: Mapping[str, str]
    requested_mode: str
    errors: list[str] = field(default_factory=list)
    message: str = ""

    def to_safe_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "requested_mode": self.requested_mode,
            "adapter_name": getattr(self.adapter, "adapter_name", type(self.adapter).__name__),
            "postgres": self.config.safe_summary(self.environ),
            "configured_keys": _configured_safe_keys(self.environ),
            "errors": [
                redact_postgres_secrets(error, self.config, self.environ)
                for error in self.errors
            ],
            "message": redact_postgres_secrets(self.message, self.config, self.environ),
        }


def select_sql_upload_adapter(
    environ: Mapping[str, str] | None = None,
    *,
    connect_factory: ConnectFactory | None = None,
) -> SqlUploadAdapterSelection:
    merged = load_sql_adapter_environ(environ)
    requested_mode = _requested_mode(merged)
    config = PostgresUploadConfig.from_environ(merged)
    errors = config.validate(merged)

    if requested_mode == "fake":
        return SqlUploadAdapterSelection(
            mode="fake",
            adapter=FakeSqlUploadAdapter(adapter_name="fake_sql_uploader"),
            config=config,
            environ=merged,
            requested_mode=requested_mode,
            errors=[],
            message="SQL upload adapter explicitly configured as fake.",
        )

    if requested_mode == "real":
        if errors:
            return SqlUploadAdapterSelection(
                mode="dry_run",
                adapter=FakeSqlUploadAdapter(adapter_name="dry_run_sql_uploader"),
                config=config,
                environ=merged,
                requested_mode=requested_mode,
                errors=list(errors),
                message=(
                    "PAPER_PIPELINE_SQL_MODE=real was requested, but PostgreSQL "
                    "configuration is incomplete; staying in dry-run mode."
                ),
            )
        return SqlUploadAdapterSelection(
            mode="real",
            adapter=PostgresSqlUploadAdapter(
                config=config,
                environ=merged,
                connect_factory=connect_factory,
            ),
            config=config,
            environ=merged,
            requested_mode=requested_mode,
            errors=[],
            message="PAPER_PIPELINE_SQL_MODE=real and PostgreSQL config is complete.",
        )

    return SqlUploadAdapterSelection(
        mode="dry_run",
        adapter=FakeSqlUploadAdapter(adapter_name="dry_run_sql_uploader"),
        config=config,
        environ=merged,
        requested_mode=requested_mode,
        errors=list(errors),
        message=(
            "SQL upload remains in dry-run mode because real mode is not configured. "
            "Set PAPER_PIPELINE_SQL_MODE=real with complete PostgreSQL config to "
            "enable the real adapter."
        ),
    )


def default_sql_upload_adapter() -> SqlUploadAdapter:
    return select_sql_upload_adapter().adapter


def load_sql_adapter_environ(
    environ: Mapping[str, str] | None = None,
) -> dict[str, str]:
    source = dict(environ if environ is not None else os.environ)
    config_values: dict[str, str] = {}
    config_file = str(source.get(_CONFIG_FILE_ENV, "")).strip()
    if config_file:
        config_values = _read_config_file(Path(config_file))
    merged = dict(config_values)
    merged.update({key: str(value) for key, value in source.items()})
    return merged


def _requested_mode(source: Mapping[str, str]) -> str:
    raw = ""
    for key in _MODE_KEYS:
        raw = str(source.get(key, "")).strip()
        if raw:
            break
    normalized = raw.lower().replace("-", "_")
    if normalized in {"postgres", "postgresql"}:
        return "real"
    if normalized in {"real", "fake", "dry_run"}:
        return normalized
    return "dry_run"


def _configured_safe_keys(source: Mapping[str, str]) -> list[str]:
    safe_keys = {
        "DATABASE_URL",
        "POSTGRES_DSN",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_DATABASE",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_PASSWORD_ENV",
        "POSTGRES_TABLE",
        "POSTGRES_SSLMODE",
        "POSTGRES_CONNECT_TIMEOUT",
        "PAPER_PIPELINE_SQL_MODE",
        "PAPER_PIPELINE_SQL_ADAPTER_MODE",
        "SQL_UPLOAD_MODE",
        _CONFIG_FILE_ENV,
    }
    return sorted(key for key in safe_keys if str(source.get(key, "")).strip())


def _read_config_file(path: Path) -> dict[str, str]:
    raw = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        decoded = json.loads(raw)
    elif path.suffix.lower() == ".toml":
        decoded = tomllib.loads(raw)
    else:
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError:
            decoded = tomllib.loads(raw)
    if not isinstance(decoded, Mapping):
        raise TypeError("SQL config file must contain a JSON/TOML object")
    return _normalize_config_mapping(decoded)


def _normalize_config_mapping(decoded: Mapping[str, Any]) -> dict[str, str]:
    flattened: dict[str, Any] = dict(decoded)
    for section_name in ("postgres", "postgresql", "database", "sql"):
        section = decoded.get(section_name)
        if isinstance(section, Mapping):
            flattened.update(section)

    aliases = {
        "PAPER_PIPELINE_SQL_MODE": (
            "PAPER_PIPELINE_SQL_MODE",
            "PAPER_PIPELINE_SQL_ADAPTER_MODE",
            "SQL_UPLOAD_MODE",
            "sql_mode",
            "adapter_mode",
            "mode",
        ),
        "POSTGRES_DSN": ("POSTGRES_DSN", "postgres_dsn", "dsn"),
        "DATABASE_URL": ("DATABASE_URL", "database_url", "url"),
        "POSTGRES_HOST": ("POSTGRES_HOST", "postgres_host", "host"),
        "POSTGRES_PORT": ("POSTGRES_PORT", "postgres_port", "port"),
        "POSTGRES_DB": ("POSTGRES_DB", "POSTGRES_DATABASE", "postgres_db", "db", "database"),
        "POSTGRES_USER": ("POSTGRES_USER", "postgres_user", "user", "username"),
        "POSTGRES_PASSWORD": ("POSTGRES_PASSWORD", "postgres_password", "password"),
        "POSTGRES_PASSWORD_ENV": (
            "POSTGRES_PASSWORD_ENV",
            "postgres_password_env",
            "password_env",
        ),
        "POSTGRES_TABLE": ("POSTGRES_TABLE", "postgres_table", "table", "table_name"),
        "POSTGRES_SSLMODE": ("POSTGRES_SSLMODE", "postgres_sslmode", "sslmode"),
        "POSTGRES_CONNECT_TIMEOUT": (
            "POSTGRES_CONNECT_TIMEOUT",
            "postgres_connect_timeout",
            "connect_timeout",
        ),
    }

    normalized: dict[str, str] = {}
    for target_key, possible_keys in aliases.items():
        for key in possible_keys:
            value = flattened.get(key)
            if value is None:
                continue
            raw = str(value).strip()
            if raw:
                normalized[target_key] = raw
                break
    return normalized
