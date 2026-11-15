"""PostgreSQL-backed SQL upload adapter."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field
from typing import Any

from paper_pipeline.sql.sql_models import SqlUploadPreview, SqlUploadPreviewItem
from paper_pipeline.sql.upload_adapter import (
    SqlUploadFailedItem,
    SqlUploadResult,
    SqlUploadedItem,
)
from paper_pipeline.sql.write_authorization import ensure_sql_write_authorized

ConnectFactory = Callable[["PostgresUploadConfig", Mapping[str, str]], Any]

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_URL_PASSWORD_RE = re.compile(r"://([^:/@\s]+):([^/@\s]+)@")


@dataclass(frozen=True, slots=True)
class PostgresUploadConfig:
    dsn: str = ""
    host: str = ""
    port: int = 5432
    database: str = ""
    user: str = ""
    password_env_name: str = "POSTGRES_PASSWORD"
    table_name: str = "paper_pipeline_uploads"
    sslmode: str = "prefer"
    connect_timeout_seconds: int = 10
    create_table: bool = True
    adapter_name: str = "postgres_sql_uploader"

    @classmethod
    def from_environ(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> PostgresUploadConfig:
        source = environ if environ is not None else os.environ
        return cls(
            dsn=str(source.get("POSTGRES_DSN", source.get("DATABASE_URL", ""))).strip(),
            host=str(source.get("POSTGRES_HOST", "")).strip(),
            port=_int_from_env(source, "POSTGRES_PORT", 5432),
            database=str(source.get("POSTGRES_DB", source.get("POSTGRES_DATABASE", ""))).strip(),
            user=str(source.get("POSTGRES_USER", "")).strip(),
            password_env_name=str(source.get("POSTGRES_PASSWORD_ENV", "POSTGRES_PASSWORD")).strip()
            or "POSTGRES_PASSWORD",
            table_name=str(source.get("POSTGRES_TABLE", "paper_pipeline_uploads")).strip()
            or "paper_pipeline_uploads",
            sslmode=str(source.get("POSTGRES_SSLMODE", "prefer")).strip() or "prefer",
            connect_timeout_seconds=_int_from_env(source, "POSTGRES_CONNECT_TIMEOUT", 10),
        )

    def resolve_password(self, environ: Mapping[str, str] | None = None) -> str:
        source = environ if environ is not None else os.environ
        return str(source.get(self.password_env_name, "")).strip()

    def connection_kwargs(self, environ: Mapping[str, str] | None = None) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": self.resolve_password(environ),
            "sslmode": self.sslmode,
            "connect_timeout": self.connect_timeout_seconds,
        }

    def safe_summary(self, environ: Mapping[str, str] | None = None) -> dict[str, object]:
        return {
            "dsn": "SET" if self.dsn else "UNSET",
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": "SET" if self.resolve_password(environ) else "UNSET",
            "password_env_name": self.password_env_name,
            "table_name": self.table_name,
            "sslmode": self.sslmode,
            "connect_timeout_seconds": self.connect_timeout_seconds,
        }

    def validate(self, environ: Mapping[str, str] | None = None) -> list[str]:
        errors: list[str] = []
        if not self.dsn:
            if not self.host:
                errors.append("POSTGRES_HOST is required")
            if not self.database:
                errors.append("POSTGRES_DB is required")
            if not self.user:
                errors.append("POSTGRES_USER is required")
            if not self.resolve_password(environ):
                errors.append(f"{self.password_env_name} is required")
        if self.port <= 0:
            errors.append("POSTGRES_PORT must be greater than 0")
        if self.connect_timeout_seconds <= 0:
            errors.append("POSTGRES_CONNECT_TIMEOUT must be greater than 0")
        try:
            quote_qualified_identifier(self.table_name)
        except ValueError as exc:
            errors.append(str(exc))
        return errors


@dataclass(slots=True)
class PostgresSqlUploadAdapter:
    config: PostgresUploadConfig = field(default_factory=PostgresUploadConfig)
    environ: Mapping[str, str] | None = None
    connect_factory: ConnectFactory | None = None
    adapter_name: str = field(init=False)
    upload_call_count: int = 0

    def __post_init__(self) -> None:
        self.adapter_name = self.config.adapter_name

    @classmethod
    def from_environ(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        connect_factory: ConnectFactory | None = None,
    ) -> PostgresSqlUploadAdapter:
        source = environ if environ is not None else os.environ
        return cls(
            config=PostgresUploadConfig.from_environ(source),
            environ=source,
            connect_factory=connect_factory,
        )

    def upload(
        self,
        *,
        preview: SqlUploadPreview,
        items: list[SqlUploadPreviewItem],
        sql_batch_id: str,
    ) -> SqlUploadResult:
        ensure_sql_write_authorized("PostgresSqlUploadAdapter.upload")
        self.upload_call_count += 1
        environ = self.environ if self.environ is not None else os.environ
        errors = self.config.validate(environ)
        if errors:
            return _failed_result(
                sql_batch_id=sql_batch_id,
                adapter_name=self.adapter_name,
                items=items,
                error=f"PostgreSQL configuration error: {'; '.join(errors)}",
            )

        connection = None
        try:
            connection = self._connect(environ)
            uploaded: list[SqlUploadedItem] = []
            unique_items, skipped_items = _deduplicate_items_by_paper_id(items)
            with connection.cursor() as cursor:
                table = quote_qualified_identifier(self.config.table_name)
                if self.config.create_table:
                    cursor.execute(_create_table_sql(table))
                for item in unique_items:
                    cursor.execute(
                        _upsert_sql(table),
                        _item_params(preview=preview, item=item, sql_batch_id=sql_batch_id),
                    )
                    uploaded.append(
                        SqlUploadedItem(
                            paper_id=item.paper_id,
                            accession_id=item.accession_id,
                            version=item.version,
                            record_id=f"{self.config.table_name}:{item.paper_id}",
                        ),
                    )
            connection.commit()
            return SqlUploadResult(
                sql_batch_id=sql_batch_id,
                adapter_name=self.adapter_name,
                success_count=len(uploaded),
                failed_count=0,
                skipped_count=len(skipped_items),
                uploaded_items=uploaded,
                failed_items=[],
                skipped_items=skipped_items,
            )
        except Exception as exc:  # noqa: BLE001 - adapter returns structured failures.
            if connection is not None:
                _safe_rollback(connection)
            error = redact_postgres_secrets(str(exc), self.config, environ)
            return _failed_result(
                sql_batch_id=sql_batch_id,
                adapter_name=self.adapter_name,
                items=items,
                error=f"PostgreSQL upload failed: {error}",
            )
        finally:
            if connection is not None:
                _safe_close(connection)

    def _connect(self, environ: Mapping[str, str]) -> Any:
        if self.connect_factory is not None:
            return self.connect_factory(self.config, environ)
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover - depends on local environment.
            raise RuntimeError(
                "psycopg is required for PostgreSQL uploads. "
                "Install the psycopg[binary] package.",
            ) from exc
        if self.config.dsn:
            return psycopg.connect(
                self.config.dsn,
                sslmode=self.config.sslmode,
                connect_timeout=self.config.connect_timeout_seconds,
            )
        return psycopg.connect(**self.config.connection_kwargs(environ))


def quote_qualified_identifier(value: str) -> str:
    parts = value.split(".")
    if not 1 <= len(parts) <= 2 or any(not _IDENTIFIER_RE.fullmatch(part) for part in parts):
        raise ValueError("POSTGRES_TABLE must be a simple table name or schema.table")
    return ".".join(f'"{part}"' for part in parts)


def redact_postgres_secrets(
    text: str,
    config: PostgresUploadConfig,
    environ: Mapping[str, str] | None = None,
) -> str:
    source = environ if environ is not None else os.environ
    redacted = text
    for value in (config.dsn, source.get("DATABASE_URL", ""), config.resolve_password(source)):
        raw = str(value or "")
        if raw:
            label = "DATABASE_URL" if raw == source.get("DATABASE_URL", "") else "secret"
            redacted = redacted.replace(raw, f"<redacted:{label}>")
    redacted = _URL_PASSWORD_RE.sub(r"://\1:<redacted>@", redacted)
    return redacted


def _create_table_sql(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    paper_id text PRIMARY KEY,
    accession_id text NOT NULL,
    version integer NOT NULL,
    title text NOT NULL,
    tier text NOT NULL,
    category text NOT NULL,
    direction text NOT NULL,
    research_method_primary text NOT NULL,
    research_method_tags jsonb NOT NULL,
    source_hash text NOT NULL,
    source_path text NOT NULL,
    md_path text NOT NULL,
    metadata_path text NOT NULL,
    index_status text NOT NULL,
    sql_status_at_preview text NOT NULL,
    version_status text NOT NULL,
    duplicate_status text NOT NULL,
    abstract text NOT NULL,
    keywords jsonb NOT NULL,
    similar_to jsonb NOT NULL,
    preview_id text NOT NULL,
    run_id text NOT NULL,
    sql_batch_id text NOT NULL,
    payload_json jsonb NOT NULL,
    uploaded_at timestamptz NOT NULL DEFAULT now()
)
"""


def _upsert_sql(table: str) -> str:
    return f"""
INSERT INTO {table} (
    paper_id,
    accession_id,
    version,
    title,
    tier,
    category,
    direction,
    research_method_primary,
    research_method_tags,
    source_hash,
    source_path,
    md_path,
    metadata_path,
    index_status,
    sql_status_at_preview,
    version_status,
    duplicate_status,
    abstract,
    keywords,
    similar_to,
    preview_id,
    run_id,
    sql_batch_id,
    payload_json
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s::jsonb
)
ON CONFLICT (paper_id) DO UPDATE SET
    accession_id = EXCLUDED.accession_id,
    version = EXCLUDED.version,
    title = EXCLUDED.title,
    tier = EXCLUDED.tier,
    category = EXCLUDED.category,
    direction = EXCLUDED.direction,
    research_method_primary = EXCLUDED.research_method_primary,
    research_method_tags = EXCLUDED.research_method_tags,
    source_hash = EXCLUDED.source_hash,
    source_path = EXCLUDED.source_path,
    md_path = EXCLUDED.md_path,
    metadata_path = EXCLUDED.metadata_path,
    index_status = EXCLUDED.index_status,
    sql_status_at_preview = EXCLUDED.sql_status_at_preview,
    version_status = EXCLUDED.version_status,
    duplicate_status = EXCLUDED.duplicate_status,
    abstract = EXCLUDED.abstract,
    keywords = EXCLUDED.keywords,
    similar_to = EXCLUDED.similar_to,
    preview_id = EXCLUDED.preview_id,
    run_id = EXCLUDED.run_id,
    sql_batch_id = EXCLUDED.sql_batch_id,
    payload_json = EXCLUDED.payload_json,
    uploaded_at = now()
"""


def _item_params(
    *,
    preview: SqlUploadPreview,
    item: SqlUploadPreviewItem,
    sql_batch_id: str,
) -> tuple[Any, ...]:
    payload = asdict(item)
    payload["preview_id"] = preview.preview_id
    payload["run_id"] = preview.run_id
    payload["sql_batch_id"] = sql_batch_id
    return (
        item.paper_id,
        item.accession_id,
        item.version,
        item.title,
        item.tier,
        item.category,
        item.direction,
        item.research_method_primary,
        json.dumps(item.research_method_tags, ensure_ascii=False),
        item.source_hash,
        item.source_path,
        item.md_path,
        item.metadata_path,
        item.index_status,
        item.sql_status,
        item.version_status,
        item.duplicate_status,
        item.abstract,
        json.dumps(item.keywords, ensure_ascii=False),
        json.dumps(item.similar_to, ensure_ascii=False),
        preview.preview_id,
        preview.run_id,
        sql_batch_id,
        json.dumps(payload, ensure_ascii=False, sort_keys=True),
    )


def _deduplicate_items_by_paper_id(
    items: list[SqlUploadPreviewItem],
) -> tuple[list[SqlUploadPreviewItem], list[str]]:
    seen: set[str] = set()
    unique_items: list[SqlUploadPreviewItem] = []
    skipped_items: list[str] = []
    for item in items:
        stable_key = item.paper_id
        if stable_key in seen:
            skipped_items.append(f"duplicate paper_id skipped: {stable_key}")
            continue
        seen.add(stable_key)
        unique_items.append(item)
    return unique_items, skipped_items


def _failed_result(
    *,
    sql_batch_id: str,
    adapter_name: str,
    items: list[SqlUploadPreviewItem],
    error: str,
) -> SqlUploadResult:
    failed_items = [
        SqlUploadFailedItem(
            paper_id=item.paper_id,
            error=error,
        )
        for item in items
    ]
    return SqlUploadResult(
        sql_batch_id=sql_batch_id,
        adapter_name=adapter_name,
        success_count=0,
        failed_count=len(failed_items),
        skipped_count=0,
        uploaded_items=[],
        failed_items=failed_items,
        skipped_items=[],
    )


def _int_from_env(source: Mapping[str, str], key: str, default: int) -> int:
    raw = str(source.get(key, "")).strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _safe_rollback(connection: Any) -> None:
    try:
        connection.rollback()
    except Exception:
        pass


def _safe_close(connection: Any) -> None:
    try:
        connection.close()
    except Exception:
        pass
