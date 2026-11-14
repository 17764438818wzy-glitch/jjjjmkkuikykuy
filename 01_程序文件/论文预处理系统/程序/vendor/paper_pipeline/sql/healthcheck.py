"""Read-only SQL health checks."""

from __future__ import annotations

import re
import json
import uuid
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from typing import Any, Literal

from paper_pipeline.sql.adapter_factory import select_sql_upload_adapter
from paper_pipeline.sql.postgres_adapter import (
    PostgresUploadConfig,
    quote_qualified_identifier,
    redact_postgres_secrets,
)
from paper_pipeline.sql.write_authorization import (
    authorization_required_message,
    has_sql_write_authorization,
)

SqlSelect1Status = Literal["pass", "fail", "skipped"]
SqlSelect1Mode = Literal["fake", "dry_run", "real", "not-configured"]
SqlSelect1ConnectFactory = Callable[[PostgresUploadConfig, Mapping[str, str]], Any]
SqlTestUploadStatus = Literal["pass", "fail", "skipped"]
SqlTestUploadMode = Literal[
    "fake",
    "dry_run",
    "not-configured",
    "uploaded",
    "failed",
]
SqlTestUploadStatusValue = Literal["dry_run", "skipped", "uploaded", "failed"]
SqlTestUploadConnectFactory = Callable[[PostgresUploadConfig, Mapping[str, str]], Any]

_SENSITIVE_NAME_PARTS = ("KEY", "TOKEN", "PASSWORD", "SECRET", "AUTHORIZATION")
_SQL_TEST_TABLE_ENV = "PAPER_PIPELINE_SQL_TEST_TABLE"
_SQL_TEST_SCHEMA_ENV = "PAPER_PIPELINE_SQL_TEST_SCHEMA"
_SQL_TEST_SOURCE = "sql_test_upload"


@dataclass(frozen=True, slots=True)
class SqlSelect1Check:
    name: str
    status: SqlSelect1Status
    mode: SqlSelect1Mode
    message: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SqlTestUploadCheck:
    name: str
    status: SqlTestUploadStatus
    mode: SqlTestUploadMode
    sql_status: SqlTestUploadStatusValue
    message: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


def run_sql_select1_check(
    *,
    environ: Mapping[str, str] | None = None,
    connect_factory: SqlSelect1ConnectFactory | None = None,
) -> SqlSelect1Check:
    selection = select_sql_upload_adapter(environ)
    if selection.mode != "real":
        safe = selection.to_safe_dict()
        if selection.requested_mode == "real" and selection.errors:
            raw_errors = safe.get("errors", [])
            errors = (
                "; ".join(str(error) for error in raw_errors)
                if isinstance(raw_errors, list)
                else str(raw_errors)
            )
            return SqlSelect1Check(
                name="postgres_select_1",
                status="skipped",
                mode="not-configured",
                message=(
                    "SQL SELECT 1 skipped because adapter is not real and "
                    f"PostgreSQL configuration is incomplete: {errors}"
                ),
            )
        return SqlSelect1Check(
            name="postgres_select_1",
            status="skipped",
            mode=selection.mode,
            message=(
                "SQL SELECT 1 skipped because adapter is not real "
                f"(mode={selection.mode}). {safe['message']}"
            ),
        )

    return check_postgres_select1(
        config=selection.config,
        environ=selection.environ,
        connect_factory=connect_factory,
    )


def run_sql_test_upload_check(
    *,
    environ: Mapping[str, str] | None = None,
    connect_factory: SqlTestUploadConnectFactory | None = None,
    run_id: str | None = None,
) -> SqlTestUploadCheck:
    selection = select_sql_upload_adapter(environ)
    if selection.mode != "real":
        safe = selection.to_safe_dict()
        if selection.requested_mode == "real" and selection.errors:
            raw_errors = safe.get("errors", [])
            errors = (
                "; ".join(str(error) for error in raw_errors)
                if isinstance(raw_errors, list)
                else str(raw_errors)
            )
            return SqlTestUploadCheck(
                name="postgres_test_upload",
                status="skipped",
                mode="not-configured",
                sql_status="skipped",
                message=(
                    "sql_status=skipped; SQL test upload skipped because adapter "
                    "is not real and PostgreSQL configuration is incomplete: "
                    f"{errors}"
                ),
            )
        return SqlTestUploadCheck(
            name="postgres_test_upload",
            status="skipped",
            mode=selection.mode,
            sql_status="dry_run",
            message=(
                f"sql_status=dry_run; SQL test upload skipped because adapter "
                f"is not real (mode={selection.mode}). {safe['message']}"
            ),
        )

    target = _resolve_sql_test_upload_target(
        selection.config,
        selection.environ,
    )
    if target.error:
        return SqlTestUploadCheck(
            name="postgres_test_upload",
            status="skipped",
            mode="not-configured",
            sql_status="skipped",
            message=f"sql_status=skipped; {target.error}",
        )

    return check_postgres_test_upload(
        config=selection.config,
        environ=selection.environ,
        target_table=target.quoted_table,
        raw_target_table=target.raw_table,
        connect_factory=connect_factory,
        run_id=run_id,
    )


def check_postgres_select1(
    *,
    config: PostgresUploadConfig,
    environ: Mapping[str, str],
    connect_factory: SqlSelect1ConnectFactory | None = None,
) -> SqlSelect1Check:
    errors = config.validate(environ)
    if errors:
        safe_errors = [
            redact_postgres_secrets(error, config, environ) for error in errors
        ]
        return SqlSelect1Check(
            name="postgres_select_1",
            status="skipped",
            mode="not-configured",
            message=(
                "SQL SELECT 1 skipped because PostgreSQL configuration is incomplete: "
                + "; ".join(safe_errors)
            ),
        )

    connection = None
    try:
        connector = connect_factory or _default_connect
        connection = connector(config, environ)
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            row = cursor.fetchone()
        if row == (1,):
            return SqlSelect1Check(
                name="postgres_select_1",
                status="pass",
                mode="real",
                message="SELECT 1 ok",
            )
        return SqlSelect1Check(
            name="postgres_select_1",
            status="fail",
            mode="real",
            message=f"PostgreSQL SELECT 1 failed: returned {row!r}",
        )
    except Exception as exc:  # noqa: BLE001 - healthcheck reports failures.
        return SqlSelect1Check(
            name="postgres_select_1",
            status="fail",
            mode="real",
            message=(
                "PostgreSQL SELECT 1 failed: "
                + _sanitize_sql_healthcheck_error(str(exc), config, environ)
            ),
        )
    finally:
        if connection is not None:
            _safe_close(connection)


def check_postgres_test_upload(
    *,
    config: PostgresUploadConfig,
    environ: Mapping[str, str],
    target_table: str,
    raw_target_table: str,
    connect_factory: SqlTestUploadConnectFactory | None = None,
    run_id: str | None = None,
) -> SqlTestUploadCheck:
    errors = config.validate(environ)
    if errors:
        safe_errors = [
            redact_postgres_secrets(error, config, environ) for error in errors
        ]
        return SqlTestUploadCheck(
            name="postgres_test_upload",
            status="skipped",
            mode="not-configured",
            sql_status="skipped",
            message=(
                "sql_status=skipped; SQL test upload skipped because PostgreSQL "
                "configuration is incomplete: "
                + "; ".join(safe_errors)
            ),
        )

    if not has_sql_write_authorization():
        return SqlTestUploadCheck(
            name="postgres_test_upload",
            status="skipped",
            mode="not-configured",
            sql_status="skipped",
            message=(
                "sql_status=skipped; SQL test upload skipped because "
                + authorization_required_message("postgres_test_upload")
            ),
        )

    test_run_id = run_id or f"sql-test-{uuid.uuid4().hex}"
    payload = {
        "run_id": test_run_id,
        "source": _SQL_TEST_SOURCE,
        "message": "paper_pipeline_sql_test_upload",
    }
    connection = None
    try:
        connector = connect_factory or _default_connect
        connection = connector(config, environ)
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            if cursor.fetchone() != (1,):
                raise RuntimeError("SELECT 1 did not return 1 before test upload")
            cursor.execute(_create_test_upload_table_sql(target_table))
            cursor.execute(
                _insert_test_upload_sql(target_table),
                (
                    test_run_id,
                    _SQL_TEST_SOURCE,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                ),
            )
            cursor.execute(
                _select_test_upload_sql(target_table),
                (test_run_id,),
            )
            row = cursor.fetchone()
            if not _test_upload_readback_matches(row, payload):
                raise RuntimeError("test upload readback did not match inserted payload")
        connection.commit()
        return SqlTestUploadCheck(
            name="postgres_test_upload",
            status="pass",
            mode="uploaded",
            sql_status="uploaded",
            message=(
                "sql_status=uploaded; SQL test upload wrote and read back "
                f"target={raw_target_table}; run_id={test_run_id}"
            ),
        )
    except Exception as exc:  # noqa: BLE001 - healthcheck reports failures.
        if connection is not None:
            _safe_rollback(connection)
        return SqlTestUploadCheck(
            name="postgres_test_upload",
            status="fail",
            mode="failed",
            sql_status="failed",
            message=(
                "sql_status=failed; PostgreSQL test upload failed: "
                + _sanitize_sql_healthcheck_error(str(exc), config, environ)
            ),
        )
    finally:
        if connection is not None:
            _safe_close(connection)


def _default_connect(
    config: PostgresUploadConfig,
    environ: Mapping[str, str],
) -> Any:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - depends on local environment.
        raise RuntimeError("psycopg is required for PostgreSQL SELECT 1") from exc

    if config.dsn:
        return psycopg.connect(
            config.dsn,
            sslmode=config.sslmode,
            connect_timeout=config.connect_timeout_seconds,
        )
    return psycopg.connect(**config.connection_kwargs(environ))


@dataclass(frozen=True, slots=True)
class _SqlTestUploadTarget:
    raw_table: str
    quoted_table: str
    error: str


def _resolve_sql_test_upload_target(
    config: PostgresUploadConfig,
    environ: Mapping[str, str],
) -> _SqlTestUploadTarget:
    raw_table = str(environ.get(_SQL_TEST_TABLE_ENV, "")).strip()
    raw_schema = str(environ.get(_SQL_TEST_SCHEMA_ENV, "")).strip()
    if not raw_table:
        return _SqlTestUploadTarget(
            raw_table="",
            quoted_table="",
            error=(
                "SQL test upload skipped because PAPER_PIPELINE_SQL_TEST_TABLE "
                "is not configured; no production table will be used by default."
            ),
        )
    if raw_schema and "." in raw_table:
        return _SqlTestUploadTarget(
            raw_table=raw_table,
            quoted_table="",
            error=(
                "SQL test upload target is ambiguous: configure either "
                "PAPER_PIPELINE_SQL_TEST_SCHEMA plus table name, or a qualified "
                "PAPER_PIPELINE_SQL_TEST_TABLE."
            ),
        )
    target = f"{raw_schema}.{raw_table}" if raw_schema else raw_table
    if target == config.table_name:
        return _SqlTestUploadTarget(
            raw_table=target,
            quoted_table="",
            error=(
                "SQL test upload skipped because PAPER_PIPELINE_SQL_TEST_TABLE "
                "matches the production POSTGRES_TABLE."
            ),
        )
    try:
        quoted = quote_qualified_identifier(target)
    except ValueError as exc:
        return _SqlTestUploadTarget(
            raw_table=target,
            quoted_table="",
            error=f"SQL test upload target is invalid: {exc}",
        )
    return _SqlTestUploadTarget(raw_table=target, quoted_table=quoted, error="")


def _create_test_upload_table_sql(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    run_id text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    source text NOT NULL,
    payload_json jsonb NOT NULL
)
""".strip()


def _insert_test_upload_sql(table: str) -> str:
    return f"""
INSERT INTO {table} (
    run_id,
    source,
    payload_json
) VALUES (
    %s, %s, %s::jsonb
)
ON CONFLICT (run_id) DO UPDATE SET
    source = EXCLUDED.source,
    payload_json = EXCLUDED.payload_json,
    created_at = now()
""".strip()


def _select_test_upload_sql(table: str) -> str:
    return f"""
SELECT source, payload_json
FROM {table}
WHERE run_id = %s
""".strip()


def _test_upload_readback_matches(row: object, payload: dict[str, str]) -> bool:
    if not isinstance(row, tuple) or len(row) != 2:
        return False
    source, readback_payload = row
    if source != _SQL_TEST_SOURCE:
        return False
    if isinstance(readback_payload, str):
        try:
            readback_payload = json.loads(readback_payload)
        except json.JSONDecodeError:
            return False
    if not isinstance(readback_payload, dict):
        return False
    return all(readback_payload.get(key) == value for key, value in payload.items())


def _safe_close(connection: Any) -> None:
    try:
        connection.close()
    except Exception:
        pass


def _safe_rollback(connection: Any) -> None:
    try:
        connection.rollback()
    except Exception:
        pass


def _sanitize_sql_healthcheck_error(
    text: str,
    config: PostgresUploadConfig,
    environ: Mapping[str, str],
) -> str:
    redacted = redact_postgres_secrets(text, config, environ)
    redacted = re.sub(
        r"(?i)\bauthorization\s*:\s*bearer\s+[^;\s]+",
        "<redacted:credential>",
        redacted,
    )
    redacted = re.sub(
        r"(?i)\bbearer\s+[^;\s]+",
        "<redacted:credential>",
        redacted,
    )
    redacted = re.sub(
        r"(?i)\b(password|token|api_key|secret)=([^;,\s]+)",
        r"\1=<redacted>",
        redacted,
    )
    for key, value in environ.items():
        if not _is_sensitive_key(key):
            continue
        raw = str(value)
        if raw:
            redacted = redacted.replace(raw, f"<redacted:{key}>")
    return redacted


def _is_sensitive_key(key: str) -> bool:
    upper = key.upper()
    return any(part in upper for part in _SENSITIVE_NAME_PARTS)
