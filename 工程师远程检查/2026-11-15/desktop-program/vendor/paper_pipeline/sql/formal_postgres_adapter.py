"""Configuration and safety guard for formal PostgreSQL uploads.

S14-6a intentionally stops before creating a real connection or executing an
upload plan. It only parses formal-specific environment variables and exposes
the guard state needed by the next step.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from paper_pipeline.indexing.package_validation import validate_local_index_package
from paper_pipeline.sql.formal_adapter import (
    FormalUploadExecutionResult,
    execute_formal_upload_plan,
)
from paper_pipeline.sql.formal_upload_plan import FormalUploadPlan

FORMAL_SQL_MODE_ENV = "PAPER_PIPELINE_FORMAL_SQL_MODE"
FORMAL_SQL_ALLOW_WRITE_ENV = "PAPER_PIPELINE_FORMAL_SQL_ALLOW_WRITE"
FORMAL_POSTGRES_DSN_ENV = "PAPER_PIPELINE_FORMAL_POSTGRES_DSN"
FORMAL_POSTGRES_HOST_ENV = "PAPER_PIPELINE_FORMAL_POSTGRES_HOST"
FORMAL_POSTGRES_PORT_ENV = "PAPER_PIPELINE_FORMAL_POSTGRES_PORT"
FORMAL_POSTGRES_DB_ENV = "PAPER_PIPELINE_FORMAL_POSTGRES_DB"
FORMAL_POSTGRES_USER_ENV = "PAPER_PIPELINE_FORMAL_POSTGRES_USER"
FORMAL_POSTGRES_PASSWORD_ENV_ENV = "PAPER_PIPELINE_FORMAL_POSTGRES_PASSWORD_ENV"
FORMAL_POSTGRES_PASSWORD_ENV = "PAPER_PIPELINE_FORMAL_POSTGRES_PASSWORD"
FORMAL_POSTGRES_SSLMODE_ENV = "PAPER_PIPELINE_FORMAL_POSTGRES_SSLMODE"
FORMAL_POSTGRES_CONNECT_TIMEOUT_ENV = "PAPER_PIPELINE_FORMAL_POSTGRES_CONNECT_TIMEOUT"

MODE_DISABLED = "disabled"
MODE_REAL = "real"

REASON_MODE_NOT_REAL = "formal_sql_mode_not_real"
REASON_WRITE_NOT_ALLOWED = "formal_sql_write_not_allowed"
REASON_CONFIG_INCOMPLETE = "formal_postgres_config_incomplete"
REASON_INVALID_PORT = "formal_postgres_invalid_port"
REASON_INVALID_CONNECT_TIMEOUT = "formal_postgres_invalid_connect_timeout"
REASON_CONNECT_FACTORY_MISSING = "formal_postgres_connect_factory_missing"
REASON_PACKAGE_VALIDATION_FAILED = "formal_execute_package_validation_failed"

ConnectFactory = Callable[..., Any]


@dataclass(frozen=True)
class FormalPostgresConfig:
    mode: str
    allow_write: bool
    dsn: str | None = None
    host: str | None = None
    port: int | None = None
    database: str | None = None
    user: str | None = None
    password: str | None = None
    sslmode: str | None = None
    connect_timeout: int | None = None
    password_env_name: str | None = None
    validation_errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class FormalPostgresUploadAdapter:
    config: FormalPostgresConfig
    connect_factory: ConnectFactory | None = None

    @classmethod
    def from_environ(
        cls,
        environ: Mapping[str, str],
        *,
        connect_factory: ConnectFactory | None = None,
    ) -> FormalPostgresUploadAdapter:
        return cls(
            config=formal_postgres_config_from_environ(environ),
            connect_factory=connect_factory,
        )

    @property
    def ready_for_real_write(self) -> bool:
        return is_real_write_enabled(self.config)

    def assert_real_write_enabled(self) -> None:
        reasons = _real_write_disabled_reasons(self.config)
        if reasons:
            raise RuntimeError(";".join(reasons))

    def execute_plan(self, plan: FormalUploadPlan) -> FormalUploadExecutionResult:
        self.assert_real_write_enabled()
        _assert_plan_package_uploadable(plan)
        if self.connect_factory is None:
            raise RuntimeError(REASON_CONNECT_FACTORY_MISSING)

        kwargs = build_connect_kwargs(self.config)
        connection = self.connect_factory(**kwargs)
        return execute_formal_upload_plan(plan, connection)


def formal_postgres_config_from_environ(
    environ: Mapping[str, str],
) -> FormalPostgresConfig:
    errors: list[str] = []
    mode = (_optional_value(environ, FORMAL_SQL_MODE_ENV) or MODE_DISABLED).lower()
    port = _positive_int_from_environ(
        environ,
        FORMAL_POSTGRES_PORT_ENV,
        REASON_INVALID_PORT,
        errors,
    )
    connect_timeout = _positive_int_from_environ(
        environ,
        FORMAL_POSTGRES_CONNECT_TIMEOUT_ENV,
        REASON_INVALID_CONNECT_TIMEOUT,
        errors,
    )
    password_env_name = _optional_value(environ, FORMAL_POSTGRES_PASSWORD_ENV_ENV)
    password = (
        _optional_value(environ, password_env_name)
        if password_env_name is not None
        else _optional_value(environ, FORMAL_POSTGRES_PASSWORD_ENV)
    )

    return FormalPostgresConfig(
        mode=mode,
        allow_write=environ.get(FORMAL_SQL_ALLOW_WRITE_ENV) == "1",
        dsn=_optional_value(environ, FORMAL_POSTGRES_DSN_ENV),
        host=_optional_value(environ, FORMAL_POSTGRES_HOST_ENV),
        port=port,
        database=_optional_value(environ, FORMAL_POSTGRES_DB_ENV),
        user=_optional_value(environ, FORMAL_POSTGRES_USER_ENV),
        password=password,
        sslmode=_optional_value(environ, FORMAL_POSTGRES_SSLMODE_ENV),
        connect_timeout=connect_timeout,
        password_env_name=password_env_name,
        validation_errors=tuple(errors),
    )


def is_real_write_enabled(config: FormalPostgresConfig) -> bool:
    return not _real_write_disabled_reasons(config)


def build_connect_kwargs(config: FormalPostgresConfig) -> dict[str, Any]:
    reasons = _connection_config_error_reasons(config)
    if reasons:
        raise ValueError(";".join(reasons))

    kwargs: dict[str, Any]
    if config.dsn is not None:
        kwargs = {"conninfo": config.dsn}
    else:
        kwargs = {
            "host": config.host,
            "dbname": config.database,
            "user": config.user,
        }
        if config.port is not None:
            kwargs["port"] = config.port

    if config.password is not None:
        kwargs["password"] = config.password
    if config.sslmode is not None:
        kwargs["sslmode"] = config.sslmode
    if config.connect_timeout is not None:
        kwargs["connect_timeout"] = config.connect_timeout
    return kwargs


def _assert_plan_package_uploadable(plan: FormalUploadPlan) -> None:
    validation = validate_local_index_package(plan.package_dir)
    if validation.valid:
        return
    raise RuntimeError(_package_validation_error(validation.issues))


def _package_validation_error(issues: list[Any]) -> str:
    issue_codes = ",".join(_issue_code(issue) for issue in issues)
    issue_details = ";".join(
        f"{_issue_code(issue)}:{message}"
        for issue in issues
        if (message := _issue_message(issue)) is not None
    )
    if not issue_details:
        return f"{REASON_PACKAGE_VALIDATION_FAILED}:{issue_codes}"
    return f"{REASON_PACKAGE_VALIDATION_FAILED}:{issue_codes};details={issue_details}"


def _issue_code(issue: Any) -> str:
    code = str(getattr(issue, "code", "")).strip()
    return code or "unknown_validation_issue"


def _issue_message(issue: Any) -> str | None:
    raw_message = getattr(issue, "message", None)
    if raw_message is None:
        return None
    message = str(raw_message).strip()
    return message or None


def _real_write_disabled_reasons(config: FormalPostgresConfig) -> tuple[str, ...]:
    reasons: list[str] = []
    if config.mode != MODE_REAL:
        reasons.append(REASON_MODE_NOT_REAL)
    if not config.allow_write:
        reasons.append(REASON_WRITE_NOT_ALLOWED)
    reasons.extend(_connection_config_error_reasons(config))
    return tuple(reasons)


def _connection_config_error_reasons(config: FormalPostgresConfig) -> tuple[str, ...]:
    reasons = list(config.validation_errors)
    if not _has_complete_connection_config(config):
        reasons.append(REASON_CONFIG_INCOMPLETE)
    return tuple(reasons)


def _has_complete_connection_config(config: FormalPostgresConfig) -> bool:
    if config.dsn is not None:
        return True
    return (
        config.host is not None
        and config.database is not None
        and config.user is not None
    )


def _optional_value(environ: Mapping[str, str], key: str | None) -> str | None:
    if key is None:
        return None
    value = environ.get(key)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _positive_int_from_environ(
    environ: Mapping[str, str],
    key: str,
    reason: str,
    errors: list[str],
) -> int | None:
    raw_value = _optional_value(environ, key)
    if raw_value is None:
        return None
    try:
        value = int(raw_value)
    except ValueError:
        errors.append(reason)
        return None
    if value <= 0:
        errors.append(reason)
        return None
    return value
