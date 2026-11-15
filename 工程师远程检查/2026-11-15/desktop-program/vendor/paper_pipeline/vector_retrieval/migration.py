"""Safe migration preview helpers for vector retrieval schema."""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any, Mapping

from paper_pipeline.vector_retrieval.postgres_adapter import (
    ConnectFactory,
    PostgresVectorConfig,
)
from paper_pipeline.vector_retrieval.schema import build_vector_schema_sql
from paper_pipeline.vector_retrieval.schema import iter_vector_schema_statements


@dataclass(frozen=True, slots=True)
class MigrationPreview:
    operation: str
    sql: str
    dry_run: bool = True
    requires_explicit_confirmation: bool = True
    will_execute: bool = False


def build_migration_preview(dimension: int = 1536) -> MigrationPreview:
    return MigrationPreview(
        operation="vector_schema_migration",
        sql=build_vector_schema_sql(dimension=dimension),
    )


class MigrationNotConfirmedError(RuntimeError):
    """Raised when a real migration is requested without confirmation."""


@dataclass(frozen=True, slots=True)
class MigrationExecutionResult:
    executed: bool
    statement_count: int
    operation: str = "vector_schema_migration"


@dataclass(slots=True)
class MigrationExecutor:
    config: PostgresVectorConfig
    environ: Mapping[str, str] | None = None
    connect_factory: ConnectFactory | None = None

    @classmethod
    def from_environ(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        connect_factory: ConnectFactory | None = None,
    ) -> "MigrationExecutor":
        source = environ if environ is not None else os.environ
        return cls(
            config=PostgresVectorConfig.from_environ(source),
            environ=source,
            connect_factory=connect_factory,
        )

    def execute(
        self,
        *,
        dimension: int = 1536,
        confirm: bool = False,
    ) -> MigrationExecutionResult:
        if not confirm:
            raise MigrationNotConfirmedError(
                "vector schema migration requires explicit confirmation",
            )
        source = self.environ if self.environ is not None else os.environ
        errors = self.config.validate(source)
        if errors:
            raise RuntimeError(f"PostgreSQL vector configuration error: {'; '.join(errors)}")
        connection = self._connect(source)
        statements = iter_vector_schema_statements(dimension=dimension)
        try:
            with connection.cursor() as cursor:
                for statement in statements:
                    cursor.execute(statement)
            connection.commit()
        except Exception:
            _safe_rollback(connection)
            raise
        finally:
            _safe_close(connection)
        return MigrationExecutionResult(
            executed=True,
            statement_count=len(statements),
        )

    def _connect(self, environ: Mapping[str, str]) -> Any:
        if self.connect_factory is not None:
            return self.connect_factory(self.config, environ)
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover - environment dependent.
            raise RuntimeError("psycopg is required for vector schema migration") from exc
        if self.config.dsn:
            return psycopg.connect(
                self.config.dsn,
                sslmode=self.config.sslmode,
                connect_timeout=self.config.connect_timeout_seconds,
            )
        return psycopg.connect(
            host=self.config.host,
            port=self.config.port,
            dbname=self.config.database,
            user=self.config.user,
            password=self.config.resolve_password(environ),
            sslmode=self.config.sslmode,
            connect_timeout=self.config.connect_timeout_seconds,
        )


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
