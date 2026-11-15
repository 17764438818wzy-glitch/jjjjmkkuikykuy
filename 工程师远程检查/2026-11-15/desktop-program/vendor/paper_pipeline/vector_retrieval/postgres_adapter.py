"""PostgreSQL / pgvector adapter boundary for vector retrieval.

This module keeps real database access behind configuration and future adapter
methods. Unit tests do not connect to PostgreSQL.
"""

from __future__ import annotations

import os
import json
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from paper_pipeline.vector_retrieval.models import (
    CandidateChunk,
    ChunkEmbedding,
    EmbeddingCostRecord,
    FormalResource,
    PaperChunk,
    RetrievalRecord,
    SqlFilter,
    VectorProcessRecord,
)


ConnectFactory = Callable[["PostgresVectorConfig", Mapping[str, str]], Any]


@dataclass(frozen=True, slots=True)
class PostgresVectorConfig:
    dsn: str = ""
    host: str = ""
    port: int = 5432
    database: str = ""
    user: str = ""
    password_env_name: str = "POSTGRES_PASSWORD"
    sslmode: str = "prefer"
    connect_timeout_seconds: int = 10
    embedding_dimension: int = 1536
    embedding_provider: str = ""
    embedding_model: str = ""

    @classmethod
    def from_environ(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> PostgresVectorConfig:
        source = environ if environ is not None else os.environ
        return cls(
            dsn=str(source.get("POSTGRES_DSN", source.get("DATABASE_URL", ""))).strip(),
            host=str(source.get("POSTGRES_HOST", "")).strip(),
            port=_int_from_env(source, "POSTGRES_PORT", 5432),
            database=str(source.get("POSTGRES_DB", source.get("POSTGRES_DATABASE", ""))).strip(),
            user=str(source.get("POSTGRES_USER", "")).strip(),
            password_env_name=str(source.get("POSTGRES_PASSWORD_ENV", "POSTGRES_PASSWORD")).strip()
            or "POSTGRES_PASSWORD",
            sslmode=str(source.get("POSTGRES_SSLMODE", "prefer")).strip() or "prefer",
            connect_timeout_seconds=_int_from_env(source, "POSTGRES_CONNECT_TIMEOUT", 10),
            embedding_dimension=_int_from_env(source, "VECTOR_EMBEDDING_DIMENSION", 1536),
            embedding_provider=str(source.get("VECTOR_EMBEDDING_PROVIDER", "")).strip(),
            embedding_model=str(source.get("VECTOR_EMBEDDING_MODEL", "")).strip(),
        )

    def resolve_password(self, environ: Mapping[str, str] | None = None) -> str:
        source = environ if environ is not None else os.environ
        return str(source.get(self.password_env_name, "")).strip()

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
        if self.embedding_dimension <= 0:
            errors.append("VECTOR_EMBEDDING_DIMENSION must be greater than 0")
        return errors

    def safe_summary(self, environ: Mapping[str, str] | None = None) -> dict[str, Any]:
        return {
            "dsn": "SET" if self.dsn else "UNSET",
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": "SET" if self.resolve_password(environ) else "UNSET",
            "password_env_name": self.password_env_name,
            "sslmode": self.sslmode,
            "connect_timeout_seconds": self.connect_timeout_seconds,
            "embedding_dimension": self.embedding_dimension,
            "embedding_provider": self.embedding_provider,
            "embedding_model": self.embedding_model,
        }


def _int_from_env(source: Mapping[str, str], key: str, default: int) -> int:
    raw = str(source.get(key, "")).strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass(slots=True)
class PostgresVectorRepository:
    config: PostgresVectorConfig
    environ: Mapping[str, str] | None = None
    connect_factory: ConnectFactory | None = None

    @classmethod
    def from_environ(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        connect_factory: ConnectFactory | None = None,
    ) -> "PostgresVectorRepository":
        source = environ if environ is not None else os.environ
        return cls(
            config=PostgresVectorConfig.from_environ(source),
            environ=source,
            connect_factory=connect_factory,
        )

    def save_chunks(self, chunks: list[PaperChunk]) -> None:
        if not chunks:
            return
        connection = self._connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    _deactivate_existing_chunks_sql(),
                    _deactivate_existing_chunks_params(chunks),
                )
                for chunk in chunks:
                    cursor.execute(_insert_chunk_sql(), _chunk_params(chunk))
            connection.commit()
        finally:
            _safe_close(connection)

    def save_embeddings(self, embeddings: list[ChunkEmbedding]) -> None:
        if not embeddings:
            return
        connection = self._connect()
        try:
            with connection.cursor() as cursor:
                for embedding in embeddings:
                    cursor.execute(
                        _insert_embedding_sql(),
                        _embedding_params(embedding),
                    )
            connection.commit()
        finally:
            _safe_close(connection)

    def search_chunks(
        self,
        query_vector: list[float],
        filters: SqlFilter,
        *,
        top_k: int,
    ) -> list[CandidateChunk]:
        connection = self._connect()
        try:
            with connection.cursor() as cursor:
                params = _search_params(query_vector, filters, top_k, self.config)
                cursor.execute(_search_sql(filters, self.config), params)
                rows = cursor.fetchall()
            return [_candidate_from_row(row) for row in rows]
        finally:
            _safe_close(connection)

    def _connect(self) -> Any:
        environ = self.environ if self.environ is not None else os.environ
        errors = self.config.validate(environ)
        if errors:
            raise RuntimeError(f"PostgreSQL vector configuration error: {'; '.join(errors)}")
        if self.connect_factory is not None:
            return self.connect_factory(self.config, environ)
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:  # pragma: no cover - environment dependent.
            raise RuntimeError(
                "psycopg is required for PostgreSQL vector retrieval",
            ) from exc
        if self.config.dsn:
            return psycopg.connect(
                self.config.dsn,
                row_factory=dict_row,
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
            row_factory=dict_row,
        )


@dataclass(slots=True)
class PostgresResourceRepository:
    config: PostgresVectorConfig
    environ: Mapping[str, str] | None = None
    connect_factory: ConnectFactory | None = None

    def list_vector_eligible_resources(self) -> list[FormalResource]:
        connection = _connect(self.config, self.environ, self.connect_factory)
        try:
            with connection.cursor() as cursor:
                cursor.execute(_eligible_resources_sql())
                rows = cursor.fetchall()
            return [_resource_from_row(row) for row in rows]
        finally:
            _safe_close(connection)

    def get_resource(self, resource_id: str) -> FormalResource | None:
        connection = _connect(self.config, self.environ, self.connect_factory)
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"{_eligible_resources_sql()} AND pr.id = %(resource_id)s",
                    {"resource_id": resource_id},
                )
                rows = cursor.fetchall()
            return _resource_from_row(rows[0]) if rows else None
        finally:
            _safe_close(connection)


@dataclass(slots=True)
class PostgresRecordRepository:
    config: PostgresVectorConfig
    environ: Mapping[str, str] | None = None
    connect_factory: ConnectFactory | None = None

    def save_retrieval_record(self, record: RetrievalRecord) -> None:
        connection = _connect(self.config, self.environ, self.connect_factory)
        try:
            with connection.cursor() as cursor:
                cursor.execute(_insert_retrieval_record_sql(), _retrieval_params(record))
            connection.commit()
        finally:
            _safe_close(connection)

    def save_cost_record(self, record: EmbeddingCostRecord) -> None:
        connection = _connect(self.config, self.environ, self.connect_factory)
        try:
            with connection.cursor() as cursor:
                cursor.execute(_insert_cost_record_sql(), _cost_params(record))
            connection.commit()
        finally:
            _safe_close(connection)

    def save_process_record(self, record: VectorProcessRecord) -> None:
        connection = _connect(self.config, self.environ, self.connect_factory)
        try:
            with connection.cursor() as cursor:
                cursor.execute(_insert_process_record_sql(), _process_params(record))
            connection.commit()
        finally:
            _safe_close(connection)


def _connect(
    config: PostgresVectorConfig,
    environ: Mapping[str, str] | None,
    connect_factory: ConnectFactory | None,
) -> Any:
    source = environ if environ is not None else os.environ
    errors = config.validate(source)
    if errors:
        raise RuntimeError(f"PostgreSQL vector configuration error: {'; '.join(errors)}")
    if connect_factory is not None:
        return connect_factory(config, source)
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover - environment dependent.
        raise RuntimeError("psycopg is required for PostgreSQL vector retrieval") from exc
    if config.dsn:
        return psycopg.connect(
            config.dsn,
            row_factory=dict_row,
            sslmode=config.sslmode,
            connect_timeout=config.connect_timeout_seconds,
        )
    return psycopg.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.user,
        password=config.resolve_password(source),
        sslmode=config.sslmode,
        connect_timeout=config.connect_timeout_seconds,
        row_factory=dict_row,
    )


def _insert_chunk_sql() -> str:
    return """
INSERT INTO paper_chunks (
    chunk_id, resource_id, paper_id, title, major, discipline, category,
    direction, thesis_type, tier, resource_status, chapter_title,
    chapter_role, paragraph_order, chunk_order, chunk_text, chunk_hash,
    text_hash, content_role, needs_real_data, research_method_tags,
    source_hash, chunk_rule_version, chunk_status, created_at, updated_at
) VALUES (
    %(chunk_id)s, %(resource_id)s, %(paper_id)s, %(title)s, %(major)s,
    %(discipline)s, %(category)s, %(direction)s, %(thesis_type)s, %(tier)s,
    %(resource_status)s, %(chapter_title)s, %(chapter_role)s,
    %(paragraph_order)s, %(chunk_order)s, %(chunk_text)s, %(chunk_hash)s,
    %(text_hash)s, %(content_role)s, %(needs_real_data)s,
    %(research_method_tags)s::jsonb, %(source_hash)s,
    %(chunk_rule_version)s, %(chunk_status)s, %(created_at)s, %(updated_at)s
)
ON CONFLICT (chunk_id) DO UPDATE SET
    chunk_text = EXCLUDED.chunk_text,
    chunk_hash = EXCLUDED.chunk_hash,
    content_role = EXCLUDED.content_role,
    needs_real_data = EXCLUDED.needs_real_data,
    research_method_tags = EXCLUDED.research_method_tags,
    chunk_rule_version = EXCLUDED.chunk_rule_version,
    chunk_status = EXCLUDED.chunk_status,
    updated_at = EXCLUDED.updated_at
"""


def _deactivate_existing_chunks_sql() -> str:
    return """
UPDATE paper_chunks
SET
    chunk_status = 'inactive',
    updated_at = NOW()
WHERE resource_id = ANY(%(resource_ids)s::uuid[])
  AND chunk_status = 'active'
"""


def _insert_embedding_sql() -> str:
    return """
INSERT INTO paper_chunk_embeddings (
    embedding_id, chunk_id, provider, model, dimension, embedding_vector,
    input_text_hash, embedding_status, error_message, estimated_tokens,
    estimated_cost, created_at, updated_at
) VALUES (
    %(embedding_id)s, %(chunk_id)s, %(provider)s, %(model)s, %(dimension)s,
    %(embedding_vector)s, %(input_text_hash)s, %(embedding_status)s,
    %(error_message)s, %(estimated_tokens)s, %(estimated_cost)s,
    %(created_at)s, %(updated_at)s
)
ON CONFLICT (chunk_id, provider, model) DO UPDATE SET
    embedding_vector = EXCLUDED.embedding_vector,
    input_text_hash = EXCLUDED.input_text_hash,
    embedding_status = EXCLUDED.embedding_status,
    error_message = EXCLUDED.error_message,
    estimated_tokens = EXCLUDED.estimated_tokens,
    estimated_cost = EXCLUDED.estimated_cost,
    updated_at = EXCLUDED.updated_at
"""


def _eligible_resources_sql() -> str:
    return """
SELECT
    pr.id AS resource_id,
    pr.paper_id,
    pr.title,
    pr.major,
    pr.discipline,
    pr.category,
    pr.direction,
    pr.thesis_type,
    pr.tier,
    pr.resource_status,
    pr.version_status,
    pr.sensitive_clean,
    pr.index_status,
    pr.sql_status,
    pr.markdown_content,
    pr.source_hash,
    pr.text_hash,
    COALESCE(pr.research_methods, '[]'::jsonb) AS research_method_tags
FROM paper_resources pr
WHERE pr.tier IN ('gold', 'silver')
  AND pr.resource_status = 'active'
  AND pr.version_status = 'current'
  AND pr.sensitive_clean = true
  AND pr.index_status = 'indexed'
  AND pr.sql_status = 'uploaded'
  AND COALESCE(pr.markdown_content, '') <> ''
"""


def _insert_retrieval_record_sql() -> str:
    return """
INSERT INTO retrieval_records (
    retrieval_id, query_title, major, direction, thesis_type,
    research_method_preference, tier_preference, sql_filters, hit_chunks,
    structured_material_package, material_package_preview,
    embedding_config_version, retrieval_config_version, created_at
) VALUES (
    %(retrieval_id)s, %(query_title)s, %(major)s, %(direction)s,
    %(thesis_type)s, %(research_method_preference)s, %(tier_preference)s,
    %(sql_filters)s::jsonb, %(hit_chunks)s::jsonb,
    %(structured_material_package)s::jsonb, %(material_package_preview)s,
    %(embedding_config_version)s, %(retrieval_config_version)s, %(created_at)s
)
ON CONFLICT (retrieval_id) DO UPDATE SET
    hit_chunks = EXCLUDED.hit_chunks,
    structured_material_package = EXCLUDED.structured_material_package,
    material_package_preview = EXCLUDED.material_package_preview
"""


def _insert_cost_record_sql() -> str:
    return """
INSERT INTO embedding_cost_records (
    cost_record_id, batch_id, paper_count, chunk_count, input_char_count,
    estimated_tokens, provider, model, success_count, failed_count,
    started_at, completed_at, estimated_cost, actual_cost, operator, created_at
) VALUES (
    %(cost_record_id)s, %(batch_id)s, %(paper_count)s, %(chunk_count)s,
    %(input_char_count)s, %(estimated_tokens)s, %(provider)s, %(model)s,
    %(success_count)s, %(failed_count)s, %(started_at)s, %(completed_at)s,
    %(estimated_cost)s, %(actual_cost)s, %(operator)s, %(created_at)s
)
ON CONFLICT (cost_record_id) DO NOTHING
"""


def _insert_process_record_sql() -> str:
    return """
INSERT INTO vector_process_records (
    process_id, batch_id, resource_id, paper_id, stage, status,
    failure_stage, error_message, retryable, retry_count, last_retry_at,
    operator, created_at, updated_at
) VALUES (
    %(process_id)s, %(batch_id)s, %(resource_id)s, %(paper_id)s,
    %(stage)s, %(status)s, %(failure_stage)s, %(error_message)s,
    %(retryable)s, %(retry_count)s, %(last_retry_at)s,
    %(operator)s, %(created_at)s, %(updated_at)s
)
ON CONFLICT (process_id) DO UPDATE SET
    status = EXCLUDED.status,
    failure_stage = EXCLUDED.failure_stage,
    error_message = EXCLUDED.error_message,
    retryable = EXCLUDED.retryable,
    retry_count = EXCLUDED.retry_count,
    last_retry_at = EXCLUDED.last_retry_at,
    updated_at = EXCLUDED.updated_at
"""


def _search_sql(filters: SqlFilter, config: PostgresVectorConfig) -> str:
    clauses = ["pce.embedding_status = 'embedded'", "pc.chunk_status = 'active'"]
    if config.embedding_provider:
        clauses.append("pce.provider = %(embedding_provider)s")
    if config.embedding_model:
        clauses.append("pce.model = %(embedding_model)s")
    if config.embedding_dimension > 0:
        clauses.append("pce.dimension = %(embedding_dimension)s")
    if filters.major:
        clauses.append("(pc.major = %(major)s OR pc.discipline = %(major)s)")
    if filters.direction:
        clauses.append("pc.direction = %(direction)s")
    if filters.thesis_type:
        clauses.append("pc.thesis_type = %(thesis_type)s")
    if filters.tier_preference:
        clauses.append("pc.tier = %(tier_preference)s")
    if filters.research_method_preference:
        clauses.append("pc.research_method_tags ? %(research_method_preference)s")
    where = " AND ".join(clauses)
    return f"""
SELECT
    pc.*,
    1 - (pce.embedding_vector <=> %(query_vector)s::vector) AS similarity_score
FROM paper_chunks pc
JOIN paper_chunk_embeddings pce ON pce.chunk_id = pc.chunk_id
WHERE {where}
ORDER BY pce.embedding_vector <=> %(query_vector)s::vector
LIMIT %(top_k)s
"""


def _chunk_params(chunk: PaperChunk) -> dict[str, Any]:
    return {
        "chunk_id": chunk.chunk_id,
        "resource_id": chunk.resource_id,
        "paper_id": chunk.paper_id,
        "title": chunk.title,
        "major": chunk.major,
        "discipline": chunk.discipline,
        "category": chunk.category,
        "direction": chunk.direction,
        "thesis_type": chunk.thesis_type,
        "tier": chunk.tier,
        "resource_status": chunk.resource_status,
        "chapter_title": chunk.chapter_title,
        "chapter_role": chunk.chapter_role,
        "paragraph_order": chunk.paragraph_order,
        "chunk_order": chunk.chunk_order,
        "chunk_text": chunk.chunk_text,
        "chunk_hash": chunk.chunk_hash,
        "text_hash": chunk.text_hash,
        "content_role": chunk.content_role,
        "needs_real_data": chunk.needs_real_data,
        "research_method_tags": _json_array(chunk.research_method_tags),
        "source_hash": chunk.source_hash,
        "chunk_rule_version": chunk.chunk_rule_version,
        "chunk_status": chunk.chunk_status,
        "created_at": chunk.created_at,
        "updated_at": chunk.updated_at,
    }


def _deactivate_existing_chunks_params(chunks: list[PaperChunk]) -> dict[str, Any]:
    resource_ids = sorted({chunk.resource_id for chunk in chunks})
    return {"resource_ids": resource_ids}


def _embedding_params(embedding: ChunkEmbedding) -> dict[str, Any]:
    return {
        "embedding_id": embedding.embedding_id,
        "chunk_id": embedding.chunk_id,
        "provider": embedding.provider,
        "model": embedding.model,
        "dimension": embedding.dimension,
        "embedding_vector": embedding.embedding_vector,
        "input_text_hash": embedding.input_text_hash,
        "embedding_status": embedding.embedding_status,
        "error_message": embedding.error_message,
        "estimated_tokens": embedding.estimated_tokens,
        "estimated_cost": embedding.estimated_cost,
        "created_at": embedding.created_at,
        "updated_at": embedding.updated_at,
    }


def _search_params(
    query_vector: list[float],
    filters: SqlFilter,
    top_k: int,
    config: PostgresVectorConfig,
) -> dict[str, Any]:
    return {
        "query_vector": query_vector,
        "embedding_provider": config.embedding_provider,
        "embedding_model": config.embedding_model,
        "embedding_dimension": config.embedding_dimension,
        "major": filters.major,
        "direction": filters.direction,
        "thesis_type": filters.thesis_type,
        "tier_preference": filters.tier_preference,
        "research_method_preference": filters.research_method_preference,
        "top_k": top_k,
    }


def _candidate_from_row(row: Mapping[str, Any]) -> CandidateChunk:
    research_tags = row.get("research_method_tags", [])
    if not isinstance(research_tags, list):
        research_tags = []
    chunk = PaperChunk(
        chunk_id=str(row["chunk_id"]),
        resource_id=str(row["resource_id"]),
        paper_id=str(row["paper_id"]),
        title=str(row["title"]),
        major=str(row["major"]),
        discipline=str(row["discipline"]),
        category=str(row["category"]),
        direction=str(row["direction"]),
        thesis_type=str(row["thesis_type"]),
        tier=str(row["tier"]),
        resource_status=str(row["resource_status"]),
        chapter_title=str(row["chapter_title"]),
        chapter_role=str(row["chapter_role"]),
        paragraph_order=int(row["paragraph_order"]),
        chunk_order=int(row["chunk_order"]),
        chunk_text=str(row["chunk_text"]),
        chunk_hash=str(row["chunk_hash"]),
        text_hash=str(row["text_hash"]),
        content_role=str(row["content_role"]),
        needs_real_data=bool(row["needs_real_data"]),
        research_method_tags=[str(item) for item in research_tags],
        source_hash=str(row["source_hash"]),
        chunk_rule_version=str(row.get("chunk_rule_version", "chunk.v1")),
        chunk_status=str(row.get("chunk_status", "active")),
    )
    return CandidateChunk(
        chunk=chunk,
        similarity_score=float(row.get("similarity_score", 0.0)),
    )


def _resource_from_row(row: Mapping[str, Any]) -> FormalResource:
    tags = row.get("research_method_tags", [])
    if not isinstance(tags, list):
        tags = []
    return FormalResource(
        resource_id=str(row["resource_id"]),
        paper_id=str(row["paper_id"]),
        title=str(row.get("title", "")),
        major=str(row.get("major", "")),
        discipline=str(row.get("discipline", "")),
        category=str(row.get("category", "")),
        direction=str(row.get("direction", "")),
        thesis_type=str(row.get("thesis_type", "")),
        tier=str(row.get("tier", "")),
        resource_status=str(row.get("resource_status", "")),
        version_status=str(row.get("version_status", "")),
        sensitive_clean=bool(row.get("sensitive_clean", False)),
        index_status=str(row.get("index_status", "")),
        sql_status=str(row.get("sql_status", "")),
        markdown_content=str(row.get("markdown_content", "")),
        source_hash=str(row.get("source_hash", "")),
        text_hash=str(row.get("text_hash", "")),
        research_method_tags=[str(item) for item in tags],
    )


def _retrieval_params(record: RetrievalRecord) -> dict[str, Any]:
    return {
        "retrieval_id": record.retrieval_id,
        "query_title": record.query_title,
        "major": record.major,
        "direction": record.direction,
        "thesis_type": record.thesis_type,
        "research_method_preference": "",
        "tier_preference": "",
        "sql_filters": _json_object(record.sql_filters),
        "hit_chunks": _json_array_object(record.hit_chunks),
        "structured_material_package": _json_object(record.structured_material_package),
        "material_package_preview": record.material_package_preview,
        "embedding_config_version": record.embedding_config_version,
        "retrieval_config_version": record.retrieval_config_version,
        "created_at": record.created_at,
    }


def _cost_params(record: EmbeddingCostRecord) -> dict[str, Any]:
    return {
        "cost_record_id": record.cost_record_id,
        "batch_id": record.batch_id,
        "paper_count": record.paper_count,
        "chunk_count": record.chunk_count,
        "input_char_count": record.input_char_count,
        "estimated_tokens": record.estimated_tokens,
        "provider": record.provider,
        "model": record.model,
        "success_count": record.success_count,
        "failed_count": record.failed_count,
        "started_at": record.started_at,
        "completed_at": record.completed_at,
        "estimated_cost": record.estimated_cost,
        "actual_cost": record.actual_cost,
        "operator": record.operator,
        "created_at": record.created_at,
    }


def _process_params(record: VectorProcessRecord) -> dict[str, Any]:
    return {
        "process_id": record.process_id,
        "batch_id": record.batch_id,
        "resource_id": record.resource_id,
        "paper_id": record.paper_id,
        "stage": record.stage,
        "status": record.status,
        "failure_stage": record.failure_stage,
        "error_message": record.error_message,
        "retryable": record.retryable,
        "retry_count": record.retry_count,
        "last_retry_at": record.last_retry_at,
        "operator": record.operator,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
    }


def _json_array(values: list[str]) -> str:
    return json.dumps(values, ensure_ascii=False)


def _json_array_object(values: list[dict[str, Any]]) -> str:
    return json.dumps(values, ensure_ascii=False)


def _json_object(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False)


def _safe_close(connection: Any) -> None:
    try:
        connection.close()
    except Exception:
        pass
