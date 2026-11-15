"""PostgreSQL + pgvector schema builder for vector retrieval.

This module only builds SQL text. It does not connect to PostgreSQL, import
database drivers, execute DDL, or read environment variables.
"""

from __future__ import annotations


VECTOR_SCHEMA_VERSION = "vector_retrieval.v1"


def iter_vector_schema_statements(dimension: int = 1536) -> tuple[str, ...]:
    if dimension <= 0:
        raise ValueError("dimension must be greater than 0")
    return (
        "CREATE EXTENSION IF NOT EXISTS vector;",
        _paper_chunks_sql(),
        _paper_chunk_embeddings_sql(dimension),
        _retrieval_records_sql(),
        _embedding_cost_records_sql(),
        _vector_process_records_sql(),
        "CREATE INDEX IF NOT EXISTS idx_paper_chunks_resource_id ON paper_chunks(resource_id);",
        "CREATE INDEX IF NOT EXISTS idx_paper_chunks_content_role ON paper_chunks(content_role);",
        "CREATE INDEX IF NOT EXISTS idx_paper_chunk_embeddings_chunk_id ON paper_chunk_embeddings(chunk_id);",
        "CREATE INDEX IF NOT EXISTS idx_retrieval_records_created_at ON retrieval_records(created_at);",
    )


def build_vector_schema_sql(dimension: int = 1536) -> str:
    return "\n\n".join(iter_vector_schema_statements(dimension)) + "\n"


def _paper_chunks_sql() -> str:
    return """
CREATE TABLE IF NOT EXISTS paper_chunks (
    chunk_id TEXT PRIMARY KEY,
    resource_id UUID NOT NULL REFERENCES paper_resources(id),
    paper_id TEXT NOT NULL,
    title TEXT NOT NULL,
    major TEXT NOT NULL,
    discipline TEXT NOT NULL,
    category TEXT NOT NULL,
    direction TEXT NOT NULL,
    thesis_type TEXT NOT NULL,
    tier TEXT NOT NULL,
    resource_status TEXT NOT NULL,
    chapter_title TEXT NOT NULL,
    chapter_role TEXT NOT NULL,
    paragraph_order INTEGER NOT NULL,
    chunk_order INTEGER NOT NULL,
    chunk_text TEXT NOT NULL,
    chunk_hash TEXT NOT NULL,
    text_hash TEXT NOT NULL,
    content_role TEXT NOT NULL,
    needs_real_data BOOLEAN NOT NULL,
    research_method_tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    source_hash TEXT NOT NULL,
    chunk_rule_version TEXT NOT NULL,
    chunk_status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
""".strip()


def _paper_chunk_embeddings_sql(dimension: int) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS paper_chunk_embeddings (
    embedding_id TEXT PRIMARY KEY,
    chunk_id TEXT NOT NULL REFERENCES paper_chunks(chunk_id),
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    dimension INTEGER NOT NULL,
    embedding_vector vector({dimension}) NOT NULL,
    input_text_hash TEXT NOT NULL,
    embedding_status TEXT NOT NULL,
    error_message TEXT NOT NULL DEFAULT '',
    estimated_tokens INTEGER NOT NULL DEFAULT 0,
    estimated_cost NUMERIC NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE(chunk_id, provider, model)
);
""".strip()


def _retrieval_records_sql() -> str:
    return """
CREATE TABLE IF NOT EXISTS retrieval_records (
    retrieval_id TEXT PRIMARY KEY,
    query_title TEXT NOT NULL,
    major TEXT NOT NULL,
    direction TEXT NOT NULL,
    thesis_type TEXT NOT NULL,
    research_method_preference TEXT NOT NULL DEFAULT '',
    tier_preference TEXT NOT NULL DEFAULT '',
    sql_filters JSONB NOT NULL,
    hit_chunks JSONB NOT NULL,
    structured_material_package JSONB NOT NULL,
    material_package_preview TEXT NOT NULL,
    embedding_config_version TEXT NOT NULL,
    retrieval_config_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
""".strip()


def _embedding_cost_records_sql() -> str:
    return """
CREATE TABLE IF NOT EXISTS embedding_cost_records (
    cost_record_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    paper_count INTEGER NOT NULL,
    chunk_count INTEGER NOT NULL,
    input_char_count INTEGER NOT NULL,
    estimated_tokens INTEGER NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    success_count INTEGER NOT NULL,
    failed_count INTEGER NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    estimated_cost NUMERIC NOT NULL,
    actual_cost NUMERIC NOT NULL DEFAULT 0,
    operator TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
""".strip()


def _vector_process_records_sql() -> str:
    return """
CREATE TABLE IF NOT EXISTS vector_process_records (
    process_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    resource_id UUID NULL REFERENCES paper_resources(id),
    paper_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    status TEXT NOT NULL,
    failure_stage TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    retryable BOOLEAN NOT NULL DEFAULT false,
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_retry_at TIMESTAMPTZ NULL,
    operator TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
""".strip()

