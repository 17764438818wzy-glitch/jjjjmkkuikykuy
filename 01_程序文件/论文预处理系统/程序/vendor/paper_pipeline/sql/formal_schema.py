"""Formal PostgreSQL schema DDL builder.

This module only builds SQL text. It does not connect to PostgreSQL, execute
DDL, read environment variables, or import database drivers.
"""

from __future__ import annotations


FORMAL_SCHEMA_VERSION = "1.0"

FORMAL_TABLE_NAMES = (
    "paper_resources",
    "paper_upload_batches",
    "paper_metadata_index",
    "paper_text_index",
    "paper_chapter_index",
    "paper_hierarchy_index",
    "paper_index_manifest",
)


_TABLE_STATEMENTS = (
    """
CREATE TABLE IF NOT EXISTS paper_resources (
    id UUID PRIMARY KEY,
    schema_version TEXT,
    paper_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    title TEXT,
    sanitized_title TEXT,
    abstract TEXT,
    abstract_source TEXT,
    keywords JSONB,
    keywords_source TEXT,
    category TEXT,
    direction TEXT,
    major TEXT,
    discipline TEXT,
    thesis_type TEXT,
    research_methods JSONB,
    tier TEXT,
    quality_tier TEXT,
    quality_reason TEXT,
    sensitive_clean BOOLEAN,
    version_status TEXT,
    duplicate_status TEXT,
    index_status TEXT,
    sql_status TEXT,
    resource_status TEXT,
    source_path TEXT,
    markdown_path TEXT,
    metadata_path TEXT,
    storage_key TEXT NULL,
    object_url TEXT NULL,
    source_hash TEXT,
    markdown_hash TEXT,
    text_hash TEXT,
    blocks_path TEXT NULL,
    blocks_hash TEXT NULL,
    block_count INTEGER NULL,
    parser_selected TEXT NULL,
    blocks_schema_version TEXT NULL,
    desensitized_at TIMESTAMPTZ NULL,
    markdown_content TEXT,
    metadata_enriched BOOLEAN,
    metadata_enriched_at TIMESTAMPTZ NULL,
    metadata_enriched_by TEXT NULL,
    llm_model TEXT NULL,
    uploaded_at TIMESTAMPTZ,
    uploaded_by TEXT,
    withdrawn_at TIMESTAMPTZ NULL,
    withdrawn_by TEXT NULL,
    withdrawn_reason TEXT NULL,
    replaced_by_paper_id TEXT NULL,
    replaced_by_version INTEGER NULL,
    replaces_paper_id TEXT NULL,
    replaces_version INTEGER NULL,
    replacement_reason TEXT NULL,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    UNIQUE(paper_id, version),
    CHECK (resource_status IN ('active', 'withdrawn', 'replaced')),
    CHECK (tier IN ('gold', 'silver'))
);
""".strip(),
    """
CREATE TABLE IF NOT EXISTS paper_upload_batches (
    id UUID PRIMARY KEY,
    schema_version TEXT,
    status TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    total_count INTEGER,
    success_count INTEGER,
    failed_count INTEGER,
    skipped_count INTEGER,
    confirmed_by TEXT,
    confirmed_at TIMESTAMPTZ,
    uploaded_by TEXT,
    notes TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    CHECK (status IN ('completed', 'partial_failed', 'failed'))
);
""".strip(),
    """
CREATE TABLE IF NOT EXISTS paper_metadata_index (
    id UUID PRIMARY KEY,
    resource_id UUID REFERENCES paper_resources(id),
    schema_version TEXT,
    paper_id TEXT,
    version INTEGER,
    title TEXT,
    sanitized_title TEXT,
    category TEXT,
    direction TEXT,
    major TEXT,
    discipline TEXT,
    thesis_type TEXT,
    research_methods JSONB,
    keywords JSONB,
    abstract TEXT,
    tier TEXT,
    quality_tier TEXT,
    quality_reason TEXT,
    sensitive_clean BOOLEAN,
    version_status TEXT,
    duplicate_status TEXT,
    index_status TEXT,
    sql_status TEXT,
    resource_status TEXT,
    source_hash TEXT,
    markdown_hash TEXT,
    text_hash TEXT,
    markdown_path TEXT,
    metadata_path TEXT,
    index_scope TEXT,
    warnings JSONB,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);
""".strip(),
    """
CREATE TABLE IF NOT EXISTS paper_text_index (
    id UUID PRIMARY KEY,
    resource_id UUID REFERENCES paper_resources(id),
    schema_version TEXT,
    paper_id TEXT,
    version INTEGER,
    title TEXT,
    abstract TEXT,
    keywords JSONB,
    category TEXT,
    direction TEXT,
    tier TEXT,
    retrieval_text TEXT,
    text_hash TEXT,
    search_vector TSVECTOR NULL,
    resource_status TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);
""".strip(),
    """
CREATE TABLE IF NOT EXISTS paper_chapter_index (
    id UUID PRIMARY KEY,
    resource_id UUID REFERENCES paper_resources(id),
    schema_version TEXT,
    paper_id TEXT,
    version INTEGER,
    chapter_id TEXT,
    chapter_order INTEGER,
    chapter_title TEXT,
    chapter_role TEXT,
    heading_path JSONB,
    text_hash TEXT,
    chapter_role_source TEXT,
    llm_model TEXT NULL,
    index_scope TEXT,
    resource_status TEXT,
    warnings JSONB,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    UNIQUE(resource_id, chapter_id)
);
""".strip(),
    """
CREATE TABLE IF NOT EXISTS paper_hierarchy_index (
    id UUID PRIMARY KEY,
    resource_id UUID REFERENCES paper_resources(id),
    schema_version TEXT,
    paper_id TEXT,
    version INTEGER,
    hierarchy_type TEXT,
    chapter_ids JSONB,
    chapter_order JSONB,
    relations JSONB,
    resource_status TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);
""".strip(),
    """
CREATE TABLE IF NOT EXISTS paper_index_manifest (
    id UUID PRIMARY KEY,
    resource_id UUID REFERENCES paper_resources(id),
    upload_batch_id UUID REFERENCES paper_upload_batches(id),
    schema_version TEXT,
    paper_id TEXT,
    version INTEGER,
    local_index_run_id TEXT,
    local_index_package_path TEXT,
    source_hash TEXT,
    markdown_hash TEXT,
    text_hash TEXT,
    blocks_hash TEXT NULL,
    index_built_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    published_by TEXT,
    warnings JSONB,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);
""".strip(),
)

_INDEX_STATEMENTS = (
    "CREATE INDEX IF NOT EXISTS idx_paper_resources_source_hash ON paper_resources(source_hash);",
    "CREATE INDEX IF NOT EXISTS idx_paper_resources_resource_status ON paper_resources(resource_status);",
    "CREATE INDEX IF NOT EXISTS idx_paper_metadata_index_resource_id ON paper_metadata_index(resource_id);",
    "CREATE INDEX IF NOT EXISTS idx_paper_text_index_resource_id ON paper_text_index(resource_id);",
    "CREATE INDEX IF NOT EXISTS idx_paper_chapter_index_resource_id ON paper_chapter_index(resource_id);",
    "CREATE INDEX IF NOT EXISTS idx_paper_chapter_index_resource_order ON paper_chapter_index(resource_id, chapter_order);",
    "CREATE INDEX IF NOT EXISTS idx_paper_hierarchy_index_resource_id ON paper_hierarchy_index(resource_id);",
    "CREATE INDEX IF NOT EXISTS idx_paper_index_manifest_resource_id ON paper_index_manifest(resource_id);",
    "CREATE INDEX IF NOT EXISTS idx_paper_index_manifest_upload_batch_id ON paper_index_manifest(upload_batch_id);",
)


def iter_formal_schema_statements() -> tuple[str, ...]:
    """Return the formal schema DDL statements in dependency order."""

    return (*_TABLE_STATEMENTS, *_INDEX_STATEMENTS)


def build_formal_schema_sql() -> str:
    """Return PostgreSQL DDL for the formal seven-table schema."""

    return "\n\n".join(iter_formal_schema_statements()) + "\n"
