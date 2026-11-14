"""Data models for the vector retrieval material package MVP."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


ELIGIBLE_TIERS = {"gold", "silver"}
ELIGIBLE_RESOURCE_STATUS = "active"
ELIGIBLE_VERSION_STATUS = "current"
ELIGIBLE_INDEX_STATUS = "indexed"
ELIGIBLE_SQL_STATUS = "uploaded"

CONTENT_ROLES = {
    "background",
    "theory",
    "method",
    "data_result",
    "problem_analysis",
    "strategy",
    "conclusion",
    "other",
}

SPORTS_MAJORS = {"体育教育", "体育健康与休闲运动", "体育学"}
SPORTS_CATEGORIES = {"体育教育", "体育健康与休闲运动"}
SPORTS_DIRECTIONS = {
    "体育实验",
    "训练干预",
    "教学干预",
    "测试指标分析",
    "运动表现影响研究",
}
SPORTS_LABEL_KEYWORDS = ("体育", "运动", "健身", "训练", "体能")


def now_utc() -> datetime:
    return datetime.now(UTC)


@dataclass(slots=True)
class ChapterInfo:
    chapter_id: str = ""
    chapter_title: str = ""
    chapter_role: str = ""
    chapter_order: int = 0
    heading_path: list[str] = field(default_factory=list)
    text: str = ""
    text_hash: str = ""


@dataclass(slots=True)
class FormalResource:
    resource_id: str
    paper_id: str
    title: str
    major: str
    discipline: str
    category: str
    direction: str
    thesis_type: str
    tier: str
    resource_status: str
    version_status: str
    sensitive_clean: bool
    index_status: str
    sql_status: str
    markdown_content: str
    source_hash: str
    text_hash: str
    chapters: list[ChapterInfo] = field(default_factory=list)
    research_method_tags: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=now_utc)
    updated_at: datetime = field(default_factory=now_utc)

    def is_vector_eligible(self) -> bool:
        return (
            self.tier in ELIGIBLE_TIERS
            and self.resource_status == ELIGIBLE_RESOURCE_STATUS
            and self.version_status == ELIGIBLE_VERSION_STATUS
            and self.sensitive_clean
            and self.index_status == ELIGIBLE_INDEX_STATUS
            and self.sql_status == ELIGIBLE_SQL_STATUS
            and bool(self.markdown_content.strip())
        )

    def is_sports_pilot_candidate(self) -> bool:
        if not self.is_vector_eligible():
            return False
        return (
            self.major in SPORTS_MAJORS
            or self.discipline in SPORTS_MAJORS
            or self.category in SPORTS_CATEGORIES
            or self.direction in SPORTS_DIRECTIONS
            or _matches_sports_label(self.category, SPORTS_CATEGORIES)
            or _matches_sports_label(self.direction, SPORTS_DIRECTIONS)
        )


def _matches_sports_label(value: str, exact_values: set[str]) -> bool:
    label = value.strip()
    if not label:
        return False
    normalized = label.removesuffix("类")
    return normalized in exact_values or any(keyword in label for keyword in SPORTS_LABEL_KEYWORDS)


@dataclass(slots=True)
class PaperChunk:
    chunk_id: str
    resource_id: str
    paper_id: str
    title: str
    major: str
    discipline: str
    category: str
    direction: str
    thesis_type: str
    tier: str
    resource_status: str
    chapter_title: str
    chapter_role: str
    paragraph_order: int
    chunk_order: int
    chunk_text: str
    chunk_hash: str
    text_hash: str
    content_role: str
    needs_real_data: bool
    research_method_tags: list[str]
    source_hash: str
    chunk_rule_version: str = "chunk.v1"
    chunk_status: str = "active"
    created_at: datetime = field(default_factory=now_utc)
    updated_at: datetime = field(default_factory=now_utc)


@dataclass(slots=True)
class ChunkEmbedding:
    embedding_id: str
    chunk_id: str
    provider: str
    model: str
    dimension: int
    embedding_vector: list[float]
    input_text_hash: str
    embedding_status: str = "embedded"
    error_message: str = ""
    estimated_tokens: int = 0
    estimated_cost: float = 0.0
    created_at: datetime = field(default_factory=now_utc)
    updated_at: datetime = field(default_factory=now_utc)


@dataclass(slots=True)
class RetrievalRequestInput:
    query_title: str
    major: str
    direction: str = ""
    thesis_type: str = ""
    research_method_preference: str = ""
    tier_preference: str = ""


@dataclass(slots=True)
class RetrievalRequest:
    retrieval_id: str
    query_title: str
    major: str
    direction: str
    thesis_type: str
    research_method_preference: str
    tier_preference: str
    query_text: str
    retrieval_config_version: str = "retrieval.v1"


@dataclass(slots=True)
class SqlFilter:
    major: str = ""
    direction: str = ""
    thesis_type: str = ""
    tier_preference: str = ""
    research_method_preference: str = ""
    content_roles: set[str] = field(default_factory=set)
    needs_real_data: bool | None = None


@dataclass(slots=True)
class CandidateChunk:
    chunk: PaperChunk
    similarity_score: float
    weighted_score: float = 0.0
    source_rank: int = 0
    role_group: str = ""


@dataclass(slots=True)
class ReviewRequest:
    retrieval_request: RetrievalRequest
    candidates: list[CandidateChunk]


@dataclass(slots=True)
class ReviewedMaterial:
    chunk_id: str
    keep: bool
    summary: str
    content_role: str
    relevance_label: str
    needs_real_data_check: bool
    review_reason: str
    similarity_score: float


@dataclass(slots=True)
class TraceabilityInfo:
    resource_ids: list[str] = field(default_factory=list)
    paper_ids: list[str] = field(default_factory=list)
    chunk_ids: list[str] = field(default_factory=list)


@dataclass(slots=True)
class MaterialItem:
    section: str
    chunk_id: str
    source_paper: str
    source_chapter: str
    content_role: str
    relevance_label: str
    summary: str
    original_chunk: str
    needs_real_data_check: bool
    traceability: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class StructuredMaterialPackage:
    query: dict[str, str]
    package_type: str
    materials: list[MaterialItem]
    missing_real_data: list[str]
    traceability: TraceabilityInfo
    retrieval_config_version: str = "retrieval.v1"
    embedding_config_version: str = "embedding.fake.v1"


@dataclass(slots=True)
class VectorizationResult:
    batch_id: str
    paper_count: int
    chunk_count: int
    success_count: int
    failed_count: int
    failures: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RetrievalRecord:
    retrieval_id: str
    query_title: str
    major: str
    direction: str
    thesis_type: str
    sql_filters: dict[str, Any]
    hit_chunks: list[dict[str, Any]]
    structured_material_package: dict[str, Any]
    material_package_preview: str
    embedding_config_version: str
    retrieval_config_version: str
    created_at: datetime = field(default_factory=now_utc)


@dataclass(slots=True)
class EmbeddingCostRecord:
    cost_record_id: str
    batch_id: str
    paper_count: int
    chunk_count: int
    input_char_count: int
    estimated_tokens: int
    provider: str
    model: str
    success_count: int
    failed_count: int
    started_at: datetime
    completed_at: datetime
    estimated_cost: float
    actual_cost: float = 0.0
    operator: str = "codex_control"
    created_at: datetime = field(default_factory=now_utc)


@dataclass(slots=True)
class VectorProcessRecord:
    process_id: str
    batch_id: str
    resource_id: str
    paper_id: str
    stage: str
    status: str
    failure_stage: str = ""
    error_message: str = ""
    retryable: bool = False
    retry_count: int = 0
    last_retry_at: datetime | None = None
    operator: str = "codex_control"
    created_at: datetime = field(default_factory=now_utc)
    updated_at: datetime = field(default_factory=now_utc)


@dataclass(slots=True)
class PilotEvaluation:
    topic_results: list[dict[str, object]]
