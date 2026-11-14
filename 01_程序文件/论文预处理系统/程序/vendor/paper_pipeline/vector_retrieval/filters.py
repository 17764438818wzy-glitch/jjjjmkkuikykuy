"""SQL-like filter construction used before semantic recall."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from paper_pipeline.vector_retrieval.models import RetrievalRequest, SqlFilter


class SqlFilterBuilder:
    def build(self, request: RetrievalRequest) -> SqlFilter:
        return SqlFilter(
            major=request.major,
            direction=request.direction,
            thesis_type=request.thesis_type,
            tier_preference=request.tier_preference,
            research_method_preference=request.research_method_preference,
        )

    def to_record(self, filters: SqlFilter) -> dict[str, Any]:
        data = asdict(filters)
        data["content_roles"] = sorted(filters.content_roles)
        return data

