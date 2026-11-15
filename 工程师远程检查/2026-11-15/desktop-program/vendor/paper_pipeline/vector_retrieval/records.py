"""Retrieval record helpers."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from paper_pipeline.vector_retrieval.models import (
    CandidateChunk,
    RetrievalRecord,
    RetrievalRequest,
    StructuredMaterialPackage,
)


class RetrievalRecordService:
    def build_record(
        self,
        request: RetrievalRequest,
        sql_filters: dict[str, Any],
        candidates: list[CandidateChunk],
        package: StructuredMaterialPackage,
        preview: str,
    ) -> RetrievalRecord:
        return RetrievalRecord(
            retrieval_id=request.retrieval_id,
            query_title=request.query_title,
            major=request.major,
            direction=request.direction,
            thesis_type=request.thesis_type,
            sql_filters=sql_filters,
            hit_chunks=[
                {
                    "chunk_id": candidate.chunk.chunk_id,
                    "similarity_score": candidate.similarity_score,
                    "weighted_score": candidate.weighted_score,
                }
                for candidate in candidates
            ],
            structured_material_package=asdict(package),
            material_package_preview=preview,
            embedding_config_version=package.embedding_config_version,
            retrieval_config_version=request.retrieval_config_version,
        )

