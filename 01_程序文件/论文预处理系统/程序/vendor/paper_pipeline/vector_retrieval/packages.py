"""Material package construction."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from paper_pipeline.vector_retrieval.general_package import GeneralPackageAdapter
from paper_pipeline.vector_retrieval.models import (
    CandidateChunk,
    MaterialItem,
    RetrievalRequest,
    ReviewedMaterial,
    StructuredMaterialPackage,
)
from paper_pipeline.vector_retrieval.real_data import MissingRealDataService
from paper_pipeline.vector_retrieval.sports_adapter import SportsTrainingAdapter
from paper_pipeline.vector_retrieval.traceability import TraceabilityService


class MaterialPackageBuilder:
    def __init__(
        self,
        traceability: TraceabilityService | None = None,
        missing_data: MissingRealDataService | None = None,
    ) -> None:
        self.general = GeneralPackageAdapter()
        self.sports = SportsTrainingAdapter()
        self.traceability = traceability or TraceabilityService()
        self.missing_data = missing_data or MissingRealDataService()

    def build(
        self,
        request: RetrievalRequest,
        candidates: list[CandidateChunk],
        reviews: list[ReviewedMaterial],
    ) -> StructuredMaterialPackage:
        chunks_by_id = {candidate.chunk.chunk_id: candidate.chunk for candidate in candidates}
        package_type = (
            self.sports.package_type
            if self.sports.applies_to(request.major, request.direction, request.query_title)
            else "general"
        )
        materials: list[MaterialItem] = []
        for review in reviews:
            if not review.keep:
                continue
            chunk = chunks_by_id.get(review.chunk_id)
            if chunk is None:
                continue
            section = (
                self.sports.section_for(review.content_role, chunk.chunk_text)
                if package_type == self.sports.package_type
                else self.general.section_for_role(review.content_role)
            )
            materials.append(
                MaterialItem(
                    section=section,
                    chunk_id=chunk.chunk_id,
                    source_paper=chunk.title,
                    source_chapter=chunk.chapter_title,
                    content_role=review.content_role,
                    relevance_label=review.relevance_label,
                    summary=review.summary,
                    original_chunk=chunk.chunk_text,
                    needs_real_data_check=review.needs_real_data_check,
                    traceability=self.traceability.item_traceability(chunk),
                ),
            )
        limited = materials[:30]
        missing = self.missing_data.missing_items(
            request,
            limited,
            package_type=package_type,
        )
        chunks = [chunks_by_id[item.chunk_id] for item in limited if item.chunk_id in chunks_by_id]
        return StructuredMaterialPackage(
            query={
                "title": request.query_title,
                "major": request.major,
                "direction": request.direction,
                "thesis_type": request.thesis_type,
            },
            package_type=package_type,
            materials=limited,
            missing_real_data=missing,
            traceability=self.traceability.build_traceability(chunks),
            retrieval_config_version=request.retrieval_config_version,
        )

    def to_record_payload(self, package: StructuredMaterialPackage) -> dict[str, Any]:
        return asdict(package)

    def to_markdown_preview(self, package: StructuredMaterialPackage) -> str:
        lines = [
            f"# {package.query['title']}",
            "",
            f"package_type: {package.package_type}",
            "",
        ]
        for item in package.materials:
            lines.extend(
                [
                    f"## {item.section}",
                    f"- 摘要：{item.summary}",
                    f"- 来源：{item.source_paper} / {item.source_chapter}",
                    f"- chunk_id：{item.chunk_id}",
                    "",
                ],
            )
        if package.missing_real_data:
            lines.append("## 缺失真实数据提醒")
            lines.extend(f"- {item}" for item in package.missing_real_data)
        return "\n".join(lines)

