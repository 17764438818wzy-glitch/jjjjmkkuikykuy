"""High-level service orchestration for the vector retrieval MVP."""

from __future__ import annotations

from paper_pipeline.vector_retrieval.candidate_processing import CandidateProcessor
from paper_pipeline.vector_retrieval.chunking import ChunkingService
from paper_pipeline.vector_retrieval.costs import CostRecordService
from paper_pipeline.vector_retrieval.embeddings import EmbeddingService
from paper_pipeline.vector_retrieval.filters import SqlFilterBuilder
from paper_pipeline.vector_retrieval.models import (
    RetrievalRequestInput,
    StructuredMaterialPackage,
    VectorizationResult,
)
from paper_pipeline.vector_retrieval.packages import MaterialPackageBuilder
from paper_pipeline.vector_retrieval.protocols import (
    EmbeddingProvider,
    RecordRepository,
    ResourceRepository,
    VectorRepository,
)
from paper_pipeline.vector_retrieval.recall import SemanticRecallService
from paper_pipeline.vector_retrieval.records import RetrievalRecordService
from paper_pipeline.vector_retrieval.requests import RetrievalRequestParser
from paper_pipeline.vector_retrieval.review import LightweightReviewService
from paper_pipeline.vector_retrieval.protocols import LightweightReviewProvider
from paper_pipeline.vector_retrieval.resources import ResourceAdmissionService


class VectorRetrievalMvpService:
    def __init__(
        self,
        *,
        resources: ResourceRepository,
        vectors: VectorRepository,
        records: RecordRepository,
        embedding_provider: EmbeddingProvider,
        review_provider: LightweightReviewProvider,
    ) -> None:
        self.resources = resources
        self.vectors = vectors
        self.records = records
        self.embedding_provider = embedding_provider
        self.resource_admission = ResourceAdmissionService(resources)
        self.chunking = ChunkingService()
        self.embedding = EmbeddingService(embedding_provider)
        self.request_parser = RetrievalRequestParser()
        self.filter_builder = SqlFilterBuilder()
        self.recall = SemanticRecallService(vectors, embedding_provider)
        self.candidates = CandidateProcessor()
        self.review = LightweightReviewService(review_provider)
        self.package_builder = MaterialPackageBuilder()
        self.retrieval_records = RetrievalRecordService()
        self.costs = CostRecordService()

    def vectorize_eligible_resources(self, *, batch_id: str) -> VectorizationResult:
        resources = self.resource_admission.list_eligible_resources()
        all_chunks = []
        failures: list[str] = []
        for resource in resources:
            try:
                all_chunks.extend(self.chunking.chunk_resource(resource))
            except Exception as exc:  # noqa: BLE001 - converted to batch failure.
                failures.append(f"{resource.resource_id}: {exc}")
        embeddings = self.embedding.embed_chunks(all_chunks)
        self.vectors.save_chunks(all_chunks)
        self.vectors.save_embeddings(embeddings)
        cost_record = self.costs.build_embedding_cost_record(
            batch_id=batch_id,
            paper_count=len(resources),
            chunk_count=len(all_chunks),
            input_char_count=sum(len(chunk.chunk_text) for chunk in all_chunks),
            provider=self.embedding_provider,
            success_count=len(resources) - len(failures),
            failed_count=len(failures),
        )
        self.records.save_cost_record(cost_record)
        return VectorizationResult(
            batch_id=batch_id,
            paper_count=len(resources),
            chunk_count=len(all_chunks),
            success_count=len(resources) - len(failures),
            failed_count=len(failures),
            failures=failures,
        )

    def retrieve_material_package(
        self,
        raw_request: RetrievalRequestInput,
    ) -> StructuredMaterialPackage:
        request = self.request_parser.parse(raw_request)
        filters = self.filter_builder.build(request)
        recalled = self.recall.recall(request, filters, top_k=120)
        processed = self.candidates.process(recalled, request=request)
        reviews = self.review.review(request, processed)
        package = self.package_builder.build(request, processed, reviews)
        preview = self.package_builder.to_markdown_preview(package)
        record = self.retrieval_records.build_record(
            request,
            self.filter_builder.to_record(filters),
            processed,
            package,
            preview,
        )
        self.records.save_retrieval_record(record)
        return package
