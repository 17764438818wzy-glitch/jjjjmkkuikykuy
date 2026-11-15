"""Resource admission and reading services."""

from __future__ import annotations

from paper_pipeline.vector_retrieval.models import FormalResource
from paper_pipeline.vector_retrieval.protocols import ResourceRepository


class ResourceAdmissionService:
    def __init__(self, repository: ResourceRepository) -> None:
        self.repository = repository

    def list_eligible_resources(self) -> list[FormalResource]:
        return self.repository.list_vector_eligible_resources()

    def filter_eligible_resources(
        self,
        resources: list[FormalResource],
    ) -> list[FormalResource]:
        return [resource for resource in resources if resource.is_vector_eligible()]

