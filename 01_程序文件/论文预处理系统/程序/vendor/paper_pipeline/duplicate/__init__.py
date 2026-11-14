"""Duplicate, near-duplicate, and version decision helpers for task 10."""

from paper_pipeline.duplicate.detector import (
    DEFAULT_SUSPECTED_DUPLICATE_THRESHOLD,
    DuplicateCandidate,
    DuplicateDetectionResult,
    apply_duplicate_detection_to_metadata,
    detect_duplicate,
)
from paper_pipeline.duplicate.similarity import SimilarityScore, calculate_similarity
from paper_pipeline.duplicate.versioning import (
    VersionRelationship,
    determine_version_relationship,
)

__all__ = [
    "DEFAULT_SUSPECTED_DUPLICATE_THRESHOLD",
    "DuplicateCandidate",
    "DuplicateDetectionResult",
    "SimilarityScore",
    "VersionRelationship",
    "apply_duplicate_detection_to_metadata",
    "calculate_similarity",
    "detect_duplicate",
    "determine_version_relationship",
]
