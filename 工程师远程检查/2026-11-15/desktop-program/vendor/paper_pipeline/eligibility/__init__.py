"""Eligibility core for formal resource admission."""

from paper_pipeline.eligibility.formal_candidate import (
    check_formal_candidate,
    load_raw_metadata_dict,
    metadata_has_explicit_field,
)
from paper_pipeline.eligibility.models import EligibilityResult, EligibilityScope

__all__ = [
    "EligibilityResult",
    "EligibilityScope",
    "check_formal_candidate",
    "load_raw_metadata_dict",
    "metadata_has_explicit_field",
]
