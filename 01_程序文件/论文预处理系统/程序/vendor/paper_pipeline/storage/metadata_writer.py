"""Metadata validation and JSON writing for formal archive records."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from paper_pipeline.domain import PaperMetadata, Tier
from paper_pipeline.domain.serialization import metadata_to_dict
from paper_pipeline.taxonomy.categories import validate_category
from paper_pipeline.taxonomy.directions import validate_direction
from paper_pipeline.taxonomy.methods import (
    RESEARCH_METHOD_TAGS,
    validate_research_method_primary,
)


@dataclass(slots=True)
class MetadataWriter:
    def write_metadata(self, metadata: PaperMetadata, path: str | Path) -> Path:
        validate_formal_metadata(metadata)
        destination = Path(path)
        if destination.name != "metadata.json":
            raise ValueError("formal metadata filename must be metadata.json")
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            json.dumps(
                metadata_to_dict(metadata),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        return destination


def validate_formal_metadata(metadata: PaperMetadata) -> None:
    tier = _coerce_tier(metadata.tier)
    if tier is Tier.STAGE0_REJECTED:
        raise ValueError("stage0_rejected is not formal metadata")

    _validate_category_direction_for_tier(metadata)
    _validate_research_method(metadata)


def _validate_category_direction_for_tier(metadata: PaperMetadata) -> None:
    tier = _coerce_tier(metadata.tier)
    category = metadata.category.strip()
    direction = metadata.direction.strip()

    if tier in {Tier.GOLD, Tier.SILVER, Tier.REJECTED}:
        category_result = validate_category(category)
        if not category_result.is_valid:
            raise ValueError("category must come from the closed category taxonomy")
        direction_result = validate_direction(category, direction)
        if not direction_result.is_valid:
            raise ValueError("direction must come from the closed direction taxonomy")
        return

    if category and not validate_category(category).is_valid:
        raise ValueError("category must come from the closed category taxonomy")
    if direction and not validate_direction(category, direction).is_valid:
        raise ValueError("direction must come from the closed direction taxonomy")


def _validate_research_method(metadata: PaperMetadata) -> None:
    primary = metadata.research_method_primary
    if primary:
        primary_result = validate_research_method_primary(primary)
        if not primary_result.is_valid:
            raise ValueError("research_method_primary must come from the fixed list")

    invalid_tags = [
        tag for tag in metadata.research_method_tags if tag not in RESEARCH_METHOD_TAGS
    ]
    if invalid_tags:
        raise ValueError("research_method_tags must come from the fixed tag list")


def _coerce_tier(tier: Tier | str) -> Tier:
    if isinstance(tier, Tier):
        return tier
    return Tier(tier)
