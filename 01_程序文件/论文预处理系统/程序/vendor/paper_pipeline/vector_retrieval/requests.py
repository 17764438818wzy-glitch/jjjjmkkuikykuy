"""Retrieval request parsing."""

from __future__ import annotations

from hashlib import sha1

from paper_pipeline.vector_retrieval.models import RetrievalRequest, RetrievalRequestInput


class RetrievalRequestParser:
    def parse(self, raw: RetrievalRequestInput) -> RetrievalRequest:
        title = raw.query_title.strip()
        major = raw.major.strip()
        direction = raw.direction.strip()
        if not title:
            raise ValueError("query_title is required")
        if not major and not direction:
            raise ValueError("major or direction is required")
        query_text = " ".join(
            item
            for item in (
                title,
                major,
                direction,
                raw.thesis_type.strip(),
                raw.research_method_preference.strip(),
            )
            if item
        )
        return RetrievalRequest(
            retrieval_id=_stable_id(query_text),
            query_title=title,
            major=major,
            direction=direction,
            thesis_type=raw.thesis_type.strip(),
            research_method_preference=raw.research_method_preference.strip(),
            tier_preference=raw.tier_preference.strip(),
            query_text=query_text,
        )


def _stable_id(value: str) -> str:
    return sha1(value.encode("utf-8")).hexdigest()[:16]

