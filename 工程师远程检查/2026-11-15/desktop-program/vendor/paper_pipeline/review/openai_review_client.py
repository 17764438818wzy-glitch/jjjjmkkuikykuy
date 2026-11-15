"""OpenAI-backed review client facade.

The concrete network call is intentionally outside the test path. This facade
keeps configuration checks local so the service can fail closed when no key is
available.
"""

from __future__ import annotations

from dataclasses import dataclass

from paper_pipeline.review.ai_review_client import AIReviewConfig, AIReviewInput


@dataclass(slots=True)
class OpenAIReviewClient:
    config: AIReviewConfig

    @property
    def is_configured(self) -> bool:
        return bool(self.config.enabled and self.config.api_key.strip())

    def review(self, request: AIReviewInput) -> str:
        if not self.is_configured:
            raise RuntimeError("OpenAI review client is not configured")
        raise NotImplementedError("OpenAI review network call is not implemented")
