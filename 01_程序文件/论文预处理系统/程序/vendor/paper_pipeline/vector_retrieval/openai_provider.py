"""OpenAI provider facades for vector retrieval.

Network calls are intentionally outside the default test path. These facades
keep configuration checks local so production wiring can fail closed.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from paper_pipeline.vector_retrieval.models import (
    ReviewRequest,
    ReviewedMaterial,
)


OpenAITransport = Callable[
    [str, dict[str, object], Mapping[str, str], int],
    dict[str, object],
]


@dataclass(frozen=True, slots=True)
class OpenAIEmbeddingConfig:
    api_key: str = ""
    base_url: str = "https://api.openai.com/v1"
    model: str = "text-embedding-3-small"
    dimension: int = 1536
    timeout_seconds: int = 60
    batch_size: int = 64
    max_retries: int = 3

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> OpenAIEmbeddingConfig:
        source = environ if environ is not None else os.environ
        return cls(
            api_key=str(source.get("VECTOR_OPENAI_API_KEY", "")).strip()
            or str(source.get("OPENAI_API_KEY", "")).strip(),
            base_url=str(source.get("VECTOR_OPENAI_BASE_URL", "")).strip()
            or str(source.get("OPENAI_BASE_URL", "")).strip()
            or "https://api.openai.com/v1",
            model=str(source.get("VECTOR_EMBEDDING_MODEL", "")).strip()
            or "text-embedding-3-small",
            dimension=_int_from_env(source, "VECTOR_EMBEDDING_DIMENSION", 1536),
            timeout_seconds=_int_from_env(source, "OPENAI_TIMEOUT_SECONDS", 60),
            batch_size=max(1, _int_from_env(source, "VECTOR_EMBEDDING_BATCH_SIZE", 64)),
            max_retries=max(1, _int_from_env(source, "VECTOR_OPENAI_MAX_RETRIES", 3)),
        )


@dataclass(slots=True)
class OpenAIEmbeddingProvider:
    config: OpenAIEmbeddingConfig
    transport: OpenAITransport | None = None
    provider_name: str = "openai"

    @property
    def model_name(self) -> str:
        return self.config.model

    @property
    def dimension(self) -> int:
        return self.config.dimension

    @property
    def is_configured(self) -> bool:
        return bool(self.config.api_key.strip())

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not self.is_configured:
            raise RuntimeError("OpenAI embedding provider is not configured")
        if not texts:
            return []
        vectors: list[list[float]] = []
        for batch in _batches(texts, self.config.batch_size):
            response = _embedding_batch_response(
                transport=self.transport or _urllib_transport,
                config=self.config,
                batch=batch,
            )
            vectors.extend(_embedding_vectors(response))
        return vectors

    def estimate_tokens(self, texts: list[str]) -> int:
        return sum(max(1, len(text) // 2) for text in texts)

    def estimate_cost(self, token_count: int) -> float:
        return round(token_count * 0.00000002, 8)


@dataclass(frozen=True, slots=True)
class OpenAIReviewConfig:
    api_key: str = ""
    base_url: str = "https://api.openai.com/v1"
    model: str = "gpt-5-mini"
    timeout_seconds: int = 60
    max_retries: int = 3

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> OpenAIReviewConfig:
        source = environ if environ is not None else os.environ
        return cls(
            api_key=str(source.get("VECTOR_OPENAI_API_KEY", "")).strip()
            or str(source.get("OPENAI_API_KEY", "")).strip(),
            base_url=str(source.get("VECTOR_REVIEW_BASE_URL", "")).strip()
            or str(source.get("VECTOR_OPENAI_BASE_URL", "")).strip()
            or str(source.get("OPENAI_BASE_URL", "")).strip()
            or "https://api.openai.com/v1",
            model=str(source.get("VECTOR_REVIEW_MODEL", "")).strip()
            or "gpt-5-mini",
            timeout_seconds=_int_from_env(
                source,
                "VECTOR_REVIEW_TIMEOUT_SECONDS",
                _int_from_env(source, "OPENAI_TIMEOUT_SECONDS", 60),
            ),
            max_retries=max(1, _int_from_env(source, "VECTOR_REVIEW_MAX_RETRIES", 3)),
        )


@dataclass(slots=True)
class OpenAIReviewProvider:
    config: OpenAIReviewConfig
    transport: OpenAITransport | None = None

    @property
    def is_configured(self) -> bool:
        return bool(self.config.api_key.strip())

    def review(self, request: ReviewRequest) -> list[ReviewedMaterial]:
        if not self.is_configured:
            raise RuntimeError("OpenAI review provider is not configured")
        response = _review_response(
            transport=self.transport or _urllib_transport,
            config=self.config,
            request=request,
        )
        return _reviewed_materials(response, request)


def _headers(api_key: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def _urllib_transport(
    url: str,
    payload: dict[str, object],
    headers: Mapping[str, str],
    timeout_seconds: int,
) -> dict[str, object]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers=dict(headers),
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        decoded: Any = json.loads(response.read().decode("utf-8"))
    if not isinstance(decoded, dict):
        raise TypeError("OpenAI response must be a JSON object")
    return dict(decoded)


def _embedding_vectors(response: Mapping[str, object]) -> list[list[float]]:
    data = response.get("data")
    if not isinstance(data, list):
        raise ValueError("OpenAI embedding response missing data list")
    vectors: list[list[float]] = []
    for item in data:
        if not isinstance(item, Mapping):
            raise ValueError("OpenAI embedding item must be an object")
        raw_vector = item.get("embedding")
        if not isinstance(raw_vector, list):
            raise ValueError("OpenAI embedding item missing embedding list")
        vectors.append([float(value) for value in raw_vector])
    return vectors


def _embedding_batch_response(
    *,
    transport: OpenAITransport,
    config: OpenAIEmbeddingConfig,
    batch: list[str],
) -> dict[str, object]:
    last_error: BaseException | None = None
    for _attempt in range(config.max_retries):
        try:
            return transport(
                f"{config.base_url.rstrip('/')}/embeddings",
                {
                    "model": config.model,
                    "input": batch,
                },
                _headers(config.api_key),
                config.timeout_seconds,
            )
        except urllib.error.HTTPError as exc:
            if not _is_retryable_http_error(exc):
                raise
            last_error = exc
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    raise RuntimeError("OpenAI embedding request was not attempted")


def _review_response(
    *,
    transport: OpenAITransport,
    config: OpenAIReviewConfig,
    request: ReviewRequest,
) -> dict[str, object]:
    last_error: BaseException | None = None
    for _attempt in range(config.max_retries):
        try:
            return transport(
                f"{config.base_url.rstrip('/')}/chat/completions",
                _review_payload(request, config.model),
                _headers(config.api_key),
                config.timeout_seconds,
            )
        except urllib.error.HTTPError as exc:
            if not _is_retryable_http_error(exc):
                raise
            last_error = exc
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    raise RuntimeError("OpenAI review request was not attempted")


def _is_retryable_http_error(error: urllib.error.HTTPError) -> bool:
    return error.code in {408, 409, 429} or error.code >= 500


def _batches(texts: list[str], batch_size: int) -> list[list[str]]:
    safe_batch_size = max(1, batch_size)
    return [
        texts[start : start + safe_batch_size]
        for start in range(0, len(texts), safe_batch_size)
    ]


def _review_payload(request: ReviewRequest, model: str) -> dict[str, object]:
    candidates = [
        {
            "chunk_id": candidate.chunk.chunk_id,
            "content_role": candidate.chunk.content_role,
            "similarity_score": candidate.similarity_score,
            "source_paper": candidate.chunk.title,
            "source_chapter": candidate.chunk.chapter_title,
            "chunk_text": candidate.chunk.chunk_text,
            "needs_real_data": candidate.chunk.needs_real_data,
            "research_method_tags": candidate.chunk.research_method_tags,
            "weighted_score": candidate.weighted_score,
        }
        for candidate in request.candidates
    ]
    system_prompt = (
        "You review retrieved paper chunks for a material package. Return only "
        "strict JSON: {\"results\": [{chunk_id, keep, summary, content_role, "
        "relevance_label, needs_real_data_check, review_reason}]}. Do not "
        "write a paper, do not fabricate real data, and do not rewrite chunks. "
        "Keep relevant chunks even when they need real data verification; mark "
        "needs_real_data_check=true and use relevance_label="
        "\"需要真实数据校验\" when numeric findings or empirical facts must be "
        "checked. If a candidate has needs_real_data=true and is related to "
        "the query, keep it as verification-needed material. Only set "
        "keep=false for off-topic, duplicate, or "
        "administrative noise."
    )
    user_payload = {
        "query_title": request.retrieval_request.query_title,
        "major": request.retrieval_request.major,
        "direction": request.retrieval_request.direction,
        "candidates": candidates,
        "allowed_relevance_labels": ["高相关", "可参考", "辅助材料", "需要真实数据校验"],
    }
    return {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": json.dumps(user_payload, ensure_ascii=False, sort_keys=True),
            },
        ],
        "response_format": {"type": "json_object"},
    }


def _reviewed_materials(
    response: Mapping[str, object],
    request: ReviewRequest,
) -> list[ReviewedMaterial]:
    payload = _decode_chat_json(response)
    results = payload.get("results")
    if not isinstance(results, list):
        raise ValueError("OpenAI review response missing results list")
    scores = {
        candidate.chunk.chunk_id: candidate.similarity_score
        for candidate in request.candidates
    }
    reviewed: list[ReviewedMaterial] = []
    for item in results:
        if not isinstance(item, Mapping):
            raise ValueError("OpenAI review result must be an object")
        chunk_id = _required_str(item, "chunk_id")
        reviewed.append(
            ReviewedMaterial(
                chunk_id=chunk_id,
                keep=_required_bool(item, "keep"),
                summary=_required_str(item, "summary"),
                content_role=_required_str(item, "content_role"),
                relevance_label=_required_str(item, "relevance_label"),
                needs_real_data_check=_required_bool(item, "needs_real_data_check"),
                review_reason=str(item.get("review_reason", "")).strip(),
                similarity_score=scores.get(chunk_id, 0.0),
            ),
        )
    return reviewed


def _decode_chat_json(response: Mapping[str, object]) -> dict[str, object]:
    content = _chat_content(response)
    if not content:
        raise ValueError("OpenAI review response missing message content")
    decoded = json.loads(content)
    if not isinstance(decoded, dict):
        raise ValueError("OpenAI review JSON must be an object")
    return dict(decoded)


def _chat_content(response: Mapping[str, object]) -> str:
    choices = response.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    first = choices[0]
    if not isinstance(first, Mapping):
        return ""
    message = first.get("message")
    if not isinstance(message, Mapping):
        return ""
    content = message.get("content")
    return content if isinstance(content, str) else ""


def _required_str(source: Mapping[str, object], key: str) -> str:
    value = source.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"OpenAI review field {key!r} must be a non-empty string")
    return value


def _required_bool(source: Mapping[str, object], key: str) -> bool:
    value = source.get(key)
    if not isinstance(value, bool):
        raise ValueError(f"OpenAI review field {key!r} must be a boolean")
    return value


def _int_from_env(source: Mapping[str, str], key: str, default: int) -> int:
    raw = str(source.get(key, "")).strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default
