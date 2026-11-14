"""Deterministic fake providers used by unit tests and dry-runs."""

from __future__ import annotations

from hashlib import sha256

from paper_pipeline.vector_retrieval.models import ReviewRequest, ReviewedMaterial


KEY_TERMS = (
    "体育",
    "训练",
    "干预",
    "实验",
    "方法",
    "数据",
    "结果",
    "前测",
    "后测",
    "测试",
    "指标",
    "问题",
    "对策",
    "结论",
    "儿童",
    "力量",
)


class DeterministicFakeEmbeddingProvider:
    provider_name = "fake"
    model_name = "deterministic-keyword-v1"

    def __init__(self, dimension: int = len(KEY_TERMS)) -> None:
        self.dimension = dimension

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [self._embed_one(text) for text in texts]

    def estimate_tokens(self, texts: list[str]) -> int:
        return sum(max(1, len(text) // 2) for text in texts)

    def estimate_cost(self, token_count: int) -> float:
        return round(token_count * 0.0000001, 8)

    def _embed_one(self, text: str) -> list[float]:
        base = [float(text.count(term)) for term in KEY_TERMS]
        counter = 0
        while len(base) < self.dimension:
            digest = sha256(f"{counter}:{text}".encode("utf-8")).digest()
            base.extend(float(value % 7) / 10 for value in digest)
            counter += 1
        vector = base[: self.dimension]
        if not any(vector):
            return [0.1 for _ in range(self.dimension)]
        return vector


class FakeLightweightReviewProvider:
    def review(self, request: ReviewRequest) -> list[ReviewedMaterial]:
        query = request.retrieval_request.query_text
        results: list[ReviewedMaterial] = []
        for candidate in request.candidates:
            chunk = candidate.chunk
            overlap = _keyword_overlap(query, chunk.chunk_text)
            keep = candidate.weighted_score > 0 or overlap > 0
            relevance = _relevance_label(
                candidate.weighted_score,
                chunk.needs_real_data,
            )
            results.append(
                ReviewedMaterial(
                    chunk_id=chunk.chunk_id,
                    keep=keep,
                    summary=_summary(chunk.chunk_text),
                    content_role=chunk.content_role,
                    relevance_label=relevance,
                    needs_real_data_check=chunk.needs_real_data,
                    review_reason="deterministic fake review",
                    similarity_score=candidate.similarity_score,
                ),
            )
        return results


def _keyword_overlap(left: str, right: str) -> int:
    return sum(1 for term in KEY_TERMS if term in left and term in right)


def _relevance_label(score: float, needs_real_data: bool) -> str:
    if needs_real_data:
        return "需要真实数据校验"
    if score >= 0.8:
        return "高相关"
    if score >= 0.35:
        return "可参考"
    return "辅助材料"


def _summary(text: str) -> str:
    compact = " ".join(text.split())
    return compact[:80]
