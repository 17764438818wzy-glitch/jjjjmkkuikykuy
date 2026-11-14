"""Sports-specific candidate relevance scoring."""

from __future__ import annotations

from paper_pipeline.vector_retrieval.models import CandidateChunk, RetrievalRequest


SPORTS_PROJECT_TERMS = {
    "游泳",
    "羽毛球",
    "篮球",
    "瑜伽",
    "跑团",
    "健身跑",
    "射箭",
    "武术",
    "体适能",
    "高考体育",
    "健身类app",
    "体育兴趣",
}
SPORTS_SCENE_TERMS = {
    "少儿",
    "幼儿",
    "青少年",
    "老年人",
    "大学生",
    "俱乐部",
    "培训机构",
    "训练营",
    "公园",
    "学院",
    "疫情",
}
SPORTS_SIGNAL_TERMS = SPORTS_PROJECT_TERMS | SPORTS_SCENE_TERMS | {"体育", "运动", "训练"}


class SportsSpecificityScorer:
    """Sports-only project and scene specificity scorer."""

    def score(self, request: RetrievalRequest, candidate: CandidateChunk) -> float:
        query_text = _combined_request_text(request)
        if not _has_sports_signal(query_text):
            return 0.0
        candidate_text = _combined_candidate_text(candidate)
        query_projects = _matched_terms(query_text, SPORTS_PROJECT_TERMS)
        candidate_projects = _matched_terms(candidate_text, SPORTS_PROJECT_TERMS)
        query_scenes = _matched_terms(query_text, SPORTS_SCENE_TERMS)
        candidate_scenes = _matched_terms(candidate_text, SPORTS_SCENE_TERMS)

        score = 0.0
        if query_projects:
            if query_projects & candidate_projects:
                score += 0.18
            elif candidate_projects:
                score -= 0.16
        if query_scenes:
            if query_scenes & candidate_scenes:
                score += 0.06
            elif candidate_scenes:
                score -= 0.04
        return score


def _combined_request_text(request: RetrievalRequest) -> str:
    return f"{request.query_title} {request.major} {request.direction}".lower()


def _combined_candidate_text(candidate: CandidateChunk) -> str:
    chunk = candidate.chunk
    return f"{chunk.title} {chunk.direction} {chunk.chapter_title} {chunk.chunk_text}".lower()


def _matched_terms(text: str, terms: set[str]) -> set[str]:
    return {term for term in terms if term.lower() in text}


def _has_sports_signal(text: str) -> bool:
    return bool(_matched_terms(text, SPORTS_SIGNAL_TERMS))
