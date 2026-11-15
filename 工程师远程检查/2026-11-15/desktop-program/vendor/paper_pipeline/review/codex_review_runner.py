"""Batch runner for Codex review candidates in the formal library."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from paper_pipeline.domain import Tier
from paper_pipeline.domain.serialization import metadata_from_json
from paper_pipeline.review.codex_review_service import (
    CodexReviewer,
    CodexReviewRunResult,
    CodexReviewService,
)
from paper_pipeline.review.openai_compatible_codex_reviewer import (
    OpenAICompatibleCodexReviewer,
)


@dataclass(frozen=True, slots=True)
class CodexReviewCandidate:
    metadata_path: Path
    markdown_path: Path
    paper_id: str
    version: int

    def to_dict(self) -> dict[str, object]:
        return {
            "metadata_path": str(self.metadata_path),
            "markdown_path": str(self.markdown_path),
            "paper_id": self.paper_id,
            "version": self.version,
        }


@dataclass(frozen=True, slots=True)
class CodexReviewBatchResult:
    library_dir: Path
    dry_run: bool
    candidates: list[CodexReviewCandidate] = field(default_factory=list)
    results: list[CodexReviewRunResult] = field(default_factory=list)

    @property
    def candidate_count(self) -> int:
        return len(self.candidates)

    @property
    def reviewed_count(self) -> int:
        return len(self.results)

    def to_dict(self) -> dict[str, object]:
        return {
            "library_dir": str(self.library_dir),
            "dry_run": self.dry_run,
            "candidate_count": self.candidate_count,
            "reviewed_count": self.reviewed_count,
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "results": [_review_result_to_dict(result) for result in self.results],
        }


def list_codex_review_candidates(
    library_dir: str | Path,
    *,
    limit: int | None = None,
) -> list[CodexReviewCandidate]:
    root = Path(library_dir)
    candidates: list[CodexReviewCandidate] = []
    for metadata_path in _iter_metadata_paths(root):
        metadata = metadata_from_json(metadata_path.read_text(encoding="utf-8"))
        tier = metadata.tier if isinstance(metadata.tier, Tier) else Tier(metadata.tier)
        if tier is not Tier.PENDING:
            continue
        if not metadata.needs_codex_review:
            continue
        markdown_path = _markdown_path_for_metadata(metadata_path)
        if not markdown_path.exists():
            continue
        candidates.append(
            CodexReviewCandidate(
                metadata_path=metadata_path,
                markdown_path=markdown_path,
                paper_id=metadata.id,
                version=metadata.version,
            ),
        )
        if limit is not None and len(candidates) >= limit:
            break
    return candidates


def run_codex_reviews(
    library_dir: str | Path,
    *,
    reviewer: CodexReviewer | None = None,
    limit: int | None = None,
    dry_run: bool = False,
) -> CodexReviewBatchResult:
    root = Path(library_dir)
    candidates = list_codex_review_candidates(root, limit=limit)
    if dry_run:
        return CodexReviewBatchResult(
            library_dir=root,
            dry_run=True,
            candidates=candidates,
        )

    resolved_reviewer = reviewer or OpenAICompatibleCodexReviewer.from_env()
    service = CodexReviewService(library_dir=root, reviewer=resolved_reviewer)
    results = [
        service.review_one(candidate.metadata_path, candidate.markdown_path)
        for candidate in candidates
    ]
    return CodexReviewBatchResult(
        library_dir=root,
        dry_run=False,
        candidates=candidates,
        results=results,
    )


def _iter_metadata_paths(root: Path):
    seen: set[Path] = set()
    patterns = ("*.metadata.json", "metadata.json")
    for pattern in patterns:
        for path in sorted(root.rglob(pattern)):
            if path in seen:
                continue
            seen.add(path)
            if _is_under_runs(path, root):
                continue
            yield path


def _is_under_runs(path: Path, root: Path) -> bool:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return False
    return bool(relative.parts) and relative.parts[0] == "runs"


def _markdown_path_for_metadata(metadata_path: Path) -> Path:
    if metadata_path.name.endswith(".metadata.json"):
        return metadata_path.with_name(
            metadata_path.name.replace(".metadata.json", ".md"),
        )
    if metadata_path.name == "metadata.json" and metadata_path.parent.name.startswith("ACC_"):
        return metadata_path.parent / f"{metadata_path.parent.name}.md"
    return metadata_path.with_suffix(".md")


def _review_result_to_dict(result: CodexReviewRunResult) -> dict[str, object]:
    return {
        "status": result.status,
        "metadata_path": str(result.metadata_path),
        "markdown_path": str(result.markdown_path),
        "applied": result.applied,
        "reason": result.reason,
        "error": result.error,
        "new_tier": (
            result.review_result.new_tier.value
            if result.review_result is not None
            else ""
        ),
        "sql_upload_triggered": (
            result.review_result.sql_upload_triggered
            if result.review_result is not None
            else False
        ),
        "index_build_triggered": (
            result.review_result.index_build_triggered
            if result.review_result is not None
            else False
        ),
    }
