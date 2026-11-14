"""Read-only review view models and disabled gateway defaults."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from paper_pipeline.domain import PaperMetadata
from paper_pipeline.domain.serialization import metadata_from_json
from paper_pipeline.review.codex_review_runner import list_codex_review_candidates
from paper_pipeline.review.codex_review_service import CodexReviewService, CodexReviewer
from paper_pipeline.review.openai_compatible_codex_reviewer import OpenAICompatibleCodexReviewer


@dataclass(frozen=True, slots=True)
class WorkbenchReviewItem:
    paper_id: str
    file_name: str
    source_path: str
    markdown_path: str
    metadata_path: str
    markdown_preview: str
    title: str
    tier: str
    quality_reason: str
    category: str
    direction: str
    research_method: str
    sensitive_clean: bool
    needs_codex_review: bool
    pending_or_failed_reason: str
    sql_eligibility_label: str
    review_history_summary: list[str]
    selected: bool = False


@dataclass(frozen=True, slots=True)
class ReviewActionResult:
    accepted: bool
    message: str
    new_tier: str = ""
    metadata_path: str = ""


class ReviewActionGateway(Protocol):
    def apply_decision(
        self,
        *,
        metadata_path: str,
        new_tier: str,
        reason: str,
        confirmed_by: str,
    ) -> ReviewActionResult:
        raise NotImplementedError

    def review_selected(
        self,
        *,
        metadata_paths: list[str],
        confirmed_by: str,
    ) -> dict[str, object]:
        raise NotImplementedError


class DisabledReviewActionGateway:
    def apply_decision(
        self,
        *,
        metadata_path: str,
        new_tier: str,
        reason: str,
        confirmed_by: str,
    ) -> ReviewActionResult:
        return ReviewActionResult(
            accepted=False,
            message="审核写入路径尚未接入，当前只展示结果。",
            new_tier=new_tier,
            metadata_path=metadata_path,
        )

    def review_selected(
        self,
        *,
        metadata_paths: list[str],
        confirmed_by: str,
    ) -> dict[str, object]:
        return {
            "reviewed_count": 0,
            "failed_count": 0,
            "results": [],
            "message": "审核复审尚未接入。",
        }


@dataclass(slots=True)
class CodexReviewActionGateway:
    library_dir: str | Path
    reviewer: CodexReviewer | None = None

    def apply_decision(
        self,
        *,
        metadata_path: str,
        new_tier: str,
        reason: str,
        confirmed_by: str,
    ) -> ReviewActionResult:
        return ReviewActionResult(
            accepted=False,
            message="请使用开始复审按钮通过 OpenAI/Codex 复审。",
            new_tier=new_tier,
            metadata_path=metadata_path,
        )

    def review_selected(
        self,
        *,
        metadata_paths: list[str],
        confirmed_by: str,
    ) -> dict[str, object]:
        reviewer = self.reviewer or OpenAICompatibleCodexReviewer.from_env()
        service = CodexReviewService(library_dir=self.library_dir, reviewer=reviewer)
        results = []
        for metadata_path in metadata_paths:
            metadata_file = Path(metadata_path)
            results.append(
                service.review_one(
                    metadata_file,
                    _markdown_path_for_metadata(metadata_file),
                    confirmed_by=confirmed_by,
                ),
            )
        failed_count = sum(1 for result in results if result.status == "codex_call_failed")
        return {
            "reviewed_count": len(results),
            "failed_count": failed_count,
            "applied_count": sum(1 for result in results if result.applied),
            "results": results,
        }


def build_review_item(
    *,
    markdown_path: str | Path,
    metadata_path: str | Path,
    selected: bool = False,
) -> WorkbenchReviewItem:
    md_path = Path(markdown_path)
    meta_path = Path(metadata_path)
    markdown_preview = _read_text(md_path, limit=6000)
    metadata = _load_metadata(meta_path)
    if metadata is None:
        partial_metadata = _load_metadata_payload(meta_path)
        if partial_metadata is not None:
            return _item_from_partial_metadata(
                partial_metadata,
                md_path,
                meta_path,
                markdown_preview,
                selected=selected,
            )
        return WorkbenchReviewItem(
            paper_id="",
            file_name=md_path.name,
            source_path="",
            markdown_path=str(md_path),
            metadata_path=str(meta_path),
            markdown_preview=markdown_preview,
            title="",
            tier="",
            quality_reason="",
            category="",
            direction="",
            research_method="",
            sensitive_clean=False,
            needs_codex_review=False,
            pending_or_failed_reason="metadata 缺失或不可读",
            sql_eligibility_label="blocked",
            review_history_summary=[],
            selected=selected,
        )
    return _item_from_metadata(metadata, md_path, meta_path, markdown_preview, selected=selected)


def build_library_review_items(
    library_dir: str | Path,
    *,
    selected_metadata_paths: set[str] | None = None,
) -> list[WorkbenchReviewItem]:
    selected_paths = {str(Path(path)) for path in selected_metadata_paths or set()}
    items: list[WorkbenchReviewItem] = []
    for candidate in list_codex_review_candidates(library_dir):
        metadata_path = str(candidate.metadata_path)
        items.append(
            build_review_item(
                markdown_path=candidate.markdown_path,
                metadata_path=candidate.metadata_path,
                selected=metadata_path in selected_paths,
            ),
        )
    return items


def _item_from_metadata(
    metadata: PaperMetadata,
    markdown_path: Path,
    metadata_path: Path,
    markdown_preview: str,
    *,
    selected: bool = False,
) -> WorkbenchReviewItem:
    tier = getattr(metadata.tier, "value", str(metadata.tier))
    return WorkbenchReviewItem(
        paper_id=f"{metadata.id}_v{metadata.version}",
        file_name=Path(metadata.source_path).name or markdown_path.name,
        source_path=metadata.source_path,
        markdown_path=str(markdown_path),
        metadata_path=str(metadata_path),
        markdown_preview=markdown_preview,
        title=metadata.title,
        tier=tier,
        quality_reason=metadata.quality_reason,
        category=metadata.category,
        direction=metadata.direction,
        research_method=metadata.research_method_primary or "",
        sensitive_clean=metadata.sensitive_clean,
        needs_codex_review=metadata.needs_codex_review,
        pending_or_failed_reason=metadata.quality_reason,
        sql_eligibility_label="ready" if tier in {"gold", "silver"} else "blocked",
        review_history_summary=[
            f"{item.created_at.isoformat() if item.created_at else ''} {item.action}: {item.comment}"
            for item in metadata.review_history
        ],
        selected=selected,
    )


def _item_from_partial_metadata(
    metadata: dict[str, Any],
    markdown_path: Path,
    metadata_path: Path,
    markdown_preview: str,
    *,
    selected: bool = False,
) -> WorkbenchReviewItem:
    paper_id = str(
        metadata.get("accession_id")
        or metadata.get("id")
        or metadata.get("paper_id")
        or markdown_path.parent.name,
    )
    title = str(metadata.get("title") or paper_id or markdown_path.stem)
    tier = str(metadata.get("tier", ""))
    index_status = str(metadata.get("index_status", ""))
    sql_status = str(metadata.get("sql_status", ""))
    quality_reason = " / ".join(
        value
        for value in [
            f"index={index_status}" if index_status else "",
            f"sql={sql_status}" if sql_status else "",
        ]
        if value
    )
    if not quality_reason:
        quality_reason = "metadata is partial but readable"
    return WorkbenchReviewItem(
        paper_id=paper_id,
        file_name=str(metadata.get("source_path") or markdown_path.name).split("\\")[-1],
        source_path=str(metadata.get("source_path", "")),
        markdown_path=str(markdown_path),
        metadata_path=str(metadata_path),
        markdown_preview=markdown_preview,
        title=title,
        tier=tier,
        quality_reason=quality_reason,
        category=str(metadata.get("category", "")),
        direction=str(metadata.get("direction", "")),
        research_method=str(metadata.get("research_method_primary", "")),
        sensitive_clean=bool(metadata.get("sensitive_clean", False)),
        needs_codex_review=bool(metadata.get("needs_codex_review", False)),
        pending_or_failed_reason=quality_reason,
        sql_eligibility_label="ready" if tier in {"gold", "silver"} else "blocked",
        review_history_summary=[],
        selected=selected,
    )


def _markdown_path_for_metadata(metadata_path: Path) -> Path:
    if metadata_path.name.endswith(".metadata.json"):
        return metadata_path.with_name(metadata_path.name.replace(".metadata.json", ".md"))
    if metadata_path.name == "metadata.json" and metadata_path.parent.name.startswith("ACC_"):
        return metadata_path.parent / f"{metadata_path.parent.name}.md"
    return metadata_path.with_suffix(".md")


def _read_text(path: Path, *, limit: int) -> str:
    try:
        return path.read_text(encoding="utf-8")[:limit]
    except OSError:
        return "Markdown 缺失"


def _load_metadata(path: Path) -> PaperMetadata | None:
    try:
        return metadata_from_json(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError, KeyError):
        return None


def _load_metadata_payload(path: Path) -> dict[str, Any] | None:
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(decoded, dict):
        return None
    return decoded
