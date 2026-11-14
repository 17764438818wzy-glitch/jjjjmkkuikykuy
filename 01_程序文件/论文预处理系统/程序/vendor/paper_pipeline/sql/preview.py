"""SQL upload preview generation and persistence."""

from __future__ import annotations

import json
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from paper_pipeline.domain import PaperMetadata
from paper_pipeline.domain.serialization import metadata_from_json
from paper_pipeline.runtime.single_entry import ensure_real_flow_50_caller
from paper_pipeline.sql.eligibility import (
    SqlEligibilityCheck,
    SqlEligibilityResult,
    check_sql_eligibility,
)
from paper_pipeline.sql.sql_models import (
    SqlUploadBlockedItem,
    SqlUploadPreview,
    SqlUploadPreviewItem,
    calculate_preview_checksum,
    file_fingerprint,
    preview_from_dict,
    preview_to_dict,
    utc_expires_iso,
    utc_now_iso,
)


@dataclass(slots=True)
class SqlUploadPreviewService:
    library_dir: str | Path
    expires_after_hours: int = 24

    def generate_preview(
        self,
        *,
        run_id: str,
        created_by: str,
        candidate_checks: Sequence[SqlEligibilityCheck] | None = None,
    ) -> SqlUploadPreview:
        ensure_real_flow_50_caller(
            "SqlUploadPreviewService.generate_preview",
            allow_test_callers=True,
            allow_modules=("paper_pipeline.sql.preview",),
        )
        checks = list(candidate_checks or self.discover_candidate_checks())
        items: list[SqlUploadPreviewItem] = []
        blocked_items: list[SqlUploadBlockedItem] = []

        for check in checks:
            result = check_sql_eligibility(check)
            metadata = check.metadata or _load_metadata(check.metadata_path)
            if result.eligible and metadata is not None:
                items.append(_preview_item_from(metadata, check, result))
            else:
                blocked_items.append(_blocked_item_from(result, check, metadata))

        preview_without_checksum = SqlUploadPreview(
            preview_id=f"sqlpreview-{uuid.uuid4().hex}",
            run_id=run_id,
            created_at=utc_now_iso(),
            created_by=created_by,
            candidate_count=len(checks),
            eligible_count=len(items),
            blocked_count=len(blocked_items),
            items=items,
            blocked_items=blocked_items,
            preview_checksum="",
            expires_at=utc_expires_iso(hours=self.expires_after_hours),
            ready_for_confirmation=bool(items),
            will_upload_sql=False,
        )
        preview = _with_checksum(preview_without_checksum)
        self.persist_preview(preview)
        return preview

    def discover_candidate_checks(self) -> list[SqlEligibilityCheck]:
        root = Path(self.library_dir)
        if not root.exists():
            return []

        checks: list[SqlEligibilityCheck] = []
        known_md_paths: set[Path] = set()
        metadata_paths = _discover_metadata_paths(root)
        for metadata_path in metadata_paths:
            metadata = _load_metadata(metadata_path)
            md_path = _markdown_path_for_metadata(metadata_path)
            known_md_paths.add(md_path)
            checks.append(
                SqlEligibilityCheck(
                    metadata=metadata,
                    metadata_path=metadata_path,
                    md_path=md_path,
                ),
            )

        md_paths = sorted(
            path
            for path in root.rglob("*.md")
            if not _is_under(path, root, "runs") and not _is_under(path, root, "indexes")
        )
        for md_path in md_paths:
            if md_path in known_md_paths:
                continue
            checks.append(
                SqlEligibilityCheck(
                    metadata=None,
                    metadata_path=_metadata_path_for_markdown(md_path),
                    md_path=md_path,
                ),
            )
        return checks

    def persist_preview(self, preview: SqlUploadPreview) -> Path:
        path = self.preview_path(preview.run_id, preview.preview_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(preview_to_dict(preview), ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return path

    def load_preview(self, preview_id: str) -> SqlUploadPreview:
        matches = sorted(Path(self.library_dir).glob(f"runs/*/sql_previews/{preview_id}.json"))
        if not matches:
            raise FileNotFoundError(preview_id)
        decoded = json.loads(matches[0].read_text(encoding="utf-8"))
        if not isinstance(decoded, dict):
            raise TypeError("SQL preview JSON must decode to an object")
        return preview_from_dict(decoded)

    def preview_path(self, run_id: str, preview_id: str) -> Path:
        return Path(self.library_dir) / "runs" / run_id / "sql_previews" / f"{preview_id}.json"


def generate_sql_upload_preview(
    library_dir: str | Path,
    run_id: str,
    created_by: str,
    candidate_checks: Sequence[SqlEligibilityCheck] | None = None,
) -> SqlUploadPreview:
    ensure_real_flow_50_caller(
        "generate_sql_upload_preview",
        allow_test_callers=True,
        allow_modules=("paper_pipeline.sql.preview",),
    )
    return SqlUploadPreviewService(library_dir).generate_preview(
        run_id=run_id,
        created_by=created_by,
        candidate_checks=candidate_checks,
    )


def generate_sql_preview(
    library_dir: str | Path,
    run_id: str,
    created_by: str,
    candidate_checks: Sequence[SqlEligibilityCheck] | None = None,
) -> SqlUploadPreview:
    ensure_real_flow_50_caller("generate_sql_preview", allow_test_callers=True)
    return generate_sql_upload_preview(
        library_dir,
        run_id,
        created_by,
        candidate_checks=candidate_checks,
    )


def _with_checksum(preview: SqlUploadPreview) -> SqlUploadPreview:
    checksum = calculate_preview_checksum(preview)
    return SqlUploadPreview(
        preview_id=preview.preview_id,
        run_id=preview.run_id,
        created_at=preview.created_at,
        created_by=preview.created_by,
        candidate_count=preview.candidate_count,
        eligible_count=preview.eligible_count,
        blocked_count=preview.blocked_count,
        items=preview.items,
        blocked_items=preview.blocked_items,
        preview_checksum=checksum,
        expires_at=preview.expires_at,
        ready_for_confirmation=preview.ready_for_confirmation,
        will_upload_sql=preview.will_upload_sql,
    )


def _preview_item_from(
    metadata: PaperMetadata,
    check: SqlEligibilityCheck,
    result: SqlEligibilityResult,
) -> SqlUploadPreviewItem:
    metadata_path = Path(check.metadata_path or "")
    md_path = Path(check.md_path or "")
    return SqlUploadPreviewItem(
        paper_id=result.paper_id,
        accession_id=metadata.id,
        version=metadata.version,
        title=metadata.title,
        tier=str(metadata.tier),
        category=metadata.category,
        direction=metadata.direction,
        research_method_primary=metadata.research_method_primary or "",
        research_method_tags=list(metadata.research_method_tags),
        source_hash=metadata.source_hash,
        source_path=metadata.source_path,
        md_path=str(md_path),
        metadata_path=str(metadata_path),
        index_status=str(metadata.index_status),
        sql_status=str(metadata.sql_status),
        version_status=str(metadata.version_status),
        duplicate_status=str(metadata.duplicate_status),
        abstract=metadata.abstract,
        keywords=list(metadata.keywords),
        similar_to=list(metadata.similar_to),
        metadata_fingerprint=file_fingerprint(metadata_path),
        md_fingerprint=file_fingerprint(md_path),
    )


def _blocked_item_from(
    result: SqlEligibilityResult,
    check: SqlEligibilityCheck,
    metadata: PaperMetadata | None,
) -> SqlUploadBlockedItem:
    paper_id = result.paper_id or _fallback_paper_id(check, metadata)
    return SqlUploadBlockedItem(
        paper_id=paper_id,
        reasons=list(result.reasons),
        blocking_fields=list(result.blocking_fields),
        suggested_next_action=result.suggested_next_action,
        metadata_path=str(check.metadata_path or ""),
        md_path=str(check.md_path or ""),
    )


def _fallback_paper_id(check: SqlEligibilityCheck, metadata: PaperMetadata | None) -> str:
    if metadata is not None:
        return f"{metadata.id}_v{metadata.version}"
    if check.metadata_path is not None:
        metadata_path = Path(check.metadata_path)
        if metadata_path.name == "metadata.json":
            return metadata_path.parent.name
        return metadata_path.name.removesuffix(".metadata.json")
    if check.md_path is not None:
        return Path(check.md_path).stem
    return ""


def _discover_metadata_paths(root: Path) -> list[Path]:
    paths: dict[Path, None] = {}
    for pattern in ("metadata.json", "*.metadata.json"):
        for path in root.rglob(pattern):
            if _is_under(path, root, "runs") or _is_under(path, root, "indexes"):
                continue
            paths[path] = None
    return sorted(paths)


def _markdown_path_for_metadata(metadata_path: Path) -> Path:
    if metadata_path.name == "metadata.json":
        return metadata_path.parent / f"{metadata_path.parent.name}.md"
    return metadata_path.with_name(metadata_path.name.removesuffix(".metadata.json") + ".md")


def _metadata_path_for_markdown(md_path: Path) -> Path:
    if md_path.parent.name == md_path.stem:
        return md_path.parent / "metadata.json"
    return md_path.with_name(f"{md_path.stem}.metadata.json")


def _load_metadata(path: str | Path | None) -> PaperMetadata | None:
    if path is None:
        return None
    candidate = Path(path)
    if not candidate.exists():
        return None
    try:
        return metadata_from_json(candidate.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError):
        return None


def _is_under(path: Path, root: Path, directory_name: str) -> bool:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return False
    return bool(relative.parts) and relative.parts[0] == directory_name
