"""S7-1 chapter index writer for local index packages."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from paper_pipeline.domain import PaperMetadata
from paper_pipeline.eligibility import EligibilityScope, check_formal_candidate
from paper_pipeline.indexing.metadata_index import METADATA_INDEX_FILENAME
from paper_pipeline.indexing.package_skeleton import (
    MANIFEST_FILENAME,
    MANIFEST_TYPE,
    SCHEMA_VERSION,
    _sha256_file,
    build_package_dir_name,
)
from paper_pipeline.indexing.paper_text_index import PAPER_TEXT_INDEX_FILENAME

INDEX_TYPE = "chapter_index"
INDEX_SCOPE = "formal_candidate"
CHAPTER_INDEX_FILENAME = "chapter_index.json"
_TMP_CHAPTER_INDEX_FILENAME = ".chapter_index.json.tmp"
_METADATA_INDEX_TYPE = "metadata_index"
_PAPER_TEXT_INDEX_TYPE = "paper_text_index"
_CHAPTER_DETECTION_WARNING = "chapter_detection_warning"

_EXCLUDED_TITLES = {
    "目录",
    "目 录",
    "摘要",
    "abstract",
    "关键词",
    "关键字",
    "keywords",
    "keyword",
}

_ROLE_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("references", ("参考文献", "references")),
    ("appendix", ("附录", "appendix")),
    ("conclusion", ("结论", "conclusion")),
    ("literature_review", ("文献综述", "研究综述", "literature")),
    ("theory_basis", ("理论基础", "理论依据")),
    ("method_design", ("研究设计", "方法", "methodology", "method")),
    ("current_situation", ("现状", "current situation")),
    ("problem_analysis", ("问题分析", "问题", "problem")),
    ("strategy_suggestion", ("对策", "建议", "策略", "strategy", "suggestion")),
    ("data_analysis", ("数据", "实证", "analysis", "财务分析", "指标分析")),
    ("introduction", ("绪论", "引言", "introduction")),
)


@dataclass(frozen=True)
class ChapterIndexBuildResult:
    paper_id: str
    version: int
    package_dir: Path
    chapter_index_path: Path
    created: bool
    skipped: bool
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ChapterRecord:
    chapter_id: str
    chapter_order: int
    chapter_title: str
    chapter_role: str
    heading_path: list[str]
    text_hash: str
    chapter_role_source: str
    llm_model: str | None
    index_scope: str
    warnings: list[str] = field(default_factory=list)


def build_chapter_index(
    metadata: PaperMetadata,
    metadata_path: Path,
    markdown_path: Path,
    package_dir: Path,
    *,
    overwrite: bool = False,
) -> ChapterIndexBuildResult:
    metadata_file = Path(metadata_path)
    markdown_file = Path(markdown_path)
    package_path = Path(package_dir)

    paper_id = str(metadata.id)
    version = metadata.version
    build_package_dir_name(paper_id, version)
    chapter_index_path = package_path / CHAPTER_INDEX_FILENAME

    eligibility = check_formal_candidate(
        metadata,
        metadata_path=metadata_file,
        markdown_path=markdown_file,
    )
    warnings = _merge_unique(eligibility.warnings)

    if not (
        eligibility.eligible
        and eligibility.scope is EligibilityScope.FORMAL_CANDIDATE
    ):
        reasons = _merge_unique(["not_formal_candidate", *eligibility.blocking_reasons])
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            chapter_index_path=chapter_index_path,
            reasons=reasons,
            warnings=warnings,
        )

    if not package_path.exists() or not package_path.is_dir():
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            chapter_index_path=chapter_index_path,
            reasons=["package_dir_missing"],
            warnings=warnings,
        )

    manifest_path = package_path / MANIFEST_FILENAME
    if not manifest_path.exists():
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            chapter_index_path=chapter_index_path,
            reasons=["package_manifest_missing"],
            warnings=warnings,
        )

    metadata_index_path = package_path / METADATA_INDEX_FILENAME
    if not metadata_index_path.exists():
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            chapter_index_path=chapter_index_path,
            reasons=["metadata_index_missing"],
            warnings=warnings,
        )

    paper_text_index_path = package_path / PAPER_TEXT_INDEX_FILENAME
    if not paper_text_index_path.exists():
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            chapter_index_path=chapter_index_path,
            reasons=["paper_text_index_missing"],
            warnings=warnings,
        )

    markdown_hash = _sha256_file(markdown_file)
    source_hash = eligibility.source_hash or _blank_to_none(metadata.source_hash)

    manifest = _read_json_object(manifest_path)
    if manifest is None:
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            chapter_index_path=chapter_index_path,
            reasons=["package_manifest_invalid"],
            warnings=warnings,
        )
    if not _manifest_matches_skeleton(
        manifest=manifest,
        metadata=metadata,
        metadata_path=metadata_file,
        markdown_path=markdown_file,
        package_dir=package_path,
        source_hash=source_hash,
    ):
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            chapter_index_path=chapter_index_path,
            reasons=["package_manifest_invalid"],
            warnings=warnings,
        )
    if manifest.get("markdown_hash") != markdown_hash:
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            chapter_index_path=chapter_index_path,
            reasons=["manifest_markdown_hash_mismatch"],
            warnings=warnings,
        )

    metadata_index = _read_json_object(metadata_index_path)
    if metadata_index is None:
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            chapter_index_path=chapter_index_path,
            reasons=["metadata_index_invalid"],
            warnings=warnings,
        )
    if not _metadata_index_matches(
        metadata_index=metadata_index,
        metadata=metadata,
        metadata_path=metadata_file,
        markdown_path=markdown_file,
        source_hash=source_hash,
    ):
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            chapter_index_path=chapter_index_path,
            reasons=["metadata_index_invalid"],
            warnings=warnings,
        )
    if metadata_index.get("markdown_hash") != markdown_hash:
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            chapter_index_path=chapter_index_path,
            reasons=["metadata_index_markdown_hash_mismatch"],
            warnings=warnings,
        )

    paper_text_index = _read_json_object(paper_text_index_path)
    if paper_text_index is None:
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            chapter_index_path=chapter_index_path,
            reasons=["paper_text_index_invalid"],
            warnings=warnings,
        )
    if not _paper_text_index_matches(
        paper_text_index=paper_text_index,
        metadata=metadata,
        metadata_path=metadata_file,
        markdown_path=markdown_file,
        markdown_hash=markdown_hash,
    ):
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            chapter_index_path=chapter_index_path,
            reasons=["paper_text_index_invalid"],
            warnings=warnings,
        )

    text_hash = _blank_to_none(paper_text_index.get("text_hash"))
    if text_hash is None:
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            chapter_index_path=chapter_index_path,
            reasons=["paper_text_index_text_hash_missing"],
            warnings=warnings,
        )

    if chapter_index_path.exists() and not overwrite:
        return _result(
            paper_id=paper_id,
            version=version,
            package_dir=package_path,
            chapter_index_path=chapter_index_path,
            reasons=["chapter_index_exists"],
            warnings=warnings,
        )

    markdown_text = markdown_file.read_text(encoding="utf-8")
    chapters, chapter_warnings = _build_chapter_records(
        paper_id=paper_id,
        version=version,
        markdown_text=markdown_text,
    )
    payload_warnings = _merge_unique([*warnings, *chapter_warnings])
    payload = _build_payload(
        metadata=metadata,
        metadata_path=metadata_file,
        markdown_path=markdown_file,
        markdown_hash=markdown_hash,
        text_hash=text_hash,
        chapters=chapters,
        warnings=payload_warnings,
    )
    _atomic_write_json(chapter_index_path, payload)

    return ChapterIndexBuildResult(
        paper_id=paper_id,
        version=version,
        package_dir=package_path,
        chapter_index_path=chapter_index_path,
        created=True,
        skipped=False,
        reasons=[],
        warnings=payload_warnings,
    )


def infer_chapter_role(chapter_title: str) -> tuple[str, str]:
    normalized = _normalize_title(chapter_title).lower()
    for role, needles in _ROLE_RULES:
        if any(needle.lower() in normalized for needle in needles):
            return role, "rule"
    return "unknown", "rule"


def _build_chapter_records(
    *,
    paper_id: str,
    version: int,
    markdown_text: str,
) -> tuple[list[ChapterRecord], list[str]]:
    titles = _detect_chapter_titles(markdown_text)
    if not titles:
        return (
            [
                ChapterRecord(
                    chapter_id="ch_001",
                    chapter_order=1,
                    chapter_title="Main Text",
                    chapter_role="unknown",
                    heading_path=["Main Text"],
                    text_hash=_chapter_text_hash(
                        paper_id=paper_id,
                        version=version,
                        chapter_order=1,
                        chapter_title="Main Text",
                    ),
                    chapter_role_source="fallback",
                    llm_model=None,
                    index_scope=INDEX_SCOPE,
                    warnings=[_CHAPTER_DETECTION_WARNING],
                ),
            ],
            [_CHAPTER_DETECTION_WARNING],
        )

    chapters: list[ChapterRecord] = []
    for order, title in enumerate(titles, start=1):
        role, role_source = infer_chapter_role(title)
        chapters.append(
            ChapterRecord(
                chapter_id=f"ch_{order:03d}",
                chapter_order=order,
                chapter_title=title,
                chapter_role=role,
                heading_path=[title],
                text_hash=_chapter_text_hash(
                    paper_id=paper_id,
                    version=version,
                    chapter_order=order,
                    chapter_title=title,
                ),
                chapter_role_source=role_source,
                llm_model=None,
                index_scope=INDEX_SCOPE,
                warnings=[],
            ),
        )
    return chapters, []


def _detect_chapter_titles(markdown_text: str) -> list[str]:
    titles: list[str] = []
    in_fenced_code = False
    for raw_line in str(markdown_text or "").replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        line = raw_line.strip()
        if re.match(r"^`{3,}|^~{3,}", line):
            in_fenced_code = not in_fenced_code
            continue
        if in_fenced_code or not line:
            continue

        title = _markdown_h1_title(line) or _plain_main_heading_title(line)
        if title is None:
            continue
        normalized = _normalize_title(title)
        if not normalized or _is_excluded_title(normalized):
            continue
        if normalized not in titles:
            titles.append(normalized)
    return titles


def _markdown_h1_title(line: str) -> str | None:
    match = re.match(r"^#{1}(?!#)\s+(.+?)\s*#*\s*$", line)
    if match is None:
        return None
    return match.group(1)


def _plain_main_heading_title(line: str) -> str | None:
    normalized = _normalize_title(line)
    if len(normalized) > 80:
        return None
    if re.match(r"^第[一二三四五六七八九十百千万\d]+章\s+\S+", normalized):
        return normalized
    if re.match(r"^[一二三四五六七八九十]+、\s*\S+", normalized):
        return normalized
    if re.match(r"^\d+[.、]?\s+\S+", normalized):
        return normalized
    return None


def _is_excluded_title(title: str) -> bool:
    normalized = re.sub(r"\s+", " ", title).strip().lower()
    compact = normalized.replace(" ", "")
    return normalized in _EXCLUDED_TITLES or compact in _EXCLUDED_TITLES


def _chapter_text_hash(
    *,
    paper_id: str,
    version: int,
    chapter_order: int,
    chapter_title: str,
) -> str:
    payload = f"{paper_id}|{version}|{chapter_order}|{chapter_title}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _manifest_matches_skeleton(
    *,
    manifest: dict[str, Any],
    metadata: PaperMetadata,
    metadata_path: Path,
    markdown_path: Path,
    package_dir: Path,
    source_hash: str | None,
) -> bool:
    expected = {
        "schema_version": SCHEMA_VERSION,
        "manifest_type": MANIFEST_TYPE,
        "paper_id": metadata.id,
        "version": metadata.version,
        "index_scope": INDEX_SCOPE,
        "source_hash": source_hash,
        "metadata_path": str(metadata_path),
        "markdown_path": str(markdown_path),
        "package_path": str(package_dir),
    }
    return all(manifest.get(key) == value for key, value in expected.items())


def _metadata_index_matches(
    *,
    metadata_index: dict[str, Any],
    metadata: PaperMetadata,
    metadata_path: Path,
    markdown_path: Path,
    source_hash: str | None,
) -> bool:
    expected = {
        "schema_version": SCHEMA_VERSION,
        "index_type": _METADATA_INDEX_TYPE,
        "paper_id": metadata.id,
        "version": metadata.version,
        "source_hash": source_hash,
        "metadata_path": str(metadata_path),
        "markdown_path": str(markdown_path),
    }
    return all(metadata_index.get(key) == value for key, value in expected.items())


def _paper_text_index_matches(
    *,
    paper_text_index: dict[str, Any],
    metadata: PaperMetadata,
    metadata_path: Path,
    markdown_path: Path,
    markdown_hash: str,
) -> bool:
    expected = {
        "schema_version": SCHEMA_VERSION,
        "index_type": _PAPER_TEXT_INDEX_TYPE,
        "paper_id": metadata.id,
        "version": metadata.version,
        "metadata_path": str(metadata_path),
        "markdown_path": str(markdown_path),
        "markdown_hash": markdown_hash,
    }
    return all(paper_text_index.get(key) == value for key, value in expected.items())


def _build_payload(
    *,
    metadata: PaperMetadata,
    metadata_path: Path,
    markdown_path: Path,
    markdown_hash: str,
    text_hash: str,
    chapters: list[ChapterRecord],
    warnings: list[str],
) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    return {
        "schema_version": SCHEMA_VERSION,
        "index_type": INDEX_TYPE,
        "paper_id": metadata.id,
        "version": metadata.version,
        "index_scope": INDEX_SCOPE,
        "markdown_path": str(markdown_path),
        "metadata_path": str(metadata_path),
        "markdown_hash": markdown_hash,
        "text_hash": text_hash,
        "chapters": [
            _chapter_to_payload(
                chapter=chapter,
                paper_id=metadata.id,
                version=metadata.version,
                updated_at=now,
            )
            for chapter in chapters
        ],
        "warnings": warnings,
        "updated_at": now,
    }


def _chapter_to_payload(
    *,
    chapter: ChapterRecord,
    paper_id: str,
    version: int,
    updated_at: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "paper_id": paper_id,
        "version": version,
        "chapter_id": chapter.chapter_id,
        "chapter_order": chapter.chapter_order,
        "chapter_title": chapter.chapter_title,
        "chapter_role": chapter.chapter_role,
        "heading_path": chapter.heading_path,
        "text_hash": chapter.text_hash,
        "chapter_role_source": chapter.chapter_role_source,
        "llm_model": chapter.llm_model,
        "index_scope": chapter.index_scope,
        "warnings": chapter.warnings,
        "updated_at": updated_at,
    }


def _read_json_object(path: Path) -> dict[str, Any] | None:
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(decoded, dict):
        return None
    return decoded


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    tmp_path = path.with_name(_TMP_CHAPTER_INDEX_FILENAME)
    encoded = json.dumps(payload, ensure_ascii=False, indent=2)
    tmp_path.write_text(f"{encoded}\n", encoding="utf-8")
    tmp_path.replace(path)


def _result(
    *,
    paper_id: str,
    version: int,
    package_dir: Path,
    chapter_index_path: Path,
    reasons: list[str],
    warnings: list[str],
) -> ChapterIndexBuildResult:
    return ChapterIndexBuildResult(
        paper_id=paper_id,
        version=version,
        package_dir=package_dir,
        chapter_index_path=chapter_index_path,
        created=False,
        skipped=True,
        reasons=reasons,
        warnings=warnings,
    )


def _normalize_title(value: object) -> str:
    text = str(value or "").strip().replace("\ufeff", "")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _blank_to_none(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _merge_unique(items: list[str]) -> list[str]:
    merged: list[str] = []
    for item in items:
        if item and item not in merged:
            merged.append(item)
    return merged
