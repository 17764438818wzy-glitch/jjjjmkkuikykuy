"""Build local BM25/full-text and vector-stub indexes for formal library papers."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path

from paper_pipeline.domain import (
    IndexStatus,
    PaperMetadata,
    Tier,
)
from paper_pipeline.domain.serialization import metadata_from_json, metadata_to_dict
from paper_pipeline.eligibility import EligibilityResult, check_formal_candidate
from paper_pipeline.indexing.index_models import (
    Bm25IndexEntry,
    IndexBuildResult,
    IndexFailure,
    IndexSkippedPaper,
    IndexablePaper,
    IndexedPaper,
    VectorIndexEntry,
    paper_key,
    utc_now_iso,
)
from paper_pipeline.indexing.index_repository import JsonIndexRepository
from paper_pipeline.runtime.single_entry import ensure_real_flow_50_caller
from paper_pipeline.storage.metadata_writer import MetadataWriter

VECTOR_STUB_MODEL = "local-hash-stub-v1"
VECTOR_STUB_DIMENSION = 0


@dataclass(slots=True)
class IndexDiscoveryResult:
    candidates: list[IndexablePaper] = field(default_factory=list)
    skipped: list[IndexSkippedPaper] = field(default_factory=list)
    failures: list[IndexFailure] = field(default_factory=list)


@dataclass(slots=True)
class IndexBuilder:
    library_dir: str | Path
    repository: JsonIndexRepository | None = None
    metadata_writer: MetadataWriter = field(default_factory=MetadataWriter)

    def build(self) -> IndexBuildResult:
        ensure_real_flow_50_caller(
            "IndexBuilder.build",
            allow_test_callers=True,
            allow_modules=("paper_pipeline.indexing.index_builder",),
        )
        run_id = f"index-{utc_now_iso()}"
        result = IndexBuildResult(run_id=run_id, started_at=utc_now_iso())
        discovery = self.discover()
        result.skipped.extend(discovery.skipped)
        result.failures.extend(discovery.failures)

        bm25_entries: list[Bm25IndexEntry] = []
        vector_entries: list[VectorIndexEntry] = []
        for paper in discovery.candidates:
            try:
                text = paper.md_path.read_text(encoding="utf-8")
                timestamp = utc_now_iso()
                bm25_entries.append(_build_bm25_entry(paper, text, timestamp))
                vector_entries.append(_build_vector_entry(paper, text, timestamp))
                paper.metadata.index_status = IndexStatus.INDEXED
                self._write_metadata_status(paper.metadata, paper.metadata_path)
                result.indexed.append(
                    IndexedPaper(
                        paper_id=paper.metadata.id,
                        accession_id=paper.metadata.id,
                        version=paper.metadata.version,
                        metadata_path=paper.metadata_path,
                        md_path=paper.md_path,
                    ),
                )
            except Exception as exc:  # pragma: no cover - defensive guard
                result.failures.append(
                    IndexFailure(
                        paper_id=paper.paper_key,
                        error=str(exc),
                        metadata_path=paper.metadata_path,
                        md_path=paper.md_path,
                    ),
                )
                self._mark_failed(paper.metadata, paper.metadata_path)

        result.finish()
        repository = self._repository()
        repository.write_indexes(bm25_entries=bm25_entries, vector_entries=vector_entries)
        repository.append_manifest(result)
        return result

    def discover(self) -> IndexDiscoveryResult:
        root = Path(self.library_dir)
        discovery = IndexDiscoveryResult()
        metadata_paths = _discover_metadata_paths(root)
        known_md_paths: set[Path] = set()

        for metadata_path in metadata_paths:
            metadata = _load_metadata(metadata_path)
            if metadata is None:
                discovery.failures.append(
                    IndexFailure(
                        paper_id=metadata_path.name.removesuffix(".metadata.json"),
                        error="metadata file is invalid",
                        metadata_path=metadata_path,
                    ),
                )
                continue

            md_path = _markdown_path_for_metadata(metadata_path)
            known_md_paths.add(md_path)

            if not md_path.exists():
                discovery.failures.append(
                    IndexFailure(
                        paper_id=paper_key(metadata),
                        error="markdown file is missing",
                        metadata_path=metadata_path,
                        md_path=md_path,
                    ),
                )
                self._mark_failed(metadata, metadata_path)
                continue

            eligibility = check_formal_candidate(
                metadata,
                metadata_path=metadata_path,
                markdown_path=md_path,
                allow_uploaded_sql=True,
            )
            if not eligibility.eligible:
                discovery.skipped.append(
                    IndexSkippedPaper(
                        paper_id=paper_key(metadata),
                        reasons=_skip_reasons(eligibility),
                        metadata_path=metadata_path,
                        md_path=md_path,
                    ),
                )
                continue

            discovery.candidates.append(
                IndexablePaper(metadata=metadata, metadata_path=metadata_path, md_path=md_path),
            )

        for md_path in sorted(path for path in root.rglob("*.md") if not _is_under_indexes(path, root)):
            if md_path in known_md_paths:
                continue
            discovery.skipped.append(
                IndexSkippedPaper(
                    paper_id=md_path.stem,
                    reasons=["metadata file is missing"],
                    md_path=md_path,
                ),
            )

        return discovery

    def _mark_failed(self, metadata: PaperMetadata, metadata_path: Path) -> None:
        metadata.index_status = IndexStatus.FAILED
        self._write_metadata_status(metadata, metadata_path)

    def _write_metadata_status(self, metadata: PaperMetadata, metadata_path: Path) -> None:
        try:
            self.metadata_writer.write_metadata(metadata, metadata_path)
        except ValueError:
            payload = metadata_to_dict(metadata)
            metadata_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
                encoding="utf-8",
            )

    def _repository(self) -> JsonIndexRepository:
        if self.repository is None:
            self.repository = JsonIndexRepository(self.library_dir)
        return self.repository


def build_formal_library_indexes(library_dir: str | Path) -> IndexBuildResult:
    ensure_real_flow_50_caller(
        "build_formal_library_indexes",
        allow_test_callers=True,
        allow_modules=("paper_pipeline.indexing.sync",),
    )
    return IndexBuilder(library_dir).build()


def _load_metadata(path: Path) -> PaperMetadata | None:
    try:
        return metadata_from_json(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError):
        return None


def _discover_metadata_paths(root: Path) -> list[Path]:
    paths: dict[Path, None] = {}
    for pattern in ("metadata.json", "*.metadata.json"):
        for path in root.rglob(pattern):
            if _is_under_indexes(path, root):
                continue
            paths[path] = None
    return sorted(paths)


def _markdown_path_for_metadata(metadata_path: Path) -> Path:
    if metadata_path.name == "metadata.json":
        return metadata_path.parent / f"{metadata_path.parent.name}.md"
    return metadata_path.with_name(metadata_path.name.removesuffix(".metadata.json") + ".md")


def _skip_reasons(eligibility: EligibilityResult) -> list[str]:
    reasons = list(eligibility.blocking_reasons)
    reasons.append(f"scope:{eligibility.scope.value}")
    if eligibility.disposition:
        reasons.append(f"disposition:{eligibility.disposition}")
    if eligibility.version is not None:
        reasons.append(f"version:{eligibility.version}")
    reasons.extend(f"warning:{warning}" for warning in eligibility.warnings)
    return reasons


def _build_bm25_entry(
    paper: IndexablePaper,
    text: str,
    timestamp: str,
) -> Bm25IndexEntry:
    metadata = paper.metadata
    terms = _extract_terms(
        [metadata.title, metadata.abstract, " ".join(metadata.keywords), text],
    )
    return Bm25IndexEntry(
        paper_id=metadata.id,
        accession_id=metadata.id,
        version=metadata.version,
        title=metadata.title,
        abstract=metadata.abstract,
        keywords=list(metadata.keywords),
        category=metadata.category,
        direction=metadata.direction,
        tier=_coerce_tier(metadata.tier).value,
        source_hash=metadata.source_hash,
        md_path=str(paper.md_path),
        metadata_path=str(paper.metadata_path),
        text_terms=terms,
        updated_at=timestamp,
    )


def _build_vector_entry(
    paper: IndexablePaper,
    text: str,
    timestamp: str,
) -> VectorIndexEntry:
    metadata = paper.metadata
    text_hash = hashlib.sha256(
        "\n".join([metadata.title, metadata.abstract, " ".join(metadata.keywords), text]).encode(
            "utf-8",
        ),
    ).hexdigest()
    return VectorIndexEntry(
        paper_id=metadata.id,
        accession_id=metadata.id,
        version=metadata.version,
        embedding_model=VECTOR_STUB_MODEL,
        embedding_dimension=VECTOR_STUB_DIMENSION,
        embedding_stub=True,
        text_hash=text_hash,
        updated_at=timestamp,
    )


def _extract_terms(parts: list[str]) -> list[str]:
    text = " ".join(part for part in parts if part).lower()
    terms = re.findall(r"[\w]+", text, flags=re.UNICODE)
    seen: set[str] = set()
    unique_terms: list[str] = []
    for term in terms:
        if term in seen:
            continue
        seen.add(term)
        unique_terms.append(term)
    return unique_terms


def _is_under_indexes(path: Path, root: Path) -> bool:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return False
    return bool(relative.parts) and relative.parts[0] == "indexes"


def _coerce_tier(value: Tier | str) -> Tier:
    if isinstance(value, Tier):
        return value
    return Tier(value)
