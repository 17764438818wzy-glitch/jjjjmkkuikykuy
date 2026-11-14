"""Index synchronization helpers."""

from __future__ import annotations

from pathlib import Path

from paper_pipeline.domain import IndexStatus, PaperMetadata
from paper_pipeline.indexing.index_builder import build_formal_library_indexes
from paper_pipeline.indexing.index_models import IndexBuildResult
from paper_pipeline.runtime.single_entry import ensure_real_flow_50_caller


def metadata_requires_index_rebuild(
    metadata: PaperMetadata,
    *,
    index_update_required: bool = False,
) -> bool:
    if index_update_required:
        return True
    status = metadata.index_status
    if not isinstance(status, IndexStatus):
        status = IndexStatus(status)
    return status in {
        IndexStatus.NOT_INDEXED,
        IndexStatus.FAILED,
        IndexStatus.NEEDS_REBUILD,
    }


def sync_formal_library_indexes(library_dir: str | Path) -> IndexBuildResult:
    ensure_real_flow_50_caller(
        "sync_formal_library_indexes",
        allow_test_callers=True,
        allow_modules=("paper_pipeline.indexing.coordinator",),
    )
    return build_formal_library_indexes(library_dir)
