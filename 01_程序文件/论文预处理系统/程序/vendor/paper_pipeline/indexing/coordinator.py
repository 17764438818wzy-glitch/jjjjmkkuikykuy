"""Coordinator facade for task 11 index synchronization."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from paper_pipeline.indexing.index_models import IndexBuildResult
from paper_pipeline.indexing.sync import sync_formal_library_indexes
from paper_pipeline.runtime.single_entry import ensure_real_flow_50_caller


@dataclass(slots=True)
class IndexCoordinator:
    library_dir: str | Path

    def sync(self) -> IndexBuildResult:
        ensure_real_flow_50_caller(
            "IndexSyncCoordinator.sync",
            allow_test_callers=True,
        )
        return sync_formal_library_indexes(self.library_dir)
