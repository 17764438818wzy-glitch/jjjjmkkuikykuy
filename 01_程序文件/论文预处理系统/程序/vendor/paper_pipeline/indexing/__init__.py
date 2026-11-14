"""Index construction and synchronization public API."""

from paper_pipeline.indexing.base import (
    Bm25Index,
    ChapterVectorIndex,
    HierarchyIndex,
    MetadataIndex,
    PaperVectorIndex,
)
from paper_pipeline.indexing.coordinator import IndexCoordinator
from paper_pipeline.indexing.in_memory_indexes import (
    InMemoryBm25Index,
    InMemoryChapterVectorIndex,
    InMemoryHierarchyIndex,
    InMemoryMetadataIndex,
    InMemoryPaperVectorIndex,
)
from paper_pipeline.indexing.index_builder import IndexBuilder, build_formal_library_indexes
from paper_pipeline.indexing.index_models import (
    Bm25IndexEntry,
    IndexBuildResult,
    IndexFailure,
    IndexSkippedPaper,
    IndexablePaper,
    IndexedPaper,
    VectorIndexEntry,
)
from paper_pipeline.indexing.index_repository import JsonIndexRepository
from paper_pipeline.indexing.sync import (
    metadata_requires_index_rebuild,
    sync_formal_library_indexes,
)

__all__ = [
    "Bm25Index",
    "Bm25IndexEntry",
    "ChapterVectorIndex",
    "HierarchyIndex",
    "InMemoryBm25Index",
    "InMemoryChapterVectorIndex",
    "InMemoryHierarchyIndex",
    "InMemoryMetadataIndex",
    "InMemoryPaperVectorIndex",
    "IndexBuildResult",
    "IndexBuilder",
    "IndexCoordinator",
    "IndexFailure",
    "IndexSkippedPaper",
    "IndexablePaper",
    "IndexedPaper",
    "JsonIndexRepository",
    "MetadataIndex",
    "PaperVectorIndex",
    "VectorIndexEntry",
    "build_formal_library_indexes",
    "metadata_requires_index_rebuild",
    "sync_formal_library_indexes",
]
