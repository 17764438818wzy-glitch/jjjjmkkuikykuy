"""Storage scanning, ledger, archive, and backup utilities."""

from typing import TYPE_CHECKING

from paper_pipeline.storage.id_allocator import (
    FormalPaperIdAllocator,
    formal_stem,
    format_formal_markdown_name,
    format_formal_metadata_name,
)
from paper_pipeline.storage.hash_ledger import (
    HashLedger,
    load_hash_ledger,
    save_hash_ledger,
)
from paper_pipeline.storage.library_repository import (
    LibraryArchiveResult,
    LibraryRepository,
)
from paper_pipeline.storage.metadata_writer import (
    MetadataWriter,
    validate_formal_metadata,
)
from paper_pipeline.storage.scanner import (
    DUPLICATE_SKIP_REASON,
    ScanResult,
    ScanSummary,
    calculate_file_hash,
    format_scan_summary,
    scan_input_raw,
)
from paper_pipeline.storage.tier_mover import TierMover, can_change_tier

if TYPE_CHECKING:
    from paper_pipeline.storage.output_cleaned_repository import OutputCleanedRepository

__all__ = [
    "DUPLICATE_SKIP_REASON",
    "FormalPaperIdAllocator",
    "HashLedger",
    "LibraryArchiveResult",
    "LibraryRepository",
    "MetadataWriter",
    "OutputCleanedRepository",
    "ScanResult",
    "ScanSummary",
    "TierMover",
    "calculate_file_hash",
    "can_change_tier",
    "formal_stem",
    "format_scan_summary",
    "format_formal_markdown_name",
    "format_formal_metadata_name",
    "load_hash_ledger",
    "save_hash_ledger",
    "scan_input_raw",
    "validate_formal_metadata",
]


def __getattr__(name: str) -> object:
    if name == "OutputCleanedRepository":
        from paper_pipeline.storage.output_cleaned_repository import (
            OutputCleanedRepository,
        )

        return OutputCleanedRepository
    raise AttributeError(name)
