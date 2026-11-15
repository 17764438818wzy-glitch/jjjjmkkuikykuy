"""Safe output_cleaned artifact helpers for the local Dity pipeline."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class PipelineArtifactBundle:
    base_dir: Path
    final_markdown: Path
    metadata_json: Path
    blocks_dir: Path
    normalized_dir: Path
    parsed_dir: Path
    parsed_raw_dir: Path
    logs_dir: Path

    @classmethod
    def create(
        cls,
        *,
        output_cleaned_dir: str | Path,
        run_id: str,
        accession_id: str,
    ) -> PipelineArtifactBundle:
        base_dir = Path(output_cleaned_dir) / "runs" / run_id / accession_id
        bundle = cls(
            base_dir=base_dir,
            final_markdown=base_dir / "final.md",
            metadata_json=base_dir / "metadata.json",
            blocks_dir=base_dir / "blocks",
            normalized_dir=base_dir / "normalized",
            parsed_dir=base_dir / "parsed",
            parsed_raw_dir=base_dir / "parsed" / "raw",
            logs_dir=base_dir / "logs",
        )
        bundle.ensure_directories()
        return bundle

    def ensure_directories(self) -> None:
        for path in (
            self.base_dir,
            self.blocks_dir,
            self.normalized_dir,
            self.parsed_dir,
            self.parsed_raw_dir,
            self.parsed_raw_dir / "mineru",
            self.parsed_raw_dir / "docling",
            self.parsed_raw_dir / "local_text",
            self.logs_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)


def make_accession_id(*, source_hash: str, source_path: str, version: int = 1) -> str:
    seed = source_hash or source_path
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12].upper()
    return f"ACC_{digest}_v{version}"
