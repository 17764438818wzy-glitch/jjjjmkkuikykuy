"""Repository for complete output_cleaned process backups."""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path

from paper_pipeline.domain import PaperMetadata
from paper_pipeline.domain.serialization import metadata_to_dict
from paper_pipeline.pipeline.artifacts import PipelineArtifactBundle


@dataclass(slots=True)
class OutputCleanedRepository:
    output_cleaned_dir: str | Path

    def backup_run_artifacts(
        self,
        *,
        run_id: str,
        accession_id: str,
        final_markdown: str | Path,
        metadata: PaperMetadata,
        blocks_dir: str | Path | None = None,
        normalized_dir: str | Path | None = None,
        parsed_dir: str | Path | None = None,
        logs_dir: str | Path | None = None,
    ) -> PipelineArtifactBundle:
        source_final = Path(final_markdown)
        if not source_final.exists():
            raise FileNotFoundError(source_final)

        bundle = PipelineArtifactBundle.create(
            output_cleaned_dir=self.output_cleaned_dir,
            run_id=run_id,
            accession_id=accession_id,
        )
        if source_final.resolve() != bundle.final_markdown.resolve():
            shutil.copy2(source_final, bundle.final_markdown)
        bundle.metadata_json.write_text(
            json.dumps(
                metadata_to_dict(metadata),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        _copy_directory_contents(blocks_dir, bundle.blocks_dir)
        _copy_directory_contents(normalized_dir, bundle.normalized_dir)
        _copy_directory_contents(parsed_dir, bundle.parsed_dir)
        _copy_directory_contents(logs_dir, bundle.logs_dir)
        return bundle


def _copy_directory_contents(source: str | Path | None, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    if source is None:
        return
    source_path = Path(source)
    if not source_path.exists():
        return
    if source_path.resolve() == destination.resolve():
        return
    shutil.copytree(source_path, destination, dirs_exist_ok=True)
