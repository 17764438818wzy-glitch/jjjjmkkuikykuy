"""Models shared by report generation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ReportGenerationContext:
    run_id: str
    input_dir: str | Path
    library_dir: str | Path
    output_cleaned_dir: str | Path
    generated_by: str = "paper_pipeline.reporting"

    @property
    def library_path(self) -> Path:
        return Path(self.library_dir)

    @property
    def run_dir(self) -> Path:
        return self.library_path / "runs" / self.run_id

    @property
    def reports_dir(self) -> Path:
        return self.run_dir / "reports"


@dataclass(frozen=True, slots=True)
class ReportGenerationResult:
    run_id: str
    reports_dir: Path
    human_report_path: Path
    machine_report_path: Path
    report_manifest_path: Path
    evidence_bundle_path: Path
    specialty_report_paths: list[Path]


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()
