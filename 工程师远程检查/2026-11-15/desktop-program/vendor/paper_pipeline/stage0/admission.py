"""Stage0 admission entry points for task 06."""

from __future__ import annotations

import zipfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from xml.etree import ElementTree

from paper_pipeline.domain import Stage0Decision
from paper_pipeline.ingestion.archive_extractor import (
    ArchiveExtractionResult,
)
from paper_pipeline.stage0.audit import (
    Stage0RejectedRecord,
    append_stage0_rejected_record,
)
from paper_pipeline.stage0.rules import evaluate_stage0_text
from paper_pipeline.storage.scanner import calculate_file_hash

_PREVIEW_BYTES = 128 * 1024


@dataclass(slots=True)
class Stage0Result:
    source_path: str
    source_hash: str
    decision: Stage0Decision
    reason: str
    confidence: float
    evidence: list[str] = field(default_factory=list)
    run_id: str = ""
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


def admit_file(
    source_path: str | Path,
    *,
    run_id: str = "",
    audit_dir: str | Path | None = None,
) -> Stage0Result:
    path = Path(source_path)
    source_hash = calculate_file_hash(path)
    text = _read_text_preview(path)
    rule_result = evaluate_stage0_text(source_name=path.name, text=text)
    result = Stage0Result(
        source_path=str(path),
        source_hash=source_hash,
        decision=rule_result.decision,
        reason=rule_result.reason,
        confidence=rule_result.confidence,
        evidence=rule_result.evidence,
        run_id=run_id,
    )
    if audit_dir is not None and result.decision == Stage0Decision.STAGE0_REJECT:
        append_stage0_rejected_record(
            Stage0RejectedRecord(
                run_id=run_id,
                source_path=result.source_path,
                source_hash=result.source_hash,
                decision=result.decision,
                reason=result.reason,
                confidence=result.confidence,
                evidence=result.evidence,
                created_at=result.created_at,
            ),
            audit_dir,
        )
    return result


def admit_extracted_members(
    extraction: ArchiveExtractionResult,
    *,
    run_id: str = "",
    audit_dir: str | Path | None = None,
) -> list[Stage0Result]:
    if not extraction.success:
        archive_path = Path(extraction.archive_path)
        source_hash = calculate_file_hash(archive_path) if archive_path.exists() else ""
        return [
            Stage0Result(
                source_path=extraction.archive_path,
                source_hash=source_hash,
                decision=Stage0Decision.PENDING,
                reason=extraction.error_reason,
                confidence=0.0,
                evidence=["archive_extraction_failed"],
                run_id=run_id or extraction.run_id,
            ),
        ]
    return [
        admit_file(
            member.source_path,
            run_id=run_id or extraction.run_id,
            audit_dir=audit_dir,
        )
        for member in extraction.members
    ]


def build_stage0_file_processor(
    *,
    run_id: str,
    work_dir: str | Path,
    audit_dir: str | Path | None = None,
):
    raise RuntimeError(
        "direct stage0 file processor execution is disabled; run through "
        "paper_pipeline.e2e.real_flow_50",
    )


def _read_text_preview(path: Path) -> str:
    if path.suffix.lower() == ".docx":
        docx_text = _read_docx_text_preview(path)
        if docx_text.strip():
            return docx_text
        return _read_binary_text_preview(path) or "docx_preview_unavailable"
    return _read_binary_text_preview(path)


def _read_binary_text_preview(path: Path) -> str:
    with path.open("rb") as file_obj:
        data = file_obj.read(_PREVIEW_BYTES)
    for encoding in ("utf-8-sig", "utf-8", "gb18030"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="ignore")


def _read_docx_text_preview(path: Path) -> str:
    try:
        with zipfile.ZipFile(path) as archive:
            with archive.open("word/document.xml") as document:
                document_xml = document.read()
    except (KeyError, OSError, RuntimeError, zipfile.BadZipFile):
        return ""

    try:
        root = ElementTree.fromstring(document_xml)
    except ElementTree.ParseError:
        return ""

    text_parts: list[str] = []
    for element in root.iter():
        local_name = element.tag.rsplit("}", maxsplit=1)[-1]
        if local_name == "t" and element.text:
            text_parts.append(element.text)
    return "\n".join(text_parts)
