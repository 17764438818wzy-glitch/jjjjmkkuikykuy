"""Docling capability detection and parser backend."""

from __future__ import annotations

import importlib
import importlib.util
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

from paper_pipeline.pipeline.document_parse import (
    DocumentParseResult,
    DocumentParseStatus,
    ParsedBlock,
    ParserAttempt,
)
from paper_pipeline.pipeline.document_parsers import DocumentParseContext

DoclingAvailabilityStatus = Literal["found", "not-installed"]
ImportAvailable = Callable[[str], bool]


@dataclass(frozen=True, slots=True)
class DoclingAvailability:
    status: DoclingAvailabilityStatus
    message: str


class DoclingClient(Protocol):
    def convert_to_blocks(
        self,
        source_path: Path,
        raw_output_dir: Path,
    ) -> Sequence[object]:
        """Convert a document into raw block-like mappings."""


class DoclingDocumentConverterClient:
    def convert_to_blocks(
        self,
        source_path: Path,
        raw_output_dir: Path,
    ) -> Sequence[object]:
        raw_output_dir.mkdir(parents=True, exist_ok=True)
        converter_module = importlib.import_module("docling.document_converter")
        converter_class = getattr(converter_module, "DocumentConverter")
        result = converter_class().convert(str(source_path))
        document = getattr(result, "document", result)
        payload = _docling_document_to_payload(document)
        (raw_output_dir / "docling.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        blocks = payload.get("blocks", [])
        return blocks if isinstance(blocks, list) else []


@dataclass(slots=True)
class DoclingParserBackend:
    availability: DoclingAvailability
    client: DoclingClient | None = None
    parser_name: str = "docling"

    def parse(self, context: DocumentParseContext, role: str) -> DocumentParseResult:
        if self.availability.status != "found":
            return _pending_result(
                context=context,
                parser_name=self.parser_name,
                role=role,
                status="unavailable",
                reason_code="pending_parser_unavailable",
                reason_text=self.availability.message,
            )

        raw_output_dir = context.output_dir / "raw" / "docling"
        try:
            raw_blocks = (self.client or DoclingDocumentConverterClient()).convert_to_blocks(
                Path(context.normalized_path),
                raw_output_dir,
            )
            blocks = _parsed_blocks(raw_blocks, source_parser=self.parser_name)
        except Exception as exc:  # noqa: BLE001 - parser boundary reports pending.
            return _pending_result(
                context=context,
                parser_name=self.parser_name,
                role=role,
                status="failed",
                reason_code="pending_parser_failed",
                reason_text=f"Docling parse failed: {type(exc).__name__}: {exc}",
            )

        _write_raw_summary(raw_output_dir, raw_blocks)
        if not blocks:
            return _pending_result(
                context=context,
                parser_name=self.parser_name,
                role=role,
                status="output_invalid",
                reason_code="pending_parser_output_invalid",
                reason_text="Docling produced no usable text blocks.",
            )

        return DocumentParseResult(
            status=DocumentParseStatus.SUCCESS,
            source_path=context.source_path,
            source_hash=context.source_hash,
            normalized_path=context.normalized_path,
            normalized_format=context.normalized_format,
            selected_parser=self.parser_name,
            attempts=[
                ParserAttempt(parser=self.parser_name, role=role, status="success"),
            ],
            blocks=blocks,
            title=blocks[0].text,
            admission_evidence=_admission_evidence_from_blocks(blocks),
        )


def resolve_docling_availability(
    *,
    import_available: ImportAvailable | None = None,
) -> DoclingAvailability:
    available = import_available or _default_import_available
    try:
        found = available("docling.document_converter")
    except ModuleNotFoundError:
        found = False
    if found:
        return DoclingAvailability(
            status="found",
            message="Docling Python API is importable.",
        )
    return DoclingAvailability(
        status="not-installed",
        message=(
            "Docling Python API is not installed; .docx primary parsing and "
            ".pdf Docling fallback will enter pending when required."
        ),
    )


def _pending_result(
    *,
    context: DocumentParseContext,
    parser_name: str,
    role: str,
    status: str,
    reason_code: str,
    reason_text: str,
) -> DocumentParseResult:
    return DocumentParseResult(
        status=DocumentParseStatus.PENDING,
        source_path=context.source_path,
        source_hash=context.source_hash,
        normalized_path=context.normalized_path,
        normalized_format=context.normalized_format,
        attempts=[
            ParserAttempt(
                parser=parser_name,
                role=role,
                status=status,
                error_summary=reason_text,
            ),
        ],
        pending_reason_code=reason_code,
        pending_reason_text=reason_text,
    )


def _parsed_blocks(
    raw_blocks: Sequence[object],
    *,
    source_parser: str,
) -> list[ParsedBlock]:
    blocks: list[ParsedBlock] = []
    for index, item in enumerate(raw_blocks, start=1):
        if not isinstance(item, Mapping):
            continue
        text = str(item.get("text", "")).strip()
        if not text:
            continue
        blocks.append(
            ParsedBlock(
                block_id=f"b{index:06d}",
                type=str(item.get("type") or "paragraph"),
                text=text,
                order=index,
                level=_optional_int(item.get("level")),
                page_start=_optional_int(item.get("page_start")),
                page_end=_optional_int(item.get("page_end")),
                source_parser=source_parser,
                confidence=_optional_float(item.get("confidence")),
            ),
        )
    return blocks


def _write_raw_summary(
    raw_output_dir: Path,
    raw_blocks: Sequence[object],
) -> None:
    raw_output_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "blocks": [
            dict(block) if isinstance(block, Mapping) else str(block)
            for block in raw_blocks
        ],
    }
    (raw_output_dir / "docling.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _docling_document_to_payload(document: object) -> dict[str, object]:
    export_to_dict = getattr(document, "export_to_dict", None)
    if callable(export_to_dict):
        decoded = export_to_dict()
        if isinstance(decoded, dict):
            return {"raw": decoded, "blocks": _blocks_from_docling_export(decoded)}

    export_to_markdown = getattr(document, "export_to_markdown", None)
    if callable(export_to_markdown):
        markdown = str(export_to_markdown())
        return {"markdown": markdown, "blocks": _blocks_from_text(markdown)}

    return {"text": str(document), "blocks": _blocks_from_text(str(document))}


def _blocks_from_docling_export(decoded: Mapping[str, object]) -> list[dict[str, object]]:
    texts = decoded.get("texts", [])
    if isinstance(texts, list):
        blocks = [_block_from_mapping(item) for item in texts if isinstance(item, Mapping)]
        return [block for block in blocks if block is not None]

    body = decoded.get("body")
    if isinstance(body, Mapping):
        children = body.get("children", [])
        if isinstance(children, list):
            blocks = [_block_from_mapping(item) for item in children if isinstance(item, Mapping)]
            return [block for block in blocks if block is not None]
    return _blocks_from_text(json.dumps(decoded, ensure_ascii=False))


def _block_from_mapping(item: Mapping[str, object]) -> dict[str, object] | None:
    text = str(item.get("text") or item.get("content") or "").strip()
    if not text:
        return None
    label = str(item.get("label") or item.get("type") or "paragraph").lower()
    block_type = "heading" if "heading" in label or "title" in label else "paragraph"
    return {"type": block_type, "text": text}


def _blocks_from_text(text: str) -> list[dict[str, object]]:
    return [
        {"type": "heading" if _looks_like_heading(line) else "paragraph", "text": line.strip()}
        for line in text.splitlines()
        if line.strip()
    ]


def _admission_evidence_from_blocks(blocks: list[ParsedBlock]) -> dict[str, object]:
    headings = [block.text for block in blocks if block.type == "heading"]
    return {
        "title_candidates": [blocks[0].text] if blocks else [],
        "first_pages_text": "\n".join(block.text for block in blocks[:20]),
        "heading_candidates": headings[:50],
        "abstract_candidates": [block.text for block in blocks if "摘要" in block.text][:3],
        "reference_signal": any("参考文献" in block.text for block in blocks),
        "journal_signal": any("doi" in block.text.lower() for block in blocks),
        "literature_review_signal": any("文献综述" in block.text for block in blocks),
    }


def _looks_like_heading(line: str) -> bool:
    candidate = line.strip()
    return candidate.startswith(("第", "摘要", "关键词", "结论", "参考文献", "#"))


def _optional_int(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if not isinstance(value, int | float | str | bytes | bytearray):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_float(value: object) -> float:
    if value is None or isinstance(value, bool):
        return 0.0
    if not isinstance(value, int | float | str | bytes | bytearray):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _default_import_available(module_name: str) -> bool:
    try:
        return importlib.util.find_spec(module_name) is not None
    except ModuleNotFoundError:
        return False
