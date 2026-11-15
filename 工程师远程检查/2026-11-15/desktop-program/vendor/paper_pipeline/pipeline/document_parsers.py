"""Parser routing for stage2_document_parse."""

from __future__ import annotations

import json
import zipfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Any, Literal, Protocol
from xml.etree import ElementTree as ET

from paper_pipeline.pipeline.document_parse import (
    DocumentParseResult,
    DocumentParseStatus,
    ParsedBlock,
    ParserAttempt,
)
from paper_pipeline.pipeline.libreoffice import build_libreoffice_conversion_command
from paper_pipeline.pipeline.mineru import (
    build_mineru_parse_command,
    materialize_mineru_json,
)


@dataclass(frozen=True, slots=True)
class DocumentParseContext:
    source_path: str
    source_hash: str
    normalized_path: str
    normalized_format: str
    output_dir: Path


@dataclass(frozen=True, slots=True)
class ParserRoute:
    primary: str
    fallback: str | None
    extra_fallbacks: tuple[str, ...] = ()


class ParserBackend(Protocol):
    parser_name: str

    def parse(self, context: DocumentParseContext, role: str) -> DocumentParseResult:
        """Return a standard document parse result."""


class ParserCommandRunner(Protocol):
    def run(
        self,
        command: list[str],
        *,
        cwd: Path | None = None,
        timeout_seconds: int,
    ) -> Any:
        """Run an external parser/converter command without a shell."""


@dataclass(slots=True)
class FakeParserBackend:
    parser_name: str
    status: Literal["success", "failed"] = "success"

    def parse(self, context: DocumentParseContext, role: str) -> DocumentParseResult:
        attempt = ParserAttempt(parser=self.parser_name, role=role, status=self.status)
        if self.status == "success":
            return DocumentParseResult(
                status=DocumentParseStatus.SUCCESS,
                source_path=context.source_path,
                source_hash=context.source_hash,
                normalized_path=context.normalized_path,
                normalized_format=context.normalized_format,
                selected_parser=self.parser_name,
                attempts=[attempt],
                blocks=[
                    ParsedBlock(
                        block_id="b000001",
                        type="paragraph",
                        text="fake parsed text",
                        order=1,
                        source_parser=self.parser_name,
                        confidence=1.0,
                    ),
                ],
            )
        return DocumentParseResult(
            status=DocumentParseStatus.PENDING,
            source_path=context.source_path,
            source_hash=context.source_hash,
            normalized_path=context.normalized_path,
            normalized_format=context.normalized_format,
            attempts=[attempt],
            pending_reason_code="pending_parser_failed",
            pending_reason_text=f"{self.parser_name} failed in fake backend",
        )


@dataclass(slots=True)
class MinerUParserBackend:
    mineru_path: Path | None
    command_runner: ParserCommandRunner
    timeout_seconds: int = 3600
    parser_name: str = "mineru"

    def parse(self, context: DocumentParseContext, role: str) -> DocumentParseResult:
        if self.mineru_path is None:
            return _pending_result(
                context=context,
                parser_name=self.parser_name,
                role=role,
                status="unavailable",
                reason_code="pending_parser_unavailable",
                reason_text="MinerU executable is not configured.",
            )

        raw_output_dir = context.output_dir / "raw" / "mineru"
        output_path = raw_output_dir / "mineru.json"
        command = build_mineru_parse_command(
            self.mineru_path,
            context.normalized_path,
            raw_output_dir,
        )
        try:
            command_result = self.command_runner.run(
                command,
                timeout_seconds=self.timeout_seconds,
            )
        except TimeoutError:
            return _pending_result(
                context=context,
                parser_name=self.parser_name,
                role=role,
                status="timeout",
                reason_code="pending_parser_timeout",
                reason_text="MinerU parse timed out.",
            )
        except Exception as exc:  # noqa: BLE001 - parser boundary reports pending.
            return _pending_result(
                context=context,
                parser_name=self.parser_name,
                role=role,
                status="failed",
                reason_code="pending_parser_failed",
                reason_text=f"MinerU parse failed: {type(exc).__name__}: {exc}",
            )

        if command_result.returncode != 0:
            return _pending_result(
                context=context,
                parser_name=self.parser_name,
                role=role,
                status="failed",
                reason_code="pending_parser_failed",
                reason_text=_command_error("MinerU parse failed", command_result),
            )

        if not materialize_mineru_json(
            output_dir=raw_output_dir,
            output_path=output_path,
            stdout=command_result.stdout,
        ):
            return _pending_result(
                context=context,
                parser_name=self.parser_name,
                role=role,
                status="output_invalid",
                reason_code="pending_parser_output_invalid",
                reason_text="MinerU did not produce readable JSON output.",
            )

        payload = _load_json_object(output_path)
        blocks = _blocks_from_mineru_payload(payload, source_parser=self.parser_name)
        if not blocks:
            return _pending_result(
                context=context,
                parser_name=self.parser_name,
                role=role,
                status="output_invalid",
                reason_code="pending_parser_output_invalid",
                reason_text="MinerU output contained no usable text blocks.",
            )

        return DocumentParseResult(
            status=DocumentParseStatus.SUCCESS,
            source_path=context.source_path,
            source_hash=context.source_hash,
            normalized_path=context.normalized_path,
            normalized_format=context.normalized_format,
            selected_parser=self.parser_name,
            attempts=[ParserAttempt(parser=self.parser_name, role=role, status="success")],
            blocks=blocks,
            title=blocks[0].text,
            admission_evidence=_admission_evidence_from_blocks(blocks),
        )


@dataclass(slots=True)
class DocxToPdfMinerUBackend:
    libreoffice_path: Path | None
    mineru_backend: ParserBackend
    command_runner: ParserCommandRunner
    timeout_seconds: int = 600
    parser_name: str = "docx_to_pdf_mineru"

    def parse(self, context: DocumentParseContext, role: str) -> DocumentParseResult:
        return _convert_then_parse(
            context=context,
            role=role,
            parser_name=self.parser_name,
            libreoffice_path=self.libreoffice_path,
            command_runner=self.command_runner,
            timeout_seconds=self.timeout_seconds,
            target_format="pdf",
            next_format="pdf",
            next_backend=self.mineru_backend,
        )


@dataclass(slots=True)
class DocToDocxDoclingBackend:
    libreoffice_path: Path | None
    docling_backend: ParserBackend
    command_runner: ParserCommandRunner
    timeout_seconds: int = 600
    parser_name: str = "doc_to_docx_docling"

    def parse(self, context: DocumentParseContext, role: str) -> DocumentParseResult:
        return _convert_then_parse(
            context=context,
            role=role,
            parser_name=self.parser_name,
            libreoffice_path=self.libreoffice_path,
            command_runner=self.command_runner,
            timeout_seconds=self.timeout_seconds,
            target_format="docx",
            next_format="docx",
            next_backend=self.docling_backend,
        )


@dataclass(slots=True)
class DocToPdfMinerUBackend:
    libreoffice_path: Path | None
    mineru_backend: ParserBackend
    command_runner: ParserCommandRunner
    timeout_seconds: int = 600
    parser_name: str = "doc_to_pdf_mineru"

    def parse(self, context: DocumentParseContext, role: str) -> DocumentParseResult:
        return _convert_then_parse(
            context=context,
            role=role,
            parser_name=self.parser_name,
            libreoffice_path=self.libreoffice_path,
            command_runner=self.command_runner,
            timeout_seconds=self.timeout_seconds,
            target_format="pdf",
            next_format="pdf",
            next_backend=self.mineru_backend,
        )


@dataclass(slots=True)
class DocxXmlParserBackend:
    parser_name: str = "docx_xml"

    def parse(self, context: DocumentParseContext, role: str) -> DocumentParseResult:
        path = Path(context.normalized_path)
        try:
            with zipfile.ZipFile(path) as archive:
                try:
                    raw_document_xml = archive.read(_DOCX_DOCUMENT_XML_PATH)
                except KeyError:
                    return _pending_result(
                        context=context,
                        parser_name=self.parser_name,
                        role=role,
                        status="output_invalid",
                        reason_code="pending_docx_document_xml_missing",
                        reason_text="word/document.xml is missing from the .docx archive.",
                    )
        except zipfile.BadZipFile:
            return _pending_result(
                context=context,
                parser_name=self.parser_name,
                role=role,
                status="output_invalid",
                reason_code="pending_docx_zip_invalid",
                reason_text=".docx file is not a readable zip archive.",
            )
        except OSError as exc:
            return _pending_result(
                context=context,
                parser_name=self.parser_name,
                role=role,
                status="failed",
                reason_code="pending_docx_xml_read_failed",
                reason_text=f"Failed to read .docx archive: {type(exc).__name__}: {exc}",
            )

        try:
            blocks = _blocks_from_docx_document_xml(
                raw_document_xml,
                source_parser=self.parser_name,
            )
        except ET.ParseError as exc:
            return _pending_result(
                context=context,
                parser_name=self.parser_name,
                role=role,
                status="output_invalid",
                reason_code="pending_docx_document_xml_invalid",
                reason_text=f"word/document.xml is not valid XML: {exc}",
            )

        if not blocks:
            return _pending_result(
                context=context,
                parser_name=self.parser_name,
                role=role,
                status="output_invalid",
                reason_code="pending_docx_text_empty",
                reason_text="word/document.xml contains no usable <w:t> text.",
            )

        return DocumentParseResult(
            status=DocumentParseStatus.SUCCESS,
            source_path=context.source_path,
            source_hash=context.source_hash,
            normalized_path=context.normalized_path,
            normalized_format=context.normalized_format,
            selected_parser=self.parser_name,
            attempts=[ParserAttempt(parser=self.parser_name, role=role, status="success")],
            blocks=blocks,
            title=blocks[0].text,
            admission_evidence=_admission_evidence_from_blocks(blocks),
        )


def route_for_normalized_format(normalized_format: str) -> ParserRoute:
    routes = {
        "docx": ParserRoute(
            primary="docling",
            fallback="docx_to_pdf_mineru",
            extra_fallbacks=("docx_xml",),
        ),
        "doc": ParserRoute(primary="doc_to_docx_docling", fallback="doc_to_pdf_mineru"),
        "pdf": ParserRoute(primary="mineru", fallback="docling"),
        "text": ParserRoute(primary="local_text", fallback=None),
        "txt": ParserRoute(primary="local_text", fallback=None),
        "md": ParserRoute(primary="local_text", fallback=None),
    }
    normalized = normalized_format.strip().lower().lstrip(".")
    return routes.get(normalized, ParserRoute(primary="unsupported", fallback=None))


def run_document_parse_route(
    context: DocumentParseContext,
    parser_backends: dict[str, ParserBackend] | None = None,
) -> DocumentParseResult:
    backends: dict[str, ParserBackend] = {
        "local_text": LocalTextParserBackend(),
        "docx_xml": DocxXmlParserBackend(),
    }
    backends.update(parser_backends or {})
    route = route_for_normalized_format(context.normalized_format)
    attempts: list[ParserAttempt] = []
    last_pending_result: DocumentParseResult | None = None

    for role, parser_name in _route_steps(route):
        if parser_name is None:
            continue
        backend = backends.get(parser_name)
        if backend is None:
            attempts.append(
                ParserAttempt(
                    parser=parser_name,
                    role=role,
                    status="unavailable",
                    error_summary=f"{parser_name} backend is not available",
                ),
            )
            continue

        start = perf_counter()
        result = backend.parse(context, role)
        elapsed = perf_counter() - start
        attempts.extend(_attempts_with_elapsed(result.attempts, elapsed))
        if result.status is DocumentParseStatus.SUCCESS:
            return DocumentParseResult(
                status=DocumentParseStatus.SUCCESS,
                source_path=result.source_path,
                source_hash=result.source_hash,
                normalized_path=result.normalized_path,
                normalized_format=result.normalized_format,
                selected_parser=result.selected_parser,
                attempts=attempts,
                blocks=result.blocks,
                title=result.title,
                page_count=result.page_count,
                language=result.language,
                admission_evidence=result.admission_evidence,
                parse_warnings=result.parse_warnings,
            )
        last_pending_result = result

    pending_reason_code = (
        "pending_unsupported_format"
        if route.primary == "unsupported"
        else _route_pending_reason_code(route, last_pending_result)
    )
    pending_reason_text = (
        f"stage2 route failed for normalized_format={context.normalized_format}"
        if route.fallback is not None and not route.extra_fallbacks
        else _route_pending_reason_text(context, last_pending_result)
    )
    return DocumentParseResult(
        status=DocumentParseStatus.PENDING,
        source_path=context.source_path,
        source_hash=context.source_hash,
        normalized_path=context.normalized_path,
        normalized_format=context.normalized_format,
        attempts=attempts,
        pending_reason_code=pending_reason_code,
        pending_reason_text=pending_reason_text,
    )


class LocalTextParserBackend:
    parser_name = "local_text"

    def parse(self, context: DocumentParseContext, role: str) -> DocumentParseResult:
        path = Path(context.normalized_path)
        text = path.read_text(encoding="utf-8")
        blocks = [
            ParsedBlock(
                block_id=f"b{index:06d}",
                type="heading" if _looks_like_heading(line) else "paragraph",
                text=line.strip(),
                order=index,
                level=1 if _looks_like_heading(line) else None,
                source_parser=self.parser_name,
                confidence=1.0,
            )
            for index, line in enumerate(
                [line for line in text.splitlines() if line.strip()],
                start=1,
            )
        ]
        if not blocks:
            return _pending_result(
                context=context,
                parser_name=self.parser_name,
                role=role,
                status="output_invalid",
                reason_code="pending_parser_output_invalid",
                reason_text="local_text parser produced no usable text blocks.",
            )
        return DocumentParseResult(
            status=DocumentParseStatus.SUCCESS,
            source_path=context.source_path,
            source_hash=context.source_hash,
            normalized_path=context.normalized_path,
            normalized_format=context.normalized_format,
            selected_parser=self.parser_name,
            attempts=[ParserAttempt(parser=self.parser_name, role=role, status="success")],
            blocks=blocks,
            title=blocks[0].text if blocks else "",
            admission_evidence=_admission_evidence_from_blocks(blocks),
        )


def _route_steps(route: ParserRoute) -> tuple[tuple[str, str | None], ...]:
    steps: list[tuple[str, str | None]] = [("primary", route.primary)]
    if route.fallback is not None:
        steps.append(("fallback", route.fallback))
    steps.extend(
        (f"fallback_{index}", parser_name)
        for index, parser_name in enumerate(route.extra_fallbacks, start=2)
    )
    return tuple(steps)


def _attempts_with_elapsed(
    attempts: list[ParserAttempt],
    elapsed: float,
) -> list[ParserAttempt]:
    return [
        ParserAttempt(
            parser=attempt.parser,
            role=attempt.role,
            status=attempt.status,
            duration_seconds=attempt.duration_seconds or elapsed,
            log_path=attempt.log_path,
            error_summary=attempt.error_summary,
        )
        for attempt in attempts
    ]


def _route_pending_reason_code(
    route: ParserRoute,
    result: DocumentParseResult | None,
) -> str:
    if (route.fallback is None or route.extra_fallbacks) and result is not None and result.pending_reason_code:
        return result.pending_reason_code
    return "pending_parse_failed"


def _route_pending_reason_text(
    context: DocumentParseContext,
    result: DocumentParseResult | None,
) -> str:
    if result is not None and result.pending_reason_text:
        return result.pending_reason_text
    return f"stage2 route failed for normalized_format={context.normalized_format}"


def _convert_then_parse(
    *,
    context: DocumentParseContext,
    role: str,
    parser_name: str,
    libreoffice_path: Path | None,
    command_runner: ParserCommandRunner,
    timeout_seconds: int,
    target_format: str,
    next_format: str,
    next_backend: ParserBackend,
) -> DocumentParseResult:
    if libreoffice_path is None:
        return _pending_result(
            context=context,
            parser_name=parser_name,
            role=role,
            status="unavailable",
            reason_code="pending_converter_unavailable",
            reason_text="LibreOffice executable is not configured.",
        )

    output_dir = context.output_dir / "converted" / parser_name
    output_dir.mkdir(parents=True, exist_ok=True)
    command = build_libreoffice_conversion_command(
        libreoffice_path,
        context.normalized_path,
        output_dir,
        target_format=target_format,
    )
    try:
        command_result = command_runner.run(
            command,
            timeout_seconds=timeout_seconds,
        )
    except TimeoutError:
        return _pending_result(
            context=context,
            parser_name=parser_name,
            role=role,
            status="timeout",
            reason_code="pending_convert_failed",
            reason_text=f"LibreOffice conversion to {target_format} timed out.",
        )
    except Exception as exc:  # noqa: BLE001 - converter boundary reports pending.
        return _pending_result(
            context=context,
            parser_name=parser_name,
            role=role,
            status="failed",
            reason_code="pending_convert_failed",
            reason_text=f"LibreOffice conversion failed: {type(exc).__name__}: {exc}",
        )

    if command_result.returncode != 0:
        return _pending_result(
            context=context,
            parser_name=parser_name,
            role=role,
            status="failed",
            reason_code="pending_convert_failed",
            reason_text=_command_error("LibreOffice conversion failed", command_result),
        )

    converted_path = _converted_path(
        output_dir=output_dir,
        source_path=Path(context.normalized_path),
        target_format=target_format,
    )
    if not converted_path.exists():
        return _pending_result(
            context=context,
            parser_name=parser_name,
            role=role,
            status="failed",
            reason_code="pending_convert_failed",
            reason_text=f"LibreOffice did not create expected {target_format} output.",
        )

    next_context = DocumentParseContext(
        source_path=context.source_path,
        source_hash=context.source_hash,
        normalized_path=str(converted_path),
        normalized_format=next_format,
        output_dir=context.output_dir,
    )
    next_result = next_backend.parse(next_context, role)
    conversion_attempt = ParserAttempt(
        parser=parser_name,
        role=role,
        status="success",
        log_path=str(converted_path),
    )
    return _with_prefixed_attempt(next_result, conversion_attempt)


def _with_prefixed_attempt(
    result: DocumentParseResult,
    attempt: ParserAttempt,
) -> DocumentParseResult:
    attempts = [attempt, *result.attempts]
    if result.status is DocumentParseStatus.SUCCESS:
        return DocumentParseResult(
            status=DocumentParseStatus.SUCCESS,
            source_path=result.source_path,
            source_hash=result.source_hash,
            normalized_path=result.normalized_path,
            normalized_format=result.normalized_format,
            selected_parser=result.selected_parser,
            attempts=attempts,
            blocks=result.blocks,
            title=result.title,
            page_count=result.page_count,
            language=result.language,
            admission_evidence=result.admission_evidence,
            parse_warnings=result.parse_warnings,
        )
    return DocumentParseResult(
        status=DocumentParseStatus.PENDING,
        source_path=result.source_path,
        source_hash=result.source_hash,
        normalized_path=result.normalized_path,
        normalized_format=result.normalized_format,
        attempts=attempts,
        pending_reason_code=result.pending_reason_code,
        pending_reason_text=result.pending_reason_text,
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


def _converted_path(
    *,
    output_dir: Path,
    source_path: Path,
    target_format: str,
) -> Path:
    suffix = target_format.split(":", maxsplit=1)[0].strip().lstrip(".")
    return output_dir / f"{source_path.stem}.{suffix}"


def _load_json_object(path: Path) -> dict[str, object]:
    decoded = json.loads(path.read_text(encoding="utf-8"))
    return decoded if isinstance(decoded, dict) else {}


def _blocks_from_mineru_payload(
    payload: Mapping[str, object],
    *,
    source_parser: str,
) -> list[ParsedBlock]:
    raw_blocks = payload.get("blocks", [])
    if isinstance(raw_blocks, list):
        blocks = [
            _parsed_block_from_mapping(item, index=index, source_parser=source_parser)
            for index, item in enumerate(raw_blocks, start=1)
            if isinstance(item, Mapping)
        ]
        parsed = [block for block in blocks if block is not None]
        if parsed:
            return parsed

    text = str(payload.get("text") or payload.get("content") or "").strip()
    if text:
        return [
            ParsedBlock(
                block_id=f"b{index:06d}",
                type="paragraph",
                text=line.strip(),
                order=index,
                source_parser=source_parser,
                confidence=1.0,
            )
            for index, line in enumerate(text.splitlines(), start=1)
            if line.strip()
        ]
    return []


def _parsed_block_from_mapping(
    item: Mapping[str, object],
    *,
    index: int,
    source_parser: str,
) -> ParsedBlock | None:
    text = str(item.get("text") or item.get("content") or "").strip()
    if not text:
        return None
    return ParsedBlock(
        block_id=f"b{index:06d}",
        type=str(item.get("type") or "paragraph"),
        text=text,
        order=index,
        source_parser=source_parser,
        confidence=1.0,
    )


_DOCX_DOCUMENT_XML_PATH = "word/document.xml"
_WORDPROCESSINGML_NAMESPACE = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"


def _blocks_from_docx_document_xml(
    raw_document_xml: bytes,
    *,
    source_parser: str,
) -> list[ParsedBlock]:
    root = ET.fromstring(raw_document_xml)
    paragraphs = _docx_paragraph_texts(root)
    return [
        ParsedBlock(
            block_id=f"b{index:06d}",
            type="heading" if _looks_like_heading(text) else "paragraph",
            text=text,
            order=index,
            level=1 if _looks_like_heading(text) else None,
            source_parser=source_parser,
            confidence=0.9,
        )
        for index, text in enumerate(paragraphs, start=1)
    ]


def _docx_paragraph_texts(root: ET.Element[str]) -> list[str]:
    paragraph_tag = f"{_WORDPROCESSINGML_NAMESPACE}p"
    text_tag = f"{_WORDPROCESSINGML_NAMESPACE}t"
    paragraphs: list[str] = []
    for paragraph in root.iter(paragraph_tag):
        text = "".join(node.text or "" for node in paragraph.iter(text_tag)).strip()
        if text:
            paragraphs.append(text)
    if paragraphs:
        return paragraphs
    all_text = "\n".join(
        text.strip()
        for text in (node.text or "" for node in root.iter(text_tag))
        if text.strip()
    )
    return [all_text] if all_text else []


def _command_error(prefix: str, result: Any) -> str:
    detail = result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}"
    return f"{prefix}: {detail}"


def _looks_like_heading(line: str) -> bool:
    candidate = line.strip()
    return candidate.startswith(("第", "摘要", "关键词", "结论", "参考文献"))


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
