"""Standard stage2 document parse schema."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any

SCHEMA_VERSION = "stage2_document_parse.v1"
STAGE_ID = "stage2_document_parse"
CODEX_NEXT_ACTION_REVIEW_STAGE2_PARSE_FAILURE = "review_stage2_parse_failure"


class DocumentParseStatus(StrEnum):
    SUCCESS = "success"
    PENDING = "pending"


@dataclass(frozen=True, slots=True)
class ParserAttempt:
    parser: str
    role: str
    status: str
    duration_seconds: float = 0.0
    log_path: str = ""
    error_summary: str = ""

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ParsedBlock:
    block_id: str
    type: str
    text: str
    order: int
    level: int | None = None
    page_start: int | None = None
    page_end: int | None = None
    source_parser: str = ""
    confidence: float = 0.0
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        return {
            key: value
            for key, value in payload.items()
            if value not in (None, [], "")
        }


@dataclass(frozen=True, slots=True)
class DocumentParseResult:
    status: DocumentParseStatus
    source_path: str
    source_hash: str
    normalized_path: str
    normalized_format: str
    selected_parser: str = ""
    attempts: list[ParserAttempt] = field(default_factory=list)
    blocks: list[ParsedBlock] = field(default_factory=list)
    title: str = ""
    page_count: int | None = None
    language: str = "zh"
    admission_evidence: dict[str, object] = field(default_factory=dict)
    parse_warnings: list[str] = field(default_factory=list)
    pending_reason_code: str = ""
    pending_reason_text: str = ""
    codex_next_action_hint: str = ""

    def to_dict(self) -> dict[str, object]:
        codex_next_action_hint = self.codex_next_action_hint
        if self.status is DocumentParseStatus.PENDING and not codex_next_action_hint:
            codex_next_action_hint = CODEX_NEXT_ACTION_REVIEW_STAGE2_PARSE_FAILURE
        payload: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "stage_id": STAGE_ID,
            "status": self.status.value,
            "source_path": self.source_path,
            "source_hash": self.source_hash,
            "normalized_path": self.normalized_path,
            "normalized_format": self.normalized_format,
            "selected_parser": self.selected_parser,
            "attempts": [attempt.to_dict() for attempt in self.attempts],
            "document": {
                "title": self.title,
                "page_count": self.page_count,
                "language": self.language,
            },
            "admission_evidence": self.admission_evidence,
            "blocks": [block.to_dict() for block in self.blocks],
            "parse_quality_metrics": parse_quality_metrics(
                self.blocks,
                self.parse_warnings,
            ),
            "parse_warnings": self.parse_warnings,
            "pending_reason_code": self.pending_reason_code,
            "pending_reason_text": self.pending_reason_text,
            "codex_next_action_hint": codex_next_action_hint,
        }
        validate_document_parse_payload(payload)
        return payload


def parse_quality_metrics(
    blocks: list[ParsedBlock],
    warnings: list[str],
) -> dict[str, object]:
    return {
        "block_count": len(blocks),
        "heading_count": sum(1 for block in blocks if block.type == "heading"),
        "table_count": sum(1 for block in blocks if block.type == "table"),
        "figure_count": sum(1 for block in blocks if block.type == "figure"),
        "formula_count": sum(1 for block in blocks if block.type == "formula"),
        "reference_block_count": sum(1 for block in blocks if block.type == "reference"),
        "pages_with_text": len({block.page_start for block in blocks if block.page_start}),
        "parser_warnings_count": len(warnings),
        "avg_block_confidence": _average_confidence(blocks),
    }


def validate_document_parse_payload(payload: Mapping[str, Any]) -> None:
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("invalid document_parse schema_version")
    if payload.get("stage_id") != STAGE_ID:
        raise ValueError("invalid document_parse stage_id")
    status = str(payload.get("status", ""))
    if status not in {DocumentParseStatus.SUCCESS.value, DocumentParseStatus.PENDING.value}:
        raise ValueError("invalid document_parse status")

    attempts = payload.get("attempts")
    if not isinstance(attempts, list) or not attempts:
        raise ValueError("document_parse attempts must be a non-empty list")
    for attempt in attempts:
        if not isinstance(attempt, Mapping):
            raise ValueError("each parser attempt must be an object")
        for key in ("parser", "role", "status"):
            if not str(attempt.get(key, "")).strip():
                raise ValueError(f"parser attempt missing required field: {key}")
        if status == DocumentParseStatus.PENDING.value:
            for key in ("duration_seconds", "log_path", "error_summary"):
                if key not in attempt:
                    raise ValueError(f"parser attempt missing required field: {key}")

    blocks = payload.get("blocks")
    if not isinstance(blocks, list):
        raise ValueError("document_parse blocks must be a list")
    if status == DocumentParseStatus.SUCCESS.value and not blocks:
        raise ValueError("success payload requires at least one block")

    seen_order: set[int] = set()
    for block in blocks:
        if not isinstance(block, Mapping):
            raise ValueError("each block must be an object")
        for key in ("block_id", "type", "text"):
            if key not in block:
                raise ValueError(f"block missing required field: {key}")
            if not str(block[key]).strip():
                raise ValueError(f"block field must be non-empty: {key}")
        if "order" not in block:
            raise ValueError("block missing required field: order")
        order = block["order"]
        if not isinstance(order, int) or isinstance(order, bool):
            raise ValueError("block order must be an integer")
        if order in seen_order:
            raise ValueError("block order values must be unique")
        seen_order.add(order)

    if status == DocumentParseStatus.PENDING.value:
        for field_name in (
            "pending_reason_code",
            "pending_reason_text",
            "codex_next_action_hint",
        ):
            if not str(payload.get(field_name, "")).strip():
                raise ValueError(f"pending payload requires {field_name}")
        if payload.get(
            "codex_next_action_hint",
        ) != CODEX_NEXT_ACTION_REVIEW_STAGE2_PARSE_FAILURE:
            raise ValueError("invalid codex_next_action_hint")


def _average_confidence(blocks: list[ParsedBlock]) -> float:
    values = [block.confidence for block in blocks if block.confidence > 0]
    if not values:
        return 0.0
    return round(sum(values) / len(values), 4)
