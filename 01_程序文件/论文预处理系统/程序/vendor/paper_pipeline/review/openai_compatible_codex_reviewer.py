"""OpenAI-compatible Codex reviewer implementation."""

from __future__ import annotations

import json
import os
import re
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

from paper_pipeline.domain import AgentResult, DuplicateStatus, PaperMetadata, Tier
from paper_pipeline.domain.serialization import metadata_to_dict

_DEFAULT_BASE_URL = "https://api.openai.com/v1"
_DEFAULT_MODEL = "gpt-5-mini"
_DEFAULT_TIMEOUT_SECONDS = 60
_ALLOWED_DECISIONS = {"approve_gold", "approve_silver", "reject", "uncertain"}

CodexReviewTransport = Callable[
    [str, dict[str, object], Mapping[str, str], int],
    dict[str, object],
]


class CodexReviewSchemaError(ValueError):
    """Raised when a Codex review response is not valid for the pipeline."""


@dataclass(frozen=True, slots=True)
class CodexReviewConfig:
    api_key: str = ""
    base_url: str = _DEFAULT_BASE_URL
    model: str = _DEFAULT_MODEL
    timeout_seconds: int = _DEFAULT_TIMEOUT_SECONDS

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> CodexReviewConfig:
        source = environ if environ is not None else os.environ
        api_key = (
            str(source.get("CODEX_REVIEW_API_KEY", "")).strip()
            or str(source.get("OPENAI_API_KEY", "")).strip()
        )
        base_url = (
            str(source.get("CODEX_REVIEW_BASE_URL", "")).strip()
            or str(source.get("OPENAI_BASE_URL", "")).strip()
            or _DEFAULT_BASE_URL
        )
        model = (
            str(source.get("CODEX_REVIEW_MODEL", "")).strip()
            or str(source.get("OPENAI_MODEL", "")).strip()
            or _DEFAULT_MODEL
        )
        return cls(
            api_key=api_key,
            base_url=base_url,
            model=model,
            timeout_seconds=_timeout_from_env(source),
        )


@dataclass(slots=True)
class OpenAICompatibleCodexReviewer:
    config: CodexReviewConfig
    transport: CodexReviewTransport | None = None
    agent_name: str = "OpenAICompatibleCodexReviewer"

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        transport: CodexReviewTransport | None = None,
    ) -> OpenAICompatibleCodexReviewer:
        return cls(config=CodexReviewConfig.from_env(environ), transport=transport)

    def review(
        self,
        *,
        task_id: str,
        pending_metadata: PaperMetadata,
        pending_body: str = "",
    ) -> AgentResult:
        if not self.config.api_key.strip():
            raise RuntimeError(
                "CODEX_REVIEW_API_KEY or OPENAI_API_KEY is required for Codex review",
            )

        request_payload = _chat_completion_payload(
            task_id=task_id,
            metadata=pending_metadata,
            pending_body=pending_body,
            model=self.config.model,
        )
        url = f"{self.config.base_url.rstrip('/')}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
        }
        try:
            raw = (self.transport or _urllib_transport)(
                url,
                request_payload,
                headers,
                self.config.timeout_seconds,
            )
        except Exception as exc:  # noqa: BLE001 - service records this as codex_call_failed.
            raise RuntimeError(_redact_secret(str(exc), self.config.api_key)) from exc

        return _agent_result_from_response(
            raw,
            task_id=task_id,
            metadata=pending_metadata,
            model=self.config.model,
            agent_name=self.agent_name,
        )


def _chat_completion_payload(
    *,
    task_id: str,
    metadata: PaperMetadata,
    pending_body: str,
    model: str,
) -> dict[str, object]:
    system_prompt = (
        "You are a Codex paper review gate. Return only strict JSON with keys "
        "decision, confidence, reason, and evidence. decision must be one of "
        "approve_gold, approve_silver, reject, uncertain. reject requires a "
        "non-empty reason. Do not classify the paper, do not change category, "
        "direction, or research method, and never authorize SQL upload."
    )
    user_payload = {
        "task_id": task_id,
        "metadata": metadata_to_dict(metadata),
        "processed_markdown": pending_body,
        "allowed_decisions": sorted(_ALLOWED_DECISIONS),
    }
    return {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": json.dumps(user_payload, ensure_ascii=False, sort_keys=True),
            },
        ],
        "response_format": {"type": "json_object"},
    }


def _agent_result_from_response(
    raw: Mapping[str, object],
    *,
    task_id: str,
    metadata: PaperMetadata,
    model: str,
    agent_name: str,
) -> AgentResult:
    payload = _decode_response_payload(raw)
    decision = _required_decision(payload)
    reason = _reason(payload, decision)
    confidence = _confidence(payload)
    target_tier = _target_tier(decision)
    evidence = _evidence(payload)
    duplicate_status = _duplicate_status(payload, metadata)
    similar_to = _similar_to(payload)
    status: Literal["success", "uncertain"] = (
        "uncertain" if decision == "uncertain" else "success"
    )

    return AgentResult(
        agent_name=agent_name,
        task_id=task_id,
        paper_id=metadata.id,
        run_id=metadata.run_id,
        status=status,
        decision={
            "new_tier_suggestion": target_tier.value,
            "duplicate_decision": duplicate_status.value,
            "similar_to": similar_to,
            "reason": reason,
            "evidence": evidence,
            "ai_decision": decision,
            "model": model,
            "decision_source": "codex_review",
        },
        confidence=confidence,
        reason=reason,
        evidence=evidence,
        needs_review=decision == "uncertain",
    )


def _decode_response_payload(raw: Mapping[str, object]) -> dict[str, object]:
    content = _chat_completion_content(raw) or _responses_content(raw)
    if not content:
        raise CodexReviewSchemaError("Codex review response did not include JSON content")
    try:
        decoded = json.loads(content)
    except json.JSONDecodeError as exc:
        raise CodexReviewSchemaError("Codex review response must be valid JSON") from exc
    if not isinstance(decoded, dict):
        raise CodexReviewSchemaError("Codex review response must be a JSON object")
    return dict(decoded)


def _chat_completion_content(raw: Mapping[str, object]) -> str:
    choices = raw.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    first = choices[0]
    if not isinstance(first, Mapping):
        return ""
    message = first.get("message")
    if not isinstance(message, Mapping):
        return ""
    content = message.get("content")
    return content if isinstance(content, str) else ""


def _responses_content(raw: Mapping[str, object]) -> str:
    output_text = raw.get("output_text")
    if isinstance(output_text, str):
        return output_text
    output = raw.get("output")
    if not isinstance(output, list):
        return ""
    chunks: list[str] = []
    for item in output:
        if not isinstance(item, Mapping):
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for content_item in content:
            if not isinstance(content_item, Mapping):
                continue
            text = content_item.get("text")
            if isinstance(text, str):
                chunks.append(text)
    return "".join(chunks)


def _required_decision(payload: Mapping[str, object]) -> str:
    decision = str(payload.get("decision", "")).strip()
    if decision not in _ALLOWED_DECISIONS:
        allowed = ", ".join(sorted(_ALLOWED_DECISIONS))
        raise CodexReviewSchemaError(f"Codex review decision must be one of: {allowed}")
    return decision


def _reason(payload: Mapping[str, object], decision: str) -> str:
    reason = str(payload.get("reason", "")).strip()
    if decision == "reject" and not reason:
        raise CodexReviewSchemaError("Codex reject decision requires a non-empty reason")
    if reason:
        return reason
    if decision == "uncertain":
        return "Codex returned uncertain decision"
    return f"Codex returned {decision}"


def _confidence(payload: Mapping[str, object]) -> float:
    value = payload.get("confidence", 0.0)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise CodexReviewSchemaError("Codex review confidence must be a number")
    confidence = float(value)
    if not 0 <= confidence <= 1:
        raise CodexReviewSchemaError("Codex review confidence must be between 0 and 1")
    return confidence


def _target_tier(decision: str) -> Tier:
    if decision == "approve_gold":
        return Tier.GOLD
    if decision == "approve_silver":
        return Tier.SILVER
    if decision == "reject":
        return Tier.REJECTED
    return Tier.MANUAL_REVIEW_REQUIRED


def _evidence(payload: Mapping[str, object]) -> list[str]:
    evidence = payload.get("evidence", [])
    if isinstance(evidence, list):
        values = [str(item) for item in evidence if str(item).strip()]
    else:
        values = []
    if "metadata" not in values:
        values.append("metadata")
    if "processed_markdown" not in values:
        values.append("processed_markdown")
    return values


def _duplicate_status(
    payload: Mapping[str, object],
    metadata: PaperMetadata,
) -> DuplicateStatus:
    value = payload.get("duplicate_decision", metadata.duplicate_status)
    if isinstance(value, DuplicateStatus):
        return value
    try:
        return DuplicateStatus(str(value))
    except ValueError as exc:
        raise CodexReviewSchemaError("Codex duplicate_decision is not valid") from exc


def _similar_to(payload: Mapping[str, object]) -> list[str]:
    values = payload.get("similar_to", [])
    if not isinstance(values, list):
        raise CodexReviewSchemaError("Codex similar_to must be a list")
    return [str(item) for item in values if str(item).strip()]


def _timeout_from_env(source: Mapping[str, str]) -> int:
    raw = str(source.get("CODEX_REVIEW_TIMEOUT_SECONDS", "")).strip()
    if not raw:
        return _DEFAULT_TIMEOUT_SECONDS
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError("CODEX_REVIEW_TIMEOUT_SECONDS must be an integer") from exc
    if value <= 0:
        raise ValueError("CODEX_REVIEW_TIMEOUT_SECONDS must be greater than 0")
    return value


def _urllib_transport(
    url: str,
    payload: dict[str, object],
    headers: Mapping[str, str],
    timeout_seconds: int,
) -> dict[str, object]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers=dict(headers),
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        decoded: Any = json.loads(response.read().decode("utf-8"))
    if not isinstance(decoded, dict):
        raise TypeError("Codex review provider response must be a JSON object")
    return dict(decoded)


def _redact_secret(text: str, api_key: str) -> str:
    redacted = text.replace(api_key, "****") if api_key else text
    return re.sub(
        r"(?i)authorization\s*[:=]\s*bearer\s+[^;,\s}\]'\"]+",
        "credential header redacted",
        redacted,
    )
