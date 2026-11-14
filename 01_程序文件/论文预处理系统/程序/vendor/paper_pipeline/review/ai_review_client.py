"""AI review client contracts and deterministic test client."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any


class AIReviewSchemaError(ValueError):
    """Raised when an AI review response is not valid for the pipeline."""


@dataclass(frozen=True, slots=True)
class AIReviewConfig:
    enabled: bool = False
    dry_run: bool = True
    api_key: str = ""
    model: str = "gpt-5-mini"

    @classmethod
    def from_env(cls) -> AIReviewConfig:
        api_key = os.getenv("OPENAI_API_KEY", "")
        enabled = _env_bool("AI_REVIEW_ENABLED", default=bool(api_key))
        dry_run = _env_bool("AI_REVIEW_DRY_RUN", default=True)
        return cls(
            enabled=enabled,
            dry_run=dry_run,
            api_key=api_key,
            model=os.getenv("AI_REVIEW_MODEL", "gpt-5-mini"),
        )

    @property
    def status(self) -> str:
        if not self.enabled:
            return "disabled"
        return "dry_run" if self.dry_run else "enabled"

    def public_dict(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "dry_run": self.dry_run,
            "model": self.model,
            "status": self.status,
        }


@dataclass(frozen=True, slots=True)
class AIReviewInput:
    paper_id: str
    run_id: str
    metadata: dict[str, object]
    markdown: str


@dataclass(frozen=True, slots=True)
class AIReviewResult:
    paper_id: str
    run_id: str
    decision: str
    target_tier: str
    confidence: float
    reason: str
    evidence: list[str] = field(default_factory=list)
    risk_flags: list[str] = field(default_factory=list)
    blocking: bool = False
    allow_index: bool = False
    allow_sql_upload: bool = False
    reviewer: str = "deepseek"
    model: str = ""
    created_at: str = ""

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class FakeAIReviewClient:
    model = "fake-review-model"

    def __init__(
        self,
        *,
        decision: str = "approve_gold",
        confidence: float = 0.88,
        risk_flags: list[str] | None = None,
        raw_response: str | None = None,
        error: Exception | None = None,
    ) -> None:
        self.decision = decision
        self.confidence = confidence
        self.risk_flags = list(risk_flags or [])
        self.raw_response = raw_response
        self.error = error
        self.call_count = 0

    @classmethod
    def approve_gold(
        cls,
        *,
        confidence: float = 0.88,
        risk_flags: list[str] | None = None,
    ) -> FakeAIReviewClient:
        return cls(
            decision="approve_gold",
            confidence=confidence,
            risk_flags=risk_flags,
        )

    @classmethod
    def approve_silver(
        cls,
        *,
        confidence: float = 0.82,
        risk_flags: list[str] | None = None,
    ) -> FakeAIReviewClient:
        return cls(
            decision="approve_silver",
            confidence=confidence,
            risk_flags=risk_flags,
        )

    @classmethod
    def reject(
        cls,
        *,
        confidence: float = 0.8,
        risk_flags: list[str] | None = None,
    ) -> FakeAIReviewClient:
        return cls(decision="reject", confidence=confidence, risk_flags=risk_flags)

    @classmethod
    def keep_pending(
        cls,
        *,
        confidence: float = 0.72,
        risk_flags: list[str] | None = None,
    ) -> FakeAIReviewClient:
        return cls(
            decision="keep_pending",
            confidence=confidence,
            risk_flags=risk_flags,
        )

    @classmethod
    def invalid_json(cls) -> FakeAIReviewClient:
        return cls(raw_response="not-json")

    @classmethod
    def raises(cls, error: Exception) -> FakeAIReviewClient:
        return cls(error=error)

    @property
    def is_configured(self) -> bool:
        return True

    def review(self, request: AIReviewInput) -> str:
        self.call_count += 1
        if self.error is not None:
            raise self.error
        if self.raw_response is not None:
            return self.raw_response
        return json.dumps(
            _payload(
                request,
                decision=self.decision,
                confidence=self.confidence,
                risk_flags=self.risk_flags,
                model=self.model,
            ),
            ensure_ascii=False,
            sort_keys=True,
        )


def parse_ai_review_result(raw: str, *, fallback_model: str) -> AIReviewResult:
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise AIReviewSchemaError("AI review response must be valid JSON") from exc
    if not isinstance(decoded, dict):
        raise AIReviewSchemaError("AI review response must be a JSON object")

    paper_id = _required_str(decoded, "paper_id")
    run_id = _required_str(decoded, "run_id")
    decision = _required_str(decoded, "decision")
    target_tier = _required_str(decoded, "target_tier")
    confidence = _required_float(decoded, "confidence")
    reason = _required_str(decoded, "reason")
    evidence = _required_str_list(decoded, "evidence")
    risk_flags = _required_str_list(decoded, "risk_flags")
    blocking = _required_bool(decoded, "blocking")
    allow_index = _required_bool(decoded, "allow_index")
    allow_sql_upload = _required_bool(decoded, "allow_sql_upload")
    reviewer = _required_str(decoded, "reviewer")
    model = str(decoded.get("model") or fallback_model)
    created_at = str(decoded.get("created_at") or _now_iso())

    expected_tier = _DECISION_TO_TIER.get(decision)
    if expected_tier is None:
        raise AIReviewSchemaError(f"unsupported AI review decision: {decision}")
    if target_tier != expected_tier:
        raise AIReviewSchemaError("AI review decision does not match target_tier")
    if not 0 <= confidence <= 1:
        raise AIReviewSchemaError("AI review confidence must be between 0 and 1")

    return AIReviewResult(
        paper_id=paper_id,
        run_id=run_id,
        decision=decision,
        target_tier=target_tier,
        confidence=confidence,
        reason=reason,
        evidence=evidence,
        risk_flags=risk_flags,
        blocking=blocking,
        allow_index=allow_index,
        allow_sql_upload=allow_sql_upload,
        reviewer=reviewer,
        model=model,
        created_at=created_at,
    )


_DECISION_TO_TIER = {
    "approve_gold": "gold",
    "approve_silver": "silver",
    "reject": "rejected",
    "keep_pending": "pending",
}


def _payload(
    request: AIReviewInput,
    *,
    decision: str,
    confidence: float,
    risk_flags: list[str],
    model: str,
) -> dict[str, object]:
    target_tier = _DECISION_TO_TIER[decision]
    return {
        "paper_id": request.paper_id,
        "run_id": request.run_id,
        "decision": decision,
        "target_tier": target_tier,
        "confidence": confidence,
        "reason": _reason_for(decision, target_tier),
        "evidence": ["metadata", "processed_markdown"],
        "risk_flags": risk_flags,
        "blocking": False,
        "allow_index": False,
        "allow_sql_upload": False,
        "reviewer": "deepseek",
        "model": model,
        "created_at": _now_iso(),
    }


def _reason_for(decision: str, target_tier: str) -> str:
    if decision == "keep_pending":
        return "AI review found insufficient evidence for a final tier"
    if decision == "reject":
        return "AI review found rejection evidence"
    return f"AI review recommends {target_tier}"


def _required_str(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise AIReviewSchemaError(f"AI review field {key!r} must be a non-empty string")
    return value


def _required_float(data: dict[str, Any], key: str) -> float:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise AIReviewSchemaError(f"AI review field {key!r} must be a number")
    return float(value)


def _required_bool(data: dict[str, Any], key: str) -> bool:
    value = data.get(key)
    if not isinstance(value, bool):
        raise AIReviewSchemaError(f"AI review field {key!r} must be a boolean")
    return value


def _required_str_list(data: dict[str, Any], key: str) -> list[str]:
    value = data.get(key)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise AIReviewSchemaError(f"AI review field {key!r} must be a string list")
    return list(value)


def _env_bool(name: str, *, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()
