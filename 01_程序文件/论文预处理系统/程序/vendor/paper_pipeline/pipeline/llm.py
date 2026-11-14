"""Configurable LLM provider boundary for pipeline health and quality checks."""

from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from typing import Any, Literal, Protocol

LLMMode = Literal["fake", "real", "failed", "config-error"]
LLMProviderName = Literal["fake", "openai", "compatible", "legacy-dity"]
LLMHealthStatus = Literal["pass", "warn", "fail"]

_DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"
_DEFAULT_OPENAI_MODEL = "gpt-5-mini"
_FAKE_MODEL = "fake-stub"
_LEGACY_DITY_MODEL = "legacy-dity-endpoint"


@dataclass(frozen=True, slots=True)
class LLMConfig:
    provider: LLMProviderName | str
    mode: LLMMode
    model: str
    api_key: str = ""
    base_url: str = ""
    endpoint: str = ""
    key_status: str = "UNSET"
    message: str = ""

    def to_safe_dict(self) -> dict[str, object]:
        return {
            "provider": self.provider,
            "mode": self.mode,
            "model": self.model,
            "base_url": self.base_url,
            "endpoint": self.endpoint,
            "key_status": self.key_status,
            "api_key": mask_llm_secret(self.api_key) if self.api_key else "UNSET",
            "message": redact_llm_secrets(self.message, self),
        }


@dataclass(frozen=True, slots=True)
class LLMHealthcheckResult:
    provider: str
    model: str
    mode: LLMMode
    status: LLMHealthStatus
    key_status: str
    llm_invoked: bool
    message: str
    error: str = ""

    def to_safe_dict(self) -> dict[str, object]:
        return asdict(self)


class LLMProvider(Protocol):
    def complete_json(
        self,
        *,
        prompt: str,
        payload: dict[str, object],
        config: LLMConfig,
        timeout_seconds: int,
    ) -> dict[str, object]:
        """Return a JSON object from the provider."""


LLMTransport = Callable[
    [str, dict[str, object], Mapping[str, str], int],
    dict[str, object],
]


class FakeLLMProvider:
    def complete_json(
        self,
        *,
        prompt: str,
        payload: dict[str, object],
        config: LLMConfig,
        timeout_seconds: int,
    ) -> dict[str, object]:
        return {
            "decision": "accept",
            "reason": "fake LLM provider used local deterministic fallback",
            "model": config.model,
        }


class OpenAICompatibleProvider:
    def __init__(self, transport: LLMTransport | None = None) -> None:
        self.transport = transport or _urllib_openai_transport

    def complete_json(
        self,
        *,
        prompt: str,
        payload: dict[str, object],
        config: LLMConfig,
        timeout_seconds: int,
    ) -> dict[str, object]:
        url = f"{config.base_url.rstrip('/')}/chat/completions"
        headers = {
            "Authorization": f"Bearer {config.api_key}",
            "Content-Type": "application/json",
        }
        request_payload: dict[str, object] = {
            "model": config.model,
            "messages": [
                {"role": "system", "content": prompt},
                {
                    "role": "user",
                    "content": json.dumps(payload, ensure_ascii=False, sort_keys=True),
                },
            ],
            "response_format": {"type": "json_object"},
        }
        try:
            raw = self.transport(url, request_payload, headers, timeout_seconds)
            return _json_from_openai_compatible_response(raw)
        except Exception as exc:  # noqa: BLE001 - provider boundary returns safe errors.
            raise RuntimeError(redact_llm_secrets(str(exc), config)) from exc


class LegacyDityEndpointProvider:
    def __init__(self, legacy_client: Any | None = None) -> None:
        self.legacy_client = legacy_client or _HttpJsonEndpointClient()

    def complete_json(
        self,
        *,
        prompt: str,
        payload: dict[str, object],
        config: LLMConfig,
        timeout_seconds: int,
    ) -> dict[str, object]:
        if not config.endpoint:
            raise RuntimeError("DITY_LLM_ENDPOINT is required for legacy-dity provider")
        return self.legacy_client.evaluate_quality(
            endpoint=config.endpoint,
            payload=payload,
            timeout_seconds=timeout_seconds,
        )


def resolve_llm_config(environ: Mapping[str, str] | None = None) -> LLMConfig:
    source = environ if environ is not None else os.environ
    requested_provider = str(source.get("LLM_PROVIDER", "")).strip().lower()
    api_key = str(source.get("OPENAI_API_KEY", "")).strip()
    key_status = "SET" if api_key else "UNSET"
    model = (
        str(source.get("OPENAI_MODEL", "")).strip()
        or str(source.get("LLM_MODEL", "")).strip()
        or _DEFAULT_OPENAI_MODEL
    )
    base_url = str(source.get("OPENAI_BASE_URL", "")).strip() or _DEFAULT_OPENAI_BASE_URL
    legacy_endpoint = str(source.get("DITY_LLM_ENDPOINT", "")).strip()

    if requested_provider == "fake":
        return LLMConfig(
            provider="fake",
            mode="fake",
            model=model if model != _DEFAULT_OPENAI_MODEL else _FAKE_MODEL,
            api_key=api_key,
            base_url=base_url,
            key_status=key_status,
            message="LLM_PROVIDER=fake selected local deterministic fallback.",
        )

    if requested_provider in {"openai", "compatible"}:
        if not api_key:
            return LLMConfig(
                provider=requested_provider,
                mode="config-error",
                model=model,
                api_key="",
                base_url=base_url,
                key_status="UNSET",
                message=(
                    f"LLM_PROVIDER={requested_provider} requires OPENAI_API_KEY; "
                    "external LLM will not be invoked."
                ),
            )
        return LLMConfig(
            provider=requested_provider,
            mode="real",
            model=model,
            api_key=api_key,
            base_url=base_url,
            key_status="SET",
            message=f"LLM_PROVIDER={requested_provider} selected real provider.",
        )

    if requested_provider:
        return LLMConfig(
            provider=requested_provider,
            mode="config-error",
            model=model,
            api_key=api_key,
            base_url=base_url,
            key_status=key_status,
            message=f"Unsupported LLM_PROVIDER={requested_provider}.",
        )

    if legacy_endpoint:
        return LLMConfig(
            provider="legacy-dity",
            mode="real",
            model=str(source.get("LLM_MODEL", "")).strip() or _LEGACY_DITY_MODEL,
            endpoint=legacy_endpoint,
            key_status="UNSET",
            message="DITY_LLM_ENDPOINT selected legacy real LLM endpoint.",
        )

    return LLMConfig(
        provider="fake",
        mode="fake",
        model=_FAKE_MODEL,
        key_status=key_status,
        message="No real LLM provider configured; using local deterministic fallback.",
    )


def resolve_llm_provider(
    config: LLMConfig,
    *,
    legacy_client: Any | None = None,
) -> LLMProvider:
    if config.provider == "legacy-dity" and config.mode == "real":
        return LegacyDityEndpointProvider(legacy_client)
    if config.provider in {"openai", "compatible"} and config.mode == "real":
        return OpenAICompatibleProvider()
    return FakeLLMProvider()


def run_llm_healthcheck(
    *,
    environ: Mapping[str, str] | None = None,
    provider: LLMProvider | None = None,
    timeout_seconds: int = 10,
) -> LLMHealthcheckResult:
    config = resolve_llm_config(environ)
    if config.mode == "fake":
        return LLMHealthcheckResult(
            provider=str(config.provider),
            model=config.model,
            mode="fake",
            status="warn",
            key_status=config.key_status,
            llm_invoked=False,
            message=config.message,
        )
    if config.mode == "config-error":
        return LLMHealthcheckResult(
            provider=str(config.provider),
            model=config.model,
            mode="config-error",
            status="warn",
            key_status=config.key_status,
            llm_invoked=False,
            message=config.message,
        )

    resolved_provider = provider or resolve_llm_provider(config)
    try:
        result = resolved_provider.complete_json(
            prompt="Return a tiny JSON healthcheck object.",
            payload={"healthcheck": True},
            config=config,
            timeout_seconds=timeout_seconds,
        )
        reason = str(result.get("reason") or "LLM healthcheck call succeeded")
        return LLMHealthcheckResult(
            provider=str(config.provider),
            model=config.model,
            mode="real",
            status="pass",
            key_status=config.key_status,
            llm_invoked=True,
            message=redact_llm_secrets(reason, config),
        )
    except Exception as exc:  # noqa: BLE001 - healthcheck reports safe failure.
        return LLMHealthcheckResult(
            provider=str(config.provider),
            model=config.model,
            mode="failed",
            status="fail",
            key_status=config.key_status,
            llm_invoked=True,
            message="LLM healthcheck call failed.",
            error=redact_llm_secrets(str(exc), config),
        )


def mask_llm_secret(value: str) -> str:
    raw = str(value or "")
    if not raw:
        return "UNSET"
    if len(raw) <= 8:
        return "SET"
    return f"{raw[:4]}...****"


def redact_llm_secrets(text: str, config: LLMConfig | None = None) -> str:
    redacted = str(text)
    redacted = re.sub(
        r"(?i)authorization\s*[:=]\s*bearer\s+[^;,\s}\]'\"]+",
        "credential header redacted",
        redacted,
    )
    redacted = re.sub(
        r"(?i)bearer\s+[^;,\s}\]'\"]+",
        "credential redacted",
        redacted,
    )
    if config is not None and config.api_key:
        redacted = redacted.replace(config.api_key, mask_llm_secret(config.api_key))
    redacted = re.sub(
        r"(?i)\b(api_key|token|secret)\s*=\s*[^;,\s}\]'\"]+",
        r"\1=<redacted>",
        redacted,
    )
    return redacted


class _HttpJsonEndpointClient:
    def evaluate_quality(
        self,
        *,
        endpoint: str,
        payload: dict[str, object],
        timeout_seconds: int,
    ) -> dict[str, object]:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            endpoint,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                decoded = json.loads(response.read().decode("utf-8"))
        except urllib.error.URLError as exc:
            raise RuntimeError(f"LLM endpoint request failed: {exc}") from exc
        if not isinstance(decoded, dict):
            raise TypeError("LLM endpoint must return a JSON object")
        return decoded


def _urllib_openai_transport(
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
        decoded = json.loads(response.read().decode("utf-8"))
    if not isinstance(decoded, dict):
        raise TypeError("LLM provider response must be a JSON object")
    return decoded


def _json_from_openai_compatible_response(raw: dict[str, object]) -> dict[str, object]:
    choices = raw.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, Mapping):
            message = first.get("message")
            if isinstance(message, Mapping):
                content = message.get("content")
                if isinstance(content, str):
                    decoded = json.loads(content)
                    if isinstance(decoded, dict):
                        return decoded
    return dict(raw)
