"""Read-only DeepSeek overview agent for the workbench."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from paper_pipeline.app.app_state import WorkbenchOverviewAgentMessage, WorkbenchState

DEFAULT_DEEPSEEK_BASE_URL = "https://api.deepseek.com"
DEFAULT_DEEPSEEK_MODEL = "deepseek-v4-flash"
DEFAULT_DEEPSEEK_TIMEOUT_SECONDS = 30

OverviewAgentTransport = Callable[
    [str, dict[str, object], dict[str, str], int],
    dict[str, object],
]

_SYSTEM_PROMPT = (
    "你是 Paper Pipeline 的只读总 Agent。你只能根据提供的本地工作台状态回答，"
    "用简洁中文说明当前运行、审核、SQL 上传和下一步。不要声称已经执行上传、"
    "不要要求用户查看英文报告，不能编造上下文里没有的文件或结果。"
)


@dataclass(frozen=True, slots=True)
class OverviewAgentConfig:
    api_key: str
    base_url: str = DEFAULT_DEEPSEEK_BASE_URL
    model: str = DEFAULT_DEEPSEEK_MODEL
    timeout_seconds: int = DEFAULT_DEEPSEEK_TIMEOUT_SECONDS

    @classmethod
    def from_environ(cls, environ: Mapping[str, str] | None = None) -> OverviewAgentConfig:
        source = environ if environ is not None else os.environ
        timeout = _int_or_default(
            source.get("DEEPSEEK_TIMEOUT_SECONDS", ""),
            DEFAULT_DEEPSEEK_TIMEOUT_SECONDS,
        )
        return cls(
            api_key=(
                str(source.get("DEEPSEEK_API_KEY", "")).strip()
                or str(source.get("OPENAI_API_KEY", "")).strip()
            ),
            base_url=(
                str(source.get("DEEPSEEK_BASE_URL", "")).strip()
                or str(source.get("OPENAI_BASE_URL", "")).strip()
                or DEFAULT_DEEPSEEK_BASE_URL
            ),
            model=(
                str(source.get("DEEPSEEK_MODEL", "")).strip()
                or str(source.get("OPENAI_MODEL", "")).strip()
                or str(source.get("LLM_MODEL", "")).strip()
                or DEFAULT_DEEPSEEK_MODEL
            ),
            timeout_seconds=timeout,
        )

    @property
    def configured(self) -> bool:
        return bool(self.api_key.strip())


@dataclass(frozen=True, slots=True)
class OverviewAgentResult:
    accepted: bool
    message: str
    answer: str = ""
    question: str = ""


class OverviewAgentService:
    def __init__(self, client: DeepSeekOverviewAgentClient | None = None) -> None:
        self.client = client or DeepSeekOverviewAgentClient()

    def answer(
        self,
        *,
        question: str,
        state: WorkbenchState,
        config: OverviewAgentConfig,
    ) -> OverviewAgentResult:
        normalized_question = question.strip()
        if not normalized_question:
            return OverviewAgentResult(
                accepted=False,
                message="请输入你想问总 Agent 的问题。",
            )
        if not config.configured:
            return OverviewAgentResult(
                accepted=False,
                message="请先在配置页设置 LLM 的 OPENAI_API_KEY，或单独设置 DEEPSEEK_API_KEY，再使用总 Agent。",
                question=normalized_question,
            )
        try:
            answer = self.client.answer(
                question=normalized_question,
                context=build_overview_agent_context(state),
                config=config,
                conversation=state.overview_agent_messages,
            )
        except Exception as exc:  # noqa: BLE001 - UI action returns safe failure text.
            return OverviewAgentResult(
                accepted=False,
                message=f"DeepSeek 总 Agent 请求失败：{_redact_secret(str(exc), config.api_key)}",
                question=normalized_question,
            )
        return OverviewAgentResult(
            accepted=True,
            message="DeepSeek 总 Agent 已返回中文说明。",
            answer=answer,
            question=normalized_question,
        )


class DeepSeekOverviewAgentClient:
    def __init__(self, transport: OverviewAgentTransport | None = None) -> None:
        self.transport = transport or _urllib_transport

    def answer(
        self,
        *,
        question: str,
        context: dict[str, object],
        config: OverviewAgentConfig,
        conversation: list[WorkbenchOverviewAgentMessage] | None = None,
    ) -> str:
        url = f"{config.base_url.rstrip('/')}/chat/completions"
        messages: list[dict[str, str]] = [{"role": "system", "content": _SYSTEM_PROMPT}]
        messages.extend(_conversation_messages(conversation or []))
        messages.append(
            {
                "role": "user",
                "content": json.dumps(
                    {"question": question, "workbench_state": context},
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            },
        )
        payload: dict[str, object] = {
            "model": config.model,
            "messages": messages,
            "stream": False,
        }
        headers = {
            "Authorization": f"Bearer {config.api_key}",
            "Content-Type": "application/json",
        }
        raw = self.transport(url, payload, headers, config.timeout_seconds)
        return _content_from_chat_completion(raw).strip()


def build_overview_agent_context(state: WorkbenchState) -> dict[str, object]:
    return {
        "run": {
            "run_id": state.current_run_id,
            "status": state.run_status,
            "stage": state.current_stage,
            "total": state.new_file_count,
            "completed": state.completed_count,
            "pending": state.pending_count,
            "failed": state.failed_count,
            "output_dir": state.output_dir,
            "report_path": state.latest_report_path,
            "evidence_bundle_path": state.latest_evidence_bundle_path,
            "next_action": state.suggested_next_action,
        },
        "files": [
            {
                "file_name": item.file_name,
                "status": item.status,
                "reason": item.reason,
                "final_markdown_path": item.final_markdown_path,
                "metadata_path": item.metadata_path,
            }
            for item in state.file_items[:20]
        ],
        "review_items": [
            {
                "paper_id": item.paper_id,
                "title": item.title,
                "tier": item.tier,
                "category": item.category,
                "direction": item.direction,
                "needs_codex_review": item.needs_codex_review,
                "reason": item.pending_or_failed_reason,
                "sql_eligibility": item.sql_eligibility_label,
                "markdown_path": item.markdown_path,
                "metadata_path": item.metadata_path,
            }
            for item in state.review_items[:20]
        ],
        "sql": _sql_context(state),
        "history": [
            {
                "run_id": item.run_id,
                "started_at": item.started_at,
                "total": item.total_count,
                "completed": item.completed_count,
                "pending": item.pending_count,
                "failed": item.failed_count,
                "report_path": item.report_path,
            }
            for item in state.history_items[:10]
        ],
    }


def _conversation_messages(
    conversation: list[WorkbenchOverviewAgentMessage],
) -> list[dict[str, str]]:
    messages: list[dict[str, str]] = []
    for item in conversation[-12:]:
        role = item.role if item.role in {"user", "assistant"} else "user"
        content = item.content.strip()
        if content:
            messages.append({"role": role, "content": content})
    return messages


def _sql_context(state: WorkbenchState) -> dict[str, object]:
    sql_state = state.sql_upload_state
    if sql_state is None:
        return {
            "eligible_count": state.sql_eligible_count,
            "blocked_count": state.sql_blocked_count,
            "uploaded_count": state.sql_uploaded_count,
            "preview_id": "",
            "blocked_reasons": [],
        }
    return {
        "mode": sql_state.sql_mode,
        "connection_status": sql_state.connection_status,
        "target_table": sql_state.target_table,
        "eligible_count": sql_state.eligible_count,
        "blocked_count": sql_state.blocked_count,
        "uploaded_count": state.sql_uploaded_count,
        "preview_id": sql_state.preview_id,
        "confirm_enabled": sql_state.confirm_enabled,
        "confirm_disabled_reason": sql_state.confirm_disabled_reason,
        "blocked_reasons": [
            {
                "paper_id": row.paper_id,
                "fields": row.blocking_fields,
                "reasons": row.reasons,
                "next_action": row.suggested_next_action,
                "metadata_path": row.metadata_path,
                "md_path": row.md_path,
            }
            for row in sql_state.blocked_reasons[:20]
        ],
    }


def _urllib_transport(
    url: str,
    payload: dict[str, object],
    headers: dict[str, str],
    timeout_seconds: int,
) -> dict[str, object]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            decoded = json.loads(response.read().decode("utf-8"))
    except urllib.error.URLError as exc:
        raise RuntimeError(f"DeepSeek request failed: {exc}") from exc
    if not isinstance(decoded, dict):
        raise TypeError("DeepSeek response must be a JSON object")
    return decoded


def _content_from_chat_completion(raw: dict[str, object]) -> str:
    choices = raw.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, Mapping):
            message = first.get("message")
            if isinstance(message, Mapping):
                content = message.get("content")
                if isinstance(content, str):
                    return content
    content = raw.get("content")
    return content if isinstance(content, str) else ""


def _int_or_default(raw: str, default: int) -> int:
    try:
        parsed = int(str(raw).strip())
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _redact_secret(text: str, api_key: str) -> str:
    if not api_key:
        return text
    return text.replace(api_key, "****")
