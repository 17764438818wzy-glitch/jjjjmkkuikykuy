"""Read-only configuration display for the workbench."""

from __future__ import annotations

import os
import shutil
from collections.abc import Callable, Mapping
from pathlib import Path

from paper_pipeline.app.app_state import WorkbenchConfigItem

PathExists = Callable[[str | Path], bool]
Which = Callable[[str], str | None]
CanImport = Callable[[str], bool]

DEFAULT_NAS_INPUT_DIR = r"\\zwjy-nas\paper_ai\input_raw"


def build_config_items(
    *,
    env: Mapping[str, str] | None = None,
    path_exists: PathExists | None = None,
    which: Which | None = None,
    can_import: CanImport | None = None,
) -> list[WorkbenchConfigItem]:
    safe_env = env if env is not None else os.environ
    exists = path_exists or _path_exists
    find = which or shutil.which
    importable = can_import or _can_import
    return [
        _nas_item(safe_env, exists),
        _postgres_item(safe_env),
        _tool_item(
            key="mineru",
            name="MinerU",
            env_keys=("MINERU_PATH", "DITY_MINERU_PATH"),
            path_names=("mineru",),
            missing_message="未找到 MinerU，请配置 MINERU_PATH 或加入 PATH。",
            env=safe_env,
            path_exists=exists,
            which=find,
        ),
        _tool_item(
            key="libreoffice",
            name="LibreOffice / LO",
            env_keys=("LIBREOFFICE_PATH", "DITY_LIBREOFFICE_PATH"),
            path_names=("soffice", "libreoffice"),
            missing_message="未找到 LibreOffice，DOC/DOCX 转换不可用。",
            env=safe_env,
            path_exists=exists,
            which=find,
        ),
        _docling_item(importable),
        _llm_item(safe_env),
        _deepseek_agent_item(safe_env),
        _codex_review_item(safe_env),
        WorkbenchConfigItem(
            name="ZIP",
            status="内置支持",
            detail="ZIP 可由 Python 标准库读取。",
            key="zip",
            severity="ok",
            source="stdlib",
            values={"adapter": "zipfile"},
        ),
        _archive_external_item(find),
    ]


def _nas_item(env: Mapping[str, str], path_exists: PathExists) -> WorkbenchConfigItem:
    path = env.get("PAPER_PIPELINE_INPUT_DIR", DEFAULT_NAS_INPUT_DIR)
    accessible = path_exists(path)
    return WorkbenchConfigItem(
        name="NAS 输入目录",
        status="可访问" if accessible else "不可访问",
        detail=f"当前输入目录：{path}" if accessible else f"当前输入目录不可访问：{path}",
        key="nas_input_dir",
        severity="ok" if accessible else "warning",
        source="PAPER_PIPELINE_INPUT_DIR" if "PAPER_PIPELINE_INPUT_DIR" in env else "default",
        values={"path": path},
        action_hint="" if accessible else "请选择可访问的 NAS 或本地输入目录。",
    )


def _postgres_item(env: Mapping[str, str]) -> WorkbenchConfigItem:
    mode = (
        env.get("PAPER_PIPELINE_SQL_MODE")
        or env.get("PAPER_PIPELINE_SQL_ADAPTER_MODE")
        or env.get("SQL_UPLOAD_MODE")
        or "dry-run"
    )
    has_dsn = bool(env.get("POSTGRES_DSN") or env.get("DATABASE_URL"))
    has_parts = bool(env.get("POSTGRES_HOST") and env.get("POSTGRES_USER") and _postgres_db(env))
    password_key = env.get("POSTGRES_PASSWORD_ENV", "POSTGRES_PASSWORD")
    has_password = bool(env.get(password_key))
    configured = (has_dsn or has_parts) and has_password
    missing: list[str] = []
    if not has_dsn and not has_parts:
        missing.extend(["POSTGRES_DSN", "POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER"])
    if not has_password:
        missing.append(password_key)
    is_real = configured and mode in {"real", "postgresql", "postgres"}
    return WorkbenchConfigItem(
        name="PostgreSQL",
        status="real" if is_real else "dry-run",
        detail=(
            "真实上传仍需要审核、预览和二次确认。"
            if configured
            else "当前 PostgreSQL 是 dry-run，不会真实写库。"
        ),
        key="postgresql",
        severity="ok" if is_real else "warning",
        source="env",
        values={
            "mode": mode,
            "target_table": env.get("POSTGRES_TABLE", ""),
            "password": "已设置" if has_password else "未设置",
        },
        missing_keys=missing,
        action_hint="" if configured else "补齐 PostgreSQL 连接配置后再启用真实上传。",
    )


def _postgres_db(env: Mapping[str, str]) -> str:
    return env.get("POSTGRES_DB") or env.get("POSTGRES_DATABASE") or ""


def _tool_item(
    *,
    key: str,
    name: str,
    env_keys: tuple[str, ...],
    path_names: tuple[str, ...],
    missing_message: str,
    env: Mapping[str, str],
    path_exists: PathExists,
    which: Which,
) -> WorkbenchConfigItem:
    for env_key in env_keys:
        candidate = env.get(env_key, "").strip()
        if candidate and path_exists(candidate):
            return WorkbenchConfigItem(
                name=name,
                status="可用",
                detail=f"来源：{env_key}",
                key=key,
                severity="ok",
                source=env_key,
                values={"path": candidate},
            )
    for path_name in path_names:
        found = which(path_name)
        if found:
            return WorkbenchConfigItem(
                name=name,
                status="可用",
                detail=f"来源：PATH/{path_name}",
                key=key,
                severity="ok",
                source="PATH",
                values={"path": found},
            )
    return WorkbenchConfigItem(
        name=name,
        status="未配置",
        detail=missing_message,
        key=key,
        severity="warning",
        source="not-found",
        missing_keys=list(env_keys),
        action_hint=missing_message,
    )


def _docling_item(can_import: CanImport) -> WorkbenchConfigItem:
    if not can_import("docling"):
        return WorkbenchConfigItem(
            name="Docling",
            status="未安装",
            detail="Docling 未安装，DOCX 主解析不可用。",
            key="docling",
            severity="warning",
            source="python-import",
            action_hint="在当前 Python 环境安装 docling。",
        )
    return WorkbenchConfigItem(
        name="Docling",
        status="可导入",
        detail="DOCX 主解析和 PDF fallback 可用。",
        key="docling",
        severity="ok",
        source="python-import",
    )


def _llm_item(env: Mapping[str, str]) -> WorkbenchConfigItem:
    has_key = bool(env.get("OPENAI_API_KEY"))
    provider = env.get("LLM_PROVIDER", "openai-compatible" if has_key else "local")
    mode = "real" if has_key else "fake"
    return WorkbenchConfigItem(
        name="LLM",
        status=mode,
        detail=(
            "真实 LLM API 已配置。"
            if has_key
            else "当前使用本地 deterministic fallback，不是真实 API。"
        ),
        key="llm",
        severity="ok" if has_key else "warning",
        source="env" if has_key else "fallback",
        values={
            "provider": provider,
            "model": env.get("OPENAI_MODEL") or env.get("LLM_MODEL", ""),
            "api_key": "已设置" if has_key else "未设置",
            "base_url": env.get("OPENAI_BASE_URL", ""),
        },
    )


def _deepseek_agent_item(env: Mapping[str, str]) -> WorkbenchConfigItem:
    has_dedicated_key = bool(env.get("DEEPSEEK_API_KEY", "").strip())
    has_llm_key = bool(env.get("OPENAI_API_KEY", "").strip())
    has_key = has_dedicated_key or has_llm_key
    source = "env" if has_dedicated_key else "llm" if has_llm_key else "not-configured"
    api_key_label = (
        "已设置"
        if has_dedicated_key
        else "已复用 LLM"
        if has_llm_key
        else "未设置"
    )
    return WorkbenchConfigItem(
        name="DeepSeek 总 Agent",
        status="ready" if has_key else "not-configured",
        detail=(
            "总览页中文问答可用。"
            if has_dedicated_key
            else "总览页中文问答将复用 LLM 配置。"
            if has_key
            else "配置 DEEPSEEK_API_KEY 后启用总览页中文问答。"
        ),
        key="deepseek_agent",
        severity="ok" if has_key else "info",
        source=source,
        values={
            "api_key": api_key_label,
            "base_url": (
                env.get("DEEPSEEK_BASE_URL")
                or env.get("OPENAI_BASE_URL")
                or "https://api.deepseek.com"
            ),
            "model": (
                env.get("DEEPSEEK_MODEL")
                or env.get("OPENAI_MODEL")
                or env.get("LLM_MODEL")
                or "deepseek-v4-flash"
            ),
        },
    )


def _codex_review_item(env: Mapping[str, str]) -> WorkbenchConfigItem:
    has_key = bool(env.get("CODEX_REVIEW_API_KEY") or env.get("OPENAI_API_KEY"))
    return WorkbenchConfigItem(
        name="Codex Review",
        status="可自动复审" if has_key else "未配置",
        detail="自动复审可用。" if has_key else "未配置 review API key，仍允许人工审核。",
        key="codex_review",
        severity="ok" if has_key else "info",
        source="env" if has_key else "not-configured",
        values={
            "api_key": "已设置" if has_key else "未设置",
            "base_url": env.get("CODEX_REVIEW_BASE_URL") or env.get("OPENAI_BASE_URL", ""),
            "model": env.get("CODEX_REVIEW_MODEL") or env.get("OPENAI_MODEL", ""),
        },
    )


def _archive_external_item(which: Which) -> WorkbenchConfigItem:
    for name in ("7z", "7zz", "7za", "unrar"):
        found = which(name)
        if found:
            return WorkbenchConfigItem(
                name="RAR / 7Z",
                status="可用",
                detail=f"来源：PATH/{name}",
                key="rar_7z",
                severity="ok",
                source="PATH",
                values={"adapter": name, "path": found},
            )
    return WorkbenchConfigItem(
        name="RAR / 7Z",
        status="缺少外部工具",
        detail="代码可识别 RAR/7Z，但需要 7z、7zz、7za 或 unrar。",
        key="rar_7z",
        severity="warning",
        source="not-found",
        missing_keys=["7z", "7zz", "7za", "unrar"],
        action_hint="安装 7-Zip 或 unrar 并加入 PATH。",
    )


def _path_exists(path: str | Path) -> bool:
    return Path(path).exists()


def _can_import(module_name: str) -> bool:
    try:
        __import__(module_name)
    except Exception:
        return False
    return True
