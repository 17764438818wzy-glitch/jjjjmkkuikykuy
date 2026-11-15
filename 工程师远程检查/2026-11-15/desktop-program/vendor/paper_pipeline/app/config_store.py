"""Workbench-owned configuration storage and editable field models."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path

CONFIG_FILE_NAME = "workbench_config.json"
SQL_CONFIG_FILE_ENV = "PAPER_PIPELINE_SQL_CONFIG_FILE"

_SAVED_SECRET_TEXT = '<REDACTED_SECRET>'


@dataclass(frozen=True, slots=True)
class WorkbenchConfigFieldDefinition:
    env_key: str
    label: str
    required: bool = False
    secret: bool = False
    placeholder: str = ""
    help_text: str = ""


@dataclass(frozen=True, slots=True)
class WorkbenchConfigDefinition:
    key: str
    title: str
    description: str
    fields: tuple[WorkbenchConfigFieldDefinition, ...] = ()
    supports_save: bool = True
    action_hint: str = ""

    @property
    def field_keys(self) -> set[str]:
        return {field.env_key for field in self.fields}


@dataclass(frozen=True, slots=True)
class WorkbenchConfigField:
    env_key: str
    label: str
    value: str = ""
    required: bool = False
    secret: bool = False
    has_saved_value: bool = False
    source: str = ""
    placeholder: str = ""
    help_text: str = ""
    display_value: str = ""

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        if self.secret and self.has_saved_value:
            payload["value"] = ""
            payload["display_value"] = _SAVED_SECRET_TEXT
        return payload


@dataclass(frozen=True, slots=True)
class WorkbenchConfigPanel:
    key: str
    title: str
    description: str
    fields: tuple[WorkbenchConfigField, ...] = ()
    supports_save: bool = True
    config_path: str = ""
    action_hint: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "key": self.key,
            "title": self.title,
            "description": self.description,
            "fields": [field.to_dict() for field in self.fields],
            "supports_save": self.supports_save,
            "config_path": self.config_path,
            "action_hint": self.action_hint,
        }


CONFIG_DEFINITIONS: dict[str, WorkbenchConfigDefinition] = {
    "nas_input_dir": WorkbenchConfigDefinition(
        key="nas_input_dir",
        title="NAS 输入目录",
        description="选择 real_flow_50 默认读取的论文输入目录。",
        fields=(
            WorkbenchConfigFieldDefinition(
                "PAPER_PIPELINE_INPUT_DIR",
                "输入目录",
                required=True,
            ),
        ),
    ),
    "postgresql": WorkbenchConfigDefinition(
        key="postgresql",
        title="PostgreSQL",
        description="配置真实 SQL 上传所需的连接信息。上传仍需审核、预览和二次确认。",
        fields=(
            WorkbenchConfigFieldDefinition("PAPER_PIPELINE_SQL_MODE", "SQL 模式"),
            WorkbenchConfigFieldDefinition("PAPER_PIPELINE_SQL_ADAPTER_MODE", "适配器模式"),
            WorkbenchConfigFieldDefinition("SQL_UPLOAD_MODE", "上传模式"),
            WorkbenchConfigFieldDefinition("POSTGRES_DSN", "DSN", secret=True),
            WorkbenchConfigFieldDefinition("DATABASE_URL", "DATABASE_URL", secret=True),
            WorkbenchConfigFieldDefinition("POSTGRES_HOST", "主机"),
            WorkbenchConfigFieldDefinition("POSTGRES_PORT", "端口"),
            WorkbenchConfigFieldDefinition("POSTGRES_DB", "数据库"),
            WorkbenchConfigFieldDefinition("POSTGRES_DATABASE", "数据库别名"),
            WorkbenchConfigFieldDefinition("POSTGRES_USER", "用户"),
            WorkbenchConfigFieldDefinition("POSTGRES_PASSWORD", "密码", secret=True),
            WorkbenchConfigFieldDefinition("POSTGRES_PASSWORD_ENV", "密码环境变量名"),
            WorkbenchConfigFieldDefinition("POSTGRES_TABLE", "目标表"),
            WorkbenchConfigFieldDefinition("POSTGRES_SSLMODE", "SSL 模式"),
            WorkbenchConfigFieldDefinition("POSTGRES_CONNECT_TIMEOUT", "连接超时秒数"),
        ),
    ),
    "mineru": WorkbenchConfigDefinition(
        key="mineru",
        title="MinerU",
        description="配置 PDF 主解析器路径和超时。",
        fields=(
            WorkbenchConfigFieldDefinition("MINERU_PATH", "MinerU 可执行文件"),
            WorkbenchConfigFieldDefinition("MINERU_TIMEOUT_SECONDS", "超时秒数"),
            WorkbenchConfigFieldDefinition("MINERU_TEST_PDF", "健康检查 PDF"),
        ),
    ),
    "libreoffice": WorkbenchConfigDefinition(
        key="libreoffice",
        title="LibreOffice / LO",
        description="配置 Office 文档转换工具路径和超时。",
        fields=(
            WorkbenchConfigFieldDefinition("LIBREOFFICE_PATH", "LibreOffice 可执行文件"),
            WorkbenchConfigFieldDefinition("LIBREOFFICE_TIMEOUT_SECONDS", "超时秒数"),
        ),
    ),
    "docling": WorkbenchConfigDefinition(
        key="docling",
        title="Docling",
        description="Docling 由当前 Python 环境提供，不保存路径配置。",
        supports_save=False,
        action_hint="请在当前 Python 环境安装 docling 后刷新状态。",
    ),
    "llm": WorkbenchConfigDefinition(
        key="llm",
        title="LLM",
        description="配置质量判断相关的 OpenAI 兼容模型。",
        fields=(
            WorkbenchConfigFieldDefinition("LLM_PROVIDER", "Provider"),
            WorkbenchConfigFieldDefinition("OPENAI_API_KEY", "API Key", secret=True),
            WorkbenchConfigFieldDefinition("OPENAI_BASE_URL", "Base URL"),
            WorkbenchConfigFieldDefinition("OPENAI_MODEL", "模型"),
            WorkbenchConfigFieldDefinition("LLM_MODEL", "模型别名"),
            WorkbenchConfigFieldDefinition("LLM_TIMEOUT_SECONDS", "超时秒数"),
        ),
    ),
    "deepseek_agent": WorkbenchConfigDefinition(
        key="deepseek_agent",
        title="DeepSeek 总 Agent",
        description="配置总览页中文问答助手。默认复用 LLM 配置，下面的 DeepSeek 项仅用于单独覆盖。",
        fields=(
            WorkbenchConfigFieldDefinition(
                "DEEPSEEK_API_KEY",
                "API Key",
                secret=True,
                help_text="留空时自动复用 LLM 的 OPENAI_API_KEY。",
            ),
            WorkbenchConfigFieldDefinition(
                "DEEPSEEK_BASE_URL",
                "Base URL",
                placeholder="https://api.deepseek.com",
                help_text="默认使用 DeepSeek 官方 OpenAI 兼容地址。",
            ),
            WorkbenchConfigFieldDefinition(
                "DEEPSEEK_MODEL",
                "模型",
                placeholder="deepseek-v4-flash",
            ),
            WorkbenchConfigFieldDefinition(
                "DEEPSEEK_TIMEOUT_SECONDS",
                "超时秒数",
                placeholder="30",
            ),
        ),
    ),
    "codex_review": WorkbenchConfigDefinition(
        key="codex_review",
        title="Codex Review",
        description="配置 pending 审核候选的自动复审能力。",
        fields=(
            WorkbenchConfigFieldDefinition("CODEX_REVIEW_API_KEY", "API Key", secret=True),
            WorkbenchConfigFieldDefinition("CODEX_REVIEW_BASE_URL", "Base URL"),
            WorkbenchConfigFieldDefinition("CODEX_REVIEW_MODEL", "模型"),
            WorkbenchConfigFieldDefinition("CODEX_REVIEW_TIMEOUT_SECONDS", "超时秒数"),
        ),
    ),
    "zip": WorkbenchConfigDefinition(
        key="zip",
        title="ZIP",
        description="ZIP 使用 Python 标准库内置支持，不需要保存配置。",
        supports_save=False,
    ),
    "rar_7z": WorkbenchConfigDefinition(
        key="rar_7z",
        title="RAR / 7Z",
        description="RAR / 7Z 当前通过 PATH 检测 7z、7zz、7za 或 unrar。",
        supports_save=False,
        action_hint="安装 7-Zip 或 unrar 后加入 PATH，再刷新状态。",
    ),
}

_SQL_FIELD_KEYS = CONFIG_DEFINITIONS["postgresql"].field_keys


class WorkbenchConfigStore:
    """Read and write the workbench-managed configuration JSON file."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    @classmethod
    def from_workbench_dir(cls, workbench_dir: str | Path) -> WorkbenchConfigStore:
        return cls(Path(workbench_dir) / CONFIG_FILE_NAME)

    def load(self) -> dict[str, str]:
        if not self.path.exists():
            return {}
        try:
            decoded = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        if not isinstance(decoded, Mapping):
            return {}
        return {
            str(key): str(value)
            for key, value in decoded.items()
            if str(key).strip() and str(value).strip()
        }

    def save_item(self, item_key: str, values: Mapping[str, object]) -> list[str]:
        definition = _definition_for(item_key)
        if not definition.supports_save:
            return []
        allowed_fields = {field.env_key: field for field in definition.fields}
        current = self.load()
        saved_keys: list[str] = []
        for key, value in values.items():
            field = allowed_fields.get(str(key))
            if field is None:
                continue
            raw = str(value).strip()
            if field.secret and not raw and current.get(field.env_key):
                continue
            if raw:
                current[field.env_key] = raw
                saved_keys.append(field.env_key)
            else:
                current.pop(field.env_key, None)
                saved_keys.append(field.env_key)
        self._write(current)
        return saved_keys

    def clear_item(self, item_key: str) -> list[str]:
        definition = _definition_for(item_key)
        current = self.load()
        removed: list[str] = []
        for key in definition.field_keys:
            if key in current:
                current.pop(key)
                removed.append(key)
        self._write(current)
        return sorted(removed)

    def build_environ(self, environ: Mapping[str, str] | None = None) -> dict[str, str]:
        merged = dict(environ if environ is not None else os.environ)
        overrides = self.build_runtime_overrides()
        merged.update(overrides)
        return merged

    def build_runtime_overrides(self) -> dict[str, str]:
        saved = self.load()
        overrides = dict(saved)
        if _SQL_FIELD_KEYS.intersection(saved):
            overrides[SQL_CONFIG_FILE_ENV] = str(self.path)
        return overrides

    def build_panel(
        self,
        item_key: str,
        *,
        environ: Mapping[str, str] | None = None,
    ) -> WorkbenchConfigPanel | None:
        definition = CONFIG_DEFINITIONS.get(item_key)
        if definition is None:
            return None
        saved = self.load()
        source_env = environ if environ is not None else os.environ
        merged = dict(source_env)
        merged.update(saved)
        fields = tuple(
            _field_from_definition(field, saved=saved, merged=merged, source_env=source_env)
            for field in definition.fields
        )
        return WorkbenchConfigPanel(
            key=definition.key,
            title=definition.title,
            description=definition.description,
            fields=fields,
            supports_save=definition.supports_save,
            config_path=str(self.path),
            action_hint=definition.action_hint,
        )

    def _write(self, values: Mapping[str, str]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(dict(sorted(values.items())), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


@contextmanager
def patched_environ(overrides: Mapping[str, str]) -> Iterator[None]:
    original = {key: os.environ.get(key) for key in overrides}
    os.environ.update({key: str(value) for key, value in overrides.items()})
    try:
        yield
    finally:
        for key, value in original.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _definition_for(item_key: str) -> WorkbenchConfigDefinition:
    definition = CONFIG_DEFINITIONS.get(item_key)
    if definition is None:
        raise KeyError(f"unknown workbench config item: {item_key}")
    return definition


def _field_from_definition(
    definition: WorkbenchConfigFieldDefinition,
    *,
    saved: Mapping[str, str],
    merged: Mapping[str, str],
    source_env: Mapping[str, str],
) -> WorkbenchConfigField:
    has_saved_value = bool(saved.get(definition.env_key))
    raw_value = str(merged.get(definition.env_key, "")).strip()
    source = ""
    if has_saved_value:
        source = "workbench_config"
    elif str(source_env.get(definition.env_key, "")).strip():
        source = "environment"
    visible_value = "" if definition.secret and has_saved_value else raw_value
    display_value = _SAVED_SECRET_TEXT if definition.secret and has_saved_value else raw_value
    return WorkbenchConfigField(
        env_key=definition.env_key,
        label=definition.label,
        value=visible_value,
        required=definition.required,
        secret=definition.secret,
        has_saved_value=has_saved_value,
        source=source,
        placeholder=definition.placeholder,
        help_text=definition.help_text,
        display_value=display_value,
    )
