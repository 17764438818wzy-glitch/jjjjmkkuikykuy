"""User-readable workbench messages."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class WorkbenchUserMessage:
    severity: str
    title: str
    message: str
    file_path: str = ""
    stage: str = ""
    tool: str = ""
    reason: str = ""
    source_error: str = ""
    action_hint: str = ""


KNOWN_ERRORS = {
    "MinerU executable is not configured": (
        "warning",
        "未找到 MinerU",
        "PDF 主解析不可用，请配置 MINERU_PATH 或把 MinerU 加入 PATH。",
        "配置 MINERU_PATH 或安装 MinerU。",
    ),
    "LibreOffice executable was not configured": (
        "warning",
        "未找到 LibreOffice",
        "DOC/DOCX 转换不可用，请配置 LIBREOFFICE_PATH 或把 LibreOffice 加入 PATH。",
        "安装 LibreOffice 并确认 soffice 可执行文件可访问。",
    ),
    "SQL upload remains in dry-run mode": (
        "info",
        "当前不会真实写入 PostgreSQL",
        "SQL 模式是 dry-run，上传区只能展示预览，不会写库。",
        "确认审核和预览后再启用 real 模式。",
    ),
    "Docling Python API is not installed": (
        "warning",
        "Docling 未安装",
        "DOCX 主解析不可用，系统会显示 fallback 路线或 pending 原因。",
        "在当前 Python 环境安装 docling。",
    ),
}


def message_from_error(
    source_error: str,
    *,
    file_path: str = "",
    stage: str = "",
    tool: str = "",
) -> WorkbenchUserMessage:
    if source_error in KNOWN_ERRORS:
        severity, title, message, action_hint = KNOWN_ERRORS[source_error]
        return WorkbenchUserMessage(
            severity=severity,
            title=title,
            message=message,
            file_path=file_path,
            stage=stage,
            tool=tool,
            reason=source_error,
            source_error=source_error,
            action_hint=action_hint,
        )
    return WorkbenchUserMessage(
        severity="error",
        title="处理失败",
        message="处理过程中出现未知错误，请查看详细原因。",
        file_path=file_path,
        stage=stage,
        tool=tool,
        reason=source_error,
        source_error=source_error,
        action_hint="根据文件、阶段、工具和详细原因定位问题。",
    )
