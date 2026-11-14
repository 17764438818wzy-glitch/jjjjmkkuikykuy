"""SQL upload audit helpers."""

from __future__ import annotations

from typing import Any

from paper_pipeline.sql.sql_models import utc_now_iso


def build_sql_audit_record(
    *,
    action: str,
    operator: str,
    preview_id: str,
    batch_id: str,
    adapter: str,
    paper_id: str,
    status: str,
    message: str = "",
    record_id: str = "",
) -> dict[str, Any]:
    return {
        "action": action,
        "operator": operator,
        "preview_id": preview_id,
        "batch_id": batch_id,
        "adapter": adapter,
        "paper_id": paper_id,
        "status": status,
        "message": message,
        "record_id": record_id,
        "created_at": utc_now_iso(),
    }
