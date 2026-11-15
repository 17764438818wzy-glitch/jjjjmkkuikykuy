"""Structured action results returned by the workbench controller."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from paper_pipeline.app.app_state import WorkbenchState


@dataclass(slots=True)
class WorkbenchActionResult:
    action: str
    accepted: bool
    message: str
    state: WorkbenchState | None = None
    data: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    requires_confirmation: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["state"] = self.state.to_dict() if self.state is not None else None
        return payload
