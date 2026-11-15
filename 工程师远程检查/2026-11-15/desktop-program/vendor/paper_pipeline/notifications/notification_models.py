"""Notification data structures for backend-only event delivery."""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class NotificationLevel(StrEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    SUCCESS = "success"


@dataclass(slots=True)
class NotificationEvent:
    run_id: str
    scenario: str
    level: NotificationLevel | str
    title: str
    message: str
    evidence: dict[str, Any] = field(default_factory=dict)
    event_id: str = field(default_factory=lambda: f"notification-{uuid.uuid4().hex}")
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def __post_init__(self) -> None:
        if not self.run_id:
            raise ValueError("run_id is required for notifications")
        if isinstance(self.level, str):
            self.level = NotificationLevel(self.level)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        level = (
            self.level
            if isinstance(self.level, NotificationLevel)
            else NotificationLevel(self.level)
        )
        payload["level"] = level.value
        return payload
