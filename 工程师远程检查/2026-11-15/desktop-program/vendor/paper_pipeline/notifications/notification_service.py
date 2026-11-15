"""Notification adapters for backend events and user-facing delivery.

The fake and file adapters remain the deterministic test/storage boundary. Real
Windows toast delivery lives in ``windows_adapter.py`` and falls back to these
safe adapters when the desktop dependency or platform is unavailable.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from paper_pipeline.notifications.notification_models import (
    NotificationEvent,
    NotificationLevel,
)


class NotificationAdapter(Protocol):
    def send(self, event: NotificationEvent) -> NotificationEvent:
        """Deliver an event through a safe backend adapter."""


@dataclass(slots=True)
class FakeNotificationAdapter:
    sent_events: list[NotificationEvent] = field(default_factory=list)
    real_system_notifications_sent: int = 0

    def send(self, event: NotificationEvent) -> NotificationEvent:
        self.sent_events.append(event)
        return event


@dataclass(slots=True)
class FileNotificationAdapter:
    path: str | Path

    def send(self, event: NotificationEvent) -> NotificationEvent:
        destination = Path(self.path)
        _ensure_not_input_raw(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event.to_dict(), ensure_ascii=False, sort_keys=True))
            handle.write("\n")
        return event


@dataclass(slots=True)
class NotificationService:
    adapter: NotificationAdapter = field(default_factory=FakeNotificationAdapter)

    def notify(
        self,
        *,
        run_id: str,
        scenario: str,
        level: NotificationLevel | str,
        title: str,
        message: str,
        evidence: dict[str, object] | None = None,
    ) -> NotificationEvent:
        event = NotificationEvent(
            run_id=run_id,
            scenario=scenario,
            level=level,
            title=title,
            message=message,
            evidence=dict(evidence or {}),
        )
        return self.adapter.send(event)


def _ensure_not_input_raw(path: Path) -> None:
    normalized = str(path).lower().replace("/", "\\")
    if "input_raw" in path.parts or "\\input_raw\\" in normalized:
        raise ValueError("notifications must not be written under input_raw")
