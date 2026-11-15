"""Windows notification adapter with safe fallback delivery."""

from __future__ import annotations

import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from paper_pipeline.notifications.notification_models import NotificationEvent
from paper_pipeline.notifications.notification_service import (
    FakeNotificationAdapter,
    NotificationAdapter,
)

NotifierLoader = Callable[[], Any]


@dataclass(slots=True)
class WindowsNotificationAdapter:
    fallback_adapter: NotificationAdapter = field(default_factory=FakeNotificationAdapter)
    allow_real_notifications: bool = True
    notifier_loader: NotifierLoader | None = None
    real_system_notifications_sent: int = 0

    def send(self, event: NotificationEvent) -> NotificationEvent:
        if not self.allow_real_notifications or sys.platform != "win32":
            return self.fallback_adapter.send(event)

        notifier = self._load_notifier()
        if notifier is None:
            return self.fallback_adapter.send(event)

        try:
            notifier.notify(
                title=event.title,
                message=_safe_message(event),
                app_name="论文流水线工作台",
            )
        except Exception:
            return self.fallback_adapter.send(event)

        self.real_system_notifications_sent += 1
        return event

    def _load_notifier(self) -> Any | None:
        if self.notifier_loader is not None:
            try:
                return self.notifier_loader()
            except Exception:
                return None
        try:
            from plyer import notification  # type: ignore[import-not-found]
        except Exception:
            return None
        return notification


def _safe_message(event: NotificationEvent) -> str:
    evidence = event.evidence
    parts = [
        f"run_id={event.run_id}",
        f"scenario={event.scenario}",
        f"pending={evidence.get('pending_count', '')}",
        f"failed={evidence.get('failed_count', '')}",
    ]
    suggested = evidence.get("suggested_next_action")
    if suggested:
        parts.append(f"next={suggested}")
    return " ".join(part for part in parts if not part.endswith("="))
