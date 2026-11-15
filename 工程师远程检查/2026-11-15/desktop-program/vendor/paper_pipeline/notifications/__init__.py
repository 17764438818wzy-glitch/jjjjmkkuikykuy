"""Notification event models and safe adapters."""

from paper_pipeline.notifications.notification_models import (
    NotificationEvent,
    NotificationLevel,
)
from paper_pipeline.notifications.notification_service import (
    FakeNotificationAdapter,
    FileNotificationAdapter,
    NotificationAdapter,
    NotificationService,
)
from paper_pipeline.notifications.windows_adapter import WindowsNotificationAdapter

__all__ = [
    "FakeNotificationAdapter",
    "FileNotificationAdapter",
    "NotificationAdapter",
    "NotificationEvent",
    "NotificationLevel",
    "NotificationService",
    "WindowsNotificationAdapter",
]
