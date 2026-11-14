"""Local workbench entrypoints and safe user-facing adapters."""

from paper_pipeline.app.actions import WorkbenchActionResult
from paper_pipeline.app.app_state import WorkbenchState, WorkbenchStatusSummary
from paper_pipeline.app.controller import WorkbenchController
from paper_pipeline.app.tray import WindowsTrayIconAdapter

__all__ = [
    "WorkbenchActionResult",
    "WorkbenchController",
    "WorkbenchState",
    "WorkbenchStatusSummary",
    "WindowsTrayIconAdapter",
]
