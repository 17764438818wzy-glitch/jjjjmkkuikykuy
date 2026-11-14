"""Safe tray adapter contract for the Windows workbench."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from paper_pipeline.app.app_state import WorkbenchState


@dataclass(frozen=True, slots=True)
class TrayMenuItem:
    label: str
    action: str
    enabled: bool = True
    requires_confirmation: bool = False


@dataclass(slots=True)
class WorkbenchTrayAdapter:
    real_tray_icons_created: int = 0

    def hover_text(self, state: WorkbenchState) -> str:
        run_id = state.current_run_id or "not_started"
        return (
            f"Paper Pipeline Workbench | run_id={run_id} | "
            f"status={state.run_status} | "
            f"pending={state.pending_count} | failed={state.failed_count}"
        )

    def menu_items(self, state: WorkbenchState) -> list[TrayMenuItem]:
        return [
            TrayMenuItem("Open workbench", "open_workbench"),
            TrayMenuItem(
                "Show status",
                "show_status",
                enabled=True,
            ),
            TrayMenuItem(
                "Exit",
                "exit",
                requires_confirmation=state.run_status == "running",
            ),
        ]

    def close_window_options(self, state: WorkbenchState) -> list[str]:
        if state.run_status == "running":
            return ["Run in background", "Cancel close"]
        return ["Close window"]


PystrayLoader = Callable[[], Any]
IconFactory = Callable[[], Any]
TrayCallback = Callable[[], None]


def _default_tray_icon() -> Any:
    try:
        from PIL.Image import new as new_image  # type: ignore[import-not-found]
        from PIL.ImageDraw import Draw  # type: ignore[import-not-found]
    except Exception:
        return None

    image = new_image("RGBA", (64, 64), (30, 80, 120, 255))
    draw = Draw(image)
    draw.rectangle((14, 16, 50, 22), fill=(255, 255, 255, 255))
    draw.rectangle((14, 30, 42, 36), fill=(255, 255, 255, 255))
    draw.rectangle((14, 44, 34, 50), fill=(255, 255, 255, 255))
    return image


def _noop() -> None:
    return None


@dataclass(slots=True)
class WindowsTrayIconAdapter(WorkbenchTrayAdapter):
    """Create a real Windows tray icon while keeping the controller testable."""

    pystray_loader: PystrayLoader | None = None
    icon_factory: IconFactory = _default_tray_icon
    on_open: TrayCallback = _noop
    on_show_status: TrayCallback = _noop
    on_exit: TrayCallback = _noop
    _icon: Any | None = None

    def create_icon(self, state: WorkbenchState) -> Any | None:
        pystray = self._load_pystray()
        if pystray is None:
            return None
        menu = pystray.Menu(
            *[
                pystray.MenuItem(
                    item.label,
                    self._callback_for(item.action),
                    enabled=item.enabled,
                    default=item.action == "open_workbench",
                )
                for item in self.menu_items(state)
            ],
        )
        self._icon = pystray.Icon(
            "paper-pipeline-workbench",
            self.icon_factory(),
            self.hover_text(state),
            menu,
        )
        self.real_tray_icons_created += 1
        return self._icon

    def run_detached(self, state: WorkbenchState) -> bool:
        icon = self._icon or self.create_icon(state)
        if icon is None:
            return False
        run_detached = getattr(icon, "run_detached", None)
        if run_detached is None:
            return False
        run_detached()
        return True

    def stop(self) -> None:
        if self._icon is not None:
            stop = getattr(self._icon, "stop", None)
            if stop is not None:
                stop()

    def _callback_for(self, action: str) -> Callable[[Any, Any], None]:
        def callback(icon: Any, item: Any) -> None:
            if action == "open_workbench":
                self.on_open()
            elif action == "show_status":
                self.on_show_status()
            elif action == "exit":
                self.on_exit()
                stop = getattr(icon, "stop", None)
                if stop is not None:
                    stop()

        return callback

    def _load_pystray(self) -> Any | None:
        if self.pystray_loader is not None:
            try:
                return self.pystray_loader()
            except Exception:
                return None
        try:
            import pystray  # type: ignore[import-not-found,import-untyped]
        except Exception:
            return None
        return pystray
