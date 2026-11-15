"""Safe path opener for workbench history actions."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable


@dataclass(frozen=True, slots=True)
class SafeOpenResult:
    accepted: bool
    message: str
    path: str = ""


@dataclass(frozen=True, slots=True)
class SafeOpenPolicy:
    allowed_roots: list[Path]

    def is_allowed(self, path: str | Path) -> bool:
        candidate = Path(path).resolve()
        if not candidate.exists():
            return False
        for root in self.allowed_roots:
            resolved_root = root.resolve()
            try:
                candidate.relative_to(resolved_root)
            except ValueError:
                continue
            return True
        return False


class SafeOpener:
    def __init__(
        self,
        policy: SafeOpenPolicy,
        *,
        open_adapter: Callable[[Path], None],
    ) -> None:
        self.policy = policy
        self.open_adapter = open_adapter

    def open(self, path: str | Path) -> SafeOpenResult:
        candidate = Path(path).resolve()
        if not self.policy.is_allowed(candidate):
            return SafeOpenResult(False, "路径不存在或不在允许打开的工作目录内。", str(candidate))
        self.open_adapter(candidate)
        return SafeOpenResult(True, "已打开。", str(candidate))
