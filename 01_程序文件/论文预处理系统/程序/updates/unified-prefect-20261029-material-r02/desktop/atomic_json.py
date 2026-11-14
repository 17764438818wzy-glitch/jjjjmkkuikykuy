"""Shared native JSON persistence; retries a local rename, never business work."""
import json
from pathlib import Path
import time
import uuid


def replace_generated(temp, path):
    # Reuse the existing engine writer's Windows policy, with a fixed deadline.
    deadline = time.monotonic() + 2.0
    delay = .01
    while True:
        try:
            temp.replace(path)
            return
        except OSError as exc:
            if getattr(exc, "winerror", None) not in {5, 32, 33}:
                raise
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise
            time.sleep(min(delay, remaining))
            delay = min(delay * 2, .2)


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + f".{uuid.uuid4().hex}.tmp")
    try:
        temp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
        replace_generated(temp, path)
    finally:
        try:
            temp.unlink(missing_ok=True)
        except OSError:
            pass
