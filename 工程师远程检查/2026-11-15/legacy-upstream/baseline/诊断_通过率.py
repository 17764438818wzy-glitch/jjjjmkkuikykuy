#!/usr/bin/env python3
"""Read-only pass-rate diagnostic for recorded pipeline runs.

The gate compares reports produced by this file with the same source-path
deduplication rule before and after the repair line.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


TOOL_VERSION = "source-path-dedup-1.0.0"


def _read_manifest(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"manifest must be an object: {path}")
    return value


def analyze(root: str | Path) -> dict[str, Any]:
    """Analyze ``root/data/runs/*/manifest.json`` without changing any file."""

    root_path = Path(root).expanduser().resolve()
    manifest_paths = sorted((root_path / "data" / "runs").glob("*/manifest.json"))
    raw_items = 0
    rows: list[tuple[str, dict[str, Any]]] = []
    errors: list[str] = []

    for manifest_path in manifest_paths:
        try:
            manifest = _read_manifest(manifest_path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"{manifest_path}: {exc}")
            continue
        run_id = str(manifest.get("run_id") or manifest_path.parent.name)
        items = manifest.get("items") or []
        if not isinstance(items, list):
            errors.append(f"{manifest_path}: items is not an array")
            continue
        raw_items += len(items)
        for item in items:
            if not isinstance(item, dict):
                continue
            source_path = str(item.get("source_path") or "").strip()
            if source_path:
                rows.append((run_id, item))

    # Sorting by run_id reproduces the frozen gate rule: the latest run wins
    # when the same source_path appears in multiple recorded runs.
    latest: dict[str, tuple[str, dict[str, Any]]] = {}
    for run_id, item in sorted(rows, key=lambda row: row[0]):
        latest[str(item["source_path"]).casefold()] = (run_id, item)

    passed = sum(item.get("status") == "pass" for _, item in latest.values())
    reasons: Counter[str] = Counter()
    selected_runs: defaultdict[str, dict[str, int]] = defaultdict(lambda: {"items": 0, "passed": 0})
    for run_id, item in latest.values():
        selected_runs[run_id]["items"] += 1
        if item.get("status") == "pass":
            selected_runs[run_id]["passed"] += 1
        else:
            reasons[str(item.get("reason_kind") or "unknown")] += 1

    return {
        "tool_version": TOOL_VERSION,
        "root": str(root_path),
        "manifests": len(manifest_paths),
        "raw_items": raw_items,
        "source_paths": len(rows),
        "dedup_items": len(latest),
        "passed": passed,
        "rejected": len(latest) - passed,
        "rate_percent": (passed / len(latest) * 100) if latest else 0.0,
        "reasons": dict(sorted(reasons.items())),
        "runs": {key: selected_runs[key] for key in sorted(selected_runs)},
        "errors": errors,
    }


def render_report(result: dict[str, Any]) -> str:
    lines = [
        "# 通过率基线（修复前）",
        "",
        f"> 诊断工具：`baseline/诊断_通过率.py`，版本 `{result['tool_version']}`。",
        f"> 数据根目录：`{result['root']}`；只读扫描 `data/runs/*/manifest.json`。",
        "> 去重规则：按 `source_path.casefold()` 分组，按 `run_id` 升序扫描并保留最后一条记录。",
        "",
        f"- manifest 数：{result['manifests']}",
        f"- 原始 item 数：{result['raw_items']}",
        f"- 有 source_path 的记录：{result['source_paths']}",
        f"- 按 source_path 去重后：{result['dedup_items']}",
        f"- 通过：{result['passed']}",
        f"- 不通过：{result['rejected']}",
        f"- 通过率：{result['passed']} / {result['dedup_items']} = {result['rate_percent']:.1f}%",
        "",
        "## 不通过原因分布（去重后）",
        "",
    ]
    if result["reasons"]:
        lines.extend(f"- {key}：{value}" for key, value in result["reasons"].items())
    else:
        lines.append("- 无")
    lines.extend(["", "## 运行批次", ""])
    for run_id, summary in result["runs"].items():
        lines.append(f"- {run_id}：{summary['items']} 篇，{summary['passed']} 篇通过")
    if result["errors"]:
        lines.extend(["", "## 清单读取错误", ""])
        lines.extend(f"- {error}" for error in result["errors"])
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--version", action="version", version=TOOL_VERSION)
    args = parser.parse_args()
    result = analyze(args.root)
    report = render_report(result)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(report, encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
