"""Measure concurrency and timing from automatic-pipeline progress logs.

The tool is deliberately read-only and standard-library-only.  It accepts one
or more ``progress.jsonl`` files or a directory containing run directories.
Unreadable files and malformed lines are reported in the result and never make
the command fail.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable


_TERMINAL_STAGE = re.compile(r"^(?:通过|不通过|失败|停止|已停止|完成|跳过|取消|中断)")


def _empty_report() -> dict[str, Any]:
    return {
        "progress_files": 0,
        "events": 0,
        "file_count": 0,
        "peak_concurrency": 0,
        "average_concurrency": 0.0,
        "wall_clock_seconds": 0.0,
        "average_file_seconds": 0.0,
        "stage_seconds": {},
        "skipped_files": [],
        "skipped_lines": 0,
    }


def _parse_time(value: Any, previous: float | None) -> float | None:
    """Parse ISO, clock-only, or numeric timestamps into seconds."""

    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        pass
    try:
        normalized = text.replace("Z", "+00:00")
        parsed = dt.datetime.fromisoformat(normalized)
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return parsed.timestamp()
    except ValueError:
        pass
    for fmt in ("%H:%M:%S.%f", "%H:%M:%S"):
        try:
            parsed = dt.datetime.strptime(text, fmt)
            seconds = (
                parsed.hour * 3600
                + parsed.minute * 60
                + parsed.second
                + parsed.microsecond / 1_000_000
            )
            if previous is not None:
                while seconds < previous:
                    seconds += 24 * 3600
            return seconds
        except ValueError:
            continue
    return None


def _iter_progress_files(inputs: Iterable[str | Path]) -> tuple[list[Path], list[dict[str, str]]]:
    files: list[Path] = []
    skipped: list[dict[str, str]] = []
    for raw in inputs:
        try:
            path = Path(raw)
            if path.is_file():
                files.append(path)
            elif path.is_dir():
                files.extend(sorted(path.rglob("progress.jsonl")))
            else:
                skipped.append({"path": str(path), "error": "路径不存在或不是文件/目录"})
        except Exception as exc:  # pragma: no cover - defensive CLI boundary
            skipped.append({"path": str(raw), "error": f"无法访问路径：{exc}"})
    unique = list(dict.fromkeys(files))
    return unique, skipped


def _read_events(path: Path) -> tuple[list[tuple[float, str, str, int]], int, str | None]:
    events: list[tuple[float, str, str, int]] = []
    skipped_lines = 0
    previous_time: float | None = None
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line_number, raw in enumerate(handle, 1):
                if not raw.strip():
                    continue
                try:
                    row = json.loads(raw)
                    if not isinstance(row, dict):
                        raise ValueError("JSON 行不是对象")
                    timestamp = _parse_time(row.get("time", row.get("timestamp")), previous_time)
                    if timestamp is None:
                        raise ValueError("缺少可解析的 time/timestamp")
                    stage = str(row.get("stage", "")).strip()
                    filename = str(row.get("file", row.get("filename", row.get("paper_id", ""))) or "").strip()
                    events.append((timestamp, stage, filename, line_number))
                    previous_time = timestamp
                except Exception:
                    skipped_lines += 1
        return events, skipped_lines, None
    except Exception as exc:
        return [], skipped_lines, str(exc)


def _run_metrics(events: list[tuple[float, str, str, int]]) -> dict[str, Any]:
    if not events:
        return {
            "events": 0,
            "file_count": 0,
            "peak": 0,
            "active_time": 0.0,
            "wall": 0.0,
            "file_durations": [],
            "stage_seconds": {},
        }
    events.sort(key=lambda item: (item[0], item[3]))
    active: set[str] = set()
    starts: dict[str, float] = {}
    last_file_time: dict[str, float] = {}
    last_file_stage: dict[str, str] = {}
    file_durations: list[float] = []
    stage_seconds: defaultdict[str, float] = defaultdict(float)
    active_time = 0.0
    peak = 0
    first_time = events[0][0]
    previous_time = first_time

    for timestamp, stage, filename, _line_number in events:
        delta = max(0.0, timestamp - previous_time)
        active_time += delta * len(active)
        previous_time = timestamp
        if filename:
            prior_time = last_file_time.get(filename)
            prior_stage = last_file_stage.get(filename)
            if prior_time is not None and prior_stage:
                stage_seconds[prior_stage] += max(0.0, timestamp - prior_time)
            last_file_time[filename] = timestamp
            last_file_stage[filename] = stage
            if filename not in starts:
                starts[filename] = timestamp
                active.add(filename)
            if _TERMINAL_STAGE.match(stage):
                start = starts.pop(filename, timestamp)
                file_durations.append(max(0.0, timestamp - start))
                active.discard(filename)
                last_file_stage.pop(filename, None)
                last_file_time.pop(filename, None)
        peak = max(peak, len(active))

    last_time = events[-1][0]
    if active:
        for filename in list(active):
            file_durations.append(max(0.0, last_time - starts.get(filename, last_time)))
    return {
        "events": len(events),
        "file_count": len(file_durations),
        "peak": peak,
        "active_time": active_time,
        "wall": max(0.0, last_time - first_time),
        "file_durations": file_durations,
        "stage_seconds": dict(stage_seconds),
    }


def measure_progress(inputs: Iterable[str | Path]) -> dict[str, Any]:
    """Return robust timing metrics for the supplied progress logs."""

    report = _empty_report()
    files, path_skips = _iter_progress_files(inputs)
    report["progress_files"] = len(files)
    report["skipped_files"].extend(path_skips)
    total_active_time = 0.0
    total_wall = 0.0
    durations: list[float] = []
    stages: defaultdict[str, float] = defaultdict(float)
    for path in files:
        events, skipped_lines, error = _read_events(path)
        report["skipped_lines"] += skipped_lines
        if error:
            report["skipped_files"].append({"path": str(path), "error": error})
            continue
        metrics = _run_metrics(events)
        report["events"] += metrics["events"]
        report["file_count"] += metrics["file_count"]
        report["peak_concurrency"] = max(report["peak_concurrency"], metrics["peak"])
        total_active_time += metrics["active_time"]
        total_wall += metrics["wall"]
        durations.extend(metrics["file_durations"])
        for stage, seconds in metrics["stage_seconds"].items():
            stages[stage] += seconds
    report["wall_clock_seconds"] = round(total_wall, 3)
    report["average_concurrency"] = round(total_active_time / total_wall, 6) if total_wall else 0.0
    report["average_file_seconds"] = round(sum(durations) / len(durations), 3) if durations else 0.0
    report["stage_seconds"] = {stage: round(seconds, 3) for stage, seconds in sorted(stages.items())}
    return report


def _human(report: dict[str, Any]) -> str:
    lines = [
        f"progress 文件数：{report['progress_files']}",
        f"事件数：{report['events']}",
        f"文件数：{report['file_count']}",
        f"最大同时处理文件数：{report['peak_concurrency']}",
        f"平均同时处理文件数：{report['average_concurrency']}",
        f"总墙钟：{report['wall_clock_seconds']} 秒",
        f"每篇平均秒数：{report['average_file_seconds']} 秒",
        "各阶段耗时之和：",
    ]
    lines.extend(f"  - {stage}: {seconds} 秒" for stage, seconds in report["stage_seconds"].items())
    lines.append(f"跳过的坏行：{report['skipped_lines']}")
    if report["skipped_files"]:
        lines.append("跳过的文件/路径：")
        lines.extend(f"  - {item['path']}: {item['error']}" for item in report["skipped_files"])
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="*", default=["data/runs"], help="progress.jsonl 文件或 runs 目录")
    parser.add_argument("--json", action="store_true", dest="as_json", help="输出 JSON")
    args = parser.parse_args(argv)
    try:
        report = measure_progress(args.inputs)
        output = json.dumps(report, ensure_ascii=False, indent=2) if args.as_json else _human(report)
        print(output)
        return 0
    except Exception as exc:  # never let a diagnostic tool crash the pipeline
        fallback = _empty_report()
        fallback["skipped_files"].append({"path": "<command>", "error": str(exc)})
        print(json.dumps(fallback, ensure_ascii=False, indent=2) if args.as_json else _human(fallback))
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
