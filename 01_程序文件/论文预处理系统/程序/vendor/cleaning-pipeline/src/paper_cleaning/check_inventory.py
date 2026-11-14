from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from typing import Iterable
from pathlib import Path

from .inventory import iter_files, normalize_exclude_dirs


def count_files(
    input_dir: str | Path,
    exclude: Iterable[str] | None = None,
) -> int:
    input_dir = Path(input_dir)
    if not input_dir.exists() or not input_dir.is_dir():
        return 0
    return sum(1 for _ in iter_files(input_dir, mode="full", exclude=exclude))


def read_inventory(inventory_path: str | Path) -> list[dict[str, str]]:
    with Path(inventory_path).open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def format_counter(counter: Counter[str], limit: int | None = None) -> list[str]:
    if not counter:
        return ["  (none)"]
    items = counter.most_common(limit)
    return [f"  {key or '(blank)'}: {value}" for key, value in items]


def build_report(
    input_dir: str | Path,
    inventory_csv: str | Path,
    exclude: Iterable[str] | None = None,
) -> str:
    root = Path(input_dir)
    root_exists = root.exists() and root.is_dir()
    root_realpath = root.resolve() if root.exists() else root.absolute()
    rows = read_inventory(inventory_csv)

    original_file_count = count_files(root, exclude=exclude)
    exclude_dirs = sorted(normalize_exclude_dirs(exclude))
    status_counts = Counter(row.get("status", "") for row in rows)
    ext_counts = Counter(row.get("file_ext", "") for row in rows)

    missing_rows = []
    non_success_rows = []
    sha_groups: defaultdict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        relative_path = row.get("original_relative_path", "")
        if relative_path and not (root / relative_path).exists():
            missing_rows.append(row)
        if row.get("status") != "success":
            non_success_rows.append(row)
        sha256 = row.get("sha256", "")
        if sha256 and row.get("status") == "success":
            sha_groups[sha256].append(row)

    duplicate_groups = {
        sha256: group for sha256, group in sha_groups.items() if len(group) > 1
    }
    duplicate_file_count = sum(len(group) for group in duplicate_groups.values())

    lines = [
        f"输入目录: {root}",
        f"输入目录是否存在: {'yes' if root_exists else 'no'}",
        f"输入目录 realpath: {root_realpath}",
        f"排除目录: {', '.join(exclude_dirs)}",
        f"原始文件数量: {original_file_count}",
        f"inventory 记录数量: {len(rows)}",
        f"二者是否一致: {'yes' if original_file_count == len(rows) else 'no'}",
        "",
        "status 分布:",
        *format_counter(status_counts),
        "",
        "file_ext 分布前 30:",
        *format_counter(ext_counts, limit=30),
        "",
        f"路径不存在数量: {len(missing_rows)}",
        f"重复 sha256 组数: {len(duplicate_groups)}",
        f"重复文件数量: {duplicate_file_count}",
        "",
        "前 20 个路径不存在记录:",
    ]
    lines.extend(
        f"  {row.get('original_relative_path', '')}" for row in missing_rows[:20]
    )
    if not missing_rows:
        lines.append("  (none)")

    lines.extend(["", "前 20 个 status != success 记录:"])
    lines.extend(
        "  "
        + row.get("original_relative_path", "")
        + " | "
        + row.get("status", "")
        + " | "
        + row.get("error_message", "")
        for row in non_success_rows[:20]
    )
    if not non_success_rows:
        lines.append("  (none)")

    return "\n".join(lines) + "\n"


def write_report(report: str, report_path: str | Path) -> None:
    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report, encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check an inventory CSV.")
    parser.add_argument("--input", required=True, help="Input directory.")
    parser.add_argument("--inventory", required=True, help="Inventory CSV path.")
    parser.add_argument("--report", required=True, help="Output report txt path.")
    parser.add_argument(
        "--exclude",
        nargs="*",
        default=None,
        help="Additional directory names to skip. Defaults are always applied.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = build_report(args.input, args.inventory, exclude=args.exclude)
    write_report(report, args.report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
