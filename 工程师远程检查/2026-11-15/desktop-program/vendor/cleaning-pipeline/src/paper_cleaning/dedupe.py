from __future__ import annotations

import csv
import hashlib
import re
import shutil
from pathlib import Path

REPORT_FIELDNAMES = [
    "file_id",
    "duplicate_type",
    "kept_file_id",
    "original_relative_path",
    "archive_path",
    "archive_inner_path",
    "reason",
    "status",
]

PAGE_MARK_RE = re.compile(r"(?m)^\s*(?:第\s*)?\d+\s*(?:页)?\s*$")
TITLE_LINE_RE = re.compile(r"(?m)^\s*(?:论文)?题目\s*[:：]?\s*.*$")
WHITESPACE_RE = re.compile(r"\s+")


def dedupe_row(
    row: dict[str, str],
    duplicate_type: str,
    kept_file_id: str,
    reason: str,
    status: str,
) -> dict[str, str]:
    return {
        "file_id": row.get("file_id", ""),
        "duplicate_type": duplicate_type,
        "kept_file_id": kept_file_id,
        "original_relative_path": row.get("original_relative_path", ""),
        "archive_path": row.get("archive_path", ""),
        "archive_inner_path": row.get("archive_inner_path", ""),
        "reason": reason,
        "status": status,
    }


def filter_inventory_for_unique_files(
    rows: list[dict[str, str]],
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    seen: dict[str, str] = {}
    unique: list[dict[str, str]] = []
    dedupe_rows: list[dict[str, str]] = []
    for row in rows:
        sha256 = row.get("sha256", "")
        if sha256 and sha256 in seen:
            duplicate = dict(row)
            duplicate["status"] = "duplicate_skipped"
            duplicate["error_message"] = f"duplicate of {seen[sha256]}"
            dedupe_rows.append(
                dedupe_row(
                    duplicate,
                    duplicate_type="file",
                    kept_file_id=seen[sha256],
                    reason="same sha256",
                    status="duplicate_skipped",
                )
            )
            continue
        if sha256:
            seen[sha256] = row.get("file_id", "")
        unique.append(row)
    return unique, dedupe_rows


def normalize_text_for_hash(text: str) -> str:
    text = TITLE_LINE_RE.sub("", text)
    text = PAGE_MARK_RE.sub("", text)
    text = WHITESPACE_RE.sub("", text)
    return text


def text_hash(text: str) -> str:
    return hashlib.sha256(normalize_text_for_hash(text).encode("utf-8")).hexdigest()


def filter_extract_report_for_unique_text(
    rows: list[dict[str, str]],
    report_path: str | Path | None = None,
    existing_dedupe_rows: list[dict[str, str]] | None = None,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    seen: dict[str, str] = {}
    unique: list[dict[str, str]] = []
    dedupe_rows = list(existing_dedupe_rows or [])
    for row in rows:
        if row.get("status") != "success" or not row.get("output_path"):
            unique.append(row)
            continue
        raw_path = Path(row["output_path"])
        content = raw_path.read_text(encoding="utf-8")
        digest = text_hash(content)
        if digest in seen:
            dedupe_rows.append(
                dedupe_row(
                    row,
                    duplicate_type="text",
                    kept_file_id=seen[digest],
                    reason="same normalized text_hash",
                    status="duplicate_text_skipped",
                )
            )
            continue
        seen[digest] = row.get("file_id", "")
        unique.append(row)
    if report_path is not None:
        write_dedupe_report(dedupe_rows, report_path)
    return unique, dedupe_rows


def write_dedupe_report(rows: list[dict[str, str]], report_path: str | Path) -> None:
    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def write_extract_report(rows: list[dict[str, str]], report_path: str | Path, fieldnames: list[str]) -> None:
    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _clear_generated_directory(path: Path) -> None:
    resolved = path.resolve()
    if resolved.anchor == str(resolved) or resolved.name in {"", ".", ".."}:
        raise ValueError(f"refusing to clear unsafe directory: {resolved}")
    if resolved.exists():
        shutil.rmtree(resolved)
    resolved.mkdir(parents=True, exist_ok=True)


def prepare_unique_raw_text_dir(
    unique_extract_rows: list[dict[str, str]],
    destination_dir: str | Path,
) -> Path:
    destination_root = Path(destination_dir)
    _clear_generated_directory(destination_root)
    for row in unique_extract_rows:
        if row.get("status") != "success" or not row.get("output_path"):
            continue
        source = Path(row["output_path"])
        destination = destination_root / source.name
        shutil.copy2(source, destination)
        row["output_path"] = str(destination)
    return destination_root
