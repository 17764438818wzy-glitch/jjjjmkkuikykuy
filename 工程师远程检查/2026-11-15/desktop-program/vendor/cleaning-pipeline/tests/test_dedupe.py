from __future__ import annotations

import csv
from pathlib import Path

from src.paper_cleaning.dedupe import (
    REPORT_FIELDNAMES,
    filter_extract_report_for_unique_text,
    filter_inventory_for_unique_files,
    normalize_text_for_hash,
)


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_filter_inventory_keeps_first_sha256_and_reports_duplicates(tmp_path: Path) -> None:
    rows = [
        {
            "file_id": "file_000001",
            "source_type": "normal",
            "original_relative_path": "a.docx",
            "archive_path": "",
            "archive_inner_path": "",
            "sha256": "same",
        },
        {
            "file_id": "file_000002",
            "source_type": "normal",
            "original_relative_path": "b.docx",
            "archive_path": "",
            "archive_inner_path": "",
            "sha256": "same",
        },
    ]

    unique, dedupe_rows = filter_inventory_for_unique_files(rows)

    assert [row["file_id"] for row in unique] == ["file_000001"]
    assert dedupe_rows == [
        {
            "file_id": "file_000002",
            "duplicate_type": "file",
            "kept_file_id": "file_000001",
            "original_relative_path": "b.docx",
            "archive_path": "",
            "archive_inner_path": "",
            "reason": "same sha256",
            "status": "duplicate_skipped",
        }
    ]


def test_filter_extract_report_keeps_first_text_hash_and_writes_report(tmp_path: Path) -> None:
    raw_a = tmp_path / "raw" / "a.txt"
    raw_b = tmp_path / "raw" / "b.txt"
    raw_a.parent.mkdir()
    raw_a.write_text("摘要\n第 1 页\n相同 正文\n参考文献", encoding="utf-8")
    raw_b.write_text("摘要\n第 2 页\n相同正文\n参考文献", encoding="utf-8")
    rows = [
        {
            "file_id": "file_000001",
            "source_type": "normal",
            "original_relative_path": "a.docx",
            "archive_path": "",
            "archive_inner_path": "",
            "output_path": str(raw_a),
            "status": "success",
        },
        {
            "file_id": "file_000002",
            "source_type": "normal",
            "original_relative_path": "b.docx",
            "archive_path": "",
            "archive_inner_path": "",
            "output_path": str(raw_b),
            "status": "success",
        },
    ]
    report = tmp_path / "dedupe_report.csv"

    unique, dedupe_rows = filter_extract_report_for_unique_text(rows, report)

    assert normalize_text_for_hash(raw_a.read_text(encoding="utf-8")) == normalize_text_for_hash(raw_b.read_text(encoding="utf-8"))
    assert [row["file_id"] for row in unique] == ["file_000001"]
    assert dedupe_rows[0]["file_id"] == "file_000002"
    assert dedupe_rows[0]["duplicate_type"] == "text"
    assert dedupe_rows[0]["kept_file_id"] == "file_000001"
    assert dedupe_rows[0]["status"] == "duplicate_text_skipped"
    with report.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == REPORT_FIELDNAMES
        assert list(reader) == dedupe_rows
