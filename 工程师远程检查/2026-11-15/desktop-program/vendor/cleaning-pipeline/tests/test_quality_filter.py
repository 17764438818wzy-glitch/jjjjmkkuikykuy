from __future__ import annotations

import csv
from pathlib import Path

from src.paper_cleaning.quality_filter import build_quality_report


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_short_text_is_rejected(tmp_path: Path) -> None:
    text_path = tmp_path / "sanitized" / "财务管理" / "FM_000001.txt"
    text_path.parent.mkdir(parents=True)
    text_path.write_text("摘要\n太短。", encoding="utf-8")
    sanitize_report = tmp_path / "sanitize_report.csv"
    write_csv(
        sanitize_report,
        ["file_id", "sanitized_path", "detected_major"],
        [{"file_id": "FM_000001", "sanitized_path": str(text_path), "detected_major": "财务管理"}],
    )

    rows = build_quality_report(sanitize_report, tmp_path / "quality.csv")

    assert rows[0]["quality_status"] == "reject"
    assert rows[0]["quality_reason"] == "too_short"


def test_missing_abstract_is_review(tmp_path: Path) -> None:
    text_path = tmp_path / "sanitized" / "财务管理" / "FM_000001.txt"
    text_path.parent.mkdir(parents=True)
    text_path.write_text("第一章\n" + "正文" * 2000 + "\n参考文献\n[1] A", encoding="utf-8")
    sanitize_report = tmp_path / "sanitize_report.csv"
    write_csv(
        sanitize_report,
        ["file_id", "sanitized_path", "detected_major"],
        [{"file_id": "FM_000001", "sanitized_path": str(text_path), "detected_major": "财务管理"}],
    )

    rows = build_quality_report(sanitize_report, tmp_path / "quality.csv")

    assert rows[0]["quality_status"] == "review"
    assert "missing_abstract" in rows[0]["quality_reason"]


def test_duplicate_is_rejected(tmp_path: Path) -> None:
    text_path = tmp_path / "sanitized" / "财务管理" / "FM_000001.txt"
    text_path.parent.mkdir(parents=True)
    text_path.write_text("摘要\n第一章\n" + "正文" * 2000 + "\n参考文献", encoding="utf-8")
    sanitize_report = tmp_path / "sanitize_report.csv"
    inventory_csv = tmp_path / "inventory.csv"
    write_csv(
        sanitize_report,
        ["file_id", "sanitized_path", "detected_major"],
        [{"file_id": "FM_000001", "sanitized_path": str(text_path), "detected_major": "财务管理"}],
    )
    write_csv(inventory_csv, ["file_id", "is_duplicate"], [{"file_id": "FM_000001", "is_duplicate": "true"}])

    rows = build_quality_report(sanitize_report, tmp_path / "quality.csv", inventory_csv)

    assert rows[0]["quality_status"] == "reject"
    assert rows[0]["quality_reason"] == "reject_duplicate"
