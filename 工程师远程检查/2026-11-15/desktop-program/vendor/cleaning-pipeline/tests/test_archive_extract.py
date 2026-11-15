from __future__ import annotations

import csv
import zipfile
from pathlib import Path

from src.paper_cleaning.archive_extract import REPORT_FIELDNAMES, extract_archives

from helpers import write_minimal_docx


def test_zip_with_docx_is_extracted_and_reported(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output" / "extracted_archives"
    report_path = tmp_path / "output" / "archive_report.csv"
    docx = tmp_path / "paper.docx"
    write_minimal_docx(docx, "摘要 正文")
    archive_path = input_dir / "bundle.zip"
    archive_path.parent.mkdir(parents=True)
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.write(docx, "nested/paper.docx")

    rows = extract_archives(input_dir, output_dir, report_path)

    assert len(rows) == 1
    assert rows[0]["status"] == "success"
    assert rows[0]["inner_path"] == "nested/paper.docx"
    assert Path(rows[0]["extracted_path"]).exists()
    with report_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == REPORT_FIELDNAMES


def test_broken_zip_is_reported_without_crashing(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output" / "extracted_archives"
    report_path = tmp_path / "output" / "archive_report.csv"
    archive_path = input_dir / "broken.zip"
    archive_path.parent.mkdir(parents=True)
    archive_path.write_bytes(b"not-a-zip")

    rows = extract_archives(input_dir, output_dir, report_path)

    assert len(rows) == 1
    assert rows[0]["status"] == "error"
    assert rows[0]["archive_path"].endswith("broken.zip")
