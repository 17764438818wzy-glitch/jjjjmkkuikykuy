from __future__ import annotations

import csv
from pathlib import Path

from src.paper_cleaning.inventory import (
    DEFAULT_EXCLUDE_DIRS,
    DOCUMENT_EXTENSIONS,
    FIELDNAMES,
    build_inventory,
    write_inventory_csv,
)


def write_file(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def test_full_mode_scans_all_files_and_marks_duplicates(tmp_path: Path) -> None:
    write_file(tmp_path / "student-a.docx", b"same-content")
    write_file(tmp_path / "nested" / "student-b.pdf", b"same-content")
    write_file(tmp_path / "image.png", b"not-a-document")

    records = build_inventory(tmp_path, mode="full")

    assert len(records) == 3
    assert {record["original_relative_path"] for record in records} == {
        "image.png",
        "nested/student-b.pdf",
        "student-a.docx",
    }
    assert all(len(record["sha256"]) == 64 for record in records)

    duplicate_records = [
        record for record in records if record["is_duplicate"] == "true"
    ]
    assert {record["file_name"] for record in duplicate_records} == {
        "student-a.docx",
        "student-b.pdf",
    }


def test_documents_mode_only_scans_document_extensions(tmp_path: Path) -> None:
    for index, ext in enumerate(sorted(DOCUMENT_EXTENSIONS)):
        write_file(tmp_path / f"document-{index}{ext}", f"doc-{ext}".encode())
    write_file(tmp_path / "spreadsheet.xlsx", b"skip")
    write_file(tmp_path / "picture.jpg", b"skip")

    records = build_inventory(tmp_path, mode="documents")

    assert len(records) == len(DOCUMENT_EXTENSIONS)
    assert {record["file_ext"] for record in records} == DOCUMENT_EXTENSIONS


def test_limit_restricts_number_of_scanned_files(tmp_path: Path) -> None:
    for index in range(5):
        write_file(tmp_path / f"{index}.txt", f"content-{index}".encode())

    records = build_inventory(tmp_path, mode="full", limit=2)

    assert len(records) == 2


def test_write_inventory_csv_uses_required_fields(tmp_path: Path) -> None:
    write_file(tmp_path / "a.txt", b"alpha")
    records = build_inventory(tmp_path, mode="documents")
    csv_path = tmp_path / "inventory.csv"

    write_inventory_csv(records, csv_path)

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    assert reader.fieldnames == FIELDNAMES
    assert rows[0]["original_relative_path"] == "a.txt"
    assert rows[0]["status"] == "success"


def test_default_excludes_skip_project_and_cache_directories(tmp_path: Path) -> None:
    write_file(tmp_path / "real-data" / "paper.docx", b"paper")
    for directory in DEFAULT_EXCLUDE_DIRS:
        write_file(tmp_path / directory / "should-not-scan.txt", b"skip")

    records = build_inventory(tmp_path, mode="full")

    assert [record["original_relative_path"] for record in records] == [
        "real-data/paper.docx"
    ]


def test_custom_exclude_extends_default_excludes(tmp_path: Path) -> None:
    write_file(tmp_path / "keep" / "paper.pdf", b"keep")
    write_file(tmp_path / "raw_export" / "private.pdf", b"skip")
    write_file(tmp_path / "output" / "inventory.csv", b"also-skip")

    records = build_inventory(tmp_path, mode="documents", exclude=["raw_export"])

    assert [record["original_relative_path"] for record in records] == [
        "keep/paper.pdf"
    ]


def test_inventory_includes_successful_extracted_archive_documents(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    extracted = tmp_path / "output" / "extracted_archives" / "bundle" / "paper.docx"
    archive_report = tmp_path / "output" / "archive_report.csv"
    write_file(input_dir / "normal.txt", b"normal")
    write_file(extracted, b"docx bytes")
    archive_report.parent.mkdir(parents=True, exist_ok=True)
    archive_report.write_text(
        "archive_path,inner_path,extracted_path,file_ext,status,error_message\n"
        f"{tmp_path / 'input' / 'bundle.zip'},nested/paper.docx,{extracted},.docx,success,\n",
        encoding="utf-8",
    )

    records = build_inventory(input_dir, mode="documents", archive_report=archive_report)

    assert {record["source_type"] for record in records} == {"normal", "archive"}
    archive_record = next(record for record in records if record["source_type"] == "archive")
    assert archive_record["archive_inner_path"] == "nested/paper.docx"
    assert archive_record["working_path"] == str(extracted)
