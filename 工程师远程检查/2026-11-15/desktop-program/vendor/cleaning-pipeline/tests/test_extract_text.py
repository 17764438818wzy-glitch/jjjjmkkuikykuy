from __future__ import annotations

import csv
import zipfile
from pathlib import Path

from src.paper_cleaning.extract_text import (
    REPORT_FIELDNAMES,
    extract_directory,
    extract_docx_text,
    extract_from_inventory,
    extract_one_file,
)


def write_minimal_docx(path: Path, document_xml: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "[Content_Types].xml",
            """<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
</Types>""",
        )
        archive.writestr(
            "_rels/.rels",
            """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
</Relationships>""",
        )
        archive.writestr("word/document.xml", document_xml)


def test_extract_docx_text_includes_paragraphs_tables_and_break_markers(
    tmp_path: Path,
) -> None:
    docx_path = tmp_path / "sample.docx"
    write_minimal_docx(
        docx_path,
        """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body>
    <w:p><w:r><w:t>封面标题</w:t></w:r></w:p>
    <w:p><w:r><w:t>第一页结束</w:t></w:r><w:r><w:br w:type="page"/></w:r></w:p>
    <w:p><w:r><w:t>摘要</w:t></w:r></w:p>
    <w:tbl>
      <w:tr>
        <w:tc><w:p><w:r><w:t>姓名</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>张三</w:t></w:r></w:p></w:tc>
      </w:tr>
      <w:tr>
        <w:tc><w:p><w:r><w:t>学号</w:t></w:r></w:p></w:tc>
        <w:tc><w:p><w:r><w:t>201908023039</w:t></w:r></w:p></w:tc>
      </w:tr>
    </w:tbl>
    <w:p>
      <w:pPr><w:sectPr/></w:pPr>
      <w:r><w:t>第一章</w:t></w:r>
    </w:p>
    <w:sectPr/>
  </w:body>
</w:document>""",
    )

    text = extract_docx_text(docx_path)

    assert text.splitlines() == [
        "封面标题",
        "第一页结束",
        "[PAGE_BREAK]",
        "摘要",
        "姓名\t张三",
        "学号\t201908023039",
        "[SECTION_BREAK]",
        "第一章",
        "[SECTION_BREAK]",
    ]


def test_doc_files_are_reported_as_unsupported(tmp_path: Path) -> None:
    doc_path = tmp_path / "legacy.doc"
    output_dir = tmp_path / "raw_text"
    doc_path.write_bytes(b"legacy binary")

    row = extract_one_file(doc_path, tmp_path, output_dir)

    assert row["status"] == "unsupported"
    assert row["error_message"] == ".doc extraction is unsupported"
    assert row["output_path"] == ""


def test_extract_directory_writes_txt_outputs_and_report_with_limit(tmp_path: Path) -> None:
    input_dir = tmp_path / "sample_small"
    output_dir = tmp_path / "raw_text"
    report_path = tmp_path / "report.csv"
    for index in range(25):
        write_minimal_docx(
            input_dir / f"{index:02d}.docx",
            f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
  <w:body><w:p><w:r><w:t>文件 {index:02d}</w:t></w:r></w:p></w:body>
</w:document>""",
        )

    rows = extract_directory(input_dir, output_dir, report_path, limit=20)

    assert len(rows) == 20
    assert all(row["status"] == "success" for row in rows)
    assert (output_dir / "00.txt").read_text(encoding="utf-8") == "文件 00"
    assert not (output_dir / "20.txt").exists()

    with report_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        report_rows = list(reader)

    assert reader.fieldnames == REPORT_FIELDNAMES
    assert report_rows == rows


def test_extract_from_inventory_reports_wps_as_unsupported(tmp_path: Path) -> None:
    source = tmp_path / "legacy.wps"
    source.write_bytes(b"legacy wps binary")
    inventory = tmp_path / "inventory.csv"
    report = tmp_path / "extract_report.csv"
    with inventory.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "file_id",
                "source_type",
                "original_relative_path",
                "archive_path",
                "archive_inner_path",
                "working_path",
                "file_ext",
                "status",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "file_id": "file_000001",
                "source_type": "normal",
                "original_relative_path": "legacy.wps",
                "archive_path": "",
                "archive_inner_path": "",
                "working_path": str(source),
                "file_ext": ".wps",
                "status": "success",
            }
        )

    rows = extract_from_inventory(inventory, tmp_path / "raw_text", report)

    assert len(rows) == 1
    assert rows[0]["status"] == "unsupported"
    assert rows[0]["error_message"] == ".wps extraction is unsupported"
