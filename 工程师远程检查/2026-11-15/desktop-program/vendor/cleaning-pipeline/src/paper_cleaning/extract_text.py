from __future__ import annotations

import argparse
import csv
import zipfile
from pathlib import Path
from xml.etree import ElementTree

REPORT_FIELDNAMES = [
    "file_id",
    "source_type",
    "original_relative_path",
    "archive_path",
    "archive_inner_path",
    "file_name",
    "input_path",
    "output_path",
    "file_ext",
    "char_count",
    "status",
    "error_message",
]

WORD_NS = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
WORD_TYPE_ATTR = f"{WORD_NS}type"
SUPPORTED_EXTENSIONS = {".docx", ".doc", ".pdf", ".txt", ".wps", ".rtf", ".odt"}


def word_tag(name: str) -> str:
    return f"{WORD_NS}{name}"


def paragraph_text(paragraph: ElementTree.Element) -> list[str]:
    parts: list[str] = []
    has_section_break = paragraph.find(f"{word_tag('pPr')}/{word_tag('sectPr')}") is not None
    if has_section_break:
        parts.append("[SECTION_BREAK]")

    text_parts: list[str] = []
    for element in paragraph.iter():
        if element.tag == word_tag("t") and element.text:
            text_parts.append(element.text)
        elif element.tag == word_tag("tab"):
            text_parts.append("\t")
        elif element.tag == word_tag("br"):
            if element.attrib.get(WORD_TYPE_ATTR) == "page":
                if "".join(text_parts).strip():
                    parts.append("".join(text_parts).strip())
                    text_parts = []
                parts.append("[PAGE_BREAK]")
            else:
                text_parts.append("\n")
        elif element.tag == word_tag("lastRenderedPageBreak"):
            if "".join(text_parts).strip():
                parts.append("".join(text_parts).strip())
                text_parts = []
            parts.append("[PAGE_BREAK]")

    paragraph_value = "".join(text_parts).strip()
    if paragraph_value:
        parts.append(paragraph_value)
    return parts


def cell_text(cell: ElementTree.Element) -> str:
    paragraphs: list[str] = []
    for paragraph in cell.findall(word_tag("p")):
        paragraph_parts = [
            part
            for part in paragraph_text(paragraph)
            if part not in {"[PAGE_BREAK]", "[SECTION_BREAK]"}
        ]
        if paragraph_parts:
            paragraphs.append(" ".join(paragraph_parts))
    return " ".join(paragraphs).strip()


def table_text(table: ElementTree.Element) -> list[str]:
    rows: list[str] = []
    for row in table.findall(word_tag("tr")):
        cells = [cell_text(cell) for cell in row.findall(word_tag("tc"))]
        if any(cells):
            rows.append("\t".join(cells).strip())
    return rows


def extract_docx_text(path: str | Path) -> str:
    path = Path(path)
    with zipfile.ZipFile(path) as archive:
        document_xml = archive.read("word/document.xml")

    document = ElementTree.fromstring(document_xml)
    body = document.find(word_tag("body"))
    if body is None:
        return ""

    lines: list[str] = []
    for child in body:
        if child.tag == word_tag("p"):
            lines.extend(paragraph_text(child))
        elif child.tag == word_tag("tbl"):
            lines.extend(table_text(child))
        elif child.tag == word_tag("sectPr"):
            lines.append("[SECTION_BREAK]")

    return "\n".join(line for line in lines if line != "")


def output_path_for(source: Path, input_root: Path, output_root: Path) -> Path:
    relative_path = source.relative_to(input_root).with_suffix(".txt")
    return output_root / relative_path


def build_report_row(
    source: Path,
    output_path: str,
    char_count: int,
    status: str,
    error_message: str = "",
    file_id: str = "",
    source_type: str = "normal",
    original_relative_path: str = "",
    archive_path: str = "",
    archive_inner_path: str = "",
) -> dict[str, str]:
    return {
        "file_id": file_id,
        "source_type": source_type,
        "original_relative_path": original_relative_path,
        "archive_path": archive_path,
        "archive_inner_path": archive_inner_path,
        "file_name": source.name,
        "input_path": str(source),
        "output_path": output_path,
        "file_ext": source.suffix.lower(),
        "char_count": str(char_count),
        "status": status,
        "error_message": error_message,
    }


def read_pdf_text(path: Path) -> str:
    try:
        from pypdf import PdfReader  # type: ignore
    except Exception as exc:
        raise RuntimeError("pdf extraction dependency is not configured") from exc
    reader = PdfReader(str(path))
    return "\n".join(page.extract_text() or "" for page in reader.pages)


def extract_text_by_extension(source: Path) -> tuple[str, str, str]:
    file_ext = source.suffix.lower()
    if file_ext == ".doc":
        return "", "unsupported", ".doc extraction is unsupported"
    if file_ext in {".wps", ".rtf", ".odt"}:
        return "", "unsupported", f"{file_ext} extraction is unsupported"
    if file_ext == ".docx":
        return extract_docx_text(source), "success", ""
    if file_ext == ".txt":
        return source.read_text(encoding="utf-8"), "success", ""
    if file_ext == ".pdf":
        try:
            return read_pdf_text(source), "success", ""
        except Exception as exc:
            return "", "unsupported", str(exc)
    return "", "skipped", "unsupported extension"


def extract_one_file(
    source: str | Path,
    input_root: str | Path,
    output_root: str | Path,
    file_id: str = "",
    source_type: str = "normal",
    original_relative_path: str = "",
    archive_path: str = "",
    archive_inner_path: str = "",
) -> dict[str, str]:
    source = Path(source)
    input_root = Path(input_root)
    output_root = Path(output_root)
    file_ext = source.suffix.lower()

    destination = output_path_for(source, input_root, output_root)
    try:
        text, status, error_message = extract_text_by_extension(source)
        if status != "success":
            return build_report_row(
                source,
                output_path="",
                char_count=0,
                status=status,
                error_message=error_message,
                file_id=file_id,
                source_type=source_type,
                original_relative_path=original_relative_path,
                archive_path=archive_path,
                archive_inner_path=archive_inner_path,
            )
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(text, encoding="utf-8")
        return build_report_row(
            source,
            output_path=str(destination),
            char_count=len(text),
            status="success",
            file_id=file_id,
            source_type=source_type,
            original_relative_path=original_relative_path,
            archive_path=archive_path,
            archive_inner_path=archive_inner_path,
        )
    except Exception as exc:
        return build_report_row(
            source,
            output_path=str(destination),
            char_count=0,
            status="error",
            error_message=str(exc),
            file_id=file_id,
            source_type=source_type,
            original_relative_path=original_relative_path,
            archive_path=archive_path,
            archive_inner_path=archive_inner_path,
        )


def iter_word_files(input_root: Path) -> list[Path]:
    return sorted(
        (
            path
            for path in input_root.rglob("*")
            if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS
        ),
        key=lambda path: path.as_posix().lower(),
    )


def write_report_csv(rows: list[dict[str, str]], report_path: str | Path) -> None:
    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def extract_directory(
    input_dir: str | Path,
    output_dir: str | Path,
    report_path: str | Path,
    limit: int = 20,
) -> list[dict[str, str]]:
    if limit < 1:
        raise ValueError("limit must be greater than 0")

    input_root = Path(input_dir).resolve()
    output_root = Path(output_dir).resolve()
    if not input_root.exists():
        raise FileNotFoundError(f"Input directory does not exist: {input_root}")
    if not input_root.is_dir():
        raise NotADirectoryError(f"Input path is not a directory: {input_root}")

    output_root.mkdir(parents=True, exist_ok=True)
    rows = [
        extract_one_file(source, input_root, output_root)
        for source in iter_word_files(input_root)[:limit]
    ]
    write_report_csv(rows, report_path)
    return rows


def read_inventory_rows(inventory_csv: str | Path) -> list[dict[str, str]]:
    with Path(inventory_csv).open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def extract_from_inventory(
    inventory_csv: str | Path,
    output_dir: str | Path,
    report_path: str | Path,
    limit: int | None = None,
) -> list[dict[str, str]]:
    output_root = Path(output_dir).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    rows = [
        row
        for row in read_inventory_rows(inventory_csv)
        if row.get("status") == "success" and row.get("file_ext", "").lower() in SUPPORTED_EXTENSIONS
    ]
    if limit is not None:
        rows = rows[:limit]
    report_rows = []
    for row in rows:
        source = Path(row["working_path"])
        destination = output_root / f"{row['file_id']}.txt"
        try:
            text, status, error_message = extract_text_by_extension(source)
            if status == "success":
                destination.write_text(text, encoding="utf-8")
                output_path = str(destination)
                char_count = len(text)
            else:
                output_path = ""
                char_count = 0
            report_rows.append(
                build_report_row(
                    source,
                    output_path=output_path,
                    char_count=char_count,
                    status=status,
                    error_message=error_message,
                    file_id=row.get("file_id", ""),
                    source_type=row.get("source_type", ""),
                    original_relative_path=row.get("original_relative_path", ""),
                    archive_path=row.get("archive_path", ""),
                    archive_inner_path=row.get("archive_inner_path", ""),
                )
            )
        except Exception as exc:
            report_rows.append(
                build_report_row(
                    source,
                    output_path=str(destination),
                    char_count=0,
                    status="error",
                    error_message=str(exc),
                    file_id=row.get("file_id", ""),
                    source_type=row.get("source_type", ""),
                    original_relative_path=row.get("original_relative_path", ""),
                    archive_path=row.get("archive_path", ""),
                    archive_inner_path=row.get("archive_inner_path", ""),
                )
            )
    write_report_csv(report_rows, report_path)
    return report_rows


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract text from sample Word files.")
    parser.add_argument("--input", required=True, help="Input directory, usually sample_small.")
    parser.add_argument("--inventory", default=None, help="Optional inventory CSV. If set, input is ignored.")
    parser.add_argument("--output", required=True, help="Output raw_text directory.")
    parser.add_argument("--report", required=True, help="Output extraction report CSV.")
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of Word files to process. Defaults to 20.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.inventory:
        rows = extract_from_inventory(args.inventory, args.output, args.report, limit=args.limit)
    else:
        rows = extract_directory(args.input, args.output, args.report, limit=args.limit)
    status_counts: dict[str, int] = {}
    for row in rows:
        status_counts[row["status"]] = status_counts.get(row["status"], 0) + 1
    print(f"Processed {len(rows)} Word files with limit={args.limit}")
    print(f"Status: {status_counts}")
    print(f"Output: {Path(args.output).resolve()}")
    print(f"Report: {Path(args.report).resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
