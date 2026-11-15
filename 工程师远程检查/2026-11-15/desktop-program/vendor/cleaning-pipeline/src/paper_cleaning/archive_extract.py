from __future__ import annotations

import argparse
import csv
import shutil
import zipfile
from pathlib import Path

ARCHIVE_EXTENSIONS = {".zip", ".rar", ".7z"}
DOCUMENT_EXTENSIONS = {".doc", ".docx", ".pdf", ".txt"}
REPORT_FIELDNAMES = [
    "archive_path",
    "inner_path",
    "extracted_path",
    "file_ext",
    "status",
    "error_message",
]


def iter_archives(input_dir: Path) -> list[Path]:
    return sorted(
        (
            path
            for path in input_dir.rglob("*")
            if path.is_file() and path.suffix.lower() in ARCHIVE_EXTENSIONS
        ),
        key=lambda path: path.as_posix().lower(),
    )


def safe_inner_path(name: str) -> Path:
    parts = [part for part in Path(name.replace("\\", "/")).parts if part not in {"", ".", ".."}]
    return Path(*parts) if parts else Path("unnamed")


def archive_output_root(output_dir: Path, archive_path: Path, input_root: Path) -> Path:
    try:
        relative = archive_path.relative_to(input_root).with_suffix("")
    except ValueError:
        relative = Path(archive_path.stem)
    return output_dir / relative


def row(
    archive_path: Path,
    inner_path: str,
    extracted_path: str,
    file_ext: str,
    status: str,
    error_message: str = "",
) -> dict[str, str]:
    return {
        "archive_path": str(archive_path),
        "inner_path": inner_path,
        "extracted_path": extracted_path,
        "file_ext": file_ext,
        "status": status,
        "error_message": error_message,
    }


def extract_zip(
    archive_path: Path,
    input_root: Path,
    output_dir: Path,
    depth: int,
    max_depth: int,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    base_output = archive_output_root(output_dir, archive_path, input_root)
    with zipfile.ZipFile(archive_path) as archive:
        for info in archive.infolist():
            if info.is_dir():
                continue
            inner = info.filename
            suffix = Path(inner).suffix.lower()
            if suffix not in DOCUMENT_EXTENSIONS and suffix not in ARCHIVE_EXTENSIONS:
                continue
            destination = base_output / safe_inner_path(inner)
            try:
                destination.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(info) as source, destination.open("wb") as handle:
                    shutil.copyfileobj(source, handle)
                status = "success" if suffix in DOCUMENT_EXTENSIONS else "archive_extracted"
                rows.append(row(archive_path, inner, str(destination), suffix, status))
                if suffix in ARCHIVE_EXTENSIONS and depth < max_depth:
                    rows.extend(extract_archive(destination, input_root, output_dir, depth + 1, max_depth))
            except RuntimeError as exc:
                rows.append(row(archive_path, inner, "", suffix, "error", str(exc)))
            except Exception as exc:
                rows.append(row(archive_path, inner, "", suffix, "error", str(exc)))
    return rows


def extract_archive(
    archive_path: Path,
    input_root: Path,
    output_dir: Path,
    depth: int = 0,
    max_depth: int = 2,
) -> list[dict[str, str]]:
    suffix = archive_path.suffix.lower()
    try:
        if suffix == ".zip":
            return extract_zip(archive_path, input_root, output_dir, depth, max_depth)
        if suffix == ".rar":
            return [row(archive_path, "", "", suffix, "unsupported", "rar extraction dependency is not configured")]
        if suffix == ".7z":
            return [row(archive_path, "", "", suffix, "unsupported", "7z extraction dependency is not configured")]
        return []
    except zipfile.BadZipFile as exc:
        return [row(archive_path, "", "", suffix, "error", str(exc))]
    except Exception as exc:
        return [row(archive_path, "", "", suffix, "error", str(exc))]


def write_archive_report(rows: list[dict[str, str]], report_path: str | Path) -> None:
    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def extract_archives(
    input_dir: str | Path,
    output_dir: str | Path,
    report_path: str | Path,
    limit: int | None = None,
    max_depth: int = 2,
) -> list[dict[str, str]]:
    input_root = Path(input_dir).resolve()
    output_root = Path(output_dir).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    archives = iter_archives(input_root)
    if limit is not None:
        archives = archives[:limit]
    rows: list[dict[str, str]] = []
    for archive_path in archives:
        rows.extend(extract_archive(archive_path, input_root, output_root, max_depth=max_depth))
    write_archive_report(rows, report_path)
    return rows


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract document files from archives.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--max-depth", type=int, default=2)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    rows = extract_archives(args.input, args.output, args.report, args.limit, args.max_depth)
    print(f"Processed archive rows: {len(rows)}")
    print(f"Output: {Path(args.output).resolve()}")
    print(f"Report: {Path(args.report).resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
