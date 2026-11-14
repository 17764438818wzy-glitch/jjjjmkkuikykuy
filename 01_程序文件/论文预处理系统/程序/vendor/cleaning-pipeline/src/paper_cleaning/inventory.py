from __future__ import annotations

import argparse
import csv
import hashlib
import logging
import os
from collections import Counter
from pathlib import Path
from typing import Iterable

DEFAULT_EXCLUDE_DIRS = (
    "cleaning-pipeline",
    "output",
    "logs",
    "sample_small",
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    ".pytest_cache",
)

DOCUMENT_EXTENSIONS = {
    ".doc",
    ".docx",
    ".pdf",
    ".txt",
    ".rtf",
    ".wps",
    ".odt",
}

ARCHIVE_TYPES = {
    ".zip": "zip",
    ".rar": "rar",
    ".7z": "7z",
    ".tar": "tar",
    ".gz": "gz",
    ".bz2": "bz2",
    ".xz": "xz",
}

FIELDNAMES = [
    "file_id",
    "source_type",
    "original_relative_path",
    "archive_path",
    "archive_inner_path",
    "working_path",
    "sanitized_relative_path",
    "file_name",
    "file_ext",
    "file_size",
    "sha256",
    "is_duplicate",
    "is_archive",
    "archive_type",
    "status",
    "error_message",
]


def normalize_relative_path(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def normalize_exclude_dirs(exclude: Iterable[str] | None = None) -> set[str]:
    normalized = {directory.lower() for directory in DEFAULT_EXCLUDE_DIRS}
    if not exclude:
        return normalized

    for item in exclude:
        for value in item.split(","):
            value = value.strip().strip("/\\")
            if not value:
                continue
            normalized.add(Path(value).name.lower())
    return normalized


def iter_files(
    input_dir: Path,
    mode: str,
    limit: int | None = None,
    exclude: Iterable[str] | None = None,
) -> Iterable[Path]:
    exclude_dirs = normalize_exclude_dirs(exclude)
    selected = 0
    for current_root, dir_names, file_names in os.walk(input_dir):
        dir_names[:] = [
            dirname
            for dirname in sorted(dir_names, key=str.lower)
            if dirname.lower() not in exclude_dirs
        ]
        current_path = Path(current_root)
        for file_name in sorted(file_names, key=str.lower):
            path = current_path / file_name
            if mode == "documents" and path.suffix.lower() not in DOCUMENT_EXTENSIONS:
                continue
            yield path
            selected += 1
            if limit is not None and selected >= limit:
                return


def calculate_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_inventory(
    input_dir: str | Path,
    mode: str = "full",
    limit: int | None = None,
    exclude: Iterable[str] | None = None,
    archive_report: str | Path | None = None,
    extracted_dir: str | Path | None = None,
) -> list[dict[str, str]]:
    root = Path(input_dir).resolve()
    if mode not in {"full", "documents"}:
        raise ValueError("mode must be 'full' or 'documents'")
    if limit is not None and limit < 1:
        raise ValueError("limit must be greater than 0")
    if not root.exists():
        raise FileNotFoundError(f"Input directory does not exist: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"Input path is not a directory: {root}")

    source_paths = list(iter_files(root, mode=mode, limit=limit, exclude=exclude))
    records: list[dict[str, str]] = []
    for index, path in enumerate(source_paths, start=1):
        relative_path = normalize_relative_path(path, root)
        records.append(build_file_record(index, path, relative_path, "normal"))

    if archive_report:
        next_index = len(records) + 1
        for archive_row in read_archive_report(archive_report):
            if archive_row.get("status") != "success":
                continue
            working_path = Path(archive_row.get("extracted_path", ""))
            if not working_path.exists() or working_path.suffix.lower() not in DOCUMENT_EXTENSIONS:
                continue
            record = build_file_record(
                next_index,
                working_path,
                archive_row.get("inner_path", working_path.name),
                "archive",
                archive_path=archive_row.get("archive_path", ""),
                archive_inner_path=archive_row.get("inner_path", ""),
            )
            records.append(record)
            next_index += 1
            if limit is not None and len(records) >= limit:
                break

    sha_counts = Counter(
        record["sha256"]
        for record in records
        if record["status"] == "success" and record["sha256"]
    )
    duplicate_hashes = {
        sha256 for sha256, count in sha_counts.items() if count > 1
    }
    for record in records:
        if record["sha256"] in duplicate_hashes:
            record["is_duplicate"] = "true"

    return records


def build_file_record(
    index: int,
    path: Path,
    original_relative_path: str,
    source_type: str,
    archive_path: str = "",
    archive_inner_path: str = "",
) -> dict[str, str]:
    file_ext = path.suffix.lower()
    archive_type = ARCHIVE_TYPES.get(file_ext, "")
    record = {
        "file_id": f"file_{index:06d}",
        "source_type": source_type,
        "original_relative_path": original_relative_path,
        "archive_path": archive_path,
        "archive_inner_path": archive_inner_path,
        "working_path": str(path),
        "sanitized_relative_path": original_relative_path,
        "file_name": path.name,
        "file_ext": file_ext,
        "file_size": "",
        "sha256": "",
        "is_duplicate": "false",
        "is_archive": "true" if archive_type else "false",
        "archive_type": archive_type,
        "status": "success",
        "error_message": "",
    }
    try:
        record["file_size"] = str(path.stat().st_size)
        record["sha256"] = calculate_sha256(path)
    except OSError as exc:
        record["status"] = "error"
        record["error_message"] = str(exc)
    return record


def read_archive_report(report_path: str | Path) -> list[dict[str, str]]:
    path = Path(report_path)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_inventory_csv(records: list[dict[str, str]], output_path: str | Path) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(records)


def configure_logging(log_path: str | Path) -> None:
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=path,
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        encoding="utf-8",
        force=True,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a file-level inventory CSV.")
    parser.add_argument("--input", required=True, help="Input directory to scan.")
    parser.add_argument("--output", required=True, help="Output CSV path.")
    parser.add_argument(
        "--mode",
        required=True,
        choices=["full", "documents"],
        help="Scan all files or document files only.",
    )
    parser.add_argument("--log", required=True, help="Log file path.")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of matching files to scan.",
    )
    parser.add_argument(
        "--exclude",
        nargs="*",
        default=None,
        help="Additional directory names to skip. Defaults are always applied.",
    )
    parser.add_argument("--archive-report", default=None, help="Optional archive extraction report CSV.")
    parser.add_argument("--extracted-dir", default=None, help="Optional extracted archive directory.")
    return parser.parse_args(argv)


def list_top_level_dirs(input_dir: str | Path, limit: int = 20) -> list[str]:
    root = Path(input_dir)
    if not root.exists() or not root.is_dir():
        return []
    directories = [
        path.name
        for path in sorted(root.iterdir(), key=lambda item: item.name.lower())
        if path.is_dir()
    ]
    return directories[:limit]


def print_startup_summary(args: argparse.Namespace) -> None:
    root = Path(args.input).resolve()
    preview_limit = 20
    if args.limit is not None:
        preview_limit = min(preview_limit, args.limit)
    preview_files = []
    if root.exists() and root.is_dir():
        preview_files = [
            normalize_relative_path(path, root)
            for path in iter_files(
                root,
                mode=args.mode,
                limit=preview_limit,
                exclude=args.exclude,
            )
        ]

    print(f"input 路径: {Path(args.input)}")
    print(f"实际扫描根目录: {root}")
    print(f"默认排除目录: {', '.join(DEFAULT_EXCLUDE_DIRS)}")
    if args.exclude:
        print(f"用户追加排除目录: {', '.join(args.exclude)}")
    print("前 20 个一级目录:")
    for directory in list_top_level_dirs(root):
        print(f"  {directory}")
    if not list_top_level_dirs(root):
        print("  (none)")
    print("前 20 个被扫描到的文件:")
    for relative_path in preview_files:
        print(f"  {relative_path}")
    if not preview_files:
        print("  (none)")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        print_startup_summary(args)
        records = build_inventory(
            args.input,
            mode=args.mode,
            limit=args.limit,
            exclude=args.exclude,
            archive_report=args.archive_report,
            extracted_dir=args.extracted_dir,
        )
        write_inventory_csv(records, args.output)
        configure_logging(args.log)
        status_counts = Counter(record["status"] for record in records)
        duplicate_count = sum(
            1 for record in records if record["is_duplicate"] == "true"
        )
        logging.info("input=%s", Path(args.input).resolve())
        logging.info("output=%s", Path(args.output).resolve())
        logging.info("mode=%s limit=%s", args.mode, args.limit)
        logging.info("default_exclude_dirs=%s", DEFAULT_EXCLUDE_DIRS)
        logging.info("custom_exclude_dirs=%s", args.exclude or [])
        logging.info("records=%s", len(records))
        logging.info("status_counts=%s", dict(status_counts))
        logging.info("duplicate_files=%s", duplicate_count)
    except Exception as exc:
        if args.log:
            configure_logging(args.log)
            logging.exception("inventory failed")
        raise exc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
