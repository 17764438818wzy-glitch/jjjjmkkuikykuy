from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import shutil
from pathlib import Path
from typing import Iterable

from .archive_extract import ARCHIVE_EXTENSIONS, REPORT_FIELDNAMES as ARCHIVE_FIELDNAMES
from .archive_extract import extract_archive
from .dedupe import (
    REPORT_FIELDNAMES as DEDUPE_FIELDNAMES,
    dedupe_row,
    text_hash,
    write_dedupe_report,
    write_extract_report,
)
from .extract_text import extract_from_inventory
from .extract_text import REPORT_FIELDNAMES as EXTRACT_REPORT_FIELDNAMES
from .inventory import FIELDNAMES as INVENTORY_FIELDNAMES
from .inventory import DOCUMENT_EXTENSIONS, build_file_record, normalize_exclude_dirs, write_inventory_csv
from .quality_filter import build_quality_report
from .quality_filter import REPORT_FIELDNAMES as QUALITY_FIELDNAMES
from .quality_filter import write_quality_report
from .sanitize import MAPPING_FIELDNAMES, REPORT_FIELDNAMES as SANITIZE_FIELDNAMES
from .sanitize import sanitize_directory, write_mapping_csv, write_report_csv
from .tech_classifier import (
    INDEX_FIELDNAMES as TECH_INDEX_FIELDNAMES,
    REPORT_FIELDNAMES as TECH_REPORT_FIELDNAMES,
    build_tech_reports,
)

TEXT_HASH_FIELDNAMES = ["text_hash", "file_id"]
SHA_INDEX_FIELDNAMES = ["sha256", "file_id"]
COMPLETED_SOURCES_FIELDNAMES = ["source_path"]
SUBJECT_REPORT_FIELDNAMES = [
    "file_id",
    "source_type",
    "original_relative_path",
    "archive_path",
    "archive_inner_path",
    "detected_title",
    "detected_subject_group",
    "detected_major_or_direction",
    "tech_type",
    "tech_subtype",
    "sanitized_text_path",
    "quality_status",
    "status",
    "error_message",
]


def configure_log(log_path: str | Path) -> None:
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(path, mode="a", encoding="utf-8")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[handler],
        force=True,
    )


def read_csv_rows(path: str | Path) -> list[dict[str, str]]:
    csv_path = Path(path)
    if not csv_path.exists():
        return []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv_rows(path: str | Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    csv_path = Path(path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def append_csv_rows(path: str | Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    if not rows:
        return
    csv_path = Path(path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not csv_path.exists() or csv_path.stat().st_size == 0
    with csv_path.open("a", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def ensure_csv(path: str | Path, fieldnames: list[str]) -> None:
    if not Path(path).exists():
        write_csv_rows(path, fieldnames, [])


def quality_status_map(quality_report: str | Path) -> dict[str, str]:
    return {
        row.get("file_id", ""): row.get("quality_status", "")
        for row in read_csv_rows(quality_report)
    }


def update_mapping_quality_status(mapping_path: str | Path, quality_report: str | Path) -> list[dict[str, str]]:
    statuses = quality_status_map(quality_report)
    rows = read_csv_rows(mapping_path)
    for row in rows:
        row["quality_status"] = statuses.get(row.get("file_id", ""), "")
    write_csv_rows(mapping_path, MAPPING_FIELDNAMES, rows)
    return rows


def write_subject_report(mapping_rows: list[dict[str, str]], report_path: str | Path) -> list[dict[str, str]]:
    rows = []
    for row in mapping_rows:
        if row.get("quality_status") not in {"pass", "review"}:
            continue
        rows.append(
            {
                "file_id": row.get("file_id", ""),
                "source_type": row.get("source_type", ""),
                "original_relative_path": row.get("original_relative_path", ""),
                "archive_path": row.get("archive_path", ""),
                "archive_inner_path": row.get("archive_inner_path", ""),
                "detected_title": row.get("detected_title", ""),
                "detected_subject_group": row.get("detected_subject_group", ""),
                "detected_major_or_direction": row.get("detected_major_or_direction", ""),
                "tech_type": row.get("tech_type", ""),
                "tech_subtype": row.get("tech_subtype", ""),
                "sanitized_text_path": row.get("sanitized_text_path", row.get("sanitized_path", "")),
                "quality_status": row.get("quality_status", ""),
                "status": row.get("status", ""),
                "error_message": row.get("error_message", ""),
            }
        )
    write_csv_rows(report_path, SUBJECT_REPORT_FIELDNAMES, rows)
    return rows


def chunked(rows: list[dict[str, str]], batch_size: int) -> Iterable[list[dict[str, str]]]:
    for start in range(0, len(rows), batch_size):
        yield rows[start : start + batch_size]


def chunked_iterable(paths: Iterable[Path], batch_size: int) -> Iterable[list[Path]]:
    batch: list[Path] = []
    for path in paths:
        batch.append(path)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def source_key(path: Path, input_root: Path) -> str:
    try:
        return path.resolve().relative_to(input_root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def iter_stage1_sources(
    input_dir: str | Path,
    limit: int | None = None,
    exclude: Iterable[str] | None = None,
) -> Iterable[Path]:
    input_root = Path(input_dir).resolve()
    exclude_dirs = normalize_exclude_dirs(exclude)
    selected = 0
    for current_root, dir_names, file_names in os.walk(input_root):
        dir_names[:] = [
            dirname
            for dirname in sorted(dir_names, key=str.lower)
            if dirname.lower() not in exclude_dirs
        ]
        current_path = Path(current_root)
        for file_name in sorted(file_names, key=str.lower):
            path = current_path / file_name
            suffix = path.suffix.lower()
            if suffix not in DOCUMENT_EXTENSIONS and suffix not in ARCHIVE_EXTENSIONS:
                continue
            yield path
            selected += 1
            if limit is not None and selected >= limit:
                return


def load_simple_index(path: str | Path, key_field: str, value_field: str) -> dict[str, str]:
    return {
        row[key_field]: row[value_field]
        for row in read_csv_rows(path)
        if row.get(key_field) and row.get(value_field)
    }


def write_simple_index(path: str | Path, fieldnames: list[str], rows: dict[str, str]) -> None:
    key_field, value_field = fieldnames
    write_csv_rows(
        path,
        fieldnames,
        [{key_field: key, value_field: value} for key, value in sorted(rows.items(), key=lambda item: item[1])],
    )


def load_completed_sources(path: str | Path) -> set[str]:
    return {row["source_path"] for row in read_csv_rows(path) if row.get("source_path")}


def infer_completed_sources_from_inventory(inventory_path: str | Path, input_root: Path) -> set[str]:
    completed: set[str] = set()
    for row in read_csv_rows(inventory_path):
        if row.get("source_type") == "archive" and row.get("archive_path"):
            completed.add(source_key(Path(row["archive_path"]), input_root))
        elif row.get("source_type", "normal") == "normal" and row.get("original_relative_path"):
            completed.add(row["original_relative_path"].replace("\\", "/"))
    return completed


def append_completed_sources(path: str | Path, sources: list[Path], input_root: Path) -> None:
    append_csv_rows(
        path,
        COMPLETED_SOURCES_FIELDNAMES,
        [{"source_path": source_key(source, input_root)} for source in sources],
    )


def next_inventory_index(path: str | Path) -> int:
    max_index = 0
    pattern = re.compile(r"^file_(\d+)$")
    for row in read_csv_rows(path):
        match = pattern.match(row.get("file_id", ""))
        if match:
            max_index = max(max_index, int(match.group(1)))
    return max_index + 1


def processed_source_file_ids(mapping_path: str | Path) -> set[str]:
    processed: set[str] = set()
    for row in read_csv_rows(mapping_path):
        raw_text_path = row.get("raw_text_path", "")
        if raw_text_path:
            processed.add(Path(raw_text_path).stem)
    return processed


def load_text_hash_index(path: str | Path) -> dict[str, str]:
    return {
        row["text_hash"]: row["file_id"]
        for row in read_csv_rows(path)
        if row.get("text_hash") and row.get("file_id")
    }


def infer_sha_index_from_inventory(inventory_path: str | Path) -> dict[str, str]:
    seen: dict[str, str] = {}
    for row in read_csv_rows(inventory_path):
        sha256 = row.get("sha256", "")
        file_id = row.get("file_id", "")
        if sha256 and file_id and sha256 not in seen:
            seen[sha256] = file_id
    return seen


def write_text_hash_index(index: dict[str, str], path: str | Path) -> None:
    write_simple_index(path, TEXT_HASH_FIELDNAMES, index)


def write_sha_index(index: dict[str, str], path: str | Path) -> None:
    write_simple_index(path, SHA_INDEX_FIELDNAMES, index)


def filter_unique_text_rows(
    extract_rows: list[dict[str, str]],
    seen_hashes: dict[str, str],
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    unique_rows: list[dict[str, str]] = []
    duplicate_rows: list[dict[str, str]] = []
    for row in extract_rows:
        if row.get("status") != "success" or not row.get("output_path"):
            unique_rows.append(row)
            continue
        raw_path = Path(row["output_path"])
        digest = text_hash(raw_path.read_text(encoding="utf-8"))
        if digest in seen_hashes:
            duplicate_rows.append(
                dedupe_row(
                    row,
                    duplicate_type="text",
                    kept_file_id=seen_hashes[digest],
                    reason="same normalized text_hash",
                    status="duplicate_text_skipped",
                )
            )
            continue
        seen_hashes[digest] = row.get("file_id", "")
        unique_rows.append(row)
    return unique_rows, duplicate_rows


def build_batch_inventory_rows(
    batch_sources: list[Path],
    input_root: Path,
    archive_output_dir: Path,
    start_index: int,
    max_depth: int = 2,
) -> tuple[list[dict[str, str]], list[dict[str, str]], int]:
    inventory_rows: list[dict[str, str]] = []
    archive_rows: list[dict[str, str]] = []
    next_index = start_index
    for source in batch_sources:
        suffix = source.suffix.lower()
        if suffix in ARCHIVE_EXTENSIONS:
            rows = extract_archive(source, input_root, archive_output_dir, max_depth=max_depth)
            archive_rows.extend(rows)
            for archive_row in rows:
                if archive_row.get("status") != "success":
                    continue
                working_path = Path(archive_row.get("extracted_path", ""))
                if not working_path.exists() or working_path.suffix.lower() not in DOCUMENT_EXTENSIONS:
                    continue
                inventory_rows.append(
                    build_file_record(
                        next_index,
                        working_path,
                        archive_row.get("inner_path", working_path.name),
                        "archive",
                        archive_path=archive_row.get("archive_path", ""),
                        archive_inner_path=archive_row.get("inner_path", ""),
                    )
                )
                next_index += 1
            continue

        if suffix in DOCUMENT_EXTENSIONS:
            inventory_rows.append(
                build_file_record(
                    next_index,
                    source,
                    source_key(source, input_root),
                    "normal",
                )
            )
            next_index += 1
    return inventory_rows, archive_rows, next_index


def filter_unique_file_rows_incremental(
    rows: list[dict[str, str]],
    seen_sha256: dict[str, str],
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    unique_rows: list[dict[str, str]] = []
    duplicate_rows: list[dict[str, str]] = []
    for row in rows:
        sha256 = row.get("sha256", "")
        if row.get("status") == "success" and sha256:
            if sha256 in seen_sha256:
                duplicate = dict(row)
                duplicate["status"] = "duplicate_skipped"
                duplicate["error_message"] = f"duplicate of {seen_sha256[sha256]}"
                duplicate_rows.append(
                    dedupe_row(
                        duplicate,
                        duplicate_type="file",
                        kept_file_id=seen_sha256[sha256],
                        reason="same sha256",
                        status="duplicate_skipped",
                    )
                )
                continue
            seen_sha256[sha256] = row.get("file_id", "")
        unique_rows.append(row)
    return unique_rows, duplicate_rows


def copy_unique_raw_text(unique_rows: list[dict[str, str]], destination_dir: str | Path) -> None:
    destination_root = Path(destination_dir)
    destination_root.mkdir(parents=True, exist_ok=True)
    for row in unique_rows:
        if row.get("status") != "success" or not row.get("output_path"):
            continue
        source = Path(row["output_path"])
        destination = destination_root / source.name
        shutil.copy2(source, destination)
        row["output_path"] = str(destination)


def safe_remove_generated_dir(path: str | Path, output_root: str | Path) -> None:
    target = Path(path).resolve()
    root = Path(output_root).resolve()
    if not target.exists():
        return
    if target == root or root not in target.parents:
        raise ValueError(f"refusing to remove directory outside output root: {target}")
    shutil.rmtree(target)


def run_stage1(
    input_dir: str | Path,
    output_dir: str | Path,
    limit: int | None = None,
    resume: bool = False,
    log_path: str | Path | None = None,
    batch_size: int = 100,
    keep_intermediate: bool = False,
    max_batches: int | None = None,
) -> dict[str, Path]:
    if batch_size < 1:
        raise ValueError("batch_size must be greater than 0")

    output_root = Path(output_dir).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    if log_path:
        configure_log(log_path)

    temp_dir = output_root / "_stage1_tmp"
    paths = {
        "temp_dir": temp_dir,
        "archive_output": temp_dir / "extracted_archives",
        "batch_root": temp_dir / "batches",
        "text_hash_index": temp_dir / "text_hash_index.csv",
        "sha_index": temp_dir / "sha_index.csv",
        "completed_sources": temp_dir / "completed_sources.csv",
        "archive_report": output_root / "archive_report.csv",
        "inventory": output_root / "inventory.csv",
        "inventory_unique": output_root / "inventory_unique.csv",
        "raw_text": temp_dir / "raw_text",
        "raw_text_unique": temp_dir / "raw_text_unique",
        "extract_report": output_root / "extract_text_report.csv",
        "extract_report_unique": output_root / "extract_text_report_unique.csv",
        "dedupe_report": output_root / "dedupe_report.csv",
        "sanitized_text": output_root / "sanitized_text",
        "sanitize_report": output_root / "sanitize_report.csv",
        "mapping": output_root / "sanitized_mapping.csv",
        "quality_report": output_root / "quality_report.csv",
        "subject_report": output_root / "subject_classification_report.csv",
        "tech_report": output_root / "tech_classification_report.csv",
        "tech_group_index": output_root / "tech_group_index.csv",
    }

    temp_dir.mkdir(parents=True, exist_ok=True)
    paths["batch_root"].mkdir(parents=True, exist_ok=True)

    logging.info(
        "stage1 start input=%s output=%s limit=%s resume=%s batch_size=%s keep_intermediate=%s",
        input_dir,
        output_root,
        limit,
        resume,
        batch_size,
        keep_intermediate,
    )

    if not resume:
        safe_remove_generated_dir(temp_dir, output_root)
        temp_dir.mkdir(parents=True, exist_ok=True)
        paths["batch_root"].mkdir(parents=True, exist_ok=True)
        write_csv_rows(paths["archive_report"], ARCHIVE_FIELDNAMES, [])
        write_csv_rows(paths["inventory"], INVENTORY_FIELDNAMES, [])
        write_csv_rows(paths["inventory_unique"], INVENTORY_FIELDNAMES, [])
        write_dedupe_report([], paths["dedupe_report"])
        write_text_hash_index({}, paths["text_hash_index"])
        write_sha_index({}, paths["sha_index"])
        write_csv_rows(paths["completed_sources"], COMPLETED_SOURCES_FIELDNAMES, [])
        write_extract_report([], paths["extract_report"], EXTRACT_REPORT_FIELDNAMES)
        write_extract_report([], paths["extract_report_unique"], EXTRACT_REPORT_FIELDNAMES)
        write_report_csv([], paths["sanitize_report"])
        write_mapping_csv([], paths["mapping"])
        write_quality_report([], paths["quality_report"])
        write_csv_rows(paths["subject_report"], SUBJECT_REPORT_FIELDNAMES, [])
        write_csv_rows(paths["tech_report"], TECH_REPORT_FIELDNAMES, [])
        write_csv_rows(paths["tech_group_index"], TECH_INDEX_FIELDNAMES, [])
    else:
        ensure_csv(paths["archive_report"], ARCHIVE_FIELDNAMES)
        ensure_csv(paths["inventory"], INVENTORY_FIELDNAMES)
        ensure_csv(paths["inventory_unique"], INVENTORY_FIELDNAMES)
        ensure_csv(paths["dedupe_report"], DEDUPE_FIELDNAMES)
        ensure_csv(paths["text_hash_index"], TEXT_HASH_FIELDNAMES)
        ensure_csv(paths["sha_index"], SHA_INDEX_FIELDNAMES)
        ensure_csv(paths["completed_sources"], COMPLETED_SOURCES_FIELDNAMES)
        ensure_csv(paths["extract_report"], EXTRACT_REPORT_FIELDNAMES)
        ensure_csv(paths["extract_report_unique"], EXTRACT_REPORT_FIELDNAMES)
        ensure_csv(paths["sanitize_report"], SANITIZE_FIELDNAMES)
        ensure_csv(paths["mapping"], MAPPING_FIELDNAMES)
        ensure_csv(paths["quality_report"], QUALITY_FIELDNAMES)
        ensure_csv(paths["subject_report"], SUBJECT_REPORT_FIELDNAMES)
        ensure_csv(paths["tech_report"], TECH_REPORT_FIELDNAMES)
        ensure_csv(paths["tech_group_index"], TECH_INDEX_FIELDNAMES)

    input_root = Path(input_dir).resolve()
    completed_sources = load_completed_sources(paths["completed_sources"]) if resume else set()
    if resume and not completed_sources:
        completed_sources = infer_completed_sources_from_inventory(paths["inventory"], input_root)
    pending_sources = (
        source
        for source in iter_stage1_sources(input_root, limit=limit)
        if source_key(source, input_root) not in completed_sources
    )
    seen_hashes = load_text_hash_index(paths["text_hash_index"])
    seen_sha256 = load_simple_index(paths["sha_index"], "sha256", "file_id")
    if resume and not seen_sha256:
        seen_sha256 = infer_sha_index_from_inventory(paths["inventory"])
    next_index = next_inventory_index(paths["inventory"])
    logging.info(
        "resume state completed_sources=%s next_inventory_index=%s sha_index=%s text_hash_index=%s",
        len(completed_sources),
        next_index,
        len(seen_sha256),
        len(seen_hashes),
    )

    processed_batches = 0
    for batch_index, batch_sources in enumerate(chunked_iterable(pending_sources, batch_size), start=1):
        if max_batches is not None and processed_batches >= max_batches:
            logging.info("stopping early after max_batches=%s", max_batches)
            return paths

        batch_dir = paths["batch_root"] / f"batch_{batch_index:06d}"
        batch_inventory = batch_dir / "inventory.csv"
        batch_inventory_unique = batch_dir / "inventory_unique.csv"
        batch_archive_output = batch_dir / "extracted_archives"
        batch_raw_text = batch_dir / "raw_text"
        batch_raw_text_unique = batch_dir / "raw_text_unique"
        batch_extract_report = batch_dir / "extract_text_report.csv"
        batch_extract_report_unique = batch_dir / "extract_text_report_unique.csv"
        safe_remove_generated_dir(batch_dir, output_root)
        batch_dir.mkdir(parents=True, exist_ok=True)

        batch_inventory_rows, batch_archive_rows, next_index = build_batch_inventory_rows(
            batch_sources,
            input_root,
            batch_archive_output,
            next_index,
        )
        unique_inventory_rows, file_dedupe_rows = filter_unique_file_rows_incremental(
            batch_inventory_rows,
            seen_sha256,
        )
        append_csv_rows(paths["archive_report"], ARCHIVE_FIELDNAMES, batch_archive_rows)
        append_csv_rows(paths["inventory"], INVENTORY_FIELDNAMES, batch_inventory_rows)
        append_csv_rows(paths["inventory_unique"], INVENTORY_FIELDNAMES, unique_inventory_rows)
        append_csv_rows(paths["dedupe_report"], DEDUPE_FIELDNAMES, file_dedupe_rows)
        write_sha_index(seen_sha256, paths["sha_index"])

        write_inventory_csv(unique_inventory_rows, batch_inventory)
        write_inventory_csv(unique_inventory_rows, batch_inventory_unique)
        extract_rows = extract_from_inventory(batch_inventory, batch_raw_text, batch_extract_report)
        unique_extract_rows, text_dedupe_rows = filter_unique_text_rows(extract_rows, seen_hashes)
        copy_unique_raw_text(unique_extract_rows, batch_raw_text_unique)
        write_extract_report(unique_extract_rows, batch_extract_report_unique, EXTRACT_REPORT_FIELDNAMES)

        append_csv_rows(paths["extract_report"], EXTRACT_REPORT_FIELDNAMES, extract_rows)
        append_csv_rows(paths["extract_report_unique"], EXTRACT_REPORT_FIELDNAMES, unique_extract_rows)
        append_csv_rows(paths["dedupe_report"], DEDUPE_FIELDNAMES, text_dedupe_rows)
        write_text_hash_index(seen_hashes, paths["text_hash_index"])

        if any(row.get("status") == "success" and row.get("output_path") for row in unique_extract_rows):
            sanitize_directory(
                batch_raw_text_unique,
                paths["sanitized_text"],
                paths["sanitize_report"],
                paths["mapping"],
                extract_report=batch_extract_report_unique,
                append=True,
            )

        append_completed_sources(paths["completed_sources"], batch_sources, input_root)

        if not keep_intermediate:
            safe_remove_generated_dir(batch_dir, output_root)
        processed_batches += 1
        logging.info(
            "batch %s complete sources=%s inventory_rows=%s file_duplicates=%s text_duplicates=%s archive_rows=%s",
            batch_index,
            len(batch_sources),
            len(batch_inventory_rows),
            len(file_dedupe_rows),
            len(text_dedupe_rows),
            len(batch_archive_rows),
        )

    if paths["sanitize_report"].exists():
        quality_rows = build_quality_report(paths["sanitize_report"], paths["quality_report"], paths["inventory_unique"])
        logging.info("quality rows=%s", len(quality_rows))
        mapping_rows = update_mapping_quality_status(paths["mapping"], paths["quality_report"])
        subject_rows = write_subject_report(mapping_rows, paths["subject_report"])
        logging.info("subject rows=%s", len(subject_rows))
        tech_rows, tech_index_rows = build_tech_reports(
            paths["mapping"],
            paths["quality_report"],
            paths["tech_report"],
            paths["tech_group_index"],
        )
        logging.info("tech rows=%s tech index rows=%s", len(tech_rows), len(tech_index_rows))

    if not keep_intermediate:
        safe_remove_generated_dir(temp_dir, output_root)

    logging.info("stage1 complete")
    return paths


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run stage 1 paper cleaning pipeline.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--log", default=None)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--keep-intermediate", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    paths = run_stage1(
        args.input,
        args.output,
        args.limit,
        args.resume,
        args.log,
        batch_size=args.batch_size,
        keep_intermediate=args.keep_intermediate,
    )
    for name, path in paths.items():
        print(f"{name}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
