from __future__ import annotations

import argparse
import shutil
from typing import Iterable
from pathlib import Path

from .inventory import DOCUMENT_EXTENSIONS, iter_files


def normalize_extensions(extensions: list[str] | None) -> set[str] | None:
    if not extensions:
        return None
    normalized = set()
    for extension in extensions:
        extension = extension.strip().lower()
        if not extension:
            continue
        normalized.add(extension if extension.startswith(".") else f".{extension}")
    return normalized


def iter_candidate_files(
    input_dir: Path,
    output_dir: Path,
    extensions: set[str] | None,
    exclude: Iterable[str] | None = None,
) -> list[Path]:
    output_resolved = output_dir.resolve()
    files = []
    for path in iter_files(input_dir, mode="full", exclude=exclude):
        try:
            path.resolve().relative_to(output_resolved)
            continue
        except ValueError:
            pass
        files.append(path)

    if extensions is not None:
        return [path for path in files if path.suffix.lower() in extensions]

    document_files = [
        path for path in files if path.suffix.lower() in DOCUMENT_EXTENSIONS
    ]
    other_files = [
        path for path in files if path.suffix.lower() not in DOCUMENT_EXTENSIONS
    ]
    return document_files + other_files


def build_sample(
    input_dir: str | Path,
    output_dir: str | Path,
    limit: int = 20,
    extensions: list[str] | None = None,
    exclude: Iterable[str] | None = None,
) -> list[str]:
    if limit < 1:
        raise ValueError("limit must be greater than 0")

    root = Path(input_dir).resolve()
    destination_root = Path(output_dir).resolve()
    if not root.exists():
        raise FileNotFoundError(f"Input directory does not exist: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"Input path is not a directory: {root}")
    destination_root.mkdir(parents=True, exist_ok=True)

    selected_extensions = normalize_extensions(extensions)
    copied: list[str] = []
    for source in iter_candidate_files(
        root,
        destination_root,
        selected_extensions,
        exclude=exclude,
    ):
        if len(copied) >= limit:
            break
        relative_path = source.relative_to(root)
        destination = destination_root / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        copied.append(relative_path.as_posix())
    return copied


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Copy a small sample of files.")
    parser.add_argument("--input", required=True, help="Input directory.")
    parser.add_argument("--output", required=True, help="Sample output directory.")
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of files to copy.",
    )
    parser.add_argument(
        "--extensions",
        nargs="*",
        default=None,
        help="Optional extension filter, for example: .pdf .docx .txt",
    )
    parser.add_argument(
        "--exclude",
        nargs="*",
        default=None,
        help="Additional directory names to skip. Defaults are always applied.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    copied = build_sample(
        args.input,
        args.output,
        limit=args.limit,
        extensions=args.extensions,
        exclude=args.exclude,
    )
    print(f"Copied {len(copied)} files to {Path(args.output).resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
