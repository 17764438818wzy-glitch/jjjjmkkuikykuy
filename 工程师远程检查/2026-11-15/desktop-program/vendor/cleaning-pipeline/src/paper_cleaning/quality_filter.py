from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path

REPORT_FIELDNAMES = [
    "file_id",
    "sanitized_path",
    "detected_major",
    "char_count",
    "has_abstract",
    "has_references",
    "has_body",
    "duplicate_status",
    "quality_status",
    "quality_reason",
]

ABSTRACT_RE = re.compile(r"摘\s*要|Abstract|ABSTRACT")
REFERENCES_RE = re.compile(r"参考文献|References", re.IGNORECASE)
BODY_RE = re.compile(r"绪论|第\s*[一1]\s*章|\n1[ .、]")


def read_csv(path: str | Path) -> list[dict[str, str]]:
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def classify_quality(text: str, duplicate_status: str = "") -> tuple[str, str, bool, bool, bool]:
    char_count = len(text)
    has_abstract = bool(ABSTRACT_RE.search(text[:5000]))
    has_references = bool(REFERENCES_RE.search(text))
    has_body = bool(BODY_RE.search(text)) or char_count >= 5000
    replacement = text.count("\ufffd")
    weird_ratio = replacement / max(char_count, 1)

    if duplicate_status in {"duplicate", "true"}:
        return "reject", "reject_duplicate", has_abstract, has_references, has_body
    if char_count < 3000:
        return "reject", "too_short", has_abstract, has_references, has_body
    if weird_ratio > 0.02:
        return "reject", "garbled_text", has_abstract, has_references, has_body
    if not has_body:
        return "reject", "no_body", has_abstract, has_references, has_body
    reasons = []
    if not has_abstract:
        reasons.append("missing_abstract")
    if not has_references:
        reasons.append("missing_references")
    if reasons:
        return "review", ";".join(reasons), has_abstract, has_references, has_body
    return "pass", "", has_abstract, has_references, has_body


def build_quality_report(
    sanitize_report: str | Path,
    output_report: str | Path,
    inventory_csv: str | Path | None = None,
) -> list[dict[str, str]]:
    sanitize_rows = read_csv(sanitize_report)
    duplicate_by_file_id: dict[str, str] = {}
    if inventory_csv and Path(inventory_csv).exists():
        for row in read_csv(inventory_csv):
            duplicate_by_file_id[row.get("file_id", "")] = (
                "duplicate" if row.get("is_duplicate") == "true" else ""
            )

    rows = []
    for sanitize_row in sanitize_rows:
        path = Path(sanitize_row["sanitized_path"])
        text = path.read_text(encoding="utf-8") if path.exists() else ""
        duplicate_status = duplicate_by_file_id.get(sanitize_row.get("file_id", ""), "")
        quality_status, reason, has_abstract, has_references, has_body = classify_quality(text, duplicate_status)
        rows.append(
            {
                "file_id": sanitize_row.get("file_id", ""),
                "sanitized_path": sanitize_row.get("sanitized_path", ""),
                "detected_major": sanitize_row.get("detected_major", ""),
                "char_count": str(len(text)),
                "has_abstract": "true" if has_abstract else "false",
                "has_references": "true" if has_references else "false",
                "has_body": "true" if has_body else "false",
                "duplicate_status": duplicate_status,
                "quality_status": quality_status,
                "quality_reason": reason,
            }
        )
    write_quality_report(rows, output_report)
    return rows


def write_quality_report(rows: list[dict[str, str]], output_report: str | Path) -> None:
    path = Path(output_report)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Filter low-quality sanitized texts.")
    parser.add_argument("--sanitize-report", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--inventory", default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    rows = build_quality_report(args.sanitize_report, args.output, args.inventory)
    print(f"Quality rows: {len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
