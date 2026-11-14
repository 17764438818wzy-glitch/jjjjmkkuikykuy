from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path

from .tech_taxonomy import TECH_RULES, UNKNOWN_TECH_SUBTYPE, UNKNOWN_TECH_TYPE, TechRule

REPORT_FIELDNAMES = [
    "file_id",
    "detected_subject_group",
    "detected_major_or_direction",
    "detected_major_group",
    "detected_major",
    "detected_title",
    "tech_type",
    "tech_subtype",
    "tech_keywords",
    "tech_detect_source",
    "tech_confidence",
    "tech_reason",
    "sanitized_text_path",
    "sanitized_path",
]

INDEX_FIELDNAMES = [
    "tech_type",
    "tech_subtype",
    "file_id",
    "sanitized_text_path",
    "sanitized_path",
    "detected_major_or_direction",
    "detected_major",
    "detected_subject_group",
    "detected_major_group",
    "detected_title",
]

SOURCE_ORDER = ("title", "filename", "path", "abstract", "toc", "body_prefix")
SOURCE_WEIGHT = {
    "title": 6,
    "filename": 5,
    "path": 4,
    "abstract": 3,
    "toc": 2,
    "body_prefix": 1,
}
ABSTRACT_RE = re.compile(r"摘要(?P<body>[\s\S]{0,1600})")
TOC_RE = re.compile(r"目录(?P<body>[\s\S]{0,1600})")


@dataclass(frozen=True)
class TechClassificationResult:
    tech_type: str
    tech_subtype: str
    tech_keywords: tuple[str, ...]
    tech_detect_source: str
    tech_confidence: float
    tech_reason: str


@dataclass(frozen=True)
class RuleMatch:
    rule: TechRule
    keywords: tuple[str, ...]
    source: str
    strong_count: int
    weak_count: int
    score: int


def read_csv(path: str | Path) -> list[dict[str, str]]:
    csv_path = Path(path)
    if not csv_path.exists():
        return []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: str | Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    csv_path = Path(path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def contains_keyword(text: str, keyword: str) -> bool:
    if not text or not keyword:
        return False
    return keyword.casefold() in text.casefold()


def matched_keywords(text: str, keywords: tuple[str, ...]) -> list[str]:
    return [keyword for keyword in keywords if contains_keyword(text, keyword)]


def extract_abstract(text: str) -> str:
    match = ABSTRACT_RE.search(text)
    return match.group(0) if match else ""


def extract_toc(text: str) -> str:
    match = TOC_RE.search(text)
    return match.group(0) if match else ""


def build_sources(
    detected_title: str = "",
    original_file_name: str = "",
    original_relative_path: str = "",
    sanitized_text: str = "",
) -> dict[str, str]:
    return {
        "title": detected_title or "",
        "filename": original_file_name or "",
        "path": original_relative_path or "",
        "abstract": extract_abstract(sanitized_text),
        "toc": extract_toc(sanitized_text),
        "body_prefix": sanitized_text[:5000] if sanitized_text else "",
    }


def score_rule(rule: TechRule, sources: dict[str, str]) -> RuleMatch | None:
    best: RuleMatch | None = None
    for source in SOURCE_ORDER:
        text = sources[source]
        strong = matched_keywords(text, rule.strong_keywords)
        weak = matched_keywords(text, rule.weak_keywords)
        if not strong and not weak:
            continue
        keywords = tuple(dict.fromkeys(strong + weak))
        strong_count = len(strong)
        weak_count = len(weak)
        if rule.priority >= 650 and strong_count == 0:
            continue
        score = SOURCE_WEIGHT[source] * 10000 + rule.priority * 10 + strong_count * 20 + weak_count * 5
        candidate = RuleMatch(rule, keywords, source, strong_count, weak_count, score)
        if best is None or candidate.score > best.score:
            best = candidate
    return best


def confidence_for(match: RuleMatch) -> float:
    value = 0.35
    value += min(match.strong_count, 3) * 0.18
    value += min(match.weak_count, 3) * 0.07
    value += SOURCE_WEIGHT[match.source] * 0.025
    return round(min(value, 1.0), 2)


def classify_tech(
    detected_title: str = "",
    original_file_name: str = "",
    original_relative_path: str = "",
    sanitized_text: str = "",
) -> TechClassificationResult:
    sources = build_sources(detected_title, original_file_name, original_relative_path, sanitized_text)
    matches = [match for rule in TECH_RULES if (match := score_rule(rule, sources))]
    if not matches:
        return TechClassificationResult(
            UNKNOWN_TECH_TYPE,
            UNKNOWN_TECH_SUBTYPE,
            (),
            "unknown",
            0.0,
            "no technical-route keywords matched",
        )

    best = max(matches, key=lambda item: item.score)
    return TechClassificationResult(
        best.rule.tech_type,
        best.rule.tech_subtype,
        best.keywords,
        best.source,
        confidence_for(best),
        f"matched {best.rule.tech_type}/{best.rule.tech_subtype} by {', '.join(best.keywords)}",
    )


def quality_status_by_file_id(quality_report: str | Path) -> dict[str, str]:
    return {
        row.get("file_id", ""): row.get("quality_status", "")
        for row in read_csv(quality_report)
    }


def build_report_row(mapping_row: dict[str, str], result: TechClassificationResult) -> dict[str, str]:
    return {
        "file_id": mapping_row.get("file_id", ""),
        "detected_subject_group": mapping_row.get("detected_subject_group", mapping_row.get("detected_major_group", "")),
        "detected_major_or_direction": mapping_row.get("detected_major_or_direction", mapping_row.get("detected_major", "")),
        "detected_major_group": mapping_row.get("detected_major_group", ""),
        "detected_major": mapping_row.get("detected_major", ""),
        "detected_title": mapping_row.get("detected_title", ""),
        "tech_type": result.tech_type,
        "tech_subtype": result.tech_subtype,
        "tech_keywords": ";".join(result.tech_keywords),
        "tech_detect_source": result.tech_detect_source,
        "tech_confidence": f"{result.tech_confidence:.2f}",
        "tech_reason": result.tech_reason,
        "sanitized_text_path": mapping_row.get("sanitized_text_path", mapping_row.get("sanitized_path", "")),
        "sanitized_path": mapping_row.get("sanitized_path", ""),
    }


def build_index_row(report_row: dict[str, str]) -> dict[str, str]:
    return {
        "tech_type": report_row["tech_type"],
        "tech_subtype": report_row["tech_subtype"],
        "file_id": report_row["file_id"],
        "sanitized_text_path": report_row["sanitized_text_path"],
        "sanitized_path": report_row["sanitized_path"],
        "detected_major_or_direction": report_row["detected_major_or_direction"],
        "detected_major": report_row["detected_major"],
        "detected_subject_group": report_row["detected_subject_group"],
        "detected_major_group": report_row["detected_major_group"],
        "detected_title": report_row["detected_title"],
    }


def build_tech_reports(
    mapping_csv: str | Path,
    quality_report: str | Path,
    output_report: str | Path,
    index_report: str | Path,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    quality_status = quality_status_by_file_id(quality_report)
    report_rows: list[dict[str, str]] = []
    index_rows: list[dict[str, str]] = []
    for mapping_row in read_csv(mapping_csv):
        file_id = mapping_row.get("file_id", "")
        if quality_status.get(file_id) not in {"pass", "review"}:
            continue
        sanitized_path = Path(mapping_row.get("sanitized_text_path") or mapping_row.get("sanitized_path", ""))
        sanitized_text = sanitized_path.read_text(encoding="utf-8") if sanitized_path.exists() else ""
        original_relative_path = mapping_row.get("original_relative_path", "")
        if mapping_row.get("tech_type") == "不适用":
            result = TechClassificationResult("不适用", "不适用", (), "not_applicable", 0.0, "non-technical paper")
        else:
            result = classify_tech(
                detected_title=mapping_row.get("detected_title", ""),
                original_file_name=Path(original_relative_path).name,
                original_relative_path=original_relative_path,
                sanitized_text=sanitized_text[:5000],
            )
        report_row = build_report_row(mapping_row, result)
        report_rows.append(report_row)
        index_rows.append(build_index_row(report_row))

    write_csv(output_report, REPORT_FIELDNAMES, report_rows)
    write_csv(index_report, INDEX_FIELDNAMES, index_rows)
    return report_rows, index_rows


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Classify project technical routes by rules.")
    parser.add_argument("--mapping", required=True)
    parser.add_argument("--quality-report", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--index", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    rows, index_rows = build_tech_reports(args.mapping, args.quality_report, args.report, args.index)
    print(f"Tech classification rows: {len(rows)}")
    print(f"Tech group index rows: {len(index_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
