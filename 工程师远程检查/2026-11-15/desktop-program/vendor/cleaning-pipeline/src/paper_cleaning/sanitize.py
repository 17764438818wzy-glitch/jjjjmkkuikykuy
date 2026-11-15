from __future__ import annotations

import argparse
import csv
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from .subject_taxonomy import (
    UNKNOWN_DIRECTION,
    SubjectClassificationResult,
    classify_subject,
    prefix_for_direction,
)

FRONT_MATTER_ANCHORS = (
    "摘要",
    "ABSTRACT",
    "Abstract",
    "目录",
    "第一章",
    "第1章",
    "绪论",
)

FRONT_MATTER_ANCHOR_RE = re.compile(
    r"(?m)(?P<anchor>^[^\S\r\n]*("
    r"摘[^\S\r\n]*要"
    r"|ABSTRACT"
    r"|Abstract"
    r"|目[^\S\r\n]*录"
    r"|第[^\S\r\n]*一[^\S\r\n]*章"
    r"|第[^\S\r\n]*1[^\S\r\n]*章"
    r"|绪论"
    r"))"
)

TITLE_LINE_RE = re.compile(
    r"(?m)^[^\S\r\n]*(?:论文)?题[^\S\r\n]*目[^\S\r\n]*[：:]?[^\S\r\n]*(?P<title>.+?)\s*$"
)
TITLE_PREFIX_RE = re.compile(r"(?m)^[^\S\r\n]*题[^\S\r\n]*目[^\S\r\n]*[：:]")
FILENAME_CLEAN_RE = re.compile(
    r"(\([12]\d{3}[-._]\d{1,2}[-._]\d{1,2}[^)]*\)|"
    r"（[12]\d{3}[-._]\d{1,2}[-._]\d{1,2}[^）]*）|"
    r"正文|论文|终稿|定稿|初稿|已修改|修改稿|修改|最终版|毕业设计|毕业论文)",
    re.IGNORECASE,
)
TITLE_FIELD_NOISE_RE = re.compile(
    r"(本科毕业论文|毕业论文|毕业设计|论文题目|题目|学院|学校|专业|班级|学号|学生姓名|姓名|作者|指导教师|导师|"
    r"摘要|目录|ABSTRACT|Abstract|Contents|Introduction|Acknowledgements|"
    r"类型|编号|国家开放大学|大学|UNIVERSITY|COLLEGE|开题报告|\[PAGE_BREAK\]|\[SECTION_BREAK\])",
    re.IGNORECASE,
)

FILENAME_TITLE_NOISE_RE = re.compile(
    r"(改重|副本|开题报告|修改版|仅供修改|查重版)",
    re.IGNORECASE,
)

CLASSIFICATION_HINT_RE = re.compile(
    r"(?ms)^DITY_CLASSIFICATION_HINT_START\s*\n.*?^DITY_CLASSIFICATION_HINT_END\s*\n?"
)

FRONT_MATTER_ANCHOR_SEARCH_CHARS = 8000
TITLE_SEARCH_CHARS = 3000


REPORT_FIELDNAMES = [
    "file_id",
    "file_name",
    "input_path",
    "output_path",
    "detected_major",
    "detected_subject_group",
    "detected_major_or_direction",
    "tech_type",
    "tech_subtype",
    "tech_keywords",
    "tech_confidence",
    "sanitized_path",
    "sanitized_text_path",
    "original_char_count",
    "sanitized_char_count",
    "removed_front_matter",
    "removed_front_matter_chars",
    "phone_hits",
    "email_hits",
    "id_card_hits",
    "student_id_hits",
    "student_name_field_hits",
    "teacher_field_hits",
    "class_field_hits",
    "contact_hits",
    "status",
    "error_message",
]

MAPPING_FIELDNAMES = [
    "file_id",
    "source_type",
    "original_relative_path",
    "archive_path",
    "archive_inner_path",
    "raw_text_path",
    "sanitized_path",
    "detected_major",
    "detected_major_group",
    "detected_subject_group",
    "detected_major_or_direction",
    "major_detect_source",
    "major_confidence",
    "detected_title",
    "tech_type",
    "tech_subtype",
    "tech_keywords",
    "tech_confidence",
    "sanitized_text_path",
    "quality_status",
    "status",
    "error_message",
]

STAT_FIELDS = [
    "phone_hits",
    "email_hits",
    "id_card_hits",
    "student_id_hits",
    "student_name_field_hits",
    "teacher_field_hits",
    "class_field_hits",
    "contact_hits",
]

PHONE_RE = re.compile(r"(?<![\d.])1[3-9]\d{9}(?![\d.])")
EMAIL_RE = re.compile(
    r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"
)
ID_CARD_RE = re.compile(
    r"(?<!\d)\d{6}(?:18|19|20)\d{2}(?:0[1-9]|1[0-2])"
    r"(?:0[1-9]|[12]\d|3[01])\d{3}[\dXx](?!\d)"
)

FIELD_SPACE = r"[^\S\r\n]*"
REQUIRED_SEPARATOR = rf"(?:{FIELD_SPACE}[：:]{FIELD_SPACE}|[^\S\r\n]+)"
STUDENT_ID_RE = re.compile(
    rf"(?P<label>学{FIELD_SPACE}号|学生证号|Student{FIELD_SPACE}ID)"
    rf"(?P<sep>{REQUIRED_SEPARATOR})"
    r"(?P<value>[A-Za-z0-9][A-Za-z0-9_-]{5,29})",
    re.IGNORECASE,
)
STUDENT_NAME_FIELD_RE = re.compile(
    rf"(?P<label>学生姓名|姓{FIELD_SPACE}名|作者)"
    rf"(?P<sep>{REQUIRED_SEPARATOR})"
    r"(?P<value>[\u4e00-\u9fff·]{2,20})"
)
TEACHER_FIELD_RE = re.compile(
    rf"(?P<label>指导教师|指导老师|导师)"
    rf"(?P<sep>{REQUIRED_SEPARATOR})"
    r"(?P<value>[\u4e00-\u9fff·]{2,20})"
)
CLASS_FIELD_RE = re.compile(
    rf"(?P<label>专业班级|班级)"
    rf"(?P<sep>{REQUIRED_SEPARATOR})"
    r"(?P<value>[^\r\n]{2,60})"
)
CONTACT_FIELD_RE = re.compile(
    rf"(?P<label>QQ|微信|WeChat)"
    rf"(?P<sep>{REQUIRED_SEPARATOR})"
    r"(?P<value>[^\s,，;；\r\n]{2,80})",
    re.IGNORECASE,
)

@dataclass(frozen=True)
class FrontMatterResult:
    text: str
    removed: bool
    removed_chars: int


@dataclass(frozen=True)
class SanitizeResult:
    text: str
    stats: dict[str, int]
    removed_front_matter: bool
    removed_front_matter_chars: int


def remove_front_matter(text: str) -> FrontMatterResult:
    search_area = text[:FRONT_MATTER_ANCHOR_SEARCH_CHARS]
    matches = list(FRONT_MATTER_ANCHOR_RE.finditer(search_area))
    if not matches:
        return FrontMatterResult(text=text, removed=False, removed_chars=0)

    first_anchor = min(match.start("anchor") for match in matches)
    if first_anchor <= 0:
        return FrontMatterResult(text=text, removed=False, removed_chars=0)
    title_line = extract_title_line(text[:first_anchor])
    retained_text = text[first_anchor:]
    if title_line:
        retained_text = f"{title_line}\n{retained_text}"
    return FrontMatterResult(
        text=retained_text,
        removed=True,
        removed_chars=first_anchor - (len(title_line) + 1 if title_line else 0),
    )


def extract_title_line(front_matter: str) -> str:
    title = extract_title_from_text(front_matter)
    if title:
        return f"题目：{title}"
    return ""


def clean_title_candidate(value: str) -> str:
    title = value.strip().strip("：:;；,，。 ")
    title = re.sub(r"^(?:写作|开题)\s+", "", title)
    title = re.sub(r"^改(?=生态学)", "", title)
    title = re.sub(r"\s*任务书$", "", title)
    title = re.sub(r"^(课题名称|论文题目|题目)\s*[:：]?\s*", "", title)
    title = re.sub(r"^\d{1,2}\.\d{1,2}\s*", "", title)
    title = re.sub(r"^(?:\d+(?:[.、]|\s+)|[\-_\s])+", "", title)
    title = re.sub(r"(?:--|——|-)[\u4e00-\u9fff]{2,4}\d*稿?$", "", title)
    title = re.sub(r"\s{2,}[\u4e00-\u9fff]{2,4}\d*稿?$", "", title)
    title = re.sub(r"[.。．][\u4e00-\u9fff]{2,4}(?:\(\d+\))?$", "", title)
    title = title.strip().strip("：:;；,，。()（） ")
    return title.strip()


def is_noise_title_line(value: str) -> bool:
    compact = re.sub(r"\s+", "", value)
    return bool(TITLE_FIELD_NOISE_RE.search(value) or TITLE_FIELD_NOISE_RE.search(compact))


def looks_like_title_line(line: str) -> bool:
    title = clean_title_candidate(line)
    if not title:
        return False
    if is_noise_title_line(title):
        return False
    if "：" in title or ":" in title:
        return False
    if "\t" in title:
        return False
    if len(title) < 6 or len(title) > 90:
        return False
    if title.lower() in {"contents", "table of contents"}:
        return False
    return bool(re.search(r"[\u4e00-\u9fffA-Za-z]", title))


def extract_title_from_text(text: str) -> str:
    search_area = front_matter_search_area(text)
    for match in TITLE_LINE_RE.finditer(search_area):
        title = clean_title_candidate(match.group("title"))
        if title:
            return title
    for raw_line in search_area.splitlines():
        line = clean_title_candidate(raw_line)
        if is_noise_title_line(line):
            break
        if looks_like_title_line(raw_line):
            return clean_title_candidate(raw_line)
    return ""


def front_matter_search_area(text: str) -> str:
    search_area = text[:TITLE_SEARCH_CHARS]
    anchor = FRONT_MATTER_ANCHOR_RE.search(search_area)
    return search_area[: anchor.start("anchor")] if anchor else search_area


def clean_title_from_filename(original_relative_path: str) -> str:
    if not original_relative_path:
        return ""
    stem = PurePosixPath(original_relative_path.replace("\\", "/")).stem
    title = FILENAME_CLEAN_RE.sub("", stem)
    title = re.sub(r"^[\u4e00-\u9fff]{2,4}\s*[-—_]\s*(?=.{4,})", "", title)
    title = clean_title_candidate(title.replace("_", " ").replace("-", " "))
    if FILENAME_TITLE_NOISE_RE.search(title):
        return ""
    has_chinese = bool(re.search(r"[\u4e00-\u9fff]", title))
    min_len = 4 if has_chinese else 6
    return title if min_len <= len(title) <= 90 else ""


def extract_title(text: str, original_relative_path: str = "") -> str:
    return extract_title_from_text(text) or clean_title_from_filename(original_relative_path)


def ensure_title_prefix(result: SanitizeResult, detected_title: str) -> SanitizeResult:
    if not detected_title or TITLE_PREFIX_RE.search(result.text[:200]):
        return result
    return SanitizeResult(
        text=f"题目：{detected_title}\n{result.text}",
        stats=result.stats,
        removed_front_matter=result.removed_front_matter,
        removed_front_matter_chars=result.removed_front_matter_chars,
    )


def detect_major(text: str, original_relative_path: str) -> str:
    return classify_subject(text, original_relative_path).detected_major_or_direction


def strip_classification_hints(text: str) -> str:
    return CLASSIFICATION_HINT_RE.sub("", text).lstrip()


def replace_field(pattern: re.Pattern[str], text: str, token: str) -> tuple[str, int]:
    def replacement(match: re.Match[str]) -> str:
        return f"{match.group('label')}{match.group('sep')}{token}"

    return pattern.subn(replacement, text)


def sanitize_text(text: str) -> SanitizeResult:
    front_matter = remove_front_matter(text)
    sanitized = front_matter.text
    stats = {field: 0 for field in STAT_FIELDS}

    sanitized, stats["phone_hits"] = PHONE_RE.subn("[PHONE]", sanitized)
    sanitized, stats["email_hits"] = EMAIL_RE.subn("[EMAIL]", sanitized)
    sanitized, stats["id_card_hits"] = ID_CARD_RE.subn("[ID_CARD]", sanitized)
    sanitized, stats["student_id_hits"] = replace_field(
        STUDENT_ID_RE,
        sanitized,
        "[STUDENT_ID]",
    )
    sanitized, stats["student_name_field_hits"] = replace_field(
        STUDENT_NAME_FIELD_RE,
        sanitized,
        "[NAME]",
    )
    sanitized, stats["teacher_field_hits"] = replace_field(
        TEACHER_FIELD_RE,
        sanitized,
        "[TEACHER]",
    )
    sanitized, stats["class_field_hits"] = replace_field(
        CLASS_FIELD_RE,
        sanitized,
        "[CLASS]",
    )
    sanitized, stats["contact_hits"] = replace_field(
        CONTACT_FIELD_RE,
        sanitized,
        "[CONTACT]",
    )

    return SanitizeResult(
        text=sanitized,
        stats=stats,
        removed_front_matter=front_matter.removed,
        removed_front_matter_chars=front_matter.removed_chars,
    )


def iter_text_files(input_dir: Path) -> list[Path]:
    return sorted(input_dir.rglob("*.txt"), key=lambda path: path.as_posix().lower())


def build_report_row(
    source: Path,
    destination: Path,
    file_id: str,
    subject_info: SubjectClassificationResult,
    original_text: str,
    result: SanitizeResult,
    status: str = "success",
    error_message: str = "",
) -> dict[str, str]:
    row = {
        "file_id": file_id,
        "file_name": source.name,
        "input_path": str(source),
        "output_path": str(destination),
        "detected_major": subject_info.detected_major_or_direction,
        "detected_subject_group": subject_info.detected_subject_group,
        "detected_major_or_direction": subject_info.detected_major_or_direction,
        "tech_type": subject_info.tech_type,
        "tech_subtype": subject_info.tech_subtype,
        "tech_keywords": ";".join(subject_info.tech_keywords),
        "tech_confidence": f"{subject_info.tech_confidence:.2f}",
        "sanitized_path": str(destination),
        "sanitized_text_path": str(destination),
        "original_char_count": str(len(original_text)),
        "sanitized_char_count": str(len(result.text)),
        "removed_front_matter": "true" if result.removed_front_matter else "false",
        "removed_front_matter_chars": str(result.removed_front_matter_chars),
        "status": status,
        "error_message": error_message,
    }
    for field in STAT_FIELDS:
        row[field] = str(result.stats[field])
    return row


def build_mapping_row(
    source: Path,
    source_root: Path,
    destination: Path,
    file_id: str,
    subject_info: SubjectClassificationResult,
    detected_title: str,
    result: SanitizeResult,
    metadata: dict[str, str] | None = None,
    status: str = "success",
    error_message: str = "",
) -> dict[str, str]:
    metadata = metadata or {}
    return {
        "file_id": file_id,
        "source_type": metadata.get("source_type", "normal"),
        "original_relative_path": metadata.get(
            "original_relative_path",
            source.relative_to(source_root).as_posix(),
        ),
        "archive_path": metadata.get("archive_path", ""),
        "archive_inner_path": metadata.get("archive_inner_path", ""),
        "raw_text_path": str(source),
        "sanitized_path": str(destination),
        "detected_major": subject_info.detected_major_or_direction,
        "detected_major_group": subject_info.detected_subject_group,
        "detected_subject_group": subject_info.detected_subject_group,
        "detected_major_or_direction": subject_info.detected_major_or_direction,
        "major_detect_source": subject_info.subject_detect_source,
        "major_confidence": subject_info.subject_confidence,
        "detected_title": detected_title,
        "tech_type": subject_info.tech_type,
        "tech_subtype": subject_info.tech_subtype,
        "tech_keywords": ";".join(subject_info.tech_keywords),
        "tech_confidence": f"{subject_info.tech_confidence:.2f}",
        "sanitized_text_path": str(destination),
        "quality_status": "",
        "status": status,
        "error_message": error_message,
    }


def empty_error_result() -> SanitizeResult:
    return SanitizeResult(
        text="",
        stats={field: 0 for field in STAT_FIELDS},
        removed_front_matter=False,
        removed_front_matter_chars=0,
    )


def write_report_csv(rows: list[dict[str, str]], report_path: str | Path) -> None:
    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def write_mapping_csv(rows: list[dict[str, str]], mapping_path: str | Path) -> None:
    path = Path(mapping_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MAPPING_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def read_existing_csv(path: str | Path) -> list[dict[str, str]]:
    csv_path = Path(path)
    if not csv_path.exists():
        return []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def counters_from_mapping(rows: list[dict[str, str]]) -> dict[str, int]:
    counters: dict[str, int] = defaultdict(int)
    for row in rows:
        file_id = row.get("file_id", "")
        match = re.match(r"^(?P<prefix>[A-Za-z0-9]+)_(?P<number>\d+)$", file_id)
        if match:
            counters[match.group("prefix")] = max(
                counters[match.group("prefix")],
                int(match.group("number")),
            )
    return counters


def destination_for(
    output_root: Path,
    subject_info: SubjectClassificationResult,
    counters: dict[str, int],
) -> tuple[str, Path]:
    direction = subject_info.detected_major_or_direction
    prefix = prefix_for_direction(direction)
    counters[prefix] += 1
    file_id = f"{prefix}_{counters[prefix]:06d}"
    return (
        file_id,
        output_root / subject_info.detected_subject_group_dir / direction / f"{file_id}.txt",
    )


def sanitize_directory(
    input_dir: str | Path,
    output_dir: str | Path,
    report_path: str | Path,
    mapping_path: str | Path,
    limit: int | None = None,
    extract_report: str | Path | None = None,
    append: bool = False,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    source_root = Path(input_dir).resolve()
    destination_root = Path(output_dir).resolve()
    if limit is not None and limit < 1:
        raise ValueError("limit must be greater than 0")
    if not source_root.exists():
        raise FileNotFoundError(f"Input directory does not exist: {source_root}")
    if not source_root.is_dir():
        raise NotADirectoryError(f"Input path is not a directory: {source_root}")
    if source_root == destination_root:
        raise ValueError("Input and output directories must be different")

    destination_root.mkdir(parents=True, exist_ok=True)
    report_rows: list[dict[str, str]] = read_existing_csv(report_path) if append else []
    mapping_rows: list[dict[str, str]] = read_existing_csv(mapping_path) if append else []
    counters: dict[str, int] = counters_from_mapping(mapping_rows) if append else defaultdict(int)
    metadata_by_raw_path = load_extract_metadata(extract_report)
    sources = iter_text_files(source_root)
    if limit is not None:
        sources = sources[:limit]

    for source in sources:
        try:
            original_text = source.read_text(encoding="utf-8")
            sanitizable_text = strip_classification_hints(original_text)
            metadata = metadata_by_raw_path.get(str(source.resolve()), {})
            original_relative_path = metadata.get(
                "original_relative_path",
                source.relative_to(source_root).as_posix(),
            )
            detected_title = extract_title(sanitizable_text, original_relative_path)
            subject_info = classify_subject(original_text, original_relative_path, detected_title)
            direction = subject_info.detected_major_or_direction
            file_id = metadata.get("file_id", "")
            if file_id:
                prefix = prefix_for_direction(direction)
                counters[prefix] += 1
                file_id = f"{prefix}_{counters[prefix]:06d}"
                destination = destination_root / subject_info.detected_subject_group_dir / direction / f"{file_id}.txt"
            else:
                file_id, destination = destination_for(destination_root, subject_info, counters)
            result = ensure_title_prefix(sanitize_text(sanitizable_text), detected_title)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(result.text, encoding="utf-8")
            report_rows.append(
                build_report_row(
                    source,
                    destination,
                    file_id,
                    subject_info,
                    original_text,
                    result,
                )
            )
            mapping_rows.append(
                build_mapping_row(
                    source,
                    source_root,
                    destination,
                    file_id,
                    subject_info,
                    detected_title,
                    result,
                    metadata=metadata,
                )
            )
        except Exception as exc:
            original_text = ""
            subject_info = classify_subject("", "", "")
            file_id, destination = destination_for(
                destination_root,
                subject_info,
                counters,
            )
            result = empty_error_result()
            report_rows.append(
                build_report_row(
                    source,
                    destination,
                    file_id,
                    subject_info,
                    original_text,
                    result,
                    status="error",
                    error_message=str(exc),
                )
            )
            mapping_rows.append(
                build_mapping_row(
                    source,
                    source_root,
                    destination,
                    file_id,
                    subject_info,
                    "",
                    result,
                    metadata=metadata_by_raw_path.get(str(source.resolve()), {}),
                    status="error",
                    error_message=str(exc),
                )
            )

    write_report_csv(report_rows, report_path)
    write_mapping_csv(mapping_rows, mapping_path)
    return report_rows, mapping_rows


def load_extract_metadata(extract_report: str | Path | None) -> dict[str, dict[str, str]]:
    if not extract_report:
        return {}
    path = Path(extract_report)
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    metadata: dict[str, dict[str, str]] = {}
    for row in rows:
        raw_path = row.get("output_path", "")
        if raw_path:
            metadata[str(Path(raw_path).resolve())] = row
    return metadata


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Remove front matter and redact explicit sensitive fields."
    )
    parser.add_argument("--input", required=True, help="Input raw_text directory.")
    parser.add_argument("--output", required=True, help="Output sanitized_text directory.")
    parser.add_argument("--report", required=True, help="Output sanitize report CSV.")
    parser.add_argument("--mapping", required=True, help="Output internal mapping CSV.")
    parser.add_argument("--extract-report", default=None, help="Optional extract_text_report.csv for source metadata.")
    parser.add_argument("--append", action="store_true", help="Append to existing sanitize report and mapping.")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of txt files to sanitize.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    rows, _mapping_rows = sanitize_directory(
        args.input,
        args.output,
        args.report,
        args.mapping,
        limit=args.limit,
        extract_report=args.extract_report,
        append=args.append,
    )
    success_count = sum(1 for row in rows if row["status"] == "success")
    error_count = len(rows) - success_count
    print(f"Processed {len(rows)} txt files: {success_count} success, {error_count} error")
    print(f"Output: {Path(args.output).resolve()}")
    print(f"Report: {Path(args.report).resolve()}")
    print(f"Mapping: {Path(args.mapping).resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
