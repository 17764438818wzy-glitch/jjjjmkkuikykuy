"""Run legacy rules with progress reporting and the requested title/acknowledgement fixes."""
from __future__ import annotations

import json
from functools import wraps
from pathlib import Path

PREFIX = "UPSTREAM_PROGRESS "


def emit(stage, filename="", message="", level="info"):
    print(PREFIX + json.dumps({"stage": stage, "file": filename, "message": message, "level": level}, ensure_ascii=False), flush=True)


def main():
    from src.paper_cleaning import extract_text, run_stage1, sanitize
    from content_rules import detect_title, remove_acknowledgements

    sanitize.extract_title = detect_title
    sanitize.extract_title_from_text = detect_title
    original_sanitize = sanitize.sanitize_text
    @wraps(original_sanitize)
    def sanitize_content(text):
        cleaned, count = remove_acknowledgements(text)
        result = original_sanitize(cleaned)
        if count:
            emit("删除致谢", message=f"已移除 {count} 个致谢章节")
        return result
    sanitize.sanitize_text = sanitize_content

    original_text_dedupe = run_stage1.filter_unique_text_rows
    def evidence_aware_dedupe(rows, seen_hashes):
        # Identical prose does not imply identical figures/tables in two binary documents.
        text_rows=[r for r in rows if Path(r.get('input_path','')).suffix.lower() not in {'.docx','.pdf'}]
        unique,duplicates=original_text_dedupe(text_rows,seen_hashes)
        keep_ids={id(r) for r in unique}
        return [r for r in rows if id(r) in keep_ids or Path(r.get('input_path','')).suffix.lower() in {'.docx','.pdf'}],duplicates
    run_stage1.filter_unique_text_rows=evidence_aware_dedupe

    source_names = {}
    original_extract = extract_text.extract_text_by_extension
    @wraps(original_extract)
    def extract(source, *args, **kwargs):
        name = Path(source).name
        emit("提取正文", name, "开始提取")
        return original_extract(source, *args, **kwargs)
    extract_text.extract_text_by_extension = extract

    original_extract_report = extract_text.build_report_row
    @wraps(original_extract_report)
    def extract_report(source, *args, **kwargs):
        result = original_extract_report(source, *args, **kwargs)
        name = Path(source).name
        source_names[result.get("file_id", "")] = name
        ok = result.get("status") == "success"
        emit("提取完成" if ok else "提取失败", name,
             f"已提取 {result.get('char_count', 0)} 字符" if ok else result.get("error_message") or result.get("status", "失败"),
             "info" if ok else "error")
        return result
    extract_text.build_report_row = extract_report

    original_paths = sanitize.iter_text_files
    class TrackedPaths(list):
        def __iter__(self):
            for path in super().__iter__():
                emit("脱敏与分类", source_names.get(path.stem, path.name), "调用原脱敏规则和固定分类表")
                yield path
    sanitize.iter_text_files = lambda root: TrackedPaths(original_paths(root))

    original_report = sanitize.build_report_row
    @wraps(original_report)
    def report(*args, **kwargs):
        row = original_report(*args, **kwargs)
        ok = row.get("status") == "success"
        path = Path(row.get("input_path", ""))
        emit("脱敏分类完成" if ok else "脱敏分类失败", source_names.get(path.stem, path.name),
             row.get("detected_major_or_direction", "") if ok else row.get("error_message", ""), "info" if ok else "error")
        return row
    sanitize.build_report_row = report

    original_quality = run_stage1.build_quality_report
    @wraps(original_quality)
    def quality(*args, **kwargs):
        emit("基础筛选", message="检查正文长度、乱码、摘要与参考文献")
        rows = original_quality(*args, **kwargs)
        emit("基础筛选完成", message=f"已检查 {len(rows)} 篇正文")
        return rows
    run_stage1.build_quality_report = quality
    emit("扫描与去重", message="原脚本开始处理当前批次")
    return run_stage1.main()


if __name__ == "__main__":
    raise SystemExit(main())
