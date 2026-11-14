"""Chunk generation for formal paper resources."""

from __future__ import annotations

import re
from hashlib import sha256

from paper_pipeline.vector_retrieval.models import FormalResource, PaperChunk


NOISE_TITLES = {
    "封面",
    "目录",
    "致谢",
    "附录",
    "附录A",
    "附录B",
    "声明页",
    "原创性声明",
    "学位论文原创性声明",
    "学士学位论文",
    "授权声明",
    "参考文献",
    "Extracted Chapter Structure",
    "Processing Evidence",
}

FRONT_MATTER_MARKERS = (
    "学位论文原创性声明",
    "原创性声明",
    "授权声明",
    "本人所提交的学位论文",
    "本人郑重声明",
)

ACKNOWLEDGEMENT_MARKERS = (
    "致谢",
    "我要感谢",
    "感谢在对我帮助",
    "四年时光",
)

APPENDIX_MARKERS = (
    "附录",
    "附录A",
    "附录B",
    "调查问卷",
    "尊敬的同学",
    "尊敬的老师",
)

CONTENT_START_PATTERNS = (
    r"^摘\s*要\b",
    r"^ABSTRACT\b",
    r"^引言\b",
    r"^绪论\b",
    r"^第\s*[一二三四五六七八九十\d]+\s*章\b",
    r"^\d+(?:\.\d+)?\s*(?:绪论|引言|研究对象|研究方法|研究结果|结果|分析)\b",
)

TAIL_NOISE_PATTERNS = (
    r"^参考文献\b",
    r"^致谢\b",
    r"^附录\b",
    r"^附\s+录\b",
    r"问卷到此结束",
    r"访谈提纲",
    r"完成这篇毕业论文.*感谢",
    r"行文至此",
    r"感谢我的",
    r"我要感谢",
)

PARSER_LOG_PREFIXES = (
    "- LibreOffice invoked:",
    "- Parser selected:",
    "- LLM invoked:",
)

REAL_DATA_KEYWORDS = (
    "前测",
    "后测",
    "实验结果",
    "统计检验",
    "显著性",
    "p值",
    "问卷",
    "样本量",
    "人数",
    "均值",
    "标准差",
    "训练数据",
    "测试指标",
)


class ChunkingService:
    def __init__(self, chunk_rule_version: str = "chunk.v1") -> None:
        self.chunk_rule_version = chunk_rule_version

    def chunk_resource(self, resource: FormalResource) -> list[PaperChunk]:
        chunks: list[PaperChunk] = []
        paragraph_order = 0
        for section_title, body in _markdown_sections(resource.markdown_content):
            if _is_noise_title(section_title):
                continue
            normalized_body = _trim_flat_document_noise(section_title, body)
            paragraphs = _paragraphs(normalized_body)
            if not paragraphs:
                continue
            role = infer_content_role(section_title, normalized_body)
            clean_paragraphs = [
                paragraph for paragraph in paragraphs if not _is_noise_paragraph(paragraph)
            ]
            for paragraph in _merge_short_paragraphs(clean_paragraphs):
                paragraph_order += 1
                chunk_order = len(chunks) + 1
                chunk_text = paragraph.strip()
                chunks.append(
                    PaperChunk(
                        chunk_id=f"{resource.resource_id}-chunk-{chunk_order}",
                        resource_id=resource.resource_id,
                        paper_id=resource.paper_id,
                        title=resource.title,
                        major=resource.major,
                        discipline=resource.discipline,
                        category=resource.category,
                        direction=resource.direction,
                        thesis_type=resource.thesis_type,
                        tier=resource.tier,
                        resource_status=resource.resource_status,
                        chapter_title=section_title,
                        chapter_role=role,
                        paragraph_order=paragraph_order,
                        chunk_order=chunk_order,
                        chunk_text=chunk_text,
                        chunk_hash=_hash_text(chunk_text),
                        text_hash=resource.text_hash,
                        content_role=role,
                        needs_real_data=needs_real_data(chunk_text),
                        research_method_tags=list(resource.research_method_tags),
                        source_hash=resource.source_hash,
                        chunk_rule_version=self.chunk_rule_version,
                    ),
                )
        return chunks


def infer_content_role(title: str, text: str) -> str:
    value = f"{title}\n{text}"
    if _contains(value, ("背景", "意义", "研究价值")):
        return "background"
    if _contains(value, ("理论", "概念", "界定", "基础")):
        return "theory"
    if _contains(value, ("研究方法", "对象", "实验设计", "样本", "方法")):
        return "method"
    if _contains(value, ("数据", "结果", "统计", "前测", "后测", "显著性")):
        return "data_result"
    if _contains(value, ("问题", "原因", "影响因素", "不足")):
        return "problem_analysis"
    if _contains(value, ("对策", "建议", "优化", "策略")):
        return "strategy"
    if _contains(value, ("结论", "总结", "展望")):
        return "conclusion"
    return "other"


def needs_real_data(text: str) -> bool:
    return any(keyword in text for keyword in REAL_DATA_KEYWORDS)


def _markdown_sections(markdown: str) -> list[tuple[str, str]]:
    current_title = "正文"
    current_lines: list[str] = []
    sections: list[tuple[str, str]] = []
    for line in markdown.splitlines():
        heading = re.match(r"^\s{0,3}#{1,6}\s+(.+?)\s*$", line)
        if heading:
            if current_lines:
                sections.append((current_title, "\n".join(current_lines)))
            current_title = heading.group(1).strip()
            current_lines = []
        else:
            current_lines.append(line)
    if current_lines:
        sections.append((current_title, "\n".join(current_lines)))
    return sections


def _paragraphs(text: str) -> list[str]:
    return [item.strip() for item in re.split(r"\n\s*\n", text) if item.strip()]


def _merge_short_paragraphs(paragraphs: list[str]) -> list[str]:
    merged: list[str] = []
    buffer = ""
    for paragraph in paragraphs:
        if len(buffer) < 120:
            buffer = f"{buffer}\n{paragraph}".strip()
        else:
            merged.append(buffer)
            buffer = paragraph
    if buffer:
        merged.append(buffer)
    return merged


def _is_noise_title(title: str) -> bool:
    return any(noise in title for noise in NOISE_TITLES)


def _trim_flat_document_noise(section_title: str, body: str) -> str:
    if not _should_trim_flat_document(section_title, body):
        return body

    lines = [line.rstrip() for line in body.splitlines()]
    start_index = _content_start_index(lines)
    trimmed = lines[start_index:] if start_index is not None else lines

    kept: list[str] = []
    skipping_toc = False
    for line in trimmed:
        stripped = line.strip()
        if not stripped:
            kept.append(line)
            continue
        if _is_table_of_contents_heading(stripped):
            skipping_toc = True
            continue
        if skipping_toc:
            if _is_content_start_line(stripped) and not _is_toc_entry_line(stripped):
                skipping_toc = False
            else:
                continue
        if _is_parser_log_line(stripped):
            continue
        if _is_tail_noise_line(stripped):
            break
        kept.append(line)
    return "\n".join(kept)


def _should_trim_flat_document(section_title: str, body: str) -> bool:
    normalized_title = section_title.strip().lower()
    if normalized_title in {"cleaned paper text", "正文", "正 文"}:
        return True
    compact = " ".join(body.split())
    return any(marker in compact for marker in FRONT_MATTER_MARKERS)


def _content_start_index(lines: list[str]) -> int | None:
    for index, line in enumerate(lines):
        if _is_content_start_line(line.strip()):
            return index
    return None


def _is_content_start_line(line: str) -> bool:
    return any(
        re.search(pattern, line, flags=re.IGNORECASE)
        for pattern in CONTENT_START_PATTERNS
    )


def _is_tail_noise_line(line: str) -> bool:
    return any(re.search(pattern, line, flags=re.IGNORECASE) for pattern in TAIL_NOISE_PATTERNS)


def _is_parser_log_line(line: str) -> bool:
    return any(line.startswith(prefix) for prefix in PARSER_LOG_PREFIXES)


def _is_table_of_contents_heading(line: str) -> bool:
    return re.sub(r"\s+", "", line) == "目录"


def _is_toc_entry_line(line: str) -> bool:
    no_space = re.sub(r"\s+", "", line)
    if not re.search(r"\d+$", no_space):
        return False
    return (
        "章" in no_space
        or no_space.startswith(("引言", "绪论", "结论", "参考文献"))
        or re.match(r"^\d+(?:\.\d+)+", no_space) is not None
    )


def _is_noise_paragraph(text: str) -> bool:
    compact = " ".join(text.split())
    no_space = re.sub(r"\s+", "", compact)
    if not compact:
        return True
    if _is_parser_log_paragraph(text):
        return True
    if _looks_like_table_of_contents(text):
        return True
    if any(marker in compact for marker in FRONT_MATTER_MARKERS):
        return True
    if "学士学位论文" in compact and {"姓名", "学号"} <= set(_front_matter_labels(no_space)):
        return True
    if any(marker in compact for marker in ACKNOWLEDGEMENT_MARKERS):
        return True
    if _looks_like_appendix(compact):
        return True
    if compact.startswith("参考文献"):
        return True
    return False


def _front_matter_labels(no_space: str) -> set[str]:
    labels: set[str] = set()
    for label in ("姓名", "学号", "指导教师", "院系", "专业"):
        if label in no_space:
            labels.add(label)
    return labels


def _looks_like_appendix(text: str) -> bool:
    if text.startswith("附录") or text.startswith("附 錄"):
        return True
    return "附录" in text and any(marker in text for marker in APPENDIX_MARKERS)


def _is_parser_log_paragraph(text: str) -> bool:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return bool(lines) and all(_is_parser_log_line(line) for line in lines)


def _looks_like_table_of_contents(text: str) -> bool:
    no_space = re.sub(r"\s+", "", text)
    if "目录" not in no_space and not text.lstrip().startswith("- 关键词"):
        return False
    chapter_refs = len(re.findall(r"第\s*[一二三四五六七八九十\d]+\s*章", text))
    has_reference_entry = "参考文献" in text
    return chapter_refs >= 2 or has_reference_entry


def _contains(text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords)


def _hash_text(text: str) -> str:
    return sha256(text.encode("utf-8")).hexdigest()
