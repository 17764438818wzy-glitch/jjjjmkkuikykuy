from __future__ import annotations

import csv
from pathlib import Path

from src.paper_cleaning.sanitize import sanitize_directory


def sanitize_one(tmp_path: Path, text: str) -> tuple[Path, dict[str, str]]:
    input_dir = tmp_path / "raw_text"
    output_dir = tmp_path / "sanitized_text"
    report_path = tmp_path / "output" / "sanitize_report.csv"
    mapping_path = tmp_path / "output" / "sanitized_mapping.csv"
    source = input_dir / "sample.txt"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text(text, encoding="utf-8")
    _report_rows, mapping_rows = sanitize_directory(input_dir, output_dir, report_path, mapping_path)
    return output_dir, mapping_rows[0]


def test_finance_output_layout(tmp_path: Path) -> None:
    output_dir, mapping = sanitize_one(tmp_path, "题目：测试\n专业：财务管理\n摘要\n正文")

    assert (output_dir / "经管财会类" / "财务管理" / "FM_000001.txt").exists()
    assert mapping["detected_subject_group"] == "经管财会类"
    assert mapping["detected_major_or_direction"] == "财务管理"
    assert mapping["tech_type"] == "不适用"


def test_technical_output_layouts(tmp_path: Path) -> None:
    cases = [
        ("基于Spring Boot的学生信息管理系统设计\n摘要\n正文", "计算机软件与数据类", "Java管理系统"),
        ("基于Python的数据可视化分析系统\n摘要\n正文", "计算机软件与数据类", "Python数据可视化"),
        ("基于YOLO的水果图像识别系统\n摘要\n正文", "计算机软件与数据类", "Python图像识别"),
        ("基于STM32的智能家居控制系统设计\n摘要\n正文", "智能硬件与控制类", "STM32"),
        ("基于51单片机的温湿度检测系统设计\n摘要\n正文", "智能硬件与控制类", "51单片机"),
        ("基于PLC的电梯控制系统设计\n摘要\n正文", "智能硬件与控制类", "PLC"),
        ("基于LabVIEW的数据采集系统设计\n摘要\n正文", "智能硬件与控制类", "LabVIEW"),
    ]
    for index, (text, group, direction) in enumerate(cases):
        case_dir = tmp_path / f"case_{index}"
        output_dir, mapping = sanitize_one(case_dir, text)
        assert list((output_dir / group / direction).glob("*.txt"))
        assert mapping["detected_subject_group"] == group
        assert mapping["detected_major_or_direction"] == direction


def test_unknown_output_layout_and_no_sanitized_docx(tmp_path: Path) -> None:
    output_dir, mapping = sanitize_one(tmp_path, "题目：无法识别\n摘要\n正文")

    assert (output_dir / "未识别_其他类" / "未识别方向" / "UNK_000001.txt").exists()
    assert mapping["detected_subject_group"] == "未识别/其他类"
    assert mapping["detected_major_or_direction"] == "未识别方向"
    assert not (tmp_path / "sanitized_docx").exists()
    assert [path.suffix for path in output_dir.rglob("*") if path.is_file()] == [".txt"]


def test_mapping_has_required_final_fields(tmp_path: Path) -> None:
    _output_dir, mapping = sanitize_one(tmp_path, "题目：基于STM32的系统设计\n摘要\n正文")

    required = {
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
        "tech_keywords",
        "tech_confidence",
        "sanitized_text_path",
        "quality_status",
        "status",
        "error_message",
    }
    assert required.issubset(mapping.keys())


def test_generic_prefix_file_ids_remain_unique_across_directions(tmp_path: Path) -> None:
    input_dir = tmp_path / "raw_text"
    output_dir = tmp_path / "sanitized_text"
    report_path = tmp_path / "output" / "sanitize_report.csv"
    mapping_path = tmp_path / "output" / "sanitized_mapping.csv"
    input_dir.mkdir(parents=True)
    (input_dir / "a.txt").write_text("题目：工程管理论文\n专业：工程管理\n摘要\n正文", encoding="utf-8")
    (input_dir / "b.txt").write_text("题目：英语论文\n专业：英语\n摘要\n正文", encoding="utf-8")

    _report_rows, mapping_rows = sanitize_directory(input_dir, output_dir, report_path, mapping_path)

    assert [row["file_id"] for row in mapping_rows] == ["MAJ_000001", "MAJ_000002"]
