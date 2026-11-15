from __future__ import annotations

import csv
from pathlib import Path

from src.paper_cleaning.tech_classifier import (
    INDEX_FIELDNAMES,
    REPORT_FIELDNAMES,
    build_tech_reports,
    classify_tech,
)


def assert_title(title: str, tech_type: str, tech_subtype: str) -> None:
    result = classify_tech(detected_title=title)
    assert result.tech_type == tech_type
    assert result.tech_subtype == tech_subtype


def test_classifies_spring_boot_management_system() -> None:
    assert_title("基于Spring Boot的学生信息管理系统设计", "Java管理系统", "管理系统")


def test_classifies_python_data_visualization() -> None:
    assert_title("基于Python的旅游数据可视化分析系统", "Python项目", "数据可视化")


def test_classifies_python_yolo_as_image_recognition() -> None:
    assert_title("基于Python和YOLO的水果识别系统", "Python项目", "图像识别")


def test_classifies_stm32() -> None:
    assert_title("基于STM32的智能家居控制系统设计", "单片机", "STM32")


def test_classifies_51_mcu() -> None:
    assert_title("基于51单片机的温湿度检测系统设计", "单片机", "51单片机")


def test_classifies_siemens_plc() -> None:
    assert_title("基于西门子PLC的电梯控制系统设计", "PLC", "西门子PLC")


def test_classifies_mitsubishi_plc() -> None:
    assert_title("基于三菱PLC的机械手控制系统设计", "PLC", "三菱PLC")


def test_classifies_labview_data_acquisition() -> None:
    assert_title("基于LabVIEW的数据采集系统设计", "LabVIEW", "数据采集")


def test_classifies_wechat_mini_program() -> None:
    assert_title("基于微信小程序的校园二手交易平台设计", "微信小程序", "小程序系统")


def test_classifies_unknown() -> None:
    assert_title("乡村旅游品牌传播路径研究", "未识别技术类型", "未识别技术子类")


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_build_tech_reports_skips_reject_and_writes_index(tmp_path: Path) -> None:
    sanitized = tmp_path / "sanitized_text" / "经管类" / "电子商务" / "EC_000001.txt"
    rejected = tmp_path / "sanitized_text" / "未识别专业" / "UNK_000001.txt"
    sanitized.parent.mkdir(parents=True)
    rejected.parent.mkdir(parents=True)
    sanitized.write_text("摘要\n基于Spring Boot和Vue的商城管理系统。\n参考文献", encoding="utf-8")
    rejected.write_text("短文", encoding="utf-8")
    mapping = tmp_path / "sanitized_mapping.csv"
    quality = tmp_path / "quality_report.csv"
    report = tmp_path / "tech_classification_report.csv"
    index = tmp_path / "tech_group_index.csv"

    write_csv(
        mapping,
        [
            "file_id",
            "detected_major_group",
            "detected_major",
            "detected_title",
            "original_relative_path",
            "sanitized_path",
        ],
        [
            {
                "file_id": "EC_000001",
                "detected_major_group": "经管类",
                "detected_major": "电子商务",
                "detected_title": "基于Spring Boot的商城管理系统设计",
                "original_relative_path": "a.docx",
                "sanitized_path": str(sanitized),
            },
            {
                "file_id": "UNK_000001",
                "detected_major_group": "未识别专业",
                "detected_major": "未识别专业",
                "detected_title": "无法判断技术类型",
                "original_relative_path": "b.docx",
                "sanitized_path": str(rejected),
            },
        ],
    )
    write_csv(
        quality,
        ["file_id", "quality_status"],
        [
            {"file_id": "EC_000001", "quality_status": "pass"},
            {"file_id": "UNK_000001", "quality_status": "reject"},
        ],
    )

    rows, index_rows = build_tech_reports(mapping, quality, report, index)

    assert len(rows) == 1
    assert rows[0]["tech_type"] == "Java管理系统"
    assert rows[0]["tech_subtype"] == "管理系统"
    assert rows[0]["file_id"] == "EC_000001"
    assert index_rows[0]["tech_type"] == "Java管理系统"
    assert index_rows[0]["tech_subtype"] == "管理系统"
    assert index_rows[0]["file_id"] == "EC_000001"
    assert index_rows[0]["sanitized_path"] == str(sanitized)
    assert index_rows[0]["detected_major"] == "电子商务"
    assert index_rows[0]["detected_major_group"] == "经管类"
    assert index_rows[0]["detected_title"] == "基于Spring Boot的商城管理系统设计"
    with report.open("r", encoding="utf-8-sig", newline="") as handle:
        assert csv.DictReader(handle).fieldnames == REPORT_FIELDNAMES
    with index.open("r", encoding="utf-8-sig", newline="") as handle:
        assert csv.DictReader(handle).fieldnames == INDEX_FIELDNAMES
