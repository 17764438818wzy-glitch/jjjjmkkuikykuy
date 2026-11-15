from __future__ import annotations

from src.paper_cleaning.major_dictionary import (
    UNKNOWN_MAJOR,
    UNKNOWN_MAJOR_GROUP,
    detect_major_info,
    normalize_major_name,
)


def test_detect_major_from_front_matter_finance_management() -> None:
    result = detect_major_info("专业：财务管理\n摘要\n正文", "乱目录/sample.txt")

    assert result.detected_major == "财务管理"
    assert result.detected_major_group == "经管类"
    assert result.major_detect_source == "front_matter"
    assert result.major_confidence == "high"


def test_normalizes_nursing_alias_to_nursing_science() -> None:
    result = detect_major_info("专业：护理\n摘要\n正文", "")

    assert result.detected_major == "护理学"
    assert result.detected_major_group == "医护类"


def test_detect_major_from_path_with_class_suffix() -> None:
    result = detect_major_info("摘要\n正文", "管理学院/财务管理1901班/xxx.txt")

    assert result.detected_major == "财务管理"
    assert result.detected_major_group == "经管类"
    assert result.major_detect_source == "path"


def test_detect_major_from_file_name() -> None:
    result = detect_major_info("摘要\n正文", "论文/计算机科学与技术毕业论文.txt")

    assert result.detected_major == "计算机科学与技术"
    assert result.detected_major_group == "计算机与信息类"


def test_detect_major_from_first_3000_chars() -> None:
    result = detect_major_info("摘要\n研究对象\n专业：学前教育\n正文", "")

    assert result.detected_major == "学前教育"
    assert result.detected_major_group == "教育类"
    assert result.major_detect_source == "body"


def test_unknown_major_falls_back_to_unknown_group() -> None:
    result = detect_major_info("摘要\n正文", "乱目录/无关键词.txt")

    assert result.detected_major == UNKNOWN_MAJOR
    assert result.detected_major_group == UNKNOWN_MAJOR_GROUP
    assert result.major_detect_source == "unknown"
    assert result.major_confidence == "none"


def test_normalize_major_variants() -> None:
    assert normalize_major_name("财务管理专业") == "财务管理"
    assert normalize_major_name("财务管理1901") == "财务管理"
    assert normalize_major_name("财务管理（香港）") == "财务管理"
    assert normalize_major_name("财务管理本科") == "财务管理"
    assert normalize_major_name("计算机专业") == "计算机科学与技术"
    assert normalize_major_name("计算机科学技术") == "计算机科学与技术"
