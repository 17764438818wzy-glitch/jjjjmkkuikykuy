from __future__ import annotations

import csv
from pathlib import Path

from src.paper_cleaning.sanitize import (
    MAPPING_FIELDNAMES,
    REPORT_FIELDNAMES,
    detect_major,
    remove_front_matter,
    sanitize_directory,
    sanitize_text,
)


def test_remove_front_matter_keeps_text_from_abstract_anchor() -> None:
    text = (
        "学生姓名：张三\n"
        "学号：201908023039\n"
        "指导教师：李老师\n\n"
        "摘要\n本文研究企业财务风险管理。"
    )

    result = remove_front_matter(text)

    assert result.text == "摘要\n本文研究企业财务风险管理。"
    assert result.removed is True
    assert result.removed_chars == text.index("摘要")
    assert "张三" not in result.text
    assert "201908023039" not in result.text


def test_remove_front_matter_keeps_spaced_chinese_abstract_anchor() -> None:
    text = (
        "本科毕业论文\n"
        "学生姓名: 张三\n"
        "学    号: 201908023039\n"
        "[SECTION_BREAK]\n"
        "摘  要\n中文摘要应该保留。\n"
        "[PAGE_BREAK]\n"
        "Abstract\nEnglish abstract should also remain."
    )

    result = remove_front_matter(text)

    assert result.text.startswith("摘  要\n中文摘要应该保留。")
    assert "Abstract" in result.text
    assert "学生姓名" not in result.text
    assert result.removed is True


def test_remove_front_matter_preserves_thesis_title_but_removes_personal_fields() -> None:
    text = (
        "本科毕业论文\n"
        "题目：\t清仓式分红对企业创新的影响研究\n"
        "学    院:\t管理学院\n"
        "专    业:\t财务管理\n"
        "学    号:\t201905171025\n"
        "学生姓名:\t方莎莎\n"
        "指导教师:\t朱新玲\n"
        "[SECTION_BREAK]\n"
        "摘  要\n摘要正文。"
    )

    result = remove_front_matter(text)

    assert result.text.startswith("题目：清仓式分红对企业创新的影响研究\n摘  要")
    assert "学    号" not in result.text
    assert "201905171025" not in result.text
    assert "学生姓名" not in result.text
    assert "方莎莎" not in result.text
    assert "指导教师" not in result.text
    assert "朱新玲" not in result.text
    assert result.removed is True


def test_remove_front_matter_handles_long_cover_before_abstract() -> None:
    text = (
        "\u672c\u79d1\u6bd5\u4e1a\u8bba\u6587\n"
        "\u9898\u76ee\uff1a\u5c0f\u5fae\u4f01\u4e1a\u76c8\u5229\u80fd\u529b\u5206\u6790\n"
        "\u4e13\u4e1a\uff1a\u8d22\u52a1\u7ba1\u7406\n"
        "\u5b66\u751f\u59d3\u540d\uff1a\u5f20\u4e09\n"
        "\u5b66\u53f7\uff1a201908023039\n"
        "\u6307\u5bfc\u6559\u5e08\uff1a\u674e\u8001\u5e08\n"
        + ("\u5c01\u9762\u8868\u683c\u5b57\u6bb5\n" * 700)
        + "\u6458\u8981\n\u6b63\u6587\u5e94\u5f53\u4fdd\u7559\u3002"
    )

    result = remove_front_matter(text)

    assert result.removed is True
    assert result.text.startswith(
        "\u9898\u76ee\uff1a\u5c0f\u5fae\u4f01\u4e1a\u76c8\u5229\u80fd\u529b\u5206\u6790\n"
        "\u6458\u8981\n\u6b63\u6587\u5e94\u5f53\u4fdd\u7559\u3002"
    )
    assert "\u5b66\u751f\u59d3\u540d" not in result.text
    assert "201908023039" not in result.text
    assert "\u6307\u5bfc\u6559\u5e08" not in result.text


def test_sanitize_text_preserves_scholar_names_in_research_sections() -> None:
    text = (
        "国内外研究现状中，张三认为企业财务风险管理非常重要。\n"
        "李四（2021）指出，内部控制会影响风险水平。\n"
        "参考文献\n"
        "[1] 王五. 企业财务风险研究."
    )

    result = sanitize_text(text)

    assert "张三" in result.text
    assert "李四" in result.text
    assert "王五" in result.text
    assert "[NAME]" not in result.text


def test_sanitize_text_replaces_phone_email_and_id_card() -> None:
    text = (
        "联系电话：13812345678\n"
        "邮箱：student@example.com\n"
        "身份证号：110105199001011234"
    )

    result = sanitize_text(text)

    assert "13812345678" not in result.text
    assert "student@example.com" not in result.text
    assert "110105199001011234" not in result.text
    assert "[PHONE]" in result.text
    assert "[EMAIL]" in result.text
    assert "[ID_CARD]" in result.text
    assert result.stats["phone_hits"] == 1
    assert result.stats["email_hits"] == 1
    assert result.stats["id_card_hits"] == 1


def test_sanitize_text_does_not_redact_phone_like_digits_inside_decimal_amounts() -> None:
    text = "营业收入\t118348876.58\t14047923607.47\t1857872306.39"

    result = sanitize_text(text)

    assert result.text == text
    assert result.stats["phone_hits"] == 0


def test_sanitize_text_replaces_student_id_only_near_field_label() -> None:
    text = "学号：201908023039\n研究样本年份包括 2021 年和 2023 年。"

    result = sanitize_text(text)

    assert "学号：[STUDENT_ID]" in result.text
    assert "201908023039" not in result.text
    assert "2021" in result.text
    assert "2023" in result.text
    assert result.stats["student_id_hits"] == 1


def test_sanitize_text_replaces_field_names_teacher_class_and_contact() -> None:
    text = (
        "学生姓名：崔紫琦\n"
        "作者：王小明\n"
        "指导教师：潘雅琼\n"
        "专业班级：财务管理（香港）1901\n"
        "QQ：123456789\n"
        "微信：student_wechat"
    )

    result = sanitize_text(text)

    assert "学生姓名：[NAME]" in result.text
    assert "作者：[NAME]" in result.text
    assert "指导教师：[TEACHER]" in result.text
    assert "专业班级：[CLASS]" in result.text
    assert "QQ：[CONTACT]" in result.text
    assert "微信：[CONTACT]" in result.text
    assert "崔紫琦" not in result.text
    assert "王小明" not in result.text
    assert "潘雅琼" not in result.text
    assert "财务管理（香港）1901" not in result.text
    assert result.stats["student_name_field_hits"] == 2
    assert result.stats["teacher_field_hits"] == 1
    assert result.stats["class_field_hits"] == 1
    assert result.stats["contact_hits"] == 2


def test_sanitize_text_does_not_redact_non_field_author_or_wechat_mentions() -> None:
    text = (
        "文献综述中，作者认为内部控制很重要。\n"
        "用户可以使用支付宝、微信支付和手机淘宝下单。\n"
        "相关研究指出，班级管理并非本文重点。"
    )

    result = sanitize_text(text)

    assert result.text == text
    assert result.stats["student_name_field_hits"] == 0
    assert result.stats["class_field_hits"] == 0
    assert result.stats["contact_hits"] == 0


def test_sanitize_directory_writes_outputs_and_report(tmp_path: Path) -> None:
    input_dir = tmp_path / "raw_text"
    output_dir = tmp_path / "sanitized_text"
    report_path = tmp_path / "output" / "sanitize_report.csv"
    mapping_path = tmp_path / "output" / "sanitized_mapping.csv"
    source_path = input_dir / "管理学院" / "财务管理1901班" / "张三_2019" / "sample.txt"
    source_path.parent.mkdir(parents=True)
    source_path.write_text(
        "题目：保留论文题目\n专业：财务管理\n学生姓名：张三\n学号：201908023039\n摘要\n电话：13900001111",
        encoding="utf-8",
    )

    report_rows, mapping_rows = sanitize_directory(
        input_dir,
        output_dir,
        report_path,
        mapping_path,
    )

    sanitized_path = output_dir / "经管财会类" / "财务管理" / "FM_000001.txt"
    output_text = sanitized_path.read_text(encoding="utf-8")
    assert output_text == "题目：保留论文题目\n摘要\n电话：[PHONE]"
    assert report_rows[0]["file_id"] == "FM_000001"
    assert report_rows[0]["detected_major"] == "财务管理"
    assert report_rows[0]["sanitized_path"] == str(sanitized_path)
    assert report_rows[0]["removed_front_matter"] == "true"
    assert report_rows[0]["phone_hits"] == "1"
    assert mapping_rows[0]["file_id"] == "FM_000001"
    assert mapping_rows[0]["original_relative_path"] == (
        "管理学院/财务管理1901班/张三_2019/sample.txt"
    )
    assert mapping_rows[0]["sanitized_path"] == str(sanitized_path)
    assert mapping_rows[0]["detected_major_group"] == "经管财会类"
    assert mapping_rows[0]["detected_subject_group"] == "经管财会类"
    assert mapping_rows[0]["detected_major_or_direction"] == "财务管理"
    assert mapping_rows[0]["major_detect_source"] == "front_matter"
    assert mapping_rows[0]["major_confidence"] == "high"
    assert mapping_rows[0]["detected_title"] == "保留论文题目"
    assert not (output_dir / "管理学院").exists()

    with report_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        report_rows = list(reader)

    assert reader.fieldnames == REPORT_FIELDNAMES
    assert report_rows[0]["file_id"] == "FM_000001"

    with mapping_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        mapping_report_rows = list(reader)

    assert reader.fieldnames == MAPPING_FIELDNAMES
    assert mapping_report_rows == mapping_rows


def test_sanitize_directory_outputs_group_and_major_path(tmp_path: Path) -> None:
    input_dir = tmp_path / "raw_text"
    output_dir = tmp_path / "sanitized_text"
    report_path = tmp_path / "output" / "sanitize_report.csv"
    mapping_path = tmp_path / "output" / "sanitized_mapping.csv"
    source_path = input_dir / "sample.txt"
    source_path.parent.mkdir(parents=True)
    source_path.write_text("题目：测试\n专业：护理\n摘要\n正文", encoding="utf-8")

    report_rows, mapping_rows = sanitize_directory(
        input_dir,
        output_dir,
        report_path,
        mapping_path,
    )

    sanitized_path = output_dir / "医护健康类" / "护理学" / "NUR_000001.txt"
    assert sanitized_path.exists()
    assert report_rows[0]["sanitized_path"] == str(sanitized_path)
    assert mapping_rows[0]["detected_major"] == "护理学"
    assert mapping_rows[0]["detected_major_group"] == "医护健康类"


def test_sanitize_directory_preserves_unlabeled_cover_title(tmp_path: Path) -> None:
    input_dir = tmp_path / "raw_text"
    output_dir = tmp_path / "sanitized_text"
    report_path = tmp_path / "output" / "sanitize_report.csv"
    mapping_path = tmp_path / "output" / "sanitized_mapping.csv"
    source_path = input_dir / "清仓式分红对企业创新的影响研究.txt"
    source_path.parent.mkdir(parents=True)
    source_path.write_text(
        "本科毕业论文\n"
        "清仓式分红对企业创新的影响研究\n"
        "学院：管理学院\n"
        "专业：财务管理\n"
        "学生姓名：张三\n"
        "摘要\n"
        "本文研究企业创新。",
        encoding="utf-8",
    )

    _report_rows, mapping_rows = sanitize_directory(
        input_dir,
        output_dir,
        report_path,
        mapping_path,
    )

    sanitized_path = output_dir / "经管财会类" / "财务管理" / "FM_000001.txt"
    output_text = sanitized_path.read_text(encoding="utf-8")
    assert output_text.startswith("题目：清仓式分红对企业创新的影响研究\n摘要")
    assert mapping_rows[0]["detected_title"] == "清仓式分红对企业创新的影响研究"
    assert "学生姓名" not in output_text


def test_extract_title_removes_topic_name_prefix() -> None:
    from src.paper_cleaning.sanitize import extract_title

    assert extract_title("课题名称\t2019年秋季长沙市雨花区初级中学花式跳绳竞赛策划方案\n摘要") == (
        "2019年秋季长沙市雨花区初级中学花式跳绳竞赛策划方案"
    )


def test_extract_title_ignores_contents_and_uses_filename() -> None:
    from src.paper_cleaning.sanitize import extract_title

    assert extract_title(
        "Contents\nAbstract\nThis paper discusses literature.",
        "0正文-the Escapism of Densher in the Wings of the Dove.docx",
    ) == "the Escapism of Densher in the Wings of the Dove"


def test_extract_title_ignores_table_header_and_uses_filename() -> None:
    from src.paper_cleaning.sanitize import extract_title

    assert extract_title(
        "产品设计\t工艺设计\t方案设计\n摘要\n正文",
        "吉首菁洋商贸会计核算制度设计方案.谢群.docx",
    ).startswith("吉首菁洋商贸会计核算制度设计方案")


def test_extract_title_ignores_intro_and_generic_cover_labels() -> None:
    from src.paper_cleaning.sanitize import extract_title

    assert extract_title(
        "Introduction\nThis paper discusses literature.",
        "0正文-the Escapism of Densher in the Wings of the Dove.docx",
    ) == "the Escapism of Densher in the Wings of the Dove"
    assert extract_title("编    号\n摘要\n正文", "建筑设计与人文化的和谐统一.docx") == "建筑设计与人文化的和谐统一"


def test_extract_title_cleans_common_filename_prefixes() -> None:
    from src.paper_cleaning.sanitize import extract_title

    assert extract_title("", "3.23（已修改）吉首菁洋商贸会计核算制度设计方案.谢群.docx") == "吉首菁洋商贸会计核算制度设计方案"
    assert extract_title("", "企业目标与会计目标的基本关系--申晶1稿.docx") == "企业目标与会计目标的基本关系"
    assert extract_title("", "3.23 语法名量词.docx") == "语法名量词"


def test_extract_title_removes_leading_name_and_rejects_cover_institution() -> None:
    from src.paper_cleaning.sanitize import extract_title

    assert extract_title("", "柴林杰- 家庭理财规划-修改.docx") == "家庭理财规划"
    assert extract_title(
        "海 南 大 学\n摘要\n正文",
        "毕业论文中小学教师薪酬制度改革研究.docx",
    ) == "中小学教师薪酬制度改革研究"
    assert extract_title(
        "FOSHAN UNIVERSITY\n摘要\n正文",
        "社会体育指导与管理专业就业前景分析与研究.docx",
    ) == "社会体育指导与管理专业就业前景分析与研究"


def test_extract_title_rejects_filename_revision_noise_and_school_report() -> None:
    from src.paper_cleaning.sanitize import extract_title

    assert extract_title("", "3.24 (改重 ）刘虹云-论文20180322修改版 - 副本.docx") == ""
    assert extract_title("", "中国传媒大学南广学院毕业开题报告.docx") == ""


def test_extract_title_cleans_standalone_workflow_markers() -> None:
    from src.paper_cleaning.sanitize import extract_title

    assert extract_title("", "写作 论线性空间与欧式空间的对比.docx") == "论线性空间与欧式空间的对比"
    assert extract_title("", "开题 基于OFDM技术的卫星移动通信多址技术分析任务书.docx") == "基于OFDM技术的卫星移动通信多址技术分析"
    assert extract_title("", "改生态学视角下普通学校音乐教育研究.docx") == "生态学视角下普通学校音乐教育研究"


def test_detect_major_from_front_matter_major_field() -> None:
    text = "题目：某论文\n专业：财务管理\n摘要\n正文"

    assert detect_major(text, "管理学院/未知班/sample.txt") == "财务管理"


def test_detect_major_from_original_path_when_front_matter_has_no_major() -> None:
    text = "题目：某论文\n摘要\n正文"

    assert detect_major(text, "管理学院/财务管理1901班/sample.txt") == "财务管理"


def test_detect_major_unknown_when_no_keyword_found() -> None:
    text = "题目：某论文\n摘要\n正文"

    assert detect_major(text, "管理学院/其他专业/sample.txt") == "未识别方向"


def test_sanitize_directory_respects_limit_and_uses_per_major_numbering(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "raw_text"
    output_dir = tmp_path / "sanitized_text"
    report_path = tmp_path / "output" / "sanitize_report.csv"
    mapping_path = tmp_path / "output" / "sanitized_mapping.csv"
    for name, major in [
        ("a.txt", "会计学"),
        ("b.txt", "市场营销"),
        ("c.txt", "未知专业"),
    ]:
        path = input_dir / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"题目：{name}\n专业：{major}\n摘要\n正文", encoding="utf-8")

    report_rows, _ = sanitize_directory(
        input_dir,
        output_dir,
        report_path,
        mapping_path,
        limit=2,
    )

    assert [row["file_id"] for row in report_rows] == ["ACC_000001", "MKT_000001"]
    assert (output_dir / "经管财会类" / "会计学" / "ACC_000001.txt").exists()
    assert (output_dir / "经管财会类" / "市场营销" / "MKT_000001.txt").exists()
    assert not (output_dir / "未识别_其他类" / "未识别方向" / "UNK_000001.txt").exists()
def test_sanitize_directory_uses_classification_hint_but_strips_it_from_output(tmp_path: Path) -> None:
    input_dir = tmp_path / "raw_text"
    output_dir = tmp_path / "sanitized_text"
    report_path = tmp_path / "output" / "sanitize_report.csv"
    mapping_path = tmp_path / "output" / "sanitized_mapping.csv"
    source_path = input_dir / "paper.txt"
    source_path.parent.mkdir(parents=True)
    source_path.write_text(
        "DITY_CLASSIFICATION_HINT_START\n"
        "\u5b66\u9662\u540d\u79f0 \u4e13\u4e1a\u540d\u79f0 \u6c49\u8bed\u8a00\u6587\u5b66 "
        "\u5b66\u751f\u59d3\u540d \u67d0\u67d0\n"
        "DITY_CLASSIFICATION_HINT_END\n"
        "\u6458\u8981\n\u672c\u6587\u5206\u6790\u300a\u56f4\u57ce\u300b\u7684\u8bbd\u523a\u827a\u672f\u3002",
        encoding="utf-8",
    )

    _report_rows, mapping_rows = sanitize_directory(
        input_dir,
        output_dir,
        report_path,
        mapping_path,
    )

    sanitized_path = output_dir / "\u6587\u5b66\u5916\u8bed\u4f20\u5a92\u7c7b" / "\u6c49\u8bed\u8a00\u6587\u5b66" / "CHI_000001.txt"
    output_text = sanitized_path.read_text(encoding="utf-8")
    assert mapping_rows[0]["file_id"] == "CHI_000001"
    assert mapping_rows[0]["detected_major_or_direction"] == "\u6c49\u8bed\u8a00\u6587\u5b66"
    assert "DITY_CLASSIFICATION_HINT_START" not in output_text
    assert "DITY_CLASSIFICATION_HINT_END" not in output_text
    assert "\u5b66\u751f\u59d3\u540d" not in output_text
    assert output_text.startswith("\u6458\u8981")
